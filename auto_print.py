import os
import json
import requests
import base64
import dropbox
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import uuid
import config
from queue import Queue
from threading import Thread
from dateutil import tz
import re

auth_string = f"{config.SHIPSTATION_API_KEY}:{config.SHIPSTATION_API_SECRET}"
encoded_auth_string = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')


headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {encoded_auth_string}"
}

# Function to refresh Dropbox access token
def refresh_dropbox_token(refresh_token, app_key, app_secret):
    token_url = "https://api.dropboxapi.com/oauth2/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": app_key,
        "client_secret": app_secret,
    }

    response = requests.post(token_url, headers=headers, data=data)

    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Error refreshing token: {response.text}")

# Refresh the Dropbox access token
DROPBOX_ACCESS_TOKEN = refresh_dropbox_token(config.REFRESH_TOKEN, config.DROPBOX_APP_KEY, config.DROPBOX_APP_SECRET)


# Set up the Dropbox client
dbx_team = dropbox.DropboxTeam(DROPBOX_ACCESS_TOKEN)
dbx = dbx_team.as_user(config.DROPBOX_TEAM_MEMBER_ID)
dbx = dbx.with_path_root(dropbox.common.PathRoot.root(config.DROPBOX_NAMESPACE_ID))


mlp_dict= {
    "SUB - MLP Organic": "Organic Lawn Plan",
    "SUB - MLP STD": "Magic Lawn Plan",
    "SUB - MLPA": "Magic Lawn Plan Annual",
    "SUB - OLFP": "Organic Lawn Fertilizer Plan Monthly",
    "SUB - SFLP": "South Florida Lawn",
    "SUB - TLP - S": "Lone Star Lawn Plan Semi Monthly",
    "SUB - TLP": "Lone Star Lawn",
    "SUB - SELP": "Southeast",
    "SUB - GSLP": "Garden State Lawn",
    "SUB - MLPS": "Seasonal",
    "SUB - MLP PG": "Pristine Green",
    "SUB - MLP PP": "Playground Plus",
    "SUB - MLP SS": "Simply Sustainable",
    "05000": "Magic Lawn Plan",
    "10000": "Magic Lawn Plan",
    "15000": "Magic Lawn Plan"
}


print_batch = str(uuid.uuid4())



# Get Pacific timezone object
tz_us_pacific = tz.gettz('US/Pacific')

# Get the current datetime in local timezone
start_time = datetime.now(tz_us_pacific)

file_move_queue = Queue()

failed = []

def move_file_worker():
    while True:
        source_path = file_move_queue.get()
        if source_path is None:
            break

        move_file_to_sent_folder(source_path)
        file_move_queue.task_done()

def lambda_handler(event, context):
    global session
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded_auth_string}"
    })

    payload = json.loads(event['body'])
    resource_url = payload["resource_url"][:-5] + 'True'
    print(f"Payload resource_url: {resource_url}")
    response = session.get(resource_url)
    data = response.json()
    print(data)

    # Filter the shipments to only include those with lawn plans
    shipments = [shipment for shipment in data['shipments'] if any(any(plan_sku in item['sku'] for plan_sku in config.lawn_plan_skus) for item in shipment['shipmentItems'])]

    print(f"Number of shipments with lawn plans: {len(shipments)}")

    # Sort the filtered shipments by order number
    sorted_shipments = sorted(shipments, key=lambda x: int(x['orderNumber'].split('-')[0]))

    worker_thread = Thread(target=move_file_worker)
    worker_thread.start()

    # Process the orders sequentially
    for shipment in sorted_shipments:
        process_order(shipment)

    # Wait for the worker thread to finish moving all files
    file_move_queue.put((None, None))
    worker_thread.join()

    print(f"Function failed for orders: {failed}")



def calculate_time_difference(shipment_create_time_str, current_time):
    # Extract date and time components from the shipment_create_time_str
    year1, month1, day1, hour1, minute1, second1, microsecond1 = [
            int(x) for x in re.findall(r'\d+', shipment_create_time_str.replace("T", " "))[:7]
        ]

    shipment_datetime = datetime(year1, month1, day1, hour1, minute1, second1, tzinfo=current_time.tzinfo)
    time_diff = current_time - shipment_datetime
    time_diff_minutes = time_diff.total_seconds() // 60

    return time_diff_minutes


def get_matching_mlp_key(sku, mlp_dict):
    for key in mlp_dict:
        if sku.startswith(key):
            return key
    return None

    
def get_folder_contents(folder):
    result = dbx.files_list_folder(folder)
    all_entries = result.entries

    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        all_entries.extend(result.entries)

    return all_entries

def move_file_to_sent_folder(source_path):
    if os.path.dirname(source_path) == config.sent_folder_path:
        return
    print(f"Moving file from {source_path} to {config.sent_folder_path}")
    dest_path = config.sent_folder_path + "/" + source_path.split('/')[-1]
    try:
        dbx.files_move_v2(source_path, dest_path)
        print(f"File moved to {dest_path}")
    except Exception as e:
        print(f"Error moving file: {e}")

def get_order_by_number(order_number):
    url = f"https://ssapi.shipstation.com/orders?orderNumber={order_number}"
    response = session.get(url)
    data = response.json()
    if data["orders"]:
        return data["orders"][len(data["orders"])-1]
    else:
        failed_orders.append(order_number)
        print(f"No orders found for order #{order_number}")
        return

def process_order(order):
    shipment_create_time_str = order['createDate']
    time_diff_minutes = calculate_time_difference(shipment_create_time_str, start_time)

    # Ignore shipments that are older than Autoprint's timeout value
    if time_diff_minutes > 15:
        print("Shipment too old, can't process")
        return

    order_number = order["orderNumber"]
    print(f"Checking order number: {order_number}")
    order_items = order['shipmentItems']

    # Get the order with its custom field information
    order_with_tags = get_order_by_number(order_number)

    folder_to_search = None

    try:
        custom_field1 = order_with_tags['advancedOptions']['customField1']
        if "First" in custom_field1:
            folder_to_search = config.folder_1
        elif "Recurring" in custom_field1:
            folder_to_search = config.folder_2
    except KeyError:
        if order['advancedOptions']['storeId'] == 310067:
            folder_to_search = config.sent_folder_path

    if folder_to_search is None:
        print(f"Error:: Order #{order_number} seems to have a lawn plan but is not a manual order and not labelled 'First' or 'Recurring'; autoprint stopping")
        return

    lawn_plans = []
    for item in order_items:
        matching_key = get_matching_mlp_key(item['sku'], mlp_dict)
        if matching_key is not None and item['sku'] not in ["SUB - LG - D", "SUB - LG - S", "SUB - LG - G"]:
            lawn_plan_name = mlp_dict[matching_key]
            lawn_plans.append(lawn_plan_name)

    if "-" in order_number:
        order_number = order_number.split("-")[0]

    with ThreadPoolExecutor(max_workers=len(lawn_plans)) as executor:
        futures = [executor.submit(search_and_print, order_number, lawn_plan, folder_to_search) for lawn_plan in lawn_plans]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in thread: {e}")

def search_and_print(order_number, lawn_plan_name, folder):
    print(f"Searching for PDF file for order number {order_number} with lawn plan {lawn_plan_name}")
    try:
        found_results = search_folder(folder, order_number, lawn_plan_name)
        for local_file_path, file_path in found_results:
            print_file(file_path, config.PRINTER_SERVICE_API_KEY, order_number)
    except Exception as e:
        failed.append(order_number)
        print(f"Error: {e}")
    return


def search_folder(folder, order_number, lawn_plan_name):
    print(f"Searching in folder: {folder}")
    found_result = []
    search_results = dbx.files_search(folder, f"{order_number}")
    pdf_search_results = [result for result in search_results.matches if result.metadata.name.endswith('.pdf')]
    if pdf_search_results:
        total_files = len(pdf_search_results)
        for index, match in enumerate(pdf_search_results):
            filename = match.metadata.name
            if order_number in filename and lawn_plan_name in filename:
                file_path = match.metadata.path_display
                local_file_path = os.path.join('/tmp', filename)
                dbx.files_download_to_file(local_file_path, file_path)
                print(f"Order #{order_number} (Plan {index + 1} of {total_files}): found {filename} in folder {folder}\nFile downloaded and saved to {local_file_path}")
                found_result.append((local_file_path, file_path))
    else:
        print(f"No file found for order number {order_number} with lawn plan {lawn_plan_name} in {folder}")
    return found_result

def print_file(file_path, printer_service_api_key, order_number):
    print(f"Printing file for order number {order_number}")
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {base64.b64encode(printer_service_api_key.encode()).decode()}'
    }

    # Download the remote file and encode it in base64
    local_file_path = os.path.join('/tmp', os.path.basename(file_path))
    dbx.files_download_to_file(local_file_path, file_path)
    with open(local_file_path, 'rb') as file:
        file_content = file.read()
        encoded_content = base64.b64encode(file_content).decode('utf-8')

    print_job = {
        'printerId': config.DESKTOP_PRINTER_ID,
        'title': f'Lawn Plan from batch {print_batch} started at time: {start_time}',
        'contentType': 'pdf_base64',
        'content': encoded_content,
        'source': 'ShipStation Lawn Plan'
    }

    print(f"Sending print job: {print_job}")
    response = session.post('https://api.printnode.com/printjobs', headers=headers, json=print_job)

    if response.status_code == 201:
        print(f"Print job submitted successfully for order number {order_number}")
        file_move_queue.put(file_path)
    if response.status_code == 400:
        failed.append(order_number)
        print(f"Error submitting print job for order number {order_number}: {response.json()['message']}")

