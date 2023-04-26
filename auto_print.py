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


printed_files=[]

print_batch = str(uuid.uuid4())



# Get Pacific timezone object
tz_us_pacific = tz.gettz('US/Pacific')

# Get the current datetime in local timezone
start_time = datetime.now(tz_us_pacific)


failed = []
def lambda_handler(event, context):
    global session
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded_auth_string}"
    })

    # Extract webhook payload
    payload = json.loads(event['body'])
    resource_url = payload["resource_url"][:-5] + 'True'
    print(f"Payload resource_url: {resource_url}")
    # Modify the resource_url so that it ends in includeShipmentItems=True rather than False; this way we can access the order items
    response = session.get(resource_url)
    data = response.json()
    print(data)
    shipments = data['shipments']
    print(f"Number of shipments: {len(shipments)}")

    # Sort the shipments by order number
    sorted_shipments = sorted(shipments, key=lambda x: int(x['orderNumber'].split('-')[0]))

    # Process the orders sequentially
    for shipment in sorted_shipments:
        process_order(shipment)

    # Move printed files to MLPs Sent
    for printed_file in printed_files:
        move_file_to_sent_folder(printed_file)

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


def process_order(order):
    shipment_create_time_str = order['createDate']
    time_diff_minutes = calculate_time_difference(shipment_create_time_str, start_time)

    # If shipment was created before Autoprint's timeout value, reject it
    if time_diff_minutes > 15:
        print("Shipment too old, can't process")
        return

    order_number = order["orderNumber"]
    print(f"Checking order number: {order_number}")

    # If the order number has a hyphen, then we're only interested in everything up to that hyphen
    if "-" in order_number:
        order_number = order_number.split("-")[0]

    order_items = order['shipmentItems']
    folders_to_search = [config.folder_1, config.folder_2]
    workers = 2
    
    if order['advancedOptions']['storeId'] == 310067:
        folders_to_search = [config.sent_folder_path]
        workers = 1
    
    for item in order_items:
        matching_key = get_matching_mlp_key(item['sku'], mlp_dict)
        if matching_key is not None and item['sku'] not in ["SUB - LG - D", "SUB - LG - S", "SUB - LG - G"]:
            print(f"{order['orderNumber']} has a lawn plan")
            lawn_plan_name = mlp_dict[matching_key]
            search_and_print(order_number, lawn_plan_name, folders_to_search, workers)

def search_and_print(order_number, lawn_plan_name, folders, threads):
    print(f"Searching for PDF file for order number {order_number} with lawn plan {lawn_plan_name}")
    try:
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(search_folder, folder, order_number, lawn_plan_name) for folder in folders]
            found_results = []
            for future in as_completed(futures):
                result = future.result()
                if result:
                    found_results.extend(result)
            
            for local_file_path, file_path in found_results:
                printed_files.append(file_path)
                print_file(local_file_path, config.PRINTER_SERVICE_API_KEY, order_number)

    except Exception as e:
        failed.append(order_number)
        print(f"Error: {e}")

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





def print_file(local_file_path, printer_service_api_key, order_number):
    print(f"Printing file for order number {order_number}")
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {base64.b64encode(printer_service_api_key.encode()).decode()}'
    }

    # Read the local file and encode it in base64
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
    if response.status_code == 400:
        failed.append(order_number)
        print(f"Error submitting print job for order number {order_number}: {response.json()['message']}")
