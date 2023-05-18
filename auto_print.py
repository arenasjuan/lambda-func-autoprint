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
from threading import Thread, Lock
from dateutil import tz
import re

auth_string = f"{config.SHIPSTATION_API_KEY}:{config.SHIPSTATION_API_SECRET}"
encoded_auth_string = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')


headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {encoded_auth_string}",
    "X-Partner": config.x_partner
}


session = requests.Session()
session.headers.update(headers)

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


print_batch = str(uuid.uuid4())

# Define the shared dictionary and lock
print_counter_dict = {}
print_counter_lock = Lock()

# Get Pacific timezone object
tz_us_pacific = tz.gettz('US/Pacific')

# Get the current datetime in local timezone
start_time = datetime.now(tz_us_pacific)

file_move_queue = Queue()

failed = []

def move_file_worker():
    global failed
    while True:
        file_info = file_move_queue.get()
        if file_info is None:
            break

        source_path, order_number = file_info
        move_file_to_sent_folder(source_path, order_number)
        file_move_queue.task_done()



def lambda_handler(event, context):
    payload = json.loads(event['Records'][0]['body'])
    resource_url = payload["resource_url"][:-5] + 'True'
    print(f"Payload resource_url: {resource_url}")
    response = session.get(resource_url)
    data = response.json()
    print(data)

    # Filter the shipments to only include those with lawn plans
    shipments = [shipment for shipment in data['shipments'] if shipment['shipmentItems'] is not None and any(any(plan_sku in item['sku'] for plan_sku in config.lawn_plan_skus) for item in shipment['shipmentItems'])]

    print(f"Number of shipments with lawn plans: {len(shipments)}")

    # Print the order numbers before sorting
    print("Order numbers before sorting:", [shipment["orderNumber"] for shipment in shipments])

    # Sort the filtered shipments by order number
    sorted_shipments = sorted(shipments, key=lambda x: int(x['orderNumber'].split('-')[0]))

    # Print the order numbers after sorting
    print("Order numbers after sorting:", [shipment["orderNumber"] for shipment in sorted_shipments])

    worker_thread = Thread(target=move_file_worker)
    worker_thread.start()

    # Process the orders sequentially
    for shipment in sorted_shipments:
        process_order(shipment)

    # Wait for the worker thread to finish moving all files
    file_move_queue.put(None)
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


def get_matching_mlp_key(sku):
    for key in config.mlp_dict:
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

def move_file_to_sent_folder(source_path, order_number):
    if os.path.dirname(source_path) == config.sent_folder_path:
        print(f"(Log for #{order_number}) File already in 'Sent', no need to move", flush=True)
        return True
    dest_path = config.sent_folder_path + "/" + source_path.split('/')[-1]
    try:
        dbx.files_move_v2(source_path, dest_path)
        print(f"(Log for #{order_number}) File moved from {source_path} to {dest_path}", flush=True)
        return True
    except Exception as e:
        failed.append(order_number)
        print(f"(Log for #{order_number}) Error moving file: {e}", flush=True)
        return False


def get_order_by_number(order):
    url = f"https://ssapi.shipstation.com/orders?orderNumber={order['orderNumber']}"
    response = session.get(url)
    data = response.json()
    if data["orders"]:
        return next(o for o in data['orders'] if o['orderId'] == order['orderId'])
    else:
        failed_orders.append(order['orderNumber'])
        print(f"(Log for #{order['orderNumber']}) No orders found for order #{order['orderNumber']}", flush=True)
        return

def process_order(order):
    original_order_number = order["orderNumber"]

    LA_print_job = False

    shipment_create_time_str = order['createDate']
    time_diff_minutes = calculate_time_difference(shipment_create_time_str, start_time)

    # Ignore shipments that are older than Autoprint's timeout value
    if time_diff_minutes > 15:
        print(f"(Log for #{original_order_number}) Shipment too old, can't process", flush=True)
        return
    
    order_items = order['shipmentItems']
    searchable_order_number = original_order_number
    order_with_tags = get_order_by_number(order)
    tags = order_with_tags.get('tagIds', None)
    if tags is not None and 65915 in tags:
        LA_print_job = True

    if "-" in searchable_order_number:
        searchable_order_number = searchable_order_number.split("-")[0]

    folder_to_search = None

    if order['advancedOptions'].get('storeId') == 310067:
        folder_to_search = config.sent_folder_path
    else:
        if tags is not None and 62743 in tags:
            folder_to_search = config.folder_1
        elif tags is not None and 62744 in tags:
            folder_to_search = config.folder_2
        else:
            custom_field2 = order_with_tags['advancedOptions'].get('customField2', None)
            if custom_field2 is not None and custom_field2 == "F":
                folder_to_search = config.folder_1
            elif custom_field2 is not None and custom_field2 == "R":
                folder_to_search = config.folder_2
            else:
                print(f"(Log for #{original_order_number}) Error: Order #{original_order_number} seems to have a lawn plan but is not a manual order and not labelled 'First' or 'Recurring'; checking 'Sent' folder", flush=True)
                folder_to_search = config.sent_folder_path

    lawn_plan_keywords = []
    lawn_plan_items = [item for item in order_items for plan_sku in config.lawn_plan_skus if plan_sku in item['sku']]
    for item in lawn_plan_items:
        matching_key = get_matching_mlp_key(item['sku'])
        lawn_plan_keyword = config.mlp_dict[matching_key]
        lawn_plan_keywords.append(lawn_plan_keyword)

    print(f"(Log for #{original_order_number}) Preparing search for order #{original_order_number} with keyword(s) {lawn_plan_keywords}", flush=True)

    # Initialize print counter for current order
    with print_counter_lock:
        print_counter_dict[original_order_number] = 0

    with ThreadPoolExecutor(max_workers=len(lawn_plan_keywords)) as executor:
        futures = [executor.submit(search_and_print, original_order_number, searchable_order_number, lawn_plan_keyword, folder_to_search, len(lawn_plan_keywords), i + 1, LA_print_job) for i, lawn_plan_keyword in enumerate(lawn_plan_keywords)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"(Log for #{original_order_number}) Error in thread: {e}", flush=True)


def search_and_print(original_order_number, searchable_order_number, lawn_plan_keyword, folder, total_plans, plan_index, LA_print_job):
    folder_name = "'First'" if folder == config.folder_1 else "'Recurring'" if folder == config.folder_2 else "'Sent'"
    print(f"(Log for #{original_order_number}) Searching for lawn plan for order #{original_order_number} with keyword \'{lawn_plan_keyword}\' in {folder_name} folder", flush=True)

    try:
        printer_api_key = config.J_PRINTER_API_KEY if LA_print_job else config.PRINTER_SERVICE_API_KEY
        printer_id = config.LA_PRINTER_ID if LA_print_job else config.PRINTER_ID
        found_results = search_folder(folder, folder_name, searchable_order_number, original_order_number, lawn_plan_keyword, total_plans, plan_index)
        for local_file_path, file_path in found_results:
            with print_counter_lock:
                print_counter_dict[original_order_number] += 1
            print_file(file_path, printer_api_key, printer_id, original_order_number)
    except Exception as e:
        failed.append(original_order_number)
        print(f"Error: {e}")
    return


def search_folder(folder, folder_name, searchable_order_number, original_order_number, lawn_plan_keyword, total_plans, plan_index, searched_folders=None):
    if searched_folders is None:
        searched_folders = []

    found_result = []
    search_results = dbx.files_search(folder, f"{searchable_order_number}")
    file_found = False

    searched_folders.append(folder_name)

    if search_results.matches:
        for match in search_results.matches:
            filename = match.metadata.name
            if lawn_plan_keyword in filename:
                file_path = match.metadata.path_display
                local_file_path = os.path.join('/tmp', filename)
                dbx.files_download_to_file(local_file_path, file_path)
                found_result.append((local_file_path, file_path))
                file_found = True
                print(f"(Log for #{original_order_number}) Order #{original_order_number} (Plan {plan_index} of {total_plans}): found {file_path} in {folder_name} folder", flush=True)

    if not file_found:
        # Search in the Sent folder if the current folder is not the Sent folder
        if folder != config.sent_folder_path:
            print(f"(Log for #{original_order_number}) No file found for order number {original_order_number} with lawn plan keyword \'{lawn_plan_keyword}\' in {folder_name}; now searching in 'Sent' folder", flush=True)
            found_result = search_folder(config.sent_folder_path, 'Sent', searchable_order_number, original_order_number, lawn_plan_keyword, total_plans, plan_index, searched_folders=searched_folders)

        if not found_result:
            failed.append(original_order_number)
            if len(searched_folders) > 1:
                searched_folders_str = ' or '.join(searched_folders)
                print(f"(Log for #{original_order_number}) Error, ending search: No file found for order #{original_order_number} in {searched_folders_str} folders", flush=True)
            else:
                print(f"(Log for #{original_order_number}) Error, ending search: No file found for order #{original_order_number} in 'Sent' folder, and no indication file is in the 'First' or 'Recurring' folders", flush=True)

    return found_result


def print_file(file_path, printer_service_api_key, printer_id, original_order_number):
    print(f"(Log for #{original_order_number}) Printing file for order #{original_order_number}", flush=True)
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
        'printerId': printer_id,
        'title': f'Lawn Plan from batch {print_batch} started at time: {start_time}',
        'contentType': 'pdf_base64',
        'content': encoded_content,
        'source': 'ShipStation Lawn Plan'
    }

    response = session.post('https://api.printnode.com/printjobs', headers=headers, json=print_job)

    if response.status_code == 201:
        print(f"(Log for #{original_order_number}) Print job submitted successfully for order #{original_order_number}", flush=True)
        file_move_queue.put((file_path, original_order_number))
    if response.status_code == 400:
        file_move_queue.put((file_path, original_order_number))
        print(f"(Log for #{original_order_number}) Error submitting print job for order #{original_order_number}: {response.json()['message']}", flush=True)