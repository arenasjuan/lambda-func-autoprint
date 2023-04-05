import os
import json
import requests
import base64
import dropbox
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import uuid
import config
from queue import Queue
from dateutil import tz
from dateutil.parser import parse
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


printed_files=[]

print_batch = str(uuid.uuid4())



# Get Pacific timezone object
tz_us_pacific = tz.gettz('US/Pacific')

# Get the current datetime in local timezone
start_time = datetime.now(tz_us_pacific)



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

    # Check the createDate value of the first shipment, which says when the shipment was created
    first_shipment = shipments[0]
    shipment_create_time_str = first_shipment['createDate']

    time_diff_minutes = calculate_time_difference(shipment_create_time_str, start_time)

    # If shipment was created before Autoprint's timeout value, reject it
    if time_diff_minutes > 15:
        return "Shipment is from too long ago, can't process"

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_order, shipment) for shipment in shipments]
        for future in as_completed(futures):
            future.result()

    # Move printed files to MLPs Sent
    for printed_file in printed_files:
        move_file_to_sent_folder(printed_file)


def calculate_time_difference(shipment_create_time_str, current_time):
    # Extract date and time components from the shipment_create_time_str
    try:
        year1, month1, day1, hour1, minute1, second1, microsecond1 = [
            int(x) for x in re.findall(r'\d+', shipment_create_time_str.replace("T", " "))[:7]
        ]
    except ValueError:
        print(f"Error: Unable to parse shipment create time: {shipment_create_time_str}")
        return None

    # Extract date and time components from the current_time datetime object
    year2, month2, day2, hour2, minute2, second2, microsecond2 = current_time.year, current_time.month, current_time.day, current_time.hour, current_time.minute, current_time.second, current_time.microsecond

    # Calculate the time difference in seconds
    total_seconds1 = (day1 * 86400) + (hour1 * 3600) + (minute1 * 60) + second1
    total_seconds2 = (day2 * 86400) + (hour2 * 3600) + (minute2 * 60) + second2
    time_diff_seconds = total_seconds2 - total_seconds1
    time_diff_minutes = time_diff_seconds // 60

    return time_diff_minutes



def process_order(order):
    # Check if the order contains a lawn plan
    order_number = order["orderNumber"]
    print(f"Checking order number: {order_number}")
    order_items = order['shipmentItems']
    folders_to_search = [config.folder_1, config.folder_2]
    workers = 2
    if order['advancedOptions']['storeId'] == 310067:
        folders_to_search = [config.sent_folder_path]
        workers = 1 

    for item in order_items:
        if (item['sku'].startswith('SUB') or item['sku'] in ['05000', '10000', '15000']) and item['sku'] not in ["SUB - LG - D", "SUB - LG - S", "SUB - LG - G"]:
            print(f"Lawn plan found in order number {order_number}")
            return search_and_print(order_number, folders_to_search, workers)
    
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

def search_and_print(order_number, folders, threads):
    print(f"Searching for PDF file for order number {order_number}")
    try:

        found_result = Queue()

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(search_folder, folder, order_number, found_result) for folder in folders]
            for future in as_completed(futures):
                result = future.result()
                if result and found_result.empty():
                    found_result.put(result)
                    local_file_path, file_path = result
                    printed_files.append(file_path)
                    print_file(local_file_path, config.PRINTER_SERVICE_API_KEY, order_number)
                    return

    except Exception as e:
        print(f"Error: {e}")



def search_folder(folder, order_number, found_result):
    print(f"Searching in folder: {folder}")

    if not found_result.empty():
        return

    folder_contents = get_folder_contents(folder)

    # Search for the file in the Dropbox folder
    search_results = dbx.files_search(folder, f"*{order_number}.pdf")

    if search_results.matches:
        print(f"Found {len(search_results.matches)} match(es) for order number {order_number}")
        for match in search_results.matches:
            filename = match.metadata.name
            if filename.endswith(f"{order_number}.pdf"):
                # Download and save the file
                file_path = match.metadata.path_display
                local_file_path = os.path.join('/tmp', filename)  # Save the file to the /tmp folder in Lambda
                dbx.files_download_to_file(local_file_path, file_path)
                print(f"File found for order number {order_number} in folder {folder}")
                print(f"File downloaded and saved to {local_file_path}")

                if found_result.empty():
                    return local_file_path, file_path
    else:
        print(f"No file found for order number {order_number} in {folder}")




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
        'printerId': 72185140,
        # Microsoft Print to PDF ID, for testing:
        #'printerId':72123119,
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
        print(f"Error submitting print job for order number {order_number}: {response.json()['message']}")
