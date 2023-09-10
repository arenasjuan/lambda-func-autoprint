import os
import json
import requests
import base64
import dropbox
import re
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import config
from queue import Queue
from threading import Thread, Lock
from dateutil import tz
import re
from io import BytesIO
import shutil
from PyPDF2 import PdfMerger, PdfFileReader
from requests.auth import HTTPBasicAuth

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

    response = requests.post(token_url, data=data)

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

# Define the shared dictionary and lock
print_counter_dict = {}
print_counter_lock = Lock()

# Get Pacific timezone object
tz_us_pacific = tz.gettz('US/Pacific')

# Get the current datetime in local timezone
start_time = datetime.now(tz_us_pacific)

file_move_queue = Queue()

failed = []

barcode_list = []

pdf_paths = []

database_entries = []

shipments_for_database = []

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

    batch_number = data['shipments'][0].get('batchNumber')

    mlp_shipments = []
    stk_shipments = []
    for shipment in data['shipments']:

        if not shipment_age_check(shipment):
            print(f"(Log for #{shipment['orderNumber']}) Shipment too old, can't process batch; ending execution", flush=True)
            return

        shipments_for_database.append(shipment)
        if shipment['shipmentItems'] is not None:
            has_lawn_plan = any(any(plan_sku in item['sku'] for plan_sku in config.lawn_plan_skus) for item in shipment['shipmentItems'])
            has_stk = any('STK' in item['sku'] or 'LYL' in item['sku'] for item in shipment['shipmentItems'])
            if has_lawn_plan:
                mlp_shipments.append(shipment)
            if has_stk:
                stk_shipments.append(shipment)
    print(f"Number of shipments: {len(data['shipments'])}")
    print(f"Number of shipments with lawn plans: {len(mlp_shipments)}")
    print(f"Number of shipments with STKs: {len(stk_shipments)}")

    if len(mlp_shipments) == 1:
        mlp_order_number = mlp_shipments[0].get('orderNumber')

    if len(stk_shipments) == 1:
        stk_order_number = stk_shipments[0].get('orderNumber')


    # Print the order numbers before sorting
    print("MLP order numbers before sorting:", [shipment["orderNumber"] for shipment in mlp_shipments])
    print("STK order numbers before sorting:", [shipment["orderNumber"] for shipment in stk_shipments])

    # Sort the filtered shipments by order number
    sorted_mlp_shipments = sorted(mlp_shipments, key=lambda x: int(x['orderNumber'].split('-')[0]))
    sorted_stk_shipments = sorted(stk_shipments, key=lambda x: int(x['orderNumber'].split('-')[0]))

    # Print the order numbers after sorting
    print("MLP order numbers after sorting:", [shipment["orderNumber"] for shipment in sorted_mlp_shipments])
    print("STK order numbers after sorting:", [shipment["orderNumber"] for shipment in sorted_stk_shipments])

    worker_thread = Thread(target=move_file_worker)
    worker_thread.start()

    # Process mlp_shipments in one thread
    mlp_thread = Thread(target=lambda: [process_order(shipment) for shipment in sorted_mlp_shipments])
    mlp_thread.start()

    current_date_folder = f"{config.batch_record_path}/{start_time.strftime('%m.%d.%Y')}"
    ensure_folder_exists(current_date_folder)

    barcode_filename = None
    barcode_destination = None
    mlp_destination = None

    # Determine the filename
    if stk_shipments:
        if batch_number:
            batch_folder = f"{current_date_folder}/Batches/Batch #{batch_number}"
            ensure_folder_exists(f"{current_date_folder}/Batches")
            ensure_folder_exists(batch_folder)
            barcode_filename = f"barcodes_batch{batch_number}_{start_time.strftime('%m-%d-%Y_%H:%M')}.pdf"
            barcode_destination = f"{batch_folder}/{barcode_filename}"
        else:
            order_folder = f"{current_date_folder}/Single Orders/Order #{stk_order_number}"
            ensure_folder_exists(f"{current_date_folder}/Single Orders")
            ensure_folder_exists(order_folder)
            barcode_filename = f"barcode_order{stk_order_number}_{start_time.strftime('%m-%d-%Y_%H:%M')}.pdf"
            barcode_destination = f"{order_folder}/{barcode_filename}"

    # Determine the MLP PDF destination
    if mlp_shipments:
        if batch_number:
            mlp_filename = f"plans_batch{batch_number}_{start_time.strftime('%m-%d-%Y_%H:%M')}.pdf"
            mlp_destination = f"{current_date_folder}/Batches/Batch #{batch_number}/{mlp_filename}"
        else:
            mlp_filename = f"plan_order{mlp_order_number}_{start_time.strftime('%m-%d-%Y_%H:%M')}.pdf"
            mlp_destination = f"{current_date_folder}/Single Orders/Order #{mlp_order_number}/{mlp_filename}"

    # Process stk_shipments in another thread
    stk_thread = Thread(target=lambda: [print_barcode(shipment["orderNumber"]) for shipment in sorted_stk_shipments])
    stk_thread.start()

    # Wait for both threads to complete
    mlp_thread.join()
    stk_thread.join()

    if barcode_filename:
        # Join the ZPL strings in barcode_list with newline characters
        zpl_string = "".join(barcode_list)

        # Convert ZPL string to PDF
        url = 'http://api.labelary.com/v1/printers/12dpmm/labels/2.25x1/'
        zpl_headers = {'Accept': 'application/pdf'}
        response = session.post(url, headers=zpl_headers, files={'file': zpl_string}, stream=True)

        # Check response status
        if response.status_code == 200:
            temp_file_path = f"/tmp/{barcode_filename}"
            response.raw.decode_content = True
            with open(temp_file_path, 'wb') as f:
                shutil.copyfileobj(response.raw, f)

            # Upload the PDF file to Dropbox
            with open(temp_file_path, 'rb') as f:
                dbx.files_upload(f.read(), barcode_destination, mode=dropbox.files.WriteMode('overwrite'))

    if pdf_paths:
        merger = PdfMerger()
        for pdf_path in pdf_paths:
            merger.append(pdf_path)

        output_filename = f"/tmp/{mlp_filename}"
        merger.write(output_filename)
        merger.close()

        with open(output_filename, 'rb') as f:
            dbx.files_upload(f.read(), mlp_destination, mode=dropbox.files.WriteMode('overwrite'))

    # Wait for the worker thread to finish moving all files
    file_move_queue.put(None)
    worker_thread.join()

    # create a ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        # use executor.map to apply the function to all shipments in parallel
        executor.map(prepare_database_entry, shipments_for_database)

    url = "https://c3qu31k2tf.execute-api.us-west-1.amazonaws.com/Prod/webhook"
    headers = {'Content-Type': 'application/json'}

    # Send database_entries to the API
    requests.post(url, headers=headers, data=json.dumps({"database_entries": database_entries}))

    print("Shipment info sent to tracking notifier")
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

def shipment_age_check(order):
    shipment_create_time_str = order['createDate']
    time_diff_minutes = calculate_time_difference(shipment_create_time_str, start_time)

    return False if time_diff_minutes > 15 else True


def prepare_database_entry(order):
    # Define carrier name
    carrier_name = None
    if order['carrierCode'].startswith('ups'):
        carrier_name = 'UPS'
    elif order['carrierCode'] == 'fedex':
        carrier_name = 'FedEx'
    else:
        carrier_name = "USPS"

    # Define shipped date
    shipped_date = datetime.now().strftime("%Y%m%d")

    # Build row data
    row_data = {
        "OrderNumber": order['orderNumber'],
        "CustomerName": order['shipTo']['name'],
        "CustomerEmail": order['customerEmail'],
        "TrackingNumber": order['trackingNumber'],
        "CarrierName": carrier_name,
        "ShippedDate": shipped_date,
        "StatusCode": None,
        "LastLocation": 'Culver City',
        "DaysAtLastLocation": 0,
        "NotificationSent": 'No',
        "Delayed": 'No',
        "Delivered": 'No'
    }

    # Define a dictionary to store product counts
    product_counts = config.products.copy()

    for item in order['shipmentItems']:
        # Check if the item is a lawn plan
        if any(plan_sku in item['sku'] for plan_sku in config.lawn_plan_skus):
            # Process lawn plan items
            matches = re.findall(r'• (\d+) ([^\n]+)', item['name'])
            for match in matches:
                quantity, product_name = int(match[0]), match[1].strip()
                if product_name in config.plan_products:
                    product_counts[config.plan_products[product_name]] += quantity
        elif item['sku'] in config.skus:
            sku_entry = config.skus[item['sku']]
            if isinstance(sku_entry, dict):
                for product, quantity in sku_entry.items():
                    product_counts[product] += quantity * item['quantity']
            else:
                product_counts[sku_entry] += item['quantity']

    # Append product counts to row_data
    row_data.update(product_counts)

    # Append row_data to the global database_entries list
    database_entries.append(row_data)


def ensure_folder_exists(path):
    try:
        dbx.files_get_metadata(path)
    except dropbox.exceptions.ApiError as e:
        if isinstance(e.error.get_path(), dropbox.files.LookupError):
            dbx.files_create_folder(path)



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
    try:
        data = response.json()
        if data["orders"]:
            return next(o for o in data['orders'] if o['orderId'] == order['orderId'])
        else:
            failed_orders.append(order['orderNumber'])
            print(f"(Log for #{order['orderNumber']}) No orders found for order #{order['orderNumber']}", flush=True)
    except ValueError:  # includes simplejson.decoder.JSONDecodeError
        print(f"Error decoding JSON from response for order #{order['orderNumber']}", flush=True)
    except Exception as e:
        print(f"An unexpected error occurred for order #{order['orderNumber']}: {str(e)}", flush=True)
    return None


def print_barcode(order_number, printer_service_api_key=config.PRINTER_SERVICE_API_KEY):
    if "-" in order_number:
        order_number = order_number.split("-")[0]

    start_position_x = 180
    start_position_y = 80

    zpl_code = f"^XA^FO{start_position_x},{start_position_y}^BY3^BCN,100,Y,N,N^FD{order_number}^XZ"
    barcode_list.append(zpl_code)

    # Create print job
    print_job = {
        'printerId': config.BARCODE_ID,
        'title': f'Barcode for Order #{order_number}',
        'contentType': 'raw_base64',
        'content': base64.b64encode(zpl_code.encode()).decode(),
        'source': 'ShipStation Barcode'
    }

    # Print
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {base64.b64encode(printer_service_api_key.encode()).decode()}'
    }
    response = session.post('https://api.printnode.com/printjobs', headers=headers, json=print_job)

    # Log
    if response.status_code == 201:
        print(f"(Log for #{order_number}) Print job submitted successfully for order #{order_number}", flush=True)
    elif response.status_code == 400:
        print(f"(Log for #{order_number}) Error submitting print job for order #{order_number}: {response.json()['message']}", flush=True)



def process_order(order):
    original_order_number = order["orderNumber"]

    order_items = order['shipmentItems']

    searchable_order_number = original_order_number

    if "-" in searchable_order_number:
        searchable_order_number = searchable_order_number.split("-")[0]

    folder_to_search = None

    if order['advancedOptions'].get('storeId') == 310067:
        folder_to_search = config.sent_folder_path
    else:
        order_with_tags = get_order_by_number(order)
        if not order_with_tags:
            print(f"(Log for #{original_order_number}) Error: Can't find order info for Order #{original_order_number}; checking 'Sent' folder by default", flush=True)
            folder_to_search = config.sent_folder_path
        else:
            tags = order_with_tags.get('tagIds', None)
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
        if 'Hero' in item['name']:
            lawn_plan_keywords.append('Hero')
        else:
            matching_key = get_matching_mlp_key(item['sku'])
            if matching_key is not None:
                lawn_plan_keyword = config.mlp_dict[matching_key]
                lawn_plan_keywords.append(lawn_plan_keyword)
            else:
                print(f"Log for #{original_order_number}: Error: Couldn't find matching key for item '{item['name']}'")

    print(f"(Log for #{original_order_number}) Preparing search for order #{original_order_number} with keyword(s) {lawn_plan_keywords}", flush=True)

    # Initialize print counter for current order
    with print_counter_lock:
        print_counter_dict[original_order_number] = 0

    with ThreadPoolExecutor(max_workers=len(lawn_plan_keywords)) as executor:
        futures = [executor.submit(search_and_print, original_order_number, searchable_order_number, lawn_plan_keyword, folder_to_search, len(lawn_plan_keywords), i + 1) for i, lawn_plan_keyword in enumerate(lawn_plan_keywords)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"(Log for #{original_order_number}) Error in thread: {e}", flush=True)


def search_and_print(original_order_number, searchable_order_number, lawn_plan_keyword, folder, total_plans, plan_index):
    folder_name = "'First'" if folder == config.folder_1 else "'Recurring'" if folder == config.folder_2 else "'Sent'"
    print(f"(Log for #{original_order_number}) Searching for lawn plan for order #{original_order_number} with keyword \'{lawn_plan_keyword}\' in {folder_name} folder", flush=True)

    try:
        printer_api_key = config.PRINTER_SERVICE_API_KEY
        printer_id = config.PRINTER_ID
        found_results = search_folder(folder, folder_name, searchable_order_number, original_order_number, lawn_plan_keyword, total_plans, plan_index)
        for local_file_path, file_path in found_results:
            with print_counter_lock:
                print_counter_dict[original_order_number] += 1
            print_file(file_path, printer_api_key, printer_id, original_order_number)
            pdf_paths.append(local_file_path)
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

    searched_folders.append(f"'{folder_name}'")

    if search_results.matches:
        for match in search_results.matches:
            filename = match.metadata.name
            if lawn_plan_keyword in filename:
                file_path = match.metadata.path_display
                local_file_path = os.path.join('/tmp', filename)
                dbx.files_download_to_file(local_file_path, file_path)
                found_result.append((local_file_path, file_path))
                file_found = True
                print(f"(Log for #{original_order_number}) Order #{original_order_number} (Plan {plan_index} of {total_plans}): found {file_path} in '{folder_name}' folder", flush=True)

    if not file_found:
        # Search in the Sent folder if it hasn't been searched yet
        if "'Sent'" not in searched_folders:
            print(f"(Log for #{original_order_number}) No file found for order number {original_order_number} with lawn plan keyword \'{lawn_plan_keyword}\' in '{folder_name}'; now searching in 'Sent' folder", flush=True)
            found_result = search_folder(config.sent_folder_path, 'Sent', searchable_order_number, original_order_number, lawn_plan_keyword, total_plans, plan_index, searched_folders=searched_folders)
        # If 'Sent' has been searched, but 'First' hasn't, search in 'First' folder
        elif "'First'" not in searched_folders:
            print(f"(Log for #{original_order_number}) No file found for order number {original_order_number} with lawn plan keyword \'{lawn_plan_keyword}\' in '{folder_name}'; now searching in 'First' folder", flush=True)
            found_result = search_folder(config.folder_1, 'First', searchable_order_number, original_order_number, lawn_plan_keyword, total_plans, plan_index, searched_folders=searched_folders)
        # If both 'Sent' and 'First' have been searched but 'Recurring' hasn't, search in 'Recurring' folder
        elif "'Recurring'" not in searched_folders:
            print(f"(Log for #{original_order_number}) No file found for order number {original_order_number} with lawn plan keyword \'{lawn_plan_keyword}\' in '{folder_name}'; now searching in 'Recurring' folder", flush=True)
            found_result = search_folder(config.folder_2, 'Recurring', searchable_order_number, original_order_number, lawn_plan_keyword, total_plans, plan_index, searched_folders=searched_folders)
        # If all folders have been searched and no result is found, log an error message
        if not found_result:
            failed.append(original_order_number)
            searched_folders_str = ', '.join(searched_folders)
            print(f"(Log for #{original_order_number}) Error, ending search: No file found for order #{original_order_number} in {searched_folders_str} folders", flush=True)

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
        'title': f'Lawn Plan for Order #{original_order_number} started at time: {start_time}',
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