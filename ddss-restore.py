import os
import sys
import json
import time
import requests
import shutil
import socket
import boto3
import subprocess
import urllib3
import hashlib
from concurrent.futures import ThreadPoolExecutor
import subprocess
urllib3.disable_warnings()

# Configuration
BUCKET_JSON = "bucket_structure.json"
SPLUNK_URL = os.getenv("SPLUNK_URL") or "https://localhost:8089"  # Update with your Splunk server URL
AUTH = (os.getenv("SPLUNK_USERNAME"), os.getenv("SPLUNK_PASSWORD"))  # Replace with Splunk credentials
DDSS_BUCKET_NAME = os.getenv("DDSS_BUCKET_NAME")
DDSS_PATH_NAME = os.getenv("DDSS_PATH_NAME")
S2_BUCKET_NAME = os.getenv("S2_BUCKET_NAME")
S2_PATH_NAME = os.getenv("S2_PATH_NAME")
#LOCAL_BASE_PATH = "/opt/splunk/var/lib/splunk"
LOCAL_BASE_PATH = os.getenv("LOCAL_BASE_PATH") or "/splunkdata/indexes/"
PROCESS_BUCKET_SCRIPT = "./process_bucket.sh"
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH") or "/opt/splunk/var/log/splunk/splunkd.log"  # Path to your Splunk log file
MAX_WORKERS = int(os.getenv("MAX_WORKERS") or 10)

CACHEMANAGER_JSON_CONTENT = {
    "file_types": ["strings_data", "sourcetypes_data", "sources_data", "hosts_data", "bucket_info", "bfidx", "tsidx", "bloomfilter", "journal_gz", "deletes"]
}
s3 = boto3.client("s3")


# Utility Functions
def wait_for_logs(timeout):
    """
    Tail the Splunk log file and wait for a specific log message indicating Splunk is back online.

    :param timeout: Maximum time to wait (in seconds)
    :return: True if the log message is detected, False otherwise
    """
    import os

    start_time = time.time()
#    expected_message = 'INFO  ServerConfig [0 MainThread] - My server name is'
    expected_message = 'My server name is'

    print(f"Waiting for log message: {expected_message}")

    try:
        # Open the log file in read mode and seek to the end
        with open(LOG_FILE_PATH, 'r') as log_file:
            log_file.seek(0, os.SEEK_END)

            while time.time() - start_time < timeout:
                # Read new lines from the log file
                line = log_file.readline()
                if not line:  # If no new line, wait and retry
                    time.sleep(1)
                    continue

                if expected_message in line:
                    print(f"Detected log message: {line.strip()}")
                    return True

    except FileNotFoundError:
        print(f"Log file not found: {LOG_FILE_PATH}")
    except Exception as e:
        print(f"\033[31mError reading log file: {e}\033[0m")

    print("Timeout reached without detecting the log message.")
    return False

def wait_for_port(host, port, timeout):
    """
    Wait for a port to become available.

    :param host: The host to check (e.g., "localhost")
    :param port: The port to check
    :param timeout: Maximum time to wait (in seconds)
    :return: True if the port is available, False otherwise
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=5):
                return True
        except (socket.timeout, ConnectionRefusedError):
            time.sleep(5)
    return False

def restart_splunk():
    """
    Restart Splunk using the REST API.

    :param auth: A tuple containing (username, password) for authentication
    """
    restart_endpoint = f"{SPLUNK_URL}/services/server/control/restart"

    print("Restarting Splunk...")
    try:
        response = requests.post(
            restart_endpoint,
            auth=AUTH, #HTTPBasicAuth(*auth),
            verify=False  # Disable SSL verification if using self-signed certificates
        )

        if response.status_code == 200:
            print("Splunk restarted successfully.")
        else:
            print(f"\033[31mError restarting Splunk: {response.status_code} - {response.text}\033[0m")
        timeout=300
        if wait_for_logs(timeout):
            print("Log message detected. Verifying port availability...")
            if wait_for_port("localhost", 8089, timeout):
                print("Splunk is back online and responding on port 8089.")
                return True
            else:
                print("\033[31mError: Port 8089 is not responding.\033[0m")
        else:
            print("\033[31mError: Log message not detected within timeout.\033[0m")

    except requests.RequestException as e:
        print(f"\033[31mError making API request: {e}\033[0m")


def splunk_api_call(endpoint, data=None, method="POST"):
    url = f"{SPLUNK_URL}{endpoint}"
    response = requests.request(method, url, auth=AUTH, data=data, verify=False)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"\033[31mError with Splunk API call to {url}: {response.status_code} - {response.text}\033[0m")
        return None


def calculate_sha(bucket_num, server_guid):
    """
    Calculate the SHA1 hash for the given bucketNum and serverGUID.

    Args:
        bucket_num (str): The bucket number.
        server_guid (str): The server GUID.

    Returns:
        str: The calculated SHA1 hash.
    """
    sha_input = f"{bucket_num}~{server_guid}"
    sha1_hash = hashlib.sha1(sha_input.encode()).hexdigest()
    return sha1_hash


def check_receipt_on_s3(index_name, bucket_num, server_guid):
    """
    Check if the receipt.json file exists on S3 for the given bucket.

    Args:
        index_name (str): The index name.
        bucket_num (str): The bucket number.
        server_guid (str): The server GUID.

    Returns:
        bool: True if the receipt.json file exists, False otherwise.
    """
    # Calculate SHA1 and construct S3 key
    sha1_hash = calculate_sha(bucket_num, server_guid)
    sha_part1 = sha1_hash[:2]
    sha_part2 = sha1_hash[2:4]
    s3_key = f"{S2_PATH_NAME}{index_name}/db/{sha_part1}/{sha_part2}/{bucket_num}~{server_guid}/receipt.json"
    try:
        s3.head_object(Bucket=S2_BUCKET_NAME, Key=s3_key)
        # print(f"Found receipt.json on S3: {s3_key}")
        return True
    except s3.exceptions.ClientError:
        # print(f"Missing receipt.json on S3: {s3_key}")
        return False


def update_json_file(json_path, bucket_name, status):
    with open(json_path, "r") as file:
        data = json.load(file)
    for index_name, buckets in data.items():
        for bucket in buckets:
            if bucket["bucket"] == bucket_name:
                bucket["status"] = status
    with open(json_path, "w") as file:
        json.dump(data, file, indent=4)

def update_multiple_status(json_path, updates):
    index_updates = {}
    for update in updates:
        # index_name, bucket_name, status
        if not update['index_name'] in index_updates:
            index_updates[update['index_name']] = {}
        index_updates[update['index_name']][update['bucket']] = update['status']

    with open(json_path, "r") as file:
        data = json.load(file)
    for index_name, buckets in data.items():
        if index_name in index_updates:
            for bucket in buckets:
                if bucket["bucket"] in index_updates[index_name]:
                    bucket["status"] = index_updates[index_name][bucket['bucket']]
    with open(json_path, "w") as file:
        json.dump(data, file, indent=4)

def check_local_status(index_name, bucket_name):
    """
    Check if Hosts.data exists locally for the given bucket.

    Args:
        index_name (str): The index name.
        bucket_name (str): The bucket name.

    Returns:
        bool: True if Hosts.data exists, False otherwise.
    """
    local_path = os.path.join(LOCAL_BASE_PATH, index_name, "db", bucket_name, "Hosts.data")
    return os.path.exists(local_path)

def load_s2_index_structure(index_name):
    """
    Load the structure of the S3 bucket for a given index into memory.

    Args:
        index_name (str): The index name.

    Returns:
        set: A set containing keys for all receipt.json files.
    """
    receipt_keys = set()
    prefix = f"{S2_PATH_NAME}{index_name}/db/"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S2_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("receipt.json"):
                receipt_keys.add(obj["Key"])
    return receipt_keys

def check_receipt_in_structure(receipt_keys, index_name, bucket_num, server_guid):
    """
    Check if the receipt.json file exists in the preloaded S3 structure.

    Args:
        receipt_keys (set): A set of preloaded S3 keys.
        index_name (str): The index name.
        bucket_num (str): The bucket number.
        server_guid (str): The server GUID.

    Returns:
        bool: True if the receipt.json file exists, False otherwise.
    """
    # Calculate SHA1 and construct S3 key
    sha1_hash = calculate_sha(bucket_num, server_guid)
    sha_part1 = sha1_hash[:2]
    sha_part2 = sha1_hash[2:4]
    s3_key = f"{S2_PATH_NAME}{index_name}/db/{sha_part1}/{sha_part2}/{bucket_num}~{server_guid}/receipt.json"

    return s3_key in receipt_keys

def generate_bucket_structure(bucket_name, prefix=""):
    """
    Generate a JSON structure with indexes and their corresponding buckets from an S3 bucket.
    Checks local files and a secondary S3 bucket for the receipt.json file.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix for filtering objects in the bucket.

    Returns:
        dict: Dictionary with indexes as keys and list of buckets as values.
    """
    paginator = s3.get_paginator("list_objects_v2")
    result = {}

    # Paginate through S3 objects for indexes
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        if "CommonPrefixes" in page:
            for index in page["CommonPrefixes"]:
                index_name = index["Prefix"].rstrip("/").split("/")[-1]
                sub_prefix = index["Prefix"]
                result[index_name.split("/")[-1]] = []
                #Get S2 listing here
                s2_index_files = load_s2_index_structure(index_name)
                # Paginate through S3 objects for buckets under each index
                sub_paginator = s3.get_paginator("list_objects_v2")
                for sub_page in sub_paginator.paginate(Bucket=bucket_name, Prefix=sub_prefix, Delimiter="/"):
                    for bucket_info in sub_page.get("CommonPrefixes", []):
                        splunk_bucket_name = bucket_info["Prefix"].split("/")[-2]
                        bucket_parts = splunk_bucket_name.split("_")
                        bucket_num = bucket_parts[3]
                        server_guid = bucket_parts[4]

                        # Determine initial status
#                        receipt_exists = check_receipt_on_s3(index_name, bucket_num, server_guid)
                        receipt_exists = check_receipt_in_structure(s2_index_files, index_name, bucket_num, server_guid)
                        hosts_data_exists = check_local_status(index_name, splunk_bucket_name)

                        if receipt_exists:
                            if hosts_data_exists:
                                status = "pendingevict"
                            else:
                                status = "done"
                        else:
                            if hosts_data_exists:
                                status = "pendingupload"
                            else:
                                status = "todo"

                        result[index_name.split("/")[-1]].append(
                            {"bucket": splunk_bucket_name, "status": status}
                        )

    # Save result to file
    with open(BUCKET_JSON, "w") as file:
        json.dump(result, file, indent=4)
    print("Bucket structure saved to bucket_structure.json")


def load_bucket_structure(json_file):
    """Load bucket structure from the JSON file."""
    with open(json_file, "r") as file:
        return json.load(file)


def save_bucket_structure(json_file, bucket_data):
    """Save updated bucket structure back to the JSON file."""
    with open(json_file, "w") as file:
        json.dump(bucket_data, file, indent=4)

def process_bucket(bucket_info, index_name, bucket_data, bucket_num):
    """
    Process a single bucket.

    Args:
        bucket_info (dict): Information about the bucket to process.
        index_name (str): The index name.
        bucket_data (dict): The entire bucket structure JSON data.
        bucket_num (int): Current bucket processing number.
    """
    bucket_name = bucket_info["bucket"]

    # Update status to "inprogress"
    bucket_info["status"] = "inprogress"
    #save_bucket_structure(BUCKET_JSON, bucket_data)

    # Call process_bucket.sh
    try:
        print(f"\033[92m[{bucket_num}]\033[00m Processing bucket: {bucket_name} for index: {index_name}")
        subprocess.run([PROCESS_BUCKET_SCRIPT, bucket_name, index_name], check=True)
        # Update status to "pendingupload" after successful processing
        bucket_info["status"] = "pendingupload"
        print(f"\033[92m[{bucket_num}]\033[00m \033[94mThawed bucket\033[00m: {bucket_name} for index: {index_name}")

    except subprocess.CalledProcessError as e:
        print(f"\033[31mError processing bucket {bucket_name}: {e}\033[0m")
        # Update status back to "todo" in case of an error
        bucket_info["status"] = "todo"

    update_cachemanager_file(index_name, bucket_name)
    bucket_info['index_name'] = index_name
    return bucket_info
    # Save the updated JSON after processing
    #save_bucket_structure(BUCKET_JSON, bucket_data)


def process_buckets(index_name, num_buckets):
    """
    Process N buckets from the specified index using concurrent processing.

    Args:
        index_name (str): The index name to process.
        num_buckets (int): The number of buckets to process.
    """
    # Load the bucket structure
    bucket_data = load_bucket_structure(BUCKET_JSON)

    # Ensure the index exists in the JSON
    if index_name not in bucket_data:
        print(f"Index '{index_name}' not found in {BUCKET_JSON}.")
        return

    # Filter buckets with status "todo"
    buckets_to_process = [bucket for bucket in bucket_data[index_name] if bucket["status"] == "todo"]
    if not buckets_to_process:
        print(f"No buckets to process for {index_name}")
        sys.exit(10)
    # Process up to N buckets concurrently
    proc_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_bucket, bucket_info, index_name, bucket_data, idx + 1)
            for idx, bucket_info in enumerate(buckets_to_process[:num_buckets])
        ]
        # Wait for all tasks to complete
        for future in futures:
            proc_results.append(future.result())
    update_multiple_status(BUCKET_JSON, proc_results)

def cacheman_bucket(index_name, bucket_num, server_guid):
    """
    Initializes the bucket in SmartStore using Splunk's REST API.

    Args:
        index_name (str): The name of the index.
        bucket_num (str): The bucket number.
        server_guid (str): The server GUID.

    Returns:
        bool: True if the request was successful, False otherwise.
    """
    # Construct the BID and API URL
    bid = f"{index_name}~{bucket_num}~{server_guid}"
    url = f"{SPLUNK_URL}/services/admin/cacheman/bid|{bid}|"

    # Make the POST request
    response = requests.post(url, auth=AUTH, data={"sid": bid}, verify=False)

    if response.status_code == 200:
        print(f"Successfully initialized bucket in cacheman: {bid}")
        return True
    else:
        print(f"Failed to initialize bucket in cacheman: {bid}. Response: {response.status_code} - {response.text}")
        return False


def attach_bucket(index_name, bucket_num, server_guid):
    """
    Attaches the specified bucket using Splunk's REST API.

    Args:
        index_name (str): The name of the index.
        bucket_num (str): The bucket number.
        server_guid (str): The server GUID.

    Returns:
        bool: True if the request was successful, False otherwise.
    """
    # Construct the BID and API URL
    bid = f"{index_name}~{bucket_num}~{server_guid}"
    url = f"{SPLUNK_URL}/services/admin/cacheman/bid|{bid}|/attach"

    # Make the POST request
    response = requests.post(url, auth=AUTH, data={"sid": bid, "directory": ""}, verify=False)

    if response.status_code == 200:
        print(f"Successfully attached bucket: {bid}")
        return True
    else:
        print(f"Failed to attach bucket: {bid}. Response: {response.status_code} - {response.text}")
        return False


def close_bucket(index_name, bucket_num, server_guid):
    """
    Closes the specified bucket using Splunk's REST API.

    Args:
        index_name (str): The name of the index.
        bucket_num (str): The bucket number.
        server_guid (str): The server GUID.

    Returns:
        bool: True if the request was successful, False otherwise.
    """
    # Construct the BID and API URL
    bid = f"{index_name}~{bucket_num}~{server_guid}"
    url = f"{SPLUNK_URL}/services/admin/cacheman/bid|{bid}|/close"

    # Make the POST request
    response = requests.post(url, auth=AUTH, data={"sid": bid}, verify=False)

    if response.status_code == 200:
        print(f"Successfully closed bucket: {bid}")
        return True
    else:
        print(f"Failed to close bucket: {bid}. Response: {response.status_code} - {response.text}")
        return False


def upload_buckets():
    """
    Loops through the bucket_structure.json file and processes all buckets with "pendingupload" status.

    Args:
        BUCKET_JSON (str): Path to the bucket_structure.json file.
    """
    # Load the JSON file
    with open(BUCKET_JSON, "r") as file:
        bucket_data = json.load(file)

    # Track if we made changes to the JSON
    modified = False

    for index_name, buckets in bucket_data.items():
        for bucket_info in buckets:
            if bucket_info["status"] == "pendingupload":
                # Extract bucket details
                bucket_name = bucket_info["bucket"]
                bucket_parts = bucket_name.split("_")
                bucket_num = bucket_parts[3]
                server_guid = bucket_parts[4]

                # Initialize bucket in cacheman
                if cacheman_bucket(index_name, bucket_num, server_guid):
                    # Attach the bucket
                    if attach_bucket(index_name, bucket_num, server_guid):
                        # Close the bucket
                        if close_bucket(index_name, bucket_num, server_guid):
                            # Update the status to "uploaded"
                            bucket_info["status"] = "uploaded"
                            modified = True

    # Save updated JSON file if any changes were made
    if modified:
        with open(BUCKET_JSON, "w") as file:
            json.dump(bucket_data, file, indent=4)
        print(f"Updated {BUCKET_JSON} with uploaded statuses.")
    else:
        print("No buckets to upload.")


def get_bucket_status(bid):
    """
    Queries Splunk REST API to get the upload status and bucket status.

    Args:
        bid (str): The bucket identifier (BID).

    Returns:
        tuple: (upload_status, bucket_status)
    """
    url = f"{SPLUNK_URL}/services/admin/cacheman/"
    query = f'|rest /services/admin/cacheman/ | search title="bid|{bid}|" | table title cm:bucket.upload_status cm:bucket.status'

    response = requests.post(
        f"{SPLUNK_URL}/services/search/jobs",
        auth=AUTH,
        data={"search": query, "output_mode": "json", "exec_mode": "oneshot"},
        verify=False,  # Disable SSL verification for localhost; adjust as needed
    )
    if response.status_code == 200:
        results = response.json()["results"]
        for result in results:
            if result["title"] == f"bid|{bid}|":
                return result["cm:bucket.upload_status"], result["cm:bucket.status"]

    return None, None


def check_buckets():
    """
    Processes buckets with status "uploaded" in the JSON file.

    Args:
        None
    """
    # Load the JSON file
    with open(BUCKET_JSON, "r") as file:
        bucket_data = json.load(file)

    modified = False

    for index_name, buckets in bucket_data.items():
        for bucket_info in buckets:
            if bucket_info["status"] == "uploaded":
                bucket_name = bucket_info["bucket"]
                bucket_parts = bucket_name.split("_")
                bucket_num = bucket_parts[3]
                server_guid = bucket_parts[4]
                bid = f"{index_name}~{bucket_num}~{server_guid}"

                # Check upload status
                while True:
                    print(f"Checking bid={bid} for path=/opt/splunk/var/lib/splunk/{index_name}/db/{bucket_name}")
                    upload_status, bucket_status = get_bucket_status(bid)

                    if upload_status == "idle":  # and bucket_status == "remote":
                        print(f"Bucket upload complete: BID={bid}, upload_status={upload_status}, bucket_status={bucket_status}")
                        break
                    else:
                        print(f"Waiting for bucket upload: BID={bid}, upload_status={upload_status}, bucket_status={bucket_status}")
                        time.sleep(5)

                # Check receipt.json in S3
                if check_receipt_on_s3(index_name, bucket_num, server_guid):
                    bucket_info["status"] = "pendingevict"
                    modified = True

    # Save updated JSON file if any changes were made
    if modified:
        with open(BUCKET_JSON, "w") as file:
            json.dump(bucket_data, file, indent=4)
        print(f"Updated {BUCKET_JSON} with pendingevict statuses.")
    else:
        print("No buckets to update.")

def update_cachemanager_file(index_name, bucket_name):
    """
    Creates or updates cachemanager_local.json in the bucket directory.

    Args:
        index_name (str): The name of the index.
        bucket_name (str): The bucket name.

    Returns:
        None
    """
    bucket_path = os.path.join(LOCAL_BASE_PATH, index_name, "db", bucket_name)
    cachemanager_file_path = os.path.join(bucket_path, "cachemanager_local.json")

    # Ensure the bucket directory exists
    if not os.path.exists(bucket_path):
        print(f"Bucket directory does not exist: {bucket_path}")
        return

    # Write the JSON content to cachemanager_local.json
    try:
        with open(cachemanager_file_path, "w") as file:
            json.dump(CACHEMANAGER_JSON_CONTENT, file, indent=4)
        print(f"Successfully updated cachemanager_local.json: {cachemanager_file_path}")
    except Exception as e:
        print(f"Failed to update cachemanager_local.json: {cachemanager_file_path}. Error: {e}")


def evict_bucket(index_name, bucket_num, server_guid):
    """
    Evicts the specified bucket using Splunk's REST API.

    Args:
        index_name (str): The name of the index.
        bucket_num (str): The bucket number.
        server_guid (str): The server GUID.

    Returns:
        bool: True if the request was successful, False otherwise.
    """
    # Construct the BID and API URL
    bid = f"{index_name}~{bucket_num}~{server_guid}"
    url = f"{SPLUNK_URL}/services/admin/cacheman/bid|{bid}|/evict"

    # Make the POST request
    response = requests.post(
        url,
        auth=AUTH,
        data={"output_mode":"json"},
        verify=False
    )
    if response.status_code == 200:
        print(f"Successfully evicted bucket: {bid}")
        return True
    else:
        print(f"Failed to evict bucket: {bid}. Response: {response.status_code} - {response.text}")
        return False


def evict_buckets():
    """
    Loops through the bucket_structure.json file and evicts all buckets with "pendingevict" status.

    Args:
        None
    """
    # Load the JSON file
    with open(BUCKET_JSON, "r") as file:
        bucket_data = json.load(file)

    # Track if we made changes to the JSON
    modified = False

    for index_name, buckets in bucket_data.items():
        for bucket_info in buckets:
            if bucket_info["status"] == "pendingevict":
                # Extract bucket details
                bucket_name = bucket_info["bucket"]
                bucket_parts = bucket_name.split("_")
                bucket_num = bucket_parts[3]
                server_guid = bucket_parts[4]

                # Update local cachemanager file with contents of bucket so deleted correctly when evicted
                update_cachemanager_file(index_name, bucket_name)

                # Evict the bucket
                if evict_bucket(index_name, bucket_num, server_guid):
                    # Update the status to "done"
                    bucket_info["status"] = "done"
                    modified = True

    # Save updated JSON file if any changes were made
    if modified:
        with open(BUCKET_JSON, "w") as file:
            json.dump(bucket_data, file, indent=4)
        print(f"Updated {BUCKET_JSON} with evicted statuses.")
    else:
        print("No pending buckets to evict.")

def get_configured_indexes():
    """
    Retrieve a list of configured indexes on the Splunk instance.

    Returns:
        set: A set of index names configured in Splunk.
    """
    try:
        # Run the Splunk btool command to get the list of indexes
        command = ["/opt/splunk/bin/splunk", "btool", "indexes", "list"]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        # Parse the output to extract index names
        indexes = set()
        for line in result.stdout.splitlines():
            if line.startswith("[") and line.endswith("]"):
                index_name = line.strip("[]")
                indexes.add(index_name)
        return indexes
    except subprocess.CalledProcessError as e:
        print(f"Error running Splunk btool: {e}")
        return set()


def determine_index_for_processing(json_file_path):
    """
    Determine the first index in the JSON file with a bucket containing a "todo" status.

    Args:
        json_file_path (str): Path to the JSON file.

    Returns:
        str: The name of the first index with a "todo" status, or None if no such index exists.
    """
    try:
        # Load the JSON file
        with open(json_file_path, "r") as file:
            data = json.load(file)

        configured_indexes = get_configured_indexes()

        # Iterate through the indices and their buckets
        for index_name, buckets in data.items():
            if index_name in configured_indexes:
                for bucket in buckets:
                    if bucket.get("status") == "todo":
                        return index_name
        # Return None if no "todo" status is found
        return None

    except Exception as e:
        print(f"Error processing the JSON file: {e}")
        return None

def main():
    proc_start_time = time.time()
    print("Starting DDSS Restore Workflow...")
    generate_bucket_structure(DDSS_BUCKET_NAME, DDSS_PATH_NAME)
    proc_time_so_far = time.time()-proc_start_time
    print(f"Bucket Structure generation took seconds={proc_time_so_far}")
    # Get index name and number of buckets from environment variables or prompt for input
    index_name = os.getenv("INDEX_NAME") or determine_index_for_processing(BUCKET_JSON)
    print(f"Processing buckets for index={index_name}")
    num_buckets = int(os.getenv("NUM_BUCKETS") or input("Enter number of buckets to process: "))
    process_buckets(index_name, num_buckets)
    restart_splunk()
    upload_buckets()
    check_buckets()
    evict_buckets()
    print("Workflow complete.")
    proc_time_so_far = time.time()-proc_start_time
    print(f"Processing took seconds={proc_time_so_far}")
    print("\033[46mSleeping 30 seconds before next execution\033[0m")
    time.sleep(30)

if __name__ == "__main__":
    main()
