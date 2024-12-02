import os
import json
import time
import requests
import shutil
import boto3
import subprocess
import urllib3
import hashlib

urllib3.disable_warnings()

# Configuration
BUCKET_JSON = "bucket_structure.json"
SPLUNK_URL = "https://localhost:8089"  # Update with your Splunk server URL
AUTH = ("admin", os.getenv("SPLUNK_PASSWORD"))  # Replace with Splunk credentials
DDSS_BUCKET_NAME = "scde-3usvpx5d8elc6o712-d0hrpb07azl9-testing2"
S2_BUCKET_NAME = "livehybrid-splunk-s2-testing"
LOCAL_BASE_PATH = "/opt/splunk/var/lib/splunk"
PROCESS_BUCKET_SCRIPT = "./process_bucket.sh"
CACHEMANAGER_JSON_CONTENT = {
    "file_types": ["strings_data", "sourcetypes_data", "sources_data", "hosts_data", "bucket_info", "bfidx", "tsidx", "bloomfilter", "journal_gz", "deletes"]
}
s3 = boto3.client("s3")


# Utility Functions
def restart_splunk():
    print("Restarting Splunk...")
    result = subprocess.run(["/opt/splunk/bin/splunk", "restart"], capture_output=True, text=True)
    if result.returncode == 0:
        print("Splunk restarted successfully.")
    else:
        print(f"Error restarting Splunk: {result.stderr}")


def splunk_api_call(endpoint, data=None, method="POST"):
    url = f"{SPLUNK_URL}{endpoint}"
    response = requests.request(method, url, auth=AUTH, data=data, verify=False)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error with Splunk API call to {url}: {response.status_code} - {response.text}")
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
    s3_key = f"{index_name}/db/{sha_part1}/{sha_part2}/{bucket_num}~{server_guid}/receipt.json"
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
    # Paginate through S3 objects
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        if "CommonPrefixes" in page:
            for index in page["CommonPrefixes"]:
                index_name = index["Prefix"].rstrip("/")
                # Get buckets under each index
                sub_prefix = f"{index_name}/"
                sub_page = s3.list_objects_v2(Bucket=bucket_name, Prefix=sub_prefix, Delimiter="/")
                result[index_name.split("/")[-1]] = []

                for bucket_info in sub_page.get("CommonPrefixes", []):
                    splunk_bucket_name = bucket_info["Prefix"].split("/")[-2]
                    bucket_parts = splunk_bucket_name.split("_")
                    bucket_num = bucket_parts[3]
                    server_guid = bucket_parts[4]

                    # Determine initial status
                    receipt_exists = check_receipt_on_s3(index_name, bucket_num, server_guid)
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

                    result[index_name.split("/")[-1]].append({"bucket": splunk_bucket_name, "status": status})

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


def process_buckets(index_name, num_buckets):
    """
    Process N buckets from the specified index.

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

    # Process up to N buckets
    for bucket_info in buckets_to_process[:num_buckets]:
        bucket_name = bucket_info["bucket"]

        # Update status to "inprogress"
        bucket_info["status"] = "inprogress"
        save_bucket_structure(BUCKET_JSON, bucket_data)

        # Call process_bucket.sh
        try:
            print(f"Processing bucket: {bucket_name} for index: {index_name}")
            subprocess.run([PROCESS_BUCKET_SCRIPT, bucket_name, index_name], check=True)
            # Update status to "processed" after successful processing
            bucket_info["status"] = "pendingupload"
        except subprocess.CalledProcessError as e:
            print(f"Error processing bucket {bucket_name}: {e}")
            # Update status back to "todo" in case of an error
            bucket_info["status"] = "todo"

        update_cachemanager_file(index_name, bucket_name)

        # Save the updated JSON after each bucket
        save_bucket_structure(BUCKET_JSON, bucket_data)
        print("Processing complete. Restart Splunk to continue.")


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

def main():
    print("Starting DDSS Restore Workflow...")
    generate_bucket_structure(DDSS_BUCKET_NAME, "")
    index_name = input("Enter index name: ")
    num_buckets = int(input("Enter number of buckets to process: "))
    process_buckets(index_name, num_buckets)
    restart_splunk()
    upload_buckets()
    check_buckets()
    evict_buckets()
    print("Workflow complete.")


if __name__ == "__main__":
    main()
