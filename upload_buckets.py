import json
import requests
import urllib3
import os
urllib3.disable_warnings()


# Paths
BUCKET_JSON = "bucket_structure.json"
SPLUNK_URL = "https://localhost:8089"  # Update to your Splunk server URL
AUTH = ("admin", os.getenv("SPLUNK_PASSWORD"))  # Replace with your Splunk credentials


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
    response = requests.post(
        url,
        auth=AUTH,
        data={"sid": bid},
        verify=False
    )

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
    response = requests.post(
        url,
        auth=AUTH,
        data={"sid": bid, "directory": ""},
        verify=False
    )

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
    response = requests.post(
        url,
        auth=AUTH,
        data={"sid": bid},
        verify=False
    )

    if response.status_code == 200:
        print(f"Successfully closed bucket: {bid}")
        return True
    else:
        print(f"Failed to close bucket: {bid}. Response: {response.status_code} - {response.text}")
        return False


def process_upload_buckets(bucket_json):
    """
    Loops through the bucket_structure.json file and processes all buckets with "pendingupload" status.

    Args:
        bucket_json (str): Path to the bucket_structure.json file.
    """
    # Load the JSON file
    with open(bucket_json, "r") as file:
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
                            bucket_info["status"] = "pendingevict"
                            modified = True

    # Save updated JSON file if any changes were made
    if modified:
        with open(bucket_json, "w") as file:
            json.dump(bucket_data, file, indent=4)
        print(f"Updated {bucket_json} with uploaded statuses.")
    else:
        print("No buckets to upload.")


def main():
    print(f"Starting upload process for buckets in {BUCKET_JSON}...")
    process_upload_buckets(BUCKET_JSON)


if __name__ == "__main__":
    main()
