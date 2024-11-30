import json
import requests
import urllib3
urllib3.disable_warnings()
import os

# Paths
BUCKET_JSON = "bucket_structure.json"
SPLUNK_URL = "https://localhost:8089"  # Update to your Splunk server URL
AUTH = ("admin", os.genenv("SPLUNK_PASSWORD"))  # Replace with your Splunk credentials

LOCAL_BASE_PATH = "/opt/splunk/var/lib/splunk"  # Base path for local bucket directories
CACHEMANAGER_JSON_CONTENT = {
    "file_types": [
        "strings_data",
        "sourcetypes_data",
        "sources_data",
        "hosts_data",
        "bucket_info",
        "bfidx",
        "tsidx",
        "bloomfilter",
        "journal_gz",
        "deletes"
    ]
}
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


def evict_pending_buckets(bucket_json):
    """
    Loops through the bucket_structure.json file and evicts all buckets with "pendingevict" status.

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
        with open(bucket_json, "w") as file:
            json.dump(bucket_data, file, indent=4)
        print(f"Updated {bucket_json} with evicted statuses.")
    else:
        print("No pending buckets to evict.")


def main():
    print(f"Starting eviction process for buckets in {BUCKET_JSON}...")
    evict_pending_buckets(BUCKET_JSON)


if __name__ == "__main__":
    main()