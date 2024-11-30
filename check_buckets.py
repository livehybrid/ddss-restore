import json
import time
import requests
import boto3
import hashlib
import urllib3
import os
urllib3.disable_warnings()


# Configuration
BUCKET_JSON = "bucket_structure.json"
SPLUNK_URL = "https://localhost:8089"  # Update to your Splunk server URL
AUTH = ("admin", os.getenv("SPLUNK_PASSWORD"))  # Replace with your Splunk credentials
BUCKET2 = "livehybrid-splunk-s2-testing"  # S3 bucket name

# Initialize S3 client
s3 = boto3.client("s3")


def get_bucket_status(bid):
    """
    Queries Splunk REST API to get the upload status and bucket status.

    Args:
        bid (str): The bucket identifier (BID).

    Returns:
        tuple: (upload_status, bucket_status)
    """
    url = f"{SPLUNK_URL}/services/admin/cacheman/"
    query = f"|rest /services/admin/cacheman/ | search title=\"bid|{bid}|\" | table title cm:bucket.upload_status cm:bucket.status"

    response = requests.post(
        f"{SPLUNK_URL}/services/search/jobs",
        auth=AUTH,
        data={"search": query, "output_mode": "json","exec_mode":"oneshot"},
        verify=False  # Disable SSL verification for localhost; adjust as needed
    )
    if response.status_code == 200:
        results = response.json()["results"]
        for result in results:
            if result["title"] == f"bid|{bid}|":
                return result["cm:bucket.upload_status"], result["cm:bucket.status"]

    return None, None


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
    sha_input = f"{bucket_num}~{server_guid}"
    sha1_hash = hashlib.sha1(sha_input.encode()).hexdigest()
    s3_key = f"{index_name}/db/{sha1_hash[:2]}/{sha1_hash[2:4]}/{bucket_num}~{server_guid}/receipt.json"
    try:
        s3.head_object(Bucket=BUCKET2, Key=s3_key)
        print(f"Found receipt.json on S3: {s3_key}")
        return True
    except s3.exceptions.ClientError:
        print(f"Missing receipt.json on S3: {s3_key}")
        return False


def process_uploaded_buckets(bucket_json):
    """
    Processes buckets with status "uploaded" in the JSON file.

    Args:
        bucket_json (str): Path to the bucket_structure.json file.
    """
    # Load the JSON file
    with open(bucket_json, "r") as file:
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

                    if upload_status == "idle": #and bucket_status == "remote":
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
        with open(bucket_json, "w") as file:
            json.dump(bucket_data, file, indent=4)
        print(f"Updated {bucket_json} with pendingevict statuses.")
    else:
        print("No buckets to update.")


def main():
    print(f"Starting check process for buckets in {BUCKET_JSON}...")
    process_uploaded_buckets(BUCKET_JSON)


if __name__ == "__main__":
    main()
