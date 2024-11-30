import boto3
import json
import os
import hashlib

# AWS S3 Buckets
BUCKET_NAME = "scde-3usvpx5d8elc6o712-d0hrpb07azl9-testing2"
BUCKET2 = "livehybrid-splunk-s2-testing"

# Prefix (if needed, otherwise leave empty to scan the entire bucket)
PREFIX = ""

# Local Splunk directory base path
LOCAL_BASE_PATH = "/opt/splunk/var/lib/splunk"

# Output JSON file
OUTPUT_JSON = "bucket_structure.json"

# S3 Client
s3 = boto3.client("s3")


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
        s3.head_object(Bucket=BUCKET2, Key=s3_key)
        #print(f"Found receipt.json on S3: {s3_key}")
        return True
    except s3.exceptions.ClientError:
        #print(f"Missing receipt.json on S3: {s3_key}")
        return False


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

    return result


def main():
    print(f"Scanning S3 bucket: {BUCKET_NAME}...")
    # Generate the bucket structure
    bucket_structure = generate_bucket_structure(BUCKET_NAME, PREFIX)

    # Write to JSON file
    with open(OUTPUT_JSON, "w") as json_file:
        json.dump(bucket_structure, json_file, indent=4)

    print(f"Bucket structure saved to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()