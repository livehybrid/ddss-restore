import json
import os
import subprocess

# Paths
BUCKET_JSON = "bucket_structure.json"
PROCESS_BUCKET_SCRIPT = "./process_bucket.sh"

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
    buckets_to_process = [
        bucket for bucket in bucket_data[index_name] if bucket["status"] == "todo"
    ]

    # Process up to N buckets
    for bucket_info in buckets_to_process[:num_buckets]:
        bucket_name = bucket_info["bucket"]

        # Update status to "inprogress"
        bucket_info["status"] = "inprogress"
        save_bucket_structure(BUCKET_JSON, bucket_data)

        # Call process_bucket.sh
        try:
            print(f"Processing bucket: {bucket_name} for index: {index_name}")
            subprocess.run(
                [PROCESS_BUCKET_SCRIPT, bucket_name, index_name],
                check=True
            )
            # Update status to "processed" after successful processing
            bucket_info["status"] = "pendingupload"
        except subprocess.CalledProcessError as e:
            print(f"Error processing bucket {bucket_name}: {e}")
            # Update status back to "todo" in case of an error
            bucket_info["status"] = "todo"

        # Save the updated JSON after each bucket
        save_bucket_structure(BUCKET_JSON, bucket_data)

def main():
    # Input parameters
    index_name = input("Enter the index name: ")
    num_buckets = int(input("Enter the number of buckets to process: "))

    # Process buckets
    process_buckets(index_name, num_buckets)
    print("Done - Now restart Splunk for the buckets to be detected")

if __name__ == "__main__":
    main()
