
# DDAA Restore

This repository provides a set of Python scripts and tools to restore, process, upload, and manage Splunk buckets from S3. 
The workflow includes scanning S3 for frozen buckets, processing them locally, uploading to SmartStore, and managing the lifecycle of these buckets.

## Setup Instructions

1. **Install Python 3.13**:
   Ensure `python3.13-venv` and `python3.13-pip` are installed.

2. **Clone the Repository**:
   ```bash
   git clone https://github.com/livehybrid/ddaa-restore.git
   cd ddaa-restore
   ```

3. **Create and Activate a Virtual Environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

4. **Install Dependencies**:
   ```bash
   pip install requests boto3
   ```

---

## Process Workflow

### Step 1: Generate the Bucket Structure
Run the `generate_bucket_structure.py` script to scan the S3 bucket for frozen Splunk buckets:
```bash
python3 generate_bucket_structure.py
```

**Output**: The script scans the specified S3 bucket and generates a `bucket_structure.json` file:
```
Scanning S3 bucket: scde-3usvpx5d8elc6o712-d0hrpb07azl9-testing2...
Bucket structure saved to bucket_structure.json
```

---

### Step 2: Process Buckets Locally
Use the `process_buckets_from_json.py` script to process buckets locally:
```bash
python3 process_buckets_from_json.py
```

- **Prompt 1**: Enter the index name (e.g., `main`).
- **Prompt 2**: Enter the number of buckets to process (e.g., `2`).

**Output Example**:
```
Processing bucket: db_1651609155_1651609155_0_169641EF-FAC0-437D-AC01-A50CA18C51DC for index: main
Creating directory structure...
Downloading journal.zst...
Rebuilding bucket...
Updating cachemanager_upload.json...
Bucket processed successfully.
```

---

### Step 3: Restart Splunk
Restart the Splunk instance to detect the processed buckets:
```bash
/opt/splunk/bin/splunk restart
```

---

### Step 4: Upload Buckets to SmartStore
Run the `upload_buckets.py` script to upload the buckets:
```bash
python3 upload_buckets.py
```

**Output Example**:
```
Starting upload process...
Successfully initialized bucket in cacheman...
Successfully attached bucket...
Successfully closed bucket...
Updated bucket_structure.json with uploaded statuses.
```

---

### Step 5: Verify Upload Status
Use the `check_buckets.py` script to verify if the bucket uploads are complete:
```bash
python3 check_buckets.py
```

**Output Example**:
```
Starting check process...
Bucket upload complete...
Found receipt.json on S3...
Updated bucket_structure.json with pendingevict statuses.
```

---

### Step 6: Evict Processed Buckets
Run the `evict_buckets.py` script to evict the uploaded buckets and update local metadata:
```bash
python3 evict_buckets.py
```

**Output Example**:
```
Starting eviction process...
Successfully evicted bucket...
Successfully updated cachemanager_local.json...
Updated bucket_structure.json with evicted statuses.
```

---

## File Descriptions

1. **`generate_bucket_structure.py`**:
   - Scans the specified S3 bucket for frozen Splunk buckets and generates a JSON structure (`bucket_structure.json`).

2. **`process_buckets_from_json.py`**:
   - Processes specified buckets locally by:
     - Downloading `journal.zst` files.
     - Rebuilding bucket structures.
     - Updating `cachemanager_upload.json`.

3. **`upload_buckets.py`**:
   - Uploads the processed buckets to SmartStore using Splunk REST API.
   - Initializes, attaches, and closes buckets in the cacheman.

4. **`check_buckets.py`**:
   - Verifies the upload status of buckets using Splunk REST API.
   - Updates bucket statuses to `"pendingevict"` if upload is complete and the `receipt.json` is found in S3.

5. **`evict_buckets.py`**:
   - Evicts uploaded buckets and creates/updates a `cachemanager_local.json` file in the local bucket directory.

6. **`process_bucket.sh`**:
   - A utility script to handle individual bucket processing tasks.
