
# **DDSS Restore Workflow**

This Python script provides a comprehensive workflow for managing the restore and SmartStore integration process for Splunk buckets from S3, including:

- Generating a bucket structure.
- Rebuilding buckets locally.
- Uploading buckets to SmartStore.
- Checking the status of uploaded buckets.
- Evicting buckets from local storage once uploaded.

---

## **Features**
1. **Generate Bucket Structure**: Scans the source S3 bucket for indexes and buckets, while also checking local status (`Hosts.data`) and the secondary S3 bucket for `receipt.json`.
2. **Rebuild Buckets**: Allows the user to rebuild specific buckets locally.
3. **Upload Buckets**: Automates uploading rebuilt buckets to Splunk SmartStore.
4. **Check Bucket Status**: Monitors the status of uploaded buckets and updates their status.
5. **Evict Buckets**: Evicts buckets from local storage after they are successfully uploaded.

---

## **Prerequisites**

1. **Python Requirements**:
   - Python 3.x
   - Required libraries: `boto3`, `requests`, `urllib3`

   Install dependencies:
   ```bash
   pip install boto3 requests urllib3
   ```

2. **Splunk Configuration**:
   - Ensure Splunk is running locally and accessible at `https://localhost:8089`.
   - Update the script with your Splunk credentials (`AUTH`) and URL (`SPLUNK_URL`).

3. **AWS Configuration**:
   - Configure AWS credentials with access to the required S3 buckets (`DDSS_BUCKET_NAME` and `S2_BUCKET_NAME`):
     ```bash
     aws configure
     ```

4. **Local File System**:
   - Buckets are assumed to be located at `/opt/splunk/var/lib/splunk`.

---

## **Usage**

### **Running the Script**

Run the script:
```bash
python dda_restore_workflow.py
```

### **Workflow**

1. **Generate Bucket Structure**:
   - The script scans the source S3 bucket (`DDSS_BUCKET_NAME`) and generates `bucket_structure.json` with the following statuses:
     - **`todo`**: No local file or receipt found in S3.
     - **`pendingupload`**: Bucket exists locally but no receipt found in S3.
     - **`pendingevict`**: Bucket exists locally and receipt found in S3.
     - **`done`**: Receipt found in S3, and bucket no longer needs local processing.

2. **Process Buckets**:
   - Enter the index name and number of buckets to rebuild locally:
     - **Index name**: `requests_apm_prod` or any other valid index.
     - **Number of buckets**: Enter `0` to skip rebuilding and proceed to check pending uploads or evictions.

3. **Restart Splunk**:
   - The script automatically restarts Splunk after processing buckets.

4. **Upload Buckets**:
   - Buckets with the `pendingupload` status are uploaded to SmartStore.

5. **Check Buckets**:
   - Monitors the status of uploaded buckets and updates their status to `pendingevict` if receipt is found.

6. **Evict Buckets**:
   - Buckets with `pendingevict` status are evicted from local storage, updating their status to `done`.

---

### **Command Line Prompts**

1. **Enter Index Name**:
   - Provide the name of the index to process (e.g., `requests_apm_prod`).
   - Enter `0` to skip bucket rebuilding and proceed with pending uploads or evictions.

2. **Enter Number of Buckets to Process**:
   - Specify how many buckets to process for rebuilding. Enter `0` to skip this step.

---

### **File Structure**

#### **Source S3 Bucket (`AA_BUCKET_NAME`)**:
```
<index_name>/
  db_1651670906_1651669557_0_CEB1E2B6-34F2-40EB-A2EF-10C5531556F9/
```

#### **Secondary S3 Bucket (`S2_BUCKET_NAME`)**:
```
<index_name>/db/<sha1[0:2]>/<sha1[2:4]>/<bucketNum>~<serverGUID>/receipt.json
```

#### **Local File System**:
```
/opt/splunk/var/lib/splunk/<index_name>/db/<bucket_name>/Hosts.data
```

#### **Output JSON File**:
```json
{
  "requests_apm_prod": [
    {
      "bucket": "db_1651670906_1651669557_0_CEB1E2B6-34F2-40EB-A2EF-10C5531556F9",
      "status": "todo"
    }
  ]
}
```

---

### **Status Flow**

1. **Initial Status**:
   - Scanned from `DDSS_BUCKET_NAME` and categorized based on local and secondary S3 checks:
     - `todo`, `pendingupload`, `pendingevict`, `done`.

2. **After Rebuilding**:
   - Updated to `pendingupload`.

3. **After Uploading**:
   - Monitored and updated to `pendingevict`.

4. **After Evicting**:
   - Updated to `done`.

---

### **Key Functions**

- **`generate_bucket_structure()`**:
  - Scans `DDSS_BUCKET_NAME` and generates the initial `bucket_structure.json`.

- **`process_buckets()`**:
  - Processes buckets with `todo` status and updates them to `pendingupload`.

- **`upload_buckets()`**:
  - Uploads buckets with `pendingupload` status to Splunk SmartStore.

- **`check_buckets()`**:
  - Monitors uploaded buckets and updates their status to `pendingevict`.

- **`evict_buckets()`**:
  - Evicts buckets with `pendingevict` status and updates them to `done`.

---

### **Troubleshooting**

- **AWS Permissions**:
  - Ensure the IAM user has `s3:ListBucket` and `s3:GetObject` permissions for `DDSS_BUCKET_NAME` and `S2_BUCKET_NAME`.

- **Splunk API Errors**:
  - Verify Splunk credentials and ensure the server is accessible at `SPLUNK_URL`.

- **Local Path Errors**:
  - Ensure the local Splunk directory exists and matches the configured `LOCAL_BASE_PATH`.

---

### **Examples**

#### **Generate Bucket Structure**
```bash
python ddss-restore.py
```

#### **Rebuild Buckets**
- Enter `requests_apm_prod` as the index and `5` for the number of buckets to process.

#### **Skip Rebuilds, Check Uploads and Evictions**
- Enter `0` for the number of buckets to skip rebuilding and proceed with pending uploads or evictions.

---

### **License**
This script is provided under the MIT License. Use at your own risk.
