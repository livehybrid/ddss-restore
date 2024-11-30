#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Check for required parameters
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <BUCKET_ID> <INDEX_NAME>"
    exit 1
fi

# Parameters
BUCKET_ID=$1
INDEX_NAME=$2

# Extract components from BUCKET_ID
BUCKET_NUM=$(echo "$BUCKET_ID" | cut -d'_' -f4)
GUID=$(echo "$BUCKET_ID" | cut -d'_' -f5)
LATEST_TIME=$(echo "$BUCKET_ID" | cut -d'_' -f2)
EARLIEST_TIME=$(echo "$BUCKET_ID" | cut -d'_' -f3 | cut -d'_' -f2)
BID="${INDEX_NAME}~${BUCKET_NUM}~${GUID}"

# Paths
BUCKET_DIR="/opt/splunk/var/lib/splunk/$INDEX_NAME/db/$BUCKET_ID"
RAWDATA_DIR="$BUCKET_DIR/rawdata"
UPLOAD_JSON="/opt/splunk/var/run/splunk/cachemanager_upload.json"

# Create directory structure
echo "Creating directory structure at $RAWDATA_DIR..."
mkdir -p "$RAWDATA_DIR"

# Download the journal.zst file from S3
S3_BUCKET="s3://scde-3usvpx5d8elc6o712-d0hrpb07azl9-testing2"
echo "Downloading journal.zst from $S3_BUCKET/$INDEX_NAME/$BUCKET_ID/rawdata/journal.zst to $RAWDATA_DIR..."
aws s3 cp "$S3_BUCKET/$INDEX_NAME/$BUCKET_ID/rawdata/journal.zst" "$RAWDATA_DIR/"

# Rebuild the bucket
echo "Rebuilding bucket $BUCKET_ID..."
/opt/splunk/bin/splunk cmd splunkd fsck repair --one-bucket --include-hots --bucket-path="$BUCKET_DIR" --index-name="$INDEX_NAME" --log-to--splunkd-log

# Append BID to cachemanager_upload.json
echo "Updating cachemanager_upload.json with BID: $BID..."
if [ ! -f "$UPLOAD_JSON" ]; then
    echo '{"bucket_ids":[]}' > "$UPLOAD_JSON"
fi
jq --arg BID "$BID" '.bucket_ids += [$BID] | .bucket_ids |= unique' "$UPLOAD_JSON" > "${UPLOAD_JSON}.tmp" && mv "${UPLOAD_JSON}.tmp" "$UPLOAD_JSON"

echo "Bucket $BUCKET_ID processed successfully."