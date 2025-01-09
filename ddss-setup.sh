# Not setting INDEX_NAME will result in the script taking the first index that requires migrations
#export INDEX_NAME=common_azure
export NUM_BUCKETS=100
export MAX_WORKERS=10
export SPLUNK_USERNAME=admin
export SPLUNK_PASSWORD=password
export DDSS_BUCKET_NAME="mystackname-btwxa8f7miac-archive"
export DDSS_PATH_NAME="somesuffix/"
export S2_BUCKET_NAME="mystackname-btwxa8f7miac-archive"
export S2_PATH_NAME="smartstore/"
# Defaults to /opt/splunk/var/lib/splunk/
# export LOCAL_BASE_PATH="/splunkdata/indexes/"
