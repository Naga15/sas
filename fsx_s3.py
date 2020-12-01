#!/usr/bin/python3

# Require python > 3.5
# Install requirements: python3 -m pip install -r requirements.txt
# Execute: python3 fsx_s3.py

import os
import sys
import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# Global log file
LOG_FILE = "fsx-" + datetime.now().strftime("%m-%d-%Y_%H:%M:%S") + ".log"

def log(message):
    logtext = "[" + datetime.now().strftime("%m/%d/%Y-%H:%M:%S") + "] - " + message
    print(logtext)
    with open(LOG_FILE, "a") as logfile:
        logfile.write(logtext + "\n")

def list_all_files(path):
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from list_all_files(entry.path)
        else:
            yield entry

def main():
    log("Started.")

    # Define variables
    EXTENSION_LIST      = [".sas7"]
    RETENTION_AGE       = 90
    RETENTION_POINT     = datetime.now() - timedelta(days=RETENTION_AGE)
    FSX_MOUNTPOINT      = "/fsx/"
    S3_CLIENT           = boto3.client("s3")
    S3_BACKUP_BUCKET    = "your-s3-bucket"
    S3_BACKUP_PREFIX    = "/"
    FILE_COUNT          = 0
    SIZE_COUNT          = 0

    # Verify mountpoint existence:
    if not os.path.exists(FSX_MOUNTPOINT): 
        log("Exited (Invalid mountpoint).")
        exit(1)

    # Handle files
    for entry in list_all_files(FSX_MOUNTPOINT):
        orig_file = os.path.relpath(entry.path, FSX_MOUNTPOINT)

        # Skip other file types
        if os.path.splitext(orig_file)[1] not in EXTENSION_LIST: continue

        orig_stat = entry.stat()
        
        try:
            backup_file = S3_CLIENT.head_object(Bucket=S3_BACKUP_BUCKET, Key=S3_BACKUP_PREFIX+orig_file)
        except ClientError:
            # Skip file without matching backup
            continue

        # Skip file with strained backup
        if backup_file["ContentLength"] != orig_stat.st_size: continue                       

        # Skip unexpired file
        if backup_file["LastModified"].replace(tzinfo=None) >= RETENTION_POINT : continue
        
        # WARNING: Actually delete the file:
        os.remove(FSX_MOUNTPOINT + orig_file)
        FILE_COUNT += 1
        SIZE_COUNT += orig_stat.st_size
        log("DELETED " + orig_file)
    
    # Log time
    log("Report: Deleted {0} files, about {1:.2f} GB total size.".format(FILE_COUNT, SIZE_COUNT/1073741824))
    log("Finished.")

if __name__ == "__main__":
    main()
