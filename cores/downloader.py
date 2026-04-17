# cores/downloader.py

import os
import boto3
import fnmatch
import logging  # <--- This is the one causing the current error
from botocore.exceptions import ClientError

# Also import any local utilities used inside this file
from cores.utils import (
    load_credentials, 
    generate_date_range, 
    format_template, 
    is_already_loaded
)
def step_download(config, credentials_file, start_date_str, end_date_str):
    """
    Downloads files from S3 into the local download_folder.
    Skips any file that has already been marked as loaded in the DB.

    Returns:
        list[str]: Paths of files downloaded (or already present and not yet loaded).
    """
    logging.info(f"\n{'='*60}")
    logging.info(f"STEP 1: DOWNLOAD  [{config['exchange_name']}]")
    logging.info(f"{'='*60}")

    access_key, secret_key = load_credentials(credentials_file)
    if not access_key or not secret_key:
        logging.error("Failed to load AWS credentials.")
        return []

    date_range = generate_date_range(start_date_str, end_date_str)
    if not date_range:
        return []

    download_folder = config["download_folder"]
    os.makedirs(download_folder, exist_ok=True)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    downloaded_files = []

    for date_obj in date_range:
        for pattern_cfg in config["file_patterns"]:
            s3_prefix = format_template(pattern_cfg["filepath_template"], date_obj)
            filename_pattern = format_template(pattern_cfg["filename_template"], date_obj)

            if "*" in filename_pattern:
                # Wildcard: list the prefix and match
                try:
                    response = s3.list_objects_v2(
                        Bucket=config["bucket"], Prefix=s3_prefix
                    )
                    all_keys = [
                        obj["Key"]
                        for obj in response.get("Contents", [])
                    ]
                except ClientError as e:
                    logging.warning(f"  Could not list {s3_prefix}: {e}")
                    continue

                matched = [
                    k for k in all_keys
                    if fnmatch.fnmatch(os.path.basename(k), filename_pattern)
                ]

                if not matched:
                    logging.warning(
                        f"  No files matched pattern '{filename_pattern}' "
                        f"on {date_obj.strftime('%Y-%m-%d')}"
                    )
                    continue

                for s3_key in matched:
                    filename = os.path.basename(s3_key)
                    local_path = os.path.join(download_folder, filename)
                    # UPDATED: Added config["log_table"] to the call
                    _download_single_file(
                        s3, config["bucket"], s3_key,
                        local_path, filename, downloaded_files, config["log_table"]
                    )
            else:
                # Exact filename
                s3_key = s3_prefix + filename_pattern
                local_path = os.path.join(download_folder, filename_pattern)
                # UPDATED: Added config["log_table"] to the call
                _download_single_file(
                    s3, config["bucket"], s3_key,
                    local_path, filename_pattern, downloaded_files, config["log_table"]
                )

    logging.info(f"\n   Total files ready for processing: {len(downloaded_files)}")
    return downloaded_files
                


def _download_single_file(s3, bucket, s3_key, local_path, filename, result_list, log_table):
    """
    Downloads one file. Skips if already loaded in DB.
    Appends local_path to result_list on success.
    """
    # UPDATED: Passed log_table into the check
    if is_already_loaded(filename, log_table):
        logging.info(f"   ✓ Skipped (already loaded in DB): {filename}")
        return

    if os.path.exists(local_path):
        logging.info(f"   ✓ Already downloaded, queued for load: {filename}")
        result_list.append(local_path)
        return

    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket, s3_key, local_path)
        logging.info(f"   ✓ Downloaded: {filename}")
        result_list.append(local_path)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            logging.warning(f"   ✗ Not found on S3: {filename}")
        else:
            logging.error(f"   ✗ S3 error for {filename}: {e}")