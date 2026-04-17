import os
import logging
from datetime import datetime

# Local imports
from cores.downloader import step_download
from cores.utils import extract_file_date, is_already_loaded

# 1. Added load_func to the signature
def run_pipeline(config, transform_func, load_func, credentials_file, start_date_str, end_date_str):
    """
    Modular Orchestrator
    - config: EXCHANGE_CONFIG
    - transform_func: The specific cleaning function (itau.step_transform)
    - load_func: The specific database function (itau.step_load)
    """
    stats = {"downloaded": 0, "loaded": 0, "skipped": 0, "failed": 0}

    # --- Step 1: Download ---
    file_paths = step_download(config, credentials_file, start_date_str, end_date_str)
    stats["downloaded"] = len(file_paths)

    logging.info(f"\n{'='*60}")
    logging.info(f"STEP 2 & 3: TRANSFORM + LOAD  [{config['exchange_name']}]")
    logging.info(f"{'='*60}")

    # --- Step 2 & 3: Transform + Load (per file) ---
    for file_path in file_paths:
        filename = os.path.basename(file_path)
        logging.info(f"\n  Processing: {filename}")

        # Guard: check if already loaded
        if is_already_loaded(filename, config["log_table"]):
            logging.info(f"   ✓ Skipped (already in DB): {filename}")
            stats["skipped"] += 1
            continue

        # 2. Use the dynamic transform function
        df = transform_func(file_path, config)
        
        # 3. Use the dynamic load function
        if df is not None:
            success = load_func(df, filename, config)
            if success:
                stats["loaded"] += 1
            else:
                stats["failed"] += 1
        else:
            stats["failed"] += 1

    # --- Summary ---
    logging.info(f"\n{'='*60}")
    logging.info(f"PIPELINE COMPLETE  [{config['exchange_name']}]")
    logging.info(f"{'='*60}")
    logging.info(f"  Files downloaded : {stats['downloaded']}")
    logging.info(f"  Files loaded     : {stats['loaded']}")
    logging.info(f"  Files skipped    : {stats['skipped']}")
    logging.info(f"  Files failed     : {stats['failed']}")
    logging.info(f"{'='*60}\n")

    return stats