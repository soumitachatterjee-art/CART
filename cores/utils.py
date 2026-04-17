# cores/utils.py

import os
import re
import json             # <--- Fixes 'json' is not defined
import logging          # <--- Fixes 'logging' is not defined
import configparser
from datetime import datetime, timedelta
# If MySQLDB is a local class/wrapper in your CART folder
from db import MySQLDB

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Axxela@123",
    "database": "cart",
    "port": 3306
}


        # ... rest of the function ...
def setup_logging(log_dir="logs", exchange_name="exchange"):
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{exchange_name}_{timestamp}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers = []

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info(f"Logging initialized. Log file: {log_file}")
    return logger, log_file


# =============================================================================
# SECTION 3: UTILITIES
# Shared helpers — no changes needed when adapting for other exchanges.
# =============================================================================

def load_credentials(credentials_file):
    """Load AWS credentials from JSON, INI/CFG, or key=value file."""
    _, ext = os.path.splitext(credentials_file)
    try:
        if ext.lower() == ".json":
            with open(credentials_file, "r") as f:
                creds = json.load(f)
            return creds.get("access_key"), creds.get("secret_key")
        elif ext.lower() in [".ini", ".cfg"]:
            config = configparser.ConfigParser()
            config.read(credentials_file)
            return config["aws"]["access_key"], config["aws"]["secret_key"]
        else:
            creds = {}
            with open(credentials_file, "r") as f:
                for line in f:
                    if "=" in line:
                        key, value = line.strip().split("=", 1)
                        creds[key.strip()] = value.strip()
            return creds.get("access_key"), creds.get("secret_key")
    except Exception as e:
        logging.error(f"Error reading credentials file: {e}")
        return None, None


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        logging.warning(f"Invalid date format: {date_str}. Expected YYYYMMDD.")
        return None


def generate_date_range(start_date_str, end_date_str, weekdays_only=True):
    start = parse_date(start_date_str)
    end = parse_date(end_date_str)
    if not start or not end or start > end:
        logging.error("Invalid date range.")
        return []
    dates = []
    current = start
    while current <= end:
        if not weekdays_only or current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def format_template(template, date_obj):
    """Replace {YYYY}, {MM}, {DD}, {YYYYMMDD} placeholders in a template string."""
    return (
        template
        .replace("{YYYY}", date_obj.strftime("%Y"))
        .replace("{MM}", date_obj.strftime("%m"))
        .replace("{DD}", date_obj.strftime("%d"))
        .replace("{YYYYMMDD}", date_obj.strftime("%Y%m%d"))
        .replace("{date}", date_obj.strftime("%Y%m%d"))
    )


def extract_file_date(filename):
    """Extract YYYYMMDD date from filename. Returns a date object or None."""
    name_without_ext = os.path.splitext(filename)[0]
    matches = re.findall(r"(\d{8})", name_without_ext)
    if matches:
        try:
            return datetime.strptime(matches[-1], "%Y%m%d").date()
        except ValueError:
            return None
    return None


def is_already_loaded(filename, log_table):
    with MySQLDB(DB_CONFIG, dictionary=True) as db:
        db.execute(
            f"SELECT filename FROM {log_table} WHERE filename = %s", (filename,)
        )
        return db.fetchone() is not None

# UPDATED: Added log_table as a parameter to ensure the query works
def mark_as_loaded(filename, file_date, log_table):
    with MySQLDB(DB_CONFIG) as db:
        db.execute(
            f"INSERT INTO {log_table} (filename, file_date) VALUES (%s, %s)",
            (filename, file_date),
        )


def format_code(value):
    """Zero-pad single-digit exchange/contract codes: '7' -> '07'."""
    s = str(value)
    return f"0{s}" if s.isdigit() and len(s) == 1 else s


