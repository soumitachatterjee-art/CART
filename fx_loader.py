# fx_loader.py

import requests
import logging
import os
from datetime import datetime, timedelta
from db import MySQLDB
from cores.utils import DB_CONFIG

# Currencies to fetch — add/remove as needed
FX_CURRENCIES = ['MYR', 'BRL', 'CNY', 'JPY', 'HKD', 'GBP', 'EUR', 'CAD']
def load_fx_rates(start_date_str, end_date_str):
    """
    Fetch historical FX rates from Frankfurter API and insert into spotrate table.
    Skips dates that are already loaded. Skips weekends (no market data).
    Mirrors the style of load_fx_files().
    """
    logging.info(f"\n{'='*60}")
    logging.info("Starting FX Rate Load Process")
    logging.info(f"{'='*60}")

    start = datetime.strptime(start_date_str, "%Y%m%d").date()
    end   = datetime.strptime(end_date_str,   "%Y%m%d").date()

    current = start
    while current <= end:

        # Skip weekends — no FX market data
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue

        date_str = current.strftime("%Y-%m-%d")

        # Check if already loaded
        try:
            with MySQLDB(DB_CONFIG, dictionary=True) as db:
                db.execute(
                    "SELECT 1 FROM spotrate WHERE date = %s LIMIT 1",
                    (current,)
                )
                already_loaded = db.fetchone()

            if already_loaded:
                logging.info(f"  ✓ Skipping (already loaded): {date_str}")
                current += timedelta(days=1)
                continue
        except Exception as e:
            logging.error(f"  ✗ DB check error for {date_str}: {e}")
            current += timedelta(days=1)
            continue

        # Fetch from Frankfurter API
        try:
            currencies_param = ",".join(FX_CURRENCIES)
            url = f"https://api.frankfurter.app/{date_str}?from=USD&to={currencies_param}"
            response = requests.get(url, timeout=10)

            if response.status_code != 200:
                logging.warning(f"  ✗ API returned {response.status_code} for {date_str} — skipping")
                current += timedelta(days=1)
                continue

            data = response.json()

            if "rates" not in data:
                logging.warning(f"  ✗ No rates in API response for {date_str} — skipping")
                current += timedelta(days=1)
                continue

            rows = [
    (current, currency, rate)
    for currency, rate in data["rates"].items()
]

# CNH not supported by Frankfurter — use CNY rate as proxy
            if "CNY" in data["rates"]:
                rows.append((current, "CNH", data["rates"]["CNY"]))

            with MySQLDB(DB_CONFIG) as db:
                db.executemany(
                    """
                    INSERT INTO spotrate (date, currency, rate)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE rate = VALUES(rate)
                    """,
                    rows
                )

            logging.info(f"  ✓ FX rates loaded for {date_str}: {list(data['rates'].keys())}")

        except requests.exceptions.Timeout:
            logging.error(f"  ✗ Timeout fetching FX for {date_str}")
        except requests.exceptions.ConnectionError:
            logging.error(f"  ✗ Connection error fetching FX for {date_str}")
        except Exception as e:
            logging.error(f"  ✗ Error loading FX for {date_str}: {e}")

        current += timedelta(days=1)

    logging.info(f"\n{'='*60}")
    logging.info("FX Rate Load Process Completed")
    logging.info(f"{'='*60}\n")