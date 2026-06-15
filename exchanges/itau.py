# exchanges/itau.py

import pandas as pd
import numpy as np
import logging
import os

# 1. Import the Database wrapper from your root folder
from db import MySQLDB 

# 2. Import the config and helpers from your cores folder
from cores.utils import DB_CONFIG, mark_as_loaded, extract_file_date

# --- ITAU CONFIG ---
EXCHANGE_CONFIG = {
    "exchange_name": "Itau",
    "bucket": "axxela-s3-pub-ds1-mumbai", 
    "download_folder": os.path.join("downloads", "Itau"),
    "file_patterns": [
        {
            # Based on your S3 path: /Itau/2026/03/12/
            "filepath_template": "Itau/{YYYY}/{MM}/{DD}/",
            # Pattern to match: Axxela Report Full YYYYMMDD.csv
            "filename_template": "Axxela Report Full {YYYYMMDD}.csv",
        }
    ],
    "target_table": "itau_trade_summary", # Ensure this table exists
    "log_table": "file_load_log_itau",
}


def step_transform(file_path, config):
    try:
        # Load CSV
        df = pd.read_csv(file_path)
        logging.info(f"Raw row count: {len(df)}")
        logging.info(f"Raw Quantity sum: {df['Qty'].sum()}")
        logging.info(f"Raw Quantity abs sum: {df['Qty'].abs().sum()}")
        if df.empty: return None

        # 1. Clean headers to remove trailing spaces
        df.columns = [c.strip() for c in df.columns]

        # 2. RENAME MAP (Matching your exact screenshot headers)
        rename_map = {
            'Trade Time': 'Trade_date',     # Column A (Date)
            'Account Code': 'AccountCode',   # Column C
            'Account Alias': 'AccountAlias', # Column D
            'Symbol': 'FullSymbol',          # Column F
            'Commodity': 'Commodity',        # Column G
            'Qty': 'Quantity',               # Column L
            'Commision Rate': 'Raw_Comm',        # Column O
            'Exchange Fee': 'Raw_Exch',          # Column P
            'Registration Fee': 'Raw_Reg'        # Column Q
        }

        # Apply Rename
        existing_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=existing_rename)

        # 3. Handle the DII / DI1 variations immediately
        if 'Commodity' in df.columns:
            df['Commodity'] = df['Commodity'].astype(str).str.upper().str.strip()
            
        valid_commodities = ['DI1', 'WDO', 'WSP', 'DOL', 'ISP']
        df = df[df['Commodity'].isin(valid_commodities)].copy()

        if df.empty: return None

        # 4. Ensure numeric types for the pro-rata math
        numeric_cols = ['Quantity', 'Raw_Comm', 'Raw_Exch', 'Raw_Reg']
        for col in numeric_cols:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 5. Calculation Logic (Summing file totals)
        # After ensuring numeric types, ADD THIS:
        df['Raw_Comm_Total'] = 0.05 * (df['Quantity'].abs())  # rate × contracts

        total_broker_comm   = df['Raw_Comm_Total'].sum()
        total_exchange_fees = df['Raw_Exch'].sum() + df['Raw_Reg'].sum()
        total_file_qty = df['Quantity'].abs().sum()

        if total_file_qty == 0: return None

        # 6. Grouping
        df_grouped = df.groupby(['Trade_date', 'AccountCode', 'AccountAlias', 'Commodity'], as_index=False).agg({
            'Quantity': 'sum'
        })

        # 7. Map to Database CtrCodes
        commodity_to_ctr = {
            'DI1': 'FUT-7N-IO', # Both map to same DB code
            'WDO': 'FUT-7N-AA',
            'WSP': 'FUT-7N-WS',
            'DOL': 'FUT-7N-CU',
            'ISP': 'FUT-7N-SP'
        }
        df_grouped['CtrCode'] = df_grouped['Commodity'].map(commodity_to_ctr)

        # 8. Allocate Fees Pro-rata
        df_grouped['ExFee'] = round((df_grouped['Quantity'] / total_file_qty) * total_exchange_fees, 2)
        df_grouped['BrokerComm'] = round((df_grouped['Quantity'] / total_file_qty) * total_broker_comm, 2)

        # Final selection for step_load
        return df_grouped.rename(columns={'Quantity': 'Qty'})[['Trade_date', 'AccountCode', 'AccountAlias', 'CtrCode', 'Qty', 'ExFee', 'BrokerComm']]

    except Exception as e:
        logging.error(f"Error in Itau transform: {e}")
        return None

def step_load(df, filename, config):
    if df is None or df.empty: return False

    insert_query = f"""
    INSERT INTO {config['target_table']} 
        (Trade_date, ClientID, AccountAlias, CtrCode, Qty, ExFee, BrokerComm)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        Qty        = VALUES(Qty),
        ExFee      = VALUES(ExFee),
        BrokerComm = VALUES(BrokerComm),
        ExFeeUSD   = NULL,
        BrokerCommUSD = NULL
    """

    data_to_insert = [tuple(row) for row in df.values]

    try:
        with MySQLDB(DB_CONFIG) as db:
            db.executemany(insert_query, data_to_insert)
        
        mark_as_loaded(filename, extract_file_date(filename), config["log_table"])

        # Run FX conversion after load
        file_date = extract_file_date(filename)
        step_convert_to_usd(config["target_table"], file_date)

        return True
    except Exception as e:
        logging.error(f"DB Error: {e}")
        return False


def step_convert_to_usd(target_table, trade_date):
    """
    Updates ExFeeUSD and BrokerCommUSD using BRL rate from spotrate table.
    Itau is always BRL so we only need one JOIN.
    """
    try:
        with MySQLDB(DB_CONFIG) as db:

            # Convert using BRL rate from spotrate
            db.execute(f"""
                UPDATE {target_table} t
                JOIN spotrate s
                  ON s.currency = 'BRL'
                 AND s.date = %s
                SET t.ExFeeUSD     = t.ExFee / s.rate,
                    t.BrokerCommUSD = t.BrokerComm / s.rate
                WHERE t.Trade_date = %s
                  AND t.ExFeeUSD IS NULL
            """, (trade_date, trade_date))

            # Check if conversion succeeded (no BRL rate in spotrate)
            db.execute(f"""
                SELECT COUNT(*) as cnt
                FROM {target_table}
                WHERE Trade_date = %s
                  AND ExFeeUSD IS NULL
            """, (trade_date,))

            result = db.fetchone()
            remaining = result[0] if result else 0

            if remaining > 0:
                logging.warning(
                    f"  ⚠ No BRL rate found in spotrate for {trade_date}. "
                    f"{remaining} rows still unconverted."
                )
            else:
                logging.info(f"  ✓ BRL→USD conversion complete for {trade_date}")

    except Exception as e:
        logging.error(f"FX conversion error for {target_table} on {trade_date}: {e}")