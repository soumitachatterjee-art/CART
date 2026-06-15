import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime
from db import MySQLDB
from cores.utils import DB_CONFIG, mark_as_loaded, extract_file_date

# --- KGI CONFIG ---
EXCHANGE_CONFIG = {
    "exchange_name": "KGI",
    "bucket": "axxela-s3-pub-ds1-mumbai",
    "download_folder": os.path.join("downloads", "KGI"),
    "file_patterns": [
        {
            "filepath_template": "KGI/{YYYY}/{MM}/{DD}/",
            "filename_template": "KT513_{YYYYMMDD}_NT.csv",
        }
    ],
    "target_table": "kgi_trades",
    "log_table": "file_load_log_kgi",
}

def step_transform(file_path, config):
    try:
        required_columns = [
            "TRADE DATE", "CLIENT CODE", "EXCHANGE", "CONTRACT", 
            "COMM CCY", "COMMISSION", "TRADELOT", "REMARKS"
        ]
        
        # 1. Read CSV with specific columns
        df = pd.read_csv(file_path, usecols=required_columns)
        if df.empty:
            return None

        # 2. Date Conversion (DD/MM/YYYY)
        df['TRADE DATE'] = pd.to_datetime(df['TRADE DATE'], format='%d/%m/%Y').dt.date
        
        # 3. Filter out REMARKS starting with 'TRF'
        df = df[~df['REMARKS'].str.startswith('TRF', na=False)].copy()

        # 4. Contract Mapping Logic
        mapping = {
            'A': 'FUT-7P-AS', 'B': 'FUT-7P-BS', 'BC': 'FUT-5N-BC',
            'BRL': 'FUT-17-G*', 'FCH': 'FUT-17-D-', 'FEF': 'FUT-17-F<',
            'GO': 'FUT-7Z-BG', 'HC': 'FUT-33-HC', 'HSI': 'FUT-33-HS',
            'HTI': 'FUT-33-DY', 'I': 'FUT-7P-IO', 'IU': 'FUT-17-IU',
            'JB': 'FUT-17-JB', 'KU': 'FUT-17-KU', 'M': 'FUT-7P-MS',
            'MCA': 'FUT-33-EH', 'NK': 'FUT-17-NK', 'NO1': 'FUT-22-MN',
            'NR': 'FUT-5N-NR', 'P': 'FUT-7P-PO', 'PO': 'FUT-49-PO',
            'SC': 'FUT-5N-SC', 'TF': 'FUT-17-TF', 'TOA3MF': 'FUT-24-A1',
            'TWD': 'FUT-17-M1', 'UC': 'FUT-17-U(', 'Y': 'FUT-7P-YS',
            'YE': 'FUT-33-US'
        }
        
        # Apply mapping or keep original if not found
        df['CtrCode'] = df['CONTRACT'].map(mapping).fillna(df['CONTRACT'])
        
        # Special Condition: JB + OSE
        df.loc[(df['CONTRACT'] == 'JB') & (df['EXCHANGE'] == 'OSE'), 'CtrCode'] = 'FUT-24-RV'

        # 5. Clean numeric columns
        df['COMMISSION'] = pd.to_numeric(df['COMMISSION'], errors='coerce').fillna(0)
        df['TRADELOT'] = pd.to_numeric(df['TRADELOT'], errors='coerce').fillna(0)

        # 6. Group & Aggregate
        group_cols = ['TRADE DATE', 'CLIENT CODE', 'CtrCode', 'COMM CCY']
        df_grouped = df.groupby(group_cols, as_index=False).agg({
            'COMMISSION': 'sum',
            'TRADELOT': 'sum'
        })

        # 7. Final selection for step_load (Match DB column order)
        df_final = pd.DataFrame({
            'Trade_Date': df_grouped['TRADE DATE'],
            'ClientID': df_grouped['CLIENT CODE'],
            'CtrCode': df_grouped['CtrCode'],
            'CommCur': df_grouped['COMM CCY'],
            'CommFee': df_grouped['COMMISSION'],
            'Qty': df_grouped['TRADELOT'].astype(int)
        })
        
        return df_final.sort_values(['Trade_Date', 'CtrCode'])

    except Exception as e:
        logging.error(f"Error in KGI transform: {e}")
        return None

def step_load(df, filename, config):
    """Inserts KGI summary with cumulative sum logic (Qty = Qty + VALUES(Qty))"""
    if df is None or df.empty:
        return False

    insert_query = f"""
        INSERT INTO {config['target_table']} 
            (Trade_Date, ClientID, CtrCode, CommCur, CommFee, Qty)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            CommCur = VALUES(CommCur),
            CommFee = CommFee + VALUES(CommFee),
            Qty = Qty + VALUES(Qty),
            CommFeeUSD = NULL
    """
    
    data_to_insert = [tuple(row) for row in df.values]

    try:
        with MySQLDB(DB_CONFIG) as db:
            db.executemany(insert_query, data_to_insert)
        
        # Log success
        mark_as_loaded(filename, extract_file_date(filename), config["log_table"])

        # Run FX conversion after load
        file_date = extract_file_date(filename)
        step_convert_to_usd(config["target_table"], file_date)

        return True
    except Exception as e:
        logging.error(f"DB Error loading KGI: {e}")
        return False


def step_convert_to_usd(target_table, trade_date):
    """
    Updates CommFeeUSD for rows where CommCur != 'USD',
    using rates from the spotrate table for the given trade_date.
    Rows already in USD are set directly (CommFeeUSD = CommFee).
    """
    try:
        with MySQLDB(DB_CONFIG) as db:

            # 1. Set CommFeeUSD directly for USD rows
            db.execute(f"""
                UPDATE {target_table}
                SET CommFeeUSD = CommFee
                WHERE Trade_Date = %s
                  AND CommCur = 'USD'
                  AND CommFeeUSD IS NULL
            """, (trade_date,))

            # 2. For non-USD rows, join with spotrate to convert
            db.execute(f"""
                UPDATE {target_table} t
                JOIN spotrate s
                  ON s.currency = t.CommCur
                 AND s.date = %s
                SET t.CommFeeUSD = t.CommFee / s.rate
                WHERE t.Trade_Date = %s
                  AND t.CommCur != 'USD'
                  AND t.CommFeeUSD IS NULL
            """, (trade_date, trade_date))

            # 3. Warn about any rows still unconverted (missing FX rate)
            db.execute(f"""
                SELECT DISTINCT CommCur
                FROM {target_table}
                WHERE Trade_Date = %s
                  AND CommFeeUSD IS NULL
            """, (trade_date,))

            missing = db.fetchall()
            if missing:
                missing_currencies = [row[0] for row in missing]
                logging.warning(
                    f"  ⚠ No FX rate found for date {trade_date}, "
                    f"currencies still unconverted: {missing_currencies}"
                )
            else:
                logging.info(f"  ✓ CommFeeUSD conversion complete for {trade_date}")

    except Exception as e:
        logging.error(f"FX conversion error for {target_table} on {trade_date}: {e}")