import pandas as pd
import logging
import os
from datetime import datetime
#from db import MySQLDB
#from cores.utils import DB_CONFIG, mark_as_loaded, extract_file_date, format_code


CR_CONFIG = {
    "exchange_name": "2026",
    "bucket": "bi-cr3-prod/SYM",
    "download_folder": os.path.join("downloads", "cr"),

    "file_patterns": [
        {
            "filepath_template": "SYM/{YYYY}/{MM}/{DD}/",
            "filename_template": "TRD_SYM_{YYYYMMDD}.csv",
        }
    ],

    "target_table": "cr_trades",
    "log_table": "file_load_log",
}

def step_transform(file_path, config):
    required_columns = [
        "datestr", "sectyp", "exchangecode", "contractcode",
        "clientaccountnumber", "trdtyp",
        "mktcur", "mktfee", "commcur", "commfee",
        "nfafeecur", "nfafee", "qty"
    ]

    try:
        df = pd.read_csv(file_path, usecols=required_columns)
    except Exception as e:
        logging.error(f"Could not read {file_path}: {e}")
        return None

    # --- Transform ---
    df['datestr'] = pd.to_datetime(df['datestr'], errors='coerce')

    df['mktfee'] = pd.to_numeric(df['mktfee'], errors='coerce').fillna(0)
    df['commfee'] = pd.to_numeric(df['commfee'], errors='coerce').fillna(0)
    df['nfafee'] = pd.to_numeric(df['nfafee'], errors='coerce').fillna(0)
    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)

    # Account mapping (optional)
   # account_mapping = load_account_mapping()  # you create this helper
    df['account'] = df['clientaccountnumber']

    # Normalize codes
    df['exchangecode'] = df['exchangecode'].astype(str).str.zfill(2)
    df['contractcode'] = df['contractcode'].astype(str).str.zfill(2)

    # CtrCode
    df['CtrCode'] = df['sectyp'] + '-' + df['exchangecode'] + '-' + df['contractcode']

    # Filter
    df = df[df['trdtyp'] == 'T']

    if df.empty:
        logging.warning(f"No valid trades in {file_path}")
        return None

    # Group
    df_grouped = df.groupby(
        ['datestr', 'CtrCode', 'account', 'mktcur', 'commcur', 'nfafeecur'],
        as_index=False
    ).agg({
        'mktfee': 'sum',
        'commfee': 'sum',
        'nfafee': 'sum',
        'qty': 'sum'
    })

    # Final format
    df_final = pd.DataFrame({
        'Trade_date': df_grouped['datestr'],
        'CtrCode': df_grouped['CtrCode'],
        'Account': df_grouped['account'],
        'mktCur': df_grouped['mktcur'],
        'ExFee': df_grouped['mktfee'],
        'CommCur': df_grouped['commcur'],
        'CommFee': df_grouped['commfee'],
        'NfafeeCur': df_grouped['nfafeecur'],
        'NfaFee': df_grouped['nfafee'],
        'Qty': df_grouped['qty'].astype(int)
    })

    df_final = df_final.sort_values(['Trade_date', 'CtrCode', 'Account'])

    logging.info(f"Transform complete: {len(df_final)} rows")
    return df_final


# def step_load(df, filename, config):

#     if df is None or df.empty:
#         logging.warning(f"Nothing to load for {filename}")
#         return False

#     insert_query = f"""
#         INSERT INTO {config['target_table']}
#         (Trade_date, CtrCode, Account, mktCur, ExFee,
#          CommCur, CommFee, NfafeeCur, NfaFee, Qty)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         ON DUPLICATE KEY UPDATE
#             mktCur = VALUES(mktCur),
#             ExFee = ExFee + VALUES(ExFee),
#             CommCur = VALUES(CommCur),
#             CommFee = CommFee + VALUES(CommFee),
#             NfafeeCur = VALUES(NfafeeCur),
#             NfaFee = NfaFee + VALUES(NfaFee),
#             Qty = Qty + VALUES(Qty),
#             ExFeeUSD = NULL,
#             CommFeeUSD = NULL,
#             NfaFeeUSD = NULL
#     """

#     try:
#         data = [tuple(row) for row in df.values]

#         with MySQLDB(DB_CONFIG) as db:
#             db.executemany(insert_query, data)

#         file_date = extract_file_date(filename) or datetime.today().date()
#         mark_as_loaded(filename, file_date, config["log_table"])

#         logging.info(f"✓ Loaded {len(data)} rows from {filename}")
#         return True

#     except Exception as e:
#         logging.error(f"DB error: {e}")
#         return False
def step_load_dummy(df, filename, config):
    if df is None or df.empty:
        print(f"{filename}: No data")
        return True

    print(f"{filename}: {len(df)} rows ready for DB")
    print(df.head())  # preview
    return True