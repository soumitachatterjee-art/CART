# exchanges/marex.py

import os
import logging
import numpy as np
import pandas as pd

from db import MySQLDB
from cores.utils import DB_CONFIG, mark_as_loaded, extract_file_date, format_code


# =============================================================================
# CONFIG
# =============================================================================

EXCHANGE_CONFIG = {
    "exchange_name": "Marex",
    "bucket": "axxela-s3-pub-ds1-mumbai",
    "download_folder": os.path.join("downloads", "Marex"),
    "file_patterns": [
        {
            "filepath_template": "Marex/{YYYY}/{MM}/{DD}/",
            "filename_template": "SYMMETRY_FINANCIALS_LIMITED_CLEARISK_CoreTrades_MFLA_*_{YYYYMMDD}.csv",
        },
        {
            "filepath_template": "Marex/{YYYY}/{MM}/{DD}/",
            "filename_template": "SYMMETRY_FINANCIALS_LIMITED_CLEARISK_CoreTrades_MFLA_*.{YYYYMMDD}.csv",
        },
    ],
    "excluded_accounts": ["EE066", "EE067", "EE068", "EE069", "EE070"],
    "valid_transaction_types": ["SP", "", "CS", "SE", "SA", " ", "BA", None, np.nan],
    "target_table": "marex_trades_march",
    "log_table":    "file_load_log_march",
}


# =============================================================================
# TRANSFORM
# =============================================================================

def step_transform(file_path, config):
    """
    Reads and transforms the raw Marex CSV into a clean DataFrame
    ready to be inserted into the target table.
    """
    required_columns = [
        "Rundate", "TradeDate", "TransactionType", "Market", "Contract",
        "OptionType", "Account", "Commission_CCY", "Clearing_Fee_CCY",
        "Exchange_Fee_CCY", "NFA_Fee_CCY", "Clearing_Fee_Amount",
        "Exchange_Fee_Amount", "Comm", "NFA_Fee_Amount", "Lots",
    ]

    try:
        df = pd.read_csv(file_path, usecols=required_columns)
    except Exception as e:
        logging.error(f"  Could not read {file_path}: {e}")
        return None

    # --- Date parsing ---
    df["Rundate"]   = pd.to_datetime(df["Rundate"],   format="%Y%m%d")
    df["TradeDate"] = pd.to_datetime(df["TradeDate"], format="%Y%m%d")

    # --- Filtering ---
    excluded    = config["excluded_accounts"]
    valid_types = config["valid_transaction_types"]

    df = df[
        (df["TransactionType"].isin(valid_types)) &
        (~df["Account"].isin(excluded))
    ].copy()

    if df.empty:
        logging.warning(f"  No records remain after filtering: {os.path.basename(file_path)}")
        return None

    logging.info(f"  Filtered to {len(df)} records")

    # --- CtrCode construction ---
    df["CtrCode"] = df.apply(
        lambda row: (
            f"OPT-{format_code(row['Market'])}-{format_code(row['Contract'])}"
            if row["OptionType"] in ["C", "P"]
            else f"FUT-{format_code(row['Market'])}-{format_code(row['Contract'])}"
        ),
        axis=1,
    )

    # --- Grouping ---
    group_cols = [
        "TradeDate", "CtrCode", "Account", "Commission_CCY",
        "Clearing_Fee_CCY", "Exchange_Fee_CCY", "NFA_Fee_CCY",
    ]
    agg_dict = {
        "Clearing_Fee_Amount": "sum",
        "Exchange_Fee_Amount": "sum",
        "Comm":                "sum",
        "NFA_Fee_Amount":      "sum",
        "Lots":                "sum",
    }
    df = df.groupby(group_cols).agg(agg_dict).reset_index()

    # --- Derived columns ---
    df["mktCur"] = df.apply(
        lambda row: row["Clearing_Fee_CCY"]
        if row["Clearing_Fee_Amount"] < 0
        else row["Exchange_Fee_CCY"],
        axis=1,
    )
    df["ExFee"] = df.apply(
        lambda row: row["Clearing_Fee_Amount"] + row["Exchange_Fee_Amount"]
        if row["Clearing_Fee_Amount"] < 0
        else row["Exchange_Fee_Amount"],
        axis=1,
    )

    # --- Final shape ---
    df_final = pd.DataFrame({
        "Trade_date": df["TradeDate"],
        "CtrCode":    df["CtrCode"],
        "Account":    df["Account"],
        "mktCur":     df["mktCur"],
        "ExFee":      df["ExFee"],
        "CommCur":    df["Commission_CCY"],
        "CommFee":    df["Comm"],
        "NfafeeCur":  df["NFA_Fee_CCY"],
        "NfaFee":     df["NFA_Fee_Amount"],
        "Qty":        df["Lots"],
    })

    df_final = df_final.sort_values(["Trade_date", "CtrCode", "Account"])
    logging.info(f"  Transform complete: {len(df_final)} records ready")
    return df_final


# =============================================================================
# LOAD
# =============================================================================

def step_load(df, filename, config):
    """
    Inserts transformed DataFrame into the DB and logs the file as loaded.
    USD values are assumed 1:1 (no FX conversion needed for Marex).
    """
    if df is None or df.empty:
        logging.warning(f"  Nothing to load for {filename}, skipping.")
        return False

    insert_query = f"""
        INSERT INTO {config['target_table']}
            (Trade_date, CtrCode, Account, mktCur, ExFee,
             CommCur, CommFee, NfafeeCur, NfaFee, Qty,
             ExFeeUSD, CommFeeUSD, NfaFeeUSD, TotalUSD)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            mktCur     = VALUES(mktCur),
            ExFee      = ExFee      + VALUES(ExFee),
            CommCur    = VALUES(CommCur),
            CommFee    = CommFee    + VALUES(CommFee),
            NfafeeCur  = VALUES(NfafeeCur),
            NfaFee     = NfaFee     + VALUES(NfaFee),
            Qty        = Qty        + VALUES(Qty),
            ExFeeUSD   = ExFeeUSD   + VALUES(ExFeeUSD),
            CommFeeUSD = CommFeeUSD + VALUES(CommFeeUSD),
            NfaFeeUSD  = NfaFeeUSD  + VALUES(NfaFeeUSD),
            TotalUSD   = TotalUSD   + VALUES(TotalUSD)
    """

    # Marex fees are already in USD — 1:1 conversion
    df["ExFeeUSD"]   = df["ExFee"]
    df["CommFeeUSD"] = df["CommFee"]
    df["NfaFeeUSD"]  = df["NfaFee"]
    df["TotalUSD"]   = df["ExFeeUSD"] + df["CommFeeUSD"] + df["NfaFeeUSD"]

    try:
        data = [tuple(row) for row in df.values]
        with MySQLDB(DB_CONFIG) as db:
            db.executemany(insert_query, data)

        file_date = extract_file_date(filename) or datetime.today().date()
        mark_as_loaded(filename, file_date, config["log_table"])

        logging.info(f"  ✓ Loaded {len(data)} records from {filename}")
        return True

    except Exception as e:
        logging.error(f"  ✗ DB error loading {filename}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False