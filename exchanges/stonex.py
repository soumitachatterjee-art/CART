import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from db import MySQLDB
# At the top of stonex.py
from cores.utils import extract_file_date, mark_as_loaded
# =============================================================================
# CONFIG
# =============================================================================

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Axxela@123",
    "database": "cart",
    "port": 3306
}

EXCHANGE_CONFIG = {
    "exchange_name": "StonEx",
    "bucket": "axxela-s3-pub-ds1-mumbai",
    "download_folder": os.path.join("downloads", "StonEx"),
    "file_patterns": [
        {
            "filepath_template": "StoneX/{YYYY}/{MM}/{DD}/",
            "filename_template": "Trades{YYYYMMDD}.csv",
        }
    ],
    "excluded_accounts": ["EE066", "EE067", "EE068", "EE069", "EE070"],
    "target_table": "stonex_trades_march",
    "log_table":    "file_load_log_stonex",
    "ctrcode_mapping": {
        ("c",   "c")    : ("1",  "c"),
        ("b",   "lco")  : ("7",  "bz"),
        ("lo2", "lo2")  : ("7",  "qk"),
        ("lo3", "lo3")  : ("7",  "qn"),
        ("ng",  "nc")   : ("7",  "ng"),
        ("cl",  "nco")  : ("7",  "cu"),
        ("rb",  "nyrb") : ("7",  "gb"),
        ("s",   "csb")  : ("1",  "s-"),
        ("sgu",   "sguu")  : ("4",  "am"),
        ("dbi",   "idbi")  : ("19",  "db"),
        ("w", "cwh")  : ("14",  "lu"),
        ("07", "cso")  : ("1",  "7"),
        ("06", "csm")  : ("1",  "6"),
        ("48", "clc")  : ("2",  "48"),
        ("62", "cfc2") : ("2",  "62"),
        ("sr3", "sr3")  : ("9c",  "cm"),
        ("bz", "nybz")  : ("7",  "bz"),
        ("br", "xbr")  : ("16",  "br"),
        ("rs", "wcaj")  : ("6",  "rs"),
        ("pbd", "pbd") : ("12",  "ld"),
        ("gc", "xgd")   : ("4",  "37"),
        ("tfm", "ittf")  : ("7g",  "cf"),
        ("hg", "xch")  : ("4",  "hg"),
        ("zsd", "zsd")  : ("12",  "l8"),
        ("cad", "cad")  : ("12",  "cp"),
        ("ahd", "ahd")  : ("12",  "au"),
        ("snd", "snd") : ("12","l7"),
        ("si", "xag") : ("4", "39"),
        ("ln", "cllh") : ("2", "ln"),
        ("kw", "kwh") : ("1", "kw"),
        ("eco", "erap") : ("25", "rg"),
        ("kc", "ncfc") : ("6", "43"),
        ("rc", "robc") : ("14", "rc"),
        ("sb", "nsu") : ("6", "27"),
        ("adm", "iadm") : ("6d", "ad"),
         ("mhg", "mhgm") : ("4", "b4"),
         ("sil", "csil") : ("4", "s<"),
         ("lo", "nco") : ("7", "cu"),
         ("mcl", "nmcl") : ("7", "ef"),
         ("t", "wbs") : ("19", "cs"),
        ("ebm", "eb2") : ("25", "b3")

    },
}


# =============================================================================
# TRANSFORM
# =============================================================================

def step_transform(file_path, config):

    # Try old format first, then new format
    old_columns = {
        "Exchange Code": "Exchange Code",
        "Instrument Code": "Instrument Code", 
        "Currency": "Currency",
        "Net Quantity": "Net Quantity",
        "Commission": "Commission",
        "Exchange Fee": "Exchange Fee",
        "Clearing Fee": "Clearing Fee",
        "NFA Fee": "NFA Fee",
    }
    new_columns = {
        "Exchange Code": "Exchange Id",
        "Instrument Code": "Product Code",
        "Currency": "Product Currency",
        "Net Quantity": "Quantity",
        "Commission": "Commission Charge",
        "Exchange Fee": "Exchange Fees Only",
        "Clearing Fee": "Clearing Fees Only",
        "NFA Fee": "NFA Fees Only",
    }

    try:
        # Detect which format by reading headers
        raw = pd.read_csv(file_path, nrows=0)
        actual_cols = raw.columns.tolist()

        if "Exchange Code" in actual_cols:
            col_map = old_columns
            usecols = ["Trade Date", "Account Id", "Option Style"] + list(old_columns.values())
        else:
            col_map = new_columns
            usecols = ["Trade Date", "Account Id", "Option Style"] + list(new_columns.values())

        df = pd.read_csv(file_path, usecols=usecols)

        # Normalize to standard names
        reverse_map = {v: k for k, v in col_map.items()}
        df = df.rename(columns=reverse_map)

    except Exception as e:
        logging.error(f"  Could not read {file_path}: {e}")
        return None

    logging.info(f"  Read {len(df)} raw records")

    # --- rest of your transform continues unchanged from here ---
    df["Trade Date"] = pd.to_datetime(df["Trade Date"], errors="coerce")
    df = df[~df["Account Id"].isin(config["excluded_accounts"])].copy()

    if df.empty:
        logging.warning(f"  No records remain after account filter")
        return None

    logging.info(f"  {len(df)} records after account filter")

    for col in ["Commission", "Exchange Fee", "Clearing Fee", "NFA Fee"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Net Quantity"] = pd.to_numeric(df["Net Quantity"], errors="coerce").fillna(0)

    df["_exch_key"] = df["Exchange Code"].astype(str).str.strip().str.lower()
    df["_inst_key"]  = df["Instrument Code"].astype(str).str.strip().str.lower()

    ctrcode_map    = config["ctrcode_mapping"]
    already_warned = set()

    def build_ctrcode(row):
        key    = (row["_exch_key"], row["_inst_key"])
        mapped = ctrcode_map.get(key)

        if mapped is None:
            if key not in already_warned:
                already_warned.add(key)
                msg = (
                    f"\n⚠️  UNMAPPED CODE FOUND — please add to ctrcode_mapping in exchanges/stonex.py:\n"
                    f"    Exchange Code  : '{row['Exchange Code']}'\n"
                    f"    Instrument Code: '{row['Instrument Code']}'\n"
                    f"    Add this line  : "
                    f"(\"{row['_exch_key']}\", \"{row['_inst_key']}\") : (\"??\", \"??\"),\n"
                )
                logging.warning(msg)
                print(msg)
            exch_part = row["Exchange Code"]
            cont_part  = row["Instrument Code"]
        else:
            exch_part, cont_part = mapped

        opt_style = str(row.get("Option Style", "")).strip()
        prefix    = "OPT" if opt_style and opt_style.lower() not in ("", "nan", "none") else "FUT"

        return f"{prefix}-{exch_part}-{cont_part}"

    df["CtrCode"] = df.apply(build_ctrcode, axis=1)

    group_cols = ["Trade Date", "Account Id", "Currency", "CtrCode"]
    agg_dict   = {
        "Commission":   "sum",
        "Exchange Fee": "sum",
        "Clearing Fee": "sum",
        "NFA Fee":      "sum",
        "Net Quantity": "sum",
    }

    df_grouped = df.groupby(group_cols).agg(agg_dict).reset_index()

    df_final = pd.DataFrame({
        "Trade_date": df_grouped["Trade Date"],
        "ClientID":   df_grouped["Account Id"],
        "CtrCode":    df_grouped["CtrCode"],
        "mktCur":     df_grouped["Currency"],
        "ExFee":      df_grouped["Exchange Fee"] + df_grouped["Clearing Fee"],
        "CommCur":    df_grouped["Currency"],
        "CommFee":    df_grouped["Commission"],
        "NfaFeeCur":  df_grouped["Currency"],
        "NfaFee":     df_grouped["NFA Fee"],
        "Qty":        df_grouped["Net Quantity"].astype(int),
        "ExFeeUSD":   None,
        "CommFeeUSD": None,
        "NfaFeeUSD":  None,
    })

    df_final = df_final.sort_values(["Trade_date", "ClientID", "CtrCode"])
    logging.info(f"  Transform complete: {len(df_final)} records ready")
    return df_final

# =============================================================================
# LOAD
# Chunked inserts — avoids timeout on large files (21MB+).
# chunk_size of 500 rows per batch keeps transactions small and fast.
# =============================================================================

def step_load(df, filename, config):
    if df is None or df.empty:
        logging.warning(f"  Nothing to load for {filename}, skipping.")
        return False

    insert_query = f"""
        INSERT INTO {config['target_table']}
            (Trade_date, ClientID, CtrCode,
             mktCur, ExFee,
             CommCur, CommFee,
             NfaFeeCur, NfaFee,
             Qty,
             ExFeeUSD, CommFeeUSD, NfaFeeUSD)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            mktCur     = VALUES(mktCur),
            ExFee      = ExFee      + VALUES(ExFee),
            CommCur    = VALUES(CommCur),
            CommFee    = CommFee    + VALUES(CommFee),
            NfaFeeCur  = VALUES(NfaFeeCur),
            NfaFee     = NfaFee     + VALUES(NfaFee),
            Qty        = Qty        + VALUES(Qty),
            ExFeeUSD   = NULL,
            CommFeeUSD = NULL,
            NfaFeeUSD  = NULL
    """

    try:
        data       = [tuple(row) for row in df.values]
        total      = len(data)
        chunk_size = 500

        with MySQLDB(DB_CONFIG) as db:
            for i in range(0, total, chunk_size):
                chunk = data[i : i + chunk_size]
                db.executemany(insert_query, chunk)
                logging.info(f"  Inserted rows {i+1}–{min(i+chunk_size, total)} of {total}")

        file_date = extract_file_date(filename) or datetime.today().date()
        mark_as_loaded(filename, file_date, config["log_table"])

        # ← NEW: run FX conversion
        step_convert_to_usd(config["target_table"], file_date)

        logging.info(f"  ✓ Loaded {total} records from {filename}")
        return True

    except Exception as e:
        logging.error(f"  ✗ DB error loading {filename}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False



def step_convert_to_usd(target_table, trade_date):
    """Converts ExFee, CommFee, NfaFee to USD using spotrate table."""
    try:
        with MySQLDB(DB_CONFIG) as db:

            # USD rows — direct copy (no date filter)
            db.execute(f"""
                UPDATE {target_table}
                SET ExFeeUSD   = ExFee,
                    CommFeeUSD = CommFee,
                    NfaFeeUSD  = NfaFee
                WHERE CommCur = 'USD'
                  AND CommFeeUSD IS NULL
            """)

            # Non-USD rows — convert using closest rate to trade_date
            db.execute(f"""
                UPDATE {target_table} t
                JOIN spotrate s
                  ON s.currency = t.CommCur
                 AND s.date = (
                     SELECT MAX(date) FROM spotrate
                     WHERE currency = t.CommCur
                       AND date <= %s
                 )
                SET t.ExFeeUSD   = t.ExFee   / s.rate,
                    t.CommFeeUSD = t.CommFee / s.rate,
                    t.NfaFeeUSD  = t.NfaFee  / s.rate
                WHERE t.CommCur != 'USD'
                  AND t.CommFeeUSD IS NULL
            """, (trade_date,))

            # Check remaining nulls
            db.execute(f"""
                SELECT DISTINCT CommCur,
                       SUM(CASE WHEN CommFeeUSD IS NULL THEN 1 ELSE 0 END) as still_null
                FROM {target_table}
                GROUP BY CommCur
                HAVING still_null > 0
            """)

            missing = db.fetchall()
            if missing:
                logging.warning(f"  ⚠ Still NULL after conversion for {trade_date}: {missing}")
            else:
                logging.info(f"  ✓ USD conversion complete for {trade_date}")

    except Exception as e:
        logging.error(f"FX conversion error for {target_table} on {trade_date}: {e}")