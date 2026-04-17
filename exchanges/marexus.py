# exchanges/maexus.py

import os
import re
import boto3
import logging
import pandas as pd

from db import MySQLDB
from cores.utils import DB_CONFIG, mark_as_loaded, extract_file_date, load_credentials


# =============================================================================
# CONFIG
# =============================================================================

EXCHANGE_CONFIG = {
    "exchange_name": "Maexus",
    "bucket": "axxela-s3-pub-ds1-mumbai",
    "download_folder": os.path.join("downloads", "Maexus"),
    "file_patterns": [
        {
            # Clean literal path confirmed by your S3 crawl
            "filepath_template": "MarexUS/{YYYY}/{MM}/{DD}/",
            "filename_template": "Marex Trades and Fees_{YYYYMMDD}.csv",
        }
    ],
    "ref_bucket": "bi-cr3-prod",
    "ref_filepath_template": "SYM/{YYYY}/{MM}/{DD}/",
    "ref_filename_template": "TRD_SYM_{YYYYMMDD}.csv",
    "target_table": "maexus_trades_march",
    "log_table":     "file_load_log_maexus",
    "credentials_file":     "aws_credentials.json",
    "ref_credentials_file": "aws_credentials_sym.json",
}

# =============================================================================
# STATIC FALLBACK MAP
# Built from actual TRD_SYM data — 76 unique tickers.
# Priority 2 in the fallback chain (after TRD_SYM daily ref file).
# If a new ticker appears in the "Still unmatched" debug output, add it here.
# =============================================================================

STATIC_EXCHANGE_MAP = {
    # NASDAQ
    "AAPL":  "XNAS",
    "ABCL":  "XNAS",
    "AMZN":  "XNAS",
    "ARDX":  "XNAS",
    "AVGO":  "XNAS",
    "BMNR":  "XNAS",
    "CMCSA": "XNAS",
    "CTAS":  "XNAS",
    "DASH":  "XNAS",
    "DXCM":  "XNAS",
    "EA":    "XNAS",
    "GEHC":  "XNAS",
    "GLNK":  "XNAS",
    "GOOG":  "XNAS",
    "GOOGL": "XNAS",
    "GRAB":  "XNAS",
    "GRMN":  "XNAS",
    "HIVE":  "XNAS",
    "IPST":  "XNAS",
    "META":  "XNAS",
    "MSFT":  "XNAS",
    "MSTR":  "XNAS",
    "NBIS":  "XNAS",
    "NFLX":  "XNAS",
    "NVTS":  "XNAS",
    "PANW":  "XNAS",
    "PURR":  "XNAS",
    "ROOT":  "XNAS",
    "STSS":  "XNAS",
    "TEM":   "XNAS",
    "TRMB":  "XNAS",
    "TSLA":  "XNAS",
    "VFS":   "XNAS",
    "VRSK":  "XNAS",
    # NYSE
    "AEP":   "XNYS",
    "AKAM":  "XNYS",
    "AMC":   "XNYS",
    "AMP":   "XNYS",
    "ARE":   "XNYS",
    "BABA":  "XNYS",
    "CLS":   "XNYS",
    "CRL":   "XNYS",
    "CTVA":  "XNYS",
    "CVS":   "XNYS",
    "DD":    "XNYS",
    "DE":    "XNYS",
    "DOCN":  "XNYS",
    "DOW":   "XNYS",
    "EMN":   "XNYS",
    "FIS":   "XNYS",
    "FOX":   "XNYS",
    "HCA":   "XNYS",
    "HIG":   "XNYS",
    "HST":   "XNYS",
    "IHS":   "XNYS",
    "ITW":   "XNYS",
    "KMI":   "XNYS",
    "KMX":   "XNYS",
    "LDOS":  "XNYS",
    "LW":    "XNYS",
    "MDT":   "XNYS",
    "MOS":   "XNYS",
    "NOC":   "XNYS",
    "NWS":   "XNYS",
    "OGN":   "XNYS",
    "ORLY":  "XNYS",
    "PEP":   "XNYS",
    "SRE":   "XNYS",
    "SYK":   "XNYS",
    "TKO":   "XNYS",
    "TROW":  "XNYS",
    "TSCO":  "XNYS",
    "TXN":   "XNYS",
    "UHS":   "XNYS",
    "XPEV":  "XNYS",
    # CBOE
    "CBOE":  "XCBO",
}


# =============================================================================
# HELPERS
# =============================================================================

def build_ctr_prefix(product_class: str) -> str:
    return str(product_class).strip()[:3].upper()


def _download_ref_file(file_path: str, config: dict, s3_client) -> str | None:
    try:
        filename = os.path.basename(file_path)
        match = re.search(r'(\d{8})', filename)
        if not match:
            return None
        YYYYMMDD = match.group(1)
        YYYY, MM, DD = YYYYMMDD[:4], YYYYMMDD[4:6], YYYYMMDD[6:8]

        folder         = config["ref_filepath_template"].format(YYYY=YYYY, MM=MM, DD=DD).strip("/")
        name           = config["ref_filename_template"].format(YYYYMMDD=YYYYMMDD)
        s3_key         = f"{folder}/{name}"
        local_ref_dir  = os.path.join("downloads", "Maexus", "ref")
        os.makedirs(local_ref_dir, exist_ok=True)
        local_ref_path = os.path.join(local_ref_dir, name)

        print(f"   -> S3: Downloading reference {s3_key}...")
        s3_client.download_file(config["ref_bucket"], s3_key, local_ref_path)
        return local_ref_path

    except Exception as e:
        print(f"   -> S3 ERROR downloading ref file: {e}")
        return None


# =============================================================================
# TRANSFORM
# =============================================================================

def step_transform(file_path: str, config: dict):
    try:
        print(f"\n--- Starting Transform for {os.path.basename(file_path)} ---")

        # ------------------------------------------------------------------
        # 1. DYNAMICALLY FIND THE HEADER ROW
        # ------------------------------------------------------------------
        temp_df = pd.read_csv(file_path, nrows=10, header=None)
        header_index = 0
        for i, row in temp_df.iterrows():
            row_str = " ".join(row.astype(str))
            if any(k in row_str for k in ["Transaction ID", "Instrument Code", "Trade Date"]):
                header_index = i
                break

        df = pd.read_csv(file_path, skiprows=header_index, low_memory=False)

        # ------------------------------------------------------------------
        # 2. CLEAN HEADERS
        # ------------------------------------------------------------------
        df.columns = [re.sub(r'[^\x20-\x7E]+', '', str(c)).strip() for c in df.columns]

        # ------------------------------------------------------------------
        # 3. LOAD REFERENCE FILE FROM S3
        # ------------------------------------------------------------------
        ref_creds = config.get("ref_credentials_file", "aws_credentials_sym.json")
        ref_key, ref_sec = load_credentials(ref_creds)
        ref_s3 = boto3.client(
            "s3",
            aws_access_key_id=ref_key,
            aws_secret_access_key=ref_sec
        )
        ref_path = _download_ref_file(file_path, config, ref_s3)
        if not ref_path:
            print("❌ Could not download reference file — aborting transform.")
            return None

        ref_df = pd.read_csv(ref_path, low_memory=False)
        ref_df.columns = [str(c).strip().lower() for c in ref_df.columns]

        # ------------------------------------------------------------------
        # 4. BUILD LOOKUP: contractcode → exchangecode (from TRD_SYM)
        # ------------------------------------------------------------------
        if "contractcode" not in ref_df.columns or "exchangecode" not in ref_df.columns:
            print(f"❌ Ref file missing required columns. Found: {list(ref_df.columns)}")
            return None

        ref_lookup = (
            ref_df[["contractcode", "exchangecode"]]
            .dropna(subset=["contractcode", "exchangecode"])
            .copy()
        )
        ref_lookup["contractcode"] = (
            ref_lookup["contractcode"].astype(str).str.upper().str.strip()
        )
        ref_lookup = ref_lookup.drop_duplicates(subset="contractcode")
        print(f"   Ref lookup built: {len(ref_lookup)} unique contractcode entries")

        # ------------------------------------------------------------------
        # 5. IDENTIFY KEY COLUMNS IN TRADE FILE
        # ------------------------------------------------------------------
        s_col = next(
            (c for c in ["Underlying Symbol", "Symbol Merged", "Instrument Code"] if c in df.columns),
            None
        )
        c_col = next(
            (c for c in ["Product Class", "Product Type"] if c in df.columns),
            None
        )

        if s_col is None:
            print(f"❌ No symbol column found. Available columns: {list(df.columns)}")
            return None

        # ------------------------------------------------------------------
        # 6. NORMALIZE TRADE SYMBOL FOR JOINING
        # ------------------------------------------------------------------
        df["_join_key"] = df[s_col].astype(str).str.upper().str.strip()

        # ------------------------------------------------------------------
        # 7. FALLBACK CHAIN
        #    Priority 1 — TRD_SYM ref file  (daily, most accurate)
        #    Priority 2 — STATIC_EXCHANGE_MAP (symbols absent from ref some days)
        #    Priority 3 — 'Exchange' column in trade file
        #    Priority 4 — 'MISSING'
        # ------------------------------------------------------------------

        # Priority 1: merge against TRD_SYM
        df = df.merge(
            ref_lookup,
            left_on="_join_key",
            right_on="contractcode",
            how="left"
        )

        # Priority 2: static map
        still_missing = df["exchangecode"].isna()
        df.loc[still_missing, "exchangecode"] = (
            df.loc[still_missing, "_join_key"].map(STATIC_EXCHANGE_MAP)
        )

        # Priority 3: 'Exchange' column from trade file
        if "Exchange" in df.columns:
            df["exchangecode"] = df["exchangecode"].fillna(df["Exchange"])

        # Priority 4: mark remaining as MISSING
        df["exchangecode_mapped"] = df["exchangecode"].fillna("MISSING")
        df.drop(columns=["_join_key", "contractcode", "exchangecode"], errors="ignore", inplace=True)

        # Summary
        missing_count = (df["exchangecode_mapped"] == "MISSING").sum()
        matched_count = len(df) - missing_count
        print(f"   Mapping result: {matched_count} matched, {missing_count} unmatched")
        if missing_count > 0:
            missed = df.loc[df["exchangecode_mapped"] == "MISSING", s_col].unique()[:20]
            print(f"   Still unmatched (add to STATIC_EXCHANGE_MAP): {missed}")

        # ------------------------------------------------------------------
        # 8. FEE CALCULATIONS
        # ------------------------------------------------------------------
        def clean_val(series):
            return (
                pd.to_numeric(
                    series.astype(str).str.replace(r'[$,\s]', '', regex=True),
                    errors='coerce'
                ).fillna(0)
            )

        raw_curr = df.get('Currency Trade ISO Code', 'USD')
        if isinstance(raw_curr, pd.Series):
            raw_curr = raw_curr.fillna('USD').iloc[0]

        df['FirstMoneyAmount'] = clean_val(df["First Money Amount"])  if "First Money Amount"  in df.columns else 0
        df['FirstMoney_Curr']  = raw_curr
        df['CommissionAmount'] = clean_val(df["Commission Amount"])   if "Commission Amount"   in df.columns else 0
        df['Commission_Curr']  = raw_curr
        df['SecFeeAmount']     = clean_val(df["SEC Fee Amount"])      if "SEC Fee Amount"      in df.columns else 0
        df['SecFee_Curr']      = raw_curr
        df['OrfAmount']        = clean_val(df["ORF Amount"])          if "ORF Amount"          in df.columns else 0
        df['Orf_Curr']         = raw_curr
        df['TotalUSD']         = (
            df['CommissionAmount'] + df['FirstMoneyAmount'] +
            df['SecFeeAmount']     + df['OrfAmount']
        )
        df['Quantity'] = pd.to_numeric(df.get('Quantity', 0), errors='coerce').fillna(0)

        # ------------------------------------------------------------------
        # 9. TRADE DATE
        # ------------------------------------------------------------------
        df['Trade_date'] = pd.to_datetime(
            df['Trade Date'], errors='coerce'
        ).dt.date if 'Trade Date' in df.columns else None

        # ------------------------------------------------------------------
        # 10. BUILD CtrCode + DISPLAY COLUMNS
        # ------------------------------------------------------------------
        if c_col:
            df["CtrCode"] = (
                df[c_col].apply(build_ctr_prefix) + "-" +
                df[s_col].astype(str).str.upper().str.strip() + "-" +
                df["exchangecode_mapped"].astype(str)
            )
        else:
            df["CtrCode"] = (
                df[s_col].astype(str).str.upper().str.strip() + "-" +
                df["exchangecode_mapped"].astype(str)
            )

        # Product Class → used as ProductType in DB
        df['ProductType']    = df['Product Class'].astype(str).str.strip()  if 'Product Class'    in df.columns else 'UNKNOWN'
        df['ProductSubType'] = df['Product Sub Type'].astype(str).str.strip() if 'Product Sub Type' in df.columns else 'UNKNOWN'

        df['AccountShortName'] = df.get('Account Short Name', pd.Series('UNKNOWN', index=df.index)).astype(str).str.strip()
        df['SymbolMerged']     = df.get('Symbol Merged',      pd.Series('UNKNOWN', index=df.index)).astype(str).str.strip()
        df['ProductName']      = df.get('Product Name',       pd.Series('UNKNOWN', index=df.index)).astype(str).str.strip()

        # ------------------------------------------------------------------
        # 11. FINAL OUTPUT (17 columns)
        # ------------------------------------------------------------------
        df_out = df[[
            "Trade_date",
            "CtrCode",
            "ProductType",
            "ProductSubType",
            "FirstMoneyAmount", "FirstMoney_Curr",
            "CommissionAmount", "Commission_Curr",
            "SecFeeAmount",     "SecFee_Curr",
            "OrfAmount",        "Orf_Curr",
            "TotalUSD",         "Quantity",
            "AccountShortName", "SymbolMerged", "ProductName",
        ]].copy()

        print(f"✅ Transform complete — rows: {len(df_out)}, MISSING exchange: {missing_count}")
        return df_out

    except Exception as e:
        import traceback
        print(f"❌ Transform error: {e}")
        traceback.print_exc()
        return None


# =============================================================================
# LOAD
# =============================================================================

def step_load(df, filename, config):
    if df is None or df.empty:
        return False

    table = config['target_table']
    print(f"   Loading {len(df)} rows into {table}...")

    data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
    chunk_size  = 5000

    base_query = f"""
        INSERT INTO {table}
            (Trade_date,
             CtrCode,
             ProductType,
             ProductSubType,
             FirstMoneyAmount, FirstMoney_Curr,
             CommissionAmount, Commission_Curr,
             SecFeeAmount,     SecFee_Curr,
             OrfAmount,        Orf_Curr,
             TotalUSD,         Quantity,
             AccountShortName, SymbolMerged,  ProductName)
        VALUES
    """

    try:
        with MySQLDB(DB_CONFIG) as db:
            for i in range(0, len(data_tuples), chunk_size):
                chunk        = data_tuples[i : i + chunk_size]
                placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(chunk))
                flattened    = [item for row in chunk for item in row]
                db.execute(base_query + placeholders, flattened)
            db.execute("COMMIT;")

        mark_as_loaded(filename, extract_file_date(filename), config["log_table"])
        print(f"✅ Loaded {filename} successfully.")
        return True

    except Exception as e:
        print(f"❌ DB load error: {e}")
        import traceback
        traceback.print_exc()
        return False