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
            "filepath_template": "MarexUS/{YYYY}/{MM}/{DD}/",
            "filename_template": "Marex Trades and Fees_{YYYYMMDD}.csv",
        }
    ],
    "ref_bucket": "bi-cr3-prod",
    "ref_filepath_template": "SYM/{YYYY}/{MM}/{DD}/",
    "ref_filename_template": "TRD_SYM_{YYYYMMDD}.csv",
    "target_table": "maexus_trades_new",
    "log_table":     "file_load_log_maexus_new",
    "credentials_file":     "aws_credentials.json",
    "ref_credentials_file": "aws_credentials_sym.json",
}


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
        if not match: return None
        
        YYYYMMDD = match.group(1)
        YYYY, MM, DD = YYYYMMDD[:4], YYYYMMDD[4:6], YYYYMMDD[6:8]

        folder = config["ref_filepath_template"].format(YYYY=YYYY, MM=MM, DD=DD).strip("/")
        name = config["ref_filename_template"].format(YYYYMMDD=YYYYMMDD)
        s3_key = f"{folder}/{name}"
        
        local_ref_dir = os.path.join("downloads", "Maexus", "ref")
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
        # DEBUG PRINT: If you don't see this in your terminal, the pipeline isn't reaching here.
        print(f"\n🚀 TRANSFORM TRIGGERED for: {os.path.basename(file_path)}")

        # 1. FIND HEADER & LOAD
        temp_df = pd.read_csv(file_path, nrows=10, header=None)
        header_index = 0
        for i, row in temp_df.iterrows():
            row_str = " ".join(row.astype(str))
            if any(k in row_str for k in ["Transaction ID", "Instrument Code", "Trade Date"]):
                header_index = i
                break

        df = pd.read_csv(file_path, skiprows=header_index, low_memory=False)
        df.columns = [re.sub(r'[^\x20-\x7E]+', '', str(c)).strip() for c in df.columns]

        # 2. REFERENCE LOOKUP (Added explicit region for Mumbai)
        ref_creds = config.get("ref_credentials_file", "aws_credentials_sym.json")
        ref_key, ref_sec = load_credentials(ref_creds)
        ref_s3 = boto3.client(
            "s3", 
            region_name="ap-south-1", 
            aws_access_key_id=ref_key, 
            aws_secret_access_key=ref_sec
        )
        
        ref_path = _download_ref_file(file_path, config, ref_s3)
        if not ref_path: return None

        ref_df = pd.read_csv(ref_path, low_memory=False)
        ref_df.columns = [str(c).strip().lower() for c in ref_df.columns]
        ref_lookup = ref_df[["contractcode", "exchangecode"]].dropna().copy()
        ref_lookup["contractcode"] = ref_lookup["contractcode"].astype(str).str.upper().str.strip()
        ref_lookup = ref_lookup.drop_duplicates(subset="contractcode")

        # 3. IDENTIFY COLUMNS
        s_col = next((c for c in ["Underlying Symbol", "Symbol Merged", "Instrument Code"] if c in df.columns), None)
        c_col = next((c for c in ["Product Class", "Product Type"] if c in df.columns), None)
        
        # 4. EXCHANGE MAPPING
        df["_join_key"] = df[s_col].astype(str).str.upper().str.strip()
        df = df.merge(ref_lookup, left_on="_join_key", right_on="contractcode", how="left")
        
        still_missing = df["exchangecode"].isna()
        df.loc[still_missing, "exchangecode"] = df.loc[still_missing, "_join_key"].map(STATIC_EXCHANGE_MAP)
        if "Exchange" in df.columns:
            df["exchangecode"] = df["exchangecode"].fillna(df["Exchange"])
        df["exchangecode_mapped"] = df["exchangecode"].fillna("MISSING")

        # 5. CLEAN NUMERICS & DATES
        def clean_val(series):
            return pd.to_numeric(series.astype(str).str.replace(r'[$,\s]', '', regex=True), errors='coerce').fillna(0)

        raw_curr = df.get('Currency Trade ISO Code', 'USD')
        if isinstance(raw_curr, pd.Series): raw_curr = raw_curr.fillna('USD').iloc[0]

        # --- IMPORTANT: CREATE THESE COLUMNS NOW ---
        df['Trade_date'] = pd.to_datetime(df['Trade Date'], errors='coerce').dt.date
        
        # Look for the Account column safely
        acc_col = 'Account Short Name' if 'Account Short Name' in df.columns else None
        if acc_col:
            df['ClientID'] = df[acc_col].astype(str).str.strip()
        else:
            df['ClientID'] = 'UNKNOWN'
        
        # Construct CtrCode
        prefix = df[c_col].apply(build_ctr_prefix) + "-" if c_col else ""
        df["CtrCode"] = (prefix + df[s_col].astype(str).str.upper().str.strip() + 
                 "-" + df["exchangecode_mapped"].astype(str)).str.strip().str.upper()

        # Financials
        df['FirstMoneyAmount'] = clean_val(df["First Money Amount"])
        df['CommissionAmount'] = clean_val(df["Commission Amount"])
        df['SecFeeAmount']     = clean_val(df["SEC Fee Amount"])
        df['OrfAmount']        = clean_val(df["ORF Amount"])
        df['Quantity']         = pd.to_numeric(df.get('Quantity', 0), errors='coerce').fillna(0)
        df['TotalUSD']         = df['CommissionAmount'] + df['FirstMoneyAmount'] + df['SecFeeAmount'] + df['OrfAmount']
        
        # 6. GROUPING & SUMMING
        df['ProductType']  = df['Product Class'].astype(str).str.strip() if 'Product Class' in df.columns else 'UNKNOWN'
        df['SymbolMerged'] = df.get('Symbol Merged', 'UNKNOWN').astype(str).str.strip()

        # ENSURE ALL THESE KEYS EXIS
        def deep_clean(series):
            return series.astype(str).str.replace(r'[^\w\-]', '', regex=True).str.upper().str.strip()

        df['ClientID'] = deep_clean(df['ClientID'])

# 2. FORCE DATE IDENTITY
        df['Trade_date'] = pd.to_datetime(df['Trade Date'], errors='coerce').dt.date

# 3. RE-BUILD CtrCode WITH CLEANED COMPONENTS
# Ensure the components are cleaned BEFORE they are concatenated
        cleaned_symbol = deep_clean(df[s_col])
        cleaned_exch   = deep_clean(df["exchangecode_mapped"])
        prefix         = df[c_col].apply(build_ctr_prefix).astype(str).str.upper().str.strip() + "-" if c_col else ""

        df["CtrCode"] = prefix + cleaned_symbol + "-" + cleaned_exch

# 4. THE GROUPBY - Absolute strictness on the 3 Primary Key columns
        group_keys = ["Trade_date", "CtrCode", "ClientID"]
        test_dupes = df[df.duplicated(subset=group_keys, keep=False)]
        if not test_dupes.empty and "NFLX" in str(test_dupes["CtrCode"]):
            print("\n‼️ DEBUG: Found the NFLX duplicates in DataFrame:")
    # Printing columns to see what is different (e.g. ProductType or a hidden col)
            print(test_dupes[group_keys + ["ProductType", "Quantity", "TotalUSD"]].to_string())

        agg_targets = {
    "FirstMoneyAmount": "sum",
    "CommissionAmount": "sum",
    "SecFeeAmount": "sum",
    "OrfAmount": "sum",
    "TotalUSD": "sum",
    "Quantity": "sum",
    "ProductType": "first", 
    "SymbolMerged": "first"
}

        df_grouped = df.groupby(group_keys).agg(agg_targets).reset_index()

        df_grouped['FirstMoney_Curr'] = raw_curr
        df_grouped['Commission_Curr'] = raw_curr
        df_grouped['SecFee_Curr']     = raw_curr
        df_grouped['Orf_Curr']        = raw_curr

        # 7. FINAL OUTPUT (15 Columns)
        df_out = df_grouped[[
            "Trade_date", "CtrCode", "ClientID", "ProductType",
            "FirstMoneyAmount", "FirstMoney_Curr",
            "CommissionAmount", "Commission_Curr",
            "SecFeeAmount", "SecFee_Curr",
            "OrfAmount", "Orf_Curr",
            "TotalUSD", "Quantity", "SymbolMerged"
        ]].copy()

        print(f"✅ Transform complete — Aggregated {len(df)} raw rows into {len(df_out)} unique records.")
        return df_out

    except Exception as e:
        import traceback
        traceback.print_exc()
        return None

# =============================================================================
# LOAD
# =============================================================================

def step_load(df, filename, config):
    if df is None or df.empty: return False

    table = config['target_table']
    print(f"   Loading {len(df)} aggregated rows into {table}...")

    data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # We use ON DUPLICATE KEY UPDATE to ADD the new values to the existing values
    # We alias the incoming data as 'new' to reference it in the update clause
    base_query = f"""
        INSERT INTO {table}
            (Trade_date, CtrCode, ClientID, ProductType,
             FirstMoneyAmount, FirstMoney_Curr,
             CommissionAmount, Commission_Curr,
             SecFeeAmount, SecFee_Curr,
             OrfAmount, Orf_Curr,
             TotalUSD, Quantity, SymbolMerged)
        VALUES
        %s 
        AS new
        ON DUPLICATE KEY UPDATE
            FirstMoneyAmount = {table}.FirstMoneyAmount + new.FirstMoneyAmount,
            CommissionAmount = {table}.CommissionAmount + new.CommissionAmount,
            SecFeeAmount     = {table}.SecFeeAmount + new.SecFeeAmount,
            OrfAmount        = {table}.OrfAmount + new.OrfAmount,
            TotalUSD         = {table}.TotalUSD + new.TotalUSD,
            Quantity         = {table}.Quantity + new.Quantity,
            ProductType      = new.ProductType,
            SymbolMerged     = new.SymbolMerged;
    """

    try:
        with MySQLDB(DB_CONFIG) as db:
            for i in range(0, len(data_tuples), 5000):
                chunk = data_tuples[i : i + 5000]
                # Create the value strings: (%s, %s...)
                placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(chunk))
                
                # We format the base_query to put the placeholders where '%s' is
                final_sql = base_query % placeholders
                
                flattened = [item for row in chunk for item in row]
                db.execute(final_sql, flattened)
            db.execute("COMMIT;")

        mark_as_loaded(filename, extract_file_date(filename), config["log_table"])
        return True
    except Exception as e:
        print(f"❌ DB load error: {e}")
        return False