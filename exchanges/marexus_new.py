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

        # 2. REFERENCE LOOKUP
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

        df['Trade_date'] = pd.to_datetime(df['Trade Date'], format='mixed',errors='coerce').dt.date

        acc_col = 'Account Short Name' if 'Account Short Name' in df.columns else None
        df['ClientID'] = df[acc_col].astype(str).str.strip() if acc_col else 'UNKNOWN'

        # Construct CtrCode
        prefix = df[c_col].apply(build_ctr_prefix) + "-" if c_col else ""
        df["CtrCode"] = (prefix + df[s_col].astype(str).str.upper().str.strip() +
                         "-" + df["exchangecode_mapped"].astype(str)).str.strip().str.upper()

        # Financials — FirstMoney and SecFee removed, OCF added
        # Financials — split commission into components
        if "Commission Amount" in df.columns:
            df['CommissionAmount'] = clean_val(df["Commission Amount"])
            df['AwayTradeFee']     = pd.Series(0, index=df.index)
            df['ExecutionFee']     = pd.Series(0, index=df.index)
            df['TradingExpFee']    = pd.Series(0, index=df.index)
        else:
            df['AwayTradeFee']  = clean_val(df["Away Trade Fee"])      if "Away Trade Fee"      in df.columns else pd.Series(0, index=df.index)
            df['ExecutionFee']  = clean_val(df["Execution Fee"])        if "Execution Fee"       in df.columns else pd.Series(0, index=df.index)
            df['TradingExpFee'] = clean_val(df["Trading Expense Fee"])  if "Trading Expense Fee" in df.columns else pd.Series(0, index=df.index)
            df['CommissionAmount'] = df['AwayTradeFee'] + df['ExecutionFee'] + df['TradingExpFee']

        df['OrfAmount']    = clean_val(df["ORF Amount"])
        df['OcfAmount']    = clean_val(df["OCF Amount"])   if "OCF Amount"   in df.columns else pd.Series(0, index=df.index)
        df['SecFeeAmount'] = clean_val(df["SEC Fee Amount"]) if "SEC Fee Amount" in df.columns else pd.Series(0, index=df.index)
        df['Quantity']     = pd.to_numeric(df.get('Quantity', 0), errors='coerce').fillna(0)

        # TotalUSD: Options → Commission + ORF + OCF + SEC; all others → Commission only
        is_option = (
            df[c_col].astype(str).str.strip().str.lower() == "option"
            if c_col else pd.Series(False, index=df.index)
        )
        df['TotalUSD'] = df['CommissionAmount'].copy()
        df.loc[is_option, 'TotalUSD'] = (
            df.loc[is_option, 'CommissionAmount'] +
            df.loc[is_option, 'OrfAmount'] +
            df.loc[is_option, 'OcfAmount'] +
            df.loc[is_option, 'SecFeeAmount'] +
            df.loc[is_option, 'AwayTradeFee'] +
            df.loc[is_option, 'ExecutionFee'] +
            df.loc[is_option, 'TradingExpFee']
        )
        # 6. GROUPING & SUMMING
        df['ProductType']  = df['Product Class'].astype(str).str.strip() if 'Product Class' in df.columns else 'UNKNOWN'
        df['SymbolMerged'] = df.get('Symbol Merged', 'UNKNOWN').astype(str).str.strip()

        def deep_clean(series):
            return series.astype(str).str.replace(r'[^\w\-]', '', regex=True).str.upper().str.strip()

        df['ClientID']   = deep_clean(df['ClientID'])
        df['Trade_date'] = pd.to_datetime(df['Trade Date'], errors='coerce').dt.date

        cleaned_symbol = deep_clean(df[s_col])
        cleaned_exch   = deep_clean(df["exchangecode_mapped"])
        prefix         = df[c_col].apply(build_ctr_prefix).astype(str).str.upper().str.strip() + "-" if c_col else ""
        df["CtrCode"]  = prefix + cleaned_symbol + "-" + cleaned_exch

        group_keys = ["Trade_date", "CtrCode", "ClientID"]
        test_dupes = df[df.duplicated(subset=group_keys, keep=False)]
        if not test_dupes.empty and "NFLX" in str(test_dupes["CtrCode"]):
            print("\n‼️ DEBUG: Found the NFLX duplicates in DataFrame:")
            print(test_dupes[group_keys + ["ProductType", "Quantity", "TotalUSD"]].to_string())

        agg_targets = {
            "CommissionAmount": "sum",
            "AwayTradeFee":     "sum",   # ← new
            "ExecutionFee":     "sum",   # ← new
            "TradingExpFee":    "sum",   # ← new
            "OrfAmount":        "sum",
            "OcfAmount":        "sum",
            "SecFeeAmount":     "sum",
            "TotalUSD":         "sum",
            "Quantity":         "sum",
            "ProductType":      "first",
            "SymbolMerged":     "first",
        }

        df_grouped = df.groupby(group_keys).agg(agg_targets).reset_index()

        df_grouped['Commission_Curr'] = raw_curr
        df_grouped['Orf_Curr']        = raw_curr
        df_grouped['Ocf_Curr']        = raw_curr
        df_grouped['Sec_Curr']        = raw_curr

        # 7. FINAL OUTPUT (13 columns)
        df_out = df_grouped[[
            "Trade_date", "CtrCode", "ClientID", "ProductType",
            "CommissionAmount", "AwayTradeFee", "ExecutionFee", "TradingExpFee", "Commission_Curr",
            "OrfAmount",        "Orf_Curr",
            "OcfAmount",        "Ocf_Curr",
            "SecFeeAmount",     "Sec_Curr",
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

    base_query = f"""
    INSERT INTO {table}
        (Trade_date, CtrCode, ClientID, ProductType,
         CommissionAmount, AwayTradeFee, ExecutionFee, TradingExpFee, Commission_Curr,
         OrfAmount, Orf_Curr,
         OcfAmount, Ocf_Curr,
         SecFeeAmount, Sec_Curr,
         TotalUSD, Quantity, SymbolMerged)
    VALUES
    %s
    AS new
    ON DUPLICATE KEY UPDATE
        CommissionAmount = {table}.CommissionAmount + new.CommissionAmount,
        AwayTradeFee     = {table}.AwayTradeFee     + new.AwayTradeFee,
        ExecutionFee     = {table}.ExecutionFee     + new.ExecutionFee,
        TradingExpFee    = {table}.TradingExpFee    + new.TradingExpFee,
        OrfAmount        = {table}.OrfAmount        + new.OrfAmount,
        OcfAmount        = {table}.OcfAmount        + new.OcfAmount,
        SecFeeAmount     = {table}.SecFeeAmount     + new.SecFeeAmount,
        TotalUSD         = {table}.TotalUSD         + new.TotalUSD,
        Quantity         = {table}.Quantity         + new.Quantity,
        ProductType      = new.ProductType,
        SymbolMerged     = new.SymbolMerged;
"""

    try:
        with MySQLDB(DB_CONFIG) as db:
            for i in range(0, len(data_tuples), 5000):
                chunk = data_tuples[i : i + 5000]
                placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s)"] * len(chunk))
                final_sql = base_query % placeholders
                flattened = [item for row in chunk for item in row]
                db.execute(final_sql, flattened)
            db.execute("COMMIT;")

        mark_as_loaded(filename, extract_file_date(filename), config["log_table"])
        return True
    except Exception as e:
        print(f"❌ DB load error: {e}")
        return False