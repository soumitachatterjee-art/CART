# Exchange Pipeline — Logic Documentation

## Overview

Processes daily trade files from multiple brokers, standardizes them, and loads them into MySQL with USD fee conversion. Every exchange follows the same pattern: `step_transform()` → `step_load()` → `step_convert_to_usd()`.

---

## Shared Infrastructure

### `cores/utils.py`

**`load_ref_table(db_config)`** — Loads `contract_ref` from MySQL into a cached DataFrame. Hit once per run.

**`enrich_with_ref(df, db_config)`** — Left-joins the trade DataFrame against `contract_ref` on `CtrCode`. Called at the end of every `step_transform()`.

### `contract_ref` Table

Acts as the **master index** for all contracts in the pipeline. Any `CtrCode` built by any exchange can be looked up here to get the full contract details. If a `CtrCode` is missing from this table, the row still loads but logs a `WARNING`.

| Column | Description |
|---|---|
| `ctrcode` | Primary key — matches the `CtrCode` built by each exchange |
| `contract_name` | Full contract description |
| `exchange_name` | Exchange (e.g. CBOT, NYMEX, ICE FUTURES EUROPE) |
| `currency` | Contract currency |

### `CtrCode` Format

```
{PREFIX}-{EXCHANGE_SEGMENT}-{CONTRACT_CODE}
```
`PREFIX` is `FUT` or `OPT`. Examples: `FUT-07-CU`, `FUT-7N-IO`, `OPT-16-ES`.

---

## Exchange: Itau

**Source:** `Axxela Report Full {YYYYMMDD}.csv` · **Table:** `itau_trade_summary` · **Currency:** BRL

### Commodities

| Commodity | CtrCode |
|---|---|
| DI1 / DII | FUT-7N-IO |
| WDO | FUT-7N-AA |
| WSP | FUT-7N-WS |
| DOL | FUT-7N-CU |
| ISP | FUT-7N-SP |

### `step_transform()`
Filters to 6 valid commodities, groups by `[Trade_date, AccountCode, AccountAlias, Commodity]`, then allocates `ExFee` and `BrokerComm` **pro-rata by quantity** from file-level totals.

### `step_load()`
Inserts with `ON DUPLICATE KEY UPDATE` — overwrites fees, resets USD columns to `NULL`. Calls `step_convert_to_usd()` after insert.

### `step_convert_to_usd()`
Divides `ExFee` and `BrokerComm` by the BRL spot rate for `trade_date`. Warns if no rate found.

---

## Exchange: KGI

**Source:** `KT513_{YYYYMMDD}_NT.csv` · **Table:** `kgi_trades` · **Currency:** Multi (from `COMM CCY`)

### `step_transform()`
Filters out `REMARKS` starting with `'TRF'`, maps short contract codes to `CtrCode` via static dict. Special case: `JB + OSE` → `FUT-24-RV` (overrides the default `FUT-17-JB`). Groups by `[TRADE DATE, CLIENT CODE, CtrCode, COMM CCY]`.

### `step_load()`
**Cumulative** insert — `CommFee` and `Qty` are **added** to existing values on duplicate. Resets `CommFeeUSD = NULL`. Calls `step_convert_to_usd()`.

### `step_convert_to_usd()`
USD rows: direct copy. Non-USD rows: divide by spot rate for `trade_date`. Logs any currencies with no rate.

---

## Exchange: StonEx

**Source:** `Trades{YYYYMMDD}.csv` · **Table:** `stonex_trades_march` · **Currency:** Multi

### File Format Detection
Two formats exist (old/new). Auto-detected by checking if `"Exchange Code"` is in the header; columns are renamed to internal names regardless of format.

### `step_transform()`
Builds `CtrCode` via `ctrcode_mapping` dict keyed on `(Exchange Code, Instrument Code)` (both lowercased). Unmapped keys log a `WARNING` with the exact line to add. `ExFee = Exchange Fee + Clearing Fee`. Groups by `[Trade Date, Account Id, Currency, CtrCode]`.

### `step_load()`
Chunked inserts (500 rows/batch). **Cumulative** — all fees and `Qty` are added. Calls `step_convert_to_usd()`.

### `step_convert_to_usd()`
USD rows: direct copy (no date filter). Non-USD: uses **closest available rate** on or before `trade_date` via `MAX(date)` subquery.

---

## Exchange: Marex

**Source:** `SYMMETRY_FINANCIALS_*_{YYYYMMDD}.csv` (2 filename patterns) · **Table:** `marex_trades_march` · **Currency:** USD (no conversion needed)

### `step_transform()`
Filters on `TransactionType` whitelist and excludes accounts `EE066–EE070`. `CtrCode` built from `Market` + `Contract` columns via `format_code()`. `ExFee` sign logic: if `Clearing_Fee_Amount < 0`, merges clearing into exchange fee.

### `step_load()`
USD columns set inline (`ExFeeUSD = ExFee`, etc.) — no `step_convert_to_usd()`. **Cumulative** on duplicate.

---

## Exchange: Maexus (Marex US)

**Source:** `Marex Trades and Fees_{YYYYMMDD}.csv` · **Ref file:** `TRD_SYM_{YYYYMMDD}.csv` (separate S3 bucket) · **Table:** `maexus_trades_march`

### `CtrCode` Format
Unique to Maexus: `{ProductClass_prefix}-{Symbol}-{ExchangeCode}` (e.g. `EQT-BABA-XNYS`). Exchange code resolved via 3-level fallback: TRD_SYM ref file → `STATIC_EXCHANGE_MAP` (76 hardcoded tickers) → `Exchange` column in file → `'MISSING'`.

### `step_transform()`
Dynamically detects the header row, downloads the daily TRD_SYM ref file from S3 using separate credentials, resolves exchange codes, cleans fee columns (strips `$`, `,`), and builds `CtrCode`.

### `step_load()`
Chunked inserts (5,000 rows/batch) with explicit `COMMIT`. No `ON DUPLICATE KEY` — raw insert only.

---

## Fee Structure by Exchange

| Exchange | CommFee | ExFee | NfaFee | ClearingFee | Notes |
|---|---|---|---|---|---|
| Itau | BrokerComm | ExFee | — | — | Pro-rata from file totals |
| KGI | CommFee | — | — | — | Per row |
| StonEx | CommFee | ExFee | NfaFee | Merged into ExFee | |
| Marex | CommFee | ExFee | NfaFee | Conditional merge | Sign-based logic |
| Maexus | CommissionAmount | FirstMoneyAmount | OrfAmount | SecFeeAmount | All 4 stored separately |

## FX Conversion by Exchange

| Exchange | Method |
|---|---|
| Itau | BRL rate exact date match |
| KGI | Per-currency exact date match |
| StonEx | Per-currency closest-rate lookup (MAX subquery) |
| Marex | None — already USD |
| Maexus | None — assumed USD |

---

## Database Tables

| Table | Exchange |
|---|---|
| `itau_trade_summary` | Itau |
| `kgi_trades` | KGI |
| `stonex_trades_march` | StonEx |
| `marex_trades_march` | Marex |
| `maexus_trades_march` | Maexus |
| `contract_ref` | Shared index — all contracts |
| `spotrate` | Shared — FX rates by date/currency |
| `file_load_log_*` | Per exchange — tracks loaded files |

---

## Adding a New Exchange
1. Create `exchanges/{name}.py` with `EXCHANGE_CONFIG`, `step_transform()`, `step_load()`, and optionally `step_convert_to_usd()`
2. Build `CtrCode` in `{PREFIX}-{segment}-{code}` format and add any new codes to `contract_ref`
3. Call `enrich_with_ref(df_final, DB_CONFIG)` at the end of `step_transform()`

## Adding a New Contract
Add a row to `contract_ref` with the new `ctrcode`. Until then, rows load fine but log a `WARNING` — re-running the file after adding the row will backfill via `ON DUPLICATE KEY UPDATE`.