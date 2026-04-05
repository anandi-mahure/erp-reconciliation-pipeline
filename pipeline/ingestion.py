"""
ERP Reconciliation Pipeline — Ingestion Module
Author: Anandi Mahure
Description: Loads source_transactions.csv and target_ledger.csv, validates
schema completeness and data types, and logs a summary report to console and
file. First stage in the reconciliation pipeline (Bronze layer equivalent).
Supports chunked ingestion for production-scale datasets (1M+ rows).
"""

import pandas as pd
import numpy as np
import logging
import os
import sys
from datetime import datetime

# ── Logging setup ──────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Expected schemas ───────────────────────────────────────────────────────────
SOURCE_SCHEMA = {
    "transaction_id": "object",
    "transaction_date": "object",
    "posting_date": "object",
    "gl_account": "object",
    "cost_centre": "object",
    "department": "object",
    "vendor_id": "object",
    "transaction_type": "object",
    "debit_amount": "float64",
    "credit_amount": "float64",
    "currency": "object",
    "exchange_rate": "float64",
    "reference_number": "object",
    "description": "object",
    "status": "object",
    "created_by": "object",
    "approved_by": "object",
    "erp_batch_id": "object",
}

TARGET_SCHEMA = {
    "ledger_transaction_id": "object",
    "transaction_date": "object",
    "posting_date": "object",
    "gl_account": "object",
    "cost_centre": "object",
    "department": "object",
    "vendor_id": "object",
    "transaction_type": "object",
    "debit_amount": "float64",
    "credit_amount": "float64",
    "currency": "object",
    "exchange_rate": "float64",
    "reference_number": "object",
    "description": "object",
    "status": "object",
    "entered_by": "object",
    "authorised_by": "object",
    "ledger_batch_id": "object",
    "ledger_entry_date": "object",
    "ledger_status": "object",
}

VALID_CURRENCIES = {"GBP", "USD", "EUR"}
VALID_STATUSES_SOURCE = {"Posted", "Pending", "Reversed"}
VALID_STATUSES_TARGET = {"Cleared", "Open", "Voided"}

CHUNK_SIZE = 50_000  # Rows per chunk — handles 1M+ row production datasets


def load_csv(filepath: str, schema: dict, label: str) -> pd.DataFrame:
    """
    Load a CSV file using chunked ingestion for production-scale datasets.
    Casts numeric columns to expected dtypes after load.
    Logs schema mismatches and extra columns.
    """
    log.info(f"Loading {label} from: {filepath}")
    if not os.path.exists(filepath):
        log.error(f"File not found: {filepath}")
        sys.exit(1)

    # ── Chunked ingestion ──────────────────────────────────────────────────────
    # Load all columns as str first to prevent silent dtype coercion.
    # Chunks are concatenated into a single DataFrame after load.
    # This pattern handles production datasets of 1M+ rows without memory issues.
    chunks = []
    for chunk in pd.read_csv(filepath, dtype=str, chunksize=CHUNK_SIZE):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)

    log.info(f"  Loaded via chunked ingestion (chunk size: {CHUNK_SIZE:,} rows)")
    log.info(f"  Raw shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # ── Schema validation ──────────────────────────────────────────────────────
    missing_cols = set(schema.keys()) - set(df.columns)
    extra_cols   = set(df.columns)   - set(schema.keys())
    if missing_cols:
        log.warning(f"  SCHEMA MISMATCH — Missing columns: {missing_cols}")
    if extra_cols:
        log.info(f"  Extra columns (not in schema): {extra_cols}")

    # ── Dtype enforcement ──────────────────────────────────────────────────────
    for col, dtype in schema.items():
        if col not in df.columns:
            continue
        if dtype in ("float64", "int64"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def validate_source(df: pd.DataFrame) -> dict:
    """Run schema and business-rule validation on source transactions."""
    results = {}

    # ── Null checks on mandatory fields ───────────────────────────────────────
    mandatory = [
        "transaction_id", "transaction_date", "gl_account",
        "debit_amount", "credit_amount"
    ]
    for col in mandatory:
        null_count = df[col].isna().sum()
        results[f"null_{col}"] = int(null_count)
        if null_count:
            log.warning(f"  NULL check FAIL — {col}: {null_count} nulls")
        else:
            log.info(f"  NULL check PASS — {col}")

    # ── Duplicate transaction IDs ──────────────────────────────────────────────
    # Critical: duplicates corrupt reconciliation join results
    dup_count = df["transaction_id"].duplicated().sum()
    results["duplicate_transaction_ids"] = int(dup_count)
    if dup_count:
        log.warning(f"  DUPLICATE check FAIL — {dup_count} duplicate transaction_ids")
    else:
        log.info(f"  DUPLICATE check PASS — transaction_id unique")

    # ── Currency validity ──────────────────────────────────────────────────────
    invalid_currency = (~df["currency"].isin(VALID_CURRENCIES)).sum()
    results["invalid_currency"] = int(invalid_currency)
    if invalid_currency:
        log.warning(f"  CURRENCY check FAIL — {invalid_currency} unrecognised currency codes")
    else:
        log.info(f"  CURRENCY check PASS — all values in {VALID_CURRENCIES}")

    # ── Amount integrity: both cannot be zero ──────────────────────────────────
    both_zero = ((df["debit_amount"] == 0) & (df["credit_amount"] == 0)).sum()
    results["both_amounts_zero"] = int(both_zero)
    if both_zero:
        log.warning(f"  AMOUNT check FAIL — {both_zero} rows where both debit and credit are 0")
    else:
        log.info(f"  AMOUNT check PASS — no zero-zero rows")

    # ── Negative amounts ───────────────────────────────────────────────────────
    neg_debit  = (df["debit_amount"]  < 0).sum()
    neg_credit = (df["credit_amount"] < 0).sum()
    results["negative_debit"]  = int(neg_debit)
    results["negative_credit"] = int(neg_credit)
    if neg_debit or neg_credit:
        log.warning(f"  NEGATIVE check FAIL — debit: {neg_debit}, credit: {neg_credit}")
    else:
        log.info(f"  NEGATIVE check PASS — no negative amounts")

    # ── FX rate integrity: GBP must have exchange_rate = 1.0 ──────────────────
    gbp_rows = df[df["currency"] == "GBP"]
    bad_fx = (gbp_rows["exchange_rate"] != 1.0).sum()
    results["bad_gbp_exchange_rate"] = int(bad_fx)
    if bad_fx:
        log.warning(f"  FX RATE check FAIL — {bad_fx} GBP rows with exchange_rate ≠ 1.0")
    else:
        log.info(f"  FX RATE check PASS — all GBP rows have rate = 1.0")

    return results


def validate_target(df: pd.DataFrame) -> dict:
    """Run schema and business-rule validation on target ledger."""
    results = {}

    # ── Null checks ────────────────────────────────────────────────────────────
    mandatory = [
        "ledger_transaction_id", "debit_amount",
        "credit_amount", "ledger_status"
    ]
    for col in mandatory:
        null_count = df[col].isna().sum()
        results[f"null_{col}"] = int(null_count)
        if null_count:
            log.warning(f"  NULL check FAIL — {col}: {null_count} nulls")
        else:
            log.info(f"  NULL check PASS — {col}")

    # ── Duplicate ledger IDs ───────────────────────────────────────────────────
    dup_count = df["ledger_transaction_id"].duplicated().sum()
    results["duplicate_ledger_ids"] = int(dup_count)
    if dup_count:
        log.warning(f"  DUPLICATE check FAIL — {dup_count} duplicate ledger_transaction_ids")
    else:
        log.info(f"  DUPLICATE check PASS — ledger_transaction_id unique")

    # ── Ledger status validity ─────────────────────────────────────────────────
    invalid_status = (~df["ledger_status"].isin(VALID_STATUSES_TARGET)).sum()
    results["invalid_ledger_status"] = int(invalid_status)
    if invalid_status:
        log.warning(f"  STATUS check FAIL — {invalid_status} unrecognised ledger_status values")
    else:
        log.info(f"  STATUS check PASS — all values in {VALID_STATUSES_TARGET}")

    return results


def print_summary(label: str, df: pd.DataFrame, val_results: dict):
    """Print a formatted financial summary block to console and log file."""
    total_debit  = df["debit_amount"].sum()
    total_credit = df["credit_amount"].sum()
    net_balance  = total_debit - total_credit
    fail_count   = sum(1 for k, v in val_results.items() if isinstance(v, int) and v > 0)

    log.info(f"\n{'='*60}")
    log.info(f"  SUMMARY — {label}")
    log.info(f"{'='*60}")
    log.info(f"  Total rows          : {len(df):,}")
    log.info(f"  Total debit  (£)    : £{total_debit:>15,.2f}")
    log.info(f"  Total credit (£)    : £{total_credit:>15,.2f}")
    log.info(f"  Net balance  (£)    : £{net_balance:>+15,.2f}")
    log.info(f"  Validation checks   : {len(val_results)} run — {fail_count} flagged")
    log.info(f"  Detailed results    : {val_results}")
    log.info(f"{'='*60}\n")


def main():
    log.info("=" * 60)
    log.info("ERP RECONCILIATION PIPELINE — INGESTION STAGE")
    log.info(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # ── Load both systems ──────────────────────────────────────────────────────
    source = load_csv(
        "data/source_transactions.csv",
        SOURCE_SCHEMA,
        "Source ERP Transactions"
    )
    target = load_csv(
        "data/target_ledger.csv",
        TARGET_SCHEMA,
        "Target General Ledger"
    )

    # ── Validate ───────────────────────────────────────────────────────────────
    log.info("\n--- Validating source_transactions ---")
    source_val = validate_source(source)

    log.info("\n--- Validating target_ledger ---")
    target_val = validate_target(target)

    # ── Summaries ──────────────────────────────────────────────────────────────
    print_summary("SOURCE ERP TRANSACTIONS", source, source_val)
    print_summary("TARGET GENERAL LEDGER",   target, target_val)

    log.info("Ingestion stage complete.")
    log.info("Proceed to: python pipeline/quality_checks.py")

    # Return DataFrames for downstream import by other pipeline stages
    return source, target


if __name__ == "__main__":
    main()
