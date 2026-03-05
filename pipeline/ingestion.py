"""
ERP Reconciliation Pipeline — Ingestion Module
Author: Anandi Mahure
Description: Loads source_transactions.csv and target_ledger.csv, validates
schema completeness and data types, and logs a summary report to console and
file. First stage in the reconciliation pipeline (Bronze layer equivalent).
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


def load_csv(filepath: str, schema: dict, label: str) -> pd.DataFrame:
    """Load a CSV file and cast columns to expected dtypes."""
    log.info(f"Loading {label} from: {filepath}")
    if not os.path.exists(filepath):
        log.error(f"File not found: {filepath}")
        sys.exit(1)

    df = pd.read_csv(filepath, dtype=str)  # Load all as str first to avoid silent coercion
    log.info(f"  Raw shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # Check for missing expected columns
    missing_cols = set(schema.keys()) - set(df.columns)
    extra_cols = set(df.columns) - set(schema.keys())
    if missing_cols:
        log.warning(f"  SCHEMA MISMATCH — Missing columns: {missing_cols}")
    if extra_cols:
        log.info(f"  Extra columns (not in schema): {extra_cols}")

    # Cast numeric columns
    for col, dtype in schema.items():
        if col not in df.columns:
            continue
        if dtype in ("float64", "int64"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def validate_source(df: pd.DataFrame) -> dict:
    """Run schema and business-rule validation on source transactions."""
    results = {}

    # Null checks on mandatory fields
    mandatory = ["transaction_id", "transaction_date", "gl_account", "debit_amount", "credit_amount"]
    for col in mandatory:
        null_count = df[col].isna().sum()
        results[f"null_{col}"] = int(null_count)
        if null_count:
            log.warning(f"  NULL check FAIL — {col}: {null_count} nulls")
        else:
            log.info(f"  NULL check PASS — {col}")

    # Duplicate transaction IDs — critical for reconciliation integrity
    dup_count = df["transaction_id"].duplicated().sum()
    results["duplicate_transaction_ids"] = int(dup_count)
    if dup_count:
        log.warning(f"  DUPLICATE check FAIL — {dup_count} duplicate transaction_ids")
    else:
        log.info(f"  DUPLICATE check PASS — transaction_id unique")

    # Currency validity
    invalid_currency = (~df["currency"].isin(VALID_CURRENCIES)).sum()
    results["invalid_currency"] = int(invalid_currency)
    log.info(f"  CURRENCY check — {invalid_currency} invalid values")

    # Amount integrity: debit + credit must not both be zero
    both_zero = ((df["debit_amount"] == 0) & (df["credit_amount"] == 0)).sum()
    results["both_amounts_zero"] = int(both_zero)
    if both_zero:
        log.warning(f"  AMOUNT check FAIL — {both_zero} rows where both debit and credit are 0")
    else:
        log.info(f"  AMOUNT check PASS — no zero-zero rows")

    # Negative amounts
    neg_debit = (df["debit_amount"] < 0).sum()
    neg_credit = (df["credit_amount"] < 0).sum()
    results["negative_debit"] = int(neg_debit)
    results["negative_credit"] = int(neg_credit)
    log.info(f"  NEGATIVE amounts — debit: {neg_debit}, credit: {neg_credit}")

    # Exchange rate sanity: GBP should have rate = 1.0
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

    mandatory = ["ledger_transaction_id", "debit_amount", "credit_amount", "ledger_status"]
    for col in mandatory:
        null_count = df[col].isna().sum()
        results[f"null_{col}"] = int(null_count)
        if null_count:
            log.warning(f"  NULL check FAIL — {col}: {null_count} nulls")
        else:
            log.info(f"  NULL check PASS — {col}")

    dup_count = df["ledger_transaction_id"].duplicated().sum()
    results["duplicate_ledger_ids"] = int(dup_count)
    log.info(f"  DUPLICATE check — {dup_count} duplicate ledger_transaction_ids")

    invalid_status = (~df["ledger_status"].isin(VALID_STATUSES_TARGET)).sum()
    results["invalid_ledger_status"] = int(invalid_status)
    log.info(f"  STATUS check — {invalid_status} unrecognised ledger_status values")

    return results


def print_summary(label: str, df: pd.DataFrame, val_results: dict):
    """Print a formatted summary block to console."""
    total_debit = df["debit_amount"].sum()
    total_credit = df["credit_amount"].sum()
    log.info(f"\n{'='*60}")
    log.info(f"  SUMMARY — {label}")
    log.info(f"{'='*60}")
    log.info(f"  Total rows          : {len(df):,}")
    log.info(f"  Total debit (£)     : £{total_debit:,.2f}")
    log.info(f"  Total credit (£)    : £{total_credit:,.2f}")
    log.info(f"  Net balance (£)     : £{total_debit - total_credit:,.2f}")
    log.info(f"  Validation results  : {val_results}")
    log.info(f"{'='*60}\n")


def main():
    log.info("=" * 60)
    log.info("ERP RECONCILIATION PIPELINE — INGESTION STAGE")
    log.info("=" * 60)

    # Load both systems
    source = load_csv("data/source_transactions.csv", SOURCE_SCHEMA, "Source ERP Transactions")
    target = load_csv("data/target_ledger.csv", TARGET_SCHEMA, "Target General Ledger")

    # Validate
    log.info("\n--- Validating source_transactions ---")
    source_val = validate_source(source)

    log.info("\n--- Validating target_ledger ---")
    target_val = validate_target(target)

    # Summary
    print_summary("SOURCE ERP TRANSACTIONS", source, source_val)
    print_summary("TARGET GENERAL LEDGER", target, target_val)

    log.info("Ingestion stage complete. Proceed to: python pipeline/quality_checks.py")

    # Return DataFrames for use by downstream stages when imported as module
    return source, target


if __name__ == "__main__":
    main()
