"""
ERP Reconciliation Pipeline — Quality Checks Module
Author: Anandi Mahure
Description: Runs 12 business-rule quality checks across both ERP source and
target ledger datasets. Outputs a structured pass/fail report to console and
saves results to outputs/quality_check_report.csv. Mirrors checks implemented
in sql/quality_checks.sql for dual-validation.
"""

import pandas as pd
import numpy as np
import logging
import os
import sys
from datetime import datetime, date

os.makedirs("logs", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/quality_checks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

VALID_CURRENCIES = {"GBP", "USD", "EUR"}
VALID_GL_ACCOUNTS = {
    "1000", "1100", "1200", "2000", "2100",
    "3000", "3100", "4000", "4100", "4200",
    "4300", "4400", "4500", "4600",
}
MAX_TRANSACTION_AMOUNT = 1_000_000.0   # Business rule: flag anything over £1M
MIN_POSTING_LAG_DAYS = 0
MAX_POSTING_LAG_DAYS = 7              # ERP policy: must post within 7 days


def run_check(name: str, df: pd.DataFrame, condition_series: pd.Series, dataset: str) -> dict:
    """
    Execute a single quality check.
    condition_series: boolean Series where True = FAIL (issue detected).
    Returns a dict summarising the check result.
    """
    fail_count = int(condition_series.sum())
    total = len(df)
    pass_fail = "PASS" if fail_count == 0 else "FAIL"
    pct = round((fail_count / total) * 100, 2) if total > 0 else 0

    status_symbol = "✓" if pass_fail == "PASS" else "✗"
    log.info(f"  [{status_symbol}] {dataset} | {name}: {pass_fail} — {fail_count:,}/{total:,} rows affected ({pct}%)")

    return {
        "dataset": dataset,
        "check_name": name,
        "status": pass_fail,
        "rows_failed": fail_count,
        "total_rows": total,
        "failure_pct": pct,
        "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def check_source(df: pd.DataFrame) -> list:
    """Run all quality checks on source_transactions."""
    results = []
    log.info("\n" + "─" * 60)
    log.info("QUALITY CHECKS — SOURCE TRANSACTIONS (ERP)")
    log.info("─" * 60)

    # QC-S01: Null transaction_id — every transaction must have an ID
    results.append(run_check(
        "QC-S01: Null transaction_id",
        df, df["transaction_id"].isna(), "source"
    ))

    # QC-S02: Duplicate transaction_id — uniqueness constraint
    results.append(run_check(
        "QC-S02: Duplicate transaction_id",
        df, df.duplicated(subset=["transaction_id"], keep=False), "source"
    ))

    # QC-S03: Both debit and credit are zero — invalid double-entry
    results.append(run_check(
        "QC-S03: Both debit and credit = 0",
        df, (df["debit_amount"] == 0) & (df["credit_amount"] == 0), "source"
    ))

    # QC-S04: Both debit and credit non-zero — violates single-sided entry rule
    results.append(run_check(
        "QC-S04: Both debit and credit > 0",
        df, (df["debit_amount"] > 0) & (df["credit_amount"] > 0), "source"
    ))

    # QC-S05: Negative amounts — should not occur; credits handled by credit_amount field
    results.append(run_check(
        "QC-S05: Negative debit or credit amount",
        df, (df["debit_amount"] < 0) | (df["credit_amount"] < 0), "source"
    ))

    # QC-S06: Amount exceeds business threshold — potential data entry error
    max_amount = df[["debit_amount", "credit_amount"]].max(axis=1)
    results.append(run_check(
        f"QC-S06: Amount > £{MAX_TRANSACTION_AMOUNT:,.0f}",
        df, max_amount > MAX_TRANSACTION_AMOUNT, "source"
    ))

    # QC-S07: Invalid currency code — only GBP, USD, EUR accepted
    results.append(run_check(
        "QC-S07: Invalid currency",
        df, ~df["currency"].isin(VALID_CURRENCIES), "source"
    ))

    # QC-S08: GBP transactions with exchange rate ≠ 1.0 — FX table error
    gbp_mask = df["currency"] == "GBP"
    results.append(run_check(
        "QC-S08: GBP with exchange_rate ≠ 1.0",
        df, gbp_mask & (df["exchange_rate"] != 1.0), "source"
    ))

    # QC-S09: Invalid GL account code
    results.append(run_check(
        "QC-S09: Invalid GL account",
        df, ~df["gl_account"].astype(str).isin(VALID_GL_ACCOUNTS), "source"
    ))

    # QC-S10: Posting date before transaction date — temporal integrity
    tx_date = pd.to_datetime(df["transaction_date"], errors="coerce")
    post_date = pd.to_datetime(df["posting_date"], errors="coerce")
    results.append(run_check(
        "QC-S10: Posting date before transaction date",
        df, post_date < tx_date, "source"
    ))

    # QC-S11: Posting lag > 7 days — ERP policy compliance
    posting_lag = (post_date - tx_date).dt.days
    results.append(run_check(
        f"QC-S11: Posting lag > {MAX_POSTING_LAG_DAYS} days",
        df, posting_lag > MAX_POSTING_LAG_DAYS, "source"
    ))

    # QC-S12: Missing approver on high-value transactions (> £10K)
    high_value_mask = max_amount > 10_000
    missing_approver = df["approved_by"].isna() | (df["approved_by"] == "")
    results.append(run_check(
        "QC-S12: High-value (>£10K) with no approver",
        df, high_value_mask & missing_approver, "source"
    ))

    return results


def check_target(df: pd.DataFrame) -> list:
    """Run all quality checks on target_ledger."""
    results = []
    log.info("\n" + "─" * 60)
    log.info("QUALITY CHECKS — TARGET LEDGER")
    log.info("─" * 60)

    # QC-T01: Null ledger_transaction_id
    results.append(run_check(
        "QC-T01: Null ledger_transaction_id",
        df, df["ledger_transaction_id"].isna(), "target"
    ))

    # QC-T02: Duplicate ledger entries — general ledger should have unique IDs
    results.append(run_check(
        "QC-T02: Duplicate ledger_transaction_id",
        df, df.duplicated(subset=["ledger_transaction_id"], keep=False), "target"
    ))

    # QC-T03: Invalid ledger_status values
    valid_statuses = {"Cleared", "Open", "Voided"}
    results.append(run_check(
        "QC-T03: Invalid ledger_status",
        df, ~df["ledger_status"].isin(valid_statuses), "target"
    ))

    # QC-T04: Both amounts zero
    results.append(run_check(
        "QC-T04: Both debit and credit = 0",
        df, (df["debit_amount"] == 0) & (df["credit_amount"] == 0), "target"
    ))

    return results


def print_summary(results: list):
    """Print a concise pass/fail summary table."""
    df_results = pd.DataFrame(results)
    pass_count = (df_results["status"] == "PASS").sum()
    fail_count = (df_results["status"] == "FAIL").sum()

    log.info("\n" + "=" * 60)
    log.info("QUALITY CHECK SUMMARY")
    log.info("=" * 60)
    log.info(f"  Total checks run : {len(results)}")
    log.info(f"  PASS             : {pass_count}")
    log.info(f"  FAIL             : {fail_count}")
    log.info(f"  Overall status   : {'✓ ALL CLEAR' if fail_count == 0 else f'⚠ {fail_count} CHECKS FAILED'}")

    if fail_count > 0:
        log.info("\n  Failed checks:")
        for r in results:
            if r["status"] == "FAIL":
                log.info(f"    - [{r['dataset']}] {r['check_name']} ({r['rows_failed']:,} rows)")

    log.info("=" * 60)
    return df_results


def main():
    log.info("=" * 60)
    log.info("ERP RECONCILIATION PIPELINE — QUALITY CHECKS STAGE")
    log.info("=" * 60)

    # Load data
    source = pd.read_csv("data/source_transactions.csv", dtype=str)
    target = pd.read_csv("data/target_ledger.csv", dtype=str)

    # Cast numeric columns
    for col in ["debit_amount", "credit_amount", "exchange_rate"]:
        source[col] = pd.to_numeric(source[col], errors="coerce").fillna(0)
        target[col] = pd.to_numeric(target[col], errors="coerce").fillna(0)

    # Run checks
    all_results = check_source(source) + check_target(target)

    # Summary
    df_results = print_summary(all_results)

    # Save report
    output_path = "outputs/quality_check_report.csv"
    df_results.to_csv(output_path, index=False)
    log.info(f"\nQuality check report saved to: {output_path}")
    log.info("Proceed to: python pipeline/reconciliation.py")

    return df_results


if __name__ == "__main__":
    main()
