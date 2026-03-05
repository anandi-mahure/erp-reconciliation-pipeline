"""
ERP Reconciliation Pipeline — Reconciliation Module
Author: Anandi Mahure
Description: Cross-system reconciliation between ERP source transactions and the
target general ledger. Identifies: (1) records missing from target, (2) records
missing from source, and (3) amount discrepancies where amounts differ by more
than £0.01. Calculates the overall reconciliation rate and delta summary.
Target metrics: 98.7% reconciliation rate, 13 discrepant records.
"""

import pandas as pd
import numpy as np
import logging
import os
import sys
from datetime import datetime

os.makedirs("logs", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

AMOUNT_TOLERANCE = 0.01  # £0.01 — standard financial reconciliation tolerance


def load_data() -> tuple:
    """Load and minimally prepare both systems for reconciliation join."""
    source = pd.read_csv("data/source_transactions.csv", dtype=str)
    target = pd.read_csv("data/target_ledger.csv", dtype=str)

    for col in ["debit_amount", "credit_amount"]:
        source[col] = pd.to_numeric(source[col], errors="coerce").fillna(0.0)
        target[col] = pd.to_numeric(target[col], errors="coerce").fillna(0.0)

    log.info(f"Source loaded: {len(source):,} transactions")
    log.info(f"Target loaded: {len(target):,} ledger entries")
    return source, target


def reconcile(source: pd.DataFrame, target: pd.DataFrame) -> dict:
    """
    Outer join on transaction_id / ledger_transaction_id.
    Classifies each row as: MATCHED, MISSING_IN_TARGET, MISSING_IN_SOURCE, AMOUNT_DISCREPANCY.
    """
    log.info("\n" + "─" * 60)
    log.info("RUNNING RECONCILIATION JOIN")
    log.info("─" * 60)

    # Prepare source: keep only key columns for the join
    src = source[["transaction_id", "debit_amount", "credit_amount",
                  "gl_account", "department", "transaction_date", "currency"]].copy()
    src.rename(columns={
        "transaction_id": "key",
        "debit_amount": "src_debit",
        "credit_amount": "src_credit",
    }, inplace=True)

    # Prepare target: align column names for the join
    tgt = target[["ledger_transaction_id", "debit_amount", "credit_amount",
                  "gl_account", "department", "transaction_date", "currency"]].copy()
    tgt.rename(columns={
        "ledger_transaction_id": "key",
        "debit_amount": "tgt_debit",
        "credit_amount": "tgt_credit",
    }, inplace=True)

    # Full outer join on the common transaction key
    merged = pd.merge(src, tgt, on="key", how="outer", suffixes=("_src", "_tgt"))
    log.info(f"Merged dataset: {len(merged):,} rows after outer join")

    # ── Classification logic ───────────────────────────────────────────────────

    def classify(row):
        src_present = pd.notna(row["src_debit"]) and pd.notna(row["src_credit"])
        tgt_present = pd.notna(row["tgt_debit"]) and pd.notna(row["tgt_credit"])

        if not src_present:
            return "MISSING_IN_SOURCE"          # In ledger but not in ERP
        if not tgt_present:
            return "MISSING_IN_TARGET"          # In ERP but not posted to ledger

        # Both present — check amount agreement within tolerance
        debit_diff = abs(row["src_debit"] - row["tgt_debit"])
        credit_diff = abs(row["src_credit"] - row["tgt_credit"])

        if debit_diff > AMOUNT_TOLERANCE or credit_diff > AMOUNT_TOLERANCE:
            return "AMOUNT_DISCREPANCY"         # Interface rounding or keying error
        return "MATCHED"

    merged["reconciliation_status"] = merged.apply(classify, axis=1)

    # Compute debit/credit deltas for discrepant rows
    merged["debit_delta"] = (merged["src_debit"] - merged["tgt_debit"]).round(4)
    merged["credit_delta"] = (merged["src_credit"] - merged["tgt_credit"]).round(4)

    return merged


def generate_report(merged: pd.DataFrame) -> dict:
    """Compute and log reconciliation KPIs."""
    total = len(merged)
    counts = merged["reconciliation_status"].value_counts().to_dict()

    matched = counts.get("MATCHED", 0)
    missing_target = counts.get("MISSING_IN_TARGET", 0)
    missing_source = counts.get("MISSING_IN_SOURCE", 0)
    discrepant = counts.get("AMOUNT_DISCREPANCY", 0)

    # Reconciliation rate = matched / total source transactions
    total_source = missing_target + matched + discrepant
    recon_rate = (matched / total_source * 100) if total_source > 0 else 0

    # Net delta across all discrepant rows
    disc_rows = merged[merged["reconciliation_status"] == "AMOUNT_DISCREPANCY"]
    net_debit_delta = disc_rows["debit_delta"].sum()
    net_credit_delta = disc_rows["credit_delta"].sum()

    log.info("\n" + "=" * 60)
    log.info("RECONCILIATION RESULTS")
    log.info("=" * 60)
    log.info(f"  Total source transactions    : {total_source:,}")
    log.info(f"  Total ledger entries         : {missing_source + matched + discrepant:,}")
    log.info(f"  ┌─ MATCHED                  : {matched:,}")
    log.info(f"  ├─ MISSING IN TARGET (ERP→) : {missing_target:,}")
    log.info(f"  ├─ MISSING IN SOURCE (→Leg) : {missing_source:,}")
    log.info(f"  └─ AMOUNT DISCREPANCY        : {discrepant:,}")
    log.info(f"  Total discrepancies          : {missing_target + missing_source + discrepant:,}")
    log.info(f"  Reconciliation rate          : {recon_rate:.1f}%")
    log.info(f"  Net debit delta (£)          : £{net_debit_delta:+.4f}")
    log.info(f"  Net credit delta (£)         : £{net_credit_delta:+.4f}")
    log.info("=" * 60)

    return {
        "total_source_transactions": total_source,
        "matched": matched,
        "missing_in_target": missing_target,
        "missing_in_source": missing_source,
        "amount_discrepancies": discrepant,
        "total_discrepancies": missing_target + missing_source + discrepant,
        "reconciliation_rate_pct": round(recon_rate, 1),
        "net_debit_delta_gbp": round(net_debit_delta, 4),
        "net_credit_delta_gbp": round(net_credit_delta, 4),
    }


def save_outputs(merged: pd.DataFrame, summary: dict):
    """Save reconciliation detail and summary to outputs directory."""
    # Full reconciliation detail (all rows with status)
    detail_path = "outputs/reconciliation_detail.csv"
    merged.to_csv(detail_path, index=False)
    log.info(f"\nReconciliation detail saved: {detail_path}")

    # Discrepancies only — what needs investigation
    disc_path = "outputs/discrepancies.csv"
    discrepant_df = merged[merged["reconciliation_status"] != "MATCHED"]
    discrepant_df.to_csv(disc_path, index=False)
    log.info(f"Discrepancy report saved: {disc_path} ({len(discrepant_df):,} rows)")

    # Summary as single-row CSV for governance pack
    summary_path = "outputs/reconciliation_summary.csv"
    pd.DataFrame([summary]).to_csv(summary_path, index=False)
    log.info(f"Summary saved: {summary_path}")


def main():
    log.info("=" * 60)
    log.info("ERP RECONCILIATION PIPELINE — RECONCILIATION STAGE")
    log.info(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    source, target = load_data()
    merged = reconcile(source, target)
    summary = generate_report(merged)
    save_outputs(merged, summary)

    log.info("\nReconciliation complete. Proceed to: python pipeline/governance_report.py")
    return merged, summary


if __name__ == "__main__":
    main()
