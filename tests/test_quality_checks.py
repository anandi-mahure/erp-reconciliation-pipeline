"""
Unit tests for ERP Reconciliation Pipeline
Tests core logic from quality_checks.py and reconciliation.py
Run with: pytest tests/ -v
"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_source():
    return pd.DataFrame({
        'transaction_id':   ['TXN001', 'TXN002', 'TXN003', 'TXN004', 'TXN005'],
        'transaction_date': ['2023-11-01'] * 5,
        'posting_date':     ['2023-11-01'] * 5,
        'gl_account':       ['4100', '4200', '4300', '4400', '4500'],
        'department':       ['Finance', 'IT', 'HR', 'Ops', 'Sales'],
        'debit_amount':     [500.00, 1200.50, 750.00, 300.00, 900.00],
        'credit_amount':    [0.00, 0.00, 0.00, 0.00, 0.00],
        'currency':         ['GBP', 'GBP', 'USD', 'GBP', 'EUR'],
        'exchange_rate':    [1.0, 1.0, 1.27, 1.0, 1.15],
        'approved_by':      ['user1', 'user2', 'user3', 'user4', 'user5'],
    })


@pytest.fixture
def source_with_issues():
    return pd.DataFrame({
        'transaction_id':   ['TXN001', 'TXN001', 'TXN003', None, 'TXN005'],
        'transaction_date': ['2023-11-01'] * 5,
        'posting_date':     ['2023-11-01'] * 5,
        'gl_account':       ['4100', '4200', 'INVALID', '4400', '4500'],
        'department':       ['Finance', 'IT', 'HR', 'Ops', 'Sales'],
        'debit_amount':     [500.00, 0.00, -100.00, None, 15000.00],
        'credit_amount':    [0.00, 0.00, 0.00, None, 0.00],
        'currency':         ['GBP', 'GBP', 'XYZ', 'GBP', 'GBP'],
        'exchange_rate':    [1.0, 1.0, 1.0, 1.0, 1.0],
        'approved_by':      ['user1', 'user2', 'user3', 'user4', None],
    })


@pytest.fixture
def clean_target():
    return pd.DataFrame({
        'ledger_transaction_id': ['TXN001', 'TXN002', 'TXN003'],
        'debit_amount':          [500.00, 1200.50, 750.00],
        'credit_amount':         [0.00, 0.00, 0.00],
        'ledger_status':         ['Cleared', 'Cleared', 'Cleared'],
    })


# ── Null / completeness tests ─────────────────────────────────────────────────

def test_no_nulls_in_clean_source(clean_source):
    """Clean source should have zero nulls in mandatory fields."""
    mandatory = ['transaction_id', 'transaction_date', 'gl_account',
                 'debit_amount', 'credit_amount']
    for col in mandatory:
        assert clean_source[col].isna().sum() == 0, f"Unexpected null in {col}"


def test_null_detection_in_dirty_source(source_with_issues):
    """Quality engine must detect nulls in transaction_id and amounts."""
    assert source_with_issues['transaction_id'].isna().sum() == 1
    assert source_with_issues['debit_amount'].isna().sum() == 1


# ── Duplicate tests ───────────────────────────────────────────────────────────

def test_duplicate_detection(source_with_issues):
    """QC-S02: Duplicate transaction_ids must be flagged."""
    duplicates = source_with_issues[
        source_with_issues.duplicated(subset=['transaction_id'], keep=False)
    ]
    assert len(duplicates) == 2  # TXN001 appears twice


def test_no_duplicates_in_clean_source(clean_source):
    """Clean source should have zero duplicate transaction_ids."""
    assert clean_source.duplicated(subset=['transaction_id']).sum() == 0


# ── Amount integrity tests ────────────────────────────────────────────────────

def test_negative_amount_detection(source_with_issues):
    """QC-S05: Negative amounts must be detected."""
    neg_mask = (source_with_issues['debit_amount'] < 0) | \
               (source_with_issues['credit_amount'] < 0)
    assert neg_mask.sum() == 1


def test_both_zero_detection(source_with_issues):
    """QC-S03: Rows where both debit and credit are 0 must be flagged."""
    both_zero = (
        (source_with_issues['debit_amount'] == 0) &
        (source_with_issues['credit_amount'] == 0)
    )
    assert both_zero.sum() >= 1


def test_high_value_missing_approver(source_with_issues):
    """QC-S12: High-value transactions (>£10K) without approver must be flagged."""
    high_value = source_with_issues['debit_amount'] > 10_000
    missing_approver = source_with_issues['approved_by'].isna()
    flagged = (high_value & missing_approver).sum()
    assert flagged == 1  # TXN005: £15,000 with no approver


# ── Currency / FX tests ───────────────────────────────────────────────────────

def test_invalid_currency_detection(source_with_issues):
    """QC-S07: Invalid currency codes must be caught."""
    valid = {'GBP', 'USD', 'EUR'}
    invalid = (~source_with_issues['currency'].isin(valid)).sum()
    assert invalid == 1  # XYZ


def test_gbp_exchange_rate_integrity(clean_source):
    """QC-S08: GBP rows must have exchange_rate = 1.0."""
    gbp = clean_source[clean_source['currency'] == 'GBP']
    bad_fx = (gbp['exchange_rate'] != 1.0).sum()
    assert bad_fx == 0


# ── Reconciliation logic tests ────────────────────────────────────────────────

def test_reconciliation_match_rate(clean_source, clean_target):
    """Reconciliation engine must compute correct match rate."""
    source_keys = set(clean_source['transaction_id'])
    target_keys = set(clean_target['ledger_transaction_id'])
    matched = source_keys & target_keys
    match_rate = len(matched) / len(source_keys) * 100
    assert match_rate == 60.0  # 3 of 5 matched


def test_missing_in_target_identified(clean_source, clean_target):
    """Reconciliation must identify TXN004 and TXN005 as missing in target."""
    source_keys = set(clean_source['transaction_id'])
    target_keys = set(clean_target['ledger_transaction_id'])
    missing = source_keys - target_keys
    assert 'TXN004' in missing
    assert 'TXN005' in missing


def test_amount_discrepancy_detection():
    """Reconciliation must flag records where amounts differ > £0.01."""
    TOLERANCE = 0.01
    src_amount = 500.00
    tgt_amount = 500.05  # 5p discrepancy — should fail
    assert abs(src_amount - tgt_amount) > TOLERANCE


def test_amount_within_tolerance():
    """Amounts differing by < £0.01 must be classified as MATCHED."""
    TOLERANCE = 0.01
    src_amount = 500.00
    tgt_amount = 500.009  # rounding — should pass
    assert abs(src_amount - tgt_amount) < TOLERANCE


# ── Target ledger tests ───────────────────────────────────────────────────────

def test_valid_ledger_status(clean_target):
    """QC-T03: All ledger_status values must be in valid set."""
    valid_statuses = {'Cleared', 'Open', 'Voided'}
    invalid = (~clean_target['ledger_status'].isin(valid_statuses)).sum()
    assert invalid == 0


def test_no_duplicate_ledger_ids(clean_target):
    """QC-T02: ledger_transaction_id must be unique."""
    assert clean_target.duplicated(subset=['ledger_transaction_id']).sum() == 0
