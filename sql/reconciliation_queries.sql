-- =============================================================================
-- reconciliation_queries.sql
-- ERP Reconciliation Pipeline — Core Reconciliation Queries
-- Author: Anandi Mahure
-- Description: Cross-system reconciliation between source_transactions (ERP)
--              and target_ledger (General Ledger). Mirrors the logic in
--              pipeline/reconciliation.py for dual-validation and audit.
-- =============================================================================

-- ----------------------------------------------------------------------------
-- 1. ROW COUNT RECONCILIATION
-- Do source and target record counts match?
-- ----------------------------------------------------------------------------

SELECT
    'source_transactions'           AS system,
    COUNT(*)                        AS record_count
FROM source_transactions

UNION ALL

SELECT
    'target_ledger'                 AS system,
    COUNT(*)                        AS record_count
FROM target_ledger;

-- Expected: counts should match. Any delta = unposted or orphan records.

-- ----------------------------------------------------------------------------
-- 2. CROSS-SYSTEM JOIN — MISSING IN TARGET
-- ERP transactions not posted to the General Ledger
-- ----------------------------------------------------------------------------

SELECT
    s.transaction_id,
    s.transaction_date,
    s.posting_date,
    s.gl_account,
    s.department,
    s.debit_amount,
    s.credit_amount,
    s.currency,
    s.transaction_type,
    s.description,
    'MISSING_IN_TARGET'             AS reconciliation_status
FROM source_transactions s
LEFT JOIN target_ledger t
    ON s.transaction_id = t.ledger_transaction_id
WHERE t.ledger_transaction_id IS NULL
ORDER BY s.posting_date DESC;

-- ----------------------------------------------------------------------------
-- 3. CROSS-SYSTEM JOIN — MISSING IN SOURCE (ORPHAN LEDGER RECORDS)
-- Ledger entries with no matching ERP source transaction
-- ----------------------------------------------------------------------------

SELECT
    t.ledger_transaction_id,
    t.transaction_date,
    t.posting_date,
    t.gl_account,
    t.department,
    t.debit_amount,
    t.credit_amount,
    t.ledger_status,
    'MISSING_IN_SOURCE'             AS reconciliation_status
FROM target_ledger t
LEFT JOIN source_transactions s
    ON t.ledger_transaction_id = s.transaction_id
WHERE s.transaction_id IS NULL
ORDER BY t.posting_date DESC;

-- ----------------------------------------------------------------------------
-- 4. AMOUNT DISCREPANCY — MATCHED KEYS, DIFFERENT VALUES
-- Records exist in both systems but amounts differ by more than £0.01
-- ----------------------------------------------------------------------------

SELECT
    s.transaction_id,
    s.transaction_date,
    s.gl_account,
    s.debit_amount                  AS src_debit,
    t.debit_amount                  AS tgt_debit,
    ABS(s.debit_amount
        - t.debit_amount)           AS debit_delta,
    s.credit_amount                 AS src_credit,
    t.credit_amount                 AS tgt_credit,
    ABS(s.credit_amount
        - t.credit_amount)          AS credit_delta,
    'AMOUNT_DISCREPANCY'            AS reconciliation_status
FROM source_transactions s
INNER JOIN target_ledger t
    ON s.transaction_id = t.ledger_transaction_id
WHERE ABS(s.debit_amount  - t.debit_amount)  > 0.01
   OR ABS(s.credit_amount - t.credit_amount) > 0.01
ORDER BY (ABS(s.debit_amount - t.debit_amount)
        + ABS(s.credit_amount - t.credit_amount)) DESC;

-- ----------------------------------------------------------------------------
-- 5. FULL RECONCILIATION DETAIL — ALL RECORDS WITH STATUS
-- Complete outer join with reconciliation classification
-- ----------------------------------------------------------------------------

SELECT
    COALESCE(s.transaction_id,
             t.ledger_transaction_id)       AS key,
    CASE
        WHEN s.transaction_id IS NULL       THEN 'MISSING_IN_SOURCE'
        WHEN t.ledger_transaction_id IS NULL THEN 'MISSING_IN_TARGET'
        WHEN ABS(s.debit_amount  - t.debit_amount)  > 0.01
          OR ABS(s.credit_amount - t.credit_amount) > 0.01
                                            THEN 'AMOUNT_DISCREPANCY'
        ELSE 'MATCHED'
    END                                     AS reconciliation_status,
    s.transaction_date                      AS src_date,
    t.transaction_date                      AS tgt_date,
    s.gl_account                            AS src_gl,
    t.gl_account                            AS tgt_gl,
    s.debit_amount                          AS src_debit,
    t.debit_amount                          AS tgt_debit,
    ROUND(ABS(COALESCE(s.debit_amount, 0)
            - COALESCE(t.debit_amount, 0)), 4) AS debit_delta,
    s.credit_amount                         AS src_credit,
    t.credit_amount                         AS tgt_credit,
    ROUND(ABS(COALESCE(s.credit_amount, 0)
            - COALESCE(t.credit_amount, 0)), 4) AS credit_delta
FROM source_transactions s
FULL OUTER JOIN target_ledger t
    ON s.transaction_id = t.ledger_transaction_id
ORDER BY reconciliation_status, key;

-- ----------------------------------------------------------------------------
-- 6. RECONCILIATION SUMMARY KPIs
-- Single-row summary for governance dashboard
-- ----------------------------------------------------------------------------

WITH recon AS (
    SELECT
        CASE
            WHEN s.transaction_id IS NULL        THEN 'MISSING_IN_SOURCE'
            WHEN t.ledger_transaction_id IS NULL  THEN 'MISSING_IN_TARGET'
            WHEN ABS(s.debit_amount  - t.debit_amount)  > 0.01
              OR ABS(s.credit_amount - t.credit_amount) > 0.01
                                                  THEN 'AMOUNT_DISCREPANCY'
            ELSE 'MATCHED'
        END AS reconciliation_status
    FROM source_transactions s
    FULL OUTER JOIN target_ledger t
        ON s.transaction_id = t.ledger_transaction_id
)
SELECT
    COUNT(*)                                    AS total_records,
    SUM(CASE WHEN reconciliation_status = 'MATCHED'
             THEN 1 ELSE 0 END)                 AS matched,
    SUM(CASE WHEN reconciliation_status = 'MISSING_IN_TARGET'
             THEN 1 ELSE 0 END)                 AS missing_in_target,
    SUM(CASE WHEN reconciliation_status = 'MISSING_IN_SOURCE'
             THEN 1 ELSE 0 END)                 AS missing_in_source,
    SUM(CASE WHEN reconciliation_status = 'AMOUNT_DISCREPANCY'
             THEN 1 ELSE 0 END)                 AS amount_discrepancies,
    SUM(CASE WHEN reconciliation_status != 'MATCHED'
             THEN 1 ELSE 0 END)                 AS total_discrepancies,
    ROUND(100.0 * SUM(CASE WHEN reconciliation_status = 'MATCHED'
                           THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 1)             AS reconciliation_rate_pct
FROM recon;

-- ----------------------------------------------------------------------------
-- 7. FINANCIAL BALANCE RECONCILIATION
-- Debit/credit totals must balance within £0.01
-- ----------------------------------------------------------------------------

SELECT
    'SOURCE'                        AS system,
    ROUND(SUM(debit_amount), 2)     AS total_debits,
    ROUND(SUM(credit_amount), 2)    AS total_credits,
    ROUND(ABS(SUM(debit_amount)
            - SUM(credit_amount)), 4) AS balance_delta,
    CASE
        WHEN ABS(SUM(debit_amount)
               - SUM(credit_amount)) < 0.01
        THEN 'BALANCED'
        ELSE 'DISCREPANCY_DETECTED'
    END                             AS balance_status
FROM source_transactions

UNION ALL

SELECT
    'TARGET'                        AS system,
    ROUND(SUM(debit_amount), 2)     AS total_debits,
    ROUND(SUM(credit_amount), 2)    AS total_credits,
    ROUND(ABS(SUM(debit_amount)
            - SUM(credit_amount)), 4) AS balance_delta,
    CASE
        WHEN ABS(SUM(debit_amount)
               - SUM(credit_amount)) < 0.01
        THEN 'BALANCED'
        ELSE 'DISCREPANCY_DETECTED'
    END                             AS balance_status
FROM target_ledger;

-- ----------------------------------------------------------------------------
-- 8. DATA FRESHNESS — WHEN WAS EACH TABLE LAST UPDATED?
-- ----------------------------------------------------------------------------

SELECT
    'source_transactions'           AS table_name,
    MAX(posting_date)               AS last_posting_date,
    DATEDIFF(DAY,
             MAX(posting_date),
             CURRENT_DATE)          AS days_since_last_post,
    CASE
        WHEN DATEDIFF(DAY,
                      MAX(posting_date),
                      CURRENT_DATE) > 1
        THEN 'STALE — CHECK FEED'
        ELSE 'CURRENT'
    END                             AS freshness_status
FROM source_transactions

UNION ALL

SELECT
    'target_ledger'                 AS table_name,
    MAX(posting_date)               AS last_posting_date,
    DATEDIFF(DAY,
             MAX(posting_date),
             CURRENT_DATE)          AS days_since_last_post,
    CASE
        WHEN DATEDIFF(DAY,
                      MAX(posting_date),
                      CURRENT_DATE) > 1
        THEN 'STALE — CHECK FEED'
        ELSE 'CURRENT'
    END                             AS freshness_status
FROM target_ledger;

-- =============================================================================
-- END
-- =============================================================================
