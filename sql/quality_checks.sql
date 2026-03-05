-- =============================================================================
-- quality_checks.sql
-- ERP Reconciliation Pipeline — Data Quality Validation Queries
-- Author: Anandi Mahure
-- Description: Comprehensive DQ checks run against source_transactions and
--              target_ledger before reconciliation. Results feed the governance
--              report. ANSI SQL with SQL Server / PostgreSQL dialect notes.
-- =============================================================================

-- ----------------------------------------------------------------------------
-- 1. NULL / COMPLETENESS CHECKS
-- ----------------------------------------------------------------------------

-- 1a. Mandatory fields must never be NULL in source
SELECT
    'source_transactions'                   AS table_name,
    'NULL_MANDATORY_FIELDS'                 AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE transaction_id IS NULL
   OR posting_date   IS NULL
   OR gl_account     IS NULL
   OR debit_amount   IS NULL
   OR credit_amount  IS NULL;

-- 1b. Same check on target ledger
SELECT
    'target_ledger'                         AS table_name,
    'NULL_MANDATORY_FIELDS'                 AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM target_ledger
WHERE ledger_id    IS NULL
   OR source_ref   IS NULL
   OR posting_date IS NULL
   OR debit_amount IS NULL
   OR credit_amount IS NULL;

-- 1c. Vendor ID populated for INVOICE transactions
SELECT
    'source_transactions'                   AS table_name,
    'MISSING_VENDOR_ON_INVOICE'             AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE transaction_type = 'INVOICE'
  AND (vendor_id IS NULL OR vendor_id = '');

-- ----------------------------------------------------------------------------
-- 2. DUPLICATE CHECKS
-- ----------------------------------------------------------------------------

-- 2a. transaction_id must be unique PK
SELECT
    'source_transactions'                   AS table_name,
    'DUPLICATE_TRANSACTION_ID'              AS check_name,
    COUNT(*) - COUNT(DISTINCT transaction_id) AS failing_rows,
    CASE WHEN COUNT(*) = COUNT(DISTINCT transaction_id)
         THEN 'PASS' ELSE 'FAIL' END        AS result
FROM source_transactions;

-- 2b. Actual duplicates for audit
SELECT transaction_id, COUNT(*) AS occurrence_count
FROM source_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- 2c. ledger_id uniqueness
SELECT
    'target_ledger'                         AS table_name,
    'DUPLICATE_LEDGER_ID'                   AS check_name,
    COUNT(*) - COUNT(DISTINCT ledger_id)    AS failing_rows,
    CASE WHEN COUNT(*) = COUNT(DISTINCT ledger_id)
         THEN 'PASS' ELSE 'FAIL' END        AS result
FROM target_ledger;

-- ----------------------------------------------------------------------------
-- 3. VALUE RANGE CHECKS
-- ----------------------------------------------------------------------------

-- 3a. No negative debit or credit amounts
SELECT
    'source_transactions'                   AS table_name,
    'NEGATIVE_DEBIT_OR_CREDIT'              AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE debit_amount < 0 OR credit_amount < 0;

-- 3b. net_amount must equal debit minus credit (1p tolerance)
SELECT
    'source_transactions'                   AS table_name,
    'NET_AMOUNT_INTEGRITY'                  AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE ABS(net_amount - (debit_amount - credit_amount)) > 0.01;

-- 3c. Outlier alert: transactions above £500K
SELECT
    transaction_id, transaction_type, debit_amount, credit_amount, description
FROM source_transactions
WHERE debit_amount > 500000 OR credit_amount > 500000
ORDER BY debit_amount DESC;

-- 3d. Zero-value transactions (suspicious — may be system artefacts)
SELECT
    'source_transactions'                   AS table_name,
    'ZERO_VALUE_TRANSACTIONS'               AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS result
FROM source_transactions
WHERE debit_amount = 0 AND credit_amount = 0;

-- ----------------------------------------------------------------------------
-- 4. DATE INTEGRITY CHECKS
-- ----------------------------------------------------------------------------

-- 4a. No future posting dates
SELECT
    'source_transactions'                   AS table_name,
    'FUTURE_POSTING_DATE'                   AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE posting_date > CURRENT_DATE;

-- 4b. Value date must not lag posting date by more than 30 days
SELECT
    'source_transactions'                   AS table_name,
    'VALUE_DATE_LAG_EXCESS_30D'             AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS result
FROM source_transactions
-- SQL Server: DATEDIFF(DAY, value_date, posting_date) > 30
-- PostgreSQL: posting_date::date - value_date::date > 30
WHERE DATEDIFF(DAY, value_date, posting_date) > 30;

-- 4c. Transactions within fiscal year window 2024
SELECT
    'source_transactions'                   AS table_name,
    'OUT_OF_PERIOD_TRANSACTIONS'            AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS result
FROM source_transactions
WHERE posting_date < '2024-01-01' OR posting_date > '2024-12-31';

-- ----------------------------------------------------------------------------
-- 5. REFERENTIAL INTEGRITY
-- ----------------------------------------------------------------------------

-- 5a. GL account in chart of accounts
SELECT
    'source_transactions'                   AS table_name,
    'INVALID_GL_ACCOUNT'                    AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE gl_account NOT IN ('4100','4200','4300','5100','5200','6000','6100','7000','7100','8000');

-- 5b. Cost centre in master list
SELECT
    'source_transactions'                   AS table_name,
    'INVALID_COST_CENTRE'                   AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE cost_centre NOT IN ('CC001','CC002','CC003','CC004','CC005','CC006','CC007','CC008');

-- 5c. Valid legal entity codes
SELECT
    'source_transactions'                   AS table_name,
    'INVALID_ENTITY_CODE'                   AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE entity NOT IN ('GB_CORP','GB_RETAIL','GB_SERVICES','IE_CORP','DE_CORP');

-- ----------------------------------------------------------------------------
-- 6. CROSS-SYSTEM RECONCILIATION
-- ----------------------------------------------------------------------------

-- 6a. Source records missing from target
SELECT
    s.transaction_id, s.posting_date, s.gl_account,
    s.net_amount, s.transaction_type,
    'MISSING_FROM_TARGET'                   AS discrepancy_type
FROM source_transactions s
LEFT JOIN target_ledger t ON s.transaction_id = t.source_ref
WHERE t.source_ref IS NULL
ORDER BY s.posting_date;

-- 6b. Orphan target records (no matching source)
SELECT
    t.ledger_id, t.source_ref, t.posting_date, t.net_amount,
    'ORPHAN_TARGET_RECORD'                  AS discrepancy_type
FROM target_ledger t
LEFT JOIN source_transactions s ON t.source_ref = s.transaction_id
WHERE s.transaction_id IS NULL
ORDER BY t.posting_date;

-- 6c. Value discrepancies between matched records (> 1p tolerance)
SELECT
    s.transaction_id, s.posting_date, s.gl_account,
    s.net_amount                            AS source_net,
    t.net_amount                            AS target_net,
    ABS(s.net_amount - t.net_amount)        AS delta_abs,
    'VALUE_DISCREPANCY'                     AS discrepancy_type
FROM source_transactions s
JOIN target_ledger t ON s.transaction_id = t.source_ref
WHERE ABS(s.net_amount - t.net_amount) > 0.01
ORDER BY delta_abs DESC;

-- 6d. Overall reconciliation summary
SELECT
    COUNT(DISTINCT s.transaction_id)                                AS total_source,
    COUNT(DISTINCT t.source_ref)                                    AS total_matched,
    COUNT(DISTINCT s.transaction_id) - COUNT(DISTINCT t.source_ref) AS unreconciled,
    ROUND(100.0 * COUNT(DISTINCT t.source_ref)
          / COUNT(DISTINCT s.transaction_id), 1)                    AS recon_rate_pct
FROM source_transactions s
LEFT JOIN target_ledger t ON s.transaction_id = t.source_ref;

-- ----------------------------------------------------------------------------
-- 7. FINANCIAL BALANCE CHECKS
-- ----------------------------------------------------------------------------

-- 7a. Debit/credit balance by entity
SELECT
    entity,
    ROUND(SUM(debit_amount), 2)             AS total_debits,
    ROUND(SUM(credit_amount), 2)            AS total_credits,
    ROUND(ABS(SUM(debit_amount) - SUM(credit_amount)), 2) AS imbalance,
    CASE WHEN ABS(SUM(debit_amount) - SUM(credit_amount)) < 0.01
         THEN 'BALANCED' ELSE 'IMBALANCED' END AS balance_status
FROM source_transactions
GROUP BY entity
ORDER BY entity;

-- 7b. Journal batch double-entry integrity
SELECT
    batch_id,
    ROUND(SUM(debit_amount), 2)             AS batch_debits,
    ROUND(SUM(credit_amount), 2)            AS batch_credits,
    ROUND(ABS(SUM(debit_amount) - SUM(credit_amount)), 2) AS imbalance
FROM source_transactions
WHERE transaction_type = 'JOURNAL'
GROUP BY batch_id
HAVING ABS(SUM(debit_amount) - SUM(credit_amount)) > 0.01
ORDER BY imbalance DESC;

-- ----------------------------------------------------------------------------
-- 8. AUDIT TRAIL
-- ----------------------------------------------------------------------------

-- 8a. POSTED transactions must have a created_by user
SELECT
    'source_transactions'                   AS table_name,
    'POSTED_WITHOUT_USER'                   AS check_name,
    COUNT(*)                                AS failing_rows,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM source_transactions
WHERE status = 'POSTED'
  AND (created_by IS NULL OR created_by = '');

-- 8b. Singleton batches — potential incomplete postings
SELECT batch_id, COUNT(*) AS record_count, 'SINGLETON_BATCH' AS flag
FROM source_transactions
GROUP BY batch_id
HAVING COUNT(*) = 1
ORDER BY batch_id;

-- =============================================================================
-- END
-- =============================================================================
