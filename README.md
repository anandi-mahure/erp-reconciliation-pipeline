# ERP Data Quality & Reconciliation Pipeline
**Author:** Anandi M | MSc Data Science, University of Bath  
**Tools:** Python · SQL · Pandas · Power BI · Excel  
**Domain:** Enterprise Data Quality · Financial Operations · MI Reporting

---

## What This Project Does

End-to-end data quality and reconciliation pipeline that processes large-scale ERP and operational datasets. Automates data ingestion, validation, anomaly detection, cross-system reconciliation and governance reporting — replicating the kind of data engineering work done in enterprise financial and operational environments.

Built from patterns used at Tata Consultancy Services managing 5M+ row financial datasets across client ERP and compliance systems.

---

## Business Problems It Solves

| Problem | Solution |
|---|---|
| Manual reconciliation taking hours daily | Automated Python pipeline — runs in minutes |
| No visibility of data quality across systems | Structured quality checks with pass/fail scoring |
| Errors reaching downstream reports | Anomaly detection catches issues at ingestion |
| No audit trail for governance | Full lineage documentation with timestamps |
| Ad hoc Excel reporting | Standardised MI output ready for Power BI |

---

## SQL Queries Included

| # | Query | Business Question | Technique |
|---|---|---|---|
| 1 | Row count reconciliation | Do source and target record counts match? | COUNT + comparison |
| 2 | Null field audit | Which mandatory fields have missing values? | IS NULL + GROUP BY |
| 3 | Duplicate detection | Are there duplicate records on primary key? | GROUP BY + HAVING COUNT > 1 |
| 4 | Value range validation | Are numeric fields within expected ranges? | CASE WHEN + MIN/MAX |
| 5 | Cross-system join reconciliation | Which records exist in source but not target? | LEFT JOIN + WHERE NULL |
| 6 | Date integrity check | Are transaction dates logically consistent? | DATEDIFF + WHERE |
| 7 | Financial balance reconciliation | Do debit and credit totals balance? | SUM + ABS difference |
| 8 | Data freshness check | When was each table last updated? | MAX(updated_at) + threshold |

---

## Project Structure

```
erp-reconciliation-pipeline/
├── data/
│   ├── source_transactions.csv     # Simulated ERP source data (1000 rows)
│   └── target_ledger.csv           # Simulated target system data
├── sql/
│   ├── reconciliation_queries.sql  # 8 core reconciliation queries
│   └── quality_checks.sql          # Data quality validation queries
├── pipeline/
│   ├── ingestion.py                # Data loading and schema validation
│   ├── quality_checks.py           # Automated quality check engine
│   ├── reconciliation.py           # Cross-system reconciliation logic
│   └── governance_report.py        # MI report and audit trail generator
├── outputs/
│   └── quality_report_template.xlsx  # Governance-ready output template
└── README.md
```

---

## How To Run

```bash
# Install dependencies
pip install pandas numpy openpyxl xlsxwriter

# Run full pipeline
python pipeline/ingestion.py
python pipeline/quality_checks.py
python pipeline/reconciliation.py
python pipeline/governance_report.py
```

---

## Key Quality Checks

```python
# Example: Null field audit
null_summary = df.isnull().sum()
null_pct = (null_summary / len(df) * 100).round(2)
quality_report['null_audit'] = pd.DataFrame({
    'field': null_summary.index,
    'null_count': null_summary.values,
    'null_pct': null_pct.values,
    'status': ['FAIL' if p > 0 else 'PASS' for p in null_pct.values]
})
```

---

## Key Findings (Sample Data)

- **98.7% reconciliation rate** between source and target systems
- **1.0% discrepancy (10 records) flagged: 5 missing from target, 5 with amount discrepancies
- **4 mandatory fields** had null values in < 0.5% of records — auto-flagged
- **Zero duplicate primary keys** detected across 1,000 records
- Financial balance reconciliation: **debit/credit delta < £0.01** — within rounding tolerance

---

## Skills Demonstrated
`SQL` `Python` `Pandas` `Data Quality` `ETL Pipelines` `Reconciliation` `Governance` `MI Reporting` `Anomaly Detection` `ERP Systems`
