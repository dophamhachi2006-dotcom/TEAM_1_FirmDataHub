# TEAM_X_FirmDataHub
## Firm Data Hub — Vietnamese Listed Companies Data Warehouse (2020–2024)

---

## Team Members

| Full Name | Student ID | Role | Contribution |
|-----------|------------|------|--------------|
| | | | |
| | | | |
| | | | |
| | | | |

---

## 20 Tickers
ASG, CMX, EVE, SFI, C32, CTF, HTG, SHI, CAP, DHA,
INN, SMC, CLC, DNP, KSB, VFG, CLL, DPR, NNC, WCS

---

## Project Structure
```
TEAM_X_FirmDataHub/
├── sql/
│   ├── schema_and_seed.sql       # Creates DB, all tables, seeds 20 firms
│   └── views.sql                 # Creates view vw_firm_panel_latest
├── etl/
│   ├── db_config.py              # MySQL connection config (reads from .env)
│   ├── import_firms.py           # Script A: Import/update firm directory
│   ├── create_snapshot.py        # Script B: Create version snapshots
│   ├── import_panel.py           # Script C: Import 38 variables into FACT tables
│   ├── qc_checks.py              # Script D: Data quality checks (15 rules)
│   └── export_panel.py           # Script E: Export clean panel dataset
├── data/
│   ├── team_tickers.csv          # 20 tickers
│   ├── firms.xlsx                # Firm directory (20 companies)
│   └── panel_2020_2024.xlsx      # Panel data: 38 variables × 20 tickers × 5 years
├── outputs/
│   ├── qc_report.csv             # Data quality report
│   └── panel_latest.csv          # Clean panel dataset for analysis
├── run_pipeline.py               # Run the full pipeline in one command
├── requirements.txt              # Python dependencies
└── README.md
```

---

## Requirements
- Python 3.8+
- MySQL 8.0+
- MySQL Workbench

---

## How to Run

### Step 1 — Install Python Dependencies

```bash
# Create and activate virtual environment (recommended)
python -m venv venv

venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# Install dependencies
pip install -r requirements.txt
```

---

### Step 2 — Initialize the Database

> ⚠️ If you have an existing database from a previous run, drop it first in MySQL Workbench:
> ```sql
> DROP DATABASE IF EXISTS vn_firm_hub;
> ```

**2a. Run schema_and_seed.sql**

Open MySQL Workbench → File → Open SQL Script → select `sql/schema_and_seed.sql`
→ Press **Ctrl+A** to select all → Press **Ctrl+Shift+Enter** to execute.

Verify:
```sql
SELECT COUNT(*) FROM dim_firm;      -- expected: 20
SELECT COUNT(*) FROM dim_exchange;  -- expected: 3
```

**2b. Run views.sql**

Open `sql/views.sql` → Ctrl+A → Ctrl+Shift+Enter.

Verify:
```sql
SHOW FULL TABLES IN vn_firm_hub WHERE TABLE_TYPE = 'VIEW';
-- expected: vw_firm_panel_latest
```

---

### Step 3 — Run the Pipeline

```bash
python run_pipeline.py
```

The pipeline will prompt for your MySQL password at startup — no `.env` file needed:

```
  Nhập password MySQL (user: root): _
```

The pipeline runs automatically in order:

| Step | Script | Description |
|------|--------|-------------|
| A | import_firms.py | Import/update 20 firms into dim_firm |
| B | create_snapshot.py | Create 15 snapshots (3 sources × 5 years) |
| C | import_panel.py | Import 100 rows × 38 variables into FACT tables |
| D | qc_checks.py | Run 15 QC rules → outputs/qc_report.csv |
| E | export_panel.py | Export clean dataset → outputs/panel_latest.csv |

**Other run options:**

```bash
# Reset the entire DB then re-run from scratch
python run_pipeline.py --reset

# Skip import steps (only run QC + export)
python run_pipeline.py --skip-import
```

---

### Step 4 — Verify Results

| Output file | Expected |
|-------------|----------|
| `outputs/panel_latest.csv` | 100 rows × 43 columns |
| `outputs/qc_report.csv` | List of data quality issues (if any) |

Quick checks in MySQL Workbench:
```sql
-- Panel row count (expected: 100)
SELECT COUNT(*) FROM vw_firm_panel_latest;

-- Preview data
SELECT ticker, fiscal_year, net_sales, total_assets, net_income
FROM vw_firm_panel_latest
LIMIT 10;

-- Snapshot summary (expected: 15)
SELECT ds.source_name, fds.fiscal_year, fds.version_tag
FROM fact_data_snapshot fds
JOIN dim_data_source ds ON ds.source_id = fds.source_id
ORDER BY ds.source_name, fds.fiscal_year;
```

---

## Data Sources

| Variable Group | Variables | Source |
|----------------|-----------|--------|
| Ownership (1–4) | managerial, state, institutional, foreign ownership | Annual Reports — major shareholder lists |
| Market (5, 23, 34–35) | shares outstanding, share price, market cap, dividend, EPS | Vietstock.vn |
| Financial (6–18, 21–22, 24, 28–33, 37) | net sales, total assets, expenses, equity, liabilities... | Audited financial statements — Cafef.vn / cophieu68.vn |
| Cashflow (25–27) | net CFO, CAPEX, net CFI | Cash flow statements |
| Innovation (19–20) | product innovation, process innovation, evidence note | Annual Reports — manually coded |
| Meta (36, 38) | employees count, firm age | Annual Reports / company profiles |

---

## QC Rules (15 rules)

| # | Rule | Field | Severity |
|---|------|-------|----------|
| 1 | Ownership ratios ∈ [0, 1] | managerial/state/institutional/foreign_own | ERROR |
| 2 | Sum of 4 ownership ratios ≤ 1 | ownership_sum | ERROR |
| 3 | shares_outstanding > 0 | shares_outstanding | ERROR |
| 4 | total_assets ≥ 0 | total_assets | ERROR |
| 5 | current_liabilities ≥ 0 | current_liabilities | ERROR |
| 6 | growth_ratio ∈ [−0.95, 5.0] | growth_ratio | WARNING |
| 7 | market_value_equity ≈ shares × price (±5%) | market_value_equity | WARNING |
| 8 | capex ≥ 0 | capex | WARNING |
| 9 | net_sales ≥ 0 | net_sales | WARNING |
| 10 | firm_age ∈ [1, 100] | firm_age | WARNING |
| 11 | cash_and_equivalents ≤ current_assets | cash_and_equivalents | ERROR |
| 12 | rnd_expenses ≤ net_sales | rnd_expenses | WARNING |
| 13 | employees_count > 0 and is an integer | employees_count | ERROR |
| 14 | managerial_inside_own ≤ 80% | managerial_inside_own | WARNING |
| 15 | current_liabilities + long_term_debt ≤ total_liabilities | liabilities_breakdown | ERROR |

---

## Data Notes
- **Currency unit**: billion VND (VND × 10⁹) in exported CSV; DB stores raw VND (unit_scale = 1,000,000,000)
- **Ownership**: decimal ratio 0–1 (e.g. 5% = 0.05)
- **growth_ratio**: NULL for fiscal year 2020 (no prior year to compute)
- **CAPEX**: Net CAPEX = |cash paid for fixed assets| − proceeds from disposals; floored at 0 per group convention
- **institutional_own**: excludes state ownership to avoid double-counting
- Some variables are NULL for service/transport firms (WCS, CLL, SFI): merchandise_purchase, WIP — not applicable to their business model
