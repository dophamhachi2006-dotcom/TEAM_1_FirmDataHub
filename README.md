# TEAM_1_FirmDataHub
## Firm Data Hub — Vietnamese Listed Companies Data Warehouse (2020–2024)

> A centralized data warehouse system for storing, validating, versioning, and exporting panel data of 20 Vietnamese listed companies across 5 fiscal years (2020–2024). The system implements a DIM + FACT + SNAPSHOT architecture in MySQL, with a fully automated Python ETL pipeline covering data import, quality control, and clean dataset export.

---

## Project Links

| Resource | Link |
|----------|------|
| 📁 Google Drive |  [TEAM_1_FirmDataHub](https://drive.google.com/drive/folders/1nyb7k94UrB86AAAZ2pCeSYMmFfcq6yuG?usp=sharing) |
| 💻 GitHub Repository | [TEAM_1_FirmDataHub](https://github.com/dophamhachi2006-dotcom/TEAM_1_FirmDataHub.git) |

---

## Team Members

| Full Name | Student ID | Role | Contribution (%) | Detailed Contribution |
|-----------|------------|------|------------------|-----------------------|
| Đỗ Phạm Hà Chi | 11245851 | Lead, Data Collection, Database Design, Pipeline Enhancement | 30% | As the team lead, responsible for overall project coordination, data collection, and database schema design. Enhanced `run_pipeline.py` to automate the entire ETL workflow. Participated in the development of all ETL modules including `import_firms.py`, `create_snapshot.py`, `import_panel.py`, `qc_checks.py`, and `export_panel.py`. |
| Trần Phương Linh | 11223797 | Data Collection, Snapshot Management, SQL Schema, Pipeline Refinement | 24% | Contributed to data collection and refinement of `create_snapshot.py` to ensure proper versioning and snapshot tracking. Assisted in designing and refining `schema_and_seed.sql` for database structure. Enhanced `run_pipeline.py` for improved workflow execution. |
| Nguyễn Thị Dương | 11245866 | Data Collection, View Development, Quality Control, Export Enhancement | 23.3% | Focused on data collection and validation. Developed and refined database views (including `vw_firm_panel_latest`) to support analytical queries. Enhanced `qc_checks.py` by implementing 15 comprehensive data quality rules and generating quality reports. Improved `export_panel.py` to log value overrides via `fact_value_override_log` — whenever a re-import changes an existing value, the old and new values are recorded with timestamp and source, enabling full audit trail of data corrections across pipeline runs. |
| Trần Thị Nhật Khánh | 11245886 | Data Collection, Snapshot Management | 22.7% | Responsible for data collection and validation. Contributed to the development and refinement of `create_snapshot.py`, ensuring proper snapshot creation and linkage with data sources. Supported versioning implementation across the pipeline. |
| **Total** | | | **100%** | |

---

## 20 Tickers
ASG, CMX, EVE, SFI, C32, CTF, HTG, SHI, CAP, DHA,
INN, SMC, CLC, DNP, KSB, VFG, CLL, DPR, NNC, WCS

---

## Project Structure
```
TEAM_1_FirmDataHub/
├── sql/
│   ├── schema_and_seed.sql       # Creates DB, all tables, seeds 20 firms
│   └── views.sql                 # Creates view vw_firm_panel_latest
├── etl/
│   ├── db_config.py              # MySQL connection config
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
- MySQL 8.0+ (running on localhost)

---

## How to Run

### Step 1 — Install Python dependencies

```bash
# Create and activate virtual environment (recommended)
python -m venv venv

venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# Install dependencies
pip install -r requirements.txt
```

---

### Step 2 — Run the pipeline

```bash
python run_pipeline.py
```

The pipeline will prompt for your MySQL password once at startup:

```
  Nhập password MySQL (user: root): _
```

Then it runs all steps automatically — **no need to open MySQL Workbench**:

| Step | Description |
|------|-------------|
| 0a | Run `schema_and_seed.sql` — create DB, all tables, seed 20 firms |
| 0b | Run `views.sql` — create `vw_firm_panel_latest` |
| A  | Import/update 20 firms into `dim_firm` |
| B  | Create 15 snapshots (3 sources × 5 years) |
| C  | Import 100 rows × 38 variables into FACT tables |
| D  | Run 15 QC rules → `outputs/qc_report.csv` |
| E  | Export clean dataset → `outputs/panel_latest.csv` |

**Other run options:**

```bash
# Drop existing DB and re-run everything from scratch
python run_pipeline.py --reset

# Skip import steps (only run QC + export)
python run_pipeline.py --skip-import
```

---

### Step 3 — Verify Results

| Output file | Expected |
|-------------|----------|
| `outputs/panel_latest.csv` | 100 rows × 43 columns |
| `outputs/qc_report.csv` | List of data quality issues (if any) |

Quick checks in MySQL Workbench:
```sql
SELECT COUNT(*) FROM vw_firm_panel_latest;   -- expected: 100

SELECT ticker, fiscal_year, net_sales, total_assets, net_income
FROM vw_firm_panel_latest
LIMIT 10;
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
- **Restatement policy**: Several companies had prior-year figures restated in subsequent annual reports (e.g. a figure reported in the 2021 BCTC may differ from the same figure as restated in the 2022 BCTC). Where restatements were identified, the **originally reported figure for that fiscal year** was retained rather than the restated version, to maintain consistency with the year-specific filing. This follows standard practice in empirical finance research on Vietnamese listed companies.
- **Ownership**: decimal ratio 0–1 (e.g. 5% = 0.05)
- **growth_ratio**: computed as (net_sales_t − net_sales_t-1) / net_sales_t-1; year 2020 uses 2019 comparative figures from the 2020 audited financial statements
- **dividend_cash_paid**: recorded as negative values following Vietnamese cash flow statement convention (cash outflows shown in parentheses); the absolute value represents actual dividends paid to shareholders
- **R&D expenditure**: NULL for all firms — none of the 20 companies reported formal R&D expenditure in their audited financial statements during 2020–2024
- **CAPEX**: Net CAPEX = |cash paid for fixed assets| − proceeds from disposals; floored at 0 per group convention
- **institutional_own**: excludes state ownership to avoid double-counting
- Some variables are NULL for service/transport firms (WCS, CLL, SFI): merchandise_purchase, WIP — not applicable to their business model
