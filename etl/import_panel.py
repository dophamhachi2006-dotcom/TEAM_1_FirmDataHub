"""
import_panel.py
--------------
Script C: Import 100 dòng panel data vào 6 bảng FACT.
Đọc file panel_2020_2024.xlsx — row 0 = header tiếng Anh, row 4 = db column name.

Snapshot mapping (theo nguồn dữ liệu):
  BCTC_Audited  → fact_financial_year, fact_cashflow_year, fact_ownership_year
  Vietstock     → fact_market_year
  AnnualReport  → fact_innovation_year, fact_firm_year_meta

Cách chạy:
    python etl/import_panel.py --file data/panel_2020_2024.xlsx
"""

import argparse
import sys
import math
import pandas as pd
import mysql.connector
from mysql.connector import Error

import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_connection


# ------------------------------------------------------------------ #
# Source → fact table mapping
# ------------------------------------------------------------------ #
# Mỗi nhóm fact table lấy snapshot_id từ nguồn tương ứng
TABLE_SOURCE = {
    'financial':  'BCTC_Audited',
    'cashflow':   'BCTC_Audited',
    'ownership':  'BCTC_Audited',
    'market':     'Vietstock',
    'innovation': 'AnnualReport',
    'meta':       'AnnualReport',
}

COL_TABLE = {
    # fact_ownership_year
    'managerial_inside_own':          'ownership',
    'state_own':                      'ownership',
    'institutional_own':              'ownership',
    'foreign_own':                    'ownership',
    # fact_market_year
    'shares_outstanding':             'market',
    'share_price':                    'market',
    'market_value_equity':            'market',
    'dividend_cash_paid':             'market',
    'eps_basic':                      'market',
    # fact_financial_year
    'net_sales':                      'financial',
    'total_assets':                   'financial',
    'selling_expenses':               'financial',
    'general_admin_expenses':         'financial',
    'intangible_assets_net':          'financial',
    'manufacturing_overhead':         'financial',
    'net_operating_income':           'financial',
    'raw_material_consumption':       'financial',
    'merchandise_purchase_year':      'financial',
    'wip_goods_purchase':             'financial',
    'outside_manufacturing_expenses': 'financial',
    'production_cost':                'financial',
    'rnd_expenses':                   'financial',
    'net_income':                     'financial',
    'total_equity':                   'financial',
    'total_liabilities':              'financial',
    'cash_and_equivalents':           'financial',
    'long_term_debt':                 'financial',
    'current_assets':                 'financial',
    'current_liabilities':            'financial',
    'growth_ratio':                   'financial',
    'inventory':                      'financial',
    'net_ppe':                        'financial',
    # fact_cashflow_year
    'net_cfo':                        'cashflow',
    'capex':                          'cashflow',
    'net_cfi':                        'cashflow',
    # fact_innovation_year
    'product_innovation':             'innovation',
    'process_innovation':             'innovation',
    'evidence_note':                  'innovation',
    # fact_firm_year_meta
    'employees_count':                'meta',
    'firm_age':                       'meta',
}


# ------------------------------------------------------------------ #
# Build snapshot lookup table: (source_name, fiscal_year) → snapshot_id
# ------------------------------------------------------------------ #
def build_snapshot_map(cursor) -> dict:
    """
    Trả về dict: (source_name, fiscal_year) → snapshot_id (lấy MAX nếu có nhiều).
    Dùng để tra cứu nhanh khi import từng dòng.
    """
    cursor.execute("""
        SELECT ds.source_name, fds.fiscal_year, MAX(fds.snapshot_id) AS snap_id
        FROM fact_data_snapshot fds
        JOIN dim_data_source ds ON ds.source_id = fds.source_id
        WHERE ds.source_name IN ('BCTC_Audited', 'Vietstock', 'AnnualReport')
        GROUP BY ds.source_name, fds.fiscal_year
    """)
    snap_map = {}
    for source_name, fiscal_year, snap_id in cursor.fetchall():
        snap_map[(source_name, int(fiscal_year))] = snap_id

    if not snap_map:
        print("[ERROR] Không tìm thấy snapshot nào trong DB.")
        print("        Hãy chạy Bước B (create_snapshot.py) trước.")
        sys.exit(1)

    print(f"\n  Snapshot map ({len(snap_map)} entries):")
    for (src, yr), sid in sorted(snap_map.items()):
        print(f"    {src:15s} {yr} → snapshot_id={sid}")

    return snap_map


def get_snap_id(snap_map: dict, table_group: str, year: int,
                ticker: str = '') -> int | None:
    """
    Tra snapshot_id đúng cho table_group × year.
    table_group: 'financial' | 'cashflow' | 'ownership' | 'market' | 'innovation' | 'meta'
    """
    source = TABLE_SOURCE[table_group]
    snap_id = snap_map.get((source, year))
    if snap_id is None:
        print(f"  [WARN] Không tìm thấy snapshot ({source}, {year})"
              f"{' cho ' + ticker if ticker else ''} — bỏ qua bảng {table_group}")
    return snap_id


# ------------------------------------------------------------------ #
# Helper
# ------------------------------------------------------------------ #
def safe(val):
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, str) and val.strip().upper() in ('NULL', 'NAN', ''):
        return None
    return val


def get_firm_id(cursor, ticker: str):
    cursor.execute("SELECT firm_id FROM dim_firm WHERE ticker = %s", (ticker,))
    row = cursor.fetchone()
    if not row:
        print(f"  [WARN] Không tìm thấy ticker '{ticker}' trong dim_firm — bỏ qua")
        return None
    return row[0]


# ------------------------------------------------------------------ #
# Override log
# ------------------------------------------------------------------ #
def get_existing_values(cursor, table: str, firm_id: int, year: int) -> dict:
    try:
        cursor.execute(f"""
            SELECT * FROM {table}
            WHERE firm_id = %s AND fiscal_year = %s
            ORDER BY snapshot_id DESC LIMIT 1
        """, (firm_id, year))
        row = cursor.fetchone()
        if not row:
            return {}
        cols = [d[0] for d in cursor.description]
        return dict(zip(cols, row))
    except:
        return {}


def log_overrides(cursor, firm_id: int, year: int,
                  table: str, old_vals: dict, new_row: dict,
                  changed_by: str = 'import_panel'):
    skip = {'firm_id', 'fiscal_year', 'snapshot_id', 'created_at',
            'unit_scale', 'currency_code', 'note'}
    for col, old_val in old_vals.items():
        if col in skip:
            continue
        new_val = safe(new_row.get(col))
        old_val_safe = old_val if old_val is not None else None
        if old_val_safe is None:
            continue
        try:
            same = (str(old_val_safe).strip() == str(new_val).strip()
                    if new_val is not None else old_val_safe is None)
        except:
            same = False
        if not same:
            try:
                cursor.execute("""
                    INSERT INTO fact_value_override_log
                        (firm_id, fiscal_year, table_name, column_name,
                         old_value, new_value, reason, changed_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    firm_id, year, table, col,
                    str(old_val_safe)[:255],
                    str(new_val)[:255] if new_val is not None else None,
                    'Re-import: gia tri thay doi so voi snapshot truoc',
                    changed_by
                ))
            except:
                pass


# ------------------------------------------------------------------ #
# Insert functions
# ------------------------------------------------------------------ #
def insert_ownership(cursor, firm_id, year, snap_id, row):
    cursor.execute("""
        INSERT INTO fact_ownership_year
            (firm_id, fiscal_year, snapshot_id,
             managerial_inside_own, state_own, institutional_own, foreign_own)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            managerial_inside_own = VALUES(managerial_inside_own),
            state_own             = VALUES(state_own),
            institutional_own     = VALUES(institutional_own),
            foreign_own           = VALUES(foreign_own)
    """, (firm_id, year, snap_id,
          safe(row.get('managerial_inside_own')),
          safe(row.get('state_own')),
          safe(row.get('institutional_own')),
          safe(row.get('foreign_own'))))


def insert_market(cursor, firm_id, year, snap_id, row):
    cursor.execute("""
        INSERT INTO fact_market_year
            (firm_id, fiscal_year, snapshot_id,
             shares_outstanding, share_price, market_value_equity,
             dividend_cash_paid, eps_basic)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            shares_outstanding  = VALUES(shares_outstanding),
            share_price         = VALUES(share_price),
            market_value_equity = VALUES(market_value_equity),
            dividend_cash_paid  = VALUES(dividend_cash_paid),
            eps_basic           = VALUES(eps_basic)
    """, (firm_id, year, snap_id,
          safe(row.get('shares_outstanding')),
          safe(row.get('share_price')),
          safe(row.get('market_value_equity')),
          safe(row.get('dividend_cash_paid')),
          safe(row.get('eps_basic'))))


def insert_financial(cursor, firm_id, year, snap_id, row):
    cursor.execute("""
        INSERT INTO fact_financial_year
            (firm_id, fiscal_year, snapshot_id, unit_scale, currency_code,
             net_sales, total_assets, selling_expenses, general_admin_expenses,
             intangible_assets_net, manufacturing_overhead, net_operating_income,
             raw_material_consumption, merchandise_purchase_year, wip_goods_purchase,
             outside_manufacturing_expenses, production_cost, rnd_expenses,
             net_income, total_equity, total_liabilities, cash_and_equivalents,
             long_term_debt, current_assets, current_liabilities,
             growth_ratio, inventory, net_ppe)
        VALUES (%s,%s,%s,1000000000,'VND',
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            net_sales                      = VALUES(net_sales),
            total_assets                   = VALUES(total_assets),
            selling_expenses               = VALUES(selling_expenses),
            general_admin_expenses         = VALUES(general_admin_expenses),
            intangible_assets_net          = VALUES(intangible_assets_net),
            manufacturing_overhead         = VALUES(manufacturing_overhead),
            net_operating_income           = VALUES(net_operating_income),
            raw_material_consumption       = VALUES(raw_material_consumption),
            merchandise_purchase_year      = VALUES(merchandise_purchase_year),
            wip_goods_purchase             = VALUES(wip_goods_purchase),
            outside_manufacturing_expenses = VALUES(outside_manufacturing_expenses),
            production_cost                = VALUES(production_cost),
            rnd_expenses                   = VALUES(rnd_expenses),
            net_income                     = VALUES(net_income),
            total_equity                   = VALUES(total_equity),
            total_liabilities              = VALUES(total_liabilities),
            cash_and_equivalents           = VALUES(cash_and_equivalents),
            long_term_debt                 = VALUES(long_term_debt),
            current_assets                 = VALUES(current_assets),
            current_liabilities            = VALUES(current_liabilities),
            growth_ratio                   = VALUES(growth_ratio),
            inventory                      = VALUES(inventory),
            net_ppe                        = VALUES(net_ppe)
    """, (firm_id, year, snap_id,
          safe(row.get('net_sales')),
          safe(row.get('total_assets')),
          safe(row.get('selling_expenses')),
          safe(row.get('general_admin_expenses')),
          safe(row.get('intangible_assets_net')),
          safe(row.get('manufacturing_overhead')),
          safe(row.get('net_operating_income')),
          safe(row.get('raw_material_consumption')),
          safe(row.get('merchandise_purchase_year')),
          safe(row.get('wip_goods_purchase')),
          safe(row.get('outside_manufacturing_expenses')),
          safe(row.get('production_cost')),
          safe(row.get('rnd_expenses')),
          safe(row.get('net_income')),
          safe(row.get('total_equity')),
          safe(row.get('total_liabilities')),
          safe(row.get('cash_and_equivalents')),
          safe(row.get('long_term_debt')),
          safe(row.get('current_assets')),
          safe(row.get('current_liabilities')),
          safe(row.get('growth_ratio')),
          safe(row.get('inventory')),
          safe(row.get('net_ppe'))))


def insert_cashflow(cursor, firm_id, year, snap_id, row):
    cursor.execute("""
        INSERT INTO fact_cashflow_year
            (firm_id, fiscal_year, snapshot_id, unit_scale, currency_code,
             net_cfo, capex, net_cfi)
        VALUES (%s,%s,%s,1000000000,'VND',%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            net_cfo = VALUES(net_cfo),
            capex   = VALUES(capex),
            net_cfi = VALUES(net_cfi)
    """, (firm_id, year, snap_id,
          safe(row.get('net_cfo')),
          safe(row.get('capex')),
          safe(row.get('net_cfi'))))


def insert_innovation(cursor, firm_id, year, snap_id, row):
    cursor.execute("""
        INSERT INTO fact_innovation_year
            (firm_id, fiscal_year, snapshot_id,
             product_innovation, process_innovation, evidence_note)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            product_innovation = VALUES(product_innovation),
            process_innovation = VALUES(process_innovation),
            evidence_note      = VALUES(evidence_note)
    """, (firm_id, year, snap_id,
          safe(row.get('product_innovation')),
          safe(row.get('process_innovation')),
          safe(row.get('evidence_note'))))


def insert_meta(cursor, firm_id, year, snap_id, row):
    cursor.execute("""
        INSERT INTO fact_firm_year_meta
            (firm_id, fiscal_year, snapshot_id,
             employees_count, firm_age)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            employees_count = VALUES(employees_count),
            firm_age        = VALUES(firm_age)
    """, (firm_id, year, snap_id,
          safe(row.get('employees_count')),
          safe(row.get('firm_age'))))


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #
def main(filepath: str):
    print(f"\n{'='*60}")
    print(f"  import_panel.py  |  file: {filepath}")
    print(f"  Snapshot IDs sẽ được tra tự động theo nguồn × năm")
    print(f"{'='*60}")

    try:
        raw = pd.read_excel(filepath, header=None)
    except FileNotFoundError:
        print(f"[ERROR] Không tìm thấy file: {filepath}")
        sys.exit(1)

    # Tìm row chứa tên db column
    db_col_row = None
    ticker_col = None
    year_col   = None

    for ri in range(min(10, len(raw))):
        row_vals = [str(v).strip().lower() if v else '' for v in raw.iloc[ri]]
        if 'ticker' in row_vals:
            db_col_row = ri
            ticker_col = row_vals.index('ticker')
            year_col   = row_vals.index('fiscal_year') if 'fiscal_year' in row_vals else None
            break
        if 'stockcode' in row_vals:
            db_col_row = ri
            ticker_col = row_vals.index('stockcode')
            year_col   = row_vals.index('yearend') if 'yearend' in row_vals else None
            break

    if db_col_row is None:
        print("[ERROR] Không tìm thấy hàng tên cột (ticker/StockCode) trong file Excel")
        sys.exit(1)

    db_cols = [str(v).strip().lower() if v else '' for v in raw.iloc[db_col_row]]

    EN_TO_DB = {
        'stockcode':                              'ticker',
        'yearend':                                'fiscal_year',
        'managerial/inside ownership':            'managerial_inside_own',
        'state ownership':                        'state_own',
        'institutional ownership':                'institutional_own',
        'foreign ownership':                      'foreign_own',
        'total share outstanding':                'shares_outstanding',
        'share price':                            'share_price',
        'market value of equity':                 'market_value_equity',
        'divident payment':                       'dividend_cash_paid',
        'eps':                                    'eps_basic',
        'net sales revenue':                      'net_sales',
        'total assets':                           'total_assets',
        'selling expenses':                       'selling_expenses',
        'general and administrative expenditure': 'general_admin_expenses',
        'general and admin. expenditure':         'general_admin_expenses',
        'value of intangible assets':             'intangible_assets_net',
        'manufacturing overhead (indirect cost)': 'manufacturing_overhead',
        'manufacturing overhead':                 'manufacturing_overhead',
        'net operating income':                   'net_operating_income',
        'consumption of raw material':            'raw_material_consumption',
        'merchandise purchase of the year':       'merchandise_purchase_year',
        'merchandise purchase of year':           'merchandise_purchase_year',
        'work-in-progess goods purchase':         'wip_goods_purchase',
        'work-in-progress goods purchase':        'wip_goods_purchase',
        'outside manufacturing expenses':         'outside_manufacturing_expenses',
        'production cost':                        'production_cost',
        'r&d expenditure':                        'rnd_expenses',
        'net income':                             'net_income',
        "total shareholders' equity":             'total_equity',
        'total liabilities':                      'total_liabilities',
        'cash and cash equivalent':               'cash_and_equivalents',
        'long-term debt':                         'long_term_debt',
        'current assets':                         'current_assets',
        'current liabiltiies':                    'current_liabilities',
        'current liabilities':                    'current_liabilities',
        'growth ratio':                           'growth_ratio',
        'total inventory':                        'inventory',
        'net plant, property and equipment':      'net_ppe',
        'net cash from operating activities':     'net_cfo',
        'capital expenditure':                    'capex',
        'cash flows from investing activities':   'net_cfi',
        'product innovation':                     'product_innovation',
        'process innovation':                     'process_innovation',
        'evidence note':                          'evidence_note',
        'number of employees':                    'employees_count',
        'firm age':                               'firm_age',
    }
    db_cols = [EN_TO_DB.get(c, c) for c in db_cols]

    try:
        ticker_col = db_cols.index('ticker')
        year_col   = db_cols.index('fiscal_year')
    except ValueError:
        print("[ERROR] Không tìm thấy cột ticker/fiscal_year sau khi map")
        sys.exit(1)

    # Bỏ qua hàng unit
    data_start = db_col_row + 1
    for ri in range(data_start, min(data_start + 3, len(raw))):
        first = str(raw.iloc[ri, ticker_col]).strip()
        if first and first.lower() not in (
                'nan', 'ratio 0-1', 'shares', 'bil.vnd',
                'ratio', '0 or 1', 'text', 'persons', 'years'):
            data_start = ri
            break

    data = raw.iloc[data_start:].reset_index(drop=True)

    # Kết nối DB và build snapshot map
    try:
        conn   = get_connection()
        cursor = conn.cursor()
    except Error as e:
        print(f"[ERROR] Không kết nối được DB: {e}")
        sys.exit(1)

    snap_map = build_snapshot_map(cursor)

    ok = errors = 0

    for i in range(len(data)):
        ticker = str(data.iloc[i, ticker_col]).strip().upper()
        if not ticker or ticker in ('NAN', ''):
            continue

        try:
            year = int(float(str(data.iloc[i, year_col])))
        except:
            continue

        row = {}
        for ci, col in enumerate(db_cols):
            if col and ci < len(data.columns):
                row[col] = data.iloc[i, ci]

        firm_id = get_firm_id(cursor, ticker)
        if not firm_id:
            errors += 1
            continue

        # Tra snapshot_id riêng cho từng nhóm bảng
        snap_bctc   = get_snap_id(snap_map, 'financial',  year, ticker)
        snap_viet   = get_snap_id(snap_map, 'market',     year, ticker)
        snap_annual = get_snap_id(snap_map, 'innovation', year, ticker)

        try:
            old_fin  = get_existing_values(cursor, 'fact_financial_year',  firm_id, year)
            old_own  = get_existing_values(cursor, 'fact_ownership_year',  firm_id, year)
            old_mkt  = get_existing_values(cursor, 'fact_market_year',     firm_id, year)
            old_cf   = get_existing_values(cursor, 'fact_cashflow_year',   firm_id, year)
            old_inv  = get_existing_values(cursor, 'fact_innovation_year', firm_id, year)
            old_meta = get_existing_values(cursor, 'fact_firm_year_meta',  firm_id, year)

            if snap_bctc:
                insert_financial(cursor,  firm_id, year, snap_bctc, row)
                insert_cashflow(cursor,   firm_id, year, snap_bctc, row)
                insert_ownership(cursor,  firm_id, year, snap_bctc, row)
            if snap_viet:
                insert_market(cursor,     firm_id, year, snap_viet, row)
            if snap_annual:
                insert_innovation(cursor, firm_id, year, snap_annual, row)
                insert_meta(cursor,       firm_id, year, snap_annual, row)

            # Log bất kỳ giá trị nào thay đổi
            changed_by = 'import_panel'
            if snap_bctc:
                log_overrides(cursor, firm_id, year, 'fact_financial_year', old_fin,  row, changed_by)
                log_overrides(cursor, firm_id, year, 'fact_ownership_year', old_own,  row, changed_by)
                log_overrides(cursor, firm_id, year, 'fact_cashflow_year',  old_cf,   row, changed_by)
            if snap_viet:
                log_overrides(cursor, firm_id, year, 'fact_market_year',    old_mkt,  row, changed_by)
            if snap_annual:
                log_overrides(cursor, firm_id, year, 'fact_innovation_year',old_inv,  row, changed_by)
                log_overrides(cursor, firm_id, year, 'fact_firm_year_meta', old_meta, row, changed_by)

            conn.commit()
            print(f"  ✓ {ticker} {year}  "
                  f"[bctc={snap_bctc} | viet={snap_viet} | annual={snap_annual}]")
            ok += 1
        except Error as e:
            conn.rollback()
            print(f"  [ERROR] {ticker} {year}: {e}")
            errors += 1

    cursor.close()
    conn.close()

    print(f"\n{'='*60}")
    print(f"  Hoàn thành: {ok} dòng OK | {errors} lỗi")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import panel data vào DB')
    parser.add_argument('--file', default='data/panel_2020_2024.xlsx')
    args = parser.parse_args()
    main(args.file)
