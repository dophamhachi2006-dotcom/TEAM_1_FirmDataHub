"""
export_panel.py
---------------
Script E: Xuất dataset panel "sạch" ra file CSV đẹp.
Output: outputs/panel_latest.csv
        (ticker, fiscal_year + 38 biến, snapshot mới nhất mỗi firm-year)

Cách chạy:
    python etl/export_panel.py
    python etl/export_panel.py --out outputs/panel_latest.csv
"""

import argparse
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_connection

# Thứ tự cột xuất ra — đúng theo 38 biến trong đề bài
COLUMN_ORDER = [
    # Định danh
    'ticker', 'company_name', 'exchange_code', 'industry_l2_name', 'fiscal_year',
    # (1-4) Ownership
    'managerial_inside_own', 'state_own', 'institutional_own', 'foreign_own',
    # (5) Market — shares
    'shares_outstanding',
    # (6-7) Financial — Revenue & Assets
    'net_sales', 'total_assets',
    # (8-9) Expenses
    'selling_expenses', 'general_admin_expenses',
    # (10-18) Cost structure
    'intangible_assets_net', 'manufacturing_overhead', 'net_operating_income',
    'raw_material_consumption', 'merchandise_purchase_year', 'wip_goods_purchase',
    'outside_manufacturing_expenses', 'production_cost', 'rnd_expenses',
    # (19-20) Innovation
    'product_innovation', 'process_innovation', 'evidence_note',
    # (21-22) Income & Equity
    'net_income', 'total_equity',
    # (23) Market cap
    'share_price', 'market_value_equity',
    # (24) Liabilities
    'total_liabilities',
    # (25-27) Cashflow
    'net_cfo', 'capex', 'net_cfi',
    # (28-33) Balance Sheet
    'cash_and_equivalents', 'long_term_debt', 'current_assets',
    'current_liabilities', 'growth_ratio', 'inventory',
    # (34-35) Market — Dividend & EPS
    'dividend_cash_paid', 'eps_basic',
    # (36) Employees
    'employees_count',
    # (37) PPE
    'net_ppe',
    # (38) Firm age
    'firm_age',
]

# Tên hiển thị đẹp cho từng cột (header tiếng Anh chuẩn)
COLUMN_LABELS = {
    'ticker':                          'StockCode',
    'company_name':                    'Company',
    'exchange_code':                   'Exchange',
    'industry_l2_name':                'Industry',
    'fiscal_year':                     'YearEnd',
    'managerial_inside_own':           'Managerial/Inside ownership',
    'state_own':                       'State ownership',
    'institutional_own':               'Institutional ownership',
    'foreign_own':                     'Foreign ownership',
    'shares_outstanding':              'Total share outstanding',
    'net_sales':                       'Net sales revenue',
    'total_assets':                    'Total assets',
    'selling_expenses':                'Selling expenses',
    'general_admin_expenses':          'General and administrative expenditure',
    'intangible_assets_net':           'Value of intangible assets',
    'manufacturing_overhead':          'Manufacturing overhead (Indirect cost)',
    'net_operating_income':            'Net operating income',
    'raw_material_consumption':        'Consumption of raw material',
    'merchandise_purchase_year':       'Merchandise purchase of the year',
    'wip_goods_purchase':              'Work-in-progress goods purchase',
    'outside_manufacturing_expenses':  'Outside manufacturing expenses',
    'production_cost':                 'Production cost',
    'rnd_expenses':                    'R&D expenditure',
    'product_innovation':              'Product innovation',
    'process_innovation':              'Process innovation',
    'evidence_note':                   'Evidence note',
    'net_income':                      'Net Income',
    'total_equity':                    "Total shareholders' equity",
    'share_price':                     'Share price',
    'market_value_equity':             'Market value of equity',
    'total_liabilities':               'Total liabilities',
    'net_cfo':                         'Net cash from operating activities',
    'capex':                           'Capital expenditure',
    'net_cfi':                         'Cash flows from investing activities',
    'cash_and_equivalents':            'Cash and cash equivalent',
    'long_term_debt':                  'Long-term debt',
    'current_assets':                  'Current assets',
    'current_liabilities':             'Current liabilities',
    'growth_ratio':                    'Growth ratio',
    'inventory':                       'Total inventory',
    'dividend_cash_paid':              'Divident payment',
    'eps_basic':                       'EPS',
    'employees_count':                 'Number of employees',
    'net_ppe':                         'Net plant, property and equipment',
    'firm_age':                        'Firm age',
}


def main(out_path: str = 'outputs/panel_latest.csv'):
    print(f"\n{'='*60}")
    print(f"  export_panel.py  |  output: {out_path}")
    print(f"{'='*60}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Lấy data từ view
    try:
        conn = get_connection()
        df   = pd.read_sql("SELECT * FROM vw_firm_panel_latest", conn)
        conn.close()
    except Exception as e:
        print(f"[ERROR] Không kết nối được DB: {e}")
        sys.exit(1)

    if df.empty:
        print("[WARN] Không có dữ liệu trong vw_firm_panel_latest!")
        return

    # Sắp xếp cột theo thứ tự chuẩn
    available = [c for c in COLUMN_ORDER if c in df.columns]
    df = df[available]

    # Sắp xếp dòng theo ticker + năm
    df = df.sort_values(['ticker', 'fiscal_year']).reset_index(drop=True)

    # Chia tỷ cho các cột tiền tệ (DB lưu đơn vị đồng, xuất ra tỷ đồng)
    MONEY_COLS = [
        'net_sales', 'total_assets', 'selling_expenses', 'general_admin_expenses',
        'intangible_assets_net', 'manufacturing_overhead', 'net_operating_income',
        'raw_material_consumption', 'merchandise_purchase_year', 'wip_goods_purchase',
        'outside_manufacturing_expenses', 'production_cost', 'rnd_expenses',
        'net_income', 'total_equity', 'total_liabilities', 'cash_and_equivalents',
        'long_term_debt', 'current_assets', 'current_liabilities', 'inventory',
        'net_ppe', 'market_value_equity', 'dividend_cash_paid',
        'net_cfo', 'capex', 'net_cfi',
    ]
    for col in MONEY_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce') / 1_000_000_000

    # Đổi tên cột sang tên hiển thị đẹp
    df = df.rename(columns=COLUMN_LABELS)

    # Làm tròn số thập phân cho đẹp
    for col in df.columns:
        if df[col].dtype == 'float64':
            # Cột ratio → 6 chữ số
            if any(kw in col.lower() for kw in ['ownership', 'ratio', 'growth']):
                df[col] = df[col].round(6)
            # Cột tiền tệ lớn → 2 chữ số
            else:
                df[col] = df[col].round(2)

    # Xuất CSV với encoding utf-8-sig (Excel đọc được tiếng Việt)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')

    # In tóm tắt
    print(f"\n  ✓ Xuất thành công!")
    print(f"  Số dòng  : {len(df)} (kỳ vọng: 100 = 20 tickers × 5 năm)")
    print(f"  Số cột   : {len(df.columns)}")
    print(f"  File     : {out_path}")

    if len(df) != 100:
        print(f"\n  ⚠ CẢNH BÁO: Kỳ vọng 100 dòng nhưng chỉ có {len(df)}!")

    # Thống kê missing values
    missing = df.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if not missing.empty:
        print(f"\n  Các cột có giá trị thiếu (top 10):")
        for col, cnt in missing.head(10).items():
            pct = cnt / len(df) * 100
            print(f"    {col:<45}: {cnt:3d} missing ({pct:.0f}%)")

    # Preview
    print(f"\n  Preview 3 dòng đầu:")
    preview_cols = ['StockCode', 'YearEnd', 'Net sales revenue', 'Total assets', 'Net Income']
    preview_cols = [c for c in preview_cols if c in df.columns]
    print(df[preview_cols].head(3).to_string(index=False))
    print(f"{'='*60}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Xuất panel dataset sạch')
    parser.add_argument('--out', default='outputs/panel_latest.csv')
    args = parser.parse_args()
    main(args.out)
