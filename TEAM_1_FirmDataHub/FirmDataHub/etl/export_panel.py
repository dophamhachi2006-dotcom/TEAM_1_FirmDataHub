"""
export_panel.py
---------------
Script E: Xuất dataset panel "sạch" ra file CSV.
Output: outputs/panel_latest.csv
        (ticker, fiscal_year + 38 biến, snapshot mới nhất mỗi firm-year)

Cách chạy:
    python etl/export_panel.py
    python etl/export_panel.py --out outputs/panel_latest.csv
"""

import argparse
import os
import pandas as pd
from db_config import get_connection


# Thứ tự cột xuất ra — đúng theo 38 biến trong đề bài
COLUMN_ORDER = [
    # Định danh
    'ticker', 'company_name', 'exchange_code', 'industry_l2_name', 'fiscal_year',
    # (1-4) Ownership
    'managerial_inside_own', 'state_own', 'institutional_own', 'foreign_own',
    # (5) Market — shares
    'shares_outstanding',
    # (6-18) Financial — P&L & Balance Sheet phần 1
    'net_sales', 'total_assets', 'selling_expenses', 'general_admin_expenses',
    'intangible_assets_net', 'manufacturing_overhead', 'net_operating_income',
    'raw_material_consumption', 'merchandise_purchase_year', 'wip_goods_purchase',
    'outside_manufacturing_expenses', 'production_cost', 'rnd_expenses',
    # (19-20) Innovation
    'product_innovation', 'process_innovation',
    # (21-22) Financial — Income & Equity
    'net_income', 'total_equity',
    # (23) Market — market cap
    'share_price', 'market_value_equity',
    # (24) Liabilities
    'total_liabilities',
    # (25-27) Cashflow
    'net_cfo', 'capex', 'net_cfi',
    # (28-33) Financial — Balance Sheet phần 2
    'cash_and_equivalents', 'long_term_debt', 'current_assets',
    'current_liabilities', 'growth_ratio', 'inventory',
    # (34-35) Market — Dividend & EPS
    'dividend_cash_paid', 'eps_basic',
    # (36) Meta — Employees
    'employees_count',
    # (37) Financial — PPE
    'net_ppe',
    # (38) Meta — Firm age
    'firm_age',
    # Extra
    'evidence_note',
]


def main(out_path: str = 'outputs/panel_latest.csv'):
    print(f"\n{'='*60}")
    print(f"  export_panel.py  |  output: {out_path}")
    print(f"{'='*60}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    conn = get_connection()
    df   = pd.read_sql("SELECT * FROM vw_firm_panel_latest", conn)
    conn.close()

    if df.empty:
        print("[WARN] Không có dữ liệu trong vw_firm_panel_latest!")
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
        return

    # Sắp xếp cột theo thứ tự chuẩn
    available = [c for c in COLUMN_ORDER if c in df.columns]
    extra     = [c for c in df.columns if c not in COLUMN_ORDER]
    df = df[available + extra]

    # Sắp xếp dòng
    df = df.sort_values(['ticker', 'fiscal_year']).reset_index(drop=True)

    df.to_csv(out_path, index=False, encoding='utf-8-sig')

    print(f"\n  ✓ Xuất thành công!")
    print(f"  Số dòng  : {len(df)} (kỳ vọng: 100 = 20 tickers × 5 năm)")
    print(f"  Số cột   : {len(df.columns)}")
    print(f"  File     : {out_path}")

    # Kiểm tra số dòng
    if len(df) != 100:
        print(f"\n  ⚠ CẢNH BÁO: Kỳ vọng 100 dòng nhưng chỉ có {len(df)}!")
        print(f"     Kiểm tra xem đã import đủ 20 tickers × 5 năm chưa.")

    # Thống kê missing values
    missing = df[available].isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if not missing.empty:
        print(f"\n  Các cột có giá trị thiếu (top 10):")
        for col, cnt in missing.head(10).items():
            pct = cnt / len(df) * 100
            print(f"    {col:40s}: {cnt:3d} missing ({pct:.0f}%)")

    print(f"\n  Preview 3 dòng đầu:")
    print(df[['ticker','fiscal_year','net_sales','total_assets','net_income']].head(3).to_string(index=False))
    print(f"{'='*60}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Xuất panel dataset sạch')
    parser.add_argument('--out', default='outputs/panel_latest.csv')
    args = parser.parse_args()
    main(args.out)
