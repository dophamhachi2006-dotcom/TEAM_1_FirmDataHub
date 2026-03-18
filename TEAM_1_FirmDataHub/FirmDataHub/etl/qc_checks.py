"""
qc_checks.py
------------
Script D: Kiểm tra chất lượng dữ liệu (QC).
Đọc từ DB → chạy các rule → xuất outputs/qc_report.csv

Cách chạy:
    python etl/qc_checks.py
    python etl/qc_checks.py --out outputs/qc_report.csv
"""

import argparse
import os
import pandas as pd
import mysql.connector
from db_config import get_connection

# Cấu hình rule growth_ratio
GROWTH_MIN = -0.95
GROWTH_MAX =  5.0

# Cho phép sai số market_cap vs shares*price (%)
MARKET_CAP_TOLERANCE = 0.05  # 5%


def load_data(conn) -> pd.DataFrame:
    """Load toàn bộ panel từ view latest."""
    sql = "SELECT * FROM vw_firm_panel_latest"
    return pd.read_sql(sql, conn)


def run_qc(df: pd.DataFrame) -> list:
    """Chạy tất cả QC rules. Trả về list dict lỗi."""
    errors = []

    def add_error(ticker, year, field, error_type, message):
        errors.append({
            'ticker':      ticker,
            'fiscal_year': year,
            'field_name':  field,
            'error_type':  error_type,
            'message':     message
        })

    for _, row in df.iterrows():
        ticker = row['ticker']
        year   = row['fiscal_year']

        # ── Rule 1: Ownership trong [0, 1] ──────────────────────────
        for col in ['managerial_inside_own', 'state_own', 'institutional_own', 'foreign_own']:
            val = row.get(col)
            if pd.notna(val):
                if val < 0 or val > 1:
                    add_error(ticker, year, col, 'OUT_OF_RANGE',
                              f"Giá trị {val:.4f} không nằm trong [0, 1]")

        # ── Rule 2: Tổng ownership ≤ 1.01 (cho phép làm tròn) ───────
        own_cols = ['managerial_inside_own', 'state_own', 'institutional_own', 'foreign_own']
        own_vals = [row.get(c) for c in own_cols if pd.notna(row.get(c))]
        if len(own_vals) == 4:
            total = sum(own_vals)
            if total > 1.01:
                add_error(ticker, year, 'ownership_sum', 'INVALID_SUM',
                          f"Tổng ownership = {total:.4f} > 1.0")

        # ── Rule 3: shares_outstanding > 0 ──────────────────────────
        val = row.get('shares_outstanding')
        if pd.notna(val) and val <= 0:
            add_error(ticker, year, 'shares_outstanding', 'NON_POSITIVE',
                      f"Số CP phải > 0, hiện tại = {val}")

        # ── Rule 4: total_assets >= 0 ────────────────────────────────
        val = row.get('total_assets')
        if pd.notna(val) and val < 0:
            add_error(ticker, year, 'total_assets', 'NEGATIVE',
                      f"Tổng tài sản âm: {val}")

        # ── Rule 5: current_liabilities >= 0 ────────────────────────
        val = row.get('current_liabilities')
        if pd.notna(val) and val < 0:
            add_error(ticker, year, 'current_liabilities', 'NEGATIVE',
                      f"Nợ ngắn hạn âm: {val}")

        # ── Rule 6: total_liabilities >= 0 ──────────────────────────
        val = row.get('total_liabilities')
        if pd.notna(val) and val < 0:
            add_error(ticker, year, 'total_liabilities', 'NEGATIVE',
                      f"Tổng nợ phải trả âm: {val}")

        # ── Rule 7: growth_ratio trong [GROWTH_MIN, GROWTH_MAX] ─────
        val = row.get('growth_ratio')
        if pd.notna(val):
            if val < GROWTH_MIN or val > GROWTH_MAX:
                add_error(ticker, year, 'growth_ratio', 'OUT_OF_RANGE',
                          f"Tăng trưởng {val:.4f} ngoài [{GROWTH_MIN}, {GROWTH_MAX}]")

        # ── Rule 8: market_cap ≈ shares × price ─────────────────────
        mve    = row.get('market_value_equity')
        shares = row.get('shares_outstanding')
        price  = row.get('share_price')
        if pd.notna(mve) and pd.notna(shares) and pd.notna(price) and shares > 0 and price > 0:
            computed = shares * price
            if computed > 0:
                diff_pct = abs(mve - computed) / computed
                if diff_pct > MARKET_CAP_TOLERANCE:
                    add_error(ticker, year, 'market_value_equity', 'INCONSISTENT',
                              f"market_cap={mve:,.0f} ≠ shares×price={computed:,.0f} "
                              f"(lệch {diff_pct*100:.1f}%)")

        # ── Rule 9: net_sales >= 0 ───────────────────────────────────
        val = row.get('net_sales')
        if pd.notna(val) and val < 0:
            add_error(ticker, year, 'net_sales', 'NEGATIVE',
                      f"Doanh thu thuần âm: {val}")

        # ── Rule 10: total_assets = total_equity + total_liabilities ─
        ta  = row.get('total_assets')
        eq  = row.get('total_equity')
        lib = row.get('total_liabilities')
        if pd.notna(ta) and pd.notna(eq) and pd.notna(lib):
            diff = abs(ta - (eq + lib))
            # Cho phép sai số 1% (do làm tròn đơn vị tỷ đồng)
            tolerance = ta * 0.01 if ta > 0 else 1
            if diff > tolerance:
                add_error(ticker, year, 'balance_sheet_check', 'INCONSISTENT',
                          f"Assets({ta}) ≠ Equity({eq}) + Liabilities({lib}), lệch={diff:.2f}")

        # ── Rule 11: product/process innovation chỉ = 0 hoặc 1 ──────
        for col in ['product_innovation', 'process_innovation']:
            val = row.get(col)
            if pd.notna(val) and val not in [0, 1, 0.0, 1.0]:
                add_error(ticker, year, col, 'INVALID_DUMMY',
                          f"Giá trị {val} không hợp lệ (chỉ chấp nhận 0 hoặc 1)")

        # ── Rule 12: employees_count > 0 ────────────────────────────
        val = row.get('employees_count')
        if pd.notna(val) and val <= 0:
            add_error(ticker, year, 'employees_count', 'NON_POSITIVE',
                      f"Số nhân viên phải > 0, hiện tại = {val}")

    return errors


def main(out_path: str = 'outputs/qc_report.csv'):
    print(f"\n{'='*60}")
    print(f"  qc_checks.py  |  output: {out_path}")
    print(f"{'='*60}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    conn = get_connection()
    df   = load_data(conn)
    conn.close()

    print(f"  Đã load {len(df)} dòng từ vw_firm_panel_latest")

    errors = run_qc(df)

    if errors:
        report = pd.DataFrame(errors)
        report.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"\n  ⚠  Tìm thấy {len(errors)} vấn đề chất lượng dữ liệu")
        print(f"  → Xem chi tiết: {out_path}")

        # Tóm tắt theo error_type
        print(f"\n  Tóm tắt theo loại lỗi:")
        summary = report.groupby('error_type').size().reset_index(name='count')
        for _, s in summary.iterrows():
            print(f"    {s['error_type']:25s} : {s['count']} lỗi")
    else:
        # Xuất file rỗng (có header) để chứng minh QC đã chạy
        pd.DataFrame(columns=['ticker','fiscal_year','field_name','error_type','message'])\
          .to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"\n  ✓ Không tìm thấy lỗi nào! Dữ liệu đạt chất lượng.")

    print(f"{'='*60}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='QC kiểm tra chất lượng dữ liệu')
    parser.add_argument('--out', default='outputs/qc_report.csv')
    args = parser.parse_args()
    main(args.out)
