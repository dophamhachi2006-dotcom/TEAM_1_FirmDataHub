"""
qc_checks.py
------------
Script D: Kiểm tra chất lượng dữ liệu — đủ 6 rules theo đề bài.
Output: outputs/qc_report.csv

Cách chạy:
    python etl/qc_checks.py
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import DB_CONFIG, _get_password

# Cấu hình ngưỡng (có thể chỉnh)
GROWTH_MIN  = -0.95
GROWTH_MAX  = 5.0
MVE_TOL_PCT = 0.05  # sai số 5% cho kiểm tra market_value_equity
BALANCE_TOL = 100_000_000   # sai số tuyệt đối (đồng) — bỏ qua chênh lệch làm tròn kế toán dưới 100 triệu đồng


def load_data() -> pd.DataFrame:
    """Load toàn bộ data từ view dùng SQLAlchemy (tránh UserWarning)."""
    cfg = DB_CONFIG
    pw  = quote_plus(_get_password())   # encode ký tự đặc biệt trong password
    url = (
        f"mysql+mysqlconnector://{cfg['user']}:{pw}"
        f"@{cfg['host']}:{cfg.get('port', 3306)}/{cfg['database']}"
    )
    engine = create_engine(url)
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM vw_firm_panel_latest"), conn)
    return df


def run_qc(df: pd.DataFrame) -> pd.DataFrame:
    """Chạy tất cả QC rules, trả về DataFrame các lỗi."""
    issues = []

    def add(ticker, year, field, etype, severity, msg):
        issues.append({
            'ticker':      ticker,
            'fiscal_year': year,
            'field_name':  field,
            'error_type':  etype,
            'severity':    severity,
            'message':     msg,
        })

    for _, row in df.iterrows():
        t = row['ticker']
        y = row['fiscal_year']

        # ── Rule 1: Ownership ratios nằm trong [0, 1] ────────────
        for col in ['managerial_inside_own', 'state_own',
                    'institutional_own', 'foreign_own']:
            val = row.get(col)
            if pd.notna(val):
                if val < 0 or val > 1:
                    add(t, y, col, 'OUT_OF_RANGE', 'ERROR',
                        f'{col} = {val:.4f} nằm ngoài [0, 1]')

        # ── Rule 2: Tổng ownership ≤ 1 ───────────────────────────
        own_vals = [row.get(c) for c in
                    ['managerial_inside_own', 'state_own',
                     'institutional_own', 'foreign_own']]
        own_vals = [v for v in own_vals if pd.notna(v)]
        if len(own_vals) == 4:
            total = sum(own_vals)
            if total > 1.001:
                add(t, y, 'ownership_sum', 'INVALID_SUM', 'ERROR',
                    f'Tổng ownership = {total:.4f} > 1.0')

        # ── Rule 3: Shares outstanding > 0 ───────────────────────
        shares = row.get('shares_outstanding')
        if pd.notna(shares):
            if shares <= 0:
                add(t, y, 'shares_outstanding', 'INVALID_VALUE', 'ERROR',
                    f'shares_outstanding = {shares} phải > 0')

        # ── Rule 4: Total assets ≥ 0 ─────────────────────────────
        assets = row.get('total_assets')
        if pd.notna(assets):
            if assets < 0:
                add(t, y, 'total_assets', 'INVALID_VALUE', 'ERROR',
                    f'total_assets = {assets:.2f} phải ≥ 0')

        # ── Rule 5: Current liabilities ≥ 0 ──────────────────────
        cl = row.get('current_liabilities')
        if pd.notna(cl):
            if cl < 0:
                add(t, y, 'current_liabilities', 'INVALID_VALUE', 'ERROR',
                    f'current_liabilities = {cl:.2f} phải ≥ 0')

        # ── Rule 6: Growth ratio trong khoảng hợp lý ─────────────
        gr = row.get('growth_ratio')
        if pd.notna(gr):
            if gr < GROWTH_MIN or gr > GROWTH_MAX:
                add(t, y, 'growth_ratio', 'OUT_OF_RANGE', 'WARNING',
                    f'Tăng trưởng {gr:.4f} ngoài [{GROWTH_MIN}, {GROWTH_MAX}]')

        # ── Rule 7: market_value_equity ≈ shares × price ─────────
        mve   = row.get('market_value_equity')
        price = row.get('share_price')
        if pd.notna(mve) and pd.notna(shares) and pd.notna(price) and shares > 0 and price > 0:
            expected = shares * price
            if expected > 0:
                diff_pct = abs(mve - expected) / expected
                if diff_pct > MVE_TOL_PCT:
                    add(t, y, 'market_value_equity', 'INCONSISTENT', 'WARNING',
                        f'MVE={mve:.0f} ≠ shares×price={expected:.0f} (chênh {diff_pct*100:.1f}%)')

        # ── Rule 8: CAPEX không âm ────────────────────────────────
        capex = row.get('capex')
        if pd.notna(capex) and capex < 0:
            add(t, y, 'capex', 'INVALID_VALUE', 'WARNING',
                f'capex = {capex:.2f} không nên âm (theo quy ước nhóm)')

        # ── Rule 9: Net sales ≥ 0 ─────────────────────────────────
        ns = row.get('net_sales')
        if pd.notna(ns) and ns < 0:
            add(t, y, 'net_sales', 'INVALID_VALUE', 'WARNING',
                f'net_sales = {ns:.2f} phải ≥ 0')

        # ── Rule 10: Firm age hợp lý ──────────────────────────────
        age = row.get('firm_age')
        if pd.notna(age):
            if age < 1 or age > 100:
                add(t, y, 'firm_age', 'OUT_OF_RANGE', 'WARNING',
                    f'firm_age = {age} ngoài khoảng [1, 100]')

        # ── Rule 11: Tiền và Tồn kho <= Tài sản ngắn hạn ─────────
        ca   = row.get('current_assets')
        cash = row.get('cash_and_equivalents')
        inv  = row.get('inventory')
        if pd.notna(ca):
            if pd.notna(cash) and cash > ca + BALANCE_TOL:
                add(t, y, 'cash_and_equivalents', 'LOGIC_ERROR', 'ERROR',
                    f'Tiền mặt ({cash:.1f}) > TS ngắn hạn ({ca:.1f})')
            if pd.notna(inv) and inv > ca + BALANCE_TOL:
                add(t, y, 'inventory', 'LOGIC_ERROR', 'ERROR',
                    f'Tồn kho ({inv:.1f}) > TS ngắn hạn ({ca:.1f})')

        # ── Rule 12: Chi phí R&D không tưởng ─────────────────────
        rnd = row.get('rnd_expenses')
        if pd.notna(rnd) and pd.notna(ns) and ns > 0:
            if rnd > ns:
                add(t, y, 'rnd_expenses', 'BUSINESS_WARNING', 'WARNING',
                    f'Chi phí R&D ({rnd:.1f}) lớn hơn cả Doanh thu thuần ({ns:.1f})')

        # ── Rule 13: Số lượng nhân viên phải hợp lý ──────────────
        emp = row.get('employees_count')
        if pd.notna(emp):
            if emp <= 0:
                add(t, y, 'employees_count', 'INVALID_VALUE', 'ERROR',
                    f'Số nhân viên ({emp}) phải lớn hơn 0')
            elif emp != int(emp):
                add(t, y, 'employees_count', 'INVALID_FORMAT', 'ERROR',
                    f'Số nhân viên ({emp}) phải là số nguyên')

        # ── Rule 14: Sở hữu của Ban điều hành quá cao ────────────
        inside_own = row.get('managerial_inside_own')
        if pd.notna(inside_own) and inside_own > 0.8:
            add(t, y, 'managerial_inside_own', 'BUSINESS_WARNING', 'WARNING',
                f'Sở hữu Ban điều hành ({inside_own*100:.1f}%) quá cao bất thường')

        # ── Rule 15: Nợ ngắn hạn + Nợ dài hạn <= Tổng nợ ────────
        tl   = row.get('total_liabilities')
        ltd  = row.get('long_term_debt')
        cl15 = row.get('current_liabilities')   # đọc lại tránh dùng biến cũ từ Rule 5
        if pd.notna(tl) and pd.notna(cl15) and pd.notna(ltd):
            if (cl15 + ltd) > tl + BALANCE_TOL:
                add(t, y, 'liabilities_breakdown', 'LOGIC_ERROR', 'ERROR',
                    f'Nợ ngắn+dài ({cl15+ltd:.1f}) > Tổng nợ ({tl:.1f})')

    return pd.DataFrame(issues, columns=[
        'ticker', 'fiscal_year', 'field_name', 'error_type', 'severity', 'message'
    ])


def main(out_path: str = 'outputs/qc_report.csv'):
    print(f"\n{'='*60}")
    print(f"  qc_checks.py  |  output: {out_path}")
    print(f"{'='*60}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    try:
        df = load_data()
    except Exception as e:
        print(f"[ERROR] Không kết nối được DB: {e}")
        sys.exit(1)

    print(f"  Đã load {len(df)} dòng từ vw_firm_panel_latest")

    report = run_qc(df)
    report.to_csv(out_path, index=False, encoding='utf-8-sig')

    if report.empty:
        print(f"  ✓ Không tìm thấy lỗi nào! Dữ liệu đạt chất lượng.")
    else:
        errors   = len(report[report['severity'] == 'ERROR'])
        warnings = len(report[report['severity'] == 'WARNING'])
        print(f"  ⚠  Tìm thấy {len(report)} vấn đề "
              f"({errors} ERROR, {warnings} WARNING)")
        print(f"  → Xem chi tiết: {out_path}")
        print(f"\n  Tóm tắt theo loại lỗi:")
        for etype, cnt in report.groupby('error_type').size().items():
            print(f"    {etype:<30}: {cnt} lỗi")
        print(f"\n  Tóm tắt theo ticker:")
        for tkr, cnt in report.groupby('ticker').size().sort_values(ascending=False).items():
            sev = report[report['ticker'] == tkr]['severity'].value_counts().to_dict()
            print(f"    {tkr:<8}: {cnt} vấn đề {sev}")

    print(f"{'='*60}\n")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default='outputs/qc_report.csv')
    args = parser.parse_args()
    main(args.out)
