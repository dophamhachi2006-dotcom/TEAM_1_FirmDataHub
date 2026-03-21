"""
import_firms.py
--------------
Script A: Import danh mục 20 doanh nghiệp vào các bảng DIM.
Input : data/firms.xlsx
Output: dim_exchange, dim_industry_l2, dim_firm (INSERT hoặc UPDATE)

Cách chạy:
    python etl/import_firms.py
    python etl/import_firms.py --file data/firms.xlsx
"""

import argparse
import sys
import pandas as pd
import mysql.connector
from mysql.connector import Error
from db_config import get_connection   # <- file db_config.py cùng thư mục


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def upsert_exchange(cursor, code: str) -> int:
    """Trả về exchange_id; tạo mới nếu chưa tồn tại."""
    code = code.strip().upper()
    cursor.execute("SELECT exchange_id FROM dim_exchange WHERE exchange_code = %s", (code,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO dim_exchange (exchange_code) VALUES (%s)",
        (code,)
    )
    return cursor.lastrowid


def upsert_industry(cursor, name: str) -> int:
    """Trả về industry_l2_id; tạo mới nếu chưa tồn tại."""
    name = name.strip()
    cursor.execute("SELECT industry_l2_id FROM dim_industry_l2 WHERE industry_l2_name = %s", (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO dim_industry_l2 (industry_l2_name) VALUES (%s)",
        (name,)
    )
    return cursor.lastrowid


def upsert_firm(cursor, row: dict, exchange_id: int, industry_l2_id: int):
    """INSERT nếu ticker chưa có; UPDATE nếu đã có."""
    ticker = row['ticker'].strip().upper()

    cursor.execute("SELECT firm_id FROM dim_firm WHERE ticker = %s", (ticker,))
    existing = cursor.fetchone()

    if existing:
        cursor.execute("""
            UPDATE dim_firm
            SET company_name   = %s,
                exchange_id    = %s,
                industry_l2_id = %s,
                founded_year   = %s,
                listed_year    = %s,
                status         = %s,
                updated_at     = CURRENT_TIMESTAMP
            WHERE ticker = %s
        """, (
            row.get('company_name', ''),
            exchange_id,
            industry_l2_id,
            row.get('founded_year') or None,
            row.get('listed_year')  or None,
            row.get('status', 'active'),
            ticker
        ))
        print(f"  [UPDATE] {ticker} — {row.get('company_name','')}")
    else:
        cursor.execute("""
            INSERT INTO dim_firm
                (ticker, company_name, exchange_id, industry_l2_id, founded_year, listed_year, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            ticker,
            row.get('company_name', ''),
            exchange_id,
            industry_l2_id,
            row.get('founded_year') or None,
            row.get('listed_year')  or None,
            row.get('status', 'active')
        ))
        print(f"  [INSERT] {ticker} — {row.get('company_name','')}")


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #

def main(filepath: str = 'data/firms.xlsx'):
    print(f"\n{'='*55}")
    print(f"  import_firms.py  |  file: {filepath}")
    print(f"{'='*55}")

    # Đọc Excel
    try:
        df = pd.read_excel(filepath, dtype=str)
    except FileNotFoundError:
        print(f"[ERROR] Không tìm thấy file: {filepath}")
        sys.exit(1)

    # Chuẩn hóa tên cột (lowercase, bỏ khoảng trắng)
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

    # Kiểm tra cột bắt buộc
    required = {'ticker', 'company_name', 'exchange', 'industry_l2'}
    missing  = required - set(df.columns)
    if missing:
        print(f"[ERROR] Thiếu cột trong Excel: {missing}")
        print(f"        Các cột hiện có: {list(df.columns)}")
        sys.exit(1)

    # Kết nối DB
    try:
        conn   = get_connection()
        cursor = conn.cursor()
    except Error as e:
        print(f"[ERROR] Không kết nối được DB: {e}")
        sys.exit(1)

    inserted = updated = errors = 0

    for _, row in df.iterrows():
        ticker = str(row.get('ticker', '')).strip().upper()
        if not ticker or ticker == 'NAN':
            continue
        try:
            exchange_id    = upsert_exchange(cursor, str(row['exchange']))
            industry_l2_id = upsert_industry(cursor, str(row['industry_l2']))
            upsert_firm(cursor, dict(row), exchange_id, industry_l2_id)
            conn.commit()
        except Error as e:
            conn.rollback()
            print(f"  [ERROR] {ticker}: {e}")
            errors += 1
    # Ghi Audit Log tự động
    print("\n  [Audit] Đang quét đối chiếu dữ liệu để ghi Log ...")

    audit_fields = {
        'fact_financial_year': ['total_assets', 'net_sales', 'net_income', 'inventory', 'total_equity'],
        'fact_market_year': ['shares_outstanding', 'share_price'],
        'fact_ownership_year': ['state_own', 'foreign_own']
    }

    try:
        audit_count = 0
        for table, fields in audit_fields.items():
            for field in fields:
                bulk_sql = f"""
                INSERT INTO fact_value_override_log 
                    (firm_id, fiscal_year, table_name, column_name, old_value, new_value, reason, changed_by)
                SELECT
                    n.firm_id, n.fiscal_year, '{table}', '{field}', 
                    CAST(o.{field} AS CHAR), CAST(n.{field} AS CHAR), 
                    CONCAT('Auto-detected change from snapshot ', n.snapshot_id), 'System_Audit'
                FROM {table} n
                JOIN (
                    SELECT f1.firm_id, f1.fiscal_year, f1.{field}
                    FROM {table} f1
                    WHERE f1.snapshot_id = (
                        SELECT MAX(snapshot_id) FROM {table} f2
                        WHERE f2.firm_id = f1.firm_id AND f2.fiscal_year = f1.fiscal_year AND f2.snapshot_id < %s
                    )
                ) o ON n.firm_id = o.firm_id AND n.fiscal_year = o.fiscal_year
                WHERE n.snapshot_id = %s
                  AND n.{field} != o.{field}
                  AND n.{field} IS NOT NULL AND o.{field} IS NOT NULL;
                """
                cursor.execute(bulk_sql, (snapshot_id, snapshot_id))
                audit_count += cursor.rowcount 
        
        conn.commit()
        if audit_count > 0:
            print(f"  ✓ Thành công! Đã ghi {audit_count} dòng log vào cột 'column_name'.")
        else:
            print("  ✓ Check hoàn tất: Không có dữ liệu nào bị thay đổi.")
            
    except Error as e:
        conn.rollback()
        print(f"  [WARN] Lỗi logic Audit: {e}")
    # ----
    cursor.close()
    conn.close()

    print(f"\n✓ Hoàn thành import firms | Lỗi: {errors}")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import danh mục doanh nghiệp')
    parser.add_argument('--file', default='data/firms.xlsx', help='Đường dẫn file Excel')
    args = parser.parse_args()
    main(args.file)
