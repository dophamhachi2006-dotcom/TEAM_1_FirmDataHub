"""
create_snapshot.py
------------------
Script B: Tạo bản ghi phiên bản (snapshot) trước mỗi lần import dữ liệu.
Trả về snapshot_id để dùng trong import_panel.py.

Cách chạy:
    python etl/create_snapshot.py --source BCTC_Audited --year 2024 --date 2025-03-31 --version v1
    python etl/create_snapshot.py --source Vietstock    --year 2024 --date 2025-03-31 --version v1
"""

import argparse
import sys
from datetime import date
import mysql.connector
from db_config import get_connection


def get_source_id(cursor, source_name: str) -> int:
    """Lấy source_id từ source_name. Báo lỗi nếu không tìm thấy."""
    cursor.execute(
        "SELECT source_id FROM dim_data_source WHERE source_name = %s",
        (source_name,)
    )
    row = cursor.fetchone()
    if not row:
        print(f"[ERROR] Không tìm thấy source_name='{source_name}' trong dim_data_source.")
        print("        Các source hợp lệ: BCTC_Audited | AnnualReport | Vietstock | CafeF | Manual")
        sys.exit(1)
    return row[0]


def create_snapshot(source_name: str, fiscal_year: int,
                    snapshot_date: str, version_tag: str,
                    created_by: str = 'etl') -> int:
    """
    Tạo snapshot mới và trả về snapshot_id.
    Nếu đã có snapshot cùng source + year + version → báo cảnh báo nhưng vẫn tạo mới.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    source_id = get_source_id(cursor, source_name)

    # Kiểm tra trùng
    cursor.execute("""
        SELECT snapshot_id, snapshot_date
        FROM   fact_data_snapshot
        WHERE  source_id = %s AND fiscal_year = %s AND version_tag = %s
    """, (source_id, fiscal_year, version_tag))
    existing = cursor.fetchone()
    if existing:
        print(f"[WARN] Đã tồn tại snapshot_id={existing[0]} "
              f"(source={source_name}, year={fiscal_year}, version={version_tag}).")
        print("       Sẽ tạo snapshot MỚI (version mới hơn).")

    cursor.execute("""
        INSERT INTO fact_data_snapshot
            (snapshot_date, fiscal_year, source_id, version_tag, created_by)
        VALUES (%s, %s, %s, %s, %s)
    """, (snapshot_date, fiscal_year, source_id, version_tag, created_by))

    conn.commit()
    snapshot_id = cursor.lastrowid

    cursor.close()
    conn.close()

    return snapshot_id


def main():
    parser = argparse.ArgumentParser(description='Tạo snapshot phiên bản dữ liệu')
    parser.add_argument('--source',  required=True,
                        help='Tên nguồn: BCTC_Audited | AnnualReport | Vietstock | CafeF | Manual')
    parser.add_argument('--year',    required=True, type=int, help='Năm tài chính (VD: 2024)')
    parser.add_argument('--date',    default=str(date.today()), help='Ngày snapshot YYYY-MM-DD')
    parser.add_argument('--version', default='v1', help='Version tag (VD: v1, v1.1)')
    parser.add_argument('--by',      default='etl', help='Người/bot tạo snapshot')
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  create_snapshot.py")
    print(f"  source={args.source} | year={args.year} | date={args.date} | version={args.version}")
    print(f"{'='*55}")

    snap_id = create_snapshot(
        source_name   = args.source,
        fiscal_year   = args.year,
        snapshot_date = args.date,
        version_tag   = args.version,
        created_by    = args.by
    )

    print(f"\n✓ Snapshot tạo thành công!")
    print(f"  snapshot_id = {snap_id}")
    print(f"  → Dùng ID này khi chạy: python etl/import_panel.py --snapshot {snap_id}")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    main()
