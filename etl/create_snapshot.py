"""
create_snapshot.py
------------------
Script B: Tạo snapshot phiên bản dữ liệu.

Logic:
  - Mỗi source × year = 1 snapshot riêng (đúng chuẩn DB)
  - Nếu file Excel không đổi → dùng snapshot cũ (same hash)
  - Nếu file Excel thay đổi → tạo snapshot mới

Cách chạy:
    python etl/create_snapshot.py --source BCTC_Audited --year 2024 --version v1
"""

import argparse
import sys
import os
import hashlib
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_connection


def get_source_id(cursor, source_name: str) -> int:
    cursor.execute(
        "SELECT source_id FROM dim_data_source WHERE source_name = %s",
        (source_name,)
    )
    row = cursor.fetchone()
    if not row:
        print(f"[ERROR] Không tìm thấy source_name='{source_name}'")
        print("        Các source hợp lệ: BCTC_Audited | AnnualReport | Vietstock | Manual")
        sys.exit(1)
    return row[0]


def get_file_hash(panel_file: str) -> str:
    """Tính hash từ file Excel — nguồn gốc thật của data."""
    if not os.path.exists(panel_file):
        return ''
    with open(panel_file, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


def create_snapshot(source_name: str, fiscal_year: int,
                    snapshot_date: str, version_tag: str,
                    created_by: str = 'etl',
                    panel_file: str = 'data/panel_2020_2024.xlsx') -> tuple:
    """
    Trả về (snapshot_id, is_new):
      - is_new=False: file Excel không đổi → dùng snapshot cũ
      - is_new=True:  file Excel thay đổi → tạo snapshot mới
    """
    conn   = get_connection()
    cursor = conn.cursor()

    source_id    = get_source_id(cursor, source_name)
    current_hash = get_file_hash(panel_file)

    # Tìm snapshot cũ cùng source + year
    cursor.execute("""
        SELECT snapshot_id, version_tag
        FROM fact_data_snapshot
        WHERE source_id = %s AND fiscal_year = %s
        ORDER BY snapshot_id DESC LIMIT 1
    """, (source_id, fiscal_year))
    existing = cursor.fetchone()

    if existing:
        snap_id, old_tag = existing
        # Lấy hash từ version_tag (lưu sau dấu #)
        old_hash = old_tag.split('#')[1] if '#' in old_tag else ''

        if old_hash == current_hash and current_hash != '':
            # File không đổi → dùng snapshot cũ
            cursor.close()
            conn.close()
            return snap_id, False

        # File thay đổi → tính version mới
        base_ver = version_tag.split('.')[0]
        cursor.execute("""
            SELECT COUNT(*) FROM fact_data_snapshot
            WHERE source_id = %s AND fiscal_year = %s
        """, (source_id, fiscal_year))
        count = cursor.fetchone()[0]
        new_version = f"{base_ver}.{count}" if count > 0 else base_ver
    else:
        new_version = version_tag

    # Lưu hash vào version_tag
    tag_with_hash = f"{new_version}#{current_hash}" if current_hash else new_version

    cursor.execute("""
        INSERT INTO fact_data_snapshot
            (snapshot_date, fiscal_year, source_id, version_tag, created_by)
        VALUES (%s, %s, %s, %s, %s)
    """, (snapshot_date, fiscal_year, source_id, tag_with_hash, created_by))

    conn.commit()
    snapshot_id = cursor.lastrowid
    cursor.close()
    conn.close()

    return snapshot_id, True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',  required=True)
    parser.add_argument('--year',    required=True, type=int)
    parser.add_argument('--date',    default=str(date.today()))
    parser.add_argument('--version', default='v1')
    parser.add_argument('--by',      default='etl')
    parser.add_argument('--panel',   default='data/panel_2020_2024.xlsx')
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  create_snapshot.py")
    print(f"  source={args.source} | year={args.year} | version={args.version}")
    print(f"{'='*55}")

    snap_id, is_new = create_snapshot(
        source_name   = args.source,
        fiscal_year   = args.year,
        snapshot_date = args.date,
        version_tag   = args.version,
        created_by    = args.by,
        panel_file    = args.panel
    )

    if is_new:
        print(f"\n  ✓ File thay đổi → Tạo snapshot MỚI!")
    else:
        print(f"\n  → File không đổi → Dùng snapshot CŨ")

    print(f"  snapshot_id = {snap_id}")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    main()
