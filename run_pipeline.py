"""
run_pipeline.py
---------------
Chạy toàn bộ pipeline Firm Data Hub từ đầu đến cuối chỉ với 1 lệnh.

Cách chạy:
    # Chạy đầy đủ (mặc định)
    python run_pipeline.py

    # Chỉ định file data khác
    python run_pipeline.py --firms data/firms.xlsx --panel data/panel_2020_2024.xlsx

    # Bỏ qua bước import (chỉ chạy QC + export)
    python run_pipeline.py --skip-import

    # Reset toàn bộ DB rồi chạy lại từ đầu
    python run_pipeline.py --reset
"""

import argparse
import subprocess
import sys
import os
import getpass
from datetime import date

# ── Màu terminal ─────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def log(msg, color=RESET):
    print(f"{color}{msg}{RESET}")

def run(cmd: list, step_name: str) -> bool:
    """Chạy lệnh, trả về True nếu thành công."""
    log(f"\n{'─'*60}", BLUE)
    log(f"  ▶  {step_name}", BOLD)
    log(f"  $ {' '.join(cmd)}", YELLOW)
    log(f"{'─'*60}", BLUE)

    result = subprocess.run(cmd, cwd=os.path.dirname(os.path.abspath(__file__)))

    if result.returncode == 0:
        log(f"  ✓ {step_name} — THÀNH CÔNG", GREEN)
        return True
    else:
        log(f"  ✗ {step_name} — THẤT BẠI (exit code {result.returncode})", RED)
        return False


def reset_db():
    """Xóa và tạo lại DB từ schema_and_seed.sql."""
    log("\n⚠  Chế độ RESET: Xóa và tạo lại toàn bộ DB...", YELLOW)
    try:
        import mysql.connector
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'etl'))
        from db_config import DB_CONFIG

        cfg = {k: v for k, v in DB_CONFIG.items() if k != 'database'}
        conn = mysql.connector.connect(**cfg)
        cursor = conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_CONFIG['database']}")
        conn.commit()
        cursor.close()
        conn.close()
        log("  ✓ Đã xóa database cũ", GREEN)
    except Exception as e:
        log(f"  ✗ Không xóa được DB: {e}", RED)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Chạy toàn bộ Firm Data Hub pipeline')
    parser.add_argument('--firms',       default='data/firms.xlsx')
    parser.add_argument('--panel',       default='data/panel_2020_2024.xlsx')
    parser.add_argument('--version',     default='v1',
                        help='Version tag cho snapshot')
    parser.add_argument('--skip-import', action='store_true',
                        help='Bỏ qua bước import firms + panel')
    parser.add_argument('--reset',       action='store_true',
                        help='Reset toàn bộ DB trước khi chạy')
    parser.add_argument('--out-qc',      default='outputs/qc_report.csv')
    parser.add_argument('--out-panel',   default='outputs/panel_latest.csv')
    args = parser.parse_args()

    py = sys.executable

    if not os.environ.get('DB_PASSWORD'):
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        if not os.path.exists(env_path):
            log("\n" + "="*50, BOLD)
            log("  Firm Data Hub — Kết nối MySQL", BOLD)
            log("="*50, BOLD)
            pw = getpass.getpass("  Nhập password MySQL (user: root): ")
            os.environ['DB_PASSWORD'] = pw
            log("  ✓ Password đã lưu cho session này\n", GREEN)

    log(f"\n{'='*60}", BOLD)
    log(f"  FIRM DATA HUB — PIPELINE", BOLD)
    log(f"  {date.today()}", BOLD)
    log(f"{'='*60}", BOLD)

    results = {}

    if args.reset:
        reset_db()

    if not args.skip_import:
        # ── Bước A: Import firms ──────────────────────────────
        ok = run(
            [py, 'etl/import_firms.py', '--file', args.firms],
            'Bước A — Import danh mục doanh nghiệp'
        )
        results['A_import_firms'] = ok
        if not ok:
            log("\n✗ Dừng pipeline do lỗi ở Bước A.", RED)
            sys.exit(1)

        # ── Bước B: Tạo snapshot — 3 nguồn × 5 năm = 15 snapshots ──
        # import_panel.py sẽ tự tra đúng snapshot_id cho từng bảng × năm
        snap_date = str(date.today())
        sources   = ['BCTC_Audited', 'Vietstock', 'AnnualReport']
        years     = [2020, 2021, 2022, 2023, 2024]
        all_ok    = True

        for src in sources:
            for yr in years:
                ok = run(
                    [py, 'etl/create_snapshot.py',
                     '--source',  src,
                     '--year',    str(yr),
                     '--date',    snap_date,
                     '--version', args.version,
                     '--panel',   args.panel],
                    f'Bước B — Snapshot {src} năm {yr}'
                )
                if not ok:
                    all_ok = False

        results['B_create_snapshot'] = all_ok
        if not all_ok:
            log("\n✗ Dừng pipeline do lỗi ở Bước B.", RED)
            sys.exit(1)

        # ── Bước C: Import panel ──────────────────────────────
        # Không truyền --snapshot nữa — import_panel tự tra từ DB
        ok = run(
            [py, 'etl/import_panel.py', '--file', args.panel],
            'Bước C — Import panel data (snapshot IDs tra tự động)'
        )
        results['C_import_panel'] = ok
        if not ok:
            log("\n⚠  Bước C có lỗi nhưng vẫn tiếp tục...", YELLOW)

    # ── Bước D: QC checks ────────────────────────────────────
    ok = run(
        [py, 'etl/qc_checks.py', '--out', args.out_qc],
        'Bước D — Kiểm tra chất lượng dữ liệu'
    )
    results['D_qc_checks'] = ok

    # ── Bước E: Export panel ──────────────────────────────────
    ok = run(
        [py, 'etl/export_panel.py', '--out', args.out_panel],
        'Bước E — Xuất dataset panel sạch'
    )
    results['E_export_panel'] = ok

    # ── Tóm tắt kết quả ──────────────────────────────────────
    log(f"\n{'='*60}", BOLD)
    log(f"  KẾT QUẢ PIPELINE", BOLD)
    log(f"{'='*60}", BOLD)
    all_ok = True
    for step, ok in results.items():
        icon  = "✓" if ok else "✗"
        color = GREEN if ok else RED
        log(f"  {icon}  {step}", color)
        if not ok:
            all_ok = False

    log(f"\n  Output:", BOLD)
    log(f"  → {args.out_qc}")
    log(f"  → {args.out_panel}")

    if all_ok:
        log(f"\n  🎉 Pipeline hoàn thành thành công!", GREEN + BOLD)
    else:
        log(f"\n  ⚠  Pipeline hoàn thành nhưng có lỗi — kiểm tra log ở trên.", YELLOW)

    log(f"{'='*60}\n", BOLD)


if __name__ == '__main__':
    main()
