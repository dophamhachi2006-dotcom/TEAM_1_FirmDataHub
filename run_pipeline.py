"""
run_pipeline.py
---------------
Chay toan bo pipeline Firm Data Hub tu dau den cuoi chi voi 1 lenh.
Tu dong khoi tao DB (schema + views) - khong can mo MySQL Workbench.

Cach chay:
    python run_pipeline.py
    python run_pipeline.py --reset
    python run_pipeline.py --skip-import
"""

import argparse
import subprocess
import sys
import os
import re
import getpass
from datetime import date

# Terminal colors
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def log(msg, color=RESET):
    print(f"{color}{msg}{RESET}")

def run(cmd: list, step_name: str) -> bool:
    log(f"\n{'─'*60}", BLUE)
    log(f"  ▶  {step_name}", BOLD)
    log(f"  $ {' '.join(cmd)}", YELLOW)
    log(f"{'─'*60}", BLUE)
    result = subprocess.run(cmd, cwd=os.path.dirname(os.path.abspath(__file__)))
    if result.returncode == 0:
        log(f"  ✓ {step_name} — THANH CONG", GREEN)
        return True
    else:
        log(f"  ✗ {step_name} — THAT BAI (exit code {result.returncode})", RED)
        return False


def run_sql_file(filepath, password, db_config, use_db=True):
    """Doc file SQL va execute tung statement mot."""
    import mysql.connector

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        sql = f.read()

    # Xoa comment don -- (tung dong)
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped.startswith('--'):
            # Cat phan comment o cuoi dong
            idx = line.find('--')
            if idx >= 0:
                line = line[:idx]
            lines.append(line)
    sql = '\n'.join(lines)

    # Xoa comment block /* ... */
    result_parts = []
    i = 0
    while i < len(sql):
        start = sql.find('/*', i)
        if start == -1:
            result_parts.append(sql[i:])
            break
        result_parts.append(sql[i:start])
        end = sql.find('*/', start + 2)
        if end == -1:
            break
        i = end + 2
    sql = ''.join(result_parts)

    # Split theo ; va loc statement rong
    statements = [s.strip() for s in sql.split(';') if s.strip()]

    cfg = {
        'host':            db_config['host'],
        'port':            db_config.get('port', 3306),
        'user':            db_config['user'],
        'password':        password,
        'charset':         'utf8mb4',
        'consume_results': True,
    }
    if use_db:
        cfg['database'] = db_config['database']

    conn   = mysql.connector.connect(**cfg)
    cursor = conn.cursor()

    for stmt in statements:
        try:
            cursor.execute(stmt)
            try:
                cursor.fetchall()
            except Exception:
                pass
            conn.commit()
        except mysql.connector.Error as e:
            # Bo qua loi "already exists" khi chay lai
            if e.errno not in (1050, 1060, 1061, 1062):
                raise

    cursor.close()
    conn.close()


def init_db(password, db_config, base_dir):
    """Buoc 0: Chay schema_and_seed.sql roi views.sql."""
    schema_file = os.path.join(base_dir, 'sql', 'schema_and_seed.sql')
    views_file  = os.path.join(base_dir, 'sql', 'views.sql')

    # 0a - schema_and_seed.sql
    log(f"\n{'─'*60}", BLUE)
    log(f"  ▶  Buoc 0a — Khoi tao DB (schema_and_seed.sql)", BOLD)
    log(f"{'─'*60}", BLUE)
    try:
        run_sql_file(schema_file, password, db_config, use_db=False)
        log(f"  ✓ Buoc 0a — THANH CONG", GREEN)
        ok0a = True
    except Exception as e:
        log(f"  ✗ Buoc 0a — THAT BAI: {e}", RED)
        ok0a = False

    # 0b - views.sql
    log(f"\n{'─'*60}", BLUE)
    log(f"  ▶  Buoc 0b — Tao Views (views.sql)", BOLD)
    log(f"{'─'*60}", BLUE)
    try:
        run_sql_file(views_file, password, db_config, use_db=True)
        log(f"  ✓ Buoc 0b — THANH CONG", GREEN)
        ok0b = True
    except Exception as e:
        log(f"  ✗ Buoc 0b — THAT BAI: {e}", RED)
        ok0b = False

    return ok0a and ok0b


def drop_db(password, db_config):
    import mysql.connector
    cfg = {
        'host':     db_config['host'],
        'port':     db_config.get('port', 3306),
        'user':     db_config['user'],
        'password': password,
        'charset':  'utf8mb4',
    }
    conn   = mysql.connector.connect(**cfg)
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {db_config['database']}")
    conn.commit()
    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--firms',       default='data/firms.xlsx')
    parser.add_argument('--panel',       default='data/panel_2020_2024.xlsx')
    parser.add_argument('--version',     default='v1')
    parser.add_argument('--skip-import', action='store_true')
    parser.add_argument('--reset',       action='store_true')
    parser.add_argument('--out-qc',      default='outputs/qc_report.csv')
    parser.add_argument('--out-panel',   default='outputs/panel_latest.csv')
    args = parser.parse_args()

    py       = sys.executable
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Hoi password 1 lan duy nhat
    if not os.environ.get('DB_PASSWORD'):
        env_path = os.path.join(base_dir, '.env')
        if not os.path.exists(env_path):
            log("\n" + "="*50, BOLD)
            log("  Firm Data Hub — Ket noi MySQL", BOLD)
            log("="*50, BOLD)
            pw = getpass.getpass("  Nhap password MySQL (user: root): ")
            os.environ['DB_PASSWORD'] = pw
            log("  ✓ Password da luu cho session nay\n", GREEN)

    sys.path.insert(0, os.path.join(base_dir, 'etl'))
    from db_config import DB_CONFIG, _get_password
    password = _get_password()

    log(f"\n{'='*60}", BOLD)
    log(f"  FIRM DATA HUB — PIPELINE", BOLD)
    log(f"  {date.today()}", BOLD)
    log(f"{'='*60}", BOLD)

    results = {}

    # Reset neu co flag
    if args.reset:
        log("\n⚠  Reset: Xoa database cu...", YELLOW)
        try:
            drop_db(password, DB_CONFIG)
            log("  ✓ Da xoa database cu", GREEN)
        except Exception as e:
            log(f"  ✗ Khong xoa duoc DB: {e}", RED)
            sys.exit(1)

    # Buoc 0: Khoi tao DB tu dong
    ok = init_db(password, DB_CONFIG, base_dir)
    results['0_init_db'] = ok
    if not ok:
        log("\n✗ Dung pipeline do loi khoi tao DB.", RED)
        sys.exit(1)

    if not args.skip_import:

        # Buoc A
        ok = run(
            [py, 'etl/import_firms.py', '--file', args.firms],
            'Buoc A — Import danh muc doanh nghiep'
        )
        results['A_import_firms'] = ok
        if not ok:
            log("\n✗ Dung pipeline do loi o Buoc A.", RED)
            sys.exit(1)

        # Buoc B
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
                    f'Buoc B — Snapshot {src} nam {yr}'
                )
                if not ok:
                    all_ok = False
        results['B_create_snapshot'] = all_ok
        if not all_ok:
            log("\n✗ Dung pipeline do loi o Buoc B.", RED)
            sys.exit(1)

        # Buoc C
        ok = run(
            [py, 'etl/import_panel.py', '--file', args.panel],
            'Buoc C — Import panel data'
        )
        results['C_import_panel'] = ok
        if not ok:
            log("\n⚠  Buoc C co loi nhung van tiep tuc...", YELLOW)

    # Buoc D
    ok = run(
        [py, 'etl/qc_checks.py', '--out', args.out_qc],
        'Buoc D — Kiem tra chat luong du lieu'
    )
    results['D_qc_checks'] = ok

    # Buoc E
    ok = run(
        [py, 'etl/export_panel.py', '--out', args.out_panel],
        'Buoc E — Xuat dataset panel sach'
    )
    results['E_export_panel'] = ok

    # Ket qua
    log(f"\n{'='*60}", BOLD)
    log(f"  KET QUA PIPELINE", BOLD)
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
        log(f"\n  Pipeline hoan thanh thanh cong!", GREEN + BOLD)
    else:
        log(f"\n  ⚠  Pipeline hoan thanh nhung co loi.", YELLOW)

    log(f"{'='*60}\n", BOLD)


if __name__ == '__main__':
    main()
