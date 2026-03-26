"""
db_config.py
------------
Cấu hình kết nối MySQL.
Thứ tự ưu tiên đọc password:
  1. File .env (nếu có)
  2. Biến môi trường DB_PASSWORD
  3. Hỏi người dùng nhập tay khi chạy
"""

import os
import getpass
import mysql.connector

# Đọc từ file .env nếu có
def _load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, val = line.partition('=')
                    os.environ.setdefault(key.strip(), val.strip())

_load_env()

# Cache password trong session để không hỏi lại nhiều lần
_cached_password = None

def _get_password() -> str:
    global _cached_password
    # Ưu tiên 1: file .env
    pw = os.environ.get('DB_PASSWORD', '')
    if pw:
        return pw
    # Ưu tiên 2: đã nhập trong cùng session
    if _cached_password is not None:
        return _cached_password
    # Ưu tiên 3: hỏi người dùng
    print("\n" + "="*50)
    print("  Firm Data Hub — Kết nối MySQL")
    print("="*50)
    _cached_password = getpass.getpass(
        f"  Nhập password MySQL (user: {os.environ.get('DB_USER','root')}): "
    )
    return _cached_password


DB_CONFIG = {
    'host':        os.environ.get('DB_HOST', 'localhost'),
    'port':        int(os.environ.get('DB_PORT', '3306')),
    'user':        os.environ.get('DB_USER', 'root'),
    'database':    os.environ.get('DB_NAME', 'vn_firm_hub'),
    'charset':     'utf8mb4',
    'use_unicode': True,
}


def get_connection():
    """Trả về connection MySQL — tự hỏi password nếu chưa có."""
    config = {**DB_CONFIG, 'password': _get_password()}
    try:
        return mysql.connector.connect(**config)
    except mysql.connector.errors.ProgrammingError:
        # Nếu sai password → xóa cache, hỏi lại
        global _cached_password
        _cached_password = None
        print("\n  [ERROR] Sai password! Thử lại...\n")
        config['password'] = _get_password()
        return mysql.connector.connect(**config)
