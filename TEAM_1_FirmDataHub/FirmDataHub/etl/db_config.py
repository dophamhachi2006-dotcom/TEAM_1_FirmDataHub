"""
db_config.py
------------
Cấu hình kết nối MySQL.
Chỉnh HOST, USER, PASSWORD, DATABASE cho đúng máy của nhóm.
"""

import mysql.connector

DB_CONFIG = {
    'host':     'localhost',
    'port':     3306,
    'user':     'root',
    'password': '1234',   # ← ĐỔI CÁI NÀY
    'database': 'vn_firm_hub',
}


def get_connection():
    """Trả về connection MySQL đang mở."""
    return mysql.connector.connect(**DB_CONFIG)
