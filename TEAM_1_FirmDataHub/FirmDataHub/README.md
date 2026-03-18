# FIRM DATA HUB — Hướng dẫn triển khai

**Nhóm:** TEAM_X  
**20 Tickers:** ASG, CMX, EVE, SFI, C32, CTF, HTG, SHI, CAP, DHA, INN, SMC, CLC, DNP, KSB, VFG, CLL, DPR, NNC, WCS  
**Phạm vi:** 2020–2024

---

## Cấu trúc thư mục

```
TEAM_X_FirmDataHub/
├── sql/
│   └── schema_and_seed.sql      ← Tạo DB + seed dữ liệu mẫu
├── etl/
│   ├── db_config.py             ← Cấu hình kết nối MySQL (SỬA FILE NÀY TRƯỚC)
│   ├── import_firms.py          ← Script A: Import 20 công ty
│   ├── create_snapshot.py       ← Script B: Tạo phiên bản snapshot
│   ├── import_panel.py          ← Script C: Import 38 biến
│   ├── qc_checks.py             ← Script D: Kiểm tra chất lượng
│   └── export_panel.py          ← Script E: Xuất dataset sạch
├── data/
│   ├── team_tickers.csv         ← Danh sách 20 mã cổ phiếu
│   ├── firms.xlsx               ← Thông tin 20 công ty (INPUT)
│   └── panel_2020_2024.xlsx     ← Dữ liệu 38 biến (INPUT)
├── outputs/
│   ├── qc_report.csv            ← Báo cáo QC (AUTO GENERATED)
│   └── panel_latest.csv         ← Dataset cuối (AUTO GENERATED)
└── README.md
```

---

## Bước 0 — Cài đặt thư viện Python

Mở Terminal (CMD / PowerShell trên Windows), chạy:

```bash
pip install pandas mysql-connector-python openpyxl
```

---

## Bước 1 — Tạo Database MySQL

### 1.1 Mở MySQL Workbench (hoặc cmd)
```bash
mysql -u root -p
```

### 1.2 Chạy file SQL
```sql
source C:/đường/dẫn/đến/TEAM_X_FirmDataHub/sql/schema_and_seed.sql
```
Hoặc trong MySQL Workbench: **File → Open SQL Script → chọn schema_and_seed.sql → Run**

✓ Sau bước này MySQL sẽ có database `vn_firm_hub` với đầy đủ bảng và 20 công ty seed.

---

## Bước 2 — Cấu hình kết nối DB

Mở file `etl/db_config.py` và sửa:

```python
DB_CONFIG = {
    'host':     'localhost',
    'user':     'root',        # ← tên user MySQL của bạn
    'password': '123456',      # ← password MySQL của bạn
    'database': 'vn_firm_hub',
}
```

---

## Bước 3 — Chuẩn bị file dữ liệu

### File `data/firms.xlsx` — cần có các cột:
| ticker | company_name | exchange | industry_l2 | founded_year | listed_year |
|--------|-------------|----------|-------------|-------------|------------|
| ASG | CTCP XNK Thủy sản An Giang | HOSE | Thủy sản | 1993 | 2007 |
| ... | ... | ... | ... | ... | ... |

### File `data/panel_2020_2024.xlsx` — cần có các cột:
| ticker | fiscal_year | net_sales | total_assets | ... (38 biến) |
|--------|-------------|-----------|--------------|----------------|
| ASG | 2020 | 1234.56 | 5678.90 | ... |
| ASG | 2021 | ... | ... | ... |

> **Đơn vị tiền tệ:** Tỷ đồng (VND × 10^9)  
> **Cột tên phải đúng chính xác** với bảng mapping trong `import_panel.py`

---

## Bước 4 — Chạy Pipeline (theo thứ tự)

Mở Terminal, `cd` vào thư mục gốc của project:

```bash
cd C:/đường/dẫn/TEAM_X_FirmDataHub
```

### 4.1 Import danh mục công ty
```bash
python etl/import_firms.py --file data/firms.xlsx
```
✓ Kết quả: 20 công ty xuất hiện trong `dim_firm`

### 4.2 Tạo snapshot cho dữ liệu tài chính
```bash
python etl/create_snapshot.py --source BCTC_Audited --year 2024 --date 2025-03-31 --version v1
```
✓ Ghi nhớ `snapshot_id` in ra màn hình (VD: `snapshot_id = 1`)

> Tạo snapshot riêng cho từng nguồn nếu cần:
> ```bash
> python etl/create_snapshot.py --source Vietstock --year 2024 --date 2025-03-31 --version v1
> python etl/create_snapshot.py --source AnnualReport --year 2024 --date 2025-03-31 --version v1
> ```

### 4.3 Import panel 38 biến
```bash
python etl/import_panel.py --file data/panel_2020_2024.xlsx --snapshot 1
```
✓ Kết quả: 100 dòng trong 6 bảng FACT

### 4.4 Kiểm tra chất lượng dữ liệu
```bash
python etl/qc_checks.py
```
✓ Kết quả: `outputs/qc_report.csv`

### 4.5 Xuất dataset sạch
```bash
python etl/export_panel.py
```
✓ Kết quả: `outputs/panel_latest.csv` (100 dòng, 38+ cột)

---

## Cách cập nhật dữ liệu (nếu phát hiện sai)

1. Tạo snapshot MỚI:
   ```bash
   python etl/create_snapshot.py --source BCTC_Audited --year 2023 --date 2025-06-01 --version v1.1
   ```
2. Import lại với snapshot mới:
   ```bash
   python etl/import_panel.py --file data/panel_2020_2024_v2.xlsx --snapshot 2
   ```
3. View `vw_firm_panel_latest` tự động dùng snapshot mới nhất → `panel_latest.csv` cập nhật tự động.

---

## Nguồn dữ liệu

| Loại dữ liệu | Nguồn | Phương pháp |
|-------------|-------|------------|
| BCTC (tài sản, doanh thu, nợ...) | Báo cáo tài chính kiểm toán | Thu thập thủ công |
| Thị trường (giá CP, vốn hóa, EPS) | Vietstock.vn / CafeF.vn | Thu thập thủ công |
| Sở hữu (ownership ratios) | Báo cáo thường niên / Vietstock | Thu thập thủ công |
| Đổi mới (innovation dummies) | Báo cáo thường niên | Đọc và coding thủ công |
| Nhân viên, tuổi DN | Báo cáo thường niên | Thu thập thủ công |

### Ghi chú về biến thiếu
- **institutional_own**: Ước tính từ danh sách cổ đông lớn công bố. Các tổ chức nhỏ trong nhóm "Cổ đông khác" không được tách riêng nên giá trị là giới hạn dưới.
- **rnd_expenses**: Nhiều công ty trong ngành sản xuất truyền thống không công bố R&D riêng → để NULL và ghi chú.
- **manufacturing_overhead, wip_goods_purchase**: Một số công ty không phân tách chi tiết trong thuyết minh BCTC → để NULL.

---

## Yêu cầu hệ thống

- Python 3.8+
- MySQL 8.0+
- Thư viện: `pandas`, `mysql-connector-python`, `openpyxl`
