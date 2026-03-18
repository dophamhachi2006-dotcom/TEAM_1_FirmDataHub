-- ============================================================
-- FIRM DATA HUB — schema_and_seed.sql
-- Dựa theo cấu trúc chuẩn vn_firm_panel.sql
-- 
-- MỤC ĐÍCH: Tạo DB + seed dữ liệu mẫu TEST để kiểm tra
--           Dùng được cho MỌI nhóm — không hard-code ticker
--
-- SAU KHI CHẠY FILE NÀY:
--   - Chạy import_firms.py  → để nạp 20 công ty của nhóm
--   - Chạy create_snapshot.py → tạo snapshot
--   - Chạy import_panel.py  → nạp 38 biến
-- ============================================================

CREATE DATABASE IF NOT EXISTS vn_firm_hub
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE vn_firm_hub;

SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================
-- 1. dim_data_source
-- ============================================================
DROP TABLE IF EXISTS dim_data_source;
CREATE TABLE dim_data_source (
  source_id   SMALLINT     NOT NULL AUTO_INCREMENT,
  source_name VARCHAR(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  source_type ENUM('market','financial_statement','ownership','text_report','manual')
              COLLATE utf8mb4_unicode_ci NOT NULL,
  provider    VARCHAR(150) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  note        VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (source_id),
  UNIQUE KEY source_name (source_name)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO dim_data_source VALUES
(1,'FiinPro',      'ownership',           'FiinGroup',       'Ownership ratios (end-of-year snapshot)'),
(2,'BCTC_Audited', 'financial_statement', 'Company/Exchange','Audited financial statements'),
(3,'Vietstock',    'market',              'Vietstock',       'Market fields (price, shares, dividend, EPS)'),
(4,'AnnualReport', 'text_report',         'Company',         'Annual report / disclosures for innovation & headcount');

-- ============================================================
-- 2. dim_exchange
-- ============================================================
DROP TABLE IF EXISTS dim_exchange;
CREATE TABLE dim_exchange (
  exchange_id   TINYINT      NOT NULL AUTO_INCREMENT,
  exchange_code VARCHAR(10)  COLLATE utf8mb4_unicode_ci NOT NULL,
  exchange_name VARCHAR(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (exchange_id),
  UNIQUE KEY exchange_code (exchange_code)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO dim_exchange VALUES
(1,'HOSE', 'Ho Chi Minh Stock Exchange'),
(2,'HNX',  'Hanoi Stock Exchange'),
(3,'UPCOM','Unlisted Public Company Market');

-- ============================================================
-- 3. dim_industry_l2
-- ============================================================
DROP TABLE IF EXISTS dim_industry_l2;
CREATE TABLE dim_industry_l2 (
  industry_l2_id   SMALLINT     NOT NULL AUTO_INCREMENT,
  industry_l2_name VARCHAR(150) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (industry_l2_id),
  UNIQUE KEY industry_l2_name (industry_l2_name)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO dim_industry_l2 VALUES
(1,'Tài nguyên Cơ bản'),
(2,'Thực phẩm và đồ uống'),
(3,'Hóa chất'),
(4,'Dầu khí'),
(5,'Hàng & Dịch vụ Công nghiệp'),
(6,'Hàng cá nhân & Gia dụng'),
(7,'Xây dựng và Vật liệu'),
(8,'Ô tô và phụ tùng'),
(9,'Y tế');

-- ============================================================
-- 4. dim_firm
-- ============================================================
DROP TABLE IF EXISTS dim_firm;
CREATE TABLE dim_firm (
  firm_id        BIGINT       NOT NULL AUTO_INCREMENT,
  ticker         VARCHAR(20)  COLLATE utf8mb4_unicode_ci NOT NULL,
  company_name   VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  exchange_id    TINYINT      NOT NULL,
  industry_l2_id SMALLINT     DEFAULT NULL,
  founded_year   SMALLINT     DEFAULT NULL,
  listed_year    SMALLINT     DEFAULT NULL,
  status         ENUM('active','delisted','inactive')
                 COLLATE utf8mb4_unicode_ci DEFAULT 'active',
  created_at     TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (firm_id),
  UNIQUE KEY ticker (ticker),
  KEY fk_firm_exchange (exchange_id),
  KEY fk_firm_industry (industry_l2_id),
  CONSTRAINT fk_firm_exchange FOREIGN KEY (exchange_id)    REFERENCES dim_exchange(exchange_id),
  CONSTRAINT fk_firm_industry FOREIGN KEY (industry_l2_id) REFERENCES dim_industry_l2(industry_l2_id)
) ENGINE=InnoDB AUTO_INCREMENT=514 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: 1 công ty TEST để kiểm tra ETL (giống file gốc giảng viên)
-- Các nhóm KHÔNG xóa dòng này — dùng để test pipeline trước khi import data thật
INSERT INTO dim_firm VALUES
(1,'TEST','Test Corporation (for ETL/DB testing)',1,5,2010,2015,'active',
 '2026-01-20 04:32:22','2026-01-20 04:32:22');

-- ============================================================
-- 5. fact_data_snapshot
-- ============================================================
DROP TABLE IF EXISTS fact_data_snapshot;
CREATE TABLE fact_data_snapshot (
  snapshot_id   BIGINT      NOT NULL AUTO_INCREMENT,
  snapshot_date DATE        NOT NULL,
  period_from   DATE        DEFAULT NULL,
  period_to     DATE        DEFAULT NULL,
  fiscal_year   SMALLINT    NOT NULL,
  source_id     SMALLINT    NOT NULL,
  version_tag   VARCHAR(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  created_by    VARCHAR(80) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  created_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (snapshot_id),
  UNIQUE KEY uq_snapshot (snapshot_date, fiscal_year, source_id, version_tag),
  KEY fk_snapshot_source (source_id),
  CONSTRAINT fk_snapshot_source FOREIGN KEY (source_id) REFERENCES dim_data_source(source_id)
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: 20 snapshots cho TEST firm (4 nguồn × 5 năm 2020-2024)
INSERT INTO fact_data_snapshot VALUES
-- Source 1: FiinPro (ownership) — 5 năm
(1, '2021-01-20',NULL,NULL,2020,1,'v1','seed','2026-01-20 04:32:22'),
(2, '2022-01-20',NULL,NULL,2021,1,'v1','seed','2026-01-20 04:32:22'),
(3, '2023-01-20',NULL,NULL,2022,1,'v1','seed','2026-01-20 04:32:22'),
(4, '2024-01-20',NULL,NULL,2023,1,'v1','seed','2026-01-20 04:32:22'),
(5, '2025-01-20',NULL,NULL,2024,1,'v1','seed','2026-01-20 04:32:22'),
-- Source 2: BCTC_Audited (financial) — 5 năm
(6, '2021-03-31',NULL,NULL,2020,2,'v1','seed','2026-01-20 04:32:22'),
(7, '2022-03-31',NULL,NULL,2021,2,'v1','seed','2026-01-20 04:32:22'),
(8, '2023-03-31',NULL,NULL,2022,2,'v1','seed','2026-01-20 04:32:22'),
(9, '2024-03-31',NULL,NULL,2023,2,'v1','seed','2026-01-20 04:32:22'),
(10,'2025-03-31',NULL,NULL,2024,2,'v1','seed','2026-01-20 04:32:22'),
-- Source 3: Vietstock (market) — 5 năm
(11,'2021-01-05',NULL,NULL,2020,3,'v1','seed','2026-01-20 04:32:22'),
(12,'2022-01-05',NULL,NULL,2021,3,'v1','seed','2026-01-20 04:32:22'),
(13,'2023-01-05',NULL,NULL,2022,3,'v1','seed','2026-01-20 04:32:22'),
(14,'2024-01-05',NULL,NULL,2023,3,'v1','seed','2026-01-20 04:32:22'),
(15,'2025-01-05',NULL,NULL,2024,3,'v1','seed','2026-01-20 04:32:22'),
-- Source 4: AnnualReport (innovation + meta) — 5 năm
(16,'2021-04-15',NULL,NULL,2020,4,'v1','seed','2026-01-20 04:32:22'),
(17,'2022-04-15',NULL,NULL,2021,4,'v1','seed','2026-01-20 04:32:22'),
(18,'2023-04-15',NULL,NULL,2022,4,'v1','seed','2026-01-20 04:32:22'),
(19,'2024-04-15',NULL,NULL,2023,4,'v1','seed','2026-01-20 04:32:22'),
(20,'2025-04-15',NULL,NULL,2024,4,'v1','seed','2026-01-20 04:32:22');

-- ============================================================
-- 6. fact_ownership_year
-- ============================================================
DROP TABLE IF EXISTS fact_ownership_year;
CREATE TABLE fact_ownership_year (
  firm_id               BIGINT        NOT NULL,
  fiscal_year           SMALLINT      NOT NULL,
  snapshot_id           BIGINT        NOT NULL,
  managerial_inside_own DECIMAL(10,6) DEFAULT NULL,
  state_own             DECIMAL(10,6) DEFAULT NULL,
  institutional_own     DECIMAL(10,6) DEFAULT NULL,
  foreign_own           DECIMAL(10,6) DEFAULT NULL,
  note                  VARCHAR(255)  COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  created_at            TIMESTAMP     NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
  KEY fk_own_snapshot (snapshot_id),
  CONSTRAINT fk_own_firm     FOREIGN KEY (firm_id)     REFERENCES dim_firm(firm_id),
  CONSTRAINT fk_own_snapshot FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: TEST firm ownership 2020-2024
INSERT INTO fact_ownership_year VALUES
(1,2020,1, 0.050000,0.000000,0.150000,0.200000,'seed','2026-01-20 04:32:22'),
(1,2021,2, 0.055000,0.000000,0.155000,0.210000,'seed','2026-01-20 04:32:22'),
(1,2022,3, 0.060000,0.000000,0.160000,0.220000,'seed','2026-01-20 04:32:22'),
(1,2023,4, 0.062000,0.000000,0.165000,0.230000,'seed','2026-01-20 04:32:22'),
(1,2024,5, 0.065000,0.000000,0.170000,0.240000,'seed','2026-01-20 04:32:22');

-- ============================================================
-- 7. fact_financial_year
-- ============================================================
DROP TABLE IF EXISTS fact_financial_year;
CREATE TABLE fact_financial_year (
  firm_id                     BIGINT        NOT NULL,
  fiscal_year                 SMALLINT      NOT NULL,
  snapshot_id                 BIGINT        NOT NULL,
  unit_scale                  BIGINT        NOT NULL DEFAULT 1,
  currency_code               CHAR(3)       COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'VND',
  net_sales                   DECIMAL(20,2) DEFAULT NULL,
  total_assets                DECIMAL(20,2) DEFAULT NULL,
  selling_expenses            DECIMAL(20,2) DEFAULT NULL,
  general_admin_expenses      DECIMAL(20,2) DEFAULT NULL,
  intangible_assets_net       DECIMAL(20,2) DEFAULT NULL,
  manufacturing_overhead      DECIMAL(20,2) DEFAULT NULL,
  net_operating_income        DECIMAL(20,2) DEFAULT NULL,
  raw_material_consumption    DECIMAL(20,2) DEFAULT NULL,
  merchandise_purchase_year   DECIMAL(20,2) DEFAULT NULL,
  wip_goods_purchase          DECIMAL(20,2) DEFAULT NULL,
  outside_manufacturing_expenses DECIMAL(20,2) DEFAULT NULL,
  production_cost             DECIMAL(20,2) DEFAULT NULL,
  rnd_expenses                DECIMAL(20,2) DEFAULT NULL,
  net_income                  DECIMAL(20,2) DEFAULT NULL,
  total_equity                DECIMAL(20,2) DEFAULT NULL,
  total_liabilities           DECIMAL(20,2) DEFAULT NULL,
  cash_and_equivalents        DECIMAL(20,2) DEFAULT NULL,
  long_term_debt              DECIMAL(20,2) DEFAULT NULL,
  current_assets              DECIMAL(20,2) DEFAULT NULL,
  current_liabilities         DECIMAL(20,2) DEFAULT NULL,
  growth_ratio                DECIMAL(10,6) DEFAULT NULL,
  inventory                   DECIMAL(20,2) DEFAULT NULL,
  net_ppe                     DECIMAL(20,2) DEFAULT NULL,
  created_at                  TIMESTAMP     NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
  KEY fk_fin_snapshot (snapshot_id),
  CONSTRAINT fk_fin_firm     FOREIGN KEY (firm_id)     REFERENCES dim_firm(firm_id),
  CONSTRAINT fk_fin_snapshot FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: TEST firm financial 2020-2024
INSERT INTO fact_financial_year VALUES
(1,2020,6,1000000000,'VND',100.00,200.00,5.00,6.00,2.50,7.00,18.00,45.00,9.00,1.50,0.80,65.00,0.60,10.00,80.00,120.00,8.00,25.00,70.00,45.00,NULL,   22.00,55.00,'2026-01-20 04:32:22'),
(1,2021,7,1000000000,'VND',110.00,215.00,5.30,6.40,2.70,7.40,19.50,48.00,9.80,1.60,0.85,69.00,0.70,11.00,86.00,129.00,8.80,26.50,75.00,47.00,0.100000,23.50,58.00,'2026-01-20 04:32:22'),
(1,2022,8,1000000000,'VND',120.00,230.00,5.60,6.80,3.00,7.90,21.00,51.00,10.50,1.70,0.90,73.00,0.85,12.00,92.00,138.00,9.50,28.00,80.00,49.50,0.090909,25.00,61.00,'2026-01-20 04:32:22'),
(1,2023,9,1000000000,'VND',130.00,250.00,6.00,7.20,3.30,8.40,22.50,54.00,11.20,1.80,1.00,78.00,0.95,13.00,100.00,150.00,10.20,30.00,86.00,52.00,0.083333,26.80,65.00,'2026-01-20 04:32:22'),
(1,2024,10,1000000000,'VND',145.00,275.00,6.60,7.80,3.70,9.20,25.00,60.00,12.50,2.00,1.15,86.00,1.20,15.00,112.00,163.00,12.00,33.00,95.00,56.00,0.115385,29.00,72.00,'2026-01-20 04:32:22');

-- ============================================================
-- 8. fact_cashflow_year
-- ============================================================
DROP TABLE IF EXISTS fact_cashflow_year;
CREATE TABLE fact_cashflow_year (
  firm_id       BIGINT        NOT NULL,
  fiscal_year   SMALLINT      NOT NULL,
  snapshot_id   BIGINT        NOT NULL,
  unit_scale    BIGINT        NOT NULL DEFAULT 1,
  currency_code CHAR(3)       COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'VND',
  net_cfo       DECIMAL(20,2) DEFAULT NULL,
  capex         DECIMAL(20,2) DEFAULT NULL,
  net_cfi       DECIMAL(20,2) DEFAULT NULL,
  created_at    TIMESTAMP     NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
  KEY fk_cf_snapshot (snapshot_id),
  CONSTRAINT fk_cf_firm     FOREIGN KEY (firm_id)     REFERENCES dim_firm(firm_id),
  CONSTRAINT fk_cf_snapshot FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: TEST firm cashflow 2020-2024
INSERT INTO fact_cashflow_year VALUES
(1,2020,6,1000000000,'VND',12.00,6.00, -5.00,'2026-01-20 04:32:22'),
(1,2021,7,1000000000,'VND',13.20,6.50, -5.80,'2026-01-20 04:32:22'),
(1,2022,8,1000000000,'VND',14.50,7.20, -6.40,'2026-01-20 04:32:22'),
(1,2023,9,1000000000,'VND',15.80,8.00, -7.10,'2026-01-20 04:32:22'),
(1,2024,10,1000000000,'VND',18.20,9.50,-8.30,'2026-01-20 04:32:22');

-- ============================================================
-- 9. fact_market_year
-- ============================================================
DROP TABLE IF EXISTS fact_market_year;
CREATE TABLE fact_market_year (
  firm_id              BIGINT        NOT NULL,
  fiscal_year          SMALLINT      NOT NULL,
  snapshot_id          BIGINT        NOT NULL,
  shares_outstanding   BIGINT        DEFAULT NULL,
  price_reference      ENUM('close_year_end','avg_year','close_fiscal_end')
                       COLLATE utf8mb4_unicode_ci DEFAULT 'close_year_end',
  share_price          DECIMAL(20,4) DEFAULT NULL,
  market_value_equity  DECIMAL(20,2) DEFAULT NULL,
  dividend_cash_paid   DECIMAL(20,2) DEFAULT NULL,
  eps_basic            DECIMAL(20,6) DEFAULT NULL,
  currency_code        CHAR(3)       COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'VND',
  created_at           TIMESTAMP     NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
  KEY fk_mkt_snapshot (snapshot_id),
  CONSTRAINT fk_mkt_firm     FOREIGN KEY (firm_id)     REFERENCES dim_firm(firm_id),
  CONSTRAINT fk_mkt_snapshot FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: TEST firm market 2020-2024
INSERT INTO fact_market_year VALUES
(1,2020,11,1000000000,'close_year_end',20000.0000,20000000000000.00,500000000000.00, 1000.000000,'VND','2026-01-20 04:32:22'),
(1,2021,12,1000000000,'close_year_end',22000.0000,22000000000000.00,550000000000.00, 1100.000000,'VND','2026-01-20 04:32:22'),
(1,2022,13,1000000000,'close_year_end',21000.0000,21000000000000.00,600000000000.00, 1200.000000,'VND','2026-01-20 04:32:22'),
(1,2023,14,1000000000,'close_year_end',25000.0000,25000000000000.00,650000000000.00, 1300.000000,'VND','2026-01-20 04:32:22'),
(1,2024,15,1000000000,'close_year_end',28000.0000,28000000000000.00,700000000000.00, 1500.000000,'VND','2026-01-20 04:32:22');

-- ============================================================
-- 10. fact_innovation_year
-- ============================================================
DROP TABLE IF EXISTS fact_innovation_year;
CREATE TABLE fact_innovation_year (
  firm_id             BIGINT       NOT NULL,
  fiscal_year         SMALLINT     NOT NULL,
  snapshot_id         BIGINT       NOT NULL,
  product_innovation  TINYINT      DEFAULT NULL,
  process_innovation  TINYINT      DEFAULT NULL,
  evidence_source_id  SMALLINT     DEFAULT NULL,
  evidence_note       VARCHAR(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  created_at          TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
  KEY fk_innov_snapshot (snapshot_id),
  KEY fk_innov_source   (evidence_source_id),
  CONSTRAINT fk_innov_firm     FOREIGN KEY (firm_id)            REFERENCES dim_firm(firm_id),
  CONSTRAINT fk_innov_snapshot FOREIGN KEY (snapshot_id)        REFERENCES fact_data_snapshot(snapshot_id),
  CONSTRAINT fk_innov_source   FOREIGN KEY (evidence_source_id) REFERENCES dim_data_source(source_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: TEST firm innovation 2020-2024
INSERT INTO fact_innovation_year VALUES
(1,2020,16,0,0,4,'Seed: no innovation reported',             '2026-01-20 04:32:22'),
(1,2021,17,1,0,4,'Seed: launched new product line',          '2026-01-20 04:32:22'),
(1,2022,18,0,1,4,'Seed: implemented new manufacturing process','2026-01-20 04:32:22'),
(1,2023,19,1,1,4,'Seed: product + process innovation',       '2026-01-20 04:32:22'),
(1,2024,20,1,0,4,'Seed: upgraded product portfolio',         '2026-01-20 04:32:22');

-- ============================================================
-- 11. fact_firm_year_meta
-- ============================================================
DROP TABLE IF EXISTS fact_firm_year_meta;
CREATE TABLE fact_firm_year_meta (
  firm_id          BIGINT   NOT NULL,
  fiscal_year      SMALLINT NOT NULL,
  snapshot_id      BIGINT   NOT NULL,
  employees_count  INT      DEFAULT NULL,
  firm_age         SMALLINT DEFAULT NULL,
  created_at       TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
  KEY fk_meta_snapshot (snapshot_id),
  CONSTRAINT fk_meta_firm     FOREIGN KEY (firm_id)     REFERENCES dim_firm(firm_id),
  CONSTRAINT fk_meta_snapshot FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: TEST firm meta 2020-2024
INSERT INTO fact_firm_year_meta VALUES
(1,2020,16,1000,11,'2026-01-20 04:32:22'),
(1,2021,17,1050,12,'2026-01-20 04:32:22'),
(1,2022,18,1100,13,'2026-01-20 04:32:22'),
(1,2023,19,1150,14,'2026-01-20 04:32:22'),
(1,2024,20,1200,15,'2026-01-20 04:32:22');

-- ============================================================
-- 12. fact_value_override_log
-- ============================================================
DROP TABLE IF EXISTS fact_value_override_log;
CREATE TABLE fact_value_override_log (
  override_id  BIGINT       NOT NULL AUTO_INCREMENT,
  firm_id      BIGINT       NOT NULL,
  fiscal_year  SMALLINT     NOT NULL,
  table_name   VARCHAR(80)  COLLATE utf8mb4_unicode_ci NOT NULL,
  column_name  VARCHAR(80)  COLLATE utf8mb4_unicode_ci NOT NULL,
  old_value    VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  new_value    VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  reason       VARCHAR(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  changed_by   VARCHAR(80)  COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  changed_at   TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (override_id),
  KEY fk_override_firm (firm_id),
  CONSTRAINT fk_override_firm FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================
-- 13. VIEW vw_firm_panel_latest
-- Lấy snapshot mới nhất cho mỗi firm × fiscal_year
-- ============================================================
CREATE OR REPLACE VIEW vw_firm_panel_latest AS
SELECT
    f.ticker, f.company_name,
    e.exchange_code,
    i.industry_l2_name,
    fy.fiscal_year,
    fy.unit_scale,
    -- Financial
    fy.net_sales, fy.total_assets, fy.selling_expenses,
    fy.general_admin_expenses, fy.intangible_assets_net,
    fy.manufacturing_overhead, fy.net_operating_income,
    fy.raw_material_consumption, fy.merchandise_purchase_year,
    fy.wip_goods_purchase, fy.outside_manufacturing_expenses,
    fy.production_cost, fy.rnd_expenses,
    fy.net_income, fy.total_equity, fy.total_liabilities,
    fy.cash_and_equivalents, fy.long_term_debt,
    fy.current_assets, fy.current_liabilities,
    fy.growth_ratio, fy.inventory, fy.net_ppe,
    -- Ownership
    oy.managerial_inside_own, oy.state_own,
    oy.institutional_own, oy.foreign_own,
    -- Market
    my.shares_outstanding, my.share_price,
    my.market_value_equity, my.dividend_cash_paid, my.eps_basic,
    -- Cashflow
    cf.net_cfo, cf.capex, cf.net_cfi,
    -- Innovation
    iv.product_innovation, iv.process_innovation, iv.evidence_note,
    -- Meta
    mt.employees_count, mt.firm_age
FROM dim_firm f
JOIN dim_exchange     e ON e.exchange_id    = f.exchange_id
LEFT JOIN dim_industry_l2 i ON i.industry_l2_id = f.industry_l2_id
JOIN fact_financial_year fy ON fy.firm_id = f.firm_id
    AND fy.snapshot_id = (
        SELECT MAX(fx.snapshot_id) FROM fact_financial_year fx
        WHERE fx.firm_id = f.firm_id AND fx.fiscal_year = fy.fiscal_year)
LEFT JOIN fact_ownership_year oy ON oy.firm_id = f.firm_id
    AND oy.fiscal_year = fy.fiscal_year
    AND oy.snapshot_id = (
        SELECT MAX(ox.snapshot_id) FROM fact_ownership_year ox
        WHERE ox.firm_id = f.firm_id AND ox.fiscal_year = fy.fiscal_year)
LEFT JOIN fact_market_year my ON my.firm_id = f.firm_id
    AND my.fiscal_year = fy.fiscal_year
    AND my.snapshot_id = (
        SELECT MAX(mx.snapshot_id) FROM fact_market_year mx
        WHERE mx.firm_id = f.firm_id AND mx.fiscal_year = fy.fiscal_year)
LEFT JOIN fact_cashflow_year cf ON cf.firm_id = f.firm_id
    AND cf.fiscal_year = fy.fiscal_year
    AND cf.snapshot_id = (
        SELECT MAX(cx.snapshot_id) FROM fact_cashflow_year cx
        WHERE cx.firm_id = f.firm_id AND cx.fiscal_year = fy.fiscal_year)
LEFT JOIN fact_innovation_year iv ON iv.firm_id = f.firm_id
    AND iv.fiscal_year = fy.fiscal_year
    AND iv.snapshot_id = (
        SELECT MAX(ix.snapshot_id) FROM fact_innovation_year ix
        WHERE ix.firm_id = f.firm_id AND ix.fiscal_year = fy.fiscal_year)
LEFT JOIN fact_firm_year_meta mt ON mt.firm_id = f.firm_id
    AND mt.fiscal_year = fy.fiscal_year
    AND mt.snapshot_id = (
        SELECT MAX(mx2.snapshot_id) FROM fact_firm_year_meta mx2
        WHERE mx2.firm_id = f.firm_id AND mx2.fiscal_year = fy.fiscal_year)
ORDER BY f.ticker, fy.fiscal_year;

SET FOREIGN_KEY_CHECKS = 1;
