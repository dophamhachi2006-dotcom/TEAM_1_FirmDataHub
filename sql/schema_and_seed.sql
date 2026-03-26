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

(1,'BCTC_Audited', 'financial_statement', 'Company/Exchange','Audited financial statements'),
(2,'Vietstock',    'market',              'Vietstock',       'Market fields (price, shares, dividend, EPS)'),
(3,'AnnualReport', 'text_report',         'Company',         'Annual report / disclosures for innovation & headcount');

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
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO dim_industry_l2 VALUES
(1, 'Tài nguyên Cơ bản'),
(2, 'Thực phẩm và đồ uống'),
(3, 'Hóa chất'),
(4, 'Dầu khí'),
(5, 'Hàng & Dịch vụ Công nghiệp'),
(6, 'Hàng cá nhân & Gia dụng'),
(7, 'Xây dựng và Vật liệu'),
(8, 'Ô tô và phụ tùng'),
(9, 'Y tế'),
-- Ngành dùng bởi 20 tickers của nhóm
(19,'Thủy sản'),
(20,'Thực phẩm & Đồ uống'),
(21,'Vận tải & Logistics'),
(22,'Xây dựng & Bất động sản'),
(23,'Thương mại'),
(24,'Sản xuất công nghiệp'),
(25,'Cao su & Nhựa'),
(26,'Khai khoáng');

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
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seed: 20 công ty thật của nhóm (firm_id 1–20)
-- exchange_id: 1=HOSE, 2=HNX, 3=UPCOM
-- industry_l2_id tham chiếu dim_industry_l2 đã seed bên trên
INSERT INTO dim_firm
    (firm_id, ticker, company_name, exchange_id, industry_l2_id, founded_year, listed_year, status, created_at, updated_at)
VALUES
( 1,'ASG','CTCP Xuất nhập khẩu Thủy sản An Giang',  1, 19, 1993, 2007,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
( 2,'CMX','CTCP Camimex Group',                      1, 19, 1978, 2007,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
( 3,'EVE','CTCP Everpia',                             1, 20, 2001, 2008,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
( 4,'SFI','CTCP Đại lý Vận tải SAFI',                1, 21, 1993, 2006,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
( 5,'C32','CTCP Đầu tư Xây dựng 3-2',               1, 22, 1976, 2010,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
( 6,'CTF','CTCP City Auto',                          1, 23, 2002, 2017,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
( 7,'HTG','CTCP Đầu tư Trang Thành',                3, 22, 2004, 2016,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
( 8,'SHI','CTCP Quốc tế Sơn Hà',                    1, 24, 1998, 2008,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
( 9,'CAP','CTCP Lâm Nông sản Thực phẩm Yên Bái',    3, 20, 1993, 2010,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(10,'DHA','CTCP Hóa An',                             1,  3, 1976, 2009,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(11,'INN','CTCP Bao bì và In Nông nghiệp',           3, 24, 1976, 2015,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(12,'SMC','CTCP Đầu tư Thương mại SMC',              1, 23, 1990, 2007,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(13,'CLC','CTCP Cát Lợi',                            1, 20, 1976, 2006,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(14,'DNP','CTCP DNP Holding',                        1, 25, 2004, 2010,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(15,'KSB','CTCP Khoáng sản và Xây dựng Bình Dương',  1, 26, 1976, 2009,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(16,'VFG','CTCP Khử trùng Việt Nam',                 1,  3, 1976, 2008,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(17,'CLL','CTCP Cảng Cát Lái',                       3, 21, 1975, 2016,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(18,'DPR','CTCP Cao su Đồng Phú',                    1, 25, 1992, 2008,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(19,'NNC','CTCP Đá Núi Nhỏ',                         1, 26, 1976, 2007,'active','2026-03-25 00:00:00','2026-03-25 00:00:00'),
(20,'WCS','CTCP Bến xe Miền Tây',                    3, 21, 2003, 2016,'active','2026-03-25 00:00:00','2026-03-25 00:00:00');

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
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Snapshots sẽ được tạo tự động khi chạy run_pipeline.py (create_snapshot.py)
-- Không seed snapshot cứng ở đây nữa

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

-- Không seed cứng — data sẽ được import qua import_panel.py

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

-- Không seed cứng — data sẽ được import qua import_panel.py

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

-- Không seed cứng — data sẽ được import qua import_panel.py

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

-- Không seed cứng — data sẽ được import qua import_panel.py

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

-- Không seed cứng — data sẽ được import qua import_panel.py

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

-- Không seed cứng — data sẽ được import qua import_panel.py

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
