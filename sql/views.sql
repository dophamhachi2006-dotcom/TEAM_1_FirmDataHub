-- ============================================================
-- views.sql
-- Tạo view vw_firm_panel_latest
-- Chạy file này sau khi đã chạy schema_and_seed.sql
-- ============================================================
DROP DATABASE IF EXISTS vn_firm_hub;
USE vn_firm_hub;

CREATE OR REPLACE VIEW vw_firm_panel_latest AS

-- 1. TẠO XƯƠNG SỐNG (Gom tất cả các năm có dữ liệu)
WITH All_Firm_Years AS (
    SELECT firm_id, fiscal_year FROM fact_financial_year
    UNION SELECT firm_id, fiscal_year FROM fact_market_year
    UNION SELECT firm_id, fiscal_year FROM fact_ownership_year
    UNION SELECT firm_id, fiscal_year FROM fact_cashflow_year
    UNION SELECT firm_id, fiscal_year FROM fact_innovation_year
    UNION SELECT firm_id, fiscal_year FROM fact_firm_year_meta
)

SELECT
    -- Định danh
    f.ticker,
    f.company_name,
    e.exchange_code,
    i.industry_l2_name,
    base.fiscal_year,   -- SỬA Ở ĐÂY: Dùng năm của xương sống thay vì của bảng Financial
    fy.unit_scale,

    -- (1-4) Ownership
    oy.managerial_inside_own,
    oy.state_own,
    oy.institutional_own,
    oy.foreign_own,

    -- (5) Shares outstanding
    my.shares_outstanding,

    -- (6-18) Financial — P&L & cost structure
    fy.net_sales,
    fy.total_assets,
    fy.selling_expenses,
    fy.general_admin_expenses,
    fy.intangible_assets_net,
    fy.manufacturing_overhead,
    fy.net_operating_income,
    fy.raw_material_consumption,
    fy.merchandise_purchase_year,
    fy.wip_goods_purchase,
    fy.outside_manufacturing_expenses,
    fy.production_cost,
    fy.rnd_expenses,

    -- (19-20) Innovation
    iv.product_innovation,
    iv.process_innovation,
    iv.evidence_note,

    -- (21-22) Income & Equity
    fy.net_income,
    fy.total_equity,

    -- (23) Market value
    my.share_price,
    my.market_value_equity,

    -- (24) Liabilities
    fy.total_liabilities,

    -- (25-27) Cashflow
    cf.net_cfo,
    cf.capex,
    cf.net_cfi,

    -- (28-33) Balance Sheet
    fy.cash_and_equivalents,
    fy.long_term_debt,
    fy.current_assets,
    fy.current_liabilities,
    fy.growth_ratio,
    fy.inventory,

    -- (34-35) Dividend & EPS
    my.dividend_cash_paid,
    my.eps_basic,

    -- (36) Employees
    mt.employees_count,

    -- (37) PPE
    fy.net_ppe,

    -- (38) Firm age
    mt.firm_age

-- 2. ĐỔI TRẠM GỐC VÀ DÙNG LEFT JOIN CHO TẤT CẢ
FROM All_Firm_Years base
JOIN dim_firm f 
    ON f.firm_id = base.firm_id
JOIN dim_exchange e 
    ON e.exchange_id = f.exchange_id
LEFT JOIN dim_industry_l2 i 
    ON i.industry_l2_id = f.industry_l2_id

-- Financial (Giờ đã bị giáng cấp xuống LEFT JOIN, bình đẳng như các bảng khác)
LEFT JOIN fact_financial_year fy
    ON fy.firm_id = base.firm_id 
    AND fy.fiscal_year = base.fiscal_year
    AND fy.snapshot_id = (
        SELECT MAX(fx.snapshot_id) FROM fact_financial_year fx
        WHERE fx.firm_id = base.firm_id AND fx.fiscal_year = base.fiscal_year
    )

-- Ownership 
LEFT JOIN fact_ownership_year oy
    ON oy.firm_id = base.firm_id 
    AND oy.fiscal_year = base.fiscal_year
    AND oy.snapshot_id = (
        SELECT MAX(ox.snapshot_id) FROM fact_ownership_year ox
        WHERE ox.firm_id = base.firm_id AND ox.fiscal_year = base.fiscal_year
    )

-- Market 
LEFT JOIN fact_market_year my
    ON my.firm_id = base.firm_id 
    AND my.fiscal_year = base.fiscal_year
    AND my.snapshot_id = (
        SELECT MAX(mx.snapshot_id) FROM fact_market_year mx
        WHERE mx.firm_id = base.firm_id AND mx.fiscal_year = base.fiscal_year
    )

-- Cashflow 
LEFT JOIN fact_cashflow_year cf
    ON cf.firm_id = base.firm_id 
    AND cf.fiscal_year = base.fiscal_year
    AND cf.snapshot_id = (
        SELECT MAX(cx.snapshot_id) FROM fact_cashflow_year cx
        WHERE cx.firm_id = base.firm_id AND cx.fiscal_year = base.fiscal_year
    )

-- Innovation 
LEFT JOIN fact_innovation_year iv
    ON iv.firm_id = base.firm_id 
    AND iv.fiscal_year = base.fiscal_year
    AND iv.snapshot_id = (
        SELECT MAX(ix.snapshot_id) FROM fact_innovation_year ix
        WHERE ix.firm_id = base.firm_id AND ix.fiscal_year = base.fiscal_year
    )

-- Meta 
LEFT JOIN fact_firm_year_meta mt
    ON mt.firm_id = base.firm_id 
    AND mt.fiscal_year = base.fiscal_year
    AND mt.snapshot_id = (
        SELECT MAX(mx2.snapshot_id) FROM fact_firm_year_meta mx2
        WHERE mx2.firm_id = base.firm_id AND mx2.fiscal_year = base.fiscal_year
    )

-- Chỉ lấy firm đang hoạt động
WHERE f.status = 'active'

ORDER BY f.ticker, base.fiscal_year;

show tables;
DESC dim_data_source;
SELECT * FROM dim_firm;
SELECT * FROM dim_industry_l2;
SELECT * FROM dim_exchange;
SELECT * FROM dim_data_source;
SELECT * FROM fact_financial_year;
SELECT * FROM fact_cashflow_year;
SELECT * FROM fact_market_year;
SELECT * FROM fact_ownership_year;
SELECT * FROM fact_innovation_year;
SELECT * FROM fact_firm_year_meta;
SELECT * FROM fact_data_snapshot;
SELECT * FROM fact_value_override_log;