
-- sql/schema_postgres.sql

DROP TABLE IF EXISTS procurement_orders;
CREATE TABLE procurement_orders (
    po_id TEXT PRIMARY KEY,
    supplier TEXT NOT NULL,
    order_date DATE NOT NULL,
    delivery_date DATE,
    item_category TEXT NOT NULL,
    order_status TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    negotiated_price NUMERIC(12,2) NOT NULL,
    defective_units INTEGER,
    compliance TEXT NOT NULL
);

DROP TABLE IF EXISTS sla_by_category;
CREATE TABLE sla_by_category (
    item_category TEXT PRIMARY KEY,
    sla_days INTEGER NOT NULL
);

INSERT INTO sla_by_category(item_category, sla_days) VALUES
    ('Raw Materials', 14),
    ('Office Supplies', 7),
    ('MRO', 10),
    ('Packaging', 9),
    ('IT Equipment', 12)
ON CONFLICT (item_category) DO NOTHING;

DROP VIEW IF EXISTS v_supplier_monthly_kpis;
CREATE VIEW v_supplier_monthly_kpis AS
WITH enriched AS (
    SELECT
        o.*,
        s.sla_days,
        CASE WHEN delivery_date IS NULL THEN NULL
             ELSE (delivery_date - order_date) END AS lead_time_days,
        CASE WHEN delivery_date IS NULL THEN NULL
             WHEN (delivery_date - order_date) <= s.sla_days THEN 1 ELSE 0 END AS on_time_flag,
        CASE WHEN unit_price > 0 THEN (unit_price - negotiated_price) / unit_price ELSE NULL END AS savings_rate,
        CASE WHEN quantity > 0 AND defective_units IS NOT NULL
             THEN defective_units::NUMERIC / quantity ELSE NULL END AS defect_rate_row
    FROM procurement_orders o
    LEFT JOIN sla_by_category s USING(item_category)
)
SELECT
    supplier,
    DATE_TRUNC('month', order_date)::date AS month,
    COUNT(*) AS orders,
    SUM(CASE WHEN on_time_flag=1 THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*),0) AS on_time_rate,
    AVG(lead_time_days) AS avg_lead_time_days,
    SUM(CASE WHEN order_status='Delivered' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*),0) AS delivery_completion_rate,
    AVG(savings_rate) AS avg_savings_rate,
    SUM(defective_units)::NUMERIC / NULLIF(SUM(quantity),0) AS defect_rate
FROM enriched
GROUP BY supplier, DATE_TRUNC('month', order_date)::date
ORDER BY month, supplier;
