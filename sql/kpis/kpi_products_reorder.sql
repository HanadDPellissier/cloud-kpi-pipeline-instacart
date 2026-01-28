DROP TABLE IF EXISTS marts.kpi_products_reorder;

CREATE TABLE marts.kpi_products_reorder AS
SELECT
    i.product_id,
    p.product_name,
    COUNT(DISTINCT i.order_id) AS orders_with_product,
    SUM(CASE WHEN i.reordered = 1 THEN 1 ELSE 0 END) AS times_reordered,
    CASE
        WHEN COUNT(DISTINCT i.order_id) = 0 THEN 0
        ELSE
            CAST(SUM(CASE WHEN i.reordered = 1 THEN 1 ELSE 0 END) AS NUMERIC)
            / COUNT(DISTINCT i.order_id)
    END AS product_reorder_rate
FROM marts.fact_order_items i
JOIN marts.dim_products p
    ON i.product_id = p.product_id
GROUP BY
    i.product_id,
    p.product_name;

CREATE INDEX IF NOT EXISTS idx_kpi_products_reorder_rate
    ON marts.kpi_products_reorder(product_reorder_rate);

CREATE INDEX IF NOT EXISTS idx_kpi_products_orders_with_product
    ON marts.kpi_products_reorder(orders_with_product);