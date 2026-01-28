DROP TABLE IF EXISTS marts.kpi_orders_by_dow;

CREATE TABLE marts.kpi_orders_by_dow AS
SELECT
    order_dow,
    COUNT(*) AS total_orders,
    AVG(items_in_order) AS avg_items_per_order,
    AVG(CASE WHEN has_any_reorder THEN 1.0 ELSE 0.0 END) AS pct_orders_with_any_reorder,
    AVG(reorder_item_rate) AS avg_reorder_item_rate
FROM marts.fact_orders
GROUP BY order_dow
ORDER BY order_dow;

CREATE INDEX IF NOT EXISTS idx_kpi_orders_by_dow
    ON marts.kpi_orders_by_dow(order_dow);