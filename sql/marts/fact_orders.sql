DROP TABLE IF EXISTS marts.fact_orders;

CREATE TABLE marts.fact_orders AS
SELECT
    o.order_id,
    o.user_id,
    o.order_number,
    o.order_dow,
    o.order_hour_of_day,
    o.days_since_prior_order,

    COUNT(p.product_id)                              AS items_in_order,
    SUM(CASE WHEN p.reordered = 1 THEN 1 ELSE 0 END) AS reordered_items_in_order,

    CASE
        WHEN COUNT(p.product_id) = 0 THEN 0
        ELSE
            CAST(SUM(CASE WHEN p.reordered = 1 THEN 1 ELSE 0 END) AS NUMERIC)
            / COUNT(p.product_id)
    END                                               AS reorder_item_rate,

    CASE
        WHEN SUM(CASE WHEN p.reordered = 1 THEN 1 ELSE 0 END) > 0
        THEN TRUE
        ELSE FALSE
    END                                               AS has_any_reorder

FROM stg.stg_orders o
LEFT JOIN stg.stg_order_products_prior p
    ON o.order_id = p.order_id
GROUP BY
    o.order_id,
    o.user_id,
    o.order_number,
    o.order_dow,
    o.order_hour_of_day,
    o.days_since_prior_order;

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id
    ON marts.fact_orders(order_id);

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_dow
    ON marts.fact_orders(order_dow);