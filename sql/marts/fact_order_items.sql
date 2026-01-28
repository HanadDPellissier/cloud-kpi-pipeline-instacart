DROP TABLE IF EXISTS marts.fact_order_items;

CREATE TABLE marts.fact_order_items AS
SELECT
    p.order_id,
    p.product_id,

    CAST(p.add_to_cart_order AS INT) AS add_to_cart_order,
    CAST(p.reordered AS INT)         AS reordered,

    o.user_id,
    o.order_dow,
    o.order_hour_of_day

FROM stg.stg_order_products_prior p
JOIN stg.stg_orders o
    ON p.order_id = o.order_id;

CREATE INDEX IF NOT EXISTS idx_fact_order_items_order_id
    ON marts.fact_order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_product_id
    ON marts.fact_order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_user_id
    ON marts.fact_order_items(user_id);