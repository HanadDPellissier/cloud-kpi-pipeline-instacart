DROP TABLE IF EXISTS stg.stg_order_products_prior;

CREATE TABLE stg.stg_order_products_prior AS
SELECT
 CAST(order_id AS int) AS order_id,
 CAST(product_id AS int) AS product_id,
 CAST(add_to_cart_order AS int) AS add_to_cart_order,
 CAST(reordered AS int) AS reordered
FROM raw.order_products_prior;

CREATE INDEX IF NOT EXISTS idx_stg_prior_order_id ON stg.stg_order_products_prior(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_prior_product_id ON stg.stg_order_products_prior(product_id);