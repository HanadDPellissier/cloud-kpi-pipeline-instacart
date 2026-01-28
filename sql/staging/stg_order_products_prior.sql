DROP TABLE IF EXISTS stg.stg_order_products_prior;

CREATE TABLE stg.stg_order_products_prior AS
SELECT
  order_id::int,
  product_id::int,
  add_to_cart_order::int,
  reordered::int
FROM raw.order_products_prior;

CREATE INDEX IF NOT EXISTS idx_stg_prior_order_id ON stg.stg_order_products_prior(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_prior_product_id ON stg.stg_order_products_prior(product_id);