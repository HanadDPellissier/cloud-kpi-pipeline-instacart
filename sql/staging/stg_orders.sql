DROP TABLE IF EXISTS stg.stg_orders;

CREATE TABLE stg.stg_orders AS
SELECT
  CAST(order_id AS INT),
  CAST(user_id AS INT),
  CAST(eval_set AS TEXT),
  CAST(order_number AS INT),
  CAST(order_dow AS INT),
  CAST(order_hour_of_day AS INT),
  CAST(days_since_prior_order AS NUMERIC)
FROM raw.orders;

-- Helpful indexes for joins
CREATE INDEX IF NOT EXISTS idx_stg_orders_user_id ON stg.stg_orders(user_id);