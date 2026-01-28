DROP TABLE IF EXISTS stg.stg_products;

CREATE TABLE stg.stg_products AS
SELECT
  CAST(product_id AS INT),
  CAST(product_name AS TEXT),
  CAST(aisle_id AS INT),
  CAST(department_id AS INT)
FROM raw.products;

CREATE INDEX IF NOT EXISTS idx_stg_products_aisle_id ON stg.stg_products(aisle_id);
CREATE INDEX IF NOT EXISTS idx_stg_products_department_id ON stg.stg_products(department_id);