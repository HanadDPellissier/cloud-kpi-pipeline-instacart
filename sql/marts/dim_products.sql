DROP TABLE IF EXISTS marts.dim_products;

CREATE TABLE marts.dim_products AS
SELECT
    p.product_id,
    p.product_name,

    p.aisle_id,
    a.aisle,

    p.department_id,
    d.department

FROM stg.stg_products p
LEFT JOIN raw.aisles a
    ON p.aisle_id = a.aisle_id
LEFT JOIN raw.departments d
    ON p.department_id = d.department_id;

CREATE INDEX IF NOT EXISTS idx_dim_products_product_id
    ON marts.dim_products(product_id);