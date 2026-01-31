from pipeline.common.db import get_conn

def ensure_raw_orders_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS raw.orders (
      order_id        INTEGER PRIMARY KEY,
      user_id         INTEGER NOT NULL,
      eval_set        TEXT NOT NULL,
      order_number    INTEGER NOT NULL,
      order_dow       INTEGER,
      order_hour_of_day INTEGER,
      days_since_prior_order NUMERIC
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            
def ensure_raw_aisles_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS raw.aisles (
      aisle_id INTEGER PRIMARY KEY,
      aisle TEXT NOT NULL
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)

def ensure_raw_departments_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS raw.departments (
      department_id INTEGER PRIMARY KEY,
      department TEXT NOT NULL
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)

def ensure_raw_products_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS raw.products (
      product_id INTEGER PRIMARY KEY,
      product_name TEXT NOT NULL,
      aisle_id INTEGER NOT NULL,
      department_id INTEGER NOT NULL
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)

def ensure_raw_order_products_prior_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS raw.order_products_prior (
      order_id INTEGER NOT NULL,
      product_id INTEGER NOT NULL,
      add_to_cart_order INTEGER,
      reordered INTEGER,
      PRIMARY KEY (order_id, product_id)
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)