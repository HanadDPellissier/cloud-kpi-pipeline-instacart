from decimal import Decimal
from typing import Optional, Tuple, List

from ingest.db import get_conn

PASS = "PASS"
FAIL = "FAIL"

def log_check(
    run_id,
    check_name: str,
    table_name: str,
    status: str,
    metric_value: Optional[Decimal] = None,
    threshold: Optional[Decimal] = None,
    details: Optional[str] = None,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.data_quality_results
                    (run_id, check_name, table_name, status, metric_value, threshold, details)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, check_name, table_name, status, metric_value, threshold, details),
            )

def scalar(sql: str) -> Decimal:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            (v,) = cur.fetchone()
            if v is None:
                return Decimal(0)
            return Decimal(str(v))

def run_checks(run_id) -> Tuple[bool, List[str]]:
    """
    Returns: (all_critical_passed, failure_messages)
    """
    failures: List[str] = []

    # ---- Check 1: row count sanity (critical) ----
    # orders should be > 1,000,000; prior should be > 10,000,000; products > 10,000
    checks = [
        ("row_count_min", "raw.orders", "SELECT COUNT(*) FROM raw.orders;", Decimal(1_000_000)),
        ("row_count_min", "raw.order_products_prior", "SELECT COUNT(*) FROM raw.order_products_prior;", Decimal(10_000_000)),
        ("row_count_min", "raw.products", "SELECT COUNT(*) FROM raw.products;", Decimal(10_000)),
    ]

    for check_name, table_name, sql, threshold in checks:
        val = scalar(sql)
        status = PASS if val >= threshold else FAIL
        log_check(run_id, check_name, table_name, status, val, threshold, details=None)
        if status == FAIL:
            failures.append(f"{check_name} failed for {table_name}: {val} < {threshold}")

    # ---- Check 2: not-null key sanity (critical) ----
    nn_checks = [
        ("not_null_key", "raw.orders", "SELECT COUNT(*) FROM raw.orders WHERE order_id IS NULL OR user_id IS NULL;", Decimal(0)),
        ("not_null_key", "raw.products", "SELECT COUNT(*) FROM raw.products WHERE product_id IS NULL OR aisle_id IS NULL OR department_id IS NULL;", Decimal(0)),
        ("not_null_key", "raw.order_products_prior", "SELECT COUNT(*) FROM raw.order_products_prior WHERE order_id IS NULL OR product_id IS NULL;", Decimal(0)),
    ]
    for check_name, table_name, sql, threshold in nn_checks:
        val = scalar(sql)
        status = PASS if val == threshold else FAIL
        log_check(run_id, check_name, table_name, status, val, threshold, details=None)
        if status == FAIL:
            failures.append(f"{check_name} failed for {table_name}: null_key_rows={val}")

    # ---- Check 3: duplicate key sanity (critical) ----
    # orders PK: order_id
    dup_orders = scalar("""
        SELECT COUNT(*) FROM (
          SELECT order_id
          FROM raw.orders
          GROUP BY order_id
          HAVING COUNT(*) > 1
        ) d;
    """)
    status = PASS if dup_orders == 0 else FAIL
    log_check(run_id, "duplicate_key", "raw.orders", status, dup_orders, Decimal(0), details="duplicate order_id groups")
    if status == FAIL:
        failures.append(f"duplicate_key failed for raw.orders: dup_groups={dup_orders}")

    # prior PK: (order_id, product_id)
    dup_prior = scalar("""
        SELECT COUNT(*) FROM (
          SELECT order_id, product_id
          FROM raw.order_products_prior
          GROUP BY order_id, product_id
          HAVING COUNT(*) > 1
        ) d;
    """)
    status = PASS if dup_prior == 0 else FAIL
    log_check(run_id, "duplicate_key", "raw.order_products_prior", status, dup_prior, Decimal(0), details="duplicate (order_id, product_id) groups")
    if status == FAIL:
        failures.append(f"duplicate_key failed for raw.order_products_prior: dup_groups={dup_prior}")

    # ---- Check 4: referential sanity (critical-ish) ----
    # Every prior.order_id should exist in orders.order_id
    missing_orders = scalar("""
        SELECT COUNT(*)
        FROM raw.order_products_prior p
        LEFT JOIN raw.orders o ON o.order_id = p.order_id
        WHERE o.order_id IS NULL;
    """)
    status = PASS if missing_orders == 0 else FAIL
    log_check(run_id, "fk_orders_exist", "raw.order_products_prior", status, missing_orders, Decimal(0), details="prior.order_id missing in orders")
    if status == FAIL:
        failures.append(f"fk_orders_exist failed: missing_order_ids={missing_orders}")

    # ---- Check 5: domain sanity (non-critical but useful) ----
    # reordered should be 0/1 only
    bad_reordered = scalar("""
        SELECT COUNT(*)
        FROM raw.order_products_prior
        WHERE reordered IS NOT NULL AND reordered NOT IN (0, 1);
    """)
    status = PASS if bad_reordered == 0 else FAIL
    log_check(run_id, "domain_reordered_0_1", "raw.order_products_prior", status, bad_reordered, Decimal(0), details="reordered outside {0,1}")
    # Treat as failure but not necessarily critical; keep it as failure message only if you want to fail the run:
    if status == FAIL:
        failures.append(f"domain_reordered_0_1 failed: bad_rows={bad_reordered}")

    all_passed = len(failures) == 0
    return all_passed, failures