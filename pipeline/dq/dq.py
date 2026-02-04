from __future__ import annotations

from decimal import Decimal
from typing import Optional, Tuple, List

from pipeline.common.db import get_conn

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
) -> None:
    # Write a single data quality result to the ops table so it can be reviewed later
    # This keeps checks auditable and tied back to a specific pipeline run
    with get_conn() as conn, conn.cursor() as cur:
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
    # Helper for queries that should return exactly one numeric value
    # Normalizes None results to zero so downstream checks stay simple
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row or row[0] is None:
            return Decimal(0)
        return Decimal(str(row[0]))


def run_checks(run_id) -> Tuple[bool, List[str]]:
    # Runs all data quality checks and returns whether the run is clean
    # Also returns a list of human-readable failure messages
    failures: List[str] = []

    # Checks to make sure ingestion didn’t silently truncate or skip data
    row_count_checks = [
        ("raw.orders", "SELECT COUNT(*) FROM raw.orders;", Decimal("1000000")),
        (
            "raw.order_products_prior",
            "SELECT COUNT(*) FROM raw.order_products_prior;",
            Decimal("10000000"),
        ),
        ("raw.products", "SELECT COUNT(*) FROM raw.products;", Decimal("10000")),
    ]

    for table_name, sql, threshold in row_count_checks:
        val = scalar(sql)
        status = PASS if val >= threshold else FAIL
        log_check(run_id, "row_count_min", table_name, status, val, threshold)
        if status == FAIL:
            failures.append(f"{table_name} row count below expected minimum: {val}")

    # Core identifiers should never be null if the data is usable
    not_null_checks = [
        (
            "raw.orders",
            "SELECT COUNT(*) FROM raw.orders WHERE order_id IS NULL OR user_id IS NULL;",
            "order_id or user_id is NULL",
        ),
        (
            "raw.products",
            """
            SELECT COUNT(*)
            FROM raw.products
            WHERE product_id IS NULL
               OR aisle_id IS NULL
               OR department_id IS NULL;
            """,
            "product_id, aisle_id, or department_id is NULL",
        ),
        (
            "raw.order_products_prior",
            """
            SELECT COUNT(*)
            FROM raw.order_products_prior
            WHERE order_id IS NULL OR product_id IS NULL;
            """,
            "order_id or product_id is NULL",
        ),
    ]

    for table_name, sql, detail in not_null_checks:
        val = scalar(sql)
        status = PASS if val == 0 else FAIL
        log_check(
            run_id,
            "not_null_key",
            table_name,
            status,
            val,
            Decimal(0),
            details=detail,
        )
        if status == FAIL:
            failures.append(f"{table_name} contains null keys: {val}")

    # Duplicate primary keys usually point to replay bugs or bad upstream data
    dup_orders = scalar(
        """
        SELECT COUNT(*) FROM (
          SELECT order_id
          FROM raw.orders
          GROUP BY order_id
          HAVING COUNT(*) > 1
        ) d;
        """
    )
    status = PASS if dup_orders == 0 else FAIL
    log_check(run_id, "duplicate_key", "raw.orders", status, dup_orders, Decimal(0))
    if status == FAIL:
        failures.append(f"raw.orders has duplicate order_id groups: {dup_orders}")

    dup_prior = scalar(
        """
        SELECT COUNT(*) FROM (
          SELECT order_id, product_id
          FROM raw.order_products_prior
          GROUP BY order_id, product_id
          HAVING COUNT(*) > 1
        ) d;
        """
    )
    status = PASS if dup_prior == 0 else FAIL
    log_check(
        run_id,
        "duplicate_key",
        "raw.order_products_prior",
        status,
        dup_prior,
        Decimal(0),
    )
    if status == FAIL:
        failures.append(
            f"raw.order_products_prior has duplicate (order_id, product_id) groups: {dup_prior}"
        )

    # Every order_products row should link back to a real order
    missing_orders = scalar(
        """
        SELECT COUNT(*)
        FROM raw.order_products_prior p
        LEFT JOIN raw.orders o ON o.order_id = p.order_id
        WHERE o.order_id IS NULL;
        """
    )
    status = PASS if missing_orders == 0 else FAIL
    log_check(
        run_id,
        "fk_orders_exist",
        "raw.order_products_prior",
        status,
        missing_orders,
        Decimal(0),
        details="order_id not found in raw.orders",
    )
    if status == FAIL:
        failures.append(f"order_products_prior rows missing orders: {missing_orders}")

    # reordered is expected to be a binary flag and nothing else
    bad_reordered = scalar(
        """
        SELECT COUNT(*)
        FROM raw.order_products_prior
        WHERE reordered IS NOT NULL
          AND reordered NOT IN (0, 1);
        """
    )
    status = PASS if bad_reordered == 0 else FAIL
    log_check(
        run_id,
        "domain_reordered_0_1",
        "raw.order_products_prior",
        status,
        bad_reordered,
        Decimal(0),
        details="reordered value outside expected range",
    )
    if status == FAIL:
        failures.append(f"invalid reordered values found: {bad_reordered}")

    # If anything failed above, the pipeline should treat this run as unhealthy
    return (len(failures) == 0), failures