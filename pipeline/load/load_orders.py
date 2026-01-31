# pipeline/load/load_orders.py

from pipeline.common.ddl import ensure_raw_orders_table
from pipeline.load.load_from_s3 import copy_csv_from_s3_to_table


def load_orders(run_date: str) -> int:
    """
    Loads raw/orders for a given run_date from S3 into Postgres raw.orders.
    Returns number of rows loaded.
    """
    ensure_raw_orders_table()

    s3_key = f"raw/orders/load_date={run_date}/orders.csv"

    n = copy_csv_from_s3_to_table(
        s3_key=s3_key,
        truncate_sql="TRUNCATE TABLE raw.orders;",
        copy_sql="""
            COPY raw.orders (
                order_id,
                user_id,
                eval_set,
                order_number,
                order_dow,
                order_hour_of_day,
                days_since_prior_order
            )
            FROM STDIN WITH (FORMAT csv, HEADER true)
        """.strip(),
        count_sql="SELECT COUNT(*) FROM raw.orders;",
    )

    return n