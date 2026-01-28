import os
import tempfile
import boto3

from config.settings import AWS_REGION, S3_BUCKET
from ingest.db import get_conn
from ingest.ddl import ensure_raw_orders_table

def load_orders_from_s3(s3_key: str) -> int:
    """
    Downloads CSV from S3 to a temp file, then psycopg3 COPY loads into raw.orders.
    Idempotent for today: TRUNCATE + COPY (safe for reruns).
    Returns: rows loaded (COUNT(*) after load).
    """
    ensure_raw_orders_table()

    s3 = boto3.client("s3", region_name=AWS_REGION)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_path = tmp.name

    try:
        s3.download_file(S3_BUCKET, s3_key, tmp_path)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE raw.orders;")

                copy_sql = """
                COPY raw.orders (order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order)
                FROM STDIN WITH (FORMAT csv, HEADER true)
                """

                # psycopg3 COPY: use context manager and stream bytes
                with cur.copy(copy_sql) as copy:
                    with open(tmp_path, "rb") as f:
                        while True:
                            chunk = f.read(1024 * 1024)  # 1MB chunks
                            if not chunk:
                                break
                            copy.write(chunk)

                cur.execute("SELECT COUNT(*) FROM raw.orders;")
                (n,) = cur.fetchone()
                return int(n)

    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass