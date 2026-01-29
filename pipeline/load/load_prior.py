import boto3

from config.settings import AWS_REGION, S3_BUCKET
from pipeline.ddl import ensure_raw_order_products_prior_table
from pipeline.db import get_conn

def load_prior_from_s3(s3_key: str) -> int:
    """
    Stream CSV directly from S3 into Postgres using psycopg3 COPY.
    Returns row count after load.
    """
    ensure_raw_order_products_prior_table()

    s3 = boto3.client("s3", region_name=AWS_REGION)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    body = obj["Body"]  # StreamingBody

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Rerun-safe for now
            cur.execute("TRUNCATE TABLE raw.order_products_prior;")

            copy_sql = """
            COPY raw.order_products_prior
              (order_id, product_id, add_to_cart_order, reordered)
            FROM STDIN WITH (FORMAT csv, HEADER true)
            """

            with cur.copy(copy_sql) as copy:
                while True:
                    chunk = body.read(8 * 1024 * 1024)  # 8MB chunks
                    if not chunk:
                        break
                    copy.write(chunk)

            cur.execute("SELECT COUNT(*) FROM raw.order_products_prior;")
            (n,) = cur.fetchone()
            return int(n)