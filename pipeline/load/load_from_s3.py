import os
import tempfile
import boto3

from config.settings import AWS_REGION, S3_BUCKET
from pipeline.db import get_conn

def copy_csv_from_s3_to_table(
    s3_key: str,
    truncate_sql: str,
    copy_sql: str,
    count_sql: str,
) -> int:
    """
    Downloads CSV from S3 to temp file, then COPY loads into Postgres.
    Returns row count from count_sql.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_path = tmp.name

    try:
        s3.download_file(S3_BUCKET, s3_key, tmp_path)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(truncate_sql)

                with cur.copy(copy_sql) as copy:
                    with open(tmp_path, "rb") as f:
                        while True:
                            chunk = f.read(1024 * 1024)
                            if not chunk:
                                break
                            copy.write(chunk)

                cur.execute(count_sql)
                (n,) = cur.fetchone()
                return int(n)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass