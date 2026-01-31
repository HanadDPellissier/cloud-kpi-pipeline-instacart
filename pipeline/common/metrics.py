from pipeline.common.db import get_conn


def fetch_one_int(sql: str) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return int(cur.fetchone()[0])


def count_rows(table: str) -> int:
    return fetch_one_int(f"SELECT COUNT(*) FROM {table};")