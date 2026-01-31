from pathlib import Path
from pipeline.common.db import get_conn

def run_sql_file(path: str):
    sql = Path(path).read_text(encoding="utf-8")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)