import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from ingest.db import get_conn
from config.settings import PIPELINE_NAME

def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def start_run(lookback_days: int = 0) -> uuid.UUID:
    run_id = uuid.uuid4()
    started_at = _utcnow()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.pipeline_runs
                (run_id, pipeline_name, started_at, status, lookback_days,
                 rows_extracted, rows_loaded_raw, rows_loaded_staging, rows_loaded_marts,
                 execution_seconds)
                VALUES (%s, %s, %s, %s, %s,
                        0, 0, 0, 0,
                        NULL)
                """,
                (run_id, PIPELINE_NAME, started_at, "RUNNING", lookback_days),
            )
    return run_id

def finish_run(
    run_id: uuid.UUID,
    status: str,
    started_monotonic: float,
    error_message: Optional[str] = None,
    rows_extracted: int = 0,
    rows_loaded_raw: int = 0,
    rows_loaded_staging: int = 0,
    rows_loaded_marts: int = 0,
):
    ended_at = _utcnow()
    exec_seconds = round(time.monotonic() - started_monotonic, 3)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ops.pipeline_runs
                   SET ended_at = %s,
                       status = %s,
                       error_message = %s,
                       rows_extracted = %s,
                       rows_loaded_raw = %s,
                       rows_loaded_staging = %s,
                       rows_loaded_marts = %s,
                       execution_seconds = %s
                 WHERE run_id = %s
                """,
                (
                    ended_at,
                    status,
                    error_message,
                    rows_extracted,
                    rows_loaded_raw,
                    rows_loaded_staging,
                    rows_loaded_marts,
                    exec_seconds,
                    run_id,
                ),
            )