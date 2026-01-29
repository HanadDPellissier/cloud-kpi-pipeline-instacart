import time
from pipeline.run_log import start_run, finish_run
from pipeline.load.load_orders import load_orders_from_s3

s3_key = "raw/orders/load_date=2026-01-27/orders.csv"

t0 = time.monotonic()
run_id = start_run(lookback_days=0)

try:
    n = load_orders_from_s3(s3_key)
    finish_run(
        run_id=run_id,
        status="SUCCESS",
        started_monotonic=t0,
        rows_extracted=n,
        rows_loaded_raw=n,
    )
    print("raw.orders rows:", n, "run_id:", run_id)
except Exception as e:
    finish_run(
        run_id=run_id,
        status="FAILED",
        started_monotonic=t0,
        error_message=str(e),
    )
    raise