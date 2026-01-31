import time
from pipeline.common.run_log import start_run, finish_run

t0 = time.monotonic()
run_id = start_run(lookback_days=0)

# pretend we did work
time.sleep(1)

finish_run(
    run_id=run_id,
    status="SUCCESS",
    started_monotonic=t0,
    rows_extracted=123,
    rows_loaded_raw=123,
)
print("logged:", run_id)