# jobs/run_orders.py

import time

from config.settings import RUN_DATE
from pipeline.common.run_log import start_run, finish_run
from pipeline.load.load_orders import load_orders


def main():
    t0 = time.monotonic()
    run_id = start_run(lookback_days=0)

    try:
        n = load_orders("2026-01-27")

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
            rows_extracted=n,
            rows_loaded_raw=n,
        )

        print("orders loaded:", n, "run_id:", run_id)

    except Exception as e:
        finish_run(
            run_id=run_id,
            status="FAILED",
            started_monotonic=t0,
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    main()