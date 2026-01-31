# jobs/load_dims.py

import time

from config.settings import RUN_DATE
from pipeline.common.run_log import start_run, finish_run
from pipeline.load.load_dims import load_dims


def main():
    t0 = time.monotonic()
    run_id = start_run(lookback_days=0)

    try:
        rows_total = load_dims(RUN_DATE)

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
            rows_extracted=rows_total,
            rows_loaded_raw=rows_total,
        )
        print("dims loaded. total_rows:", rows_total, "run_id:", run_id)

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