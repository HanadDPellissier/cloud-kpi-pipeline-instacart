import time

from pipeline.common.run_log import start_run, finish_run
from pipeline.load.load_prior import load_prior_from_s3
from config.settings import RUN_DATE

def main():
    s3_key = "raw/order_products_prior/load_date=2026-01-27/order_products__prior.csv"

    t0 = time.monotonic()
    run_id = start_run(lookback_days=0)

    try:
        n = load_prior_from_s3(s3_key)

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
            rows_extracted=n,
            rows_loaded_raw=n,
        )

        print("prior loaded:", n, "run_id:", run_id)

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