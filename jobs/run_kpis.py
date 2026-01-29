import time

from pipeline.run_log import start_run, finish_run
from pipeline.sql.sql_runner import run_sql_file

KPI_FILES = [
    "sql/kpis/kpi_orders_by_dow.sql",
    "sql/kpis/kpi_products_reorder.sql",
]

def main():
    t0 = time.monotonic()
    run_id = start_run(lookback_days=0)

    try:
        for f in KPI_FILES:
            run_sql_file(f)

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
        )

        print("kpis built successfully. run_id:", run_id)

    except Exception as e:
        finish_run(
            run_id=run_id,
            status="FAILED",
            started_monotonic=t0,
            error_message=str(e)[:4000],
        )
        raise

if __name__ == "__main__":
    main()