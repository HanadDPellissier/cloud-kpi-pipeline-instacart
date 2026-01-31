import time
import uuid

from pipeline.common.run_log import start_run, finish_run

from jobs.load_dims import main as load_dims_main
from jobs.run_orders import main as run_orders_main
from jobs.run_prior import main as run_prior_main
from jobs.run_dq import main as run_dq_main
from jobs.run_staging import main as run_staging_main
from jobs.run_marts import main as run_marts_main
from jobs.run_kpis import main as run_kpis_main


def _step(name: str, fn):
    print(f"\n=== STEP: {name} ===")
    t0 = time.monotonic()
    fn()
    print(f"=== DONE: {name} ({round(time.monotonic() - t0, 2)}s) ===")


def main():
    """
    Master orchestrator:
    - Runs the pipeline in correct order
    - Enforces DQ gating (abort if DQ fails)
    - Logs a single master run in ops.pipeline_runs
    """
    master_started = time.monotonic()
    master_run_id = start_run(lookback_days=0)

    try:
        _step("load_dims", load_dims_main)
        _step("load_orders", run_orders_main)
        _step("load_prior", run_prior_main)

        # DQ gate: if this raises, we stop the pipeline.
        _step("data_quality", run_dq_main)

        _step("staging", run_staging_main)
        _step("marts", run_marts_main)
        _step("kpis", run_kpis_main)

        finish_run(
            run_id=master_run_id,
            status="SUCCESS",
            started_monotonic=master_started,
        )
        print("\nPIPELINE SUCCESS. master_run_id:", master_run_id)

    except Exception as e:
        finish_run(
            run_id=master_run_id,
            status="FAILED",
            started_monotonic=master_started,
            error_message=str(e)[:4000],
        )
        print("\nPIPELINE FAILED. master_run_id:", master_run_id)
        raise


if __name__ == "__main__":
    main()