import time

from pipeline.run_log import start_run, finish_run
from pipeline.dq.dq import run_checks

def main():
    t0 = time.monotonic()
    run_id = start_run(lookback_days=0)

    try:
        ok, failures = run_checks(run_id)

        if not ok:
            msg = " | ".join(failures)[:4000]  # keep error_message reasonable
            finish_run(
                run_id=run_id,
                status="FAILED",
                started_monotonic=t0,
                error_message=msg,
            )
            raise RuntimeError(msg)

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
        )

        print("dq: PASS", "run_id:", run_id)

    except Exception as e:
        # If we got here without logging failed, ensure it’s logged
        try:
            finish_run(
                run_id=run_id,
                status="FAILED",
                started_monotonic=t0,
                error_message=str(e)[:4000],
            )
        except Exception:
            pass
        raise

if __name__ == "__main__":
    main()