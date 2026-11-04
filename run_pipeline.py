"""Run the whole pipeline with one command:  python run_pipeline.py

    ingest     Kaggle -> ./data -> PostgreSQL raw_data   (+ validation, reconciliation)
    transform  raw_data -> pandas -> PostgreSQL production (+ reconciliation)
    publish    production -> Google Sheets (-> Data Studio dashboard)

Options:
    --steps ingest transform     run only some steps
    --force-download             re-download the Kaggle files
    --skip-publish               stop after transform (no Google credentials needed)
    --dry-run-publish            size-check the publish step without writing

Every run is recorded in audit.pipeline_runs.
"""
import argparse
import sys
import time
from datetime import datetime, timezone

import pandas as pd

import config
import ingest
import publish
import transform

log = config.get_logger("pipeline")
STEPS = ["ingest", "transform", "publish"]


def record_run(started, finished, steps, status, message):
    try:
        pd.DataFrame([dict(started_at=started, finished_at=finished, steps=",".join(steps),
                           status=status, seconds=round((finished - started).total_seconds(), 1),
                           message=message[:500])]).to_sql(
            "pipeline_runs", config.get_engine(), schema=config.AUDIT_SCHEMA, if_exists="append", index=False)
    except Exception as exc:  # noqa: BLE001  (the database itself may be the problem)
        log.warning("Could not write audit.pipeline_runs: %s", exc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--steps", nargs="+", choices=STEPS, default=STEPS)
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--skip-publish", action="store_true")
    parser.add_argument("--dry-run-publish", action="store_true")
    args = parser.parse_args()

    steps = [s for s in STEPS if s in args.steps and not (s == "publish" and args.skip_publish)]
    runners = {
        "ingest": lambda: ingest.run(force_download=args.force_download),
        "transform": transform.run,
        "publish": lambda: publish.run(dry_run=args.dry_run_publish),
    }

    started = datetime.now(timezone.utc)
    log.info("=" * 70)
    log.info("Pipeline started: %s", " -> ".join(steps))
    for i, step in enumerate(steps, 1):
        t0 = time.perf_counter()
        log.info("[%d/%d] %s ...", i, len(steps), step.upper())
        try:
            runners[step]()
        except Exception as exc:  # noqa: BLE001
            log.exception("[%d/%d] %s FAILED: %s", i, len(steps), step.upper(), exc)
            log.error("Pipeline stopped. Later steps were not run so stale data is not published.")
            record_run(started, datetime.now(timezone.utc), steps, "failed", f"{step}: {exc}")
            return 1
        log.info("[%d/%d] %s done in %.1fs", i, len(steps), step.upper(), time.perf_counter() - t0)

    finished = datetime.now(timezone.utc)
    record_run(started, finished, steps, "success", "ok")
    log.info("Pipeline finished successfully in %.1fs", (finished - started).total_seconds())
    return 0


if __name__ == "__main__":
    sys.exit(main())
