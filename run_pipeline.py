"""Run the whole pipeline with one command:

    python run_pipeline.py                  # ingest -> transform -> publish
    python run_pipeline.py --skip-publish   # local only (no Google Sheets)
    python run_pipeline.py --steps transform publish
"""
import argparse
import logging
import sys
import time

import ingest
import publish
import transform

log = logging.getLogger("pipeline")

STEPS = {"ingest": ingest.main, "transform": transform.main, "publish": publish.main}


def main():
    parser = argparse.ArgumentParser(description="NBA games data pipeline")
    parser.add_argument("--steps", nargs="+", choices=list(STEPS), default=list(STEPS),
                        help="steps to run (default: all, in order)")
    parser.add_argument("--skip-publish", action="store_true",
                        help="run everything except the Google Sheets publish")
    args = parser.parse_args()
    steps = [s for s in STEPS if s in args.steps and not (s == "publish" and args.skip_publish)]

    start = time.time()
    for step in steps:
        log.info("===== %s =====", step.upper())
        t0 = time.time()
        try:
            STEPS[step]()
        except Exception:
            log.exception("Step '%s' failed - pipeline stopped.", step)
            sys.exit(1)
        log.info("%s done in %.1fs", step, time.time() - t0)
    log.info("Pipeline finished (%s) in %.1fs", " -> ".join(steps), time.time() - start)


if __name__ == "__main__":
    main()
