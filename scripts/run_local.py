"""Manual local run against the dev dataset.

    python scripts/run_local.py                 # both destinations
    python scripts/run_local.py --refetch-days 90
    python scripts/run_local.py --only motherduck

Reads credentials from .dlt/secrets.toml. Writes to `strava_dev_$USER` schema.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from strava_dlt.pipeline import run_pipelines  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refetch-days", type=int, default=7)
    parser.add_argument(
        "--only",
        choices=["bigquery", "motherduck"],
        help="Run against a single destination instead of both.",
    )
    args = parser.parse_args()

    os.environ.setdefault("STRAVA_DLT_TARGET", "dev")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    destinations = (args.only,) if args.only else ("bigquery", "motherduck")
    run_pipelines(target="dev", refetch_days=args.refetch_days, destinations=destinations)


if __name__ == "__main__":
    main()
