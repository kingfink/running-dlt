"""Strava → BigQuery + MotherDuck dlt pipelines.

Cron + Modal:
    modal deploy modal_app.py                              # registers the daily cron
    modal run modal_app.py::sync                           # ad-hoc run on Modal
    modal run modal_app.py::sync --lookback-days 99999     # full backfill / escape hatch

Local dev (reads .dlt/secrets.toml + env vars for project/dataset/location):
    python modal_app.py
    python modal_app.py --lookback-days 90
    python modal_app.py --destinations motherduck
"""

import modal

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install_from_pyproject("pyproject.toml")
    .add_local_python_source("strava_dlt")
)

app = modal.App("strava-dlt")

# Each Modal Secret carries the destination's project/dataset/location env vars
# *and* the dlt credential blob. See README for the full list.
SECRETS = [
    modal.Secret.from_name("strava-oauth"),
    modal.Secret.from_name("running-dlt-bq-sync"),
    modal.Secret.from_name("running-dlt-md-sync"),
]


@app.function(
    image=image,
    secrets=SECRETS,
    schedule=modal.Cron("0 6 * * *"),
    timeout=60 * 60,
)
def sync(lookback_days: int = 7) -> None:
    """Run both pipelines. Cron uses the default; pass a wider
    `--lookback-days` from `modal run` for backfills or to recover edits."""
    from strava_dlt.pipeline import run_pipelines

    run_pipelines(lookback_days=lookback_days)


if __name__ == "__main__":
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Local dev run.")
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument(
        "--destinations",
        default="bigquery,motherduck",
        help="Comma-separated list of destinations to write to.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from strava_dlt.pipeline import run_pipelines

    destinations = tuple(d.strip() for d in args.destinations.split(",") if d.strip())
    run_pipelines(lookback_days=args.lookback_days, destinations=destinations)
