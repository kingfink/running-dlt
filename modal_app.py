"""Strava → BigQuery + MotherDuck dlt pipelines.

Cron + Modal:
    modal deploy modal_app.py                              # registers the daily cron
    modal run modal_app.py::sync                           # ad-hoc prod run on Modal
    modal run modal_app.py::sync --lookback-days 99999     # full backfill / escape hatch

Local dev (against `strava_dev_$USER`, reading .dlt/secrets.toml):
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

SECRETS = [
    modal.Secret.from_name("strava-oauth"),     # SOURCES__STRAVA__CLIENT_ID/SECRET/REFRESH_TOKEN
    modal.Secret.from_name("dlt-bigquery"),     # DESTINATION__BIGQUERY__CREDENTIALS__*
    modal.Secret.from_name("dlt-motherduck"),   # DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD
]


@app.function(
    image=image,
    secrets=SECRETS,
    schedule=modal.Cron("0 6 * * *"),
    timeout=60 * 60,
)
def sync(lookback_days: int = 7) -> None:
    """Run both pipelines against prod. Cron uses the default; pass a wider
    `--lookback-days` from `modal run` for backfills or to recover edits."""
    import os

    os.environ["STRAVA_DLT_TARGET"] = "prod"
    from strava_dlt.pipeline import run_pipelines

    run_pipelines(target="prod", lookback_days=lookback_days)


if __name__ == "__main__":
    import argparse
    import logging
    import os

    parser = argparse.ArgumentParser(description="Local dev run against `strava_dev_$USER`.")
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument(
        "--destinations",
        default="bigquery,motherduck",
        help="Comma-separated list of destinations to write to.",
    )
    args = parser.parse_args()

    os.environ.setdefault("STRAVA_DLT_TARGET", "dev")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from strava_dlt.pipeline import run_pipelines

    destinations = tuple(d.strip() for d in args.destinations.split(",") if d.strip())
    run_pipelines(target="dev", lookback_days=args.lookback_days, destinations=destinations)
