"""Modal entrypoint for the Strava → BigQuery + MotherDuck dlt pipelines.

    modal deploy modal_app.py                              # registers the daily cron
    modal run modal_app.py::sync                           # ad-hoc run with default 7-day window
    modal run modal_app.py::sync --lookback-days 99999     # full backfill / escape hatch
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
