"""Modal entrypoint for the Strava → BigQuery + MotherDuck dlt pipelines.

Schedule:
    sync_strava_scheduled — daily 06:00 UTC, target=prod, refetch_days=7

Ad-hoc:
    modal run modal_app.py::sync_strava_manual --refetch-days 90
        # widen refetch window for an out-of-band re-pull (the "escape hatch")
"""

import modal

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install_from_pyproject("pyproject.toml")
    .add_local_python_source("strava_dlt")
)

app = modal.App("strava-dlt")

SECRETS = [
    # STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN
    # dlt picks these up via SOURCES__STRAVA__CLIENT_ID etc.; we set both
    # naming styles in the Modal Secret for clarity (see docs/modal-deploy.md).
    modal.Secret.from_name("strava-oauth"),
    # DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID/__PRIVATE_KEY/__CLIENT_EMAIL
    modal.Secret.from_name("dlt-bigquery"),
    # DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD = <motherduck token>
    modal.Secret.from_name("dlt-motherduck"),
]

ENV = {"STRAVA_DLT_TARGET": "prod"}


@app.function(
    image=image,
    secrets=SECRETS,
    schedule=modal.Cron("0 6 * * *"),
    timeout=60 * 30,
)
def sync_strava_scheduled() -> None:
    import os

    os.environ.update(ENV)
    from strava_dlt.pipeline import run_pipelines

    run_pipelines(target="prod")


@app.function(
    image=image,
    secrets=SECRETS,
    timeout=60 * 60,
)
def sync_strava_manual(refetch_days: int = 7) -> None:
    """Out-of-band run. Widen `refetch_days` (e.g., 99999) to force a full backfill."""
    import os

    os.environ.update(ENV)
    from strava_dlt.pipeline import run_pipelines

    run_pipelines(target="prod", refetch_days=refetch_days)
