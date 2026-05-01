"""Strava → BigQuery + MotherDuck dlt pipelines.

Cron + Modal:
    modal deploy modal_app.py                                    # registers daily cron
    modal run modal_app.py::sync                                 # last 7 days, both destinations
    modal run modal_app.py::sync --since=2019-01-01 \\
        --until=2020-01-01                                       # explicit backfill window
    modal run modal_app.py::sync --reset-seen=1                  # force refetch (ignore seen_ids)

Local dev (reads .dlt/secrets.toml + env vars for project/dataset/location):
    python modal_app.py
    python modal_app.py --since 2024-01-01 --until 2024-02-01
    python modal_app.py --destinations motherduck
    python modal_app.py --reset-seen                             # force refetch

Knobs:
    STRAVA_REFETCH_DAYS  Activities started within this many days of `until` are
                         always refetched (default 7). Older ones are skipped if
                         their id is already in pipeline state's `seen_ids`.
"""

from datetime import UTC, datetime, timedelta

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


def _resolve_window(since: str | None, until: str | None) -> tuple[datetime, datetime]:
    """Default window: last 7 days through now (UTC). ISO date YYYY-MM-DD
    on either bound is interpreted as UTC midnight."""
    now = datetime.now(UTC)
    until_dt = datetime.fromisoformat(until).replace(tzinfo=UTC) if until else now
    since_dt = (
        datetime.fromisoformat(since).replace(tzinfo=UTC) if since else until_dt - timedelta(days=7)
    )
    return since_dt, until_dt


@app.function(
    image=image,
    secrets=SECRETS,
    schedule=modal.Cron("0 6 * * *"),
    timeout=60 * 60,
)
def sync(since: str = "", until: str = "", reset_seen: str = "") -> None:
    """Run both pipelines over `[since, until)`. Cron uses the defaults
    (last 7 days from now). For backfill:
        modal run modal_app.py::sync --since=YYYY-MM-DD --until=YYYY-MM-DD
    Year-at-a-time chunks fit comfortably under Strava's default 1000-call
    daily quota for a single athlete.

    `since`/`until`/`reset_seen` are typed `str = ""` rather than richer types
    so Modal's web "Try It" UI renders them as text inputs (the UI doesn't
    introspect PEP 604 unions or bools cleanly). Empty string means "use the
    default"; any non-empty `reset_seen` clears `seen_ids` before running.
    """
    from strava_dlt.pipeline import run_pipelines

    since_dt, until_dt = _resolve_window(since or None, until or None)
    run_pipelines(since=since_dt, until=until_dt, reset_seen=bool(reset_seen))


if __name__ == "__main__":
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Local dev run.")
    parser.add_argument("--since", help="ISO date YYYY-MM-DD; default: 7 days before --until.")
    parser.add_argument("--until", help="ISO date YYYY-MM-DD; default: now (UTC).")
    parser.add_argument(
        "--destinations",
        default="bigquery,motherduck",
        help="Comma-separated list of destinations to write to.",
    )
    parser.add_argument(
        "--reset-seen",
        action="store_true",
        help="Clear seen_ids before running so every activity in the window is refetched.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from strava_dlt.pipeline import run_pipelines

    since_dt, until_dt = _resolve_window(args.since, args.until)
    destinations = tuple(d.strip() for d in args.destinations.split(",") if d.strip())
    run_pipelines(
        since=since_dt,
        until=until_dt,
        destinations=destinations,
        reset_seen=args.reset_seen,
    )
