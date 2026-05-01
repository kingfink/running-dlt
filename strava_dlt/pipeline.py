import logging
import os

import dlt

from strava_dlt.source import DEFAULT_LOOKBACK_DAYS, strava_source

logger = logging.getLogger(__name__)


def run_pipelines(
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    destinations: tuple[str, ...] = ("bigquery", "motherduck"),
) -> dict[str, object]:
    """Run the Strava sync against each destination, sequentially.

    Destination project / dataset / location come from env vars:
      - bigquery: BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_LOCATION (optional)
      - motherduck: MOTHERDUCK_DATABASE, MOTHERDUCK_DATASET

    Each pipeline keeps its own state, so a partial failure (BQ ok,
    MotherDuck failed) recovers cleanly on the next run.
    """
    dataset_for: dict[str, str] = {}
    if "bigquery" in destinations:
        dlt.config["destination.bigquery.project_id"] = _required_env("BIGQUERY_PROJECT")
        if location := os.environ.get("BIGQUERY_LOCATION"):
            dlt.config["destination.bigquery.location"] = location
        dataset_for["bigquery"] = _required_env("BIGQUERY_DATASET")
    if "motherduck" in destinations:
        dlt.config["destination.motherduck.database"] = _required_env("MOTHERDUCK_DATABASE")
        dataset_for["motherduck"] = _required_env("MOTHERDUCK_DATASET")

    results: dict[str, object] = {}
    for destination in destinations:
        dataset_name = dataset_for[destination]
        pipeline = dlt.pipeline(
            pipeline_name=f"strava_{destination}",
            destination=destination,
            dataset_name=dataset_name,
        )
        logger.info(
            "Running %s -> %s.%s (lookback_days=%d)",
            pipeline.pipeline_name, destination, dataset_name, lookback_days,
        )
        results[destination] = pipeline.run(strava_source(lookback_days=lookback_days))
    return results


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value
