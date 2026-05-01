import logging
import os
from datetime import datetime

import dlt

from strava_dlt.source import strava_source

logger = logging.getLogger(__name__)


def run_pipelines(
    since: datetime,
    until: datetime,
    destinations: tuple[str, ...] = ("bigquery", "motherduck"),
) -> dict[str, object]:
    """Run the Strava sync against each destination over `[since, until)`.

    Destination project / dataset / location come from env vars:
      - bigquery: BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_LOCATION (optional)
      - motherduck: MOTHERDUCK_DATABASE, MOTHERDUCK_DATASET

    Each pipeline keeps its own state, so a partial failure (BQ ok,
    MotherDuck failed) recovers cleanly on the next run.
    """
    dataset_for: dict[str, str] = {}
    if "bigquery" in destinations:
        _normalize_bigquery_private_key_env()
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
            "Running %s -> %s.%s (since=%s until=%s)",
            pipeline.pipeline_name,
            destination,
            dataset_name,
            since.isoformat(),
            until.isoformat(),
        )
        results[destination] = pipeline.run(strava_source(since=since, until=until))
    return results


def _normalize_bigquery_private_key_env() -> None:
    # Modal Secrets entered via the dashboard often store the SA private key
    # with literal `\n` sequences instead of real newlines, which makes
    # `cryptography` reject the PEM at byte 0. Same fix as aej-dlt.
    # TODO: revisit holistically — sibling repos keep duplicating this; the
    # cleaner long-term shape is a single `DESTINATION__BIGQUERY__CREDENTIALS`
    # env holding the SA JSON verbatim, which dlt's JSON loader unescapes.
    key = "DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"
    private_key = os.environ.get(key)
    if private_key and "\\n" in private_key:
        os.environ[key] = private_key.replace("\\n", "\n")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value
