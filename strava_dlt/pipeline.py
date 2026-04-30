import logging

import dlt

from strava_dlt.config import Target, resolve_dataset_name
from strava_dlt.source import DEFAULT_LOOKBACK_DAYS, strava_source

logger = logging.getLogger(__name__)


def run_pipelines(
    target: Target,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    destinations: tuple[str, ...] = ("bigquery", "motherduck"),
) -> dict[str, object]:
    """Run the Strava sync against each destination, sequentially.

    The source is invoked once per destination — each pipeline keeps its own
    state, so a partial failure (BQ ok, MotherDuck failed) recovers cleanly
    on the next run.
    """
    dataset_name = resolve_dataset_name(target)
    results: dict[str, object] = {}
    for destination in destinations:
        pipeline = dlt.pipeline(
            pipeline_name=f"strava_{destination}_{target}",
            destination=destination,
            dataset_name=dataset_name,
        )
        logger.info(
            "Running %s -> %s.%s (lookback_days=%d)",
            pipeline.pipeline_name, destination, dataset_name, lookback_days,
        )
        results[destination] = pipeline.run(strava_source(lookback_days=lookback_days))
    return results
