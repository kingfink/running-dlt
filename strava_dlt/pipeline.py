import logging

import dlt

from strava_dlt.config import Target, resolve_dataset_name
from strava_dlt.source import DEFAULT_REFETCH_DAYS, strava_source

logger = logging.getLogger(__name__)


def run_pipelines(
    target: Target,
    refetch_days: int = DEFAULT_REFETCH_DAYS,
    destinations: tuple[str, ...] = ("bigquery", "motherduck"),
) -> dict[str, object]:
    """Run the Strava sync against each destination, sequentially.

    The source is invoked once per destination — each pipeline keeps its own
    state, so a partial failure (BQ ok, MotherDuck failed) recovers cleanly
    on the next run. The duplicated extract is intentional and cheap; for a
    single athlete it's well inside Strava's 1000-call/day budget.
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
            "Running pipeline %s -> %s.%s (refetch_days=%d)",
            pipeline.pipeline_name,
            destination,
            dataset_name,
            refetch_days,
        )
        info = pipeline.run(strava_source(refetch_days=refetch_days))
        logger.info("Done %s: %s", pipeline.pipeline_name, info)
        results[destination] = info

    return results
