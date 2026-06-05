import logging
import os
from datetime import UTC, datetime
from typing import Any

import dlt
from dlt.common.pipeline import TPipelineState
from tailor_made_dlt_sources.strava import strava_source

logger = logging.getLogger(__name__)


class StravaRateLimitedRun(RuntimeError):
    """One or more destinations finished early because Strava 429-retries
    were exhausted. The rows extracted before the limit were loaded — this
    raise exists so cron / Modal flag the run as failed."""


def run_pipelines(
    since: datetime,
    until: datetime,
    destinations: tuple[str, ...] = ("bigquery", "motherduck"),
    reset_seen: bool = False,
) -> dict[str, object]:
    """Run the Strava sync against each destination over `[since, until)`.

    Destination project / dataset / location come from env vars:
      - bigquery: BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_LOCATION (optional)
      - motherduck: MOTHERDUCK_DATABASE, MOTHERDUCK_DATASET

    Each pipeline keeps its own state, so a partial failure (BQ ok,
    MotherDuck failed) recovers cleanly on the next run. If any destination
    rate-limited mid-run, all destinations still attempt their run, then
    `StravaRateLimitedRun` is raised at the end with the affected paths.

    `reset_seen=True` clears each destination's `seen_ids` before running so
    every activity in the window is refetched.
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

    # Shared per-run cache: the first destination's source populates it from
    # `/activities/{id}` responses, the second reads them and skips the GET.
    # Halves Strava API call count on backfills (the dominant rate-limit cost)
    # without reintroducing the filesystem-staging layer rejected in the
    # original design.
    detail_cache: dict[int, dict] = {}

    results: dict[str, object] = {}
    rate_limited: dict[str, str] = {}
    for destination in destinations:
        dataset_name = dataset_for[destination]
        pipeline = dlt.pipeline(
            pipeline_name=f"strava_{destination}",
            destination=destination,
            dataset_name=dataset_name,
        )
        logger.info(
            "Running %s -> %s.%s (since=%s until=%s reset_seen=%s detail_cache_size=%s)",
            pipeline.pipeline_name,
            destination,
            dataset_name,
            since.isoformat(),
            until.isoformat(),
            reset_seen,
            len(detail_cache),
        )
        info, status = run_strava_pipeline(
            pipeline,
            since=since,
            until=until,
            reset_seen=reset_seen,
            source_kwargs={"detail_cache": detail_cache},
        )
        results[destination] = info
        if path := status.get("rate_limited_path"):
            rate_limited[destination] = path

    if rate_limited:
        raise StravaRateLimitedRun(
            f"Rate-limited mid-run; partial data committed. Destinations affected: {rate_limited}"
        )
    return results


def run_strava_pipeline(
    pipeline: dlt.Pipeline,
    *,
    since: datetime,
    until: datetime,
    reset_seen: bool = False,
    source_kwargs: dict[str, Any] | None = None,
) -> tuple[object, dict[str, Any]]:
    """Run one Strava pipeline and persist `seen_ids` only after data loads.

    The source reads existing `seen_ids` so it can skip old details, but it only
    reports ids that were extracted in this process. Once dlt reports that the
    data load finished without failed jobs, we write those ids to pipeline state
    in a second state-only load package. That keeps the state write after the
    data persistence boundary.
    """
    status: dict[str, Any] = {}
    source_kwargs = dict(source_kwargs or {})

    logger.info(
        "Starting Strava data load pipeline=%s since=%s until=%s reset_seen=%s local_seen_count=%s",
        pipeline.pipeline_name,
        since.isoformat(),
        until.isoformat(),
        reset_seen,
        len(_seen_ids_from_state(getattr(pipeline, "state", {}))),
    )
    info = pipeline.run(
        strava_source(
            since=since,
            until=until,
            status=status,
            reset_seen=reset_seen,
            **source_kwargs,
        )
    )

    extracted_ids = _unique_ints(status.get("extracted_activity_ids", []))
    loaded_ids = _unique_ints(status.get("loaded_activity_ids", extracted_ids))
    status["loaded_activity_ids"] = loaded_ids

    logger.info(
        "Finished Strava data load pipeline=%s has_failed_jobs=%s load_ids=%s "
        "extracted_activity_count=%s skipped_activity_count=%s "
        "cache_hit_activity_count=%s rate_limited_path=%s",
        pipeline.pipeline_name,
        getattr(info, "has_failed_jobs", None),
        getattr(info, "loads_ids", None),
        len(extracted_ids),
        len(status.get("skipped_activity_ids", [])),
        len(status.get("cache_hit_activity_ids", [])),
        status.get("rate_limited_path"),
    )

    if getattr(info, "has_failed_jobs", False):
        logger.warning(
            "Not persisting Strava state because data load reported failed jobs "
            "pipeline=%s load_ids=%s extracted_activity_count=%s",
            pipeline.pipeline_name,
            getattr(info, "loads_ids", None),
            len(extracted_ids),
        )
        return info, status

    _persist_seen_ids_after_load(
        pipeline,
        activity_ids=loaded_ids,
        since=since,
        until=until,
        reset_seen=reset_seen,
    )
    return info, status


def _persist_seen_ids_after_load(
    pipeline: dlt.Pipeline,
    *,
    activity_ids: list[int],
    since: datetime,
    until: datetime,
    reset_seen: bool,
) -> object:
    logger.info(
        "Persisting Strava state after successful data load pipeline=%s since=%s "
        "until=%s loaded_activity_count=%s reset_seen=%s",
        pipeline.pipeline_name,
        since.isoformat(),
        until.isoformat(),
        len(activity_ids),
        reset_seen,
    )

    before_count = 0
    after_count = 0
    added_count = 0
    with pipeline.managed_state(extract_state=True) as state:
        resource_state = _strava_activities_resource_state(state)
        before_seen = _unique_ints(resource_state.get("seen_ids", []))
        before_count = len(before_seen)

        if reset_seen:
            merged_seen = []
            seen_set: set[int] = set()
        else:
            merged_seen = list(before_seen)
            seen_set = set(before_seen)

        for activity_id in activity_ids:
            if activity_id not in seen_set:
                merged_seen.append(activity_id)
                seen_set.add(activity_id)

        resource_state["seen_ids"] = merged_seen
        resource_state["last_successful_window"] = {
            "since": since.isoformat(),
            "until": until.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "loaded_activity_count": len(activity_ids),
            "reset_seen": reset_seen,
        }
        after_count = len(merged_seen)
        added_count = max(after_count - before_count, 0)

    pipeline.normalize()
    state_info = pipeline.load()
    if getattr(state_info, "has_failed_jobs", False):
        raise RuntimeError(
            "Failed to persist Strava state after successful data load "
            f"pipeline={pipeline.pipeline_name} load_ids={getattr(state_info, 'loads_ids', None)}"
        )

    logger.info(
        "Persisted Strava state pipeline=%s state_load_ids=%s seen_count_before=%s "
        "seen_count_after=%s added_seen_ids=%s",
        pipeline.pipeline_name,
        getattr(state_info, "loads_ids", None),
        before_count,
        after_count,
        added_count,
    )
    return state_info


def _strava_activities_resource_state(state: TPipelineState) -> dict[str, Any]:
    return (
        state.setdefault("sources", {})
        .setdefault("strava", {})
        .setdefault("resources", {})
        .setdefault("activities", {})
    )


def _seen_ids_from_state(state: TPipelineState) -> list[int]:
    return _unique_ints(
        state.get("sources", {})
        .get("strava", {})
        .get("resources", {})
        .get("activities", {})
        .get("seen_ids", [])
    )


def _unique_ints(values: object) -> list[int]:
    if not isinstance(values, list):
        return []
    seen: set[int] = set()
    unique: list[int] = []
    for value in values:
        try:
            item = int(value)
        except (TypeError, ValueError):
            continue
        if item not in seen:
            unique.append(item)
            seen.add(item)
    return unique


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
