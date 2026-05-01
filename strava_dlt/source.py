import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import dlt

from strava_dlt.auth import StravaClient, StravaRateLimitExceeded

logger = logging.getLogger(__name__)

# Strava's getActivityById returns several nested objects/arrays. By default,
# dlt would explode each into a child table (`activities__splits_metric`,
# `activities__laps`, ...). With `max_table_nesting=0` on the source plus
# `data_type=json` hints here, every listed field stays as a single JSON column
# on the parent `activities` row.
_JSON_COLUMNS: dict[str, dict[str, Any]] = {
    f: {"data_type": "json"}
    for f in (
        "splits_metric",
        "splits_standard",
        "laps",
        "segment_efforts",
        "best_efforts",
        "photos",
        "stats_visibility",
        "map",
        "gear",
        "athlete",
        "start_latlng",
        "end_latlng",
        "similar_activities",
        "available_zones",
    )
}


def _parse_start_date(s: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


@dlt.source(name="strava", max_table_nesting=0)
def strava_source(
    since: datetime,
    until: datetime,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    status: dict | None = None,
    reset_seen: bool = False,
):
    """Strava activity source over a half-open `[since, until)` window.

    `activity_ids` paginates the athlete-activity list (parent, not persisted).
    `activities` fans out via `getActivityById` and is the only table written.

    Resilient extraction: if Strava's read quota is exhausted, the offending
    resource ends its generator cleanly so dlt commits whatever was already
    extracted. The path that hit the limit is recorded in `status` so the
    caller (`run_pipelines`) can raise `StravaRateLimitedRun` after the load.

    Skip-already-loaded: each activity id is recorded in pipeline state under
    `seen_ids`. Subsequent runs skip detail fetches for ids in `seen_ids` whose
    `start_date` is older than `STRAVA_REFETCH_DAYS` (default 7) before `until`
    — recent activities are still refetched in case they were edited. Pass
    `reset_seen=True` to clear `seen_ids` and force a full refetch.
    """
    status = status if status is not None else {}
    client = StravaClient(client_id, client_secret, refresh_token)
    after_epoch = int(since.timestamp())
    before_epoch = int(until.timestamp())
    refetch_days = int(os.environ.get("STRAVA_REFETCH_DAYS", "7"))
    refetch_cutoff = until - timedelta(days=refetch_days)

    # Closure-level flags shared across the resource generators for one run.
    _reset_pending = reset_seen
    _rate_limited = False
    _seen: set[int] | None = None
    _seen_ids: list[int] | None = None

    @dlt.resource(name="activity_ids", selected=False, write_disposition="replace")
    def activity_ids():
        nonlocal _rate_limited
        page = 1
        while True:
            try:
                batch = client.get(
                    "/athlete/activities",
                    params={
                        "after": after_epoch,
                        "before": before_epoch,
                        "page": page,
                        "per_page": 200,
                    },
                )
            except StravaRateLimitExceeded:
                logger.warning(
                    "rate-limited paginating /athlete/activities — committing what we have"
                )
                status["rate_limited_path"] = "/athlete/activities"
                _rate_limited = True
                return
            assert isinstance(batch, list)
            if not batch:
                break
            yield from batch
            if len(batch) < 200:
                break
            page += 1

    @dlt.transformer(
        data_from=activity_ids,
        name="activities",
        write_disposition="merge",
        primary_key="id",
        columns=_JSON_COLUMNS,
    )
    def activity_detail(summary: dict):
        nonlocal _reset_pending, _rate_limited, _seen, _seen_ids

        if _rate_limited:
            return

        if _seen is None:
            state = dlt.current.resource_state()
            if _reset_pending:
                state["seen_ids"] = []
                _reset_pending = False
            _seen_ids = state.setdefault("seen_ids", [])
            _seen = set(_seen_ids)

        activity_id = summary["id"]
        start_date = _parse_start_date(summary.get("start_date", ""))
        if activity_id in _seen and start_date is not None and start_date < refetch_cutoff:
            logger.debug(
                "skipping %s (already loaded, started %s)",
                activity_id,
                summary.get("start_date"),
            )
            return

        try:
            detail = client.get(f"/activities/{activity_id}")
        except StravaRateLimitExceeded:
            logger.warning(
                "rate-limited fetching /activities/%s — committing what we have",
                activity_id,
            )
            status["rate_limited_path"] = f"/activities/{activity_id}"
            _rate_limited = True
            return

        yield detail
        if activity_id not in _seen:
            _seen.add(activity_id)
            _seen_ids.append(activity_id)

    return activity_ids, activity_detail
