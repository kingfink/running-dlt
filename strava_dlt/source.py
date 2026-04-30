from datetime import datetime
from typing import Any

import dlt

from strava_dlt.auth import StravaAuth, StravaClient

DEFAULT_REFETCH_DAYS = 7

# Top-level fields in getActivityById that are objects or arrays. Forced to JSON
# columns so they live on the parent `activities` row instead of being expanded
# into child tables.
_JSON_COLUMNS: dict[str, dict[str, Any]] = {
    "splits_metric": {"data_type": "json"},
    "splits_standard": {"data_type": "json"},
    "laps": {"data_type": "json"},
    "segment_efforts": {"data_type": "json"},
    "best_efforts": {"data_type": "json"},
    "photos": {"data_type": "json"},
    "stats_visibility": {"data_type": "json"},
    "map": {"data_type": "json"},
    "gear": {"data_type": "json"},
    "athlete": {"data_type": "json"},
    "start_latlng": {"data_type": "json"},
    "end_latlng": {"data_type": "json"},
    "similar_activities": {"data_type": "json"},
    "available_zones": {"data_type": "json"},
}


def _parse_iso_to_epoch(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


@dlt.source(name="strava", max_table_nesting=0)
def strava_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    refetch_days: int = DEFAULT_REFETCH_DAYS,
):
    """Strava activity source.

    Parent resource paginates the athlete-activity list; a transformer fans out
    to getActivityById for each summary. The list endpoint's payload is a
    strict subset of the detail endpoint, so we only persist the detailed
    `activities` table — the parent is `selected=False`.
    """
    auth = StravaAuth(client_id, client_secret, refresh_token)
    client = StravaClient(auth)

    @dlt.resource(name="activity_ids", selected=False, write_disposition="replace")
    def activity_ids():
        state = dlt.current.resource_state()
        last_synced_epoch = int(state.get("last_synced_epoch", 0))
        lookback = max(0, refetch_days) * 24 * 3600
        after = max(0, last_synced_epoch - lookback)

        max_seen = last_synced_epoch
        page = 1
        while True:
            batch = client.get(
                "/athlete/activities",
                params={"after": after, "page": page, "per_page": 200},
            )
            if not batch:
                break
            assert isinstance(batch, list)
            for activity in batch:
                start_date = activity.get("start_date")
                if start_date:
                    ts = _parse_iso_to_epoch(start_date)
                    if ts > max_seen:
                        max_seen = ts
                yield activity
            if len(batch) < 200:
                break
            page += 1

        state["last_synced_epoch"] = max_seen

    @dlt.transformer(
        data_from=activity_ids,
        name="activities",
        write_disposition="merge",
        primary_key="id",
        columns=_JSON_COLUMNS,
    )
    def activity_detail(summary: dict):
        yield client.get(f"/activities/{summary['id']}")

    return activity_ids, activity_detail
