from datetime import datetime
from typing import Any

import dlt

from strava_dlt.auth import StravaClient

DEFAULT_LOOKBACK_DAYS = 7

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


@dlt.source(name="strava", max_table_nesting=0)
def strava_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
):
    """Strava activity source.

    `activity_ids` paginates the athlete-activity list (parent, not persisted).
    `activities` fans out via `getActivityById` and is the only table written.
    """
    client = StravaClient(client_id, client_secret, refresh_token)

    @dlt.resource(name="activity_ids", selected=False, write_disposition="replace")
    def activity_ids():
        # Manual trailing-window watermark. We can't use dlt.sources.incremental
        # here because Strava's `after` is exclusive and doesn't natively support
        # a "re-pull the last N days" lookback for capturing edits.
        state = dlt.current.resource_state()
        last_synced_epoch = int(state.get("last_synced_epoch", 0))
        after = max(0, last_synced_epoch - max(0, lookback_days) * 86400)

        max_seen = last_synced_epoch
        page = 1
        while True:
            batch = client.get(
                "/athlete/activities",
                params={"after": after, "page": page, "per_page": 200},
            )
            assert isinstance(batch, list)
            if not batch:
                break
            for activity in batch:
                if start := activity.get("start_date"):
                    ts = int(datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp())
                    max_seen = max(max_seen, ts)
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
