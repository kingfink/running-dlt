from datetime import datetime
from typing import Any

import dlt

from strava_dlt.auth import StravaClient

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
    since: datetime,
    until: datetime,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
):
    """Strava activity source over a half-open `[since, until)` window.

    `activity_ids` paginates the athlete-activity list (parent, not persisted).
    `activities` fans out via `getActivityById` and is the only table written.
    Stateless: re-running the same window is idempotent (merge on `id`).
    """
    client = StravaClient(client_id, client_secret, refresh_token)
    after_epoch = int(since.timestamp())
    before_epoch = int(until.timestamp())

    @dlt.resource(name="activity_ids", selected=False, write_disposition="replace")
    def activity_ids():
        page = 1
        while True:
            batch = client.get(
                "/athlete/activities",
                params={
                    "after": after_epoch,
                    "before": before_epoch,
                    "page": page,
                    "per_page": 200,
                },
            )
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
        yield client.get(f"/activities/{summary['id']}")

    return activity_ids, activity_detail
