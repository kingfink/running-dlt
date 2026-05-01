"""Smoke test for strava_source — uses `responses` to stub Strava HTTP calls
and a temporary local DuckDB destination to confirm the pipeline shape end-to-end.

Run with: uv run pytest -q
"""

from __future__ import annotations

from pathlib import Path

import dlt
import pytest
import responses

from strava_dlt.auth import STRAVA_API_BASE, STRAVA_TOKEN_URL
from strava_dlt.source import strava_source


@pytest.fixture
def stub_strava():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.post(
            STRAVA_TOKEN_URL,
            json={
                "access_token": "fake-access",
                "refresh_token": "rt",
                "expires_at": 9999999999,
                "expires_in": 21600,
                "token_type": "Bearer",
            },
        )
        rsps.get(
            f"{STRAVA_API_BASE}/athlete/activities",
            json=[
                {"id": 1001, "start_date": "2024-01-01T10:00:00Z", "name": "Morning Run"},
                {"id": 1002, "start_date": "2024-01-02T10:00:00Z", "name": "Evening Ride"},
            ],
        )
        rsps.get(
            f"{STRAVA_API_BASE}/activities/1001",
            json={
                "id": 1001,
                "start_date": "2024-01-01T10:00:00Z",
                "name": "Morning Run",
                "distance": 5000.0,
                "splits_metric": [{"split": 1, "distance": 1000.0}],
                "laps": [{"lap_index": 1}],
                "map": {"polyline": "abc"},
            },
        )
        rsps.get(
            f"{STRAVA_API_BASE}/activities/1002",
            json={
                "id": 1002,
                "start_date": "2024-01-02T10:00:00Z",
                "name": "Evening Ride",
                "distance": 20000.0,
                "splits_metric": [],
                "laps": [],
                "map": {"polyline": "xyz"},
            },
        )
        yield rsps


def test_source_loads_to_duckdb(tmp_path: Path, stub_strava, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    monkeypatch.chdir(tmp_path)

    pipeline = dlt.pipeline(
        pipeline_name="strava_test",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="strava_test",
    )
    info = pipeline.run(
        strava_source(
            client_id="cid",
            client_secret="csec",
            refresh_token="rt",
            lookback_days=7,
        )
    )
    assert info.has_failed_jobs is False

    with pipeline.sql_client() as c:
        row_count = c.execute_sql("SELECT COUNT(*) FROM activities")[0][0]
        assert row_count == 2

        # JSON columns should exist as a single column on the parent row, not
        # as child tables.
        names = {r[0] for r in c.execute_sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'strava_test'"
        )}
        # No `activities__splits_metric` etc. child tables.
        assert not any(t.startswith("activities__") for t in names), names
