"""Smoke test for strava_source — uses `responses` to stub Strava HTTP calls
and a temporary local DuckDB destination to confirm the pipeline shape end-to-end.

Run with: uv run pytest -q
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import dlt
import pytest
import responses

from strava_dlt.auth import STRAVA_API_BASE, STRAVA_TOKEN_URL
from strava_dlt.source import strava_source

# Window used across tests. Cutoff = until - 7 days = 2024-01-25, so
# start_dates earlier than that are eligible for skip-if-seen.
SINCE = datetime(2024, 1, 1, tzinfo=UTC)
UNTIL = datetime(2024, 2, 1, tzinfo=UTC)


def _stub_token(rsps):
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


def _stub_summary_list(rsps, summaries):
    rsps.get(f"{STRAVA_API_BASE}/athlete/activities", json=summaries)


def _stub_detail(rsps, activity_id, **extra):
    return rsps.get(
        f"{STRAVA_API_BASE}/activities/{activity_id}",
        json={
            "id": activity_id,
            "start_date": extra.pop("start_date", "2024-01-01T10:00:00Z"),
            "name": extra.pop("name", f"Activity {activity_id}"),
            "distance": extra.pop("distance", 5000.0),
            "splits_metric": [],
            "laps": [],
            "map": {"polyline": "abc"},
            **extra,
        },
    )


@pytest.fixture
def stub_strava():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _stub_token(rsps)
        _stub_summary_list(
            rsps,
            [
                {"id": 1001, "start_date": "2024-01-01T10:00:00Z", "name": "Morning Run"},
                {"id": 1002, "start_date": "2024-01-02T10:00:00Z", "name": "Evening Ride"},
            ],
        )
        _stub_detail(rsps, 1001, start_date="2024-01-01T10:00:00Z", name="Morning Run")
        _stub_detail(
            rsps, 1002, start_date="2024-01-02T10:00:00Z", name="Evening Ride", distance=20000.0
        )
        yield rsps


def _make_pipeline(tmp_path: Path, name: str = "strava_test") -> dlt.Pipeline:
    """DuckDB pipeline with state isolated to tmp_path so tests don't share state."""
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / "test.duckdb")),
        dataset_name="strava_test",
        pipelines_dir=str(tmp_path / "pipelines"),
    )


def _seen_ids(pipeline: dlt.Pipeline) -> list[int]:
    return (
        pipeline.state.get("sources", {})
        .get("strava", {})
        .get("resources", {})
        .get("activities", {})
        .get("seen_ids", [])
    )


def test_source_loads_to_duckdb(tmp_path: Path, stub_strava, monkeypatch):
    monkeypatch.chdir(tmp_path)

    pipeline = _make_pipeline(tmp_path)
    info = pipeline.run(
        strava_source(
            client_id="cid",
            client_secret="csec",
            refresh_token="rt",
            since=SINCE,
            until=UNTIL,
        )
    )
    assert info.has_failed_jobs is False

    with pipeline.sql_client() as c:
        row_count = c.execute_sql("SELECT COUNT(*) FROM activities")[0][0]
        assert row_count == 2

        # JSON columns should exist as a single column on the parent row, not
        # as child tables.
        names = {
            r[0]
            for r in c.execute_sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'strava_test'"
            )
        }
        # No `activities__splits_metric` etc. child tables.
        assert not any(t.startswith("activities__") for t in names), names


def test_skip_already_fetched(tmp_path: Path, monkeypatch):
    """Second run over the same window should skip detail fetches for ids that
    are already in seen_ids and whose start_date is older than the refetch
    cutoff (default 7 days before `until`)."""
    monkeypatch.chdir(tmp_path)
    pipeline = _make_pipeline(tmp_path)

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _stub_token(rsps)
        _stub_summary_list(
            rsps,
            [
                {"id": 1001, "start_date": "2024-01-01T10:00:00Z", "name": "Morning Run"},
                {"id": 1002, "start_date": "2024-01-02T10:00:00Z", "name": "Evening Ride"},
            ],
        )
        d1 = _stub_detail(rsps, 1001, start_date="2024-01-01T10:00:00Z")
        d2 = _stub_detail(rsps, 1002, start_date="2024-01-02T10:00:00Z")

        info = pipeline.run(
            strava_source(
                client_id="c", client_secret="s", refresh_token="rt", since=SINCE, until=UNTIL
            )
        )
        assert info.has_failed_jobs is False
        assert d1.call_count == 1
        assert d2.call_count == 1
        assert sorted(_seen_ids(pipeline)) == [1001, 1002]

        # Second run: same window, same summary list. Both ids' start_date is
        # older than the cutoff and both are in seen_ids → skip detail fetches.
        info2 = pipeline.run(
            strava_source(
                client_id="c", client_secret="s", refresh_token="rt", since=SINCE, until=UNTIL
            )
        )
        assert info2.has_failed_jobs is False
        assert d1.call_count == 1
        assert d2.call_count == 1

    with pipeline.sql_client() as c:
        row_count = c.execute_sql("SELECT COUNT(*) FROM activities")[0][0]
        assert row_count == 2


def test_graceful_exit_on_rate_limit(tmp_path: Path, monkeypatch):
    """When detail fetch exhausts 429 retries, the resource ends cleanly:
    rows already yielded are loaded, the rate-limited path is recorded in
    `status`, and seen_ids contains only successfully-loaded ids."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("strava_dlt.auth.time.sleep", lambda *_: None)
    pipeline = _make_pipeline(tmp_path)

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _stub_token(rsps)
        _stub_summary_list(
            rsps,
            [
                {"id": 1001, "start_date": "2024-01-01T10:00:00Z", "name": "Morning Run"},
                {"id": 1002, "start_date": "2024-01-02T10:00:00Z", "name": "Evening Ride"},
            ],
        )
        _stub_detail(rsps, 1001)
        # 1002 always returns 429; auth.py retries 3x, then raises StravaRateLimitExceeded.
        rsps.get(
            f"{STRAVA_API_BASE}/activities/1002",
            json={"message": "Rate Limit Exceeded"},
            status=429,
        )

        status: dict = {}
        info = pipeline.run(
            strava_source(
                client_id="c",
                client_secret="s",
                refresh_token="rt",
                since=SINCE,
                until=UNTIL,
                status=status,
            )
        )

    assert info.has_failed_jobs is False
    assert status.get("rate_limited_path") == "/activities/1002"
    assert _seen_ids(pipeline) == [1001]

    with pipeline.sql_client() as c:
        ids = sorted(r[0] for r in c.execute_sql("SELECT id FROM activities"))
        assert ids == [1001]


def test_reset_seen_forces_refetch(tmp_path: Path, monkeypatch):
    """With reset_seen=True, ids already in seen_ids are refetched."""
    monkeypatch.chdir(tmp_path)
    pipeline = _make_pipeline(tmp_path)

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _stub_token(rsps)
        _stub_summary_list(
            rsps,
            [
                {"id": 1001, "start_date": "2024-01-01T10:00:00Z", "name": "Morning Run"},
                {"id": 1002, "start_date": "2024-01-02T10:00:00Z", "name": "Evening Ride"},
            ],
        )
        d1 = _stub_detail(rsps, 1001, start_date="2024-01-01T10:00:00Z")
        d2 = _stub_detail(rsps, 1002, start_date="2024-01-02T10:00:00Z")

        # First run populates seen_ids.
        pipeline.run(
            strava_source(
                client_id="c", client_secret="s", refresh_token="rt", since=SINCE, until=UNTIL
            )
        )
        assert d1.call_count == 1
        assert d2.call_count == 1

        # Without reset_seen, second run would skip both. With reset_seen, both
        # are refetched.
        pipeline.run(
            strava_source(
                client_id="c",
                client_secret="s",
                refresh_token="rt",
                since=SINCE,
                until=UNTIL,
                reset_seen=True,
            )
        )
        assert d1.call_count == 2
        assert d2.call_count == 2
        # seen_ids is rebuilt from this run's successful yields.
        assert sorted(_seen_ids(pipeline)) == [1001, 1002]
