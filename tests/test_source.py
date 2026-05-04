"""Smoke test for strava_source — uses `responses` to stub Strava HTTP calls
and a temporary local DuckDB destination to confirm the pipeline shape end-to-end.

Run with: uv run pytest -q
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import dlt
import pytest
import requests
import responses

from strava_dlt.auth import (
    STRAVA_API_BASE,
    STRAVA_READ_WINDOW_SECONDS,
    STRAVA_TOKEN_URL,
    StravaClient,
    _retry_after_seconds,
    _seconds_to_next_window,
)
from strava_dlt.pipeline import run_strava_pipeline
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


def _make_pipeline(
    tmp_path: Path,
    name: str = "strava_test",
    pipelines_dir: Path | None = None,
) -> dlt.Pipeline:
    """DuckDB pipeline with state isolated to tmp_path so tests don't share state."""
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.duckdb(str(tmp_path / "test.duckdb")),
        dataset_name="strava_test",
        pipelines_dir=str(pipelines_dir or tmp_path / "pipelines"),
    )


def _source_kwargs() -> dict[str, str]:
    return {"client_id": "c", "client_secret": "s", "refresh_token": "rt"}


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

        info, status = run_strava_pipeline(
            pipeline,
            since=SINCE,
            until=UNTIL,
            source_kwargs=_source_kwargs(),
        )
        assert info.has_failed_jobs is False
        assert d1.call_count == 1
        assert d2.call_count == 1
        assert sorted(status["loaded_activity_ids"]) == [1001, 1002]
        assert sorted(_seen_ids(pipeline)) == [1001, 1002]

        # Second run: same window, same summary list. Both ids' start_date is
        # older than the cutoff and both are in seen_ids → skip detail fetches.
        info2, status2 = run_strava_pipeline(
            pipeline,
            since=SINCE,
            until=UNTIL,
            source_kwargs=_source_kwargs(),
        )
        assert info2.has_failed_jobs is False
        assert sorted(status2["skipped_activity_ids"]) == [1001, 1002]
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
        info, status = run_strava_pipeline(
            pipeline,
            since=SINCE,
            until=UNTIL,
            source_kwargs=_source_kwargs(),
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
        run_strava_pipeline(
            pipeline,
            since=SINCE,
            until=UNTIL,
            source_kwargs=_source_kwargs(),
        )
        assert d1.call_count == 1
        assert d2.call_count == 1

        # Without reset_seen, second run would skip both. With reset_seen, both
        # are refetched.
        run_strava_pipeline(
            pipeline,
            since=SINCE,
            until=UNTIL,
            reset_seen=True,
            source_kwargs=_source_kwargs(),
        )
        assert d1.call_count == 2
        assert d2.call_count == 2
        # seen_ids is rebuilt from this run's successful yields.
        assert sorted(_seen_ids(pipeline)) == [1001, 1002]


def test_seen_ids_restore_from_destination_state(tmp_path: Path, monkeypatch, caplog):
    """A fresh pipeline directory should restore seen_ids from DuckDB state and
    skip old detail fetches. This proves state made it to the destination, not
    just the local dlt working folder."""
    monkeypatch.chdir(tmp_path)
    caplog.set_level("INFO")

    first = _make_pipeline(tmp_path, pipelines_dir=tmp_path / "first_pipelines")
    fresh = _make_pipeline(tmp_path, pipelines_dir=tmp_path / "fresh_pipelines")

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

        run_strava_pipeline(
            first,
            since=SINCE,
            until=UNTIL,
            source_kwargs=_source_kwargs(),
        )
        assert sorted(_seen_ids(first)) == [1001, 1002]

        run_strava_pipeline(
            fresh,
            since=SINCE,
            until=UNTIL,
            source_kwargs=_source_kwargs(),
        )

    assert sorted(_seen_ids(fresh)) == [1001, 1002]
    assert d1.call_count == 1
    assert d2.call_count == 1
    assert "Persisting Strava state after successful data load" in caplog.text
    assert "Persisted Strava state" in caplog.text


def test_seen_ids_not_persisted_when_data_load_has_failed_jobs(monkeypatch):
    class FakePipeline:
        pipeline_name = "strava_fake"

        def run(self, source):
            return SimpleNamespace(has_failed_jobs=True, loads_ids=["failed-load"])

    persist_calls = []

    def fake_source(**kwargs):
        kwargs["status"]["loaded_activity_ids"] = [1001]
        return object()

    monkeypatch.setattr("strava_dlt.pipeline.strava_source", fake_source)
    monkeypatch.setattr(
        "strava_dlt.pipeline._persist_seen_ids_after_load",
        lambda *args, **kwargs: persist_calls.append((args, kwargs)),
    )

    info, status = run_strava_pipeline(
        FakePipeline(),
        since=SINCE,
        until=UNTIL,
        source_kwargs=_source_kwargs(),
    )

    assert info.has_failed_jobs is True
    assert status["loaded_activity_ids"] == [1001]
    assert persist_calls == []


def test_detail_cache_shared_across_destinations(tmp_path: Path, monkeypatch):
    """A `detail_cache` dict shared across two `run_strava_pipeline` calls
    lets the second destination's source skip `/activities/{id}` fetches that
    the first destination already made — the dominant rate-limit win for
    backfills, and the reason backfills no longer hit Strava 2x per id."""
    monkeypatch.chdir(tmp_path)
    bq = _make_pipeline(tmp_path, name="strava_bq", pipelines_dir=tmp_path / "bq")
    md = _make_pipeline(tmp_path, name="strava_md", pipelines_dir=tmp_path / "md")
    detail_cache: dict[int, dict] = {}

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

        info1, status1 = run_strava_pipeline(
            bq,
            since=SINCE,
            until=UNTIL,
            source_kwargs={**_source_kwargs(), "detail_cache": detail_cache},
        )
        assert info1.has_failed_jobs is False
        assert d1.call_count == 1
        assert d2.call_count == 1
        assert sorted(detail_cache.keys()) == [1001, 1002]
        assert status1.get("cache_hit_activity_ids", []) == []

        # Second destination starts with empty seen_ids (separate pipeline)
        # but inherits the populated cache → no extra `/activities/{id}` calls.
        info2, status2 = run_strava_pipeline(
            md,
            since=SINCE,
            until=UNTIL,
            source_kwargs={**_source_kwargs(), "detail_cache": detail_cache},
        )
        assert info2.has_failed_jobs is False
        assert d1.call_count == 1
        assert d2.call_count == 1
        assert sorted(status2["cache_hit_activity_ids"]) == [1001, 1002]
        assert sorted(status2["loaded_activity_ids"]) == [1001, 1002]

    with md.sql_client() as c:
        ids = sorted(r[0] for r in c.execute_sql("SELECT id FROM activities"))
        assert ids == [1001, 1002]


def test_get_retries_on_connection_error(monkeypatch):
    """Today's failing run died because a `RemoteDisconnected` mid-pagination
    bubbled out of `client.get` un-retried, killing a 25-min backfill on a
    transient TCP reset. The retry wrapper should now treat it like a 5xx."""
    monkeypatch.setattr("strava_dlt.auth.time.sleep", lambda *_: None)

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _stub_token(rsps)
        # First call: connection drops mid-response. Second call: succeeds.
        rsps.get(
            f"{STRAVA_API_BASE}/athlete/activities",
            body=requests.ConnectionError("Remote end closed connection"),
        )
        rsps.get(
            f"{STRAVA_API_BASE}/athlete/activities",
            json=[{"id": 1, "start_date": "2024-01-01T10:00:00Z"}],
        )

        client = StravaClient("c", "s", "rt")
        result = client.get("/athlete/activities")
        assert result == [{"id": 1, "start_date": "2024-01-01T10:00:00Z"}]


def test_get_raises_after_exhausting_connection_retries(monkeypatch):
    """If transport errors persist beyond the retry budget, raise — don't
    silently return None and confuse the caller."""
    monkeypatch.setattr("strava_dlt.auth.time.sleep", lambda *_: None)

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _stub_token(rsps)
        # 4 consecutive connection errors — exceeds the 4-attempt retry budget.
        for _ in range(5):
            rsps.get(
                f"{STRAVA_API_BASE}/athlete/activities",
                body=requests.ConnectionError("kaboom"),
            )

        client = StravaClient("c", "s", "rt")
        with pytest.raises(requests.ConnectionError):
            client.get("/athlete/activities")


def test_seconds_to_next_window_aligns_to_15min_boundary():
    """Sleep to the next wall-clock 15-min boundary, not a fixed 60s — Strava's
    rate-limit window resets at :00, :15, :30, :45 UTC, so a 60s sleep often
    lands inside the same window and earns another 429."""
    # Pick an epoch that's snapped to a 15-min boundary so the math is clean.
    boundary = 1_700_000_000 - (1_700_000_000 % STRAVA_READ_WINDOW_SECONDS)

    # 1s after boundary → ~15 min till next, plus 5–15s buffer/jitter.
    wait = _seconds_to_next_window(boundary + 1)
    expected_min = STRAVA_READ_WINDOW_SECONDS - 1 + 5
    expected_max = STRAVA_READ_WINDOW_SECONDS - 1 + 15
    assert expected_min <= wait <= expected_max

    # 60s before next boundary → ~60s + buffer.
    wait = _seconds_to_next_window(boundary + STRAVA_READ_WINDOW_SECONDS - 60)
    assert 65 <= wait <= 75


def test_retry_after_header_is_honored():
    """Strava sometimes sends `Retry-After: <seconds>` on 429. Trust it."""
    resp = SimpleNamespace(headers={"Retry-After": "42"})
    assert _retry_after_seconds(resp) == 42

    # Garbage / missing header falls back to next-window sleep.
    assert _retry_after_seconds(SimpleNamespace(headers={"Retry-After": "junk"})) >= 5
    assert _retry_after_seconds(SimpleNamespace(headers={})) >= 5


def test_client_logs_get_requests(caplog):
    caplog.set_level("INFO", logger="strava_dlt.auth")

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _stub_token(rsps)
        _stub_summary_list(rsps, [])

        client = StravaClient("c", "s", "rt")
        assert client.get("/athlete/activities", params={"after": 1, "before": 2}) == []

    messages = [r.getMessage() for r in caplog.records]
    assert any(
        "Strava GET /athlete/activities" in m
        and "params={'after': 1, 'before': 2}" in m
        and "attempt=1" in m
        for m in messages
    )
    assert any("Strava GET /athlete/activities" in m and "status=200" in m for m in messages)
