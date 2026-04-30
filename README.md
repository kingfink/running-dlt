# running-dlt

Strava → BigQuery + MotherDuck via [dlt](https://dlthub.com/), scheduled on [Modal](https://modal.com/).

Pages `/athlete/activities` and fans out to `getActivityById` via a `@dlt.transformer`.
Nested arrays (laps, splits, segment efforts) stay as JSON columns on a single
`activities` table — no child tables. `merge` on activity `id`, with a trailing
7-day lookback window each run to catch post-hoc edits.

## Local dev

Prereqs: `uv`, a Strava API app ([create one](https://www.strava.com/settings/api),
callback domain `localhost`), a BigQuery service account, a MotherDuck token.

```bash
uv sync --extra dev
cp .dlt/secrets.toml.example .dlt/secrets.toml   # fill in client_id, client_secret, BQ SA, MD token
uv run python scripts/authorize.py               # one-time: mints refresh_token, paste back into secrets.toml
uv run python scripts/run_local.py --destinations motherduck   # smoke test
uv run python scripts/run_local.py                             # both destinations
```

Writes to `strava_dev_$USER`. Re-runs are idempotent (merge on `id`).

## Deploy to Modal

Three Modal Secrets:

- `strava-oauth` — `SOURCES__STRAVA__CLIENT_ID`, `..._CLIENT_SECRET`, `..._REFRESH_TOKEN`
- `dlt-bigquery` — `DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID`, `..._PRIVATE_KEY`, `..._CLIENT_EMAIL` (service account scoped to one BQ dataset, plus `roles/bigquery.jobUser` at project level)
- `dlt-motherduck` — `DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD`

```bash
uv sync --extra modal
uv run modal deploy modal_app.py                            # registers the daily cron
uv run modal run modal_app.py::sync --lookback-days 99999   # one-time backfill
```

After the backfill, the cron (`0 6 * * *` UTC) takes over. The same `sync`
function serves both the schedule and ad-hoc invocations.

## Layout

| Path | What it does |
| --- | --- |
| `strava_dlt/auth.py` | Strava OAuth + retry/backoff client. |
| `strava_dlt/source.py` | `@dlt.source` with parent `activity_ids` resource and `activities` transformer. |
| `strava_dlt/pipeline.py` | `run_pipelines(target, lookback_days, destinations)` — one `dlt.pipeline` per destination. |
| `strava_dlt/config.py` | `STRAVA_DLT_TARGET=prod\|dev` → dataset name (`strava` vs `strava_dev_$USER`). |
| `modal_app.py` | Single `sync` function: cron-scheduled and manually invocable. |
| `scripts/authorize.py` | One-time helper to mint a refresh token. |
| `scripts/run_local.py` | Manual local run against `strava_dev_$USER`. |
| `tests/test_source.py` | Smoke test against a stubbed Strava API + local DuckDB. |

## Notes

- Two pipelines, source extracted twice (once per destination). Simpler than a `filesystem` staging layer; well within Strava's 1000-call/day cap for one athlete.
- The cron-scheduled `sync` runs with the default `lookback_days=7`. To widen the window, invoke it manually with `modal run modal_app.py::sync --lookback-days N`.
- Strava sometimes rotates the refresh token. The pipeline logs a `WARNING` if it does — update the Modal Secret manually when you see one.

## Forking for another REST API

Most of the scaffolding is generic. To swap in a different source:

- Rewrite `strava_dlt/auth.py` (different auth flavor) and `strava_dlt/source.py` (endpoints, incremental column, JSON fields).
- Rename the `strava_dlt/` package, the Modal app, and the secret names.
- Everything else (pipeline factory, prod/dev split, cron, merge-on-id) stays.
