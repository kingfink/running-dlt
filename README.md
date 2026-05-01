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

# Project / dataset / location are env-driven (not in secrets.toml):
export BIGQUERY_PROJECT=<gcp-project-id>
export BIGQUERY_DATASET=strava_dev_$USER
export BIGQUERY_LOCATION=US                      # optional
export MOTHERDUCK_DATABASE=running
export MOTHERDUCK_DATASET=strava_dev_$USER

uv run python scripts/authorize.py               # one-time: mints refresh_token, paste back into secrets.toml
uv run python modal_app.py --destinations motherduck   # smoke test
uv run python modal_app.py                             # both destinations
```

Re-runs are idempotent (merge on `id`).

## Deploy to Modal

Three Modal Secrets:

- `strava-oauth` — `SOURCES__STRAVA__CLIENT_ID`, `..._CLIENT_SECRET`, `..._REFRESH_TOKEN`
- `running-dlt-bq-sync` — `BIGQUERY_PROJECT`, `BIGQUERY_DATASET` (e.g. `strava`), `BIGQUERY_LOCATION` (optional), plus `DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY` and `..._CLIENT_EMAIL` (service account scoped to the target dataset, plus `roles/bigquery.jobUser` at project level)
- `running-dlt-md-sync` — `MOTHERDUCK_DATABASE` (e.g. `running`), `MOTHERDUCK_DATASET` (e.g. `strava`), and `DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD`

```bash
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
| `strava_dlt/pipeline.py` | `run_pipelines(lookback_days, destinations)` — one `dlt.pipeline` per destination, dataset/project/location read from env. |
| `modal_app.py` | `sync` function (cron + `modal run`); `python modal_app.py` for local dev. |
| `scripts/authorize.py` | One-time helper to mint a refresh token. |
| `tests/test_source.py` | Smoke test against a stubbed Strava API + local DuckDB. |

## Notes

- Two pipelines, source extracted twice (once per destination). Simpler than a `filesystem` staging layer; well within Strava's 1000-call/day cap for one athlete.
- The cron-scheduled `sync` runs with the default `lookback_days=7`. To widen the window, invoke it manually with `modal run modal_app.py::sync --lookback-days N`.
- Strava sometimes rotates the refresh token. The pipeline logs a `WARNING` if it does — update the Modal Secret manually when you see one.
- **Env-var layout is provisional.** The current per-destination split (`BIGQUERY_*` / `MOTHERDUCK_*`) mirrors the sibling [`aej-dlt`](https://github.com/kingfink/aej-dlt) repo for consistency, with no built-in environment (prod/dev) split. Once we templatize this and need to support multiple environments cleanly, revisit — likely a layered scheme (e.g. a single `*_ENV` flag toggling dataset suffixes, or per-env Modal secrets).

## Forking for another REST API

Most of the scaffolding is generic. To swap in a different source:

- Rewrite `strava_dlt/auth.py` (different auth flavor) and `strava_dlt/source.py` (endpoints, incremental column, JSON fields).
- Rename the `strava_dlt/` package, the Modal app, and the secret names.
- Everything else (pipeline factory, env-driven destination config, cron, merge-on-id) stays.
