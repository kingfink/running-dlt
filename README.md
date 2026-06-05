# running-dlt

Strava → BigQuery + MotherDuck via [dlt](https://dlthub.com/), scheduled on [Modal](https://modal.com/).

Pages `/athlete/activities` and fans out to `getActivityById` via a `@dlt.transformer`.
Nested arrays (laps, splits, segment efforts) stay as JSON columns on a single
`activities` table — no child tables. `merge` on activity `id` over an explicit
`[since, until)` window. Daily cron defaults to `now - 7d` through `now` so the
last week's activities (including post-hoc title/kudos/gear edits) get refreshed
each run; backfill is a separate per-window invocation (see below). Older
activities are skipped when their ids are already present in destination-backed
dlt state (`seen_ids`), and that state is written only after the data load
finishes without failed jobs.

## Local dev

Prereqs: `uv`, a Strava API app ([create one](https://www.strava.com/settings/api),
callback domain `localhost`), a BigQuery service account, a MotherDuck token.

```bash
uv sync --extra dev
cp .dlt/secrets.toml.example .dlt/secrets.toml   # fill in client_id, client_secret, BQ SA, MD token

# Project / dataset / location are env-driven (not in secrets.toml):
export BIGQUERY_PROJECT=running-and-stuff
export BIGQUERY_DATASET=strava_dev_$USER
export BIGQUERY_LOCATION=US                      # optional
export MOTHERDUCK_DATABASE=running
export MOTHERDUCK_DATASET=strava_dev_$USER

uv run python scripts/authorize.py               # one-time: mints refresh_token, paste back into secrets.toml
uv run python modal_app.py --destinations motherduck                          # last 7 days, MotherDuck only
uv run python modal_app.py                                                    # last 7 days, both destinations
uv run python modal_app.py --since 2024-01-01 --until 2024-02-01              # explicit window
uv run python modal_app.py --reset-seen                                       # force refetch, then rebuild seen_ids
```

Re-runs over the same window are idempotent (merge on `id`) and should skip old
detail fetches once `seen_ids` has been restored from destination state.

## Deploy to Modal

Three Modal Secrets:

- `strava-oauth` — `SOURCES__STRAVA__CLIENT_ID`, `..._CLIENT_SECRET`, `..._REFRESH_TOKEN`
- `running-dlt-bq-sync` — `BIGQUERY_PROJECT`, `BIGQUERY_DATASET` (e.g. `strava`), `BIGQUERY_LOCATION` (optional), plus `DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY` and `..._CLIENT_EMAIL` (service account scoped to the target dataset, plus `roles/bigquery.jobUser` at project level). The `private_key` value can be pasted straight from the SA JSON file — escaped `\n` sequences are converted to real PEM newlines at runtime (`strava_dlt/pipeline.py::_normalize_bigquery_private_key_env`). **TODO:** sibling dlt repos (`aej-dlt`) carry the same shim. Long-term, switch to a single `DESTINATION__BIGQUERY__CREDENTIALS` env holding the SA JSON verbatim — dlt's JSON loader handles `\n` correctly and the shim goes away.
- `running-dlt-md-sync` — `MOTHERDUCK_DATABASE` (e.g. `running`), `MOTHERDUCK_DATASET` (e.g. `strava`), and `DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD`

```bash
uv run modal deploy modal_app.py                                              # registers the daily cron

# Backfill, year at a time. Each chunk fits Strava's default 1000-call daily
# quota for a single athlete; year boundaries make it easy to resume after a
# transient failure (re-runs over the same window are idempotent on `id`).
uv run modal run modal_app.py::sync --since=2019-01-01 --until=2020-01-01
uv run modal run modal_app.py::sync --since=2020-01-01 --until=2021-01-01
# ...up to the current year.

# Force a refetch if state needs rebuilding:
uv run modal run modal_app.py::sync --since=2019-01-01 --until=2020-01-01 --reset-seen=1
```

After the backfill, the cron (`0 6 * * *` UTC) takes over with the default
last-7-days window. The same `sync` function serves both the schedule and
ad-hoc invocations — pass `--since`/`--until` to override the window.

## Layout

| Path | What it does |
| --- | --- |
| `tailor_made_dlt_sources.strava` | Shared Strava OAuth client and `@dlt.source` with parent `activity_ids` resource and `activities` transformer. Pulls `[since, until)`, reads `seen_ids`, logs request params, and reports extracted/skipped ids. |
| `strava_dlt/pipeline.py` | `run_pipelines(since, until, destinations)` — one `dlt.pipeline` per destination, dataset/project/location read from env; persists `seen_ids` after successful data loads. |
| `modal_app.py` | `sync(since, until)` (cron + `modal run`); `python modal_app.py` for local dev. Default window is `now - 7d` through `now` (UTC). |
| `scripts/authorize.py` | One-time helper to mint a refresh token. |
| `tests/test_source.py` | Smoke test against a stubbed Strava API + local DuckDB. |

## Notes

- Two pipelines, source extracted once per destination. Each destination keeps its own `seen_ids`, so request counts can differ after partial failures or state rebuilds.
- The cron-scheduled `sync` runs with the default last-7-days window from `now`. Override with `modal run modal_app.py::sync --since=YYYY-MM-DD --until=YYYY-MM-DD`. State logs include the window, API params, extracted/skipped counts, and `Persisted Strava state ...` after the state-only load package succeeds.
- Strava sometimes rotates the refresh token. The pipeline logs a `WARNING` if it does — update the Modal Secret manually when you see one.
- **Env-var layout is provisional.** The current per-destination split (`BIGQUERY_*` / `MOTHERDUCK_*`) mirrors the sibling [`aej-dlt`](https://github.com/kingfink/aej-dlt) repo for consistency, with no built-in environment (prod/dev) split. Once we templatize this and need to support multiple environments cleanly, revisit — likely a layered scheme (e.g. a single `*_ENV` flag toggling dataset suffixes, or per-env Modal secrets).

## Forking for another REST API

Most of the orchestration is generic. To swap in a different source, add or
import another `tailor_made_dlt_sources` source, then update `strava_dlt/pipeline.py`
and the Modal app / secret names. Destination setup, cron, and state persistence
stay local to this repo.
