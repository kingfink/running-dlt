# running-dlt

Strava activity sync to BigQuery and MotherDuck via [dlt](https://dlthub.com/), scheduled on [Modal](https://modal.com/).

The pipeline pages `/athlete/activities` for new IDs and fans out via a `@dlt.transformer` to `getActivityById` for the rich detail payload (laps, splits, segment efforts, photos). Nested arrays land as JSON columns on a single `activities` table — no child tables. It re-fetches the trailing 7 days every run to capture post-hoc edits (titles, kudos, gear), keyed on activity `id` with `write_disposition="merge"`.

```
                       ┌──────────────────────────────┐
                       │  Modal app (modal_app.py)    │
                       │  cron: 0 6 * * *  UTC        │
                       └─────────────┬────────────────┘
                                     │  run_pipelines(target="prod")
                                     ▼
            ┌────────────────────────────────────────────────┐
            │ strava_dlt.pipeline.run_pipelines              │
            │   bq.run(strava_source(refetch_days=7))        │
            │   md.run(strava_source(refetch_days=7))        │
            └─────────┬──────────────────────┬───────────────┘
                      ▼                      ▼
              BigQuery: strava       MotherDuck: strava
```

## Quick start (local dev)

Prerequisites: `uv`, a registered [Strava API app](docs/strava-app-setup.md), a BigQuery service account, a MotherDuck token.

```bash
git clone <this repo>
cd running-dlt

uv sync --extra dev
cp .dlt/secrets.toml.example .dlt/secrets.toml
# Fill in client_id + client_secret + BigQuery SA + MotherDuck token.

uv run python scripts/authorize.py
# Paste the OAuth `code` query param; copy the printed refresh_token into
# .dlt/secrets.toml under [sources.strava] refresh_token.

uv run python scripts/run_local.py --only motherduck
# First run smoke-tests against MotherDuck only; writes to schema strava_dev_$USER.

uv run python scripts/run_local.py
# Then run both destinations.
```

Re-running is idempotent: `merge` on activity `id` deduplicates, the trailing 7-day refetch picks up edits.

## Deploy to Modal

See [docs/modal-deploy.md](docs/modal-deploy.md). Briefly:

```bash
uv sync --extra modal
# Create three Modal Secrets: strava-oauth, dlt-bigquery, dlt-motherduck.
uv run modal deploy modal_app.py
uv run modal run modal_app.py::sync_strava_manual --refetch-days 99999  # one-time backfill
```

The cron-triggered `sync_strava_scheduled` then runs every day at 06:00 UTC.

## Layout

| Path | What it does |
| --- | --- |
| `strava_dlt/auth.py` | OAuth refresh-token rotation, retry/backoff for the Strava REST client. |
| `strava_dlt/source.py` | `@dlt.source` with parent `activity_ids` resource and `activity_detail` transformer. |
| `strava_dlt/pipeline.py` | `run_pipelines(target)` factory — instantiates one `dlt.pipeline` per destination. |
| `strava_dlt/config.py` | `STRAVA_DLT_TARGET=prod\|dev` → dataset name (`strava` vs `strava_dev_$USER`). |
| `modal_app.py` | Scheduled + manual Modal entrypoints. |
| `scripts/authorize.py` | One-time helper to mint a Strava refresh token. |
| `scripts/run_local.py` | Manual local invocation against the dev dataset. |
| `tests/test_source.py` | Smoke test against a stubbed Strava API + local DuckDB. |

## Prod vs dev

`STRAVA_DLT_TARGET=prod` → dataset `strava`. Modal cron sets this.
Anything else (default `dev`) → dataset `strava_dev_<user>`. Local dev never collides with prod marts.

The two pipelines (`strava_bq_<target>`, `strava_md_<target>`) have unique names so their state files don't clobber each other.

## Adapting this template

See [docs/adapting-template.md](docs/adapting-template.md). The Modal layout, pipeline factory, prod/dev split, and merge-on-id pattern transfer to most REST APIs — you mostly rewrite `auth.py` and `source.py`.

## Notes

- **Rate limits.** Strava is 100 calls/15min and 1000 calls/day. A single-athlete daily sync is well inside both. Two pipelines means 2× the calls (intentional — see plan); still nowhere near the cap.
- **Refresh token rotation.** Strava sometimes returns a new refresh token on use. We log a `WARNING` if it does — update the Modal Secret manually when you see it.
- **Webhooks.** Not implemented. The trailing 7-day re-fetch covers the common case (you re-title a ride within a week). If you need event-accurate edit capture, see the plan's webhook discussion.
