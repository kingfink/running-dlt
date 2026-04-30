# Modal deploy

This deploys two Modal functions:

- `sync_strava_scheduled` — runs daily at 06:00 UTC. Cron-only, no inputs.
- `sync_strava_manual` — invocable on demand with a custom `--refetch-days`. Use it for backfills or to re-pull edits older than the trailing-7-day window.

## 1. Install Modal locally

```bash
uv sync --extra modal
uv run modal token new   # one-time login
```

## 2. Provision Modal Secrets

Three secrets, one per system. The names below are the ones `modal_app.py` references — rename in both places if you prefer.

### `strava-oauth`

| Env var | Value |
| --- | --- |
| `STRAVA_CLIENT_ID` | from Strava app |
| `STRAVA_CLIENT_SECRET` | from Strava app |
| `STRAVA_REFRESH_TOKEN` | from `scripts/authorize.py` |
| `SOURCES__STRAVA__CLIENT_ID` | same as `STRAVA_CLIENT_ID` (dlt naming) |
| `SOURCES__STRAVA__CLIENT_SECRET` | same as `STRAVA_CLIENT_SECRET` |
| `SOURCES__STRAVA__REFRESH_TOKEN` | same as `STRAVA_REFRESH_TOKEN` |

The `SOURCES__STRAVA__*` set is what dlt actually reads via `dlt.secrets.value`. Keep the unprefixed copies too — they make logs and one-off `modal run python -c '...'` debugging readable.

### `dlt-bigquery`

Create a dedicated GCP service account scoped to **just the BigQuery dataset(s) you want this pipeline to write to**, not the whole project. Roles:

- `roles/bigquery.dataEditor` on the `strava` dataset (and `strava_dev_*` if you'll run dev from Modal).
- `roles/bigquery.jobUser` at the project level.

Download the JSON key. The relevant fields go into the secret:

| Env var | Value |
| --- | --- |
| `DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID` | from JSON `project_id` |
| `DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY` | from JSON `private_key` (multiline OK) |
| `DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL` | from JSON `client_email` |

### `dlt-motherduck`

| Env var | Value |
| --- | --- |
| `DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD` | MotherDuck access token from https://app.motherduck.com → Settings → Tokens |
| `DESTINATION__MOTHERDUCK__CREDENTIALS__DATABASE` | `strava` (or whatever you want the MotherDuck database to be called) |

## 3. Deploy

```bash
uv run modal deploy modal_app.py
```

## 4. First backfill

```bash
uv run modal run modal_app.py::sync_strava_manual --refetch-days 99999
```

This forces the source's lookback window to cover all of history, which on first run effectively does a full backfill into prod (`strava` dataset in both BigQuery and MotherDuck). Watch the logs — full history for an active athlete is typically a few hundred to a couple thousand activities, well inside Strava's 1000/day cap.

After this completes, the daily cron takes over.

## Operating notes

- **Two pipelines, one source called twice.** Each pipeline has its own state. If MotherDuck fails mid-run but BigQuery succeeded, the next run still re-fetches the trailing 7 days for both — they re-converge naturally.
- **Don't enable Modal's "rerun on failure" without thinking.** dlt is idempotent in normal conditions, but a partial-write scenario might double-count if you re-run before investigating. Check `_dlt_loads` before a manual rerun.
- **Cost.** Daily Modal invocations of a small dlt pipeline are cents/month. The dominant cost is BigQuery storage + MotherDuck plan if any.
