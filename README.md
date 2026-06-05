# running-dlt

Strava -> BigQuery + MotherDuck via [dlt](https://dlthub.com/), scheduled on
[Modal](https://modal.com/).

The pipeline pages `/athlete/activities` and fans out to `getActivityById` via
the shared `tailor_made_dlt_sources.strava` source. Nested arrays such as laps,
splits, and segment efforts stay as JSON columns on a single `activities` table.
Loads merge on activity `id` over an explicit `[since, until)` window.

Daily runs refresh the last seven days so post-hoc title, kudos, gear, and
metadata edits are picked up. Older activities are skipped when their ids are
already present in destination-backed dlt state, and that state is written only
after the data load finishes without failed jobs.

## Public visibility

This repository is public for reference, but it is not currently maintained as
an open-source project and is not intended to accept outside contributions. No
license is provided.

## Local dev

Prereqs:

- `uv`
- a Strava API app with callback domain `localhost`
- BigQuery and MotherDuck destinations for your own data

```bash
uv sync --extra dev
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Fill in `.dlt/secrets.toml`, then set destination configuration in the
environment:

```bash
export BIGQUERY_PROJECT=<gcp-project-id>
export BIGQUERY_DATASET=strava_dev_$USER
export BIGQUERY_LOCATION=US
export MOTHERDUCK_DATABASE=<database-name>
export MOTHERDUCK_DATASET=strava_dev_$USER
```

Mint a Strava refresh token once, then paste it back into `.dlt/secrets.toml`:

```bash
uv run python scripts/authorize.py
```

Run the sync locally:

```bash
uv run python modal_app.py
uv run python modal_app.py --destinations motherduck
uv run python modal_app.py --since 2024-01-01 --until 2024-02-01
uv run python modal_app.py --reset-seen
```

Re-runs over the same window are idempotent and skip old detail fetches once
`seen_ids` has been restored from destination state.

## Modal

The Modal app expects secrets containing Strava OAuth values, BigQuery
destination configuration and service account credentials, and MotherDuck
destination configuration and token credentials. See `modal_app.py` and
`.dlt/secrets.toml.example` for the environment variable names.

```bash
uv run modal deploy modal_app.py
uv run modal run modal_app.py::sync --since=2024-01-01 --until=2024-02-01
uv run modal run modal_app.py::sync --since=2024-01-01 --until=2024-02-01 --reset-seen=1
```

The scheduled Modal function uses the default last-seven-days window. The same
`sync` function also serves ad-hoc backfills by passing `--since` and `--until`.

## Layout

| Path | What it does |
| --- | --- |
| `tailor_made_dlt_sources.strava` | Shared Strava OAuth client and `@dlt.source`. |
| `strava_dlt/pipeline.py` | Runs one dlt pipeline per destination and persists destination-backed `seen_ids`. |
| `modal_app.py` | Modal app, cron entry point, and local CLI wrapper. |
| `scripts/authorize.py` | One-time helper to mint a Strava refresh token. |
| `tests/` | Pipeline and integration-style tests using stubbed Strava HTTP calls. |
