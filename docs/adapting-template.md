# Adapting this template

Most of the scaffolding here is portable to any REST API source. The Modal layout, the prod/dev split, the two-pipelines-one-source pattern, and the merge-on-id idempotency story all carry over unchanged. What you actually need to rewrite is small.

## Files you rewrite

| File | What changes |
| --- | --- |
| `strava_dlt/auth.py` | Replace with whatever the new API needs — API key, OAuth2 client-credentials, signed JWT, etc. |
| `strava_dlt/source.py` | Different endpoints, different incremental column, different JSON fields. |
| `.dlt/secrets.toml.example` | New `[sources.<name>]` block. |
| `docs/strava-app-setup.md` | Replace with the new API's app-registration walkthrough. |

## Files you rename but keep the structure

| File | What changes |
| --- | --- |
| `strava_dlt/` package directory | Rename to `<source>_dlt/`. |
| `strava_dlt/pipeline.py` | Replace `strava_source` import; the destination loop is generic. |
| `modal_app.py` | Rename `app = modal.App("strava-dlt")` and the function names; the schedule, secrets list, and entrypoint structure stay. |
| Modal Secret names (`strava-oauth`, `dlt-bigquery`, `dlt-motherduck`) | Pick names that match the new source. |

## Files you mostly leave alone

- `strava_dlt/config.py` — change the default dataset name from `strava` to your source's name.
- `pyproject.toml` — change `name`, `description`, the package name in `[tool.hatch.build.targets.wheel]`. Add/remove dlt destination extras if you want a different mix.
- `scripts/run_local.py` — generic; updates only if your source doesn't have a `--only` per-destination concept.

## Patterns worth preserving

- **`@dlt.source(max_table_nesting=0)`** plus per-column `{"data_type": "json"}` hints. This is opinionated — the alternative (default flattening into child tables) is also fine, but mixing the two within one source is confusing.
- **`selected=False` parent + transformer for child detail.** Use this any time the list endpoint's payload is a strict subset of the detail endpoint's. Otherwise prefer two top-level resources.
- **Trailing-N-day refetch via manual `dlt.current.resource_state()`.** The built-in `dlt.sources.incremental` is great for forward-only state but doesn't have a clean lookback-window primitive. Manual state is ~10 lines and easy to reason about.
- **Two pipelines for two destinations, source extracted twice.** Simpler than a `filesystem` staging layer, fine for small data. Swap to a staging layer when extract cost dominates.

## Patterns worth re-evaluating

- **Daily 06:00 UTC cron.** Pick a cadence that suits the API's rate limit and your data's freshness needs.
- **JSON columns vs child tables.** Default dlt flattens; we override. For analytics-heavy sources where you'll mostly query the nested arrays, child tables may be the better default.
- **`refetch_days=7` window.** Strava-specific reasoning. For an immutable event stream you can drop this to 0 and use `write_disposition="append"`.
