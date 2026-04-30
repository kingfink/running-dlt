# Strava API app setup

You need to register a Strava API application once to get a `client_id` + `client_secret`, then run a one-time OAuth handshake to mint a long-lived `refresh_token`.

## 1. Register an app

1. Sign in at https://www.strava.com.
2. Go to https://www.strava.com/settings/api.
3. Fill in:
   - **Application Name:** anything (e.g., `running-dlt`).
   - **Category:** Data Importer.
   - **Website:** any URL you control (a GitHub repo URL is fine).
   - **Authorization Callback Domain:** `localhost`.
4. Save. Copy the **Client ID** and **Client Secret**.

## 2. Drop creds into `.dlt/secrets.toml`

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
```

Fill in `[sources.strava]` `client_id` and `client_secret`. Leave `refresh_token` as the placeholder for now.

## 3. Mint a refresh token

```bash
uv run python scripts/authorize.py
```

The script prints a Strava authorize URL. Open it in a browser, click **Authorize**, and your browser will be redirected to a `http://localhost/?...&code=<LONG_HEX>&...` URL that fails to load — that's expected. Copy the `code` value out of the address bar and paste it back into the script prompt.

The script exchanges the code for a refresh token and prints it. Copy that into `.dlt/secrets.toml` under `[sources.strava]` `refresh_token`.

## 4. Verify

```bash
uv run python scripts/run_local.py --only motherduck --refetch-days 1
```

If your refresh token works, you'll see dlt log a successful pipeline run.

## Scopes

The script requests `read,activity:read_all`. `activity:read_all` is required to read your private activities; downgrade to `activity:read` if you only want public activities.

## Refresh token rotation

Strava can rotate the refresh token when it's used. The pipeline's auth layer logs a `WARNING` if this happens — when you see one, update the secret with the new value (see the warning's instructions). In practice this is rare.
