import logging
import threading
import time

import requests

logger = logging.getLogger(__name__)

STRAVA_API_BASE = "https://www.strava.com/api/v3"
STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"


class StravaAuth:
    """Holds Strava OAuth state and mints short-lived access tokens.

    Strava sometimes rotates the refresh token on use. We don't try to
    persist the new value back to the Modal Secret — we log a WARNING
    so the operator can rotate it manually.
    """

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        if not (client_id and client_secret and refresh_token):
            raise ValueError("Strava client_id, client_secret, and refresh_token are required.")
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._access_token: str | None = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def _refresh(self) -> None:
        resp = requests.post(
            STRAVA_TOKEN_URL,
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
            },
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        self._access_token = body["access_token"]
        self._expires_at = float(body["expires_at"])
        new_refresh = body.get("refresh_token")
        if new_refresh and new_refresh != self._refresh_token:
            logger.warning(
                "Strava rotated the refresh token. Update the STRAVA_REFRESH_TOKEN "
                "secret (Modal Secret 'strava-oauth' or .dlt/secrets.toml) with the "
                "new value to avoid a future auth failure. New token starts with %s...",
                new_refresh[:6],
            )
            self._refresh_token = new_refresh

    def access_token(self) -> str:
        with self._lock:
            if not self._access_token or time.time() >= self._expires_at - 60:
                self._refresh()
            assert self._access_token is not None
            return self._access_token


class StravaClient:
    """Thin GET-only client with auth, retry, and rate-limit handling."""

    def __init__(self, auth: StravaAuth, session: requests.Session | None = None):
        self._auth = auth
        self._session = session or requests.Session()

    def get(self, path: str, params: dict | None = None) -> dict | list:
        url = f"{STRAVA_API_BASE}{path}"
        attempt = 0
        while True:
            attempt += 1
            headers = {"Authorization": f"Bearer {self._auth.access_token()}"}
            resp = self._session.get(url, headers=headers, params=params, timeout=60)

            if resp.status_code == 401 and attempt == 1:
                # Token may have been invalidated mid-flight — force a refresh and retry once.
                self._auth._access_token = None  # noqa: SLF001
                continue

            if resp.status_code == 429:
                # Strava 15-min and daily windows. No Retry-After; back off and try later.
                wait = min(60 * 2 ** (attempt - 1), 900)
                logger.warning("Strava 429 on %s — sleeping %ss before retry", path, wait)
                time.sleep(wait)
                if attempt >= 5:
                    resp.raise_for_status()
                continue

            if 500 <= resp.status_code < 600 and attempt < 5:
                wait = min(2 ** attempt, 30)
                logger.warning(
                    "Strava %d on %s — sleeping %ss before retry", resp.status_code, path, wait
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()
