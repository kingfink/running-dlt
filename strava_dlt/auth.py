import logging
import time

import requests

logger = logging.getLogger(__name__)

STRAVA_API_BASE = "https://www.strava.com/api/v3"
STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"


class StravaClient:
    """GET-only Strava REST client. Refreshes the access token on demand and
    retries 401 (once), 429 (rate limit), and 5xx with exponential backoff.

    Strava sometimes rotates the refresh token on use. We log a WARNING when
    that happens — update the Modal Secret / .dlt/secrets.toml manually.
    """

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        if not (client_id and client_secret and refresh_token):
            raise ValueError("client_id, client_secret, and refresh_token are required.")
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._access_token: str | None = None
        self._expires_at: float = 0.0
        self._session = requests.Session()

    def _access(self) -> str:
        if self._access_token and time.time() < self._expires_at - 60:
            return self._access_token
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
        if (new_rt := body.get("refresh_token")) and new_rt != self._refresh_token:
            logger.warning("Strava rotated the refresh token — update STRAVA_REFRESH_TOKEN.")
            self._refresh_token = new_rt
        return self._access_token

    def get(self, path: str, params: dict | None = None) -> dict | list:
        url = f"{STRAVA_API_BASE}{path}"
        for attempt in range(1, 6):
            headers = {"Authorization": f"Bearer {self._access()}"}
            resp = self._session.get(url, headers=headers, params=params, timeout=60)

            if resp.status_code == 401 and attempt == 1:
                self._access_token = None  # force re-mint
                continue
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                wait = min(60 * 2 ** (attempt - 1), 900)
                logger.warning("Strava %d on %s — sleeping %ds", resp.status_code, path, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        return resp.json()
