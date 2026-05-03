import logging
import time

import requests

logger = logging.getLogger(__name__)

STRAVA_API_BASE = "https://www.strava.com/api/v3"
STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"


class StravaRateLimitExceeded(requests.HTTPError):
    """Raised when 429 retries are exhausted. Source layer catches this and
    ends its generator cleanly so dlt loads whatever was extracted, while
    `run_pipelines` still raises after the load to flag the run as failed."""


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
        logger.info("Refreshing Strava access token")
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
        if not resp.ok:
            _raise_with_body(resp, "/oauth/token")
        body = resp.json()
        self._access_token = body["access_token"]
        self._expires_at = float(body["expires_at"])
        logger.info("Refreshed Strava access token expires_at=%s", self._expires_at)
        if (new_rt := body.get("refresh_token")) and new_rt != self._refresh_token:
            logger.warning("Strava rotated the refresh token — update STRAVA_REFRESH_TOKEN.")
            self._refresh_token = new_rt
        return self._access_token

    def get(self, path: str, params: dict | None = None) -> dict | list:
        # 3 attempts → backoff ladder 60s + 120s + 240s = 7 min worst case before
        # giving up. Strava's daily quota only resets at midnight UTC, so longer
        # waits aren't useful — bank what we have and let the next run retry.
        url = f"{STRAVA_API_BASE}{path}"
        resp = None
        for attempt in range(1, 4):
            headers = {"Authorization": f"Bearer {self._access()}"}
            logger.info("Strava GET %s params=%s attempt=%s", path, params or {}, attempt)
            resp = self._session.get(url, headers=headers, params=params, timeout=60)
            logger.info(
                "Strava GET %s params=%s attempt=%s status=%s rate_limit=%s",
                path,
                params or {},
                attempt,
                resp.status_code,
                _rate_limit_headers(resp),
            )

            if resp.status_code == 401 and attempt == 1:
                logger.warning("Strava 401 on %s — refreshing token and retrying", path)
                self._access_token = None  # force re-mint
                continue
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                wait = 60 * 2 ** (attempt - 1)
                logger.warning(
                    "Strava %d on %s params=%s — sleeping %ds",
                    resp.status_code,
                    path,
                    params or {},
                    wait,
                )
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                _raise_with_body(resp, path)
            return resp.json()
        # Exhausted retries. 429 gets a typed exception so the source can catch
        # it and end its generator cleanly; 5xx still raises generic HTTPError.
        if resp is not None and resp.status_code == 429:
            body = resp.text[:500] if resp.text else "<empty body>"
            raise StravaRateLimitExceeded(
                f"Strava 429 on {path} after retries: {body}", response=resp
            )
        _raise_with_body(resp, path)


def _rate_limit_headers(resp: requests.Response) -> str:
    limit = resp.headers.get("X-RateLimit-Limit")
    usage = resp.headers.get("X-RateLimit-Usage")
    if limit or usage:
        return f"usage={usage or '?'} limit={limit or '?'}"
    return "unavailable"


def _raise_with_body(resp: requests.Response, path: str) -> None:
    """Raise HTTPError with Strava's response body included. The default
    `raise_for_status` message hides the body, which often carries the actual
    diagnosis (e.g. `{"errors":[{"resource":"AccessToken","field":"scope"}]}`)."""
    body = resp.text[:500] if resp.text else "<empty body>"
    raise requests.HTTPError(
        f"Strava {resp.status_code} on {path}: {body}",
        response=resp,
    )
