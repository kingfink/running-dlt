"""One-time helper to mint a Strava refresh token.

Walks you through the OAuth authorization-code flow:

    1. Print the authorize URL.
    2. You open it in a browser, click Authorize, get redirected to a localhost
       URL Strava can't actually reach. Copy the `code=...` query param.
    3. Paste it back here. We exchange it for a refresh token and print it.

Reads client_id and client_secret from .dlt/secrets.toml or env vars
(STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET).
"""

from __future__ import annotations

import os
import sys
import tomllib
import urllib.parse
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
SECRETS_FILE = REPO_ROOT / ".dlt" / "secrets.toml"
AUTHORIZE_URL = "https://www.strava.com/oauth/authorize"
TOKEN_URL = "https://www.strava.com/oauth/token"
REDIRECT_URI = "http://localhost"
SCOPE = "read,activity:read_all"


def load_creds() -> tuple[str, str]:
    client_id = os.environ.get("STRAVA_CLIENT_ID")
    client_secret = os.environ.get("STRAVA_CLIENT_SECRET")
    if client_id and client_secret:
        return client_id, client_secret

    if not SECRETS_FILE.exists():
        sys.exit(
            f"No env vars and {SECRETS_FILE} not found.\n"
            f"Copy .dlt/secrets.toml.example to .dlt/secrets.toml and fill in "
            f"client_id + client_secret first."
        )
    data = tomllib.loads(SECRETS_FILE.read_text())
    strava = data.get("sources", {}).get("strava", {})
    client_id = strava.get("client_id")
    client_secret = strava.get("client_secret")
    if not client_id or not client_secret:
        sys.exit(f"{SECRETS_FILE} is missing [sources.strava] client_id and/or client_secret.")
    return client_id, client_secret


def main() -> None:
    client_id, client_secret = load_creds()

    auth_url = f"{AUTHORIZE_URL}?" + urllib.parse.urlencode(
        {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
            "approval_prompt": "auto",
            "scope": SCOPE,
        }
    )

    print("Step 1: open this URL in a browser and click Authorize:\n")
    print(f"    {auth_url}\n")
    print("Step 2: after authorizing, your browser will be redirected to")
    print("        http://localhost/?state=&code=<LONG_HEX>&scope=read,activity:read_all")
    print("        (the page will fail to load — that's expected).")
    print("Step 3: copy the value of the `code` parameter and paste it here.\n")

    code = input("code> ").strip()
    if not code:
        sys.exit("No code provided.")

    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    if not resp.ok:
        sys.exit(f"Token exchange failed: {resp.status_code} {resp.text}")
    body = resp.json()

    refresh_token = body.get("refresh_token")
    if not refresh_token:
        sys.exit(f"No refresh_token in response: {body}")

    print("\nSuccess. Your refresh_token is:\n")
    print(f"    {refresh_token}\n")
    print("Next steps:")
    print(f"  Local:   add it to {SECRETS_FILE} under [sources.strava] refresh_token = '...'")
    print("  Modal:   set STRAVA_REFRESH_TOKEN in the 'strava-oauth' Modal Secret.")


if __name__ == "__main__":
    main()
