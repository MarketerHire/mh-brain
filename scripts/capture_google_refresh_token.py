#!/usr/bin/env python3
"""Capture an OAuth refresh token for Google APIs (GSC + YouTube Analytics).

Use case
--------
Some clients can only grant our `ai@marketerhire.com` account User-level (GSC)
or Editor-level (YouTube) access — Fivetran requires Owner / Manager. The
mh-brain `gsc.py` and `youtube_analytics.py` adapters work fine with
User/Editor tokens, but we need to capture a refresh token outside the
Fivetran flow.

This script runs the standard Google OAuth 2.0 installed-app flow against
`ai@marketerhire.com`, prints the refresh token, and (optionally) writes it
to the client's datasources.json or to a Modal secret.

Required scopes
---------------
- https://www.googleapis.com/auth/webmasters.readonly      (GSC)
- https://www.googleapis.com/auth/yt-analytics.readonly    (YouTube Analytics)
- https://www.googleapis.com/auth/youtube.readonly         (YouTube channel listing)

Usage
-----
    # 1. Set OAuth client (reuse Google Ads OAuth client; scopes are additive)
    export GOOGLE_OAUTH_CLIENT_ID="..."        # from GCP console; same as GOOGLE_ADS_CLIENT_ID is fine
    export GOOGLE_OAUTH_CLIENT_SECRET="..."

    # 2. Run the script with the desired scopes
    python scripts/capture_google_refresh_token.py --scopes gsc,youtube

    # 3. The script prints a URL. Open it in a browser logged in as
    #    ai@marketerhire.com, click Allow, then paste the resulting code
    #    back into the terminal.

    # 4. Output: refresh_token, plus the YouTube channel_id if youtube scope
    #    was included.

    # 5. Write the token into one of:
    #    a) Modal secret on mh-brain workspace:
    #         GSC_REFRESH_TOKEN  (works for all clients sharing this OAuth principal)
    #         YT_REFRESH_TOKEN
    #    b) Per-client in mh1-hq/clients/{slug}/config/datasources.json:
    #         integrations.gsc.refresh_token = "..."
    #         integrations.youtube_analytics.refresh_token = "..."

Constraints
-----------
- Run on a workstation with a browser. The OAuth flow opens a redirect to
  http://localhost:8765 by default.
- The OAuth client (CLIENT_ID/CLIENT_SECRET) must have these scopes enabled
  in the GCP console. If your Google Ads OAuth client doesn't include
  webmasters / yt-analytics scopes, create a new "Desktop app" client.
- Refresh tokens issued for a Google Cloud Project that is in "Testing" mode
  expire after 7 days. Move the OAuth consent screen to "In production"
  for stable long-lived tokens.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import secrets
import socketserver
import sys
import threading
import urllib.parse
import urllib.request
import webbrowser
from typing import Optional

SCOPES = {
    "gsc": "https://www.googleapis.com/auth/webmasters.readonly",
    "youtube": "https://www.googleapis.com/auth/yt-analytics.readonly https://www.googleapis.com/auth/youtube.readonly",
}

REDIRECT_PORT = int(os.environ.get("OAUTH_CALLBACK_PORT", "8765"))
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}"
TOKEN_URL = "https://oauth2.googleapis.com/token"
AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"

_received_code: Optional[str] = None
_received_state: Optional[str] = None


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        global _received_code, _received_state
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        _received_code = params.get("code", [None])[0]
        _received_state = params.get("state", [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        if _received_code:
            self.wfile.write(b"<h2>OK</h2><p>You can close this tab.</p>")
        else:
            self.wfile.write(b"<h2>Error</h2><p>No code returned.</p>")

    def log_message(self, *_args, **_kwargs):  # silence
        return


def run_oauth_flow(client_id: str, client_secret: str, scopes: str) -> dict:
    state = secrets.token_urlsafe(16)
    auth_params = {
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": scopes,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    url = f"{AUTH_URL}?{urllib.parse.urlencode(auth_params)}"
    print(f"\n[1/3] Open this URL in a browser logged in as ai@marketerhire.com:\n\n{url}\n")
    try:
        webbrowser.open(url)
    except Exception:
        pass

    server = socketserver.TCPServer(("localhost", REDIRECT_PORT), _CallbackHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[2/3] Waiting for OAuth callback on {REDIRECT_URI} ...")
    while _received_code is None:
        pass
    server.shutdown()

    if _received_state != state:
        raise RuntimeError("OAuth state mismatch; aborting.")

    print("[3/3] Exchanging authorization code for refresh token ...")
    body = urllib.parse.urlencode({
        "code": _received_code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }).encode()
    req = urllib.request.Request(
        TOKEN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_youtube_channel_id(access_token: str) -> Optional[str]:
    """Fetch the channel_id of the authenticated user's primary YouTube channel."""
    req = urllib.request.Request(
        "https://www.googleapis.com/youtube/v3/channels?part=id&mine=true",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        items = data.get("items") or []
        return items[0]["id"] if items else None
    except Exception as e:
        print(f"WARN: could not fetch YouTube channel_id: {e}", file=sys.stderr)
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument(
        "--scopes",
        default="gsc,youtube",
        help="Comma-separated scopes to request: gsc,youtube",
    )
    parser.add_argument(
        "--client-slug",
        default=None,
        help="If set, suggest the datasources.json path for the given client.",
    )
    args = parser.parse_args()

    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID") or os.environ.get("GOOGLE_ADS_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET") or os.environ.get("GOOGLE_ADS_CLIENT_SECRET")
    if not client_id or not client_secret:
        print(
            "ERROR: set GOOGLE_OAUTH_CLIENT_ID + GOOGLE_OAUTH_CLIENT_SECRET "
            "(or GOOGLE_ADS_CLIENT_ID/SECRET) before running.",
            file=sys.stderr,
        )
        return 2

    scope_list = [s.strip() for s in args.scopes.split(",") if s.strip()]
    scopes = " ".join(SCOPES[s] for s in scope_list if s in SCOPES)
    if not scopes:
        print(f"ERROR: no valid scopes; choose from: {', '.join(SCOPES)}", file=sys.stderr)
        return 2

    token_response = run_oauth_flow(client_id, client_secret, scopes)
    refresh_token = token_response.get("refresh_token")
    access_token = token_response.get("access_token")

    if not refresh_token:
        print(
            "ERROR: no refresh_token returned. Likely cause: this OAuth client "
            "previously issued a refresh token for these scopes. Revoke the prior "
            "grant at https://myaccount.google.com/permissions and retry.",
            file=sys.stderr,
        )
        return 1

    print("\n" + "=" * 60)
    print("CAPTURED REFRESH TOKEN")
    print("=" * 60)
    print(f"\n  refresh_token: {refresh_token}")
    print(f"  scopes:        {scopes}")

    if "youtube" in scope_list and access_token:
        channel_id = fetch_youtube_channel_id(access_token)
        if channel_id:
            print(f"  channel_id:    {channel_id}")

    print("\n--- WRITE THIS TOKEN INTO ONE OF: ---\n")
    if args.client_slug:
        path = f"/Applications/MH1/mh1-hq/clients/{args.client_slug}/config/datasources.json"
        print(f"  a) Per-client (preferred): {path}")
        print(f"     integrations.gsc.refresh_token = \"{refresh_token}\"")
        print(f"     integrations.youtube_analytics.refresh_token = \"{refresh_token}\"")
    else:
        print("  a) Per-client: mh1-hq/clients/{slug}/config/datasources.json")
        print("     integrations.gsc.refresh_token / integrations.youtube_analytics.refresh_token")
    print("\n  b) Global Modal secret on mh-brain workspace:")
    print("     modal secret create mh-brain-google-oauth GSC_REFRESH_TOKEN=... YT_REFRESH_TOKEN=...")
    print("\nDO NOT commit the refresh token to git. Treat it like a password.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
