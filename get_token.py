#!/usr/bin/env python3
"""
Mint an Upstox access token.

Upstox tokens die every day at ~03:30 IST and the standard OAuth flow has no silent
refresh — the authorization step needs a real browser login with your 2FA. So this is
a once-a-day interactive step, not something the server can do for itself.

    export UPSTOX_API_KEY=...
    export UPSTOX_API_SECRET=...
    export UPSTOX_REDIRECT_URI=https://your-app/callback   # must match the console
    python get_token.py

Prints the login URL, takes the `code` from the redirect, exchanges it, and prints the
access token. Put that in UPSTOX_ACCESS_TOKEN wherever the app runs.
"""
import os, sys, webbrowser
from urllib.parse import urlencode, urlparse, parse_qs

import requests

AUTH_URL = "https://api.upstox.com/v2/login/authorization/dialog"
TOKEN_URL = "https://api.upstox.com/v2/login/authorization/token"


def main():
    key = os.getenv("UPSTOX_API_KEY", "").strip()
    secret = os.getenv("UPSTOX_API_SECRET", "").strip()
    redirect = os.getenv("UPSTOX_REDIRECT_URI", "").strip()

    missing = [n for n, v in [("UPSTOX_API_KEY", key),
                              ("UPSTOX_API_SECRET", secret),
                              ("UPSTOX_REDIRECT_URI", redirect)] if not v]
    if missing:
        sys.exit("Missing env vars: " + ", ".join(missing))

    qs = urlencode({"client_id": key, "redirect_uri": redirect, "response_type": "code"})
    login_url = f"{AUTH_URL}?{qs}"

    print("\n1. Open this URL and log in:\n")
    print("   " + login_url + "\n")
    try:
        webbrowser.open(login_url)
    except Exception:
        pass

    print("2. After login you land on your redirect URI with ?code=... in the address bar.")
    raw = input("\nPaste the full redirected URL (or just the code): ").strip()

    code = raw
    if raw.startswith("http"):
        code = (parse_qs(urlparse(raw).query).get("code") or [""])[0]
    if not code:
        sys.exit("No authorization code found in that input.")

    resp = requests.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Accept": "application/json"},
        data={"code": code, "client_id": key, "client_secret": secret,
              "redirect_uri": redirect, "grant_type": "authorization_code"},
        timeout=30,
    )

    if resp.status_code != 200:
        sys.exit(f"Token exchange failed [{resp.status_code}]: {resp.text[:400]}")

    body = resp.json()
    token = body.get("access_token")
    if not token:
        sys.exit(f"No access_token in response: {body}")

    print("\n" + "=" * 62)
    print("ACCESS TOKEN (valid until ~03:30 IST tomorrow)")
    print("=" * 62)
    print(token)
    print("=" * 62)
    for f in ("user_id", "user_name", "email"):
        if body.get(f):
            print(f"{f}: {body[f]}")
    print("\nSet it where the app runs, e.g.:")
    print(f'  export UPSTOX_ACCESS_TOKEN="{token[:18]}..."')
    print("\nOn Render: Environment -> UPSTOX_ACCESS_TOKEN -> Save (triggers a redeploy).")


if __name__ == "__main__":
    main()
