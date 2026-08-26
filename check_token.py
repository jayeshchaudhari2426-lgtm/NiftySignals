#!/usr/bin/env python3
"""
Inspect an Upstox access token — answers "do I need to refresh this daily?"

    export UPSTOX_ACCESS_TOKEN='eyJ0eXAiOiJKV1Q...'
    python check_token.py

Reads the `exp` claim out of the JWT locally. Nothing is sent anywhere and no secret
is required — a JWT payload is plain base64url, which is exactly why you should never
paste a token into a chat window, an issue tracker, or a screenshot.

Add --probe to also make one live API call and confirm the server agrees.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import upstox  # noqa: E402


def main():
    token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
    if not token:
        sys.exit("Set UPSTOX_ACCESS_TOKEN first.")

    print(f"\ntoken: {token[:14]}...{token[-6:]}  ({len(token)} chars)")

    info = upstox.token_expiry_info(token)
    if not info["exp"]:
        print("\nNo readable `exp` claim.")
        print("Either this isn't a JWT or it's opaque — fall back to the empirical test:")
        print("run this again after 03:30 IST tomorrow and see whether --probe still passes.")
    else:
        print(f"expires:   {info['exp']}")
        print(f"remaining: {info['days_remaining']} days")

        if info["expired"]:
            print("\n>>> ALREADY EXPIRED — mint a new one with get_token.py.")
        elif info["long_lived"]:
            print("\n>>> LONG-LIVED. It outlives tomorrow morning, so this is an")
            print("    extended / algo-trading token. No daily refresh needed.")
            print("    Set a calendar reminder a week before the date above.")
        else:
            print("\n>>> SHORT-LIVED — dies at the next daily rollover (~03:30 IST).")
            print("    You'll need a refresh strategy; see the README.")

    if info["claims"]:
        print("\nclaims:")
        for k, v in sorted(info["claims"].items()):
            print(f"  {k}: {v}")

    if "--probe" in sys.argv:
        print("\nprobing the live API...")
        try:
            ok = upstox.check_token()
            print("  server accepts the token" if ok else "  server REJECTED the token")
        except Exception as e:
            print(f"  probe failed: {e}")


if __name__ == "__main__":
    main()
