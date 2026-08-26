"""
Upstox market-data provider for NiftySignals.

Replaces the NSE website scrape (live quotes) and optionally yfinance (history).
Credentials come from the environment only — never hardcode them:

    UPSTOX_API_KEY        client id from the Upstox developer console
    UPSTOX_API_SECRET     client secret          (only needed to mint a token)
    UPSTOX_ACCESS_TOKEN   daily token            (required at runtime)
    UPSTOX_REDIRECT_URI   registered redirect    (only needed to mint a token)

IMPORTANT — token lifetime. Upstox access tokens expire every day at ~03:30 IST.
There is no silent refresh in the standard OAuth flow: minting a new one requires an
interactive login. Everything here therefore treats an expired token as a normal,
expected condition — it degrades to the fallback provider rather than going dark.
See get_token.py and the README section on keeping the token fresh.
"""
import os, gzip, json, time, logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
import pandas as pd
import pytz

log = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

API_BASE = os.getenv("UPSTOX_API_BASE", "https://api.upstox.com/v2")
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
INSTRUMENTS_CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instruments_nse.json")
INSTRUMENTS_MAX_AGE_H = 20  # refresh daily; NSE adds/removes listings

# The quotes endpoint accepts a batch of instrument keys. Upstox documents a ceiling
# (500 at time of writing) but it has moved between API versions — keep it low enough
# to be safely under any limit and tune with UPSTOX_QUOTE_BATCH if you confirm higher.
QUOTE_BATCH = int(os.getenv("UPSTOX_QUOTE_BATCH", "100"))
REQUEST_TIMEOUT = int(os.getenv("UPSTOX_TIMEOUT", "20"))
RATE_SLEEP = float(os.getenv("UPSTOX_RATE_SLEEP", "0.25"))  # between batches
# Start warning in the logs this many days before the token lapses.
TOKEN_WARN_DAYS = float(os.getenv("UPSTOX_TOKEN_WARN_DAYS", "7"))

_state = {
    "instruments": None,      # trading_symbol -> instrument_key
    "instruments_ts": 0.0,
    "indices": None,          # index name -> instrument_key
    "token_valid": None,      # None = untested, True/False after first call
    "last_error": None,
    "last_ok": None,
}


# --------------------------------------------------------------------------
# credentials
# --------------------------------------------------------------------------
def decode_token_claims(token: Optional[str] = None) -> Optional[dict]:
    """Read the JWT payload without verifying it.

    We are not authenticating anything here — the server already does that. We only
    want the `exp` claim so the app can state its own token lifetime as a fact instead
    of assuming one. No secret is needed: a JWT payload is plain base64url.
    """
    import base64
    tok = (token or access_token() or "").strip()
    if tok.count(".") < 2:
        return None
    try:
        payload = tok.split(".")[1]
        payload += "=" * (-len(payload) % 4)  # restore stripped padding
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception as e:
        log.warning(f"could not decode token payload: {e}")
        return None


def token_expiry_info(token: Optional[str] = None) -> dict:
    """When does this token die, and how long have we got?

    Returns {"exp": iso|None, "seconds_remaining": int|None, "days_remaining": float|None,
             "expired": bool|None, "long_lived": bool|None, "claims": {...}}
    long_lived is True when the token outlives a single trading day, i.e. this is an
    extended/algo-trading token rather than one that needs minting every morning.
    """
    claims = decode_token_claims(token)
    out = {"exp": None, "seconds_remaining": None, "days_remaining": None,
           "expired": None, "long_lived": None, "claims": {}}
    if not claims:
        return out

    # Surface only non-sensitive claims.
    out["claims"] = {k: v for k, v in claims.items()
                     if k in ("sub", "iat", "exp", "iss", "aud", "user_type", "scope")}

    exp = claims.get("exp")
    if not exp:
        return out
    try:
        exp = int(exp)
    except (TypeError, ValueError):
        return out
    if exp > 1e11:      # some issuers stamp milliseconds
        exp //= 1000

    exp_dt = datetime.fromtimestamp(exp, tz=pytz.UTC).astimezone(IST)
    remaining = (exp_dt - datetime.now(IST)).total_seconds()
    out["exp"] = exp_dt.strftime("%Y-%m-%d %H:%M:%S IST")
    out["seconds_remaining"] = int(remaining)
    out["days_remaining"] = round(remaining / 86400, 2)
    out["expired"] = remaining <= 0
    out["long_lived"] = remaining > 36 * 3600   # survives past tomorrow morning
    return out


def access_token() -> Optional[str]:
    t = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
    return t or None


def is_configured() -> bool:
    return bool(access_token())


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token()}",
        "Accept": "application/json",
    }


def status() -> Dict:
    """Health snapshot for /api/health."""
    exp = token_expiry_info()
    return {
        "configured": is_configured(),
        "token_valid": _state["token_valid"],
        "token_expires": exp["exp"],
        "token_days_remaining": exp["days_remaining"],
        "token_long_lived": exp["long_lived"],
        "token_expired": exp["expired"],
        "last_ok": _state["last_ok"],
        "last_error": _state["last_error"],
        "instruments_loaded": len(_state["instruments"] or {}),
        "api_base": API_BASE,
    }


# --------------------------------------------------------------------------
# instrument master  (public file — no auth needed)
# --------------------------------------------------------------------------
def _download_instruments() -> List[dict]:
    r = requests.get(INSTRUMENTS_URL, timeout=90)
    r.raise_for_status()
    try:
        rows = json.loads(gzip.decompress(r.content))
    except (OSError, gzip.BadGzipFile):
        rows = r.json()  # already decompressed by the transport
    return rows


def load_instruments(force: bool = False) -> Dict[str, str]:
    """trading_symbol -> instrument_key for NSE cash equities.

    Cached on disk so a restart doesn't re-download 2 MB. Upstox keys are ISIN-based
    (NSE_EQ|INE002A01018), so they survive ticker renames — one of the reasons this is
    sturdier than symbol-string lookups.
    """
    age_ok = (time.time() - _state["instruments_ts"]) < INSTRUMENTS_MAX_AGE_H * 3600
    if _state["instruments"] and age_ok and not force:
        return _state["instruments"]

    rows = None
    if not force and os.path.exists(INSTRUMENTS_CACHE):
        age_h = (time.time() - os.path.getmtime(INSTRUMENTS_CACHE)) / 3600
        if age_h < INSTRUMENTS_MAX_AGE_H:
            try:
                rows = json.load(open(INSTRUMENTS_CACHE))
                log.info(f"Upstox instruments from cache ({age_h:.1f}h old)")
            except Exception:
                rows = None

    if rows is None:
        try:
            rows = _download_instruments()
            try:
                json.dump(rows, open(INSTRUMENTS_CACHE, "w"))
            except Exception as e:
                log.warning(f"instrument cache write failed: {e}")
            log.info(f"Upstox instruments downloaded: {len(rows)} rows")
        except Exception as e:
            log.error(f"instrument master download failed: {e}")
            if _state["instruments"]:
                return _state["instruments"]
            return {}

    eq, idx = {}, {}
    for x in rows:
        seg = x.get("segment")
        key = x.get("instrument_key")
        if not key:
            continue
        if seg == "NSE_EQ" and x.get("instrument_type") == "EQ":
            ts = (x.get("trading_symbol") or "").strip().upper()
            if ts:
                eq[ts] = key
        elif seg == "NSE_INDEX":
            nm = (x.get("name") or "").strip()
            if nm:
                idx[nm.lower()] = key

    _state["instruments"] = eq
    _state["indices"] = idx
    _state["instruments_ts"] = time.time()
    log.info(f"Upstox: {len(eq)} NSE equities, {len(idx)} indices mapped")
    return eq


def instrument_key(symbol: str) -> Optional[str]:
    return load_instruments().get(symbol.strip().upper())


def index_key(name: str) -> Optional[str]:
    load_instruments()
    return (_state["indices"] or {}).get(name.strip().lower())


def map_symbols(symbols: List[str]):
    """Split a symbol list into (key->symbol map, unmapped symbols)."""
    inst = load_instruments()
    key_to_sym, missing = {}, []
    for s in symbols:
        k = inst.get(s.strip().upper())
        if k:
            key_to_sym[k] = s
        else:
            missing.append(s)
    return key_to_sym, missing


# --------------------------------------------------------------------------
# request helper
# --------------------------------------------------------------------------
class UpstoxAuthError(RuntimeError):
    """Token missing, expired, or rejected."""


def _get(path: str, params: Optional[dict] = None) -> dict:
    if not is_configured():
        raise UpstoxAuthError("UPSTOX_ACCESS_TOKEN not set")
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers=_headers(), params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code in (401, 403):
        _state["token_valid"] = False
        _state["last_error"] = f"{r.status_code} {r.text[:200]}"
        raise UpstoxAuthError(
            f"Upstox rejected the token ({r.status_code}). Access tokens expire daily "
            f"at ~03:30 IST — mint a fresh one. Detail: {r.text[:200]}"
        )
    if r.status_code == 429:
        _state["last_error"] = "rate limited (429)"
        raise RuntimeError("Upstox rate limit hit (429)")
    r.raise_for_status()
    _state["token_valid"] = True
    _state["last_ok"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    return r.json()


def _num(x):
    try:
        if x is None:
            return None
        v = float(x)
        return None if pd.isna(v) else v
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# live quotes
# --------------------------------------------------------------------------
def get_quotes(symbols: List[str]) -> Dict[str, dict]:
    """{symbol: {"ltp": float, "volume": float|None, "open/high/low/prev_close": ...}}

    `volume` is the day's cumulative traded quantity — exactly what the volume gate
    needs, and the field the old NSE scrape was throwing away.
    """
    key_to_sym, missing = map_symbols(symbols)
    if missing:
        log.warning(f"Upstox: {len(missing)} symbols unmapped, e.g. {missing[:5]}")
    if not key_to_sym:
        return {}

    keys = list(key_to_sym.keys())
    out: Dict[str, dict] = {}

    for i in range(0, len(keys), QUOTE_BATCH):
        chunk = keys[i:i + QUOTE_BATCH]
        try:
            payload = _get("/market-quote/quotes", {"instrument_key": ",".join(chunk)})
        except UpstoxAuthError:
            raise
        except Exception as e:
            log.warning(f"quotes batch {i // QUOTE_BATCH + 1}: {e}")
            continue

        data = payload.get("data") or {}
        for resp_key, q in data.items():
            if not isinstance(q, dict):
                continue
            # Upstox echoes results under a different key shape than the request
            # (e.g. "NSE_EQ:RELIANCE" for a requested "NSE_EQ|INE002A01018"), and this
            # has changed between versions. Resolve via the payload's own
            # instrument_token first, then fall back to the trailing symbol.
            sym = None
            tok = q.get("instrument_token") or q.get("instrument_key")
            if tok and tok in key_to_sym:
                sym = key_to_sym[tok]
            else:
                tail = resp_key.split(":")[-1].split("|")[-1].strip().upper()
                if resp_key in key_to_sym:
                    sym = key_to_sym[resp_key]
                elif tail in {s.upper() for s in symbols}:
                    sym = tail
            if not sym:
                continue

            ohlc = q.get("ohlc") or {}
            ltp = _num(q.get("last_price")) or _num(ohlc.get("close"))
            if not ltp:
                continue
            out[sym] = {
                "ltp": ltp,
                "volume": _num(q.get("volume")),
                "open": _num(ohlc.get("open")),
                "high": _num(ohlc.get("high")),
                "low": _num(ohlc.get("low")),
                "prev_close": _num(q.get("prev_close")) or _num(ohlc.get("close")),
            }

        if i + QUOTE_BATCH < len(keys):
            time.sleep(RATE_SLEEP)

    with_vol = sum(1 for v in out.values() if v.get("volume"))
    log.info(f"Upstox: {len(out)} quotes, {with_vol} with volume")
    return out


# --------------------------------------------------------------------------
# historical candles
# --------------------------------------------------------------------------
def get_history(symbol: str, days: int = 120, interval: str = "day") -> Optional[pd.DataFrame]:
    """Daily OHLCV for one symbol, oldest-first, DatetimeIndex.

    Note the historical endpoint excludes the in-progress session; today's bar is
    fetched separately via get_intraday() or supplied live by the quote loop.
    """
    key = instrument_key(symbol)
    if not key:
        return None
    to_d = datetime.now(IST).date()
    from_d = to_d - timedelta(days=int(days * 1.6) + 10)  # pad for weekends/holidays
    try:
        payload = _get(f"/historical-candle/{key}/{interval}/{to_d}/{from_d}")
    except UpstoxAuthError:
        raise
    except Exception as e:
        log.warning(f"history {symbol}: {e}")
        return None
    return _candles_to_df((payload.get("data") or {}).get("candles") or [])


def get_intraday(symbol: str, interval: str = "30minute") -> Optional[pd.DataFrame]:
    """Today's candles for one symbol. Useful for a true intraday volume profile."""
    key = instrument_key(symbol)
    if not key:
        return None
    try:
        payload = _get(f"/historical-candle/intraday/{key}/{interval}")
    except UpstoxAuthError:
        raise
    except Exception as e:
        log.warning(f"intraday {symbol}: {e}")
        return None
    return _candles_to_df((payload.get("data") or {}).get("candles") or [])


def _candles_to_df(candles: List[list]) -> Optional[pd.DataFrame]:
    """Upstox candle rows are [ts, open, high, low, close, volume, oi]."""
    if not candles:
        return None
    rows = []
    for c in candles:
        if len(c) < 6:
            continue
        rows.append({
            "ts": c[0], "Open": _num(c[1]), "High": _num(c[2]),
            "Low": _num(c[3]), "Close": _num(c[4]), "Volume": _num(c[5]) or 0.0,
        })
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts", "Close"])
    if df.empty:
        return None
    df["ts"] = df["ts"].dt.tz_convert(IST).dt.tz_localize(None).dt.normalize()
    df = df.set_index("ts").sort_index()
    df.index.name = None
    return df[~df.index.duplicated(keep="last")]


def check_token() -> bool:
    """Cheap authenticated probe so startup can report token health immediately."""
    if not is_configured():
        _state["token_valid"] = False
        _state["last_error"] = "UPSTOX_ACCESS_TOKEN not set"
        return False
    try:
        key = instrument_key("RELIANCE") or "NSE_EQ|INE002A01018"
        _get("/market-quote/ltp", {"instrument_key": key})
        exp = token_expiry_info()
        if exp["exp"]:
            d = exp["days_remaining"]
            if exp["long_lived"]:
                log.info(f"Upstox token OK — long-lived, expires {exp['exp']} ({d} days). "
                         "No daily refresh needed.")
            else:
                log.warning(f"Upstox token OK but SHORT-LIVED — expires {exp['exp']} "
                            f"({d} days). Plan a daily refresh.")
            if d is not None and d < TOKEN_WARN_DAYS:
                log.warning(f"*** Upstox token expires in {d} days — mint a new one. ***")
        else:
            log.info("Upstox token OK (no readable exp claim)")
        return True
    except UpstoxAuthError as e:
        log.error(f"Upstox token check failed: {e}")
        return False
    except Exception as e:
        # Network blip — don't condemn the token over it.
        log.warning(f"Upstox token check inconclusive: {e}")
        _state["last_error"] = str(e)[:200]
        return bool(_state["token_valid"])
