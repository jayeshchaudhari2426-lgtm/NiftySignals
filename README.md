# NiftySignals

Nifty technical-signal dashboard. FastAPI backend (Render) + static HTML frontend (Vercel).

Live: https://nifty-signals.vercel.app/

## Signal Log tab

Sidebar tab **📜 Signal Log**.

- **This Week** — every BUY/SELL fired in the trailing 7 days
- **Archive** — everything older, in the same tab behind a toggle
- Buy-only / Sell-only filters, stock search, live "since signal" returns
- Backed by `GET /api/signal-log?days=7` (the SQLite `signal_history` table, split at the cutoff)

## Email — handled entirely outside this repo

The backend sends no email and holds no credentials. It exposes signals as JSON and an
external scheduler does the reading and mailing.

### `GET /api/digest-payload`

| Query | Window | Use |
|---|---|---|
| `?session=latest` | the last completed session the strategy evaluated | **the daily mail — use this** |
| `?days=1` (default) | whole calendar day | only useful if signals are stamped today |
| `?days=N` | N calendar days back (max 31) | Monday catch-up if a day was missed |
| `?minutes=N` | rolling N minutes (max 1440) | intraday polling between scheduler runs |
| `?since=YYYY-MM-DD HH:MM:SS` | everything after that IST timestamp | precise resume |

Response fields worth knowing:

- `empty` — true when nothing fired; the scheduler should send no mail at all
- `subject_hint` — ready-made subject line
- `count` / `buy_count` / `sell_count`
- `signals[]` — symbol, name, sector, signal, signal_price, current_price, pct_change, timestamp
- `source` on each signal — `db` or `live` (see below)
- `market_open`, `initialized`, `last_update`, `universe` — health context

### Two scheduled tasks

**Daily calls** — weekdays ~09:20 IST, after the 09:00 reload has finished:

> Fetch https://niftysignals.onrender.com/api/digest-payload?session=latest — it may take up
> to a minute on the first try because the server sleeps when idle, so retry once if it times
> out. If `empty` is true, do nothing and send no email. Otherwise email me the calls as a
> table with columns Stock, Signal, Signal Price, Current, Since Signal, RSI. Use
> `subject_hint` as the subject and put `window.session_date` in the first line. No commentary
> or investment advice.

Why `session=latest` and not `days=1`: indicators run on daily candles, so Friday's 09:00
reload evaluates Thursday's completed bar. Asking for today's date returns nothing.

**Intraday alerts** — weekdays during market hours, at whatever interval you schedule.
Ask for a window slightly wider than the interval so a slow run can't drop a signal
(30-minute schedule → `?minutes=35`). Overlap means an occasional repeat, which is the
safer failure:

> Fetch https://niftysignals.onrender.com/api/digest-payload?minutes=35 — retry once if it
> times out. If `empty` is true, send nothing. Otherwise email me the new signals as a table.
> Use `subject_hint` as the subject. No commentary.

### Timing

Signals fire in two places:

- **09:00 IST reload** — the full 90-day recompute on completed daily candles.
- **The 1-minute loop during market hours** — the live path. It now pulls
  `totalTradedVolume` from NSE alongside the price and writes it into today's bar, so the
  volume gate can actually pass intraday. Previously the live bar was created with
  `Volume: 0` and never updated, which made `Volume > 1.5x VolSMA20` impossible to satisfy —
  every intraday signal was silently dropped.

A stock keeps its BUY/SELL badge until the opposite signal fires, so the dashboard shows the
most recent call within the 90-day lookback.

### The volume gate

`VolSMA20` is the average of the **prior** 20 sessions (`.rolling(20).mean().shift(1)`). It
used to include the current bar in its own benchmark, which inflated the threshold on exactly
the spike days the strategy exists to catch.

Intraday, volume is cumulative-so-far, so comparing it raw against a full-day average would
only ever pass near the close. The live path projects the full-session total using a typical
NSE volume curve (U-shaped — heavy at the open and into the close) and tests the projection.
Actual traded volume stays truthful in `Volume`; only the gate reads the projected
`VolForSignal` column.

| Env var | Default | Meaning |
|---|---|---|
| `VOL_MULTIPLIER` | `1.5` | Volume must exceed this multiple of VolSMA20 |
| `LIVE_VOL_MODE` | `project` | `project` = scale to full session, `raw` = compare cumulative as-is (fires late, never early), `off` = daily candles only |
| `MIN_SESSION_FRACTION` | `0.08` | Suppress the projection for the first ~30 min, when it is noisiest |

Each stock exposes `vol_ratio`, `volume_today`, `volume_projected` and `session_fraction` so
the gate is inspectable from `/api/signals` rather than a black box.

## Data provider — Upstox

Live quotes come from the Upstox API by default (`DATA_PROVIDER=upstox`). This replaces
the NSE website scrape, which was an unauthenticated session against a public page whose
payload shape could change without notice.

### Credentials — environment only, never committed

| Env var | Needed for | Notes |
|---|---|---|
| `UPSTOX_ACCESS_TOKEN` | runtime | **expires daily at ~03:30 IST** |
| `UPSTOX_API_KEY` | minting a token | client id from the developer console |
| `UPSTOX_API_SECRET` | minting a token | keep it secret; rotate if it ever leaks |
| `UPSTOX_REDIRECT_URI` | minting a token | must match the console exactly |

### Check what kind of token you actually have

Don't assume — read it. Upstox tokens are JWTs, so the expiry is in the token itself:

```bash
export UPSTOX_ACCESS_TOKEN='eyJ0eXAiOiJKV1Q...'
python check_token.py            # add --probe to also hit the live API
```

This decodes the `exp` claim locally and tells you whether the token is long-lived or
dies at the next daily rollover. Nothing leaves your machine — which is also the reason
a token should never go into a chat, a ticket, or a screenshot: the payload is plain
base64, readable by anyone who sees it.

`GET /api/health` reports the same thing continuously under `upstox`:
`token_expires`, `token_days_remaining`, `token_long_lived`, `token_expired`. Startup
logs a warning once the token is inside `UPSTOX_TOKEN_WARN_DAYS` (default 7) of lapsing,
so a ten-year token still tells you before it goes.

### Algo Trading apps

An app registered under the **Algo Trading** tab issues a token that persists until you
revoke it, rather than one that dies at ~03:30 IST daily. If `check_token.py` reports
`LONG-LIVED`, the refresh problem below does not apply to you — set a calendar reminder
for the expiry date and move on.

Keep `UPSTOX_FALLBACK=1` anyway. A long-lived token can still stop working: you may hit
Revoke, the app registration itself expires, or the broker invalidates the session. The
fallback costs nothing while the token is healthy.

### The daily token problem (short-lived tokens only)

Upstox access tokens expire every morning and the standard OAuth flow has **no silent
refresh** — reauthorising needs an interactive login with 2FA. A long-running server
cannot do that for itself. Practical options:

1. Run `python get_token.py` each morning and paste the token into Render's environment.
2. Automate the browser login (Playwright + TOTP) on a machine you control, and push the
   token to Render via its API. Effective, but it means storing your login factors.
3. Ask Upstox about an extended/long-lived token for read-only market data if your
   account is eligible.

Whichever you pick, the app treats an expired token as routine rather than fatal: the
quote loop logs it, sets `token_valid: false`, and falls back to the NSE scrape so the
dashboard keeps updating. Set `UPSTOX_FALLBACK=0` if you would rather it go quiet than
serve scraped data. `GET /api/health` reports `data_provider`, `quote_source` (which
source actually served the last cycle) and the full `upstox` status block.

### Instrument keys

Upstox addresses instruments by ISIN-based keys (`NSE_EQ|INE002A01018`), resolved from
the public instrument master and cached in `instruments_nse.json` for 20 hours. All 500
Nifty 500 symbols map, and because the keys are ISIN-based they survive ticker renames.

| Env var | Default | Meaning |
|---|---|---|
| `DATA_PROVIDER` | `upstox` | `upstox` or `nse` |
| `UPSTOX_FALLBACK` | `1` | fall back to NSE/yfinance when Upstox is unavailable |
| `UPSTOX_HISTORY` | `0` | `1` = daily candles from Upstox instead of yfinance |
| `UPSTOX_QUOTE_BATCH` | `100` | instrument keys per quote request |

`UPSTOX_HISTORY=1` is opt-in because the historical endpoint is one request per symbol —
500 requests against a rate limit, versus yfinance's batched download. Anything Upstox
fails to return is filled from yfinance, so the two can run side by side.

## Universe

All 500 Nifty 500 constituents, loaded at startup from `nifty500.csv` (`symbol,name,sector`).
To refresh after an index rebalance, download `ind_nifty500list.csv` from niftyindices.com and
rewrite the three columns. `YF_OVERRIDES` in `main.py` maps any NSE symbol whose Yahoo ticker
differs; it is currently empty because all 500 resolve directly. If the CSV is missing at
deploy time the app boots on a 5-stock fallback rather than crashing.

### Why `source` matters

`days` mode merges SQLite history with the in-memory store, so the digest is still correct
after a restart has wiped `signals.db` — `load_historical_data()` rebuilds that day's signals
in memory from 90-day history, and those come back tagged `live`.

Rolling windows (`minutes` / `since`) are DB-only: they need a fired-at time, and rebuilt
store entries don't have one. So an intraday poll immediately after a restart returns nothing
even if signals fired earlier. The end-of-day digest still catches them.

## Endpoints

- `GET /api/health`
- `GET /api/signals` — full universe with indicators
- `GET /api/signal-log?days=7` — week + archive
- `GET /api/digest-payload` — see table above
- `GET /api/history` — last 500 signals
- `GET /api/indices` — index strip (was documented but never routed; fixed)
- `GET /api/notifications`, `POST /api/notifications/read`
- `GET /api/rsi-screener`
- `GET /api/chart-data/{symbol}/{period}`, `/api/index-history/{symbol}/{period}`, `/api/stock/{symbol}`

## Run

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in UPSTOX_ACCESS_TOKEN
python check_token.py       # confirm the token's lifetime
uvicorn main:app --host 0.0.0.0 --port 8000
```

Every setting has a working default, so the app boots with no configuration — it just
falls back to the NSE scrape for live quotes until `UPSTOX_ACCESS_TOKEN` is set. See
`.env.example` for the full list.

Deploying to Render: `render.yaml` is included. Set `UPSTOX_ACCESS_TOKEN` in the
dashboard (it is marked `sync: false` so it never lives in git).

⚠️ SQLite on Render's free tier is ephemeral — the archive resets whenever the instance sleeps
or redeploys. Attach a persistent disk, or move `signals.db` to Postgres, if you want it to
accumulate.
