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

Signals are generated **once a day, at the 09:00 IST reload**, from completed daily candles.
The 1-minute loop during market hours refreshes prices only — it cannot create a signal,
because volume is not updated intraday and the `Volume > 1.5x VolSMA20` gate never passes.
A stock keeps its BUY/SELL badge until the opposite signal fires, so the dashboard shows the
most recent call within the 90-day lookback, not a call from today.

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
- `GET /api/indices`, `/api/notifications`, `/api/rsi-screener`
- `GET /api/chart-data/{symbol}/{period}`, `/api/index-history/{symbol}/{period}`, `/api/stock/{symbol}`

## Run

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port $PORT
```

No environment variables are required.

⚠️ SQLite on Render's free tier is ephemeral — the archive resets whenever the instance sleeps
or redeploys. Attach a persistent disk, or move `signals.db` to Postgres, if you want it to
accumulate.
