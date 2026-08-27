"""
Nifty 500 Live Signal Backend v6
Render.com | uvicorn main:app --host 0.0.0.0 --port $PORT
"""
import os,time,logging,sqlite3,json
from datetime import datetime,timedelta
from typing import Dict,List,Optional,Any
from collections import deque
import pytz,pandas as pd,requests,yfinance as yf,numpy as np
import upstox
from fastapi import FastAPI,HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

try:
    from dotenv import load_dotenv; load_dotenv()   # local .env; no-op on Render
except Exception:
    pass

logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s")
log=logging.getLogger(__name__)
IST=pytz.timezone("Asia/Kolkata")
MARKET_OPEN=(9,15); MARKET_CLOSE=(15,30)
DB_PATH="signals.db"; PRICE_HISTORY_LEN=30
APP_VERSION="6.4-nifty500-volume-fix"
WEEK_DAYS=7

# --- Volume gate -----------------------------------------------------------
VOL_MULTIPLIER=float(os.getenv("VOL_MULTIPLIER","1.5"))   # Volume > 1.5x VolSMA20
# Intraday volume is cumulative-so-far, so it cannot be compared directly against a
# full-day 20-session average. We project the full-session total using a typical NSE
# volume curve. Set LIVE_VOL_MODE=off to only ever evaluate completed daily candles.
LIVE_VOL_MODE=os.getenv("LIVE_VOL_MODE","project").lower()  # project | raw | off
# Below this fraction of the session the projection is too noisy to trust.
MIN_SESSION_FRACTION=float(os.getenv("MIN_SESSION_FRACTION","0.08"))  # ~30 min in

# --- Data provider ---------------------------------------------------------
# upstox   live quotes (and optionally history) from the Upstox API
# nse      the legacy NSE website scrape
DATA_PROVIDER=os.getenv("DATA_PROVIDER","upstox").lower()
# Use Upstox for the 90-day daily candles too, instead of yfinance. One request per
# symbol, so it is slower than yfinance's batch download, but it is the same source
# as the live quotes and has no delisted-ticker gaps.
UPSTOX_HISTORY=os.getenv("UPSTOX_HISTORY","0") in ("1","true","yes")
# Fall back to NSE/yfinance when Upstox is unavailable (expired token, outage).
UPSTOX_FALLBACK=os.getenv("UPSTOX_FALLBACK","1") in ("1","true","yes")



# ---------------------------------------------------------------------------
# Universe: loaded from nifty500.csv (NSE's official Nifty 500 constituent list).
# To refresh, download the latest ind_nifty500list.csv from niftyindices.com and
# re-run: pandas.read_csv(...)[["Symbol","Company Name","Industry"]] -> nifty500.csv
# ---------------------------------------------------------------------------
UNIVERSE_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nifty500.csv")

# Symbols whose NSE ticker differs from the Yahoo Finance ticker, or that need
# pinning after a corporate action. NSE symbol -> Yahoo symbol (without .NS).
YF_OVERRIDES = {}

# Fallback so the app still boots if the CSV is missing from the deploy.
FALLBACK_STOCKS = [
    {"symbol":"HDFCBANK","name":"HDFC Bank","sector":"Financial Services"},
    {"symbol":"ICICIBANK","name":"ICICI Bank","sector":"Financial Services"},
    {"symbol":"RELIANCE","name":"Reliance Industries","sector":"Oil Gas & Consumable Fuels"},
    {"symbol":"TCS","name":"Tata Consultancy Services","sector":"Information Technology"},
    {"symbol":"INFY","name":"Infosys","sector":"Information Technology"},
]

def load_universe():
    """Read the Nifty 500 constituent list from disk."""
    try:
        df = pd.read_csv(UNIVERSE_CSV)
        df.columns = [c.strip().lower() for c in df.columns]
        # Accept either our normalised header or NSE's raw download header.
        ren = {"company name":"name", "industry":"sector"}
        df = df.rename(columns={k:v for k,v in ren.items() if k in df.columns})
        if "symbol" not in df.columns:
            raise ValueError("no 'symbol' column")
        rows, seen = [], set()
        for _, r in df.iterrows():
            sym = str(r["symbol"]).strip().upper()
            if not sym or sym == "NAN" or sym in seen:
                continue
            seen.add(sym)
            rows.append({
                "symbol": sym,
                "name":   str(r.get("name", sym)).strip() or sym,
                "sector": str(r.get("sector", "")).strip() or "Other",
            })
        if not rows:
            raise ValueError("empty universe")
        log.info(f"Universe: {len(rows)} stocks from {os.path.basename(UNIVERSE_CSV)}")
        return rows
    except Exception as e:
        log.error(f"Could not load {UNIVERSE_CSV}: {e} — falling back to built-in list")
        return list(FALLBACK_STOCKS)

STOCKS = load_universe()

def yf_symbol(sym):
    """NSE symbol -> Yahoo Finance ticker."""
    return f"{YF_OVERRIDES.get(sym, sym)}.NS"

STOCK_MAP={s["symbol"]:s for s in STOCKS}

INDICES_LIST=[
    # Broad Market
    {"symbol":"^NSEI",      "name":"NIFTY 50",       "yf":"^NSEI",       "category":"Broad"},
    {"symbol":"^NSEBANK",   "name":"NIFTY BANK",      "yf":"^NSEBANK",    "category":"Broad"},
    {"symbol":"^CNX100",    "name":"NIFTY 100",       "yf":"^CNX100",     "category":"Broad"},
    {"symbol":"^CNX200",    "name":"NIFTY 200",       "yf":"^CNX200",     "category":"Broad"},
    {"symbol":"^CRSLDX",    "name":"NIFTY 500",       "yf":"^CRSLDX",     "category":"Broad"},
    # Mid & Small Cap
    {"symbol":"^NSMIDCP",   "name":"NIFTY MIDCAP 50", "yf":"^NSMIDCP",    "category":"MidSmall"},
    {"symbol":"^CNXSC",     "name":"NIFTY SMALLCAP",  "yf":"^CNXSC",      "category":"MidSmall"},
    # Sectoral
    {"symbol":"^CNXIT",     "name":"NIFTY IT",        "yf":"^CNXIT",      "category":"Sectoral"},
    {"symbol":"^CNXPHARMA", "name":"NIFTY PHARMA",    "yf":"^CNXPHARMA",  "category":"Sectoral"},
    {"symbol":"^CNXAUTO",   "name":"NIFTY AUTO",      "yf":"^CNXAUTO",    "category":"Sectoral"},
    {"symbol":"^CNXFMCG",   "name":"NIFTY FMCG",      "yf":"^CNXFMCG",    "category":"Sectoral"},
    {"symbol":"^CNXMETAL",  "name":"NIFTY METAL",     "yf":"^CNXMETAL",   "category":"Sectoral"},
    {"symbol":"^CNXREALTY", "name":"NIFTY REALTY",    "yf":"^CNXREALTY",  "category":"Sectoral"},
    {"symbol":"^CNXENERGY", "name":"NIFTY ENERGY",    "yf":"^CNXENERGY",  "category":"Sectoral"},
    {"symbol":"^CNXINFRA",  "name":"NIFTY INFRA",     "yf":"^CNXINFRA",   "category":"Sectoral"},
    {"symbol":"^CNXPSE",    "name":"NIFTY PSE",       "yf":"^CNXPSE",     "category":"Sectoral"},
]

store={
    "last_update":"Not yet updated","is_market_open":False,"initialized":False,
    "stocks":{},"ohlcv":{},"weekly_ohlcv":{},"indices":{},"price_history":{},
    "notifications":deque(maxlen=100),
    "rsi_store":{},
    "last_db_error":None,
    "quote_source":None,"upstox_error":None,
}

PERIOD_MAP={
    "1H" :{"period":"1d",   "interval":"5m"},    # today intraday 5-min bars
    "1D" :{"period":"5d",   "interval":"30m"},   # intraday 30-min bars
    "5D" :{"period":"5d",   "interval":"1d"},    # 5 trading days, daily bars
    "1W" :{"period":"5d",   "interval":"60m"},   # 1 week, hourly bars
    "1M" :{"period":"1mo",  "interval":"1d"},    # 1 month daily
    "3M" :{"period":"3mo",  "interval":"1d"},    # 3 months daily
    "6M" :{"period":"6mo",  "interval":"1d"},    # 6 months daily
    "1Y" :{"period":"1y",   "interval":"1d"},    # 1 year daily
    "5Y" :{"period":"5y",   "interval":"1wk"},   # 5 years weekly
    "ALL":{"period":"max",  "interval":"1mo"},   # all time monthly
}

def add_notification(ntype,symbol,name,price,detail=""):
    ts=datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    store["notifications"].appendleft({
        "id":int(time.time()*1000),"type":ntype,"symbol":symbol,
        "name":name,"price":price,"detail":detail,"timestamp":ts,"read":False
    })

def init_db():
    con=sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS signal_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,signal TEXT,date TEXT,price REAL,ts TEXT)""")
    con.execute("CREATE INDEX IF NOT EXISTS ix_sig_date ON signal_history(date)")
    try:
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_sig ON signal_history(symbol,signal,date)")
    except Exception as e:
        log.warning(f"unique index (existing duplicates?): {e}")
    con.commit(); con.close()

def save_signal(symbol,signal,date,price):
    ts=datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    con=sqlite3.connect(DB_PATH)
    con.execute("INSERT OR IGNORE INTO signal_history VALUES(NULL,?,?,?,?,?)",(symbol,signal,date,price,ts))
    con.commit(); con.close()

def extract_all_signals(df,sym):
    """Every BUY/SELL bar in the frame, not just the most recent one.
    Signals sit on daily closes, so the fired-at time is that session's 15:30."""
    out=[]
    for i in range(len(df)):
        r=df.iloc[i]
        sig="BUY" if r.get("BuySignal",False) else ("SELL" if r.get("SellSignal",False) else None)
        if not sig: continue
        try: px=float(r["Close"])
        except Exception: continue
        if pd.isna(px): continue
        d=df.index[i].strftime("%Y-%m-%d")
        out.append((sym,sig,d,round(px,2),f"{d} 15:30:00"))
    return out

def record_signals_bulk(rows):
    """Persist detected signals. Duplicates are dropped by the unique index."""
    if not rows: return 0
    try:
        con=sqlite3.connect(DB_PATH)
        cur=con.execute("SELECT COUNT(*) FROM signal_history"); before=cur.fetchone()[0]
        con.executemany("INSERT OR IGNORE INTO signal_history VALUES(NULL,?,?,?,?,?)",rows)
        con.commit()
        after=con.execute("SELECT COUNT(*) FROM signal_history").fetchone()[0]
        con.close()
        return after-before
    except Exception as e:
        log.error(f"record_signals_bulk: {e}")
        store["last_db_error"]=f"{type(e).__name__}: {e}"
        return 0

def _enrich(rows):
    """Attach name/sector/live price/return to raw signal_history rows."""
    out=[]
    for r in rows:
        sym=r[0]; e=STOCK_MAP.get(sym,{})
        cp=store["stocks"].get(sym,{}).get("current_price",r[3])
        pct=round((cp-r[3])/r[3]*100,2) if r[3] else 0
        out.append({"symbol":sym,"name":e.get("name",sym),"sector":e.get("sector",""),
                    "signal":r[1],"signal_date":r[2],"signal_price":r[3],
                    "current_price":cp,"pct_change":pct,"timestamp":r[4]})
    return out

def get_history(limit=500):
    con=sqlite3.connect(DB_PATH)
    cur=con.execute("SELECT symbol,signal,date,price,ts FROM signal_history ORDER BY id DESC LIMIT ?",(limit,))
    rows=cur.fetchall(); con.close()
    return _enrich(rows)

def get_signal_log(days=WEEK_DAYS,archive_limit=2000):
    """Split the signal history into the trailing `days` window and everything older."""
    cutoff=(datetime.now(IST)-timedelta(days=days)).strftime("%Y-%m-%d")
    con=sqlite3.connect(DB_PATH)
    recent=con.execute("SELECT symbol,signal,date,price,ts FROM signal_history "
                       "WHERE date>=? ORDER BY id DESC",(cutoff,)).fetchall()
    archive=con.execute("SELECT symbol,signal,date,price,ts FROM signal_history "
                        "WHERE date<? ORDER BY id DESC LIMIT ?",(cutoff,archive_limit)).fetchall()
    total=con.execute("SELECT COUNT(*) FROM signal_history").fetchone()[0]
    con.close()
    return {"cutoff":cutoff,"days":days,"total":total,
            "recent":_enrich(recent),"archive":_enrich(archive)}

def get_signals_for_date(date_str):
    con=sqlite3.connect(DB_PATH)
    rows=con.execute("SELECT symbol,signal,date,price,ts FROM signal_history "
                     "WHERE date=? ORDER BY id ASC",(date_str,)).fetchall()
    con.close()
    return _enrich(rows)

def _latest_session_date():
    """The newest date any signal is stamped with — i.e. the last completed session
    the strategy has evaluated. Signals are computed on daily candles, so on Friday
    morning the freshest calls are dated Thursday."""
    dates=[e.get("signal_date") for e in store["stocks"].values()
           if e.get("signal") in ("BUY","SELL") and e.get("signal_date")]
    try:
        con=sqlite3.connect(DB_PATH)
        r=con.execute("SELECT MAX(date) FROM signal_history").fetchone()
        con.close()
        if r and r[0]: dates.append(r[0])
    except Exception: pass
    return max(dates) if dates else None

def build_digest_payload(days=1,minutes=None,since=None,session=None):
    """Signals shaped for an external scheduler to read and mail.

    Windows:
      session=latest  the last completed trading session the strategy evaluated  ← use this
      days=N          N calendar days back from today
      minutes=N       rolling window (DB only — needs a fired-at time)
      since=TS        everything after 'YYYY-MM-DD HH:MM:SS' IST

    days/session modes merge SQLite history with the in-memory store, because SQLite is
    ephemeral on Render's free tier: after a restart the DB is empty, but load_historical_data()
    has already rebuilt the signals in memory from 90-day history.
    """
    now=datetime.now(IST)
    rolling = minutes is not None or since is not None
    if rolling:
        if since:
            try: cutoff_ts=str(since).strip().replace("T"," ")[:19]
            except Exception: cutoff_ts=(now-timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            cutoff_ts=(now-timedelta(minutes=max(1,min(int(minutes),1440)))).strftime("%Y-%m-%d %H:%M:%S")
        con=sqlite3.connect(DB_PATH)
        rows=con.execute("SELECT symbol,signal,date,price,ts FROM signal_history "
                         "WHERE ts>? ORDER BY id DESC",(cutoff_ts,)).fetchall()
        con.close()
        out=_enrich(rows)
        for s in out: s["source"]="db"
        window={"mode":"rolling","since":cutoff_ts}
    else:
        if session=="latest":
            d=_latest_session_date()
            dates=[d] if d else []
            window={"mode":"latest_session","session_date":d}
        else:
            dates=[(now-timedelta(days=i)).strftime("%Y-%m-%d") for i in range(max(1,min(days,31)))]
            window={"mode":"daily","days":len(dates),"covers":dates}
        seen=set(); out=[]
        for d in dates:
            for s in get_signals_for_date(d):
                k=f"{s['symbol']}|{s['signal']}|{s['signal_date']}"
                if k in seen: continue
                seen.add(k); s["source"]="db"; out.append(s)
        for sym,e in store["stocks"].items():
            if e.get("signal") not in ("BUY","SELL"): continue
            if e.get("signal_date") not in dates: continue
            k=f"{sym}|{e['signal']}|{e['signal_date']}"
            if k in seen: continue
            seen.add(k)
            out.append({"symbol":sym,"name":e.get("name",sym),"sector":e.get("sector",""),
                        "signal":e["signal"],"signal_date":e.get("signal_date"),
                        "signal_price":e.get("signal_price"),"current_price":e.get("current_price"),
                        "pct_change":e.get("pct_change",0),"rsi14":e.get("rsi14"),
                        "timestamp":None,"source":"live"})
    out.sort(key=lambda s:(s["signal"],-(s.get("signal_price") or 0)))
    buys=[s for s in out if s["signal"]=="BUY"]
    n=len(out)
    label=(window.get("session_date") or now.strftime("%Y-%m-%d")) if not rolling else now.strftime("%H:%M IST")
    return {
        "generated_at":now.strftime("%Y-%m-%d %H:%M:%S IST"),
        "date":now.strftime("%Y-%m-%d"),"window":window,
        "market_open":store["is_market_open"],"initialized":store["initialized"],
        "last_update":store["last_update"],"universe":len(store["stocks"]),
        "count":n,"buy_count":len(buys),"sell_count":n-len(buys),
        "empty":n==0,
        "subject_hint":(f"NiftySignals — no new calls {label}" if not n else
                        f"NiftySignals {label} — {n} call{'s' if n!=1 else ''} "
                        f"({len(buys)} BUY / {n-len(buys)} SELL)"),
        "signals":out,
    }

def compute_rsi(series,period=14):
    delta=series.diff()
    gain=delta.clip(lower=0); loss=-delta.clip(upper=0)
    avg_gain=gain.ewm(com=period-1,min_periods=period).mean()
    avg_loss=loss.ewm(com=period-1,min_periods=period).mean()
    rs=avg_gain/avg_loss.replace(0,1e-10)
    return (100-(100/(1+rs))).round(2)

# Typical NSE cumulative-volume curve: fraction of the day's total traded by N
# minutes into the session. Volume is U-shaped — heavy at the open and into the
# close — so a flat elapsed-time projection badly overstates the day early on.
_VOL_CURVE_MIN =[0,  15,   30,  45,  60,  90, 120, 150, 180, 210, 240, 270, 300, 330, 360, 375]
_VOL_CURVE_FRAC=[0,.075, .130,.180,.220,.300,.370,.440,.500,.560,.620,.690,.770,.860,.950,1.00]
SESSION_MINUTES=375  # 09:15 -> 15:30

def session_elapsed_fraction(now=None):
    """Fraction of the day's expected volume that should have traded by now."""
    now=now or datetime.now(IST)
    mins=(now.hour*60+now.minute)-(MARKET_OPEN[0]*60+MARKET_OPEN[1])
    mins=max(0,min(mins,SESSION_MINUTES))
    return float(np.interp(mins,_VOL_CURVE_MIN,_VOL_CURVE_FRAC))

def project_session_volume(cum_volume,now=None):
    """Scale volume-so-far up to a full-session estimate.

    Returns None when the projection should not be used — too early in the session,
    no volume data, or live projection disabled. A None result means the signal gate
    falls back to raw cumulative volume, which simply won't fire until real volume
    genuinely exceeds the threshold. That is the safe failure direction.
    """
    if LIVE_VOL_MODE=="off" or not cum_volume or cum_volume<=0:
        return None
    if LIVE_VOL_MODE=="raw":
        return float(cum_volume)
    frac=session_elapsed_fraction(now)
    if frac<MIN_SESSION_FRACTION:
        return None
    return float(cum_volume)/frac

def compute_indicators(df,live_projected_vol=None):
    """Indicators + Triple Confluence signals.

    live_projected_vol: full-session volume estimate for the LAST bar when that bar
    is still in progress. Volume itself stays truthful (actual traded so far); only
    the signal gate uses the projection, via the VolForSignal column.
    """
    df=df.copy(); df.sort_index(inplace=True)
    # yfinance hands back Volume as int64; the live loop writes fractional projected
    # volume into it, which raises on an int column in pandas 2.x. Normalise once here
    # so every downstream write is safe.
    if "Volume" in df.columns:
        df["Volume"]=pd.to_numeric(df["Volume"],errors="coerce").astype("float64")
    df["SMA5"]=df["Close"].rolling(5).mean().round(2)
    df["EMA13"]=df["Close"].ewm(span=13,adjust=False).mean().round(2)
    df["EMA26"]=df["Close"].ewm(span=26,adjust=False).mean().round(2)
    # Average of the PRIOR 5 sessions. Without shift(1) the current bar is inside
    # its own benchmark, which inflates the threshold exactly on the spike days the
    # strategy is meant to catch.
    df["VolSMA20"]=df["Volume"].rolling(5).mean().shift(1).round(0)
    df["RSI14"]=compute_rsi(df["Close"],14)
    df["MaxInd"]=df[["SMA5","EMA13","EMA26"]].max(axis=1)
    df["MinInd"]=df[["SMA5","EMA13","EMA26"]].min(axis=1)
    df["Conjunction"]=(df["MaxInd"]*0.99)<=(df["MinInd"]*1.01)

    df["VolForSignal"]=df["Volume"].astype("float64")
    if live_projected_vol is not None and len(df):
        df.iloc[-1,df.columns.get_loc("VolForSignal")]=float(live_projected_vol)

    df["VolConfirm"]=df["VolForSignal"]>(df["VolSMA20"]*VOL_MULTIPLIER)
    df["SMA5_Rising"]=df["SMA5"]>df["SMA5"].shift(2)
    df["SMA5_Falling"]=df["SMA5"]<df["SMA5"].shift(2)
    df["BuySignal"]=df["Conjunction"]&df["VolConfirm"]&(df["Close"]>df["SMA5"])&df["SMA5_Rising"]
    df["SellSignal"]=df["Conjunction"]&df["VolConfirm"]&(df["Close"]<df["SMA5"])&df["SMA5_Falling"]
    return df

def compute_weekly_rsi(df_daily):
    try:
        df_w=df_daily["Close"].resample("W").last().dropna()
        if len(df_w)<15: return None,None
        rsi=compute_rsi(df_w,14)
        return float(rsi.iloc[-1]),float(rsi.iloc[-2]) if len(rsi)>1 else None
    except: return None,None

def detect_signal(df):
    result={"signal":"HOLD","signal_date":None,"signal_price":None}
    for i in range(len(df)-1,-1,-1):
        row=df.iloc[i]
        if row.get("BuySignal",False):
            result={"signal":"BUY","signal_date":df.index[i].strftime("%Y-%m-%d"),"signal_price":float(row["Close"])}; break
        if row.get("SellSignal",False):
            result={"signal":"SELL","signal_date":df.index[i].strftime("%Y-%m-%d"),"signal_price":float(row["Close"])}; break
    return result

_nse_session=None; _nse_ts=0.0

def get_nse_session():
    global _nse_session,_nse_ts
    if _nse_session is None or (time.time()-_nse_ts)>300:
        s=requests.Session()
        s.headers.update({"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                          "Referer":"https://www.nseindia.com","Accept":"*/*"})
        try: s.get("https://www.nseindia.com",timeout=15); time.sleep(0.5)
        except: pass
        _nse_session=s; _nse_ts=time.time()
    return _nse_session

def _num(x):
    """NSE sends numbers as strings with commas, and '-' for missing."""
    try:
        if x is None: return None
        v=str(x).replace(",","").strip()
        if v in ("","-","NA"): return None
        return float(v)
    except Exception: return None

def fetch_live_quotes():
    """Live quotes from the configured provider.

    Returns {symbol: {"ltp": float, "volume": float|None, ...}}.

    Upstox is preferred: the token is account-scoped rather than a scraped session, and
    `volume` is a documented field instead of a payload shape that can shift underneath
    us. The NSE scrape stays as fallback so an expired token degrades the dashboard
    instead of blanking it — which matters because Upstox tokens expire every day.
    """
    provider = DATA_PROVIDER
    if provider == "upstox":
        if not upstox.is_configured():
            log.warning("DATA_PROVIDER=upstox but UPSTOX_ACCESS_TOKEN is unset — using NSE")
        else:
            try:
                q = upstox.get_quotes([s["symbol"] for s in STOCKS])
                if q:
                    store["quote_source"] = "upstox"
                    return q
                log.warning("Upstox returned no quotes — falling back to NSE")
            except upstox.UpstoxAuthError as e:
                store["upstox_error"] = str(e)
                log.error(f"{e}  -> falling back to NSE for this cycle")
            except Exception as e:
                log.error(f"Upstox quotes failed: {e} -> falling back to NSE")
        if not UPSTOX_FALLBACK:
            store["quote_source"] = "none"
            return {}
    q = fetch_nse_prices()
    store["quote_source"] = "nse" if q else "none"
    return q


def fetch_nse_prices():
    """Return {symbol: {"ltp":float, "volume":float|None}}.

    Volume is the day's cumulative traded quantity. The old version dropped it
    entirely, which is why the volume gate could never pass intraday.
    """
    s=get_nse_session(); quotes={}
    # NIFTY 500 first so it covers the whole universe in one call; the others
    # backfill anything the 500 endpoint happens to miss.
    for index in ["NIFTY%20500","NIFTY%20200","NIFTY%20MIDCAP%20150","NIFTY%20SMALLCAP%20250"]:
        try:
            r=s.get(f"https://www.nseindia.com/api/equity-stockIndices?index={index}",timeout=20)
            r.raise_for_status()
            for item in r.json().get("data",[]):
                sym=item.get("symbol","")
                ltp=_num(item.get("lastPrice"))
                if not sym or not ltp or sym in quotes: continue
                vol=_num(item.get("totalTradedVolume"))
                if vol is None:
                    # Some payloads carry value-in-lakhs instead; derive shares from it.
                    val=_num(item.get("totalTradedValue"))
                    vol=(val*1e5)/ltp if (val and ltp) else None
                quotes[sym]={"ltp":ltp,"volume":vol}
        except Exception as e: log.warning(f"NSE {index}: {e}")
    if quotes:
        with_vol=sum(1 for q in quotes.values() if q["volume"])
        log.info(f"NSE: {len(quotes)} quotes, {with_vol} with volume")
    return quotes

def _fetch_history_upstox(symbols):
    """90-day daily candles from Upstox, one request per symbol.

    Slower than yfinance's batched download, so it is opt-in via UPSTOX_HISTORY=1.
    Returns {symbol: DataFrame}; symbols that fail are simply absent and the caller
    fills them from yfinance.
    """
    out={}
    for i,sym in enumerate(symbols):
        try:
            df=upstox.get_history(sym,days=90,interval="day")
            if df is not None and len(df)>=25: out[sym]=df
        except upstox.UpstoxAuthError as e:
            log.error(f"Upstox history aborted at {sym}: {e}")
            break
        except Exception as e:
            log.warning(f"Upstox history {sym}: {e}")
        if i and i%50==0:
            log.info(f"Upstox history {i}/{len(symbols)}")
            time.sleep(0.5)
        time.sleep(float(os.getenv("UPSTOX_HIST_SLEEP","0.05")))
    log.info(f"Upstox history: {len(out)}/{len(symbols)} symbols")
    return out

def load_historical_data():
    log.info(f"Loading {len(STOCKS)} stocks...")
    all_sig_rows=[]
    symbols=[s["symbol"] for s in STOCKS]

    upstox_hist={}
    if UPSTOX_HISTORY and upstox.is_configured():
        try: upstox_hist=_fetch_history_upstox(symbols)
        except Exception as e: log.error(f"Upstox history pass failed: {e}")

    def _ingest(sym,df):
        """Indicators + signals + store write for one symbol."""
        df=df.dropna(subset=["Close"])
        if df.empty: return False
        df=compute_indicators(df); sig=detect_signal(df); last=df.iloc[-1]
        all_sig_rows.extend(extract_all_signals(df,sym))
        def v(x): return round(float(x),2) if not pd.isna(x) else None
        rsi_cur,rsi_prev=compute_weekly_rsi(df)
        store["rsi_store"][sym]={"rsi":rsi_cur,"rsi_prev":rsi_prev}
        store["stocks"][sym]={
            "symbol":sym,"name":STOCK_MAP[sym]["name"],"sector":STOCK_MAP[sym]["sector"],
            "current_price":v(last["Close"]),"signal":sig["signal"],
            "signal_date":sig["signal_date"],"signal_price":sig["signal_price"],
            "pct_change":0.0,"sma5":v(last["SMA5"]),"ema13":v(last["EMA13"]),
            "ema26":v(last["EMA26"]),"volsma20":v(last["VolSMA20"]),
            "conjunction":bool(last.get("Conjunction",False)),
            "vol_confirm":bool(last.get("VolConfirm",False)),
            "volume_confirm":bool(last.get("VolConfirm",False)),
            "price_above_zone":bool(last.get("Close",0)>last.get("SMA5",0)) if not pd.isna(last.get("SMA5")) else False,
            "price_below_zone":bool(last.get("Close",0)<last.get("SMA5",0)) if not pd.isna(last.get("SMA5")) else False,
            "sma5_rising":bool(last.get("SMA5_Rising",False)),
            "sma5_falling":bool(last.get("SMA5_Falling",False)),
            "rsi":rsi_cur,"rsi_prev":rsi_prev,
            "rsi14":v(last.get("RSI14")),
            "volume_today":int(last["Volume"]) if not pd.isna(last.get("Volume")) else 0,
            "volume_projected":None,"session_fraction":None,
            "vol_ratio":round(float(last["VolForSignal"])/float(last["VolSMA20"]),2)
                if not pd.isna(last.get("VolSMA20")) and last.get("VolSMA20") else None,
        }
        store["ohlcv"][sym]=df
        closes=df["Close"].dropna().tolist()[-PRICE_HISTORY_LEN:]
        store["price_history"][sym]=deque(closes,maxlen=PRICE_HISTORY_LEN)
        if sig["signal_price"] and v(last["Close"]):
            sp=sig["signal_price"]; cp=v(last["Close"])
            store["stocks"][sym]["pct_change"]=round((cp-sp)/sp*100,2)
        return True

    for sym,df in upstox_hist.items():
        try: _ingest(sym,df.copy())
        except Exception as ex: log.warning(f"{sym}: {ex}")

    remaining=[s for s in symbols if s not in store["stocks"]]
    if remaining: log.info(f"yfinance for {len(remaining)} remaining symbols")
    yf_syms=[yf_symbol(sym) for sym in remaining]
    for b in range(0,len(yf_syms),50):
        byf=yf_syms[b:b+50]; bsym=remaining[b:b+50]
        log.info(f"Batch {b//50+1}/{(len(yf_syms)+49)//50}")
        try:
            raw=yf.download(byf,period="90d",interval="1d",group_by="ticker",
                           auto_adjust=True,progress=False,threads=True)
            for sym,yfs in zip(bsym,byf):
                try:
                    df=raw.copy() if len(byf)==1 else (raw[yfs].copy() if yfs in raw.columns.get_level_values(0) else None)
                    if df is None: continue
                    _ingest(sym,df)
                except Exception as ex: log.warning(f"{sym}: {ex}")
        except Exception as e: log.error(f"Batch: {e}")
        time.sleep(1)
    added=record_signals_bulk(all_sig_rows)
    log.info(f"Done. {len(store['stocks'])} stocks. Signals detected: {len(all_sig_rows)}, newly recorded: {added}.")

def load_index_data():
    try:
        for idx in INDICES_LIST:
            ticker=yf.Ticker(idx["yf"]); hist=ticker.history(period="2d")
            if len(hist)>=1:
                price=float(hist["Close"].iloc[-1]); prev=float(hist["Close"].iloc[-2]) if len(hist)>=2 else price
                pct=round((price-prev)/prev*100,2) if prev else 0
                day_open=float(hist["Open"].iloc[-1]); day_high=float(hist["High"].iloc[-1]); day_low=float(hist["Low"].iloc[-1])
                store["indices"][idx["symbol"]]={
                    "symbol":idx["symbol"],"name":idx["name"],
                    "category":idx.get("category","Broad"),
                    "price":round(price,2),"prev_close":round(prev,2),"pct_change":pct,
                    "day_open":round(day_open,2),"day_high":round(day_high,2),"day_low":round(day_low,2),
                    "change":round(price-prev,2),
                }
    except Exception as e: log.warning(f"Index: {e}")

def is_market_hours():
    now=datetime.now(IST)
    if now.weekday()>=5: return False
    return MARKET_OPEN<=(now.hour,now.minute)<=MARKET_CLOSE

def update_prices():
    store["is_market_open"]=is_market_hours()
    if not store["is_market_open"]: return
    try: quotes=fetch_live_quotes()
    except Exception as e: log.error(f"NSE: {e}"); return
    now_ist=datetime.now(IST); today=now_ist.strftime("%Y-%m-%d")
    frac=session_elapsed_fraction(now_ist)
    fired=[]
    for sym,entry in store["stocks"].items():
        q=quotes.get(sym)
        if not q: continue
        ltp=q["ltp"]; cum_vol=q.get("volume")
        entry["current_price"]=ltp
        h=store["price_history"].get(sym)
        if h: h.append(ltp)
        sp=entry.get("signal_price")
        if sp: entry["pct_change"]=round((ltp-sp)/sp*100,2)
        df=store["ohlcv"].get(sym)
        if df is None or df.empty: continue
        today_ts=pd.Timestamp(today)
        if df["Volume"].dtype.kind!="f":
            df["Volume"]=pd.to_numeric(df["Volume"],errors="coerce").astype("float64")
        if today_ts in df.index:
            df.loc[today_ts,"Close"]=ltp
            df.loc[today_ts,"High"]=max(float(df.loc[today_ts,"High"]),ltp)
            df.loc[today_ts,"Low"] =min(float(df.loc[today_ts,"Low"]),ltp)
            # Cumulative for the day — overwrite, never accumulate.
            if cum_vol is not None: df.loc[today_ts,"Volume"]=float(cum_vol)
        else:
            nr=pd.DataFrame({"Open":[ltp],"High":[ltp],"Low":[ltp],"Close":[ltp],
                             "Volume":[float(cum_vol) if cum_vol is not None else 0.0]},
                           index=[today_ts])
            df=pd.concat([df,nr])

        # Today's bar is incomplete, so scale volume-so-far to a full-session estimate
        # before testing it against the 20-day average.
        projected=project_session_volume(cum_vol,now_ist)
        df=compute_indicators(df,live_projected_vol=projected)
        store["ohlcv"][sym]=df; last=df.iloc[-1]
        entry["volume_today"]=int(cum_vol) if cum_vol else 0
        entry["volume_projected"]=int(projected) if projected else None
        entry["session_fraction"]=round(frac,3)
        entry["vol_ratio"]=round(float(last["VolForSignal"])/float(last["VolSMA20"]),2) \
            if not pd.isna(last.get("VolSMA20")) and last.get("VolSMA20") else None
        def v(x): return round(float(x),2) if not pd.isna(x) else None
        prev_rsi=entry.get("rsi14")
        entry["sma5"]=v(last["SMA5"]); entry["ema13"]=v(last["EMA13"])
        entry["ema26"]=v(last["EMA26"]); entry["rsi14"]=v(last.get("RSI14"))
        entry["conjunction"]=bool(last.get("Conjunction",False))
        entry["vol_confirm"]=bool(last.get("VolConfirm",False))
        entry["volume_confirm"]=bool(last.get("VolConfirm",False))
        _s5=last.get("SMA5")
        entry["price_above_zone"]=bool(ltp>_s5) if not pd.isna(_s5) else False
        entry["price_below_zone"]=bool(ltp<_s5) if not pd.isna(_s5) else False
        entry["sma5_rising"]=bool(last.get("SMA5_Rising",False))
        entry["sma5_falling"]=bool(last.get("SMA5_Falling",False))
        cur_rsi=entry["rsi14"]
        if prev_rsi is not None and cur_rsi is not None:
            if prev_rsi<49 and cur_rsi>=49:
                add_notification("RSI_CROSS",sym,entry["name"],ltp,
                    f"Weekly RSI crossed above 49 — now at {cur_rsi}")
        prev_sig=entry.get("signal")
        new_sig=None
        if df["BuySignal"].iloc[-1] and prev_sig!="BUY":   new_sig="BUY"
        elif df["SellSignal"].iloc[-1] and prev_sig!="SELL": new_sig="SELL"
        if new_sig:
            entry.update({"signal":new_sig,"signal_date":today,"signal_price":ltp,"pct_change":0.0})
            save_signal(sym,new_sig,today,ltp)
            add_notification(new_sig,sym,entry["name"],ltp,
                             f"Triple Confluence {new_sig} signal fired at ₹{ltp}")
            log.info(f"{new_sig} {sym}@{ltp}")
            fired.append(sym)
    store["last_update"]=now_ist.strftime("%Y-%m-%d %H:%M:%S IST")
    if fired: log.info(f"{len(fired)} new signal(s) this cycle: {', '.join(fired[:10])}")

def index_update_job():
    if is_market_hours(): load_index_data()


app=FastAPI(title="Nifty500 Signals v6",version="6.0")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=False,allow_methods=["*"],allow_headers=["*"])


@app.api_route("/api/health", methods=["GET","HEAD"])
def health():
    try:
        con=sqlite3.connect(DB_PATH)
        db_rows=con.execute("SELECT COUNT(*) FROM signal_history").fetchone()[0]
        con.close()
    except Exception as e:
        db_rows=f"error: {e}"
    return {"status":"ok","version":APP_VERSION,"initialized":store["initialized"],
            "last_update":store["last_update"],"is_market_open":store["is_market_open"],
            "total_stocks":len(store["stocks"]),"frames_in_memory":len(store["ohlcv"]),
            "signal_log_rows":db_rows,"last_db_error":store.get("last_db_error"),
            "data_provider":DATA_PROVIDER,"quote_source":store.get("quote_source"),
            "upstox":upstox.status(),
            "volume_gate":{"multiplier":VOL_MULTIPLIER,"live_mode":LIVE_VOL_MODE,
                           "min_session_fraction":MIN_SESSION_FRACTION,
                           "session_fraction_now":round(session_elapsed_fraction(),3)}}

@app.get("/api/indices")
def get_indices():
    """Index strip on the dashboard. load_index_data() already populates this;
    the route was missing, so the frontend's fetch was 404ing silently."""
    vals=list(store["indices"].values())
    order={"Broad":0,"MidSmall":1,"Sectoral":2}
    vals.sort(key=lambda x:(order.get(x.get("category","Broad"),9),x.get("name","")))
    return {"indices":vals,"count":len(vals),"last_update":store["last_update"]}

@app.get("/api/notifications")
def get_notifications(limit:int=50,unread_only:bool=False):
    items=list(store["notifications"])
    if unread_only: items=[n for n in items if not n.get("read")]
    items=items[:max(1,min(int(limit),100))]
    return {"notifications":items,
            "unread":sum(1 for n in store["notifications"] if not n.get("read")),
            "total":len(store["notifications"])}

@app.post("/api/notifications/read")
def mark_notifications_read():
    for n in store["notifications"]: n["read"]=True
    return {"ok":True,"marked":len(store["notifications"])}

@app.get("/api/signals")
def get_signals():
    order={"BUY":0,"SELL":1,"HOLD":2}
    sl=list(store["stocks"].values())
    sl.sort(key=lambda x:(order.get(x.get("signal","HOLD"),2),
                          -(pd.Timestamp(x["signal_date"]).timestamp() if x.get("signal_date") else 0)))
    for s in sl:
        h=store["price_history"].get(s["symbol"])
        s["price_history"]=list(h) if h else []
    return {"last_update":store["last_update"],"is_market_open":store["is_market_open"],
            "total":len(sl),"stocks":sl}

@app.get("/api/history")
def get_history_ep(): return {"history":get_history(500)}

@app.get("/api/signal-log")
def signal_log_ep(days:int=WEEK_DAYS):
    """Trailing-week signals plus the older archive, both from the same store."""
    days=max(1,min(days,365))
    return get_signal_log(days=days)

@app.get("/api/rebuild-signal-log")
def rebuild_signal_log():
    """Re-extract every signal from the frames already in memory and write them to the log.
    Uses no network — safe to hit any time. Duplicates are ignored by the unique index."""
    rows=[]; errs=[]
    for sym,df in store["ohlcv"].items():
        try: rows.extend(extract_all_signals(df,sym))
        except Exception as e: errs.append(f"{sym}: {type(e).__name__}: {e}")
    added=record_signals_bulk(rows)
    try:
        con=sqlite3.connect(DB_PATH)
        total=con.execute("SELECT COUNT(*) FROM signal_history").fetchone()[0]
        con.close()
    except Exception as e:
        total=f"error: {e}"
    return {"version":APP_VERSION,"frames_scanned":len(store["ohlcv"]),
            "signals_detected":len(rows),"newly_recorded":added,"signal_log_rows":total,
            "db_error":store.get("last_db_error"),"sample_errors":errs[:5]}

@app.get("/api/digest-payload")
def digest_payload(days:int=1,minutes:Optional[int]=None,since:Optional[str]=None,
                   session:Optional[str]=None):
    """Signals as flat JSON for an external scheduler. Read-only, no credentials.
    ?session=latest → calls from the last completed session  (use this for the daily mail)
    ?days=1         → whole calendar day
    ?minutes=35     → rolling window
    ?since=TS       → everything after 'YYYY-MM-DD HH:MM:SS' IST
    """
    return build_digest_payload(days=days,minutes=minutes,since=since,session=session)

@app.get("/api/rsi-screener")
def rsi_screener(min_rsi:float=45,max_rsi:float=60):
    result=[]
    for sym,entry in store["stocks"].items():
        rsi=entry.get("rsi14")
        if rsi is None: continue
        if not (min_rsi<=rsi<=max_rsi): continue
        prev_rsi=store["rsi_store"].get(sym,{}).get("rsi_prev")
        rising=prev_rsi is not None and rsi>prev_rsi
        h=store["price_history"].get(sym)
        result.append({
            "symbol":sym,"name":entry["name"],"sector":entry["sector"],
            "current_price":entry["current_price"],"rsi":rsi,"rsi_prev":prev_rsi,
            "rsi_rising":rising,"signal":entry["signal"],
            "pct_change":entry.get("pct_change",0),
            "price_history":list(h) if h else [],
        })
    result.sort(key=lambda x:x["rsi"],reverse=True)
    return {"stocks":result,"count":len(result)}

@app.get("/api/chart-data/{symbol}/{period}")
def get_chart_data(symbol:str,period:str):
    symbol=symbol.upper()
    info=STOCK_MAP.get(symbol)
    if not info: raise HTTPException(status_code=404,detail="Symbol not found")
    cfg=PERIOD_MAP.get(period,PERIOD_MAP["3M"])
    try:
        # Use Ticker.history() — always returns a flat DataFrame regardless of yfinance version.
        # yf.download() with a single ticker returns MultiIndex columns in yfinance >=0.2.x
        # which causes KeyError on row["Close"] → 500.
        ticker=yf.Ticker(yf_symbol(symbol))
        df=ticker.history(period=cfg["period"],interval=cfg["interval"],auto_adjust=True)
        if df.empty: raise HTTPException(status_code=404,detail="No data")
        df.sort_index(inplace=True)
        # Drop timezone info from index so strftime works cleanly
        if hasattr(df.index,"tz") and df.index.tz is not None:
            df.index=df.index.tz_localize(None)
        if len(df)>5:
            df["SMA5"]=df["Close"].rolling(5).mean()
            df["EMA13"]=df["Close"].ewm(span=13,adjust=False).mean()
            df["EMA26"]=df["Close"].ewm(span=26,adjust=False).mean()
            df["RSI14"]=compute_rsi(df["Close"],14)
        def v(x):
            try: return None if pd.isna(x) else round(float(x),2)
            except: return None
        rows=[]
        for dt,row in df.iterrows():
            rows.append({
                "date":dt.strftime("%Y-%m-%d %H:%M") if cfg["interval"] in ["5m","30m","60m"] else dt.strftime("%Y-%m-%d"),
                "open":v(row.get("Open")),"high":v(row.get("High")),
                "low":v(row.get("Low")),"close":v(row.get("Close")),
                "volume":int(row["Volume"]) if "Volume" in row and not pd.isna(row["Volume"]) else 0,
                "sma5":v(row.get("SMA5")),"ema13":v(row.get("EMA13")),
                "ema26":v(row.get("EMA26")),"rsi14":v(row.get("RSI14")),
            })
        return {"symbol":symbol,"period":period,"data":rows}
    except HTTPException: raise
    except Exception as e:
        log.error(f"chart-data {symbol}/{period}: {e}")
        raise HTTPException(status_code=500,detail=str(e))

@app.get("/api/index-history/{symbol}/{period}")
def get_index_history(symbol:str,period:str):
    cfg=PERIOD_MAP.get(period,PERIOD_MAP["3M"])
    try:
        ticker=yf.Ticker(symbol)
        df=ticker.history(period=cfg["period"],interval=cfg["interval"],auto_adjust=True)
        if df.empty: raise HTTPException(status_code=404,detail="No data")
        if hasattr(df.index,"tz") and df.index.tz is not None:
            df.index=df.index.tz_localize(None)
        def v(x):
            try: return None if pd.isna(x) else round(float(x),2)
            except: return None
        rows=[]
        for dt,row in df.iterrows():
            rows.append({
                "date":dt.strftime("%Y-%m-%d %H:%M") if cfg["interval"] in ["5m","30m","60m"] else dt.strftime("%Y-%m-%d"),
                "close":v(row.get("Close")),"high":v(row.get("High")),
                "low":v(row.get("Low")),"open":v(row.get("Open")),
            })
        return {"symbol":symbol,"period":period,"data":rows}
    except HTTPException: raise
    except Exception as e:
        log.error(f"index-history {symbol}/{period}: {e}")
        raise HTTPException(status_code=500,detail=str(e))

@app.get("/api/stock/{symbol}")
def get_stock(symbol:str):
    symbol=symbol.upper()
    if symbol not in store["ohlcv"]: raise HTTPException(status_code=404,detail="Symbol not found")
    df=store["ohlcv"][symbol].copy().tail(90)
    rows=[]
    for dt,row in df.iterrows():
        def v(x): return None if pd.isna(x) else round(float(x),2)
        rows.append({"date":dt.strftime("%Y-%m-%d"),"open":v(row["Open"]),"high":v(row["High"]),
                     "low":v(row["Low"]),"close":v(row["Close"]),
                     "volume":int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
                     "sma5":v(row.get("SMA5")),"ema13":v(row.get("EMA13")),
                     "ema26":v(row.get("EMA26")),"rsi14":v(row.get("RSI14")),
                     "volsma20":v(row.get("VolSMA20")),
                     "conjunction":bool(row.get("Conjunction",False)),
                     "vol_confirm":bool(row.get("VolConfirm",False)),
                     "buy_signal":bool(row.get("BuySignal",False)),
                     "sell_signal":bool(row.get("SellSignal",False))})
    info=store["stocks"].get(symbol,STOCK_MAP.get(symbol,{})).copy()
    h=store["price_history"].get(symbol)
    info["price_history"]=list(h) if h else []
    return {"symbol":symbol,"info":info,"ohlcv":rows}

@app.on_event("startup")
def startup():
    init_db()
    if DATA_PROVIDER=="upstox":
        try:
            upstox.load_instruments()
            if not upstox.check_token():
                log.warning("Upstox token is not usable — live quotes will fall back to NSE. "
                            "Tokens expire daily at ~03:30 IST; run get_token.py for a new one.")
        except Exception as e:
            log.error(f"Upstox init: {e}")
    load_historical_data(); load_index_data()
    store["is_market_open"]=is_market_hours(); store["initialized"]=True
    scheduler=BackgroundScheduler(timezone=IST)
    scheduler.add_job(update_prices,"interval",minutes=1,id="price_update")
    scheduler.add_job(index_update_job,"interval",minutes=5,id="index_update")
    scheduler.add_job(load_historical_data,"cron",hour=9,minute=0,day_of_week="mon-fri",id="daily_reload")
    scheduler.start()
    log.info("Server ready. Signals are exposed via /api/digest-payload for external schedulers.")

if __name__=="__main__":
    import uvicorn
    uvicorn.run("main:app",host="0.0.0.0",port=int(os.getenv("PORT",8000)))
