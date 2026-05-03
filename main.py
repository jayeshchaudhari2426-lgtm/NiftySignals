"""
Nifty 200 Live Signal Backend
Deploy on Render.com  |  Start: uvicorn main:app --host 0.0.0.0 --port $PORT

Signal Logic — Triple Confluence Band Overlap:
  CONJUNCTION = each of SMA5, EMA13, EMA26 gets its own ±1% band.
                Conjunction is TRUE when all three bands overlap each other.
                Formula: max(SMA5,EMA13,EMA26)*0.99 <= min(SMA5,EMA13,EMA26)*1.01

  BUY  → Conjunction TRUE + Volume > 1.5x 20d avg + Price > SMA5 + SMA5 rising
  SELL → Conjunction TRUE + Volume > 1.5x 20d avg + Price < SMA5 + SMA5 falling
  HOLD → Any condition fails
"""

import os, time, logging, sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import deque
import pytz, pandas as pd, requests, yfinance as yf

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from apscheduler.schedulers.background import BackgroundScheduler
from jose import jwt, JWTError

# ─── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SECRET_KEY   = os.getenv("SECRET_KEY", "niftysignals_secret_2024_changeme")
ALGORITHM    = "HS256"
TOKEN_HOURS  = 12
IST          = pytz.timezone("Asia/Kolkata")
MARKET_OPEN  = (9, 15)
MARKET_CLOSE = (15, 30)
DB_PATH      = "signals.db"
PRICE_HISTORY_LEN = 20   # sparkline points kept per stock

# ─── USERS ───────────────────────────────────────────────────────────────────
USERS: Dict[str, str] = {
    "admin@nifty.com":    "Admin@123",
    "trader1@nifty.com":  "Trade@001",
    "trader2@nifty.com":  "Trade@002",
    "trader3@nifty.com":  "Trade@003",
    "trader4@nifty.com":  "Trade@004",
    "trader5@nifty.com":  "Trade@005",
    "trader6@nifty.com":  "Trade@006",
    "trader7@nifty.com":  "Trade@007",
    "trader8@nifty.com":  "Trade@008",
    "trader9@nifty.com":  "Trade@009",
    "trader10@nifty.com": "Trade@010",
}

# ─── NIFTY 200 STOCKS ────────────────────────────────────────────────────────
STOCKS = [
    {"symbol":"HDFCBANK",   "name":"HDFC Bank",                 "sector":"Banking"},
    {"symbol":"ICICIBANK",  "name":"ICICI Bank",                "sector":"Banking"},
    {"symbol":"SBIN",       "name":"State Bank of India",       "sector":"Banking"},
    {"symbol":"KOTAKBANK",  "name":"Kotak Mahindra Bank",       "sector":"Banking"},
    {"symbol":"AXISBANK",   "name":"Axis Bank",                 "sector":"Banking"},
    {"symbol":"INDUSINDBK", "name":"IndusInd Bank",             "sector":"Banking"},
    {"symbol":"BANKBARODA", "name":"Bank of Baroda",            "sector":"Banking"},
    {"symbol":"PNB",        "name":"Punjab National Bank",      "sector":"Banking"},
    {"symbol":"CANBK",      "name":"Canara Bank",               "sector":"Banking"},
    {"symbol":"FEDERALBNK", "name":"Federal Bank",              "sector":"Banking"},
    {"symbol":"IDFCFIRSTB", "name":"IDFC First Bank",           "sector":"Banking"},
    {"symbol":"BANDHANBNK", "name":"Bandhan Bank",              "sector":"Banking"},
    {"symbol":"AUBANK",     "name":"AU Small Finance Bank",     "sector":"Banking"},
    {"symbol":"YESBANK",    "name":"Yes Bank",                  "sector":"Banking"},
    {"symbol":"UNIONBANK",  "name":"Union Bank of India",       "sector":"Banking"},
    {"symbol":"TCS",        "name":"Tata Consultancy Services", "sector":"IT"},
    {"symbol":"INFY",       "name":"Infosys",                   "sector":"IT"},
    {"symbol":"HCLTECH",    "name":"HCL Technologies",          "sector":"IT"},
    {"symbol":"WIPRO",      "name":"Wipro",                     "sector":"IT"},
    {"symbol":"TECHM",      "name":"Tech Mahindra",             "sector":"IT"},
    {"symbol":"LTIM",       "name":"LTIMindtree",               "sector":"IT"},
    {"symbol":"COFORGE",    "name":"Coforge",                   "sector":"IT"},
    {"symbol":"MPHASIS",    "name":"Mphasis",                   "sector":"IT"},
    {"symbol":"PERSISTENT", "name":"Persistent Systems",        "sector":"IT"},
    {"symbol":"LTTS",       "name":"L&T Technology Services",   "sector":"IT"},
    {"symbol":"KPITTECH",   "name":"KPIT Technologies",         "sector":"IT"},
    {"symbol":"TATAELXSI",  "name":"Tata Elxsi",               "sector":"IT"},
    {"symbol":"RELIANCE",   "name":"Reliance Industries",       "sector":"Energy"},
    {"symbol":"ONGC",       "name":"ONGC",                      "sector":"Energy"},
    {"symbol":"BPCL",       "name":"Bharat Petroleum",          "sector":"Energy"},
    {"symbol":"IOC",        "name":"Indian Oil Corp",           "sector":"Energy"},
    {"symbol":"HINDPETRO",  "name":"Hindustan Petroleum",       "sector":"Energy"},
    {"symbol":"GAIL",       "name":"GAIL India",                "sector":"Energy"},
    {"symbol":"PETRONET",   "name":"Petronet LNG",              "sector":"Energy"},
    {"symbol":"NTPC",       "name":"NTPC",                      "sector":"Power"},
    {"symbol":"POWERGRID",  "name":"Power Grid Corp",           "sector":"Power"},
    {"symbol":"TATAPOWER",  "name":"Tata Power",                "sector":"Power"},
    {"symbol":"ADANIGREEN", "name":"Adani Green Energy",        "sector":"Power"},
    {"symbol":"TORNTPOWER", "name":"Torrent Power",             "sector":"Power"},
    {"symbol":"JSWENERGY",  "name":"JSW Energy",                "sector":"Power"},
    {"symbol":"NHPC",       "name":"NHPC",                      "sector":"Power"},
    {"symbol":"SJVN",       "name":"SJVN",                      "sector":"Power"},
    {"symbol":"CESC",       "name":"CESC",                      "sector":"Power"},
    {"symbol":"HINDUNILVR", "name":"Hindustan Unilever",        "sector":"FMCG"},
    {"symbol":"ITC",        "name":"ITC",                       "sector":"FMCG"},
    {"symbol":"NESTLEIND",  "name":"Nestle India",              "sector":"FMCG"},
    {"symbol":"BRITANNIA",  "name":"Britannia Industries",      "sector":"FMCG"},
    {"symbol":"DABUR",      "name":"Dabur India",               "sector":"FMCG"},
    {"symbol":"MARICO",     "name":"Marico",                    "sector":"FMCG"},
    {"symbol":"GODREJCP",   "name":"Godrej Consumer Products",  "sector":"FMCG"},
    {"symbol":"COLPAL",     "name":"Colgate-Palmolive India",   "sector":"FMCG"},
    {"symbol":"MARUTI",     "name":"Maruti Suzuki",             "sector":"Auto"},
    {"symbol":"TATAMOTORS", "name":"Tata Motors",               "sector":"Auto"},
    {"symbol":"BAJAJ-AUTO", "name":"Bajaj Auto",                "sector":"Auto"},
    {"symbol":"HEROMOTOCO", "name":"Hero MotoCorp",             "sector":"Auto"},
    {"symbol":"M&M",        "name":"Mahindra & Mahindra",       "sector":"Auto"},
    {"symbol":"EICHERMOT",  "name":"Eicher Motors",             "sector":"Auto"},
    {"symbol":"MOTHERSON",  "name":"Samvardhana Motherson",     "sector":"Auto Anc"},
    {"symbol":"BALKRISIND", "name":"Balkrishna Industries",     "sector":"Auto Anc"},
    {"symbol":"MRF",        "name":"MRF",                       "sector":"Auto Anc"},
    {"symbol":"APOLLOTYRE", "name":"Apollo Tyres",              "sector":"Auto Anc"},
    {"symbol":"BHARATFORG", "name":"Bharat Forge",              "sector":"Auto Anc"},
    {"symbol":"BAJFINANCE", "name":"Bajaj Finance",             "sector":"Finance"},
    {"symbol":"BAJAJFINSV", "name":"Bajaj Finserv",             "sector":"Finance"},
    {"symbol":"CHOLAFIN",   "name":"Cholamandalam Finance",     "sector":"Finance"},
    {"symbol":"MUTHOOTFIN", "name":"Muthoot Finance",           "sector":"Finance"},
    {"symbol":"SBICARD",    "name":"SBI Card",                  "sector":"Finance"},
    {"symbol":"PFC",        "name":"Power Finance Corp",        "sector":"Finance"},
    {"symbol":"RECLTD",     "name":"REC Limited",               "sector":"Finance"},
    {"symbol":"IREDA",      "name":"IREDA",                     "sector":"Finance"},
    {"symbol":"HDFCLIFE",   "name":"HDFC Life Insurance",       "sector":"Insurance"},
    {"symbol":"SBILIFE",    "name":"SBI Life Insurance",        "sector":"Insurance"},
    {"symbol":"ICICIGI",    "name":"ICICI Lombard GIC",         "sector":"Insurance"},
    {"symbol":"ICICIPRULI", "name":"ICICI Prudential Life",     "sector":"Insurance"},
    {"symbol":"SUNPHARMA",  "name":"Sun Pharmaceutical",        "sector":"Pharma"},
    {"symbol":"CIPLA",      "name":"Cipla",                     "sector":"Pharma"},
    {"symbol":"DRREDDY",    "name":"Dr. Reddy's Laboratories",  "sector":"Pharma"},
    {"symbol":"DIVISLAB",   "name":"Divi's Laboratories",       "sector":"Pharma"},
    {"symbol":"ZYDUSLIFE",  "name":"Zydus Lifesciences",        "sector":"Pharma"},
    {"symbol":"AUROPHARMA", "name":"Aurobindo Pharma",          "sector":"Pharma"},
    {"symbol":"LUPIN",      "name":"Lupin",                     "sector":"Pharma"},
    {"symbol":"TORNTPHARM", "name":"Torrent Pharma",            "sector":"Pharma"},
    {"symbol":"ALKEM",      "name":"Alkem Laboratories",        "sector":"Pharma"},
    {"symbol":"GLENMARK",   "name":"Glenmark Pharma",           "sector":"Pharma"},
    {"symbol":"GRANULES",   "name":"Granules India",            "sector":"Pharma"},
    {"symbol":"APOLLOHOSP", "name":"Apollo Hospitals",          "sector":"Healthcare"},
    {"symbol":"LALPATHLAB", "name":"Dr Lal PathLabs",           "sector":"Healthcare"},
    {"symbol":"JSWSTEEL",   "name":"JSW Steel",                 "sector":"Metals"},
    {"symbol":"TATASTEEL",  "name":"Tata Steel",                "sector":"Metals"},
    {"symbol":"SAIL",       "name":"SAIL",                      "sector":"Metals"},
    {"symbol":"HINDZINC",   "name":"Hindustan Zinc",            "sector":"Metals"},
    {"symbol":"VEDL",       "name":"Vedanta",                   "sector":"Metals"},
    {"symbol":"NATIONALUM", "name":"National Aluminium",        "sector":"Metals"},
    {"symbol":"COALINDIA",  "name":"Coal India",                "sector":"Mining"},
    {"symbol":"NMDC",       "name":"NMDC",                      "sector":"Mining"},
    {"symbol":"ULTRACEMCO", "name":"UltraTech Cement",          "sector":"Cement"},
    {"symbol":"GRASIM",     "name":"Grasim Industries",         "sector":"Cement"},
    {"symbol":"AMBUJACEM",  "name":"Ambuja Cements",            "sector":"Cement"},
    {"symbol":"ACC",        "name":"ACC",                       "sector":"Cement"},
    {"symbol":"SHREECEM",   "name":"Shree Cement",              "sector":"Cement"},
    {"symbol":"KAJARIACER", "name":"Kajaria Ceramics",          "sector":"Building"},
    {"symbol":"ASTRAL",     "name":"Astral",                    "sector":"Building"},
    {"symbol":"SUPREMEIND", "name":"Supreme Industries",        "sector":"Building"},
    {"symbol":"ASIANPAINT", "name":"Asian Paints",              "sector":"Paints"},
    {"symbol":"BERGEPAINT", "name":"Berger Paints",             "sector":"Paints"},
    {"symbol":"PIDILITIND", "name":"Pidilite Industries",       "sector":"Chemicals"},
    {"symbol":"SRF",        "name":"SRF",                       "sector":"Chemicals"},
    {"symbol":"DEEPAKNTR",  "name":"Deepak Nitrite",            "sector":"Chemicals"},
    {"symbol":"NAVINFLUOR", "name":"Navin Fluorine",            "sector":"Chemicals"},
    {"symbol":"LT",         "name":"Larsen & Toubro",           "sector":"Engineering"},
    {"symbol":"ADANIPORTS", "name":"Adani Ports",               "sector":"Infrastructure"},
    {"symbol":"ADANIENT",   "name":"Adani Enterprises",         "sector":"Diversified"},
    {"symbol":"SIEMENS",    "name":"Siemens India",             "sector":"Engineering"},
    {"symbol":"ABB",        "name":"ABB India",                 "sector":"Engineering"},
    {"symbol":"BHEL",       "name":"Bharat Heavy Elec",         "sector":"Engineering"},
    {"symbol":"THERMAX",    "name":"Thermax",                   "sector":"Engineering"},
    {"symbol":"CUMMINSIND", "name":"Cummins India",             "sector":"Engineering"},
    {"symbol":"BEL",        "name":"Bharat Electronics",        "sector":"Defence"},
    {"symbol":"HAL",        "name":"Hindustan Aeronautics",     "sector":"Defence"},
    {"symbol":"MAZDOCK",    "name":"Mazagon Dock",              "sector":"Defence"},
    {"symbol":"COCHINSHIP", "name":"Cochin Shipyard",           "sector":"Defence"},
    {"symbol":"GRSE",       "name":"Garden Reach Shipbuilders", "sector":"Defence"},
    {"symbol":"TITAN",      "name":"Titan Company",             "sector":"Consumer"},
    {"symbol":"TRENT",      "name":"Trent",                     "sector":"Retail"},
    {"symbol":"DMART",      "name":"Avenue Supermarts",         "sector":"Retail"},
    {"symbol":"BATAINDIA",  "name":"Bata India",                "sector":"Retail"},
    {"symbol":"PAGEIND",    "name":"Page Industries",           "sector":"Textile"},
    {"symbol":"HAVELLS",    "name":"Havells India",             "sector":"Electronics"},
    {"symbol":"VOLTAS",     "name":"Voltas",                    "sector":"Electronics"},
    {"symbol":"POLYCAB",    "name":"Polycab India",             "sector":"Electronics"},
    {"symbol":"DIXON",      "name":"Dixon Technologies",        "sector":"Electronics"},
    {"symbol":"AMBER",      "name":"Amber Enterprises",         "sector":"Electronics"},
    {"symbol":"INDHOTEL",   "name":"Indian Hotels (Taj)",       "sector":"Hospitality"},
    {"symbol":"IRCTC",      "name":"IRCTC",                     "sector":"Travel"},
    {"symbol":"INTERGLOBE", "name":"IndiGo (InterGlobe)",       "sector":"Aviation"},
    {"symbol":"LEMONTRE",   "name":"Lemon Tree Hotels",         "sector":"Hospitality"},
    {"symbol":"VBL",        "name":"Varun Beverages",           "sector":"Beverages"},
    {"symbol":"MCDOWELL-N", "name":"United Spirits",            "sector":"Beverages"},
    {"symbol":"RADICO",     "name":"Radico Khaitan",            "sector":"Beverages"},
    {"symbol":"ZOMATO",     "name":"Zomato",                    "sector":"Tech"},
    {"symbol":"PAYTM",      "name":"Paytm",                     "sector":"Fintech"},
    {"symbol":"NYKAA",      "name":"Nykaa",                     "sector":"E-Commerce"},
    {"symbol":"DELHIVERY",  "name":"Delhivery",                 "sector":"Logistics"},
    {"symbol":"DLF",        "name":"DLF",                       "sector":"Real Estate"},
    {"symbol":"GODREJPROP", "name":"Godrej Properties",         "sector":"Real Estate"},
    {"symbol":"OBEROIRLTY", "name":"Oberoi Realty",             "sector":"Real Estate"},
    {"symbol":"PRESTIGE",   "name":"Prestige Estates",          "sector":"Real Estate"},
    {"symbol":"PHOENIXLTD", "name":"Phoenix Mills",             "sector":"Real Estate"},
    {"symbol":"BRIGADE",    "name":"Brigade Enterprises",       "sector":"Real Estate"},
    {"symbol":"JUBLFOOD",   "name":"Jubilant FoodWorks",        "sector":"QSR"},
    {"symbol":"GODREJIND",  "name":"Godrej Industries",         "sector":"Diversified"},
    {"symbol":"TATACOMM",   "name":"Tata Communications",       "sector":"Telecom"},
    {"symbol":"HFCL",       "name":"HFCL",                      "sector":"Telecom"},
    {"symbol":"AIAENG",     "name":"AIA Engineering",           "sector":"Industrials"},
    {"symbol":"GRINDWELL",  "name":"Grindwell Norton",          "sector":"Industrials"},
]

_seen = set()
_dedup = []
for s in STOCKS:
    if s["symbol"] not in _seen:
        _seen.add(s["symbol"])
        _dedup.append(s)
STOCKS = _dedup
STOCK_MAP: Dict[str, dict] = {s["symbol"]: s for s in STOCKS}

INDICES_LIST = [
    {"symbol":"^NSEI",      "name":"NIFTY 50"},
    {"symbol":"^NSEBANK",   "name":"NIFTY BANK"},
    {"symbol":"^CNXIT",     "name":"NIFTY IT"},
    {"symbol":"^CNXPHARMA", "name":"NIFTY PHARMA"},
    {"symbol":"^CNXAUTO",   "name":"NIFTY AUTO"},
]

# ─── IN-MEMORY STORE ─────────────────────────────────────────────────────────
store: Dict[str, Any] = {
    "last_update":    "Not yet updated",
    "is_market_open": False,
    "initialized":    False,
    "stocks":         {},
    "ohlcv":          {},
    "indices":        {},
    "price_history":  {},  # symbol → deque of last 20 prices
}

# ─── DATABASE ─────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS signal_history (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol  TEXT NOT NULL,
            signal  TEXT NOT NULL,
            date    TEXT NOT NULL,
            price   REAL NOT NULL,
            ts      TEXT NOT NULL
        )
    """)
    con.commit(); con.close()

def save_signal(symbol, signal, date, price):
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO signal_history (symbol,signal,date,price,ts) VALUES (?,?,?,?,?)",
                (symbol, signal, date, price, ts))
    con.commit(); con.close()

def get_history(limit=500):
    con = sqlite3.connect(DB_PATH)
    cur = con.execute("SELECT symbol,signal,date,price,ts FROM signal_history ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall(); con.close()
    result = []
    for r in rows:
        sym = r[0]; entry = STOCK_MAP.get(sym, {})
        cur_price = store["stocks"].get(sym, {}).get("current_price", r[3])
        pct = round((cur_price - r[3]) / r[3] * 100, 2) if r[3] else 0
        result.append({"symbol":sym,"name":entry.get("name",sym),"sector":entry.get("sector",""),
                        "signal":r[1],"signal_date":r[2],"signal_price":r[3],
                        "current_price":cur_price,"pct_change":pct,"timestamp":r[4]})
    return result

# ─── NSE ─────────────────────────────────────────────────────────────────────
_nse_session: Optional[requests.Session] = None
_nse_refresh_ts: float = 0

def get_nse_session():
    global _nse_session, _nse_refresh_ts
    if _nse_session is None or (time.time() - _nse_refresh_ts) > 300:
        s = requests.Session()
        s.headers.update({
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept":"*/*","Accept-Language":"en-US,en;q=0.9",
            "Referer":"https://www.nseindia.com","Connection":"keep-alive",
        })
        try:
            s.get("https://www.nseindia.com", timeout=15); time.sleep(0.5)
        except: pass
        _nse_session = s; _nse_refresh_ts = time.time()
    return _nse_session

def fetch_nse_prices():
    s = get_nse_session()
    r = s.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20200", timeout=15)
    r.raise_for_status()
    prices = {}
    for item in r.json().get("data", []):
        sym = item.get("symbol",""); ltp = item.get("lastPrice",0)
        if sym and ltp:
            prices[sym] = float(str(ltp).replace(",",""))
    return prices

# ─── INDICATORS ──────────────────────────────────────────────────────────────
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); df.sort_index(inplace=True)
    df["SMA5"]    = df["Close"].rolling(5).mean().round(2)
    df["EMA13"]   = df["Close"].ewm(span=13, adjust=False).mean().round(2)
    df["EMA26"]   = df["Close"].ewm(span=26, adjust=False).mean().round(2)
    df["VolSMA20"] = df["Volume"].rolling(20).mean().round(0)

    # ── CONJUNCTION: each line's ±1% band must overlap all others ──────────
    # Band overlap condition: max(SMA5,EMA13,EMA26)*0.99 <= min(SMA5,EMA13,EMA26)*1.01
    df["MaxInd"] = df[["SMA5","EMA13","EMA26"]].max(axis=1)
    df["MinInd"] = df[["SMA5","EMA13","EMA26"]].min(axis=1)
    df["Conjunction"] = (df["MaxInd"] * 0.99) <= (df["MinInd"] * 1.01)

    # Volume confirmation
    df["VolConfirm"] = df["Volume"] > (df["VolSMA20"] * 1.5)

    # SMA5 direction: today vs 2 days ago
    df["SMA5_Rising"]  = df["SMA5"] > df["SMA5"].shift(2)
    df["SMA5_Falling"] = df["SMA5"] < df["SMA5"].shift(2)

    # BUY: conjunction + vol + price above SMA5 + SMA5 rising
    df["BuySignal"] = (
        df["Conjunction"] &
        df["VolConfirm"] &
        (df["Close"] > df["SMA5"]) &
        df["SMA5_Rising"]
    )

    # SELL: conjunction + vol + price below SMA5 + SMA5 falling
    df["SellSignal"] = (
        df["Conjunction"] &
        df["VolConfirm"] &
        (df["Close"] < df["SMA5"]) &
        df["SMA5_Falling"]
    )
    return df

def detect_current_signal(df):
    result = {"signal":"HOLD","signal_date":None,"signal_price":None}
    for i in range(len(df)-1, -1, -1):
        row = df.iloc[i]
        if row.get("BuySignal", False):
            result = {"signal":"BUY","signal_date":df.index[i].strftime("%Y-%m-%d"),"signal_price":float(row["Close"])}
            break
        if row.get("SellSignal", False):
            result = {"signal":"SELL","signal_date":df.index[i].strftime("%Y-%m-%d"),"signal_price":float(row["Close"])}
            break
    return result

# ─── HISTORICAL LOAD ──────────────────────────────────────────────────────────
def load_historical_data():
    log.info("Loading historical data from Yahoo Finance...")
    symbols = [s["symbol"] for s in STOCKS]
    yf_syms = [f"{sym}.NS" for sym in symbols]

    for b in range(0, len(yf_syms), 50):
        batch_yf  = yf_syms[b:b+50]
        batch_sym = symbols[b:b+50]
        log.info(f"Batch {b//50+1}: {len(batch_yf)} stocks")
        try:
            raw = yf.download(batch_yf, period="90d", interval="1d",
                              group_by="ticker", auto_adjust=True, progress=False, threads=True)
            for sym, yf_sym in zip(batch_sym, batch_yf):
                try:
                    df = raw.copy() if len(batch_yf)==1 else (raw[yf_sym].copy() if yf_sym in raw.columns.get_level_values(0) else None)
                    if df is None: continue
                    df = df.dropna(subset=["Close"])
                    if df.empty: continue
                    df  = compute_indicators(df)
                    sig = detect_current_signal(df)
                    last = df.iloc[-1]

                    def v(x): return round(float(x),2) if not pd.isna(x) else None

                    store["stocks"][sym] = {
                        "symbol":        sym,
                        "name":          STOCK_MAP[sym]["name"],
                        "sector":        STOCK_MAP[sym]["sector"],
                        "current_price": v(last["Close"]),
                        "signal":        sig["signal"],
                        "signal_date":   sig["signal_date"],
                        "signal_price":  sig["signal_price"],
                        "pct_change":    0.0,
                        "sma5":          v(last["SMA5"]),
                        "ema13":         v(last["EMA13"]),
                        "ema26":         v(last["EMA26"]),
                        "volsma20":      v(last["VolSMA20"]),
                        "conjunction":   bool(last.get("Conjunction", False)),
                        "vol_confirm":   bool(last.get("VolConfirm", False)),
                    }
                    store["ohlcv"][sym] = df

                    # Seed price history from last 20 closing prices
                    closes = df["Close"].dropna().tolist()[-PRICE_HISTORY_LEN:]
                    store["price_history"][sym] = deque(closes, maxlen=PRICE_HISTORY_LEN)

                    if sig["signal_price"] and v(last["Close"]):
                        sp = sig["signal_price"]; cp = v(last["Close"])
                        store["stocks"][sym]["pct_change"] = round((cp-sp)/sp*100, 2)
                except Exception as ex:
                    log.warning(f"{sym} failed: {ex}")
        except Exception as e:
            log.error(f"Batch failed: {e}")
        time.sleep(1)

    log.info(f"Done. {len(store['stocks'])} stocks ready.")

def load_index_data():
    try:
        for idx in INDICES_LIST:
            ticker = yf.Ticker(idx["symbol"])
            hist   = ticker.history(period="2d")
            if len(hist) >= 1:
                price = float(hist["Close"].iloc[-1])
                prev  = float(hist["Close"].iloc[-2]) if len(hist)>=2 else price
                pct   = round((price-prev)/prev*100, 2) if prev else 0
                store["indices"][idx["symbol"]] = {
                    "symbol":idx["symbol"],"name":idx["name"],
                    "price":round(price,2),"prev_close":round(prev,2),"pct_change":pct,
                }
    except Exception as e:
        log.warning(f"Index load failed: {e}")

# ─── MARKET HOURS ─────────────────────────────────────────────────────────────
def is_market_hours():
    now = datetime.now(IST)
    if now.weekday() >= 5: return False
    return MARKET_OPEN <= (now.hour, now.minute) <= MARKET_CLOSE

# ─── PRICE UPDATE JOB ─────────────────────────────────────────────────────────
def update_prices():
    store["is_market_open"] = is_market_hours()
    if not store["is_market_open"]: return

    try:
        prices = fetch_nse_prices()
        log.info(f"NSE: {len(prices)} prices")
    except Exception as e:
        log.error(f"NSE failed: {e}"); return

    now_ist = datetime.now(IST)
    today   = now_ist.strftime("%Y-%m-%d")

    for sym, entry in store["stocks"].items():
        ltp = prices.get(sym)
        if ltp is None: continue

        entry["current_price"] = ltp

        # Update price history for sparkline
        hist = store["price_history"].get(sym)
        if hist is not None:
            hist.append(ltp)

        sp = entry.get("signal_price")
        if sp: entry["pct_change"] = round((ltp-sp)/sp*100, 2)

        df = store["ohlcv"].get(sym)
        if df is None or df.empty: continue

        idx_dates = df.index.strftime("%Y-%m-%d").tolist()
        if today in idx_dates:
            li = df.index[-1]
            df.at[li,"Close"] = ltp
            df.at[li,"High"]  = max(df.at[li,"High"], ltp)
            df.at[li,"Low"]   = min(df.at[li,"Low"],  ltp)
        else:
            new_row = pd.DataFrame({"Open":[ltp],"High":[ltp],"Low":[ltp],"Close":[ltp],"Volume":[0]},
                                   index=[pd.Timestamp(today)])
            df = pd.concat([df, new_row])

        df = compute_indicators(df)
        store["ohlcv"][sym] = df
        last = df.iloc[-1]

        def v(x): return round(float(x),2) if not pd.isna(x) else None
        entry["sma5"]       = v(last["SMA5"])
        entry["ema13"]      = v(last["EMA13"])
        entry["ema26"]      = v(last["EMA26"])
        entry["volsma20"]   = v(last["VolSMA20"])
        entry["conjunction"]= bool(last.get("Conjunction", False))
        entry["vol_confirm"]= bool(last.get("VolConfirm", False))

        prev_sig = entry.get("signal")
        if df["BuySignal"].iloc[-1] and prev_sig != "BUY":
            entry.update({"signal":"BUY","signal_date":today,"signal_price":ltp,"pct_change":0.0})
            save_signal(sym,"BUY",today,ltp); log.info(f"BUY {sym}@{ltp}")
        elif df["SellSignal"].iloc[-1] and prev_sig != "SELL":
            entry.update({"signal":"SELL","signal_date":today,"signal_price":ltp,"pct_change":0.0})
            save_signal(sym,"SELL",today,ltp); log.info(f"SELL {sym}@{ltp}")

    store["last_update"] = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")

def index_update_job():
    if is_market_hours(): load_index_data()

# ─── AUTH ─────────────────────────────────────────────────────────────────────
security = HTTPBearer()

def create_token(email):
    expire = datetime.utcnow() + timedelta(hours=TOKEN_HOURS)
    return jwt.encode({"sub":email,"exp":expire}, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(creds: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(creds.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub","")
        if not email: raise HTTPException(status_code=401, detail="Invalid token")
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Token expired or invalid")

# ─── FASTAPI ──────────────────────────────────────────────────────────────────
app = FastAPI(title="Nifty200 Signals API", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

@app.post("/api/login")
def login(body: dict):
    email = body.get("email","").strip().lower()
    password = body.get("password","")
    if USERS.get(email) != password:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"token": create_token(email), "email": email}

@app.get("/api/health")
def health():
    return {"status":"ok","initialized":store["initialized"],
            "last_update":store["last_update"],"is_market_open":store["is_market_open"],
            "total_stocks":len(store["stocks"])}

@app.get("/api/signals")
def get_signals(email: str = Depends(verify_token)):
    order = {"BUY":0,"SELL":1,"HOLD":2}
    stocks_list = list(store["stocks"].values())
    stocks_list.sort(key=lambda x: order.get(x.get("signal","HOLD"),2))
    # Attach price history (sparkline)
    for s in stocks_list:
        sym  = s["symbol"]
        hist = store["price_history"].get(sym)
        s["price_history"] = list(hist) if hist else []
    return {"last_update":store["last_update"],"is_market_open":store["is_market_open"],
            "total":len(stocks_list),"stocks":stocks_list}

@app.get("/api/history")
def get_history_endpoint(email: str = Depends(verify_token)):
    return {"history": get_history(500)}

@app.get("/api/indices")
def get_indices(email: str = Depends(verify_token)):
    return {"indices": list(store["indices"].values())}

@app.get("/api/stock/{symbol}")
def get_stock_detail(symbol: str, email: str = Depends(verify_token)):
    symbol = symbol.upper()
    if symbol not in store["ohlcv"]:
        raise HTTPException(status_code=404, detail="Symbol not found")
    df = store["ohlcv"][symbol].copy().tail(60)
    rows = []
    for dt, row in df.iterrows():
        def v(x): return None if pd.isna(x) else round(float(x),2)
        rows.append({
            "date":        dt.strftime("%Y-%m-%d"),
            "open":        v(row["Open"]),"high":v(row["High"]),
            "low":         v(row["Low"]), "close":v(row["Close"]),
            "volume":      int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
            "sma5":        v(row.get("SMA5")),
            "ema13":       v(row.get("EMA13")),
            "ema26":       v(row.get("EMA26")),
            "volsma20":    v(row.get("VolSMA20")),
            "conjunction": bool(row.get("Conjunction", False)),
            "vol_confirm": bool(row.get("VolConfirm", False)),
            "buy_signal":  bool(row.get("BuySignal",  False)),
            "sell_signal": bool(row.get("SellSignal", False)),
        })
    stock_info = store["stocks"].get(symbol, STOCK_MAP.get(symbol,{}))
    hist = store["price_history"].get(symbol)
    stock_info["price_history"] = list(hist) if hist else []
    return {"symbol":symbol,"info":stock_info,"ohlcv":rows}

@app.on_event("startup")
def startup():
    init_db()
    load_historical_data()
    load_index_data()
    store["is_market_open"] = is_market_hours()
    store["initialized"]    = True
    scheduler = BackgroundScheduler(timezone=IST)
    scheduler.add_job(update_prices,        "interval", minutes=1,  id="price_update")
    scheduler.add_job(index_update_job,     "interval", minutes=5,  id="index_update")
    scheduler.add_job(load_historical_data, "cron", hour=9, minute=0, day_of_week="mon-fri", id="daily_reload")
    scheduler.start()
    log.info("Server ready.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT",8000)))
