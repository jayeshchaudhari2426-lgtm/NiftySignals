"""
Nifty 200 Live Signal Backend
Deploy on Render.com  |  Start: uvicorn main:app --host 0.0.0.0 --port $PORT
Signal Logic: EMA13 crosses EMA26 → BUY/SELL | SMA5 as trend filter
"""

import os, time, logging, sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pytz, numpy as np, pandas as pd, requests, yfinance as yf

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from apscheduler.schedulers.background import BackgroundScheduler
from jose import jwt, JWTError

# ─── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SECRET_KEY  = os.getenv("SECRET_KEY", "niftysignals_secret_2024_changeme")
ALGORITHM   = "HS256"
TOKEN_HOURS = 12
IST         = pytz.timezone("Asia/Kolkata")
MARKET_OPEN  = (9, 15)
MARKET_CLOSE = (15, 30)
DB_PATH = "signals.db"

# ─── USERS  (edit freely — email: password) ──────────────────────────────────
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
    # BANKING
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
    # IT
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
    # ENERGY / OIL / POWER
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
    # FMCG
    {"symbol":"HINDUNILVR", "name":"Hindustan Unilever",        "sector":"FMCG"},
    {"symbol":"ITC",        "name":"ITC",                       "sector":"FMCG"},
    {"symbol":"NESTLEIND",  "name":"Nestle India",              "sector":"FMCG"},
    {"symbol":"BRITANNIA",  "name":"Britannia Industries",      "sector":"FMCG"},
    {"symbol":"DABUR",      "name":"Dabur India",               "sector":"FMCG"},
    {"symbol":"MARICO",     "name":"Marico",                    "sector":"FMCG"},
    {"symbol":"GODREJCP",   "name":"Godrej Consumer Products",  "sector":"FMCG"},
    {"symbol":"COLPAL",     "name":"Colgate-Palmolive India",   "sector":"FMCG"},
    # AUTO
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
    # FINANCE / INSURANCE
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
    # PHARMA / HEALTHCARE
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
    # METALS / MINING
    {"symbol":"JSWSTEEL",   "name":"JSW Steel",                 "sector":"Metals"},
    {"symbol":"TATASTEEL",  "name":"Tata Steel",                "sector":"Metals"},
    {"symbol":"SAIL",       "name":"SAIL",                      "sector":"Metals"},
    {"symbol":"HINDZINC",   "name":"Hindustan Zinc",            "sector":"Metals"},
    {"symbol":"VEDL",       "name":"Vedanta",                   "sector":"Metals"},
    {"symbol":"NATIONALUM", "name":"National Aluminium",        "sector":"Metals"},
    {"symbol":"COALINDIA",  "name":"Coal India",                "sector":"Mining"},
    {"symbol":"NMDC",       "name":"NMDC",                      "sector":"Mining"},
    # CEMENT / BUILDING
    {"symbol":"ULTRACEMCO", "name":"UltraTech Cement",          "sector":"Cement"},
    {"symbol":"GRASIM",     "name":"Grasim Industries",         "sector":"Cement"},
    {"symbol":"AMBUJACEM",  "name":"Ambuja Cements",            "sector":"Cement"},
    {"symbol":"ACC",        "name":"ACC",                       "sector":"Cement"},
    {"symbol":"SHREECEM",   "name":"Shree Cement",              "sector":"Cement"},
    {"symbol":"KAJARIACER", "name":"Kajaria Ceramics",          "sector":"Building"},
    {"symbol":"ASTRAL",     "name":"Astral",                    "sector":"Building"},
    {"symbol":"SUPREMEIND", "name":"Supreme Industries",        "sector":"Building"},
    # PAINTS / CHEMICALS
    {"symbol":"ASIANPAINT", "name":"Asian Paints",              "sector":"Paints"},
    {"symbol":"BERGEPAINT", "name":"Berger Paints",             "sector":"Paints"},
    {"symbol":"PIDILITIND", "name":"Pidilite Industries",       "sector":"Chemicals"},
    {"symbol":"SRF",        "name":"SRF",                       "sector":"Chemicals"},
    {"symbol":"DEEPAKNTR",  "name":"Deepak Nitrite",            "sector":"Chemicals"},
    {"symbol":"NAVINFLUOR", "name":"Navin Fluorine",            "sector":"Chemicals"},
    # ENGINEERING / INFRA
    {"symbol":"LT",         "name":"Larsen & Toubro",           "sector":"Engineering"},
    {"symbol":"ADANIPORTS", "name":"Adani Ports",               "sector":"Infrastructure"},
    {"symbol":"ADANIENT",   "name":"Adani Enterprises",         "sector":"Diversified"},
    {"symbol":"SIEMENS",    "name":"Siemens India",             "sector":"Engineering"},
    {"symbol":"ABB",        "name":"ABB India",                 "sector":"Engineering"},
    {"symbol":"BHEL",       "name":"Bharat Heavy Elec",         "sector":"Engineering"},
    {"symbol":"THERMAX",    "name":"Thermax",                   "sector":"Engineering"},
    {"symbol":"CUMMINSIND", "name":"Cummins India",             "sector":"Engineering"},
    # DEFENCE
    {"symbol":"BEL",        "name":"Bharat Electronics",        "sector":"Defence"},
    {"symbol":"HAL",        "name":"Hindustan Aeronautics",     "sector":"Defence"},
    {"symbol":"MAZDOCK",    "name":"Mazagon Dock",              "sector":"Defence"},
    {"symbol":"COCHINSHIP", "name":"Cochin Shipyard",           "sector":"Defence"},
    {"symbol":"GRSE",       "name":"Garden Reach Shipbuilders", "sector":"Defence"},
    # CONSUMER / RETAIL
    {"symbol":"TITAN",      "name":"Titan Company",             "sector":"Consumer"},
    {"symbol":"TRENT",      "name":"Trent",                     "sector":"Retail"},
    {"symbol":"DMART",      "name":"Avenue Supermarts",         "sector":"Retail"},
    {"symbol":"BATAINDIA",  "name":"Bata India",                "sector":"Retail"},
    {"symbol":"PAGEIND",    "name":"Page Industries",           "sector":"Textile"},
    # ELECTRONICS
    {"symbol":"HAVELLS",    "name":"Havells India",             "sector":"Electronics"},
    {"symbol":"VOLTAS",     "name":"Voltas",                    "sector":"Electronics"},
    {"symbol":"POLYCAB",    "name":"Polycab India",             "sector":"Electronics"},
    {"symbol":"DIXON",      "name":"Dixon Technologies",        "sector":"Electronics"},
    {"symbol":"AMBER",      "name":"Amber Enterprises",         "sector":"Electronics"},
    # HOSPITALITY / TRAVEL / AVIATION
    {"symbol":"INDHOTEL",   "name":"Indian Hotels (Taj)",       "sector":"Hospitality"},
    {"symbol":"IRCTC",      "name":"IRCTC",                     "sector":"Travel"},
    {"symbol":"INTERGLOBE", "name":"IndiGo (InterGlobe)",       "sector":"Aviation"},
    {"symbol":"LEMONTRE",   "name":"Lemon Tree Hotels",         "sector":"Hospitality"},
    # BEVERAGES
    {"symbol":"VBL",        "name":"Varun Beverages",           "sector":"Beverages"},
    {"symbol":"MCDOWELL-N", "name":"United Spirits",            "sector":"Beverages"},
    {"symbol":"RADICO",     "name":"Radico Khaitan",            "sector":"Beverages"},
    # NEW-AGE TECH
    {"symbol":"ZOMATO",     "name":"Zomato",                    "sector":"Tech"},
    {"symbol":"PAYTM",      "name":"Paytm",                     "sector":"Fintech"},
    {"symbol":"NYKAA",      "name":"Nykaa",                     "sector":"E-Commerce"},
    {"symbol":"DELHIVERY",  "name":"Delhivery",                 "sector":"Logistics"},
    # REAL ESTATE
    {"symbol":"DLF",        "name":"DLF",                       "sector":"Real Estate"},
    {"symbol":"GODREJPROP", "name":"Godrej Properties",         "sector":"Real Estate"},
    {"symbol":"OBEROIRLTY", "name":"Oberoi Realty",             "sector":"Real Estate"},
    {"symbol":"PRESTIGE",   "name":"Prestige Estates",          "sector":"Real Estate"},
    {"symbol":"PHOENIXLTD", "name":"Phoenix Mills",             "sector":"Real Estate"},
    {"symbol":"BRIGADE",    "name":"Brigade Enterprises",       "sector":"Real Estate"},
    # MISC
    {"symbol":"JUBLFOOD",   "name":"Jubilant FoodWorks",        "sector":"QSR"},
    {"symbol":"GODREJIND",  "name":"Godrej Industries",         "sector":"Diversified"},
    {"symbol":"TATACOMM",   "name":"Tata Communications",       "sector":"Telecom"},
    {"symbol":"HFCL",       "name":"HFCL",                      "sector":"Telecom"},
    {"symbol":"AIAENG",     "name":"AIA Engineering",           "sector":"Industrials"},
    {"symbol":"GRINDWELL",  "name":"Grindwell Norton",          "sector":"Industrials"},
]

# De-duplicate by symbol
_seen = set()
_STOCKS_DEDUP = []
for s in STOCKS:
    if s["symbol"] not in _seen:
        _seen.add(s["symbol"])
        _STOCKS_DEDUP.append(s)
STOCKS = _STOCKS_DEDUP

STOCK_MAP: Dict[str, dict] = {s["symbol"]: s for s in STOCKS}

# ─── NIFTY INDICES ────────────────────────────────────────────────────────────
INDICES_LIST = [
    {"symbol": "^NSEI",     "name": "NIFTY 50"},
    {"symbol": "^NSEBANK",  "name": "NIFTY BANK"},
    {"symbol": "^CNXIT",    "name": "NIFTY IT"},
    {"symbol": "^CNXPHARMA","name": "NIFTY PHARMA"},
    {"symbol": "^CNXAUTO",  "name": "NIFTY AUTO"},
]

# ─── IN-MEMORY STORE ─────────────────────────────────────────────────────────
store: Dict[str, Any] = {
    "last_update":    "Not yet updated",
    "is_market_open": False,
    "initialized":    False,
    "stocks":         {},
    "ohlcv":          {},
    "indices":        {},
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
    con.commit()
    con.close()

def save_signal(symbol: str, signal: str, date: str, price: float):
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO signal_history (symbol, signal, date, price, ts) VALUES (?,?,?,?,?)",
        (symbol, signal, date, price, ts)
    )
    con.commit()
    con.close()

def get_history(limit: int = 500) -> List[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.execute(
        "SELECT symbol, signal, date, price, ts FROM signal_history ORDER BY id DESC LIMIT ?",
        (limit,)
    )
    rows = cur.fetchall()
    con.close()
    result = []
    for r in rows:
        sym   = r[0]
        entry = STOCK_MAP.get(sym, {})
        cur_price = store["stocks"].get(sym, {}).get("current_price", r[3])
        pct = round((cur_price - r[3]) / r[3] * 100, 2) if r[3] else 0
        result.append({
            "symbol":       sym,
            "name":         entry.get("name", sym),
            "sector":       entry.get("sector", ""),
            "signal":       r[1],
            "signal_date":  r[2],
            "signal_price": r[3],
            "current_price":cur_price,
            "pct_change":   pct,
            "timestamp":    r[4],
        })
    return result

# ─── NSE SESSION ──────────────────────────────────────────────────────────────
_nse_session: Optional[requests.Session] = None
_nse_refresh_ts: float = 0

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com",
    "Connection":      "keep-alive",
}

def get_nse_session() -> requests.Session:
    global _nse_session, _nse_refresh_ts
    if _nse_session is None or (time.time() - _nse_refresh_ts) > 300:
        s = requests.Session()
        s.headers.update(NSE_HEADERS)
        try:
            s.get("https://www.nseindia.com", timeout=15)
            time.sleep(0.5)
            _nse_session    = s
            _nse_refresh_ts = time.time()
            log.info("NSE session refreshed")
        except Exception as e:
            log.warning(f"NSE session init failed: {e}")
            _nse_session = s
    return _nse_session

def fetch_nse_prices() -> Dict[str, float]:
    session = get_nse_session()
    url  = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20200"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    prices: Dict[str, float] = {}
    for item in data.get("data", []):
        sym = item.get("symbol", "")
        ltp = item.get("lastPrice", 0)
        if sym and ltp:
            prices[sym] = float(str(ltp).replace(",", ""))
    return prices

# ─── INDICATOR COMPUTATION ────────────────────────────────────────────────────
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.sort_index(inplace=True)
    df["SMA5"]    = df["Close"].rolling(5).mean().round(2)
    df["EMA13"]   = df["Close"].ewm(span=13, adjust=False).mean().round(2)
    df["EMA26"]   = df["Close"].ewm(span=26, adjust=False).mean().round(2)
    df["VolSMA5"] = df["Volume"].rolling(5).mean().round(0)
    # Signal: EMA13 crosses EMA26
    df["BuySignal"]  = (df["EMA13"] > df["EMA26"]) & (df["EMA13"].shift(1) <= df["EMA26"].shift(1))
    df["SellSignal"] = (df["EMA13"] < df["EMA26"]) & (df["EMA13"].shift(1) >= df["EMA26"].shift(1))
    return df

def detect_current_signal(df: pd.DataFrame) -> dict:
    result = {"signal": "HOLD", "signal_date": None, "signal_price": None}
    for i in range(len(df) - 1, -1, -1):
        row = df.iloc[i]
        if row.get("BuySignal", False):
            result["signal"]       = "BUY"
            result["signal_date"]  = df.index[i].strftime("%Y-%m-%d")
            result["signal_price"] = float(row["Close"])
            break
        if row.get("SellSignal", False):
            result["signal"]       = "SELL"
            result["signal_date"]  = df.index[i].strftime("%Y-%m-%d")
            result["signal_price"] = float(row["Close"])
            break
    return result

# ─── HISTORICAL DATA LOAD ─────────────────────────────────────────────────────
def load_historical_data():
    log.info("Loading historical data from Yahoo Finance...")
    symbols    = [s["symbol"] for s in STOCKS]
    yf_syms    = [f"{sym}.NS" for sym in symbols]
    batch_size = 50

    for batch_start in range(0, len(yf_syms), batch_size):
        batch_yf  = yf_syms[batch_start : batch_start + batch_size]
        batch_sym = symbols[batch_start : batch_start + batch_size]
        log.info(f"Downloading batch {batch_start//batch_size + 1}: {len(batch_yf)} stocks")
        try:
            raw = yf.download(
                batch_yf,
                period="90d",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            for sym, yf_sym in zip(batch_sym, batch_yf):
                try:
                    if len(batch_yf) == 1:
                        df = raw.copy()
                    else:
                        if yf_sym not in raw.columns.get_level_values(0):
                            continue
                        df = raw[yf_sym].copy()
                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        continue
                    df  = compute_indicators(df)
                    sig = detect_current_signal(df)
                    last = df.iloc[-1]
                    store["stocks"][sym] = {
                        "symbol":        sym,
                        "name":          STOCK_MAP[sym]["name"],
                        "sector":        STOCK_MAP[sym]["sector"],
                        "current_price": float(last["Close"]),
                        "signal":        sig["signal"],
                        "signal_date":   sig["signal_date"],
                        "signal_price":  sig["signal_price"],
                        "pct_change":    0.0,
                        "sma5":    round(float(last["SMA5"]),    2) if not pd.isna(last["SMA5"])    else None,
                        "ema13":   round(float(last["EMA13"]),   2) if not pd.isna(last["EMA13"])   else None,
                        "ema26":   round(float(last["EMA26"]),   2) if not pd.isna(last["EMA26"])   else None,
                        "volsma5": round(float(last["VolSMA5"]), 0) if not pd.isna(last["VolSMA5"]) else None,
                    }
                    store["ohlcv"][sym] = df
                    if sig["signal_price"]:
                        cp = float(last["Close"])
                        sp = sig["signal_price"]
                        store["stocks"][sym]["pct_change"] = round((cp - sp) / sp * 100, 2)
                except Exception as ex:
                    log.warning(f"Processing {sym} failed: {ex}")
        except Exception as e:
            log.error(f"Batch download failed: {e}")
        time.sleep(1)

    log.info(f"Historical load complete. {len(store['stocks'])} stocks ready.")

def load_index_data():
    try:
        for idx in INDICES_LIST:
            ticker = yf.Ticker(idx["symbol"])
            hist   = ticker.history(period="2d")
            if len(hist) >= 1:
                price = float(hist["Close"].iloc[-1])
                prev  = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else price
                pct   = round((price - prev) / prev * 100, 2) if prev else 0
                store["indices"][idx["symbol"]] = {
                    "symbol":     idx["symbol"],
                    "name":       idx["name"],
                    "price":      round(price, 2),
                    "prev_close": round(prev,  2),
                    "pct_change": pct,
                }
    except Exception as e:
        log.warning(f"Index load failed: {e}")

# ─── MARKET HOURS ─────────────────────────────────────────────────────────────
def is_market_hours() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    t = (now.hour, now.minute)
    return MARKET_OPEN <= t <= MARKET_CLOSE

# ─── SCHEDULER JOB ────────────────────────────────────────────────────────────
def update_prices():
    store["is_market_open"] = is_market_hours()
    if not store["is_market_open"]:
        return

    prices: Dict[str, float] = {}
    try:
        prices = fetch_nse_prices()
        log.info(f"NSE: got prices for {len(prices)} stocks")
    except Exception as e:
        log.error(f"NSE fetch failed: {e}")

    now_ist = datetime.now(IST)
    today   = now_ist.strftime("%Y-%m-%d")

    for sym, entry in store["stocks"].items():
        ltp = prices.get(sym)
        if ltp is None:
            continue

        entry["current_price"] = ltp

        sp = entry.get("signal_price")
        if sp:
            entry["pct_change"] = round((ltp - sp) / sp * 100, 2)

        df = store["ohlcv"].get(sym)
        if df is None or df.empty:
            continue

        idx_dates = df.index.strftime("%Y-%m-%d").tolist()
        if today in idx_dates:
            last_idx = df.index[-1]
            df.at[last_idx, "Close"] = ltp
            df.at[last_idx, "High"]  = max(df.at[last_idx, "High"], ltp)
            df.at[last_idx, "Low"]   = min(df.at[last_idx, "Low"],  ltp)
        else:
            new_row = pd.DataFrame(
                {"Open": [ltp], "High": [ltp], "Low": [ltp], "Close": [ltp], "Volume": [0]},
                index=[pd.Timestamp(today)]
            )
            df = pd.concat([df, new_row])

        df = compute_indicators(df)
        store["ohlcv"][sym] = df

        last = df.iloc[-1]
        entry["sma5"]    = round(float(last["SMA5"]),    2) if not pd.isna(last["SMA5"])    else None
        entry["ema13"]   = round(float(last["EMA13"]),   2) if not pd.isna(last["EMA13"])   else None
        entry["ema26"]   = round(float(last["EMA26"]),   2) if not pd.isna(last["EMA26"])   else None
        entry["volsma5"] = round(float(last["VolSMA5"]), 0) if not pd.isna(last["VolSMA5"]) else None

        if df["BuySignal"].iloc[-1] and entry.get("signal") != "BUY":
            entry["signal"]       = "BUY"
            entry["signal_date"]  = today
            entry["signal_price"] = ltp
            entry["pct_change"]   = 0.0
            save_signal(sym, "BUY", today, ltp)
            log.info(f"NEW BUY: {sym} @ {ltp}")
        elif df["SellSignal"].iloc[-1] and entry.get("signal") != "SELL":
            entry["signal"]       = "SELL"
            entry["signal_date"]  = today
            entry["signal_price"] = ltp
            entry["pct_change"]   = 0.0
            save_signal(sym, "SELL", today, ltp)
            log.info(f"NEW SELL: {sym} @ {ltp}")

    store["last_update"] = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")

def index_update_job():
    if is_market_hours():
        load_index_data()

# ─── AUTH ─────────────────────────────────────────────────────────────────────
security = HTTPBearer()

def create_token(email: str) -> str:
    expire = datetime.utcnow() + timedelta(hours=TOKEN_HOURS)
    return jwt.encode({"sub": email, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(creds: HTTPAuthorizationCredentials = Depends(security)) -> str:
    try:
        payload = jwt.decode(creds.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub", "")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Token expired or invalid")

# ─── FASTAPI APP ──────────────────────────────────────────────────────────────
app = FastAPI(title="Nifty200 Signals API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/login")
def login(body: dict):
    email    = body.get("email", "").strip().lower()
    password = body.get("password", "")
    if USERS.get(email) != password:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"token": create_token(email), "email": email}

@app.get("/api/health")
def health():
    return {
        "status":         "ok",
        "initialized":    store["initialized"],
        "last_update":    store["last_update"],
        "is_market_open": store["is_market_open"],
        "total_stocks":   len(store["stocks"]),
    }

@app.get("/api/signals")
def get_signals(email: str = Depends(verify_token)):
    stocks_list = list(store["stocks"].values())
    order = {"BUY": 0, "SELL": 1, "HOLD": 2}
    stocks_list.sort(key=lambda x: order.get(x.get("signal", "HOLD"), 2))
    return {
        "last_update":    store["last_update"],
        "is_market_open": store["is_market_open"],
        "total":          len(stocks_list),
        "stocks":         stocks_list,
    }

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
    df   = store["ohlcv"][symbol].copy().tail(60)
    rows = []
    for dt, row in df.iterrows():
        def v(x):
            return None if pd.isna(x) else round(float(x), 2)
        rows.append({
            "date":        dt.strftime("%Y-%m-%d"),
            "open":        v(row["Open"]),
            "high":        v(row["High"]),
            "low":         v(row["Low"]),
            "close":       v(row["Close"]),
            "volume":      int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
            "sma5":        v(row.get("SMA5")),
            "ema13":       v(row.get("EMA13")),
            "ema26":       v(row.get("EMA26")),
            "volsma5":     v(row.get("VolSMA5")),
            "buy_signal":  bool(row.get("BuySignal",  False)),
            "sell_signal": bool(row.get("SellSignal", False)),
        })
    stock_info = store["stocks"].get(symbol, STOCK_MAP.get(symbol, {}))
    return {"symbol": symbol, "info": stock_info, "ohlcv": rows}

# ─── STARTUP ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    init_db()
    load_historical_data()
    load_index_data()
    store["is_market_open"] = is_market_hours()
    store["initialized"]    = True

    scheduler = BackgroundScheduler(timezone=IST)
    scheduler.add_job(update_prices,        "interval", minutes=1, id="price_update")
    scheduler.add_job(index_update_job,     "interval", minutes=5, id="index_update")
    scheduler.add_job(load_historical_data, "cron", hour=9, minute=0, day_of_week="mon-fri", id="daily_reload")
    scheduler.start()
    log.info("Scheduler started. Server ready.")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
