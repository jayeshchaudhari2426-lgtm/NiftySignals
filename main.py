"""
Nifty 500 Live Signal Backend v6
Render.com | uvicorn main:app --host 0.0.0.0 --port $PORT
"""
import os,time,logging,sqlite3,json
from datetime import datetime,timedelta
from typing import Dict,List,Optional,Any
from collections import deque
import pytz,pandas as pd,requests,yfinance as yf,numpy as np
from fastapi import FastAPI,HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s")
log=logging.getLogger(__name__)
IST=pytz.timezone("Asia/Kolkata")
MARKET_OPEN=(9,15); MARKET_CLOSE=(15,30)
DB_PATH="signals.db"; PRICE_HISTORY_LEN=30


STOCKS=[
    {"symbol":"HDFCBANK","name":"HDFC Bank","sector":"Banking"},
    {"symbol":"ICICIBANK","name":"ICICI Bank","sector":"Banking"},
    {"symbol":"SBIN","name":"State Bank of India","sector":"Banking"},
    {"symbol":"KOTAKBANK","name":"Kotak Mahindra Bank","sector":"Banking"},
    {"symbol":"AXISBANK","name":"Axis Bank","sector":"Banking"},
    {"symbol":"INDUSINDBK","name":"IndusInd Bank","sector":"Banking"},
    {"symbol":"BANKBARODA","name":"Bank of Baroda","sector":"Banking"},
    {"symbol":"PNB","name":"Punjab National Bank","sector":"Banking"},
    {"symbol":"CANBK","name":"Canara Bank","sector":"Banking"},
    {"symbol":"UNIONBANK","name":"Union Bank of India","sector":"Banking"},
    {"symbol":"FEDERALBNK","name":"Federal Bank","sector":"Banking"},
    {"symbol":"IDFCFIRSTB","name":"IDFC First Bank","sector":"Banking"},
    {"symbol":"BANDHANBNK","name":"Bandhan Bank","sector":"Banking"},
    {"symbol":"AUBANK","name":"AU Small Finance Bank","sector":"Banking"},
    {"symbol":"YESBANK","name":"Yes Bank","sector":"Banking"},
    {"symbol":"INDIANB","name":"Indian Bank","sector":"Banking"},
    {"symbol":"BANKINDIA","name":"Bank of India","sector":"Banking"},
    {"symbol":"MAHABANK","name":"Bank of Maharashtra","sector":"Banking"},
    {"symbol":"RBLBANK","name":"RBL Bank","sector":"Banking"},
    {"symbol":"KARURVYSYA","name":"Karur Vysya Bank","sector":"Banking"},
    {"symbol":"UJJIVANSFB","name":"Ujjivan Small Finance Bank","sector":"Banking"},
    {"symbol":"EQUITASBNK","name":"Equitas Small Finance Bank","sector":"Banking"},
    {"symbol":"TCS","name":"Tata Consultancy Services","sector":"IT"},
    {"symbol":"INFY","name":"Infosys","sector":"IT"},
    {"symbol":"HCLTECH","name":"HCL Technologies","sector":"IT"},
    {"symbol":"WIPRO","name":"Wipro","sector":"IT"},
    {"symbol":"TECHM","name":"Tech Mahindra","sector":"IT"},
    {"symbol":"LTIM","name":"LTIMindtree","sector":"IT"},
    {"symbol":"COFORGE","name":"Coforge","sector":"IT"},
    {"symbol":"MPHASIS","name":"Mphasis","sector":"IT"},
    {"symbol":"PERSISTENT","name":"Persistent Systems","sector":"IT"},
    {"symbol":"LTTS","name":"L&T Technology Services","sector":"IT"},
    {"symbol":"KPITTECH","name":"KPIT Technologies","sector":"IT"},
    {"symbol":"TATAELXSI","name":"Tata Elxsi","sector":"IT"},
    {"symbol":"MASTEK","name":"Mastek","sector":"IT"},
    {"symbol":"ZENSAR","name":"Zensar Technologies","sector":"IT"},
    {"symbol":"SONATSOFTW","name":"Sonata Software","sector":"IT"},
    {"symbol":"CYIENT","name":"Cyient","sector":"IT"},
    {"symbol":"TANLA","name":"Tanla Platforms","sector":"IT"},
    {"symbol":"CAMS","name":"CAMS","sector":"IT"},
    {"symbol":"KFINTECH","name":"KFin Technologies","sector":"IT"},
    {"symbol":"RELIANCE","name":"Reliance Industries","sector":"Energy"},
    {"symbol":"ONGC","name":"ONGC","sector":"Energy"},
    {"symbol":"BPCL","name":"Bharat Petroleum","sector":"Energy"},
    {"symbol":"IOC","name":"Indian Oil Corp","sector":"Energy"},
    {"symbol":"HINDPETRO","name":"Hindustan Petroleum","sector":"Energy"},
    {"symbol":"GAIL","name":"GAIL India","sector":"Energy"},
    {"symbol":"PETRONET","name":"Petronet LNG","sector":"Energy"},
    {"symbol":"MRPL","name":"MRPL","sector":"Energy"},
    {"symbol":"IGL","name":"Indraprastha Gas","sector":"Energy"},
    {"symbol":"MGL","name":"Mahanagar Gas","sector":"Energy"},
    {"symbol":"GUJGASLTD","name":"Gujarat Gas","sector":"Energy"},
    {"symbol":"NTPC","name":"NTPC","sector":"Power"},
    {"symbol":"POWERGRID","name":"Power Grid Corp","sector":"Power"},
    {"symbol":"TATAPOWER","name":"Tata Power","sector":"Power"},
    {"symbol":"ADANIGREEN","name":"Adani Green Energy","sector":"Power"},
    {"symbol":"TORNTPOWER","name":"Torrent Power","sector":"Power"},
    {"symbol":"JSWENERGY","name":"JSW Energy","sector":"Power"},
    {"symbol":"NHPC","name":"NHPC","sector":"Power"},
    {"symbol":"SJVN","name":"SJVN","sector":"Power"},
    {"symbol":"CESC","name":"CESC","sector":"Power"},
    {"symbol":"ADANIPOWER","name":"Adani Power","sector":"Power"},
    {"symbol":"KEC","name":"KEC International","sector":"Power"},
    {"symbol":"KALPATPOWR","name":"Kalpataru Power","sector":"Power"},
    {"symbol":"HINDUNILVR","name":"Hindustan Unilever","sector":"FMCG"},
    {"symbol":"ITC","name":"ITC","sector":"FMCG"},
    {"symbol":"NESTLEIND","name":"Nestle India","sector":"FMCG"},
    {"symbol":"BRITANNIA","name":"Britannia Industries","sector":"FMCG"},
    {"symbol":"DABUR","name":"Dabur India","sector":"FMCG"},
    {"symbol":"MARICO","name":"Marico","sector":"FMCG"},
    {"symbol":"GODREJCP","name":"Godrej Consumer Products","sector":"FMCG"},
    {"symbol":"COLPAL","name":"Colgate-Palmolive India","sector":"FMCG"},
    {"symbol":"EMAMILTD","name":"Emami","sector":"FMCG"},
    {"symbol":"TATACONSUM","name":"Tata Consumer Products","sector":"FMCG"},
    {"symbol":"MARUTI","name":"Maruti Suzuki","sector":"Auto"},
    {"symbol":"TATAMOTORS","name":"Tata Motors","sector":"Auto"},
    {"symbol":"BAJAJ-AUTO","name":"Bajaj Auto","sector":"Auto"},
    {"symbol":"HEROMOTOCO","name":"Hero MotoCorp","sector":"Auto"},
    {"symbol":"M&M","name":"Mahindra & Mahindra","sector":"Auto"},
    {"symbol":"EICHERMOT","name":"Eicher Motors","sector":"Auto"},
    {"symbol":"TVSMOTORS","name":"TVS Motor Company","sector":"Auto"},
    {"symbol":"ASHOKLEY","name":"Ashok Leyland","sector":"Auto"},
    {"symbol":"ESCORTS","name":"Escorts Kubota","sector":"Auto"},
    {"symbol":"MOTHERSON","name":"Samvardhana Motherson","sector":"Auto Anc"},
    {"symbol":"BALKRISIND","name":"Balkrishna Industries","sector":"Auto Anc"},
    {"symbol":"MRF","name":"MRF","sector":"Auto Anc"},
    {"symbol":"APOLLOTYRE","name":"Apollo Tyres","sector":"Auto Anc"},
    {"symbol":"BHARATFORG","name":"Bharat Forge","sector":"Auto Anc"},
    {"symbol":"EXIDEIND","name":"Exide Industries","sector":"Auto Anc"},
    {"symbol":"UNOMINDA","name":"UNO Minda","sector":"Auto Anc"},
    {"symbol":"BOSCHLTD","name":"Bosch","sector":"Auto Anc"},
    {"symbol":"TIINDIA","name":"Tube Investments of India","sector":"Auto Anc"},
    {"symbol":"ENDURANCE","name":"Endurance Technologies","sector":"Auto Anc"},
    {"symbol":"BAJFINANCE","name":"Bajaj Finance","sector":"Finance"},
    {"symbol":"BAJAJFINSV","name":"Bajaj Finserv","sector":"Finance"},
    {"symbol":"CHOLAFIN","name":"Cholamandalam Finance","sector":"Finance"},
    {"symbol":"MUTHOOTFIN","name":"Muthoot Finance","sector":"Finance"},
    {"symbol":"SBICARD","name":"SBI Card","sector":"Finance"},
    {"symbol":"PFC","name":"Power Finance Corp","sector":"Finance"},
    {"symbol":"RECLTD","name":"REC Limited","sector":"Finance"},
    {"symbol":"IREDA","name":"IREDA","sector":"Finance"},
    {"symbol":"MANAPPURAM","name":"Manappuram Finance","sector":"Finance"},
    {"symbol":"M&MFIN","name":"M&M Financial Services","sector":"Finance"},
    {"symbol":"SHRIRAMFIN","name":"Shriram Finance","sector":"Finance"},
    {"symbol":"LTFH","name":"L&T Finance","sector":"Finance"},
    {"symbol":"ANGELONE","name":"Angel One","sector":"Finance"},
    {"symbol":"CDSL","name":"CDSL","sector":"Finance"},
    {"symbol":"BSE","name":"BSE Limited","sector":"Finance"},
    {"symbol":"MCX","name":"MCX India","sector":"Finance"},
    {"symbol":"IRFC","name":"IRFC","sector":"Finance"},
    {"symbol":"HUDCO","name":"HUDCO","sector":"Finance"},
    {"symbol":"LICHOUSING","name":"LIC Housing Finance","sector":"Finance"},
    {"symbol":"HDFCLIFE","name":"HDFC Life Insurance","sector":"Insurance"},
    {"symbol":"SBILIFE","name":"SBI Life Insurance","sector":"Insurance"},
    {"symbol":"ICICIGI","name":"ICICI Lombard GIC","sector":"Insurance"},
    {"symbol":"ICICIPRULI","name":"ICICI Prudential Life","sector":"Insurance"},
    {"symbol":"LICI","name":"LIC of India","sector":"Insurance"},
    {"symbol":"STARHEALTH","name":"Star Health Insurance","sector":"Insurance"},
    {"symbol":"SUNPHARMA","name":"Sun Pharmaceutical","sector":"Pharma"},
    {"symbol":"CIPLA","name":"Cipla","sector":"Pharma"},
    {"symbol":"DRREDDY","name":"Dr. Reddy's Laboratories","sector":"Pharma"},
    {"symbol":"DIVISLAB","name":"Divi's Laboratories","sector":"Pharma"},
    {"symbol":"ZYDUSLIFE","name":"Zydus Lifesciences","sector":"Pharma"},
    {"symbol":"AUROPHARMA","name":"Aurobindo Pharma","sector":"Pharma"},
    {"symbol":"LUPIN","name":"Lupin","sector":"Pharma"},
    {"symbol":"TORNTPHARM","name":"Torrent Pharma","sector":"Pharma"},
    {"symbol":"ALKEM","name":"Alkem Laboratories","sector":"Pharma"},
    {"symbol":"GLENMARK","name":"Glenmark Pharma","sector":"Pharma"},
    {"symbol":"GRANULES","name":"Granules India","sector":"Pharma"},
    {"symbol":"LAURUSLABS","name":"Laurus Labs","sector":"Pharma"},
    {"symbol":"IPCALAB","name":"IPCA Laboratories","sector":"Pharma"},
    {"symbol":"ABBOTINDIA","name":"Abbott India","sector":"Pharma"},
    {"symbol":"NATCOPHARM","name":"Natco Pharma","sector":"Pharma"},
    {"symbol":"AJANTPHARM","name":"Ajanta Pharma","sector":"Pharma"},
    {"symbol":"APOLLOHOSP","name":"Apollo Hospitals","sector":"Healthcare"},
    {"symbol":"LALPATHLAB","name":"Dr Lal PathLabs","sector":"Healthcare"},
    {"symbol":"MAXHEALTH","name":"Max Healthcare","sector":"Healthcare"},
    {"symbol":"FORTIS","name":"Fortis Healthcare","sector":"Healthcare"},
    {"symbol":"METROPOLIS","name":"Metropolis Healthcare","sector":"Healthcare"},
    {"symbol":"KIMS","name":"Krishna Institute of Medical Sciences","sector":"Healthcare"},
    {"symbol":"NH","name":"Narayana Hrudayalaya","sector":"Healthcare"},
    {"symbol":"JSWSTEEL","name":"JSW Steel","sector":"Metals"},
    {"symbol":"TATASTEEL","name":"Tata Steel","sector":"Metals"},
    {"symbol":"SAIL","name":"SAIL","sector":"Metals"},
    {"symbol":"HINDZINC","name":"Hindustan Zinc","sector":"Metals"},
    {"symbol":"VEDL","name":"Vedanta","sector":"Metals"},
    {"symbol":"NATIONALUM","name":"National Aluminium","sector":"Metals"},
    {"symbol":"HINDCOPPER","name":"Hindustan Copper","sector":"Metals"},
    {"symbol":"APLAPOLLO","name":"APL Apollo Tubes","sector":"Metals"},
    {"symbol":"RATNAMANI","name":"Ratnamani Metals","sector":"Metals"},
    {"symbol":"NMDC","name":"NMDC","sector":"Mining"},
    {"symbol":"COALINDIA","name":"Coal India","sector":"Mining"},
    {"symbol":"JINDALSTEL","name":"Jindal Steel & Power","sector":"Metals"},
    {"symbol":"ULTRACEMCO","name":"UltraTech Cement","sector":"Cement"},
    {"symbol":"GRASIM","name":"Grasim Industries","sector":"Cement"},
    {"symbol":"AMBUJACEM","name":"Ambuja Cements","sector":"Cement"},
    {"symbol":"ACC","name":"ACC","sector":"Cement"},
    {"symbol":"SHREECEM","name":"Shree Cement","sector":"Cement"},
    {"symbol":"JKCEMENT","name":"JK Cement","sector":"Cement"},
    {"symbol":"RAMCOCEM","name":"The Ramco Cements","sector":"Cement"},
    {"symbol":"KAJARIACER","name":"Kajaria Ceramics","sector":"Building"},
    {"symbol":"ASTRAL","name":"Astral","sector":"Building"},
    {"symbol":"SUPREMEIND","name":"Supreme Industries","sector":"Building"},
    {"symbol":"CENTURYPLY","name":"Century Plyboards","sector":"Building"},
    {"symbol":"ASIANPAINT","name":"Asian Paints","sector":"Paints"},
    {"symbol":"BERGEPAINT","name":"Berger Paints","sector":"Paints"},
    {"symbol":"KANSAINER","name":"Kansai Nerolac Paints","sector":"Paints"},
    {"symbol":"PIDILITIND","name":"Pidilite Industries","sector":"Chemicals"},
    {"symbol":"SRF","name":"SRF","sector":"Chemicals"},
    {"symbol":"DEEPAKNTR","name":"Deepak Nitrite","sector":"Chemicals"},
    {"symbol":"NAVINFLUOR","name":"Navin Fluorine","sector":"Chemicals"},
    {"symbol":"AARTIIND","name":"Aarti Industries","sector":"Chemicals"},
    {"symbol":"VINATI","name":"Vinati Organics","sector":"Chemicals"},
    {"symbol":"TATACHEM","name":"Tata Chemicals","sector":"Chemicals"},
    {"symbol":"ALKYLAMINE","name":"Alkyl Amines Chemicals","sector":"Chemicals"},
    {"symbol":"LT","name":"Larsen & Toubro","sector":"Engineering"},
    {"symbol":"SIEMENS","name":"Siemens India","sector":"Engineering"},
    {"symbol":"ABB","name":"ABB India","sector":"Engineering"},
    {"symbol":"BHEL","name":"Bharat Heavy Electricals","sector":"Engineering"},
    {"symbol":"THERMAX","name":"Thermax","sector":"Engineering"},
    {"symbol":"CUMMINSIND","name":"Cummins India","sector":"Engineering"},
    {"symbol":"GRINDWELL","name":"Grindwell Norton","sector":"Engineering"},
    {"symbol":"AIAENG","name":"AIA Engineering","sector":"Engineering"},
    {"symbol":"PRAJ","name":"Praj Industries","sector":"Engineering"},
    {"symbol":"BEL","name":"Bharat Electronics","sector":"Defence"},
    {"symbol":"HAL","name":"Hindustan Aeronautics","sector":"Defence"},
    {"symbol":"MAZDOCK","name":"Mazagon Dock","sector":"Defence"},
    {"symbol":"COCHINSHIP","name":"Cochin Shipyard","sector":"Defence"},
    {"symbol":"GRSE","name":"Garden Reach Shipbuilders","sector":"Defence"},
    {"symbol":"BDL","name":"Bharat Dynamics","sector":"Defence"},
    {"symbol":"ADANIPORTS","name":"Adani Ports","sector":"Infrastructure"},
    {"symbol":"ADANIENT","name":"Adani Enterprises","sector":"Diversified"},
    {"symbol":"DLF","name":"DLF","sector":"Real Estate"},
    {"symbol":"GODREJPROP","name":"Godrej Properties","sector":"Real Estate"},
    {"symbol":"OBEROIRLTY","name":"Oberoi Realty","sector":"Real Estate"},
    {"symbol":"PRESTIGE","name":"Prestige Estates","sector":"Real Estate"},
    {"symbol":"BRIGADE","name":"Brigade Enterprises","sector":"Real Estate"},
    {"symbol":"PHOENIXLTD","name":"Phoenix Mills","sector":"Real Estate"},
    {"symbol":"LODHA","name":"Macrotech Developers","sector":"Real Estate"},
    {"symbol":"TITAN","name":"Titan Company","sector":"Consumer"},
    {"symbol":"TRENT","name":"Trent","sector":"Retail"},
    {"symbol":"DMART","name":"Avenue Supermarts","sector":"Retail"},
    {"symbol":"BATAINDIA","name":"Bata India","sector":"Retail"},
    {"symbol":"NYKAA","name":"Nykaa","sector":"E-Commerce"},
    {"symbol":"KALYANKJIL","name":"Kalyan Jewellers","sector":"Jewellery"},
    {"symbol":"HAVELLS","name":"Havells India","sector":"Electronics"},
    {"symbol":"VOLTAS","name":"Voltas","sector":"Electronics"},
    {"symbol":"POLYCAB","name":"Polycab India","sector":"Electronics"},
    {"symbol":"DIXON","name":"Dixon Technologies","sector":"Electronics"},
    {"symbol":"AMBER","name":"Amber Enterprises","sector":"Electronics"},
    {"symbol":"VGUARD","name":"V-Guard Industries","sector":"Electronics"},
    {"symbol":"KEI","name":"KEI Industries","sector":"Electronics"},
    {"symbol":"BLUESTAR","name":"Blue Star","sector":"Electronics"},
    {"symbol":"BHARTIARTL","name":"Bharti Airtel","sector":"Telecom"},
    {"symbol":"HFCL","name":"HFCL","sector":"Telecom"},
    {"symbol":"TATACOMM","name":"Tata Communications","sector":"Telecom"},
    {"symbol":"INDUS","name":"Indus Towers","sector":"Telecom"},
    {"symbol":"JUBLFOOD","name":"Jubilant FoodWorks","sector":"QSR"},
    {"symbol":"WESTLIFE","name":"Westlife Foodworld","sector":"QSR"},
    {"symbol":"DEVYANI","name":"Devyani International","sector":"QSR"},
    {"symbol":"INDHOTEL","name":"Indian Hotels (Taj)","sector":"Hospitality"},
    {"symbol":"EIH","name":"EIH (Oberoi Hotels)","sector":"Hospitality"},
    {"symbol":"LEMONTRE","name":"Lemon Tree Hotels","sector":"Hospitality"},
    {"symbol":"IRCTC","name":"IRCTC","sector":"Travel"},
    {"symbol":"INTERGLOBE","name":"IndiGo (InterGlobe)","sector":"Aviation"},
    {"symbol":"VBL","name":"Varun Beverages","sector":"Beverages"},
    {"symbol":"UBL","name":"United Breweries","sector":"Beverages"},
    {"symbol":"RADICO","name":"Radico Khaitan","sector":"Beverages"},
    {"symbol":"ZOMATO","name":"Zomato","sector":"New Age Tech"},
    {"symbol":"PAYTM","name":"Paytm","sector":"Fintech"},
    {"symbol":"DELHIVERY","name":"Delhivery","sector":"Logistics"},
    {"symbol":"POLICYBZR","name":"PB Fintech","sector":"Fintech"},
    {"symbol":"CHAMBAL","name":"Chambal Fertilisers","sector":"Fertilizers"},
    {"symbol":"COROMANDEL","name":"Coromandel International","sector":"Fertilizers"},
    {"symbol":"PIIND","name":"PI Industries","sector":"Agro Chemicals"},
    {"symbol":"RALLIS","name":"Rallis India","sector":"Agro Chemicals"},
    {"symbol":"WELSPUNLIV","name":"Welspun Living","sector":"Textile"},
    {"symbol":"RAYMOND","name":"Raymond","sector":"Textile"},
    {"symbol":"KPRMILL","name":"KPR Mill","sector":"Textile"},
    {"symbol":"TRIDENT","name":"Trident","sector":"Textile"},
    {"symbol":"ZEEL","name":"Zee Entertainment","sector":"Media"},
    {"symbol":"SUNTV","name":"Sun TV Network","sector":"Media"},
    {"symbol":"PVRINOX","name":"PVR INOX","sector":"Media"},
    {"symbol":"SAREGAMA","name":"Saregama India","sector":"Media"},
    {"symbol":"BLUEDART","name":"Blue Dart Express","sector":"Logistics"},
    {"symbol":"CONCOR","name":"Container Corp of India","sector":"Logistics"},
    {"symbol":"GODREJIND","name":"Godrej Industries","sector":"Diversified"},
    {"symbol":"BALRAMCHIN","name":"Balrampur Chini Mills","sector":"Sugar"},
    {"symbol":"PAGEIND","name":"Page Industries","sector":"Textile"},
    {"symbol":"MOFSL","name":"Motilal Oswal Financial","sector":"Finance"},
]

_seen=set(); _dd=[]
for s in STOCKS:
    if s["symbol"] not in _seen: _seen.add(s["symbol"]); _dd.append(s)
STOCKS=_dd
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
    con.commit(); con.close()

def save_signal(symbol,signal,date,price):
    ts=datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    con=sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO signal_history VALUES(NULL,?,?,?,?,?)",(symbol,signal,date,price,ts))
    con.commit(); con.close()

def get_history(limit=500):
    con=sqlite3.connect(DB_PATH)
    cur=con.execute("SELECT symbol,signal,date,price,ts FROM signal_history ORDER BY id DESC LIMIT ?",(limit))
    rows=cur.fetchall(); con.close()
    result=[]
    for r in rows:
        sym=r[0]; e=STOCK_MAP.get(sym,{})
        cp=store["stocks"].get(sym,{}).get("current_price",r[3])
        pct=round((cp-r[3])/r[3]*100,2) if r[3] else 0
        result.append({"symbol":sym,"name":e.get("name",sym),"sector":e.get("sector",""),
                       "signal":r[1],"signal_date":r[2],"signal_price":r[3],
                       "current_price":cp,"pct_change":pct,"timestamp":r[4]})
    return result

def compute_rsi(series,period=14):
    delta=series.diff()
    gain=delta.clip(lower=0); loss=-delta.clip(upper=0)
    avg_gain=gain.ewm(com=period-1,min_periods=period).mean()
    avg_loss=loss.ewm(com=period-1,min_periods=period).mean()
    rs=avg_gain/avg_loss.replace(0,1e-10)
    return (100-(100/(1+rs))).round(2)

def compute_indicators(df):
    df=df.copy(); df.sort_index(inplace=True)
    df["SMA5"]=df["Close"].rolling(5).mean().round(2)
    df["EMA13"]=df["Close"].ewm(span=13,adjust=False).mean().round(2)
    df["EMA26"]=df["Close"].ewm(span=26,adjust=False).mean().round(2)
    df["VolSMA20"]=df["Volume"].rolling(20).mean().round(0)
    df["RSI14"]=compute_rsi(df["Close"],14)
    df["MaxInd"]=df[["SMA5","EMA13","EMA26"]].max(axis=1)
    df["MinInd"]=df[["SMA5","EMA13","EMA26"]].min(axis=1)
    df["Conjunction"]=(df["MaxInd"]*0.99)<=(df["MinInd"]*1.01)
    df["VolConfirm"]=df["Volume"]>(df["VolSMA20"]*1.5)
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

def fetch_nse_prices():
    s=get_nse_session(); prices={}
    for index in ["NIFTY%20200","NIFTY%20MIDCAP%20150","NIFTY%20SMALLCAP%20250"]:
        try:
            r=s.get(f"https://www.nseindia.com/api/equity-stockIndices?index={index}",timeout=15)
            r.raise_for_status()
            for item in r.json().get("data",[]):
                sym=item.get("symbol",""); ltp=item.get("lastPrice",0)
                if sym and ltp: prices[sym]=float(str(ltp).replace(",",""))
        except Exception as e: log.warning(f"NSE {index}: {e}")
    return prices

def load_historical_data():
    log.info(f"Loading {len(STOCKS)} stocks...")
    symbols=[s["symbol"] for s in STOCKS]
    yf_syms=[f"{sym}.NS" for sym in symbols]
    for b in range(0,len(yf_syms),50):
        byf=yf_syms[b:b+50]; bsym=symbols[b:b+50]
        log.info(f"Batch {b//50+1}/{(len(yf_syms)+49)//50}")
        try:
            raw=yf.download(byf,period="90d",interval="1d",group_by="ticker",
                           auto_adjust=True,progress=False,threads=True)
            for sym,yfs in zip(bsym,byf):
                try:
                    df=raw.copy() if len(byf)==1 else (raw[yfs].copy() if yfs in raw.columns.get_level_values(0) else None)
                    if df is None: continue
                    df=df.dropna(subset=["Close"])
                    if df.empty: continue
                    df=compute_indicators(df); sig=detect_signal(df); last=df.iloc[-1]
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
                        "rsi":rsi_cur,"rsi_prev":rsi_prev,
                        "rsi14":v(last.get("RSI14")),
                    }
                    store["ohlcv"][sym]=df
                    closes=df["Close"].dropna().tolist()[-PRICE_HISTORY_LEN:]
                    store["price_history"][sym]=deque(closes,maxlen=PRICE_HISTORY_LEN)
                    if sig["signal_price"] and v(last["Close"]):
                        sp=sig["signal_price"]; cp=v(last["Close"])
                        store["stocks"][sym]["pct_change"]=round((cp-sp)/sp*100,2)
                except Exception as ex: log.warning(f"{sym}: {ex}")
        except Exception as e: log.error(f"Batch: {e}")
        time.sleep(1)
    log.info(f"Done. {len(store['stocks'])} stocks.")

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
    try: prices=fetch_nse_prices(); log.info(f"NSE: {len(prices)} prices")
    except Exception as e: log.error(f"NSE: {e}"); return
    now_ist=datetime.now(IST); today=now_ist.strftime("%Y-%m-%d")
    for sym,entry in store["stocks"].items():
        ltp=prices.get(sym)
        if ltp is None: continue
        entry["current_price"]=ltp
        h=store["price_history"].get(sym)
        if h: h.append(ltp)
        sp=entry.get("signal_price")
        if sp: entry["pct_change"]=round((ltp-sp)/sp*100,2)
        df=store["ohlcv"].get(sym)
        if df is None or df.empty: continue
        if today in df.index.strftime("%Y-%m-%d").tolist():
            li=df.index[-1]; df.at[li,"Close"]=ltp
            df.at[li,"High"]=max(df.at[li,"High"],ltp); df.at[li,"Low"]=min(df.at[li,"Low"],ltp)
        else:
            nr=pd.DataFrame({"Open":[ltp],"High":[ltp],"Low":[ltp],"Close":[ltp],"Volume":[0]},
                           index=[pd.Timestamp(today)])
            df=pd.concat([df,nr])
        df=compute_indicators(df); store["ohlcv"][sym]=df; last=df.iloc[-1]
        def v(x): return round(float(x),2) if not pd.isna(x) else None
        prev_rsi=entry.get("rsi14")
        entry["sma5"]=v(last["SMA5"]); entry["ema13"]=v(last["EMA13"])
        entry["ema26"]=v(last["EMA26"]); entry["rsi14"]=v(last.get("RSI14"))
        entry["conjunction"]=bool(last.get("Conjunction",False))
        entry["vol_confirm"]=bool(last.get("VolConfirm",False))
        cur_rsi=entry["rsi14"]
        if prev_rsi is not None and cur_rsi is not None:
            if prev_rsi<49 and cur_rsi>=49:
                add_notification("RSI_CROSS",sym,entry["name"],ltp,
                    f"Weekly RSI crossed above 49 — now at {cur_rsi}")
        prev_sig=entry.get("signal")
        if df["BuySignal"].iloc[-1] and prev_sig!="BUY":
            entry.update({"signal":"BUY","signal_date":today,"signal_price":ltp,"pct_change":0.0})
            save_signal(sym,"BUY",today,ltp)
            add_notification("BUY",sym,entry["name"],ltp,f"Triple Confluence BUY signal fired at ₹{ltp}")
            log.info(f"BUY {sym}@{ltp}")
        elif df["SellSignal"].iloc[-1] and prev_sig!="SELL":
            entry.update({"signal":"SELL","signal_date":today,"signal_price":ltp,"pct_change":0.0})
            save_signal(sym,"SELL",today,ltp)
            add_notification("SELL",sym,entry["name"],ltp,f"Triple Confluence SELL signal fired at ₹{ltp}")
            log.info(f"SELL {sym}@{ltp}")
    store["last_update"]=now_ist.strftime("%Y-%m-%d %H:%M:%S IST")

def index_update_job():
    if is_market_hours(): load_index_data()


app=FastAPI(title="Nifty500 Signals v6",version="6.0")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=False,allow_methods=["*"],allow_headers=["*"])


@app.api_route("/api/health", methods=["GET","HEAD"])
def health():
    return {"status":"ok","initialized":store["initialized"],"last_update":store["last_update"],
            "is_market_open":store["is_market_open"],"total_stocks":len(store["stocks"])}

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

@app.get("/api/indices")
def get_indices(): return {"indices":list(store["indices"].values())}

@app.get("/api/notifications")
def get_notifications():
    return {"notifications":list(store["notifications"])}

@app.post("/api/notifications/read")
def mark_read():
    for n in store["notifications"]: n["read"]=True
    return {"ok":True}

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
        ticker=yf.Ticker(f"{symbol}.NS")
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
    init_db(); load_historical_data(); load_index_data()
    store["is_market_open"]=is_market_hours(); store["initialized"]=True
    scheduler=BackgroundScheduler(timezone=IST)
    scheduler.add_job(update_prices,"interval",minutes=1,id="price_update")
    scheduler.add_job(index_update_job,"interval",minutes=5,id="index_update")
    scheduler.add_job(load_historical_data,"cron",hour=9,minute=0,day_of_week="mon-fri",id="daily_reload")
    scheduler.start(); log.info("Server ready.")

if __name__=="__main__":
    import uvicorn
    uvicorn.run("main:app",host="0.0.0.0",port=int(os.getenv("PORT",8000)))
