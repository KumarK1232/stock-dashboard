#!/usr/bin/env python3
# TopBottom_Universe vFinal26 - Auto-Import Automation
# --- EXPERT MODIFICATIONS ---
# 1. AUTOMATION: Script now scans your Downloads folder on startup.
# 2. AUTOMATION: Detects the newest 'favorites*.xlsx' file.
# 3. AUTOMATION: Moves and overwrites the master database automatically.
# 4. EXISTING: Keeps the JS logic to batch save multiple favorites at once.
# 5. NEW: Added Price Trend Days display on top of charts.

from __future__ import annotations
import os, sys, time, json, math, random, logging, urllib.request, urllib.parse, webbrowser
import glob
from io import StringIO
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import threading, queue
import pandas as pd, numpy as np
import shutil 
import imaplib
import email
import io
import os
import pandas as pd
import sys
import pandas_market_calendars as mcal


try:
    import pandas as pd
    import numpy as np
    from bs4 import BeautifulSoup
    from dateutil import parser
except ImportError:
    print("CRITICAL ERROR: Missing libraries.")
    print("Run: pip install pandas numpy beautifulsoup4 lxml openpyxl python-dateutil")
    sys.exit(1)
# This adds the folder where the script lives to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

# Now try the import
try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Error: BeautifulSoup4 not found. Please install it: pip install beautifulsoup4 lxml")
    sys.exit(1)

try:
    from favorites_report_builder import generate_favorites_tile_report
except ImportError:
    print("Error: favorites_report_builder.py not found. Please make sure you renamed the file.")
    sys.exit(1)

# -------------------- CONFIG --------------------
SCRIPT_VERSION = "vFinal26-AutoImport"

# --- NEW: User Requested Price Trend Days ---
# --- EMAIL / INBOX CONFIG ---
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
SENDER_EMAIL = "stockusals@gmail.com"
INBOX_LOOKBACK_DAYS = 1

# --- PRICE TREND CONFIG ---
PRICE_TREND_DAYS = [2, 3, 5, 7, 9, 11, 15, 30, 60, 90, 180, 360]
# --------------------------------------------

# ... (Keep all your imports at the top)

# -------------------- UPDATED PATHS FOR GITHUB --------------------
# This uses the script's location as the base, making it "portable"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Create a "Output" folder inside your project directory if it doesn't exist
MASTER_OUTPUT_DIR = os.path.join(BASE_DIR, "docs")
os.makedirs(MASTER_OUTPUT_DIR, exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M")

# --- FIXED PATHS FOR GITHUB (NO TIMESTAMPS) ---
# We use fixed names so we overwrite the old files and save space.
OUT_HTML_INBOX  = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Inbox.html")
OUT_HTML_UNIV   = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Universal.html")
OUT_HTML_WATCH  = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Watchlist.html") 
OUT_HTML_SECTOR = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Sector.html")
OUT_HTML_FAV    = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Favorites_Tile.html") 
OUT_CSV         = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Flagged.csv")
OUT_TXT         = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Summary.txt")

# Move Cache outside of MASTER_OUTPUT_DIR (docs) to keep the website clean
# BASE_DIR is the root of your project
CACHE_DIR = os.path.join(BASE_DIR, "tb_cache")
CHARTS_DIR = os.path.join(BASE_DIR, "charts")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)

# Update Excel file locations to be relative to the script
WATCHLIST_FILE = os.path.join(BASE_DIR, "watchlist.xlsx") 
FAVORITES_FILE = os.path.join(BASE_DIR, "favorites.xlsx") 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "docs")
CACHE_DIR = os.path.join(BASE_DIR, "cache")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
# ... (Continue with the rest of your functions)

# --- NEW: Define Downloads Folder ---
# This attempts to find your default Windows Downloads folder
DOWNLOADS_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")
USE_WATCHLIST_EXCEL = True

UNIVERSE_LIMIT = 300
EAGER_RENDER_FIRST_N = 18
THREADS = 25
REQUEST_TIMEOUT = 20
LOCAL_PLOTLY_FILE = "plotly-latest.min.js"
XLSX_JS_FILE = "xlsx.full.min.js" 

INTRADAY_INTERVAL = "5m"
INTRADAY_DAYS = 8
DAILY_LOOKBACK_DAYS = 180
WEEKLY_LOOKBACK_DAYS = 365 
RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0

AUTO_REFRESH_MINUTES_DEFAULT = 10
CHART_HEIGHT = 450
TABLE_ROWS_DAILY = 30
TABLE_ROWS_INTRADAY = 30

finviz_lock = threading.Lock()

# Logging
log_buffer = StringIO()
logger = logging.getLogger("TopBottom_v24")
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)
logger.addHandler(logging.StreamHandler(log_buffer))
#--------------------Inbox Email Helpers --------------------
# -------------------- Global State --------------------
inbox_tickers_extra_data = {} # {Ticker: {price, date, source}}

# -------------------- Inbox Automation --------------------

def insert_or_update_inbox(ticker, price, date_str, source, filename):
    """Helper to store inbox findings for the report builder."""
    inbox_tickers_extra_data[ticker] = {
        'price': price,
        'date': date_str,
        'source': f"{source} ({filename})"
    }

from email.header import decode_header

from email.header import decode_header

def parse_inbox():
    """Extracts tickers from Gmail attachments."""
    logger.info(f"--- AUTOMATION: Checking Inbox (Lookback: {INBOX_LOOKBACK_DAYS} days) ---")
    results = {}
    
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD: 
        logger.error("CRITICAL: EMAIL_ADDRESS or EMAIL_PASSWORD is missing!")
        return results

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        mail.select("inbox")
        
        # Format date for IMAP search
        dt = (datetime.now() - timedelta(days=INBOX_LOOKBACK_DAYS)).strftime("%d-%b-%Y")
        search_criteria = f'(SINCE "{dt}" FROM "{SENDER_EMAIL}")'
        _, ids = mail.search(None, search_criteria)
        
        if not ids[0]:
            logger.warning(f"No emails found from {SENDER_EMAIL} since {dt}")
            mail.logout()
            return results

        for uid in ids[0].split():
            _, data = mail.fetch(uid, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])
            
            # Get date of email
            try: d_str = parser.parse(msg.get("date")).strftime("%Y-%m-%d")
            except: d_str = datetime.now().strftime("%Y-%m-%d")
            
            for part in msg.walk():
                fname = part.get_filename()
                if fname and ("csv" in fname.lower() or "xls" in fname.lower()):
                    logger.info(f"Processing attachment: {fname}")
                    try:
                        payload = part.get_payload(decode=True)
                        # Read CSV or Excel
                        if "csv" in fname.lower():
                            df = pd.read_csv(io.BytesIO(payload))
                        else:
                            df = pd.read_excel(io.BytesIO(payload))
                        
                        # Fix column names to be lowercase for matching
                        df.columns = [str(c).lower().strip() for c in df.columns]
                        
                        t_col = next((c for c in df.columns if "ticker" in c or "symbol" in c), None)
                        p_col = next((c for c in df.columns if "price" in c or "current" in c), None)
                        
                        if t_col is None:
                            logger.warning(f"Skipping {fname}: No 'ticker' or 'symbol' column found.")
                            continue

                        for _, r in df.iterrows():
                            t = str(r[t_col]).strip().upper()
                            if not t or len(t) > 8 or t == 'NAN': continue
                            
                            p = float(r[p_col]) if p_col and not pd.isna(r[p_col]) else 0.0
                            insert_or_update_inbox(t, p, d_str, "InboxAttachment", fname)
                            results[t] = {'price': p, 'date': d_str}
                            
                    except Exception as e:
                        logger.error(f"Error reading attachment {fname}: {e}")
        mail.logout()
    except Exception as e:
        logger.error(f"Inbox connection/parsing failed: {e}")
    
    logger.info(f"Inbox scan complete. Found {len(results)} unique tickers.")
    return results

# -------------------- Existing Helpers (Summarized) --------------------
# (Including auto_import_favorites_from_downloads, fetchers, and compute_indicators from your original script)
# -------------------- Automation Helpers --------------------
def auto_import_favorites_from_downloads():
    """
    Scans the Downloads folder for any file starting with 'favorites' and ending in '.xlsx'.
    It takes the NEWEST one found, moves it to the project folder, and overwrites the old one.
    """
    logger.info("--- AUTOMATION: Checking Downloads folder for new favorites... ---")
    
    if not os.path.exists(DOWNLOADS_FOLDER):
        logger.warning(f"Could not find Downloads folder at: {DOWNLOADS_FOLDER}")
        return

    # Look for favorites.xlsx, favorites (1).xlsx, favorites (2).xlsx, etc.
    pattern = os.path.join(DOWNLOADS_FOLDER, "favorites*.xlsx")
    candidates = glob.glob(pattern)
    
    if not candidates:
        logger.info("No new 'favorites.xlsx' found in Downloads. Using existing database.")
        return

    # Find the newest file based on modification time
    try:
        newest_file = max(candidates, key=os.path.getmtime)
        logger.info(f"Found new favorites file: {newest_file}")
        
        # Ensure target directory exists
        os.makedirs(os.path.dirname(FAVORITES_FILE), exist_ok=True)
        
        # Move and Overwrite
        try:
            # remove old file first to ensure clean move
            if os.path.exists(FAVORITES_FILE):
                os.remove(FAVORITES_FILE)
            
            shutil.move(newest_file, FAVORITES_FILE)
            logger.info(f"SUCCESS: Imported and overwrote {FAVORITES_FILE}")
            
            # Optional: Clean up other duplicates in downloads to keep it tidy?
            # For safety, we only move the one we used.
            
        except Exception as e:
            logger.error(f"Failed to move file: {e}")

    except Exception as e:
        logger.error(f"Error during auto-import: {e}")

# -------------------- NEW: Trend Helper Function --------------------
def generate_trend_html(df_hist):
    """
    Generates a row of colored boxes showing price trends for PRICE_TREND_DAYS.
    Returns HTML string.
    """
    if df_hist is None or df_hist.empty:
        return ""

    # CSS for the trend bar (Inline to ensure it renders correctly)
    html = """
    <style>
        .trend-container { display: flex; flex-wrap: wrap; gap: 5px; margin: 8px 0 12px 0; align-items: center; }
        .trend-box { 
            font-size: 10px; font-weight: 700; color: white; 
            padding: 3px 6px; border-radius: 4px; text-align: center; min-width: 35px;
            font-family: sans-serif; line-height: 1.2; box-shadow: 0 1px 2px rgba(0,0,0,0.1);
        }
        .trend-up { background-color: #10b981; border: 1px solid #059669; } /* Green */
        .trend-down { background-color: #ef4444; border: 1px solid #dc2626; } /* Red */
        .trend-flat { background-color: #6b7280; border: 1px solid #4b5563; } /* Gray */
    </style>
    <div class='trend-container'>
        <span style="font-size:11px; color:#888; margin-right:4px;">Trend:</span>
    """
    
    current_price = df_hist['Close'].iloc[-1]
    
    for days in PRICE_TREND_DAYS:
        # Check if we have enough data (days + 1 for calculation)
        if len(df_hist) > days:
            # Get price 'days' ago. iloc[-1] is today.
            past_price = df_hist['Close'].iloc[-(days + 1)]
            
            if past_price == 0 or pd.isna(past_price): continue 
            
            change_pct = ((current_price - past_price) / past_price) * 100
            
            # Determine color class
            if change_pct > 0:
                css_class = "trend-up"
                sign = "+"
            elif change_pct < 0:
                css_class = "trend-down"
                sign = ""
            else:
                css_class = "trend-flat"
                sign = ""
                
            # Create the box
            html += f"<div class='trend-box {css_class}' title='{days} Days Ago: ${past_price:.2f}'>{days}D<br>{sign}{change_pct:.1f}%</div>"
            
    html += "</div>"
    return html

# -------------------- Helpers --------------------
def money(v:Optional[float]) -> str:
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))): return "n/a"
        return f"${float(v):.2f}"
    except:
        return "n/a"

def safe_str(v, fmt=".2f"):
    try:
        return f"{float(v):{fmt}}"
    except:
        return "n/a"

def cache_path(name:str)->str:
    return os.path.join(CACHE_DIR, name)

def is_cache_fresh(path:str, hours:int=12)->bool:
    if not os.path.exists(path):
        return False
    try:
        mtime = os.path.getmtime(path)
        return (time.time() - mtime) < hours * 3600
    except Exception:
        return False

def unique_tickers(ticker_list: List[str]) -> List[str]:
    seen = set()
    unique_list = []
    for t in ticker_list:
        t_clean = str(t).strip().upper()
        if t_clean and t_clean not in seen:
            unique_list.append(t_clean)
            seen.add(t_clean)
    return unique_list

# -------------------- Universe builders --------------------
def fetch_sp500()->List[str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html = resp.read().decode("utf-8","ignore")
        for df in pd.read_html(StringIO(html)):
            cols = [str(c).lower() for c in df.columns]
            if "symbol" in cols:
                col = df.columns[cols.index("symbol")]
                return df[col].astype(str).str.replace(".","-",regex=False).str.strip().str.upper().tolist()
    except Exception as e:
        logger.warning("fetch_sp500 failed: %s. Using fallback list.", e)
    return ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA"]

def fetch_nasdaq100()->List[str]:
    url = "https://en.wikipedia.org/wiki/NASDAQ-100"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html = resp.read().decode("utf-8","ignore")
        for df in pd.read_html(StringIO(html)):
            for c in df.columns:
                if str(c).lower() in ("ticker","symbol"):
                    return df[c].astype(str).str.replace(".","-",regex=False).str.strip().str.upper().tolist()
    except Exception as e:
        logger.warning("fetch_nasdaq100 failed: %s. Using empty list.", e)
    return []

def fetch_core_etfs()->List[str]:
    return ["SPY","QQQ","IWM","DIA","VTI","VOO","GLD","SLV","USO","UNG","TLT","AGG","VNQ","XLF","XLK","XLE","XLY","XLV"]

def fetch_leverage_etfs()->List[str]:
    return sorted(list({"TQQQ","SQQQ","SPXL","SPXS","UPRO","SPXU","SOXL","SOXS","FNGU","FNGD","TNA","TZA","SSO","SDS","UDOW","SDOW","TMF","TMV","LABU","LABD","TECL","TECS","DDM","DUST"}))

def fetch_global_etfs()->List[str]:
    return ["EFA","EWJ","EWZ","FXI","EEM","VEA","VWO","IEFA","VXUS","ACWI","VTI","VGK","BNDW","INDA","MCHI","IEMG"]

def fetch_crypto()->List[str]:
    return ["BTC-USD","ETH-USD","BNB-USD","SOL-USD","ADA-USD","XRP-USD","DOGE-USD"]

def fetch_commodities()->List[str]:
    return ["GC=F","SI=F","CL=F","NG=F","GLD","SLV","USO","UNG"]

def build_universe(limit:int=UNIVERSE_LIMIT)->Dict[str,List[str]]:
    cache_file = cache_path("univ_v23_deduped.json")
    if is_cache_fresh(cache_file, 24):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                logger.info("Loading universe from fresh cache: %s", cache_file)
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load universe cache: {e}")
    
    logger.info("Building fresh universe, cache is stale or missing...")
    seen = set()
    data = {}
    
    sp = fetch_sp500(); nq = fetch_nasdaq100()
    stocks = unique_tickers(sp + nq)
    if not stocks:
        stocks = ["AAPL","MSFT","NVDA","AMZN","GOOGL"]
    data["Stocks"] = stocks[:limit]
    seen.update(data["Stocks"])
    lev_raw = unique_tickers(fetch_leverage_etfs())
    data["Leverage ETF"] = [t for t in lev_raw if t not in seen]
    seen.update(data["Leverage ETF"])
    crypto_raw = unique_tickers(fetch_crypto())
    data["Crypto"] = [t for t in crypto_raw if t not in seen]
    seen.update(data["Crypto"])
    commodities_raw = unique_tickers(fetch_commodities())
    data["Commodities"] = [t for t in commodities_raw if t not in seen]
    seen.update(data["Commodities"])
    global_etfs_raw = unique_tickers(fetch_global_etfs())
    data["GlobalETF"] = [t for t in global_etfs_raw if t not in seen]
    seen.update(data["GlobalETF"])
    etfs_raw = unique_tickers(fetch_core_etfs())
    data["ETFs"] = [t for t in etfs_raw if t not in seen]
    seen.update(data["ETFs"])
    try:
        with open(cache_file,"w",encoding="utf-8") as f: json.dump(data,f)
    except Exception as e:
        logger.warning(f"Could not write universe cache: {e}")
    total_tickers = sum(len(v) for v in data.values())
    logger.info(f"Built universe: Stocks {len(data['Stocks'])}, ETFs {len(data['ETFs'])}, Lev {len(data['Leverage ETF'])}, Crypto {len(data['Crypto'])}. Total: {total_tickers}")
    return data

# -------------------- Watchlist Excel loader --------------------
def load_watchlist_from_excel(path:Optional[str]=None) -> tuple[Optional[Dict[str,List[str]]], Optional[Dict[str,Dict[str,Any]]]]:
    if not path or not os.path.exists(path):
        logger.info("Watchlist file not found: %s", path) 
        return None, None
    try:
        xls = pd.ExcelFile(path)
        out_map = {} 
        out_data = {} 
        
        for sheet in xls.sheet_names:
            try:
                df = xls.parse(sheet)
                
                # Clean headers
                if df is not None and not df.empty:
                    df.columns = df.columns.astype(str).str.strip()

                if df is None or df.empty or 'Ticker' not in df.columns:
                    logger.warning(f"Watchlist sheet '{sheet}' in {path} is empty or missing 'Ticker' column. Skipping.")
                    continue
                
                sheet_tickers = []
                for _, row in df.iterrows():
                    ticker = str(row['Ticker']).strip().upper()
                    if not ticker:
                        continue
                    
                    sheet_tickers.append(ticker)
                    
                    try:
                        entry_price = row.get('EntryPrice')
                        if pd.isna(entry_price): entry_price = None
                        else: entry_price = float(entry_price)

                        entry_date_raw = row.get('EntryDate')
                        entry_date = None
                        if not pd.isna(entry_date_raw):
                             entry_date = pd.to_datetime(entry_date_raw).strftime('%Y-%m-%d')
                        
                        out_data[ticker] = {'price': entry_price, 'date': entry_date}
                    except Exception as e:
                        if ticker not in out_data:
                            out_data[ticker] = {'price': None, 'date': None}

                if sheet_tickers:
                    out_map[sheet.strip()] = unique_tickers(sheet_tickers)
                    logger.info("Loaded %d tickers from sheet '%s' in %s", len(sheet_tickers), sheet, path)
                    
            except Exception as e:
                logger.warning("Could not load sheet %s from %s: %s", sheet, path, e)
                
        return (out_map or None), (out_data or None)
    except Exception as e:
        logger.error("Failed reading watchlist excel %s: %s", path, e)
        return None, None

# -------------------- Data fetchers --------------------
def fetch_chart_yahoo_json(ticker:str, interval:str="1d", days:int=365)->Optional[pd.DataFrame]:
    try:
        range_str = f"{max(1, days)}d"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?range={range_str}&interval={interval}"
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8","ignore")
        
        jobj = json.loads(raw)
        res = jobj.get("chart",{}).get("result")
        if not res: return None
        
        r = res[0]
        timestamps = r.get("timestamp", [])
        if not timestamps: return None

        tz_offset_seconds = r.get("meta", {}).get("gmtoffset", 0)
        tz = timezone(timedelta(seconds=tz_offset_seconds))

        quote = r.get("indicators",{}).get("quote",[{}])[0]
        opens = quote.get("open", []); highs = quote.get("high", []); lows = quote.get("low", []); closes = quote.get("close", []); volumes = quote.get("volume", [])
        
        rows=[]
        for i,t in enumerate(timestamps):
            if i >= len(closes) or closes[i] is None: continue
            dt = datetime.fromtimestamp(int(t), tz)
            rows.append({
                "Date": dt, "Open": opens[i], "High": highs[i], "Low": lows[i], "Close": closes[i], "Volume": volumes[i]
            })
            
        if not rows: return None
        df = pd.DataFrame(rows)
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
        df = df.sort_values("Date").reset_index(drop=True)
        return df
    except Exception:
        if interval == '1d': return fetch_daily_csv(ticker, days=days)
        return None

def fetch_daily_csv(ticker:str, days:int=DAILY_LOOKBACK_DAYS)->Optional[pd.DataFrame]:
    try:
        end = int(time.time()); start = end - int(days) * 86400
        url = f"https://query1.finance.yahoo.com/v7/finance/download/{urllib.parse.quote(ticker)}?period1={start}&period2={end}&interval=1d&events=history&includeAdjustedClose=true"
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8","ignore")
        if not raw or "404 Not Found" in raw: return None
        df = pd.read_csv(StringIO(raw))
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date","Close"]).sort_values("Date").reset_index(drop=True)
            df['Date'] = df['Date'].dt.tz_localize(timezone.utc)
            return df
    except Exception: pass
    return None

def fetch_intraday(ticker:str, interval:str=INTRADAY_INTERVAL, days:int=INTRADAY_DAYS)->Optional[pd.DataFrame]:
    return fetch_chart_yahoo_json(ticker, interval=interval, days=days)

def fetch_weekly(ticker:str, days:int=WEEKLY_LOOKBACK_DAYS)->Optional[pd.DataFrame]:
    return fetch_chart_yahoo_json(ticker, interval="1wk", days=days)

def fetch_metadata(ticker: str) -> dict:
    cache_file = cache_path(f"{ticker}_meta_v1.json")
    if is_cache_fresh(cache_file, 7 * 24):
        try:
            with open(cache_file, "r") as f: return json.load(f)
        except: pass

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}?modules=assetProfile,calendarEvents"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8", "ignore")
        jobj = json.loads(raw)
        result = jobj.get('quoteSummary', {}).get('result', [{}])[0]
        sector = result.get('assetProfile', {}).get('sector')
        earnings_date_iso = None
        earnings_events = result.get('calendarEvents', {}).get('earnings', {}).get('earningsDate', [])
        if earnings_events:
            ts = earnings_events[0].get('raw')
            if ts: earnings_date_iso = datetime.fromtimestamp(int(ts), timezone.utc).isoformat()
        data = {'sector': sector, 'earningsDate': earnings_date_iso}
        with open(cache_file, "w") as f: json.dump(data, f)
        return data
    except Exception:
        return {'sector': None, 'earningsDate': None}

def fetch_earnings_date_finviz(ticker: str) -> Optional[datetime]:
    with finviz_lock:
        time.sleep(0.5 + random.random() * 0.5) 
        try:
            url = f"https://finviz.com/quote.ashx?t={urllib.parse.quote(ticker)}"
            headers = {"User-Agent": "Mozilla/5.0"}
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                html = resp.read().decode("utf-8","ignore")
            soup = BeautifulSoup(html, 'lxml')
            earnings_header_cell = soup.find('td', class_='snapshot-td2', string='Earnings')
            if not earnings_header_cell: return None
            date_cell = earnings_header_cell.find_next_sibling('td')
            if not date_cell: return None
            date_str = date_cell.text.strip()
            if not date_str or date_str == "-": return None
            parts = date_str.split()
            if len(parts) < 2: return None
            month_str = parts[0]; day_str = parts[1].replace(',', '')
            current_year = datetime.now().year
            try: parsed_date = datetime.strptime(f"{month_str} {day_str} {current_year}", "%b %d %Y")
            except ValueError: return None
            if parsed_date < datetime.now() - timedelta(days=2):
                 parsed_date = parsed_date.replace(year=current_year + 1)
            return parsed_date.replace(tzinfo=timezone.utc)
        except Exception: return None

def compute_indicators(df:pd.DataFrame)->pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    d = df.copy()
    for c in ['Open','High','Low','Close','Volume']:
        d[c] = pd.to_numeric(d.get(c), errors='coerce')
    d['Date'] = pd.to_datetime(d['Date'], errors='coerce') 
    d = d.dropna(subset=['Date','Close']).sort_values('Date').reset_index(drop=True)
    if d.empty: return pd.DataFrame()
    
    d['ema9'] = d['Close'].ewm(span=9, adjust=False).mean()
    d['ema21'] = d['Close'].ewm(span=21, adjust=False).mean()
    
    delta = d['Close'].diff()
    up = delta.clip(lower=0); down = -delta.clip(upper=0)
    avg_up = up.rolling(RSI_PERIOD, min_periods=1).mean()
    avg_down = down.rolling(RSI_PERIOD, min_periods=1).mean()
    rs = avg_up / avg_down.replace(0, np.nan)
    d['RSI'] = 100 - (100/(1+rs))
    d['RSI'] = d['RSI'].fillna(50.0)
    
    prev = d['Close'].shift(1)
    tr = pd.concat([(d['High']-d['Low']).abs(), (d['High']-prev).abs(), (d['Low']-prev).abs()], axis=1).max(axis=1)
    d['ATR'] = tr.rolling(14, min_periods=1).mean()
    
    d['BB_mid'] = d['Close'].rolling(BB_PERIOD, min_periods=1).mean()
    d['BB_std'] = d['Close'].rolling(BB_PERIOD, min_periods=1).std().fillna(0)
    d['BB_upper'] = d['BB_mid'] + BB_STD * d['BB_std']
    d['BB_lower'] = d['BB_mid'] - BB_STD * d['BB_std']
    
    return d

def find_local_extrema(values:List[float], lookback:int=14, prominence_mult:float=0.6)->Dict[str,List[int]]:
    n = len(values)
    if n == 0: return {'peaks':[], 'troughs':[]}
    clean = [v if v is not None and not (isinstance(v,float) and (math.isnan(v) or math.isinf(v))) else None for v in values]
    vals = [v for v in clean if v is not None]
    if len(vals) < 2: return {'peaks':[], 'troughs':[]}
    base_std = float(np.std(vals)); prom = max(0.01, base_std * prominence_mult); half = max(1, lookback//2)
    peaks=[]; troughs=[]
    for i in range(n):
        v = clean[i]
        if v is None: continue
        left = max(0, i-half); right = min(n-1, i+half)
        window = [clean[j] for j in range(left, right+1) if clean[j] is not None]
        if not window: continue
        v_max = max(window); v_min = min(window)
        if v >= v_max and (v - v_min) >= prom: peaks.append(i)
        if v <= v_min and (v_max - v) >= prom: troughs.append(i)
    return {'peaks':peaks, 'troughs':troughs}

def pivot_confirmable(df:pd.DataFrame, idx:int, typ:str)->bool:
    if df is None or df.empty or idx<0 or idx>=len(df): return False
    try:
        price = float(df['Close'].iloc[idx]); rsi = float(df.get('RSI', 50).iloc[idx])
        bb_up = float(df.get('BB_upper', np.nan).iloc[idx]); bb_low = float(df.get('BB_lower', np.nan).iloc[idx])
    except Exception: return False
    if pd.isna(bb_up) or pd.isna(bb_low): band = None
    else: band = bb_up - bb_low
    bb_ok = False; rsi_ok = False
    if typ == 'peak':
        if band is not None and band > 0: bb_ok = ((bb_up - price) <= (0.08 * band))
        rsi_ok = rsi >= 60
    else:
        if band is not None and band > 0: bb_ok = ((price - bb_low) <= (0.08 * band))
        rsi_ok = rsi <= 40
    return bb_ok or rsi_ok

def analyze_ticker(ticker:str, entry_data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str,Any]]:
    try:
        tags = []
        trade_details = {} 
        now_utc = datetime.now(timezone.utc)
        recent_thresh_daily = now_utc - timedelta(days=4)
        recent_thresh_weekly = now_utc - timedelta(days=14)
        
        meta = fetch_metadata(ticker)
        sector = meta.get('sector')

        earnings_date_dt = None
        if meta.get('earningsDate'):
            earnings_date_dt = datetime.fromisoformat(meta['earningsDate'])

        if earnings_date_dt is None:
            earnings_date_dt = fetch_earnings_date_finviz(ticker)

        if earnings_date_dt:
            try:
                days_diff = (earnings_date_dt - now_utc).total_seconds() / 86400.0
                if 0 <= days_diff <= 15: tags.append("UPCOMING_EARNINGS")
                if -15 <= days_diff < 0: tags.append("POST_EARNINGS")
            except Exception: pass
        
        dp = cache_path(f"{ticker}_daily.csv"); daily = None
        if is_cache_fresh(dp, 6):
            try: 
                daily = pd.read_csv(dp, parse_dates=["Date"])
                if not daily.empty:
                    if daily['Date'].dt.tz is None: daily['Date'] = daily['Date'].dt.tz_localize(timezone.utc)
                    else: daily['Date'] = daily['Date'].dt.tz_convert(timezone.utc)
            except: daily = None
                
        if daily is None or daily.empty:
            daily = fetch_chart_yahoo_json(ticker, interval="1d", days=DAILY_LOOKBACK_DAYS)
            if daily is None or daily.empty: return None 
            try: daily.to_csv(dp, index=False)
            except: pass
        
        if daily is not None and not daily.empty:
             daily['Date'] = daily['Date'].dt.tz_convert(timezone.utc)
        else: return None

        if len(daily) < 21: return None
            
        daily_ind = compute_indicators(daily)
        daily_closes = daily_ind['Close'].tolist()

        # --- NEW PERFORMANCE & RSI FILTERS (Fixed Position) ---
        if len(daily_closes) >= 2:
            last_close = daily_closes[-1]
            
            # Helper to calculate return percentages
            def get_ret(days):
                if len(daily_closes) > days:
                    prev = daily_closes[-(days+1)]
                    return (last_close - prev) / prev if prev != 0 else 0
                return 0

            # Performance tags
            m_ret = get_ret(21); q_ret = get_ret(63); h_ret = get_ret(126); y_ret = get_ret(252)
            
            if m_ret > 0.05: tags.append("MONTHLY_UP")
            elif m_ret < -0.05: tags.append("MONTHLY_DOWN")
            
            if q_ret > 0.10: tags.append("QUARTERLY_UP")
            elif q_ret < -0.10: tags.append("QUARTERLY_DOWN")
            
            if h_ret > 0.15: tags.append("HALFYEARLY_UP")
            elif h_ret < -0.15: tags.append("HALFYEARLY_DOWN")
            
            if y_ret > 0.20: tags.append("YEARLY_UP")
            elif y_ret < -0.20: tags.append("YEARLY_DOWN")

            # YTD Calculation
            year_start = datetime(now_utc.year, 1, 1, tzinfo=timezone.utc)
            ytd_df = daily_ind[daily_ind['Date'] >= year_start]
            if not ytd_df.empty:
                ytd_start_price = ytd_df['Close'].iloc[0]
                ytd_ret = (last_close - ytd_start_price) / ytd_start_price
                if ytd_ret > 0: tags.append("YTD_UP")
                else: tags.append("YTD_DOWN")

            # RSI Logic
            last_rsi = daily_ind['RSI'].iloc[-1]
            if last_rsi >= 70: tags.append("RSI_OVERBOUGHT")
            elif last_rsi <= 30: tags.append("RSI_OVERSOLD")
        # --- END NEW FILTERS ---

        daily_ext = find_local_extrema(daily_closes, lookback=14)

        ip = cache_path(f"{ticker}_intraday.csv"); intr = None
        if is_cache_fresh(ip, 1):
            try: 
                intr = pd.read_csv(ip, parse_dates=["Date"])
                if not intr.empty:
                    if intr['Date'].dt.tz is None: intr['Date'] = intr['Date'].dt.tz_localize(timezone.utc)
                    else: intr['Date'] = intr['Date'].dt.tz_convert(timezone.utc)
            except: intr = None
                
        if intr is None or intr.empty:
            intr = fetch_intraday(ticker, interval=INTRADAY_INTERVAL, days=INTRADAY_DAYS)
            if intr is not None and not intr.empty:
                try: intr.to_csv(ip, index=False)
                except: pass
        
        if intr is not None and not intr.empty:
            intr['Date'] = intr['Date'].dt.tz_convert(timezone.utc)

        intr_ind = compute_indicators(intr) if (intr is not None and not intr.empty) else pd.DataFrame()
        intr_closes = intr_ind['Close'].tolist() if not intr_ind.empty else []
        intr_ext = find_local_extrema(intr_closes, lookback=30)
        
        wp = cache_path(f"{ticker}_weekly.csv"); weekly = None
        if is_cache_fresh(wp, 24):
            try: 
                weekly = pd.read_csv(wp, parse_dates=["Date"])
                if not weekly.empty:
                    if weekly['Date'].dt.tz is None: weekly['Date'] = weekly['Date'].dt.tz_localize(timezone.utc)
                    else: weekly['Date'] = weekly['Date'].dt.tz_convert(timezone.utc)
            except: weekly = None

        if weekly is None or weekly.empty:
            weekly = fetch_weekly(ticker, days=WEEKLY_LOOKBACK_DAYS)
            if weekly is not None and not weekly.empty:
                try: weekly.to_csv(wp, index=False)
                except: pass

        if weekly is not None and not weekly.empty:
            weekly['Date'] = weekly['Date'].dt.tz_convert(timezone.utc)

        weekly_ind = compute_indicators(weekly) if (weekly is not None and not weekly.empty) else pd.DataFrame()
        weekly_closes = weekly_ind['Close'].tolist() if not weekly_ind.empty else []
        weekly_ext = find_local_extrema(weekly_closes, lookback=8)
        
        if not intr_ind.empty:
            for p in reversed(intr_ext.get('peaks',[])):
                if p < 0 or p >= len(intr_ind): continue
                if (len(intr_ind)-1 - p) <= 2 and pivot_confirmable(intr_ind,p,'peak'):
                    tags.append("INTRADAY_TOP"); break
            for p in reversed(intr_ext.get('troughs',[])):
                if p < 0 or p >= len(intr_ind): continue
                if (len(intr_ind)-1 - p) <= 2 and pivot_confirmable(intr_ind,p,'trough'):
                    tags.append("INTRADAY_BOTTOM"); break
        for p in reversed(daily_ext.get('peaks',[])):
            if p < 0 or p >= len(daily_ind): continue
            dt = daily_ind['Date'].iloc[p]
            if dt >= recent_thresh_daily and (len(daily_ind)-1 - p) <= 2 and pivot_confirmable(daily_ind,p,'peak'):
                tags.append("DAILY_TOP")
                try:
                    peak_high = daily_ind['High'].iloc[p]; atr_val = daily_ind['ATR'].iloc[p]; last_close = daily_closes[-1]
                    sl = peak_high + (1.0 * atr_val); tp = last_close - (2.0 * (sl - last_close))
                    trade_details['DAILY_TOP'] = { 'sl': sl, 'tp': tp, 'entry': last_close, 'desc': f"Daily top confirmed near {money(peak_high)}. Entry (short): {money(last_close)}."}
                except: pass
                break
        for p in reversed(daily_ext.get('troughs',[])):
            if p < 0 or p >= len(daily_ind): continue
            dt = daily_ind['Date'].iloc[p]
            if dt >= recent_thresh_daily and (len(daily_ind)-1 - p) <= 2 and pivot_confirmable(daily_ind,p,'trough'):
                tags.append("DAILY_BOTTOM")
                try:
                    trough_low = daily_ind['Low'].iloc[p]; atr_val = daily_ind['ATR'].iloc[p]; last_close = daily_closes[-1]
                    sl = trough_low - (1.0 * atr_val); tp = last_close + (2.0 * (last_close - sl)) 
                    trade_details['DAILY_BOTTOM'] = { 'sl': sl, 'tp': tp, 'entry': last_close, 'desc': f"Daily bottom confirmed near {money(trough_low)}. Entry (long): {money(last_close)}."}
                except: pass
                break
        if len(daily_closes) >= 9:
            last9_closes = daily_closes[-9:]; last_close = daily_closes[-1]
            if not pd.isna(last_close) and last_close >= max(last9_closes): tags.append("NINE_DAY_HIGH")
            if not pd.isna(last_close) and last_close <= min(last9_closes): tags.append("NINE_DAY_LOW")
        if len(daily_closes) >= 21:
            prev20_high = max(daily_closes[-21:-1]); prev20_low = min(daily_closes[-21:-1]); last_close = daily_closes[-1]
            if not pd.isna(last_close):
                if last_close > prev20_high:
                    tags.append("BREAKOUT_UP")
                    try:
                        atr_val = daily_ind['ATR'].iloc[-1]; sl = prev20_high - (1.0 * atr_val); tp = last_close + (2.0 * (last_close - sl))
                        trade_details['BREAKOUT_UP'] = { 'sl': sl, 'tp': tp, 'entry': last_close, 'desc': f"Breakout above 20-day high ({money(prev20_high)}). Entry: {money(last_close)}."}
                    except: pass
                if last_close < prev20_low:
                    tags.append("BREAKOUT_DOWN")
                    try:
                        atr_val = daily_ind['ATR'].iloc[-1]; sl = prev20_low + (1.0 * atr_val); tp = last_close - (2.0 * (sl - last_close))
                        trade_details['BREAKOUT_DOWN'] = { 'sl': sl, 'tp': tp, 'entry': last_close, 'desc': f"Breakdown below 20-day low ({money(prev20_low)}). Entry: {money(last_close)}."}
                    except: pass
        
        found_wk_top = False; found_wk_bot = False
        wk_top_dt = datetime.min.replace(tzinfo=timezone.utc); wk_bot_dt = datetime.min.replace(tzinfo=timezone.utc)
        if not weekly_ind.empty:
            for p in reversed(weekly_ext.get('peaks',[])):
                if p < 0 or p >= len(weekly_ind): continue
                dt = weekly_ind['Date'].iloc[p]
                if dt >= recent_thresh_weekly: found_wk_top = True; wk_top_dt = dt; break
            for p in reversed(weekly_ext.get('troughs',[])):
                if p < 0 or p >= len(weekly_ind): continue
                dt = weekly_ind['Date'].iloc[p]
                if dt >= recent_thresh_weekly: found_wk_bot = True; wk_bot_dt = dt; break
            if found_wk_top and found_wk_bot:
                if wk_top_dt > wk_bot_dt: tags.append("RECENT_WEEKLY_TOP") 
                else: tags.append("RECENT_WEEKLY_BOTTOM") 
            elif found_wk_top: tags.append("RECENT_WEEKLY_TOP")
            elif found_wk_bot: tags.append("RECENT_WEEKLY_BOTTOM")

        intr_top = max([intr_closes[i] for i in intr_ext['peaks']]) if intr_ext['peaks'] else None
        intr_bottom = min([intr_closes[i] for i in intr_ext['troughs']]) if intr_ext['troughs'] else None
        
        last_close = daily_closes[-1] if daily_closes else None
        last_date = None
        if not daily_ind.empty:
            try: last_date = daily_ind['Date'].iloc[-1].strftime('%Y-%m-%d')
            except: last_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        result = {
            "ticker": ticker, "tags": sorted(list(set(tags))), "daily_df": daily_ind,
            "intraday_df": intr_ind, "daily_len": len(daily_ind), "intr_len": len(intr_ind),
            "daily_peaks": daily_ext['peaks'], "daily_troughs": daily_ext['troughs'],
            "intr_peaks": intr_ext['peaks'], "intr_troughs": intr_ext['troughs'],
            "intr_top": intr_top, "intr_bottom": intr_bottom, "earnings_date": earnings_date_dt,
            "trade_details": trade_details, "sector": sector,
            "last_close": last_close, "last_date": last_date,
        }
        
        if entry_data:
            result['entry_price'] = entry_data.get('price')
            result['entry_date'] = entry_data.get('date')
            
        return result
    except Exception as e:
        logger.error("analyze_ticker %s error: %s", ticker, e)
        return None

def worker_fn(in_q:queue.Queue, out_q:queue.Queue, watchlist_data: Dict[str, Any]): 
    while True:
        try: t = in_q.get_nowait()
        except queue.Empty: break
        try:
            entry_data = watchlist_data.get(t)
            res = analyze_ticker(t, entry_data=entry_data) 
            if res: out_q.put(res)
        except Exception: pass
        finally:
            in_q.task_done()
            time.sleep(0.03 + random.random()*0.07)

def run_scan_list(ticker_list:List[str], watchlist_data: Dict[str, Any])->List[Dict[str,Any]]: 
    q_in = queue.Queue(); q_out = queue.Queue()
    for t in ticker_list: q_in.put(t)
    threads=[]
    n = min(max(1, q_in.qsize()), THREADS)
    for _ in range(n):
        th = threading.Thread(target=worker_fn, args=(q_in, q_out, watchlist_data), daemon=True)
        th.start(); threads.append(th)
    q_in.join()
    results=[]
    while not q_out.empty():
        try: results.append(q_out.get_nowait())
        except: break
    logger.info(f"Scan complete, {len(results)} results collected.")
    return results

# -------------------- HTML helpers --------------------
def _df_to_payload(df:pd.DataFrame, max_bars:int=800)->Dict[str,Any]:
    if df is None or df.empty: return {}
    d = df.copy().tail(max_bars).reset_index(drop=True)
    labels = []
    is_intraday = False
    if len(d) > 1:
        try:
            time_delta = d['Date'].iloc[1] - d['Date'].iloc[0]
            if time_delta < timedelta(days=1): is_intraday = True
        except: pass
    date_format = "%Y-%m-%d %H:%M" if is_intraday else "%Y-%m-%d"
    for x in d['Date'].tolist():
        try: labels.append(x.strftime(date_format))
        except: labels.append(str(x))
    return {
        "labels": labels,
        "open": [None if pd.isna(x) else float(x) for x in d['Open'].tolist()],
        "high": [None if pd.isna(x) else float(x) for x in d['High'].tolist()],
        "low": [None if pd.isna(x) else float(x) for x in d['Low'].tolist()],
        "close": [None if pd.isna(x) else float(x) for x in d['Close'].tolist()],
        "volume": [None if pd.isna(x) else int(x) for x in d['Volume'].tolist()],
        "bb_upper": [None if 'BB_upper' not in d or pd.isna(x) else float(x) for x in (d.get('BB_upper', pd.Series([np.nan]*len(d))).tolist())],
        "bb_mid": [None if 'BB_mid' not in d or pd.isna(x) else float(x) for x in (d.get('BB_mid', pd.Series([np.nan]*len(d))).tolist())],
        "bb_lower": [None if 'BB_lower' not in d or pd.isna(x) else float(x) for x in (d.get('BB_lower', pd.Series([np.nan]*len(d))).tolist())]
    }

def _df_to_table_data(df:pd.DataFrame, num_rows:int)->Dict[str,Any]:
    if df is None or df.empty: return {}
    d = df.copy().tail(num_rows).reset_index(drop=True)
    is_intraday = False
    if len(d) > 1:
        try:
            time_delta = d['Date'].iloc[1] - d['Date'].iloc[0]
            if time_delta < timedelta(days=1): is_intraday = True
        except: pass
    date_format = "%Y-%m-%d %H:%M" if is_intraday else "%Y-%m-%d"
    labels = []
    for x in d['Date'].tolist():
        try: labels.append(x.strftime(date_format))
        except: labels.append(str(x))
    return {
        "labels": labels, "open": [float(x) if not pd.isna(x) else None for x in d['Open'].tolist()],
        "high": [float(x) if not pd.isna(x) else None for x in d['High'].tolist()],
        "low": [float(x) if not pd.isna(x) else None for x in d['Low'].tolist()],
        "close": [float(x) for x in d['Close'].tolist()],
        "volume": [int(x) if not pd.isna(x) else 0 for x in d['Volume'].tolist()]
    }

def make_inline_payload_js(div_id:str, chart_payload:Dict[str,Any], markers:List[Dict[str,Any]]=None, table_data:Dict[str,Any]=None)->str:
    try:
        obj = {"data": chart_payload, "markers": markers or [], "tableData": table_data or {}}
        js = "window._tb_chart_payloads = window._tb_chart_payloads || {};\n"
        js += f"window._tb_chart_payloads['{div_id}'] = {json.dumps(obj)};\n"
        return "<script>" + js + "</script>"
    except Exception: return ""

def embed_local_plotly_text() -> str:
    try:
        if os.path.exists(LOCAL_PLOTLY_FILE):
            with open(LOCAL_PLOTLY_FILE, "r", encoding="utf-8", errors="ignore") as f: return f.read()
    except Exception: pass
    return ""

def embed_local_xlsx_text() -> str:
    try:
        if os.path.exists(XLSX_JS_FILE):
            with open(XLSX_JS_FILE, "r", encoding="utf-8", errors="ignore") as f: return f.read()
    except Exception: pass
    return ""

# -------------------- Build HTML --------------------
def generate_html_page(
    page_type: str, 
    data_groups: Dict[str,List[Dict[str,Any]]],
    outpath: str,
    nav_link: Dict[str, str], 
    source_info: str,
    timestamp_str: str,
    report_js_template: str,
    existing_favorites: List[Dict[str, Any]] = None 
):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 1. Page Title Mapping (Including Inbox)
    page_title = {
        "universal":"Universal", 
        "watchlist":"Watchlist", 
        "sector":"Sector",
        "inbox": "Inbox Alerts"
    }.get(page_type, "Report")
    
    group_names = list(data_groups.keys())
    
    # Pass existing favorites to JS
    fav_json = json.dumps(existing_favorites or [])

    # 2. CSS Loading & Setup
    try:
        with open('report_style.css', 'r', encoding='utf-8') as f: 
            report_css = f.read()
    except Exception:
        report_css = "body { font-family: sans-serif; background-color: #f8f9fa; color: #333; padding: 20px; }"

    scroll_btn_css = """
    #scrollTopBtn {
      display: none; position: fixed; bottom: 20px; right: 30px; z-index: 99;
      border: none; outline: none; background-color: #007bff; color: white;
      cursor: pointer; padding: 10px 15px; border-radius: 8px; font-size: 1rem; font-weight: bold;
    }
    #scrollTopBtn:hover { background-color: #0056b3; }
    """
    report_css += scroll_btn_css

    parts=[]
    parts.append("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>")
    parts.append(f"<title>TopBottom {page_title} {SCRIPT_VERSION} — {now_str}</title>")
    parts.append(f"<style>{report_css}</style>")
    parts.append("</head><body>")

    # --- 3. Toolbar (Navigation) ---
    parts.append("<div class='top-toolbar'><div class='brand'>TopBottom — " + SCRIPT_VERSION + "</div>")
    parts.append("<div class='controls'>")

    # Universal
    if page_type == 'universal': parts.append("<button class='btn primary'>🌐 Universal</button>")
    elif 'univ_file' in nav_link: parts.append(f"<a href='{nav_link['univ_file']}' class='btn ghost'>🌐 Universal</a>")

    # Watchlist
    if 'watch_file' in nav_link:
        if page_type == 'watchlist': parts.append("<button class='btn primary'>⭐ Watchlist</button>")
        else: parts.append(f"<a href='{nav_link['watch_file']}' class='btn ghost'>⭐ Watchlist</a>")
    
    # Inbox (NEW LOGIC)
    if 'inbox_file' in nav_link:
        if page_type == 'inbox': parts.append("<button class='btn primary'>📩 Inbox</button>")
        else: parts.append(f"<a href='{nav_link['inbox_file']}' class='btn ghost'>📩 Inbox</a>")

    # Sector
    if 'sector_file' in nav_link:
        if page_type == 'sector': parts.append("<button class='btn primary'>📊 Sector</button>")
        else: parts.append(f"<a href='{nav_link['sector_file']}' class='btn ghost'>📊 Sector</a>")
            
    if 'fav_file' in nav_link: parts.append(f"<a href='{nav_link['fav_file']}' class='btn ghost'>❤️ Favorites</a>")

    parts.append("</div>") 
    parts.append("<div style='width:16px'></div>")
    
    # --- 4. Filter Area ---
    parts.append("<div class='controls'><div class='filter-area'>") 
    parts.append("<button class='btn' onclick=\"filterGroup('ALL')\">All</button>")
    for gname in sorted(group_names):
        parts.append(f"<button class='btn' data-filter='{gname}' data-text='{gname}' onclick=\"toggleTagButton(this,'{gname}')\">{gname}</button>")
    
    parts.append("<div style='width:12px; border-left:1px solid #ccc; margin:0 4px;'></div>")

    # Technical Buttons
    tech_btns = [
        ('INTRADAY_TOP', 'Intraday Top'), ('INTRADAY_BOTTOM', 'Intraday Bottom'),
        ('DAILY_TOP', 'Daily Top'), ('DAILY_BOTTOM', 'Daily Bottom'),
        ('NINE_DAY_HIGH', '9-day High'), ('NINE_DAY_LOW', '9-day Low'),
        ('BREAKOUT_UP', 'Breakout ↑'), ('BREAKOUT_DOWN', 'Breakout ↓'),
        ('UPCOMING_EARNINGS', 'Upcoming E (15d)'), ('POST_EARNINGS', 'Post E (15d)'),
        ('RECENT_WEEKLY_TOP', 'Recent W-Top'), ('RECENT_WEEKLY_BOTTOM', 'Recent W-Bottom'),
        ('MONTHLY_UP', 'Month ↑'), ('MONTHLY_DOWN', 'Month ↓'),
        ('YTD_UP', 'YTD ↑'), ('YTD_DOWN', 'YTD ↓'),
        ('RSI_OVERBOUGHT', 'RSI OB'), ('RSI_OVERSOLD', 'RSI OS')
    ]

    for tag_id, label in tech_btns:
        style = ""
        if any(x in tag_id for x in ['TOP', 'UP', 'HIGH', 'OVERSOLD']): 
            style = "background-color:#f0fff4;color:#2f855a;border-color:#c6f6d5;"
        elif any(x in tag_id for x in ['BOTTOM', 'DOWN', 'LOW', 'OVERBOUGHT']): 
            style = "background-color:#fff5f5;color:#c53030;border-color:#fecaca;"
        parts.append(f"<button class='btn' data-filter='{tag_id}' data-text='{label}' onclick=\"toggleTagButton(this,'{tag_id}')\" style='{style}'>{label}</button>")

    parts.append("</div>")
    
    # Utility Buttons
    parts.append("<div style='margin-left:auto;display:flex;gap:8px;align-items:center'>")
    parts.append("<button class='btn' style='background:#22c55e;color:white;border:1px solid #16a34a;' onclick='exportFavorites()'>💾 Save Favorites DB</button>") 
    parts.append("<select id='modeSelect' class='btn' onchange='setMode(this.value); updateFilterState();'><option value='STRICT' selected>STRICT</option><option value='NORMAL'>NORMAL</option><option value='LOOSE'>LOOSE</option></select>")
    parts.append("<button class='btn' onclick='downloadCSV()'>Download CSV</button>")
    parts.append("<button class='btn' onclick='manualRefresh()'>🔄 Refresh</button>")
    parts.append("<label class='small' style='margin-left:6px'>Auto-Refresh</label><input id='autoRefreshToggle' type='checkbox' onchange='toggleAutoRefresh(this.checked)'>")
    parts.append("</div></div>") 

    # --- 5. Main Content ---
    parts.append("<div class='container' style='max-width: 95%;'>")
    parts.append("<div class='card'><div class='header-row'><div><strong>Source:</strong> " + source_info + "</div><div id='statusMsg' class='status'>Mode: STRICT • Filters: none</div></div><div class='small'>Tip: Click '+ Add' on multiple stocks, then 'Save Favorites DB' to download. Next time you run the script, it will auto-import from Downloads.</div></div>")
    
    parts.append(f"<div id='view_content_area' data-page-type='{page_type}'>") 
    parts.append(f"<h2 style='margin-top:8px'>{page_title} Universe</h2>")
    parts.append("<div id='no_results_msg' class='card' style='display:none; color: var(--muted); text-align: center; padding: 30px;'>No stocks match the current filter combination.</div>")

    total_signals = sum(len(v) for v in data_groups.values())
    if not data_groups or total_signals==0:
        parts.append(f"<div class='card'>No signals in {page_title}</div>")
    else:
        for tab, items in data_groups.items():
            if not items: continue
            
            group_name = tab 
            safe_tab_id = "".join(c for c in tab if c.isalnum())
            parts.append(f"<div class='card group-card' id='group_card_{safe_tab_id}' data-group-name='{group_name}'><h3>{group_name} ({len(items)})</h3>")
            
            for idx, s in enumerate(items[:UNIVERSE_LIMIT]):
                ticker = s.get('ticker')
                tags_from_analysis = s.get('tags', [])
                
                all_tags = set(tags_from_analysis)
                all_tags.add(group_name) 
                if page_type == 'sector' and s.get('sector') and s.get('sector') not in group_names:
                    all_tags.add(s['sector'])
                data_tags_str = ",".join(sorted(list(all_tags)))
                
                # Badges
                badges_html = ""
                if not tags_from_analysis:
                    badges_html = "<span class='badge' style='background-color:#f7fafc; color:#718096; border:1px solid #e2e8f0;'>No Signal</span>"
                else:
                    for tag in tags_from_analysis:
                        badges_html += f" <span class='badge'>{tag}</span>"

                sector_name = s.get('sector', 'N/A')
                if (page_type == 'watchlist' or page_type == 'inbox') and sector_name != 'N/A':
                    badges_html += f" <span class='badge' style='background:#fcf5ff; color:#7b3896; border:1px solid #e8d0f1;'>🏢 {sector_name}</span>"

                if page_type == 'inbox' and s.get('entry_price'):
                    badges_html += f" <span class='badge' style='background:#fffaf0; color:#9c4221; border:1px solid #feebc8;'>📩 Alert: {money(s['entry_price'])}</span>"

                earnings_date_dt = s.get('earnings_date')
                earnings_str = earnings_date_dt.strftime('%Y-%m-%d') if isinstance(earnings_date_dt, datetime) else ""
                if earnings_str:
                    badges_html += f" <span class='badge' style='background:#f0f5ff; color:#434190; border:1px solid #c3dafe;'>🗓️ Earnings: {earnings_str}</span>"

                parts.append(f"<div class='signal_card card' data-ticker='{ticker}' data-tags='{data_tags_str}'>")
                
                last_close = s.get('last_close')
                last_date = s.get('last_date')
                add_button_html = ""
                
                if last_close and not pd.isna(last_close):
                    c_price = f"{last_close:.2f}"
                    c_date = last_date if last_date else datetime.now().strftime('%Y-%m-%d')
                    add_button_html = f"<button id='favbtn_{ticker}' class='btn' onclick=\"addToFavorite('{ticker}', '{c_price}', '{c_date}', this)\" style='font-size: 0.8rem; padding: 4px 8px; margin-left: 10px;'>+ Add to Favorite</button>"
                
                parts.append(f"<div style='display:flex;justify-content:space-between;align-items:center'><div><a href='https://finviz.com/quote.ashx?t={ticker}&p=d' target='_blank' style='font-weight:700;color:var(--primary);font-size:1.1rem;'>{ticker}</a> {add_button_html} {badges_html}</div><div class='small'>{('Intraday Top: '+money(s.get('intr_top'))) if s.get('intr_top') else ''}</div></div>")
                
                # --- NEW MULTI-TAG TRADE DETAILS ---
                trade_details = s.get('trade_details', {})
                if trade_details and tags_from_analysis:
                    details_html = f"<div style='border: 1px solid #e2e8f0; padding: 12px; border-radius: 8px; margin-top: 12px; background: #fdfdfd; font-size: 0.9em;'>"
                    details_html += f"<h4 style='margin-top: 0; margin-bottom: 8px; color: var(--primary);'>Trade Strategy Analysis</h4>"
                    
                    ordered_tags = sorted(tags_from_analysis, key=lambda x: "0" if "DAILY" in x or "BREAKOUT" in x else "1")
                    for tag in ordered_tags:
                        if tag in trade_details:
                            detail = trade_details[tag]
                            details_html += f"<div style='margin-bottom: 10px; border-left: 3px solid #cbd5e0; padding-left: 10px;'>"
                            details_html += f"<div style='font-weight:bold;'>Signal: {tag}</div>"
                            details_html += f"<p style='margin: 4px 0;'><strong>Technicals:</strong> {detail.get('desc', 'n/a')}</p>"
                            details_html += "<div style='display: flex; flex-wrap: wrap; gap: 20px;'>"
                            details_html += f"<div><strong>Entry:</strong> {money(detail.get('entry'))}</div>"
                            details_html += f"<div><strong style='color: #2f855a;'>Target:</strong> {money(detail.get('tp'))}</div>"
                            details_html += f"<div><strong style='color: #c53030;'>Stop Loss:</strong> {money(detail.get('sl'))}</div>"
                            details_html += "</div></div>"
                    
                    if earnings_str:
                         details_html += f"<p style='margin: 8px 0 0 0; color:#434190; font-size: 0.85em;'><strong>🗓️ Upcoming Earnings:</strong> {earnings_str}</p>"
                    details_html += "</div>"
                    parts.append(details_html)
                
                # Price Trend Bar
                parts.append(generate_trend_html(s.get('daily_df')))

                # Charts
                intr_div = f"{page_type}_intr_{idx}_{safe_tab_id}"; daily_div = f"{page_type}_daily_{idx}_{safe_tab_id}"
                parts.append(f"<div class='grid' style='margin-top:8px'>")
                parts.append(f"<div><div id='{intr_div}' style='height:{CHART_HEIGHT}px;min-width:240px'></div><div class='chart-controls'>")
                parts.append(f"<a href='https://stockanalysis.com/stocks/{ticker.lower()}/' target='_blank' class='btn'>📈 Forecast</a>")
                parts.append(f"<button class='btn' onclick=\"toggleTable('{intr_div}')\">📋 Toggle Table</button>")
                parts.append(f"</div><div id='{intr_div}_table' class='chart-table' style='display:none'></div></div>")
                parts.append(f"<div><div id='{daily_div}' style='height:{CHART_HEIGHT}px;min-width:240px'></div><div class='chart-controls'>")
                parts.append(f"<a href='https://stockanalysis.com/stocks/{ticker.lower()}/' target='_blank' class='btn'>📈 Forecast</a>")
                parts.append(f"<button class='btn' onclick=\"toggleTable('{daily_div}')\">📋 Toggle Table</button>")
                parts.append(f"</div><div id='{daily_div}_table' class='chart-table' style='display:none'></div></div>")
                parts.append(f"</div>") 
                
                intr_payload = _df_to_payload(s.get('intraday_df'))
                daily_payload = _df_to_payload(s.get('daily_df'))
                intr_table_data = _df_to_table_data(s.get('intraday_df'), TABLE_ROWS_INTRADAY)
                daily_table_data = _df_to_table_data(s.get('daily_df'), TABLE_ROWS_DAILY)
                
                def get_markers(dframe, peaks, troughs, length):
                    m = []
                    if dframe is not None and not dframe.empty:
                        try:
                            for p in (peaks or [])[-50:]:
                                if isinstance(p,int) and 0 <= p < length: m.append({"type":"peak","pos":p,"price": float(dframe['Close'].iloc[p])})
                            for t in (troughs or [])[-50:]:
                                if isinstance(t,int) and 0 <= t < length: m.append({"type":"trough","pos":t,"price": float(dframe['Close'].iloc[t])})
                        except: pass
                    return m

                im = get_markers(s.get('intraday_df'), s.get('intr_peaks'), s.get('intr_troughs'), s.get('intr_len',0))
                dm = get_markers(s.get('daily_df'), s.get('daily_peaks'), s.get('daily_troughs'), s.get('daily_len',0))

                if intr_payload.get("labels"): parts.append(make_inline_payload_js(intr_div, intr_payload, im, intr_table_data))
                if daily_payload.get("labels"): parts.append(make_inline_payload_js(daily_div, daily_payload, dm, daily_table_data))
                
                parts.append("</div>") # End signal_card
            parts.append("</div>") # End group-card
    parts.append("</div>") # End container

    # --- 6. Logs & Footer ---
    logs = log_buffer.getvalue()[-30000:]
    parts.append("<div class='card'><details open><summary style='cursor: pointer; font-weight: bold; font-size: 1.25rem; margin-bottom: 10px;'>Latest Logs (Click to collapse)</summary>")
    parts.append("<div style='font-family:monospace;background:#081025;color:#e6f1ff;padding:10px;border-radius:8px;white-space:pre-wrap;font-size:12px; margin-top: 10px; max-height: 400px; overflow-y: auto;'>") 
    parts.append((logs or "").replace("<","&lt;").replace(">","&gt;"))
    parts.append("</div></details></div>")

    parts.append(f"<div class='footer-small'>Generated by TopBottom_Universe {SCRIPT_VERSION} — {now_str}</div>")
    parts.append("<button onclick='scrollToTop()' id='scrollTopBtn' title='Go to top'>↑ Top</button>")

    # --- 7. JS Dependencies ---
    if plotly_text := embed_local_plotly_text(): parts.append("<script>" + plotly_text + "</script>")
    else: parts.append("<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>")
    if xlsx_text := embed_local_xlsx_text(): parts.append("<script>" + xlsx_text + "</script>")
    else: parts.append("<script src='https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js'></script>")
    
    parts.append(f"<script>window.initialFavorites = {fav_json};</script>")

    js_final = report_js_template.replace("%EAGER%", str(EAGER_RENDER_FIRST_N)).replace("%REF%", str(AUTO_REFRESH_MINUTES_DEFAULT)).replace("%HEIGHT%", str(CHART_HEIGHT)).replace("%TABLEROWS_DAILY%", str(TABLE_ROWS_DAILY)).replace("%TABLEROWS_INTRADAY%", str(TABLE_ROWS_INTRADAY))
    parts.append("<script>" + js_final + "</script>")

    # --- 8. Dynamic Override Logic (Restoring Count Functionality) ---
    group_names_json = json.dumps(group_names)
    js_override_logic = f"""
    <script>
    (function() {{
        window.currentFavorites = window.initialFavorites || [];
        
        window.updateFavButtons = function() {{
            window.currentFavorites.forEach(function(fav) {{
                var btn = document.getElementById('favbtn_' + fav.Ticker);
                if (btn) {{
                    btn.innerHTML = "✅ Added";
                    btn.style.background = "#e2e8f0"; btn.style.color = "#333"; btn.disabled = true;
                }}
            }});
        }};

        window.addToFavorite = function(ticker, price, date, btnElement) {{
            if (window.currentFavorites.find(f => f.Ticker === ticker)) return;
            var newFav = {{ 'Ticker': ticker, 'EntryPrice': price, 'EntryDate': date }};
            window.currentFavorites.push(newFav);
            localStorage.setItem('local_favorites_pending', JSON.stringify(window.currentFavorites));
            if (btnElement) {{ btnElement.innerHTML = "✅ Added"; btnElement.disabled = true; }}
        }};

        window.exportFavorites = function() {{
            var ws = XLSX.utils.json_to_sheet(window.currentFavorites);
            var wb = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(wb, ws, "Sheet1");
            XLSX.writeFile(wb, "favorites.xlsx");
        }};

        window.updateFilterState = function() {{
            var allCards = document.querySelectorAll('.signal_card');
            var visibleCards = document.querySelectorAll('.signal_card:not([style*="display: none"])');
            var allButtons = document.querySelectorAll('.filter-area .btn[data-filter]');
            var statusMsg = document.getElementById('statusMsg');
            var currentMode = document.getElementById('modeSelect') ? document.getElementById('modeSelect').value : 'STRICT';
            
            var activeFilterNames = [];
            document.querySelectorAll('.filter-area .btn.active').forEach(b => {{ if(b.dataset.text) activeFilterNames.push(b.dataset.text); }});
            
            if (statusMsg) {{
                statusMsg.innerHTML = `Mode: ${{currentMode}} • Filters: ${{activeFilterNames.length > 0 ? activeFilterNames.join(' AND ') : 'none'}} • Showing ${{visibleCards.length}} of ${{allCards.length}} stocks`;
            }}

            allButtons.forEach(function(btn) {{
                if (!btn.dataset.text) return;
                var tagToCount = btn.dataset.filter;
                if (btn.classList.contains('active')) {{
                    btn.innerHTML = btn.dataset.text;
                }} else {{
                    var count = 0;
                    visibleCards.forEach(function(card) {{
                        var tags = card.dataset.tags ? card.dataset.tags.split(',') : [];
                        if (tags.includes(tagToCount)) count++;
                    }});
                    btn.innerHTML = `${{btn.dataset.text}} (${{count}})`;
                }}
            }});
        }};

        window.addEventListener('load', function() {{
            window.updateFavButtons();
            var _orig_toggle = window.toggleTagButton;
            var _orig_filter = window.filterGroup;
            var groupFilters = new Set({group_names_json});
            var allGroupCards = document.querySelectorAll('.card.group-card');

            window.filterGroup = function(tag) {{
                _orig_filter(tag);
                allGroupCards.forEach(c => c.style.display = 'block');
                window.updateFilterState();
            }};

            window.toggleTagButton = function(btn, tag) {{
                _orig_toggle(btn, tag);
                var activeFilters = Array.from(document.querySelectorAll('.filter-area .btn.active')).map(b => b.dataset.filter);
                var currentGroupFilters = activeFilters.filter(f => groupFilters.has(f));
                
                allGroupCards.forEach(function(gc) {{
                    if (currentGroupFilters.length > 0) {{
                        gc.style.display = currentGroupFilters.includes(gc.dataset.groupName) ? 'block' : 'none';
                    }} else {{
                        gc.style.display = 'block';
                    }}
                }});
                window.updateFilterState();
            }};
            window.updateFilterState();
        }});
    }})();
    </script>
    """
    parts.append(js_override_logic + "</body></html>")

    # 9. Final File Write
    try:
        with open(outpath, "w", encoding="utf-8") as f:
            f.write("\n".join(parts))
        logger.info("Saved HTML report: %s", outpath)
    except Exception as e:
        logger.error("Could not write HTML: %s", e)
# -------------------- Main --------------------
def clean_output_directory(path: str):
    if not os.path.exists(path):
        try: os.makedirs(path)
        except Exception: pass
        return
    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path): os.unlink(item_path)
            elif os.path.isdir(item_path): shutil.rmtree(item_path)
        except Exception: pass

def main():
    # --- AUTOMATION: Run auto-import first ---
    auto_import_favorites_from_downloads()

    clean_output_directory(MASTER_OUTPUT_DIR)
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(CHARTS_DIR, exist_ok=True)
            
    # --- 1. Load Watchlists & Favorites ---
    watchmap_final = {}
    watchdata_final = {}
    favmap_final = {}
    favdata_final = {}
    current_favorites_list = [] 

    if USE_WATCHLIST_EXCEL:
        watchmap_orig, watchdata_orig = load_watchlist_from_excel(WATCHLIST_FILE)
        watchmap_final = (watchmap_orig or {}).copy()
        watchdata_final = (watchdata_orig or {}).copy()
        
        favmap_new, favdata_new = load_watchlist_from_excel(FAVORITES_FILE)
        favmap_final = (favmap_new or {}).copy()
        favdata_final = (favdata_new or {}).copy()
        
        if favdata_new:
            for ticker, info in favdata_new.items():
                current_favorites_list.append({
                    'Ticker': ticker,
                    'EntryPrice': info.get('price'),
                    'EntryDate': info.get('date')
                })
        
        watchlist_tickers = set(t for group in watchmap_final.values() for t in group)
        favorite_tickers = set(t for group in favmap_final.values() for t in group)
        
        all_entry_data = watchdata_final.copy()
        all_entry_data.update(favdata_final) 

    # --- NEW: 2. Parse Inbox Tickers ---
    inbox_map = parse_inbox()  # Logic provided in previous step
    inbox_tickers = set(inbox_map.keys())
    # Update entry data with inbox prices/dates if available
    all_entry_data.update({t: {'price': v['price'], 'date': v['date']} for t, v in inbox_map.items()})

    # --- 3. Build Universal Ticker List ---
    universe_map = build_universe()
    all_universe_tickers = set(t for group in universe_map.values() for t in group)

    # --- 4. Combine Tickers and Process ---
    if 'watchlist_tickers' not in locals(): watchlist_tickers = set()
    if 'favorite_tickers' not in locals(): favorite_tickers = set()
    if 'all_entry_data' not in locals(): all_entry_data = {}

    # Added inbox_tickers to the union
    tickers_to_scan = all_universe_tickers.union(watchlist_tickers).union(favorite_tickers).union(inbox_tickers)
    logger.info("Total unique tickers to scan (including Inbox): %d", len(tickers_to_scan))

    q = queue.Queue()
    results = []
    for t in tickers_to_scan: q.put(t)
    
    def worker():
        while True:
            try: ticker = q.get_nowait()
            except queue.Empty: break
            try:
                entry_data = all_entry_data.get(ticker)
                result = analyze_ticker(ticker, entry_data=entry_data) 
                if result:
                    tags = result.get('tags', [])
                    # Include if flagged OR if it belongs to a specific user-defined list
                    if tags or (ticker in watchlist_tickers) or (ticker in favorite_tickers) or (ticker in inbox_tickers):
                        results.append(result)
            except Exception as e: logger.error(f"Worker error: {e}")
            finally: q.task_done()

    threads = []
    for _ in range(THREADS):
        t = threading.Thread(target=worker, daemon=True)
        t.start(); threads.append(t)
    q.join()
    
    # --- 5. Grouping Results ---
    groups_univ: Dict[str, List[Dict[str, Any]]] = {k:[] for k in universe_map.keys()}
    groups_wl: Dict[str, List[Dict[str, Any]]] = {k:[] for k in watchmap_final.keys()}
    groups_fav: Dict[str, List[Dict[str, Any]]] = {k:[] for k in favmap_final.keys()}
    groups_inbox: Dict[str, List[Dict[str, Any]]] = {"Recent Inbox Alerts": []} # NEW
    groups_sector: Dict[str, List[Dict[str, Any]]] = {}

    for r in results:
        ticker = r['ticker']
        # Group Universe
        for cat, tickers in universe_map.items():
            if ticker in tickers and r.get('tags'): groups_univ[cat].append(r)
        # Group Watchlist
        for cat, tickers in watchmap_final.items():
            if ticker in tickers: groups_wl[cat].append(r)
        # Group Favorites
        for cat, tickers in favmap_final.items():
            if ticker in tickers: groups_fav[cat].append(r)
        # Group Inbox (NEW)
        if ticker in inbox_tickers:
            groups_inbox["Recent Inbox Alerts"].append(r)
        
        # Sector grouping
        if r.get('tags'):
            sector_name = r.get('sector') or "Other"
            if sector_name not in groups_sector: groups_sector[sector_name] = []
            groups_sector[sector_name].append(r)
    
    # --- 6. Export and Generate Reports ---
    # (CSV/TXT Export logic remains the same)
    flagged_results = [r for r in results if r.get('tags')]
    if flagged_results:
        df_out = pd.DataFrame(flagged_results)
        # ... (keep existing dataframe processing) ...
        df_out.sort_values(by=['ticker'], inplace=True)
        df_out.to_csv(OUT_CSV, index=False)

    # --- 7. HTML Report Generation ---
    try:
        with open('report_script.js', 'r', encoding='utf-8') as f: report_js_template = f.read()
    except Exception: report_js_template = "alert('report_script.js not found.');"

    # Updated navigation with new inbox_file
    nav_links = {
        "univ_file": os.path.basename(OUT_HTML_UNIV),
        "watch_file": os.path.basename(OUT_HTML_WATCH),
        "sector_file": os.path.basename(OUT_HTML_SECTOR),
        "fav_file": os.path.basename(OUT_HTML_FAV),
        "inbox_file": os.path.basename(OUT_HTML_INBOX) # NEW
    }

    # Generate Universal, Watchlist, Sector reports
    generate_html_page(page_type="universal", data_groups=groups_univ, outpath=OUT_HTML_UNIV, nav_link=nav_links, source_info="Universal", timestamp_str=TIMESTAMP, report_js_template=report_js_template, existing_favorites=current_favorites_list)
    if watchmap_final:
        generate_html_page(page_type="watchlist", data_groups=groups_wl, outpath=OUT_HTML_WATCH, nav_link=nav_links, source_info="Watchlist", timestamp_str=TIMESTAMP, report_js_template=report_js_template, existing_favorites=current_favorites_list)
    generate_html_page(page_type="sector", data_groups=groups_sector, outpath=OUT_HTML_SECTOR, nav_link=nav_links, source_info="Sector", timestamp_str=TIMESTAMP, report_js_template=report_js_template, existing_favorites=current_favorites_list)
    
    # NEW: Generate Inbox Report
    generate_html_page(
        page_type="inbox", data_groups=groups_inbox, outpath=OUT_HTML_INBOX,
        nav_link=nav_links, source_info="Inbox Alerts", timestamp_str=TIMESTAMP,
        report_js_template=report_js_template, existing_favorites=current_favorites_list
    )

    if favmap_final:
        generate_favorites_tile_report(data_groups=groups_fav, outpath=OUT_HTML_FAV, nav_link=nav_links, source_info="Favorites", timestamp_str=TIMESTAMP, script_version=SCRIPT_VERSION, report_js_template=report_js_template)
        
    try:
        # Default open to the Inbox if items found, otherwise Universal
        target_open = OUT_HTML_INBOX if groups_inbox["Recent Inbox Alerts"] else OUT_HTML_UNIV
        if os.path.exists(target_open):
            if __name__ == "__main__" and os.getenv("GITHUB_ACTIONS") != "true":
             webbrowser.open(f'file://{os.path.abspath(target_open)}')
             logger.info("Opened HTML report: %s", os.path.abspath(target_open))
    except Exception as e: logger.error("Failed to open browser: %s", e)
     


def market_is_open():
    nyse = mcal.get_calendar("NYSE")
    now = pd.Timestamp.now(tz="America/New_York")
    sched = nyse.schedule(start_date=now.date(), end_date=now.date())
    if sched.empty:
        return False
    return sched.iloc[0]["market_open"] <= now <= sched.iloc[0]["market_close"]

   
if __name__ == "__main__":
    if not market_is_open():
     print("Market closed — skipping run.")
     sys.exit(0)
    main()



