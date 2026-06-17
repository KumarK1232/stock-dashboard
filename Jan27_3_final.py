#!/usr/bin/env python3
# TopBottom  c_Universe vFinal30 - Inbox Fixed & New Indicators Added
# --- EXPERT MODIFICATIONS (FIXED VERSION) ---
# 1. PRESERVED: All original logic (Inbox, Indicators, Trading Rules).
# 2. FIXED: Filters now   click and update counts dynamically.
# 3. FIXED: Charts now render using Lazy Loading (IntersectionObserver).
# 4. FIXED: CSS and JS are robustly embedded. 

from __future__ import annotations
from unittest import result
import os, sys, time, json, math, random, logging, urllib.request, urllib.parse, webbrowser
import glob
from io import StringIO
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import threading, queue
import shutil 
import imaplib
import email
import io
import pandas_market_calendars as mcal
from typing import List

try:
    import pandas as pd
    import numpy as np
    from bs4 import BeautifulSoup
    from dateutil import parser
except ImportError:
    print("CRITICAL ERROR: Missing libraries.")
    print("Run: pip install pandas numpy beautifulsoup4 openpyxl python-dateutil pandas_market_calendars")
    sys.exit(1)

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

try:
    from favorites_report_builder import generate_favorites_tile_report
except ImportError:
    # print("Warning: favorites_report_builder.py not found. Favorites tile report will be skipped.")
    def generate_favorites_tile_report(*args, **kwargs): pass

# -------------------- CONFIG --------------------
SCRIPT_VERSION = "vFinal30-InboxFixed-Repaired"

# --- EMAIL / INBOX CONFIG ---
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
SENDER_EMAIL = "stockusals@gmail.com"
INBOX_LOOKBACK_DAYS = 30

# --- PRICE TREND CONFIG ---
PRICE_TREND_DAYS = [2, 3, 5, 7, 9, 11, 15, 30, 60, 90, 180, 360]

# --- FILE PATHS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_OUTPUT_DIR = os.path.join(BASE_DIR, "docs")
os.makedirs(MASTER_OUTPUT_DIR, exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M")

OUT_HTML_INBOX  = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Inbox.html")
OUT_HTML_UNIV   = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Universal.html")
OUT_HTML_WATCH  = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Watchlist.html") 
OUT_HTML_SECTOR = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Sector.html")
OUT_HTML_FAV    = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Favorites_Tile.html") 
OUT_CSV         = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Flagged.csv")
OUT_TXT         = os.path.join(MASTER_OUTPUT_DIR, "TopBottom_Summary.txt")

CACHE_DIR = os.path.join(BASE_DIR, "tb_cache")
CHARTS_DIR = os.path.join(BASE_DIR, "charts")

WATCHLIST_FILE = os.path.join(BASE_DIR, "watchlist.xlsx") 
FAVORITES_FILE = os.path.join(BASE_DIR, "favorites.xlsx") 

DOWNLOADS_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")
USE_WATCHLIST_EXCEL = True
ENABLE_CSV_EXPORT = False
ENABLE_Sector = False

# --- SIZE OPTIMIZATION SETTINGS ---
UNIVERSE_LIMIT = 2000       
MAX_HISTORY_DAILY = 252       
MAX_HISTORY_INTRADAY = 150    
EAGER_RENDER_FIRST_N = 18

THREADS = 25
REQUEST_TIMEOUT = 20
LOCAL_PLOTLY_FILE = "plotly-latest.min.js"
XLSX_JS_FILE = "xlsx.full.min.js" 

INTRADAY_INTERVAL = "5m"
INTRADAY_DAYS = 5             
DAILY_LOOKBACK_DAYS = 300     
WEEKLY_LOOKBACK_DAYS = 365 
RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0

AUTO_REFRESH_MINUTES_DEFAULT = 10
CHART_HEIGHT = 450
TABLE_ROWS_DAILY = 30
TABLE_ROWS_INTRADAY = 30

finviz_lock = threading.Lock()

log_buffer = StringIO()
logger = logging.getLogger("TopBottom_v30")
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)
logger.addHandler(logging.StreamHandler(log_buffer))

# -------------------- Global State --------------------
inbox_tickers_extra_data = {} 
# --- EMBEDDED JAVASCRIPT TEMPLATE ---
report_js_template = """
// =========================================================================
// report_script.js — Dynamic Counts + Filters + Charts
// =========================================================================

var activeFilters = new Set();

function applyFilters() {
    const cards = document.querySelectorAll('.signal_card');
    let visibleCount = 0;

    cards.forEach(card => {
        const cardTags = (card.dataset.tags || "").split(',').map(t => t.trim());
        let show = true;

        if (activeFilters.size > 0) {
            // Check if card has ALL selected filter tags
            show = Array.from(activeFilters).every(f => cardTags.includes(f));
        }

        card.style.display = show ? "block" : "none";
        if (show) visibleCount++;
    });

    document.getElementById('visible-count').innerText = visibleCount;
    updateDynamicCounts();
}

function updateDynamicCounts() {
    const cards = Array.from(document.querySelectorAll('.signal_card'));
    const allPossibleFilters = document.querySelectorAll('.filter-btn');

    allPossibleFilters.forEach(btn => {
        const filter = btn.dataset.filter;
        if (filter === "ALL") return;

        const count = cards.filter(card => {
            const tags = (card.dataset.tags || "").split(',').map(t => t.trim());
            return tags.includes(filter);
        }).length;

        const countSpan = btn.querySelector('span');
        if (countSpan) countSpan.innerText = count;
    });
}

function toggleFilter(btn) {
    const filter = btn.dataset.filter;
    if (filter === "ALL") {
        activeFilters.clear();
        document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
    } else {
        document.querySelector('.filter-btn[data-filter="ALL"]').classList.remove('active');
        if (activeFilters.has(filter)) {
            activeFilters.delete(filter);
            btn.classList.remove('active');
        } else {
            activeFilters.add(filter);
            btn.classList.add('active');
        }
    }
    
    if (activeFilters.size === 0) {
        document.querySelector('.filter-btn[data-filter="ALL"]').click();
    }
    applyFilters();
}

document.addEventListener('DOMContentLoaded', () => {
    updateDynamicCounts();
});
"""
# -------------------- Inbox Automation --------------------
def insert_or_update_inbox(ticker, price, date_str, source, filename):
    inbox_tickers_extra_data[ticker] = {
        'price': price,
        'date': date_str,
        'source': f"{source} ({filename})"
    }

def parse_inbox():
    logger.info(f"--- AUTOMATION: Checking Inbox (Lookback: {INBOX_LOOKBACK_DAYS} days) ---")
    results = {}
    
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD: 
        logger.warning("EMAIL_ADDRESS or EMAIL_PASSWORD missing. Skipping inbox check.")
        return results

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        mail.select("inbox")
        
        dt = (datetime.now() - timedelta(days=INBOX_LOOKBACK_DAYS)).strftime("%d-%b-%Y")
        search_criteria = f'(SINCE "{dt}" FROM "{SENDER_EMAIL}")'
        _, ids = mail.search(None, search_criteria)
        
        if not ids[0]:
            logger.info(f"No emails found from {SENDER_EMAIL} since {dt}")
            mail.logout()
            return results

        for uid in ids[0].split():
            _, data = mail.fetch(uid, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])
            
            try: d_str = parser.parse(msg.get("date")).strftime("%Y-%m-%d")
            except: d_str = datetime.now().strftime("%Y-%m-%d")
            
            for part in msg.walk():
                fname = part.get_filename()
                if fname and ("csv" in fname.lower() or "xls" in fname.lower()):
                    logger.info(f"Processing attachment: {fname}")
                    try:
                        payload = part.get_payload(decode=True)
                        if "csv" in fname.lower():
                            df = pd.read_csv(io.BytesIO(payload))
                        else:
                            df = pd.read_excel(io.BytesIO(payload))
                        
                        df.columns = [str(c).lower().strip() for c in df.columns]
                        
                        t_col = next((c for c in df.columns if "ticker" in c or "symbol" in c), None)
                        p_col = next((c for c in df.columns if "price" in c or "current" in c), None)
                        
                        if t_col is None: continue

                        for _, r in df.iterrows():
                            t = str(r[t_col]).strip().upper()
                            if not t or len(t) > 8 or t == 'NAN': continue
                            
                            # Clean price input
                            try:
                                raw_p = str(r[p_col]) if p_col else "0"
                                clean_p = raw_p.replace('$', '').replace(',', '').strip()
                                p = float(clean_p)
                            except: p = 0.0

                            insert_or_update_inbox(t, p, d_str, "InboxAttachment", fname)
                            results[t] = {'price': p, 'date': d_str}
                            
                    except Exception as e:
                        logger.error(f"Error reading attachment {fname}: {e}")
        mail.logout()
    except Exception as e:
        logger.error(f"Inbox connection/parsing failed: {e}")
    return results

# -------------------- Automation Helpers --------------------
def auto_import_favorites_from_downloads():
    logger.info("--- AUTOMATION: Checking Downloads folder for new favorites... ---")
    if not os.path.exists(DOWNLOADS_FOLDER):
        logger.warning(f"Could not find Downloads folder at: {DOWNLOADS_FOLDER}")
        return
    pattern = os.path.join(DOWNLOADS_FOLDER, "favorites*.xlsx")
    candidates = glob.glob(pattern)
    if not candidates:
        logger.info("No new 'favorites.xlsx' found in Downloads.")
        return
    try:
        newest_file = max(candidates, key=os.path.getmtime)
        logger.info(f"Found new favorites file: {newest_file}")
        os.makedirs(os.path.dirname(FAVORITES_FILE), exist_ok=True)
        try:
            if os.path.exists(FAVORITES_FILE):
                os.remove(FAVORITES_FILE)
            shutil.move(newest_file, FAVORITES_FILE)
            logger.info(f"SUCCESS: Imported and overwrote {FAVORITES_FILE}")
        except Exception as e:
            logging.error(f"Failed to move file: {e}")
    except Exception as e:
        logging.error(f"Error during auto-import: {e}")

# -------------------- Trend Helper --------------------
def generate_trend_html(df_hist):
    if df_hist is None or df_hist.empty: return ""
    html = """
    <style>
        .trend-container { display: flex; flex-wrap: wrap; gap: 5px; margin: 8px 0 12px 0; align-items: center; }
        .trend-box { 
            font-size: 10px; font-weight: 700; color: white; 
            padding: 3px 6px; border-radius: 4px; text-align: center; min-width: 35px;
            font-family: sans-serif; line-height: 1.2; box-shadow: 0 1px 2px rgba(0,0,0,0.1);
        }
        .trend-up { background-color: #10b981; border: 1px solid #059669; }
        .trend-down { background-color: #ef4444; border: 1px solid #dc2626; }
        .trend-flat { background-color: #6b7280; border: 1px solid #4b5563; }
    </style>
    <div class='trend-container'><span style="font-size:11px; color:#888; margin-right:4px;">Trend:</span>
    """
    current_price = df_hist['Close'].iloc[-1]
    for days in PRICE_TREND_DAYS:
        if len(df_hist) > days:
            past_price = df_hist['Close'].iloc[-(days + 1)]
            if past_price == 0 or pd.isna(past_price): continue 
            change_pct = ((current_price - past_price) / past_price) * 100
            if change_pct > 0: css_class, sign = "trend-up", "+"
            elif change_pct < 0: css_class, sign = "trend-down", ""
            else: css_class, sign = "trend-flat", ""
            html += f"<div class='trend-box {css_class}' title='{days} Days Ago: ${past_price:.2f}'>{days}D<br>{sign}{change_pct:.1f}%</div>"
    html += "</div>"
    return html

# -------------------- Helpers --------------------
def money(v:Optional[float]) -> str:
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))): return "n/a"
        return f"${float(v):.2f}"
    except: return "n/a"

def cache_path(name:str)->str:
    return os.path.join(CACHE_DIR, name)

def is_cache_fresh(path:str, hours:int=12)->bool:
    if not os.path.exists(path): return False
    try:
        mtime = os.path.getmtime(path)
        return (time.time() - mtime) < hours * 3600
    except Exception: return False

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
def fetch_sp500() -> List[str]:
    combined_tickers = set()
    headers = {"User-Agent": "Mozilla/5.0"}
    
    print("Starting Finviz fetch...")
    finviz_tickers = []
    target_count = 400  
    current_offset = 1  
    base_finviz_url = "https://finviz.com/screener.ashx?v=111&f=geo_usa,sh_price_o10,sh_avgvol_o800,ta_sma200_a,ta_sma50_below&o=-marketcap"

    try:
        while len(finviz_tickers) < target_count:
            page_url = f"{base_finviz_url}&r={current_offset}"
            req = urllib.request.Request(page_url, headers=headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                html = resp.read().decode("utf-8", "ignore")
            
            found = False
            for df in pd.read_html(StringIO(html)):
                cols = [str(c).lower() for c in df.columns]
                if "ticker" in cols and len(df) > 10:
                    raw = df[df.columns[cols.index("ticker")]].astype(str).str.strip().str.upper().tolist()
                    valid = [t for t in raw if t != "TICKER" and 1 <= len(t) <= 6 and t.isalpha()]
                    if valid:
                        finviz_tickers.extend(valid)
                        found = True
                        print(f"  Finviz Offset {current_offset}: Found {len(valid)} tickers.")
                    break 
            if not found: break
            current_offset += 20
            time.sleep(1.5) 
        combined_tickers.update(finviz_tickers[:target_count])
    except Exception as e:
        print(f"Finviz fetch failed: {e}")

    try:
        print("Starting Wikipedia fetch...")
        wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        req = urllib.request.Request(wiki_url, headers=headers)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html = resp.read().decode("utf-8", "ignore")
        for df in pd.read_html(StringIO(html)):
            cols = [str(c).lower() for c in df.columns]
            if "symbol" in cols:
                tickers = df[df.columns[cols.index("symbol")]].astype(str).str.replace(".", "-", regex=False).str.strip().str.upper().tolist()
                combined_tickers.update(tickers)
                break
    except Exception as e:
        print(f"Wikipedia fetch failed: {e}")

    if not combined_tickers:
        return ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]
    return sorted(list(combined_tickers))

def fetch_nasdaq100() -> List[str]:
    combined_tickers = set()
    print("Starting Finviz Nasdaq fetch...")
    finviz_tickers = []
    current_offset = 1 
    base_finviz_url = "https://finviz.com/screener.ashx?v=111&f=geo_usa,exch_nasd,sh_price_o10,sh_avgvol_o500,ta_sma200_a,ta_sma50_below&o=-marketcap"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        while len(finviz_tickers) < 300:
            page_url = f"{base_finviz_url}&r={current_offset}"
            req = urllib.request.Request(page_url, headers=headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                html = resp.read().decode("utf-8", "ignore")
            found = False
            for df in pd.read_html(StringIO(html)):
                cols = [str(c).lower() for c in df.columns]
                if "ticker" in cols and len(df) > 10:
                    raw = df[df.columns[cols.index("ticker")]].astype(str).str.replace(".", "-", regex=False).str.strip().str.upper().tolist()
                    valid = [t for t in raw if t != "TICKER" and 1 <= len(t) <= 6 and t.isalpha()]
                    if valid:
                        finviz_tickers.extend(valid)
                        found = True
                        print(f"  Offset {current_offset}: Found {len(valid)} records.")
                    break
            if not found: break
            current_offset += 20
            time.sleep(1.5)
        combined_tickers.update(finviz_tickers)
    except Exception as e:
        print(f"Finviz fetch failed: {e}")

    try:
        wiki_url = "https://en.wikipedia.org/wiki/NASDAQ-100"
        req = urllib.request.Request(wiki_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html = resp.read().decode("utf-8", "ignore")
        for df in pd.read_html(StringIO(html)):
            for c in df.columns:
                if str(c).lower() in ("ticker", "symbol"):
                    tickers = df[c].astype(str).str.replace(".", "-", regex=False).str.strip().str.upper().tolist()
                    combined_tickers.update(tickers)
                    break
    except Exception as e:
        print(f"Wikipedia fetch failed: {e}")

    if not combined_tickers:
        return ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA"]
    return sorted(list(combined_tickers))

def fetch_core_etfs() -> List[str]:
    # 1. Hardcoded Core ETFs
    core_etfs = [
        "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "GLD", "SLV", 
        "USO", "UNG", "TLT", "AGG", "VNQ", "XLF", "XLK", "XLE", 
        "XLY", "XLV"
    ]
    
    # 2. Scrape "Most Active" (with pagination to get all records)
    active_tickers = []
    base_url = "https://finance.yahoo.com/markets/stocks/most-active/"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    offset = 0
    count = 100  # Yahoo allows fetching 100 rows per page
    
    try:
        while True:
            # Query with offset and count to get all pages (0-100, 100-200, etc.)
            params = {"start": offset, "count": count}
            
            response = requests.get(base_url, headers=headers, params=params, timeout=10)
            
            if response.status_code != 200:
                break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            rows = soup.select('table tbody tr')
            
            # If no rows returned, we are done
            if not rows:
                break
            
            page_tickers = []
            for row in rows:
                # Symbol is usually the first link or span in the row
                symbol_tag = row.select_one('td span.symbol, td a')
                if symbol_tag:
                    ticker = symbol_tag.text.strip()
                    # Filter out garbage data or long strings
                    if ticker and ticker.isalpha():
                        page_tickers.append(ticker)
            
            if not page_tickers:
                break
                
            active_tickers.extend(page_tickers)
            
            # If we got fewer records than the 'count', we are on the last page
            if len(page_tickers) < count:
                break
                
            offset += count
            time.sleep(0.5) # Slight pause to avoid rate limits

    except Exception:
        # If scraping fails entirely, pass silently and return just the core list
        pass

    # 3. Combine, deduplicate, and sort
    final_list = list(set(core_etfs + active_tickers))
    final_list.sort()
    
    return final_list

def fetch_leverage_etfs()->List[str]:
    return sorted(list({"TQQQ","SQQQ","SPXL","SPXS","UPRO","SPXU","SOXL","SOXS","FNGU","FNGD","TNA","TZA","SSO","SDS","UDOW","SDOW","TMF","TMV","LABU","LABD","TECL","TECS","DDM","DUST"}))

def fetch_global_etfs()->List[str]:
    return ["EFA","EWJ","EWZ","FXI","EEM","VEA","VWO","IEFA","VXUS","ACWI","VTI","VGK","BNDW","INDA","MCHI","IEMG"]

def fetch_crypto()->List[str]:
    return ["BTC-USD","ETH-USD","BNB-USD","SOL-USD","ADA-USD","XRP-USD","DOGE-USD"]

def fetch_commodities()->List[str]:
    return ["GC=F","SI=F","CL=F","NG=F","GLD","SLV","USO","UNG"]

def build_universe(limit:int=UNIVERSE_LIMIT)->Dict[str,List[str]]:
    cache_file = cache_path("univ_v30_deduped.json")
    if is_cache_fresh(cache_file, 24):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                logger.info("Loading universe from cache...")
                return json.load(f)
        except Exception: pass
    
    logger.info("Building fresh universe...")
    seen = set()
    data = {}
    
    sp = fetch_sp500(); nq = fetch_nasdaq100()
    stocks = unique_tickers(sp + nq)
    if not stocks: stocks = ["AAPL","MSFT","NVDA","AMZN","GOOGL"]
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
    except Exception: pass
    return data

# -------------------- Watchlist Excel loader --------------------
def load_watchlist_from_excel(path:Optional[str]=None):
    if not path or not os.path.exists(path):
        return None, None
    try:
        xls = pd.ExcelFile(path)
        out_map = {}; out_data = {} 
        for sheet in xls.sheet_names:
            try:
                df = xls.parse(sheet)
                if df is not None and not df.empty:
                    df.columns = df.columns.astype(str).str.strip()
                if df is None or df.empty or 'Ticker' not in df.columns: continue
                
                sheet_tickers = []
                for _, row in df.iterrows():
                    ticker = str(row['Ticker']).strip().upper()
                    if not ticker: continue
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
                    except:
                        if ticker not in out_data: out_data[ticker] = {'price': None, 'date': None}
                if sheet_tickers:
                    out_map[sheet.strip()] = unique_tickers(sheet_tickers)
            except: pass
        return (out_map or None), (out_data or None)
    except Exception: return None, None

# -------------------- Data fetchers --------------------
def fetch_chart_yahoo_json(ticker:str, interval:str="1d", days:int=365)->Optional[pd.DataFrame]:
    try:
        range_str = f"{max(1, days)}d"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ticker)}?range={range_str}&interval={interval}"
        req = urllib.request.Request(url, headers={"User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90,120)}.0.0.0 Safari/537.36"})
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
            with open(cache_file, "r") as f:
                return json.load(f)
        except: pass
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}?modules=assetProfile,calendarEvents,defaultKeyStatistics,price"
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
        # v2.0: Extract market cap from API response
        mcap_raw = None
        try:
            dks = result.get('defaultKeyStatistics', {})
            price_mod = result.get('price', {})
            mcap_raw = price_mod.get('marketCap', {}).get('raw') or dks.get('enterpriseValue', {}).get('raw')
        except: pass
        data = {'sector': sector, 'earningsDate': earnings_date_iso, 'marketCap': mcap_raw}
        with open(cache_file, "w") as f: json.dump(data, f)
        return data
    except Exception:
        return {'sector': None, 'earningsDate': None}
def calculate_ttm_scalper(df):
    """
    Identifies TTM Scalper Buy signals (Pivot Low followed by reversal).
    """
    if df is None or len(df) < 5:
        return df
    
    # Create shifted columns to compare previous candles
    # Low_1 means "Low of 1 candle ago"
    df['Low_1'] = df['Low'].shift(1)
    df['Low_2'] = df['Low'].shift(2)
    df['High_1'] = df['High'].shift(1)
    
    # Scalper Buy Logic:
    # 1. The low 2 days ago was lower than the low 1 day ago? (Wait, strict TTM is 3 lower lows)
    # Let's use a robust "Pivot Reversal" logic which is safer:
    # "Price made a low, failed to go lower, and broke the previous high"
    
    # Logic: Current Low is higher than Previous Low, AND Current Close > Previous High
    # This signifies the momentum has officially shifted UP.
    df['scalper_buy'] = (
        (df['Low'] > df['Low_1']) &       # We stopped making lower lows
        (df['Close'] > df['High_1']) &    # We broke the prior candle's ceiling
        (df['Close'] > df['Open'])        # It's a green candle
    )
    
    return df
def fetch_earnings_date_finviz(ticker: str) -> Optional[datetime]:
    with finviz_lock:
        time.sleep(0.5 + random.random() * 0.5) 
        try:
            url = f"https://finviz.com/quote.ashx?t={urllib.parse.quote(ticker)}"
            headers = {"User-Agent": "Mozilla/5.0"}
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                html = resp.read().decode("utf-8","ignore")
            soup = BeautifulSoup(html, 'html.parser')
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

def market_cap_bucket(mcap):
    try:
        if mcap is None: return None
        if mcap >= 10_000_000_000: return "LARGE_CAP"
        if mcap >= 2_000_000_000: return "MID_CAP"
        if mcap >= 300_000_000: return "SMALL_CAP"
        return "MICRO_CAP"
    except Exception:
        return None
import re
import requests

def fetch_market_cap_alt(ticker: str):
    """
    Zero-API fallback market cap fetch using Finviz HTML.
    Returns market cap in dollars or None.
    """
    try:
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=6)
        if r.status_code != 200:
            return None

        m = re.search(r'Market Cap</td><td.*?>(.*?)</td>', r.text)
        if not m:
            return None

        val = m.group(1).strip().upper()
        mult = {"T":1e12,"B":1e9,"M":1e6,"K":1e3}
        return float(val[:-1]) * mult.get(val[-1], 1)
    except Exception:
        return None
    

import os, requests
from datetime import datetime, timedelta, timezone
import pandas as pd

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

def fetch_market_cap_fallback(ticker):
    try:
        if FINNHUB_API_KEY:
            r = requests.get(
                f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={FINNHUB_API_KEY}",
                timeout=4
            )
            if r.status_code == 200:
                j = r.json()
                if j.get("marketCapitalization"):
                    return float(j["marketCapitalization"]) * 1_000_000
    except:
        pass

    try:
        if POLYGON_API_KEY:
            r = requests.get(
                f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}",
                timeout=4
            )
            if r.status_code == 200:
                j = r.json()
                if j.get("results", {}).get("market_cap"):
                    return float(j["results"]["market_cap"])
    except:
        pass

    return None
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()

    # -------------------------
    # CLEANING
    # -------------------------
    for c in ['Open', 'High', 'Low', 'Close', 'Volume']:
        d[c] = pd.to_numeric(d.get(c), errors='coerce')

    d['Date'] = pd.to_datetime(d['Date'], errors='coerce')
    d = d.dropna(subset=['Date', 'Close']).sort_values('Date').reset_index(drop=True)
    if d.empty:
        return pd.DataFrame()

    # -------------------------
    # MOVING AVERAGES
    # -------------------------
    d['ema5'] = d['Close'].ewm(span=5, adjust=False).mean()
    d['ema9'] = d['Close'].ewm(span=9, adjust=False).mean()
    d['ema10'] = d['Close'].ewm(span=10, adjust=False).mean()
    d['ema20'] = d['Close'].ewm(span=20, adjust=False).mean()
    d['ema21'] = d['Close'].ewm(span=21, adjust=False).mean()  # legacy compat
    d['ema50'] = d['Close'].ewm(span=50, adjust=False).mean()
    d['ema200'] = d['Close'].ewm(span=200, adjust=False).mean()
    # --- Moving Averages ---
    d['ma5']  = d['Close'].rolling(5, min_periods=1).mean()
    d['ma10'] = d['Close'].rolling(10, min_periods=1).mean()
    d['ma20'] = d['Close'].rolling(20, min_periods=1).mean()
    d['ma30'] = d['Close'].rolling(30, min_periods=1).mean()
    
    # --- VWAP ---
    tp = (d['High'] + d['Low'] + d['Close']) / 3
    d['VWAP'] = (tp * d['Volume']).cumsum() / d['Volume'].replace(0, np.nan).cumsum()

    # --- SAR (Parabolic SAR) ---
    

    # -------------------------
    # RSI
    # -------------------------
    delta = d['Close'].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_up = up.rolling(RSI_PERIOD, min_periods=1).mean()
    avg_down = down.rolling(RSI_PERIOD, min_periods=1).mean()
    rs = avg_up / avg_down.replace(0, np.nan)
    d['RSI'] = 100 - (100 / (1 + rs))
    d['RSI'] = d['RSI'].fillna(50.0)

    # -------------------------
    # ATR
    # -------------------------
    prev = d['Close'].shift(1)
    tr = pd.concat([
        (d['High'] - d['Low']).abs(),
        (d['High'] - prev).abs(),
        (d['Low'] - prev).abs()
    ], axis=1).max(axis=1)
    d['ATR'] = tr.rolling(14, min_periods=1).mean()

    # -------------------------
    # BOLLINGER BANDS (FULL + SAFE)
    # -------------------------
    d['BB_mid'] = d['Close'].rolling(BB_PERIOD, min_periods=1).mean()
    d['BB_std'] = d['Close'].rolling(BB_PERIOD, min_periods=1).std(ddof=0).fillna(0)
    d['BB_upper'] = d['BB_mid'] + BB_STD * d['BB_std']
    d['BB_lower'] = d['BB_mid'] - BB_STD * d['BB_std']

    # -------------------------
    # TREND SLOPE (FOR AI SCORE LINE COLORING)
    # -------------------------
    d['slope_5'] = d['Close'].diff(5)
    d['slope_10'] = d['Close'].diff(10)
    d['trend_strength'] = (d['ema5'] - d['ema20']) / d['Close'] * 100

    # -------------------------
    # MA RELATION FLAGS (FILTER LOGIC)
    # -------------------------
    d['ma5_gt_ma10'] = d['ema5'] > d['ema10']
    d['ma5_lt_ma10'] = d['ema5'] < d['ema10']
    d['ma10_gt_ma20'] = d['ema10'] > d['ema20']
    d['ma10_lt_ma20'] = d['ema10'] < d['ema20']

    return d


def analyze_ticker(ticker: str, entry_data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    try:
        tags = []
        trade_details = {}
        now_utc = datetime.now(timezone.utc)
        recent_thresh_daily = now_utc - timedelta(days=4)
        recent_thresh_weekly = now_utc - timedelta(days=14)

        # -------------------------------------------------------
        # SAFE COLUMN HELPER
        # -------------------------------------------------------
        def col(df, *names):
            for n in names:
                if n in df.columns:
                    return df[n]
            raise KeyError(names)

        # -------------------------------------------------------
        # SIGNAL MODE
        # -------------------------------------------------------
        SIGNAL_MODE = "NORMAL"
        if SIGNAL_MODE == "TIGHT":
            RSI_BUY = 58
            RSI_SELL = 42
            EMA_BUFFER = 0.006
        elif SIGNAL_MODE == "LOOSE":
            RSI_BUY = 45
            RSI_SELL = 38
            EMA_BUFFER = -0.002
        else:
            RSI_BUY = 52
            RSI_SELL = 45
            EMA_BUFFER = 0.001

        # -------------------------------------------------------
        # METADATA / MARKET CAP / EARNINGS
        # -------------------------------------------------------
        meta = fetch_metadata(ticker) or {}
        sector = meta.get("sector")
        mcap = safe_float(meta.get("marketCap"))
        if not mcap or mcap <= 0:
            try:
                import yfinance as yf
                _tk_info = yf.Ticker(ticker).info
                mcap = safe_float(_tk_info.get("marketCap"))
            except: pass
        if not mcap or mcap <= 0:
            mcap = fetch_market_cap_fallback(ticker)

        mcap_bucket = None
        if mcap and mcap > 0:
            if mcap >= 10_000_000_000:
                mcap_bucket = "LARGE_CAP"
                tags += ["LARGE_CAP", "Large Cap"]
            elif mcap >= 2_000_000_000:
                mcap_bucket = "MID_CAP"
                tags += ["MID_CAP", "Mid Cap"]
            else:
                mcap_bucket = "SMALL_CAP"
                tags += ["SMALL_CAP", "Small Cap"]

        # ------------------ EARNINGS DATE ------------------
        earnings_date_dt = None
        raw_ed = meta.get("earningsDate") or meta.get("earningsTimestamp")

        try:
            if isinstance(raw_ed, (list, tuple)) and raw_ed:
                raw_ed = raw_ed[0]
            if isinstance(raw_ed, (int, float)):
                earnings_date_dt = datetime.fromtimestamp(raw_ed, tz=timezone.utc)
            elif isinstance(raw_ed, str):
                earnings_date_dt = datetime.fromisoformat(raw_ed.replace("Z", "+00:00"))
            elif isinstance(raw_ed, datetime):
                earnings_date_dt = raw_ed

            if earnings_date_dt and earnings_date_dt.tzinfo is None:
                earnings_date_dt = earnings_date_dt.replace(tzinfo=timezone.utc)
            elif earnings_date_dt:
                earnings_date_dt = earnings_date_dt.astimezone(timezone.utc)
        except:
            earnings_date_dt = None

        if earnings_date_dt is None:
            # v2.0: Try yfinance for earnings date first
            try:
                import yfinance as yf
                _tk_cal = yf.Ticker(ticker).calendar
                if _tk_cal is not None:
                    _ed_val = None
                    if isinstance(_tk_cal, dict):
                        _ed_val = _tk_cal.get('Earnings Date')
                    elif hasattr(_tk_cal, 'columns'):
                        try: _ed_val = _tk_cal.iloc[0, 0] if not _tk_cal.empty else None
                        except: pass
                    if _ed_val is not None:
                        if isinstance(_ed_val, (list, tuple)) and _ed_val:
                            earnings_date_dt = pd.Timestamp(_ed_val[0]).to_pydatetime()
                        elif hasattr(_ed_val, 'year'):
                            earnings_date_dt = pd.Timestamp(_ed_val).to_pydatetime()
                        if earnings_date_dt and earnings_date_dt.tzinfo is None:
                            earnings_date_dt = earnings_date_dt.replace(tzinfo=timezone.utc)
            except:
                pass
            # Finviz fallback
            if earnings_date_dt is None:
                try:
                    earnings_date_dt = fetch_earnings_date_finviz(ticker)
                    if earnings_date_dt and earnings_date_dt.tzinfo is None:
                        earnings_date_dt = earnings_date_dt.replace(tzinfo=timezone.utc)
                except:
                    earnings_date_dt = None

        if earnings_date_dt:
            try:
                days_diff = (earnings_date_dt - now_utc).total_seconds() / 86400
                if 0 <= days_diff <= 15:
                    tags += ["UPCOMING_EARNINGS", "UPCOMING_E", "Upcoming E (15d)"]
                elif -15 <= days_diff < 0:
                    tags += ["POST_EARNINGS", "POST_E", "Post E (15d)"]
            except:
                pass

        # -------------------------------------------------------
        # DAILY DATA
        # -------------------------------------------------------
        dp = cache_path(f"{ticker}_daily.csv")
        daily = None
        if is_cache_fresh(dp, 1):
            try:
                daily = pd.read_csv(dp, parse_dates=["Date"])
            except:
                daily = None

        if daily is None or daily.empty:
            daily = fetch_chart_yahoo_json(ticker, interval="1d", days=DAILY_LOOKBACK_DAYS)
            if daily is None or daily.empty:
                return None
            try:
                daily.to_csv(dp, index=False)
            except:
                pass

        daily["Date"] = pd.to_datetime(daily["Date"], utc=True)
        if len(daily) < 40:
            return None

        daily_ind = compute_indicators(daily)
        daily_closes = daily_ind["Close"].tolist()
        last_close = daily_closes[-1]

        # -------------------------------------------------------
        # AI SCORE
        # -------------------------------------------------------
        buy_score = calculate_ai_score(daily_ind)
        if buy_score >= 60:
            tags += ["BUYING_OPPORTUNITY", "Buying Opportunity"]

        # -------------------------------------------------------
        # DAILY SIGNALS
        # -------------------------------------------------------
        daily_signal = False
        daily_bear_signal = False
        
        # 1. Run the Scalper Calculation on our data
        daily_ind = calculate_ttm_scalper(daily_ind)

        try:
            # Get latest values
            close = daily_ind["Close"].iloc[-1]
            prev_close = daily_ind["Close"].iloc[-2]
            ema20 = col(daily_ind, "EMA20", "ema20").iloc[-1]
            ema50 = col(daily_ind, "EMA50", "ema50").iloc[-1]
            ema200 = col(daily_ind, "EMA200", "ema200").iloc[-1]
            rsi = col(daily_ind, "RSI", "rsi").iloc[-1]
            
            # Check for Scalper Signal (Today or Yesterday)
            scalper_fire = daily_ind['scalper_buy'].iloc[-1]
            
            # Basic Trend Defintions
            trend_up = ema20 > ema50 > ema200
            trend_down = ema20 < ema50 < ema200
            momentum_up = rsi >= RSI_BUY and close > prev_close
            momentum_down = rsi <= RSI_SELL and close < prev_close

            # --- ORIGINAL LOGIC ---
            if close > ema20 * (1 + EMA_BUFFER) and trend_up and momentum_up:
                daily_signal = True
                # We add specific descriptive tags, but these won't need new buttons
                tags += ["DAILY_STRONG_TREND"] if rsi >= 62 else ["DAILY_WEAK_TREND"]

            # --- NEW TTM SCALPER LOGIC (The "High Upside" Update) ---
            # If we have a Scalper Buy AND we are in a general uptrend (Above EMA50)
            if scalper_fire and close > ema50:
                # We FORCE daily_signal to True so it shows up in your existing filter
                daily_signal = True 
                
                # We also tag it as a "Buying Opportunity" automatically
                tags += ["SCALPER_REVERSAL", "Buying Opportunity"]
                
                # Boost the AI Score so it ranks higher
                buy_score += 20 

            # --- BEAR LOGIC ---
            if close < ema20 * (1 - EMA_BUFFER) and trend_down and momentum_down:
                daily_bear_signal = True
                tags += ["DAILY_BEAR_SIGNAL"]

            # Final Tag Assignment for UI
            if daily_signal:
                tags += ["DAILY_SIGNAL", "Daily Signal"]
                
        except Exception as e:
            # Safe failover if data is missing
            pass


        # -------------------------------------------------------
        # MONTHLY SIGNALS (REAL FIX — TAGS ALWAYS EMITTED)
        # -------------------------------------------------------
        monthly_signal = False
        monthly_bear_signal = False

        try:
            mp = cache_path(f"{ticker}_monthly.csv")
            monthly = None
            if is_cache_fresh(mp, 7):
                try:
                    monthly = pd.read_csv(mp, parse_dates=["Date"])
                except:
                    monthly = None

            if monthly is None or monthly.empty:
                monthly = fetch_chart_yahoo_json(ticker, interval="1mo", days=2000)
                if monthly is not None and not monthly.empty:
                    try:
                        monthly.to_csv(mp, index=False)
                    except:
                        pass

            if monthly is not None and len(monthly) >= 6:
                monthly["Date"] = pd.to_datetime(monthly["Date"], utc=True)
                monthly_ind = compute_indicators(monthly)

                close = monthly_ind["Close"].iloc[-1]
                prev_close = monthly_ind["Close"].iloc[-2]
                ema10 = col(monthly_ind, "EMA10", "ema10").iloc[-1]
                ema20 = col(monthly_ind, "EMA20", "ema20").iloc[-1]
                rsi = col(monthly_ind, "RSI", "rsi").iloc[-1]

                if close > ema10 and ema10 > ema20 and rsi >= 55 and close > prev_close:
                    monthly_signal = True
                    tags += ["MONTH_UP", "Month ↑"]

                elif close < ema10 and ema10 < ema20 and rsi <= 45 and close < prev_close:
                    monthly_bear_signal = True
                    tags += ["MONTH_DOWN", "Month ↓"]

                # ✅ REQUIRED FOR UI FILTER
                if "MONTH_UP" in tags or "MONTH_DOWN" in tags:
                    tags.append("MONTHLY_SIGNAL")

        except:
            pass

        # -------------------------------------------------------
        # HIGH / LOW FILTERS (UI MATCHED)
        # -------------------------------------------------------
        try:
            for n in (3, 5, 7, 9, 15, 20):
                if len(daily_ind) >= n:
                    if last_close >= daily_ind["High"].rolling(n).max().iloc[-1]:
                        tags += [f"{n}D High", f"{n}_DAY_HIGH"]
                    if last_close <= daily_ind["Low"].rolling(n).min().iloc[-1]:
                        tags += [f"{n}D Low", f"{n}_DAY_LOW"]
        except:
            pass

        # -------------------------------------------------------
        # MOVING AVERAGES
        # -------------------------------------------------------
        try:
            ma5 = safe_float(daily_ind.get("MA5", daily_ind.get("ma5")).iloc[-1])
            ma10 = safe_float(daily_ind.get("MA10", daily_ind.get("ma10")).iloc[-1])
            ma20 = safe_float(daily_ind.get("MA20", daily_ind.get("ma20")).iloc[-1])

            if ma5 and ma10:
                tags += ["MA5 > MA10", "MA5_GT_MA10"] if ma5 > ma10 else ["MA5 < MA10", "MA5_LT_MA10"]
            if ma10 and ma20:
                tags += ["MA10 > MA20", "MA10_GT_MA20"] if ma10 > ma20 else ["MA10 < MA20", "MA10_LT_MA20"]
        except:
            pass

        # -------------------------------------------------------
        # INBOX / ENTRY LOGIC (FIXED ENTRY +1M)
        # -------------------------------------------------------
        if entry_data:
            tags += ["INBOX", "Inbox"]
            try:
                raw_price = entry_data.get("price", 0)
                if isinstance(raw_price, str):
                    raw_price = raw_price.replace("$", "").replace(",", "").strip()
                entry_price = float(raw_price)

                raw_date = entry_data.get("date")
                entry_date = None
                try:
                    if isinstance(raw_date, str):
                        entry_date = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    elif isinstance(raw_date, datetime):
                        entry_date = raw_date
                    if entry_date and entry_date.tzinfo is None:
                        entry_date = entry_date.replace(tzinfo=timezone.utc)
                except:
                    entry_date = None

                if entry_price > 0:
                    pnl_pct = (last_close - entry_price) / entry_price
                    trade_details["entry_price"] = entry_price
                    trade_details["entry_date"] = raw_date
                    trade_details["pnl_pct"] = pnl_pct

                    if pnl_pct > 0:
                        tags += ["ENTRY_PLUS", "Entry +"]
                    elif pnl_pct < 0:
                        tags += ["ENTRY_MINUS", "Entry −"]

                    try:
                        if len(daily_closes) >= 6:
                            p1w = (daily_closes[-1] - daily_closes[-6]) / daily_closes[-6]
                            trade_details["pnl_1w"] = p1w
                            if p1w > 0:
                                tags += ["ENTRY_PLUS_1W", "Entry +1W"]

                        if len(daily_closes) >= 21:
                            p1m = (daily_closes[-1] - daily_closes[-21]) / daily_closes[-21]
                            trade_details["pnl_1m"] = p1m
                            if p1m > 0:
                                tags += ["ENTRY_PLUS_1M", "Entry +1M"]
                    except:
                        pass

                # ✅ DATE-BASED ENTRY +1M (UI FILTER NEEDS THIS)
               # ✅ IMPROVED DATE-BASED FILTERS
                if entry_date:
                    # Use a date-only comparison to avoid timezone/hour issues
                    now_date = now_utc.date()
                    item_date = entry_date.date()
                    diff_days = (now_date - item_date).days

                    if diff_days == 0:
                        tags.append("Today")
                    elif diff_days == 1:
                        tags.append("Yesterday")
                    
                    if 0 <= diff_days <= 3:
                        tags.append("Last 3 Days")
                    
                    if 0 <= diff_days <= 7:
                        tags.append("Last 7 Days")

                    # Keep your existing 1M logic if you want it
                    if diff_days >= 30:
                        tags.append("ENTRY_PLUS_1M")
            except:
                pass

        # -------------------------------------------------------
        # RETURNS / MONTH / QUARTER / YTD (FIXED TAGGING)
        # -------------------------------------------------------
        monthly_trend = False
        quarterly_trend = False

        if len(daily_closes) >= 2:
            def get_ret(days):
                if len(daily_closes) > days:
                    prev = daily_closes[-(days + 1)]
                    return (last_close - prev) / prev if prev else 0
                return 0

            m_ret = get_ret(21)
            q_ret = get_ret(63)

            if m_ret > 0.03:
                tags += ["MONTH_UP", "Month ↑"]
                monthly_trend = True
            elif m_ret < -0.03:
                tags += ["MONTH_DOWN", "Month ↓"]
                monthly_trend = True

            if q_ret > 0.10:
                tags += ["QUARTER_UP", "Quarter ↑"]
                quarterly_trend = True
            elif q_ret < -0.10:
                tags += ["QUARTER_DOWN", "Quarter ↓"]
                quarterly_trend = True

            # ---- unified signal tags (CRITICAL FIX) ----
            if "MONTH_UP" in tags or "MONTH_DOWN" in tags:
                tags += ["MONTHLY_SIGNAL", "Monthly Signal"]

            if "QUARTER_UP" in tags or "QUARTER_DOWN" in tags:
                tags += ["QUARTERLY_SIGNAL", "Quarterly Signal"]

            year_start = datetime(now_utc.year, 1, 1, tzinfo=timezone.utc)
            ytd_df = daily_ind[daily_ind["Date"] >= year_start]
            if not ytd_df.empty:
                ytd_start_price = ytd_df["Close"].iloc[0]
                ytd_ret = (last_close - ytd_start_price) / ytd_start_price
                tags += ["YTD ↑", "YTD_UP"] if ytd_ret > 0 else ["YTD ↓", "YTD_DOWN"]

            if len(daily_ind) >= 2:
                last_ema20 = col(daily_ind, "EMA20", "ema20").iloc[-1]
                prev_ema20 = col(daily_ind, "EMA20", "ema20").iloc[-2]
                prev_close = daily_closes[-2]
                if prev_close < prev_ema20 and last_close > last_ema20:
                    tags += ["CROSS_ABOVE_20EMA", "Cross > 20EMA"]
                if prev_close > prev_ema20 and last_close < last_ema20:
                    tags += ["CROSS_BELOW_20EMA", "Cross < 20EMA"]

            if ("EMA20" in daily_ind or "ema20" in daily_ind) and ("EMA50" in daily_ind or "ema50" in daily_ind):
                e20 = col(daily_ind, "EMA20", "ema20").iloc[-1]
                e50 = col(daily_ind, "EMA50", "ema50").iloc[-1]
                if last_close > e20 and e20 > e50:
                    tags += ["BUILDING_TREND", "Building Trend"]

            if len(daily_closes) > 20:
                lookback_len = min(len(daily_closes), 252)
                year_low = min(daily_closes[-lookback_len:])
                if year_low > 0:
                    dist_from_low = (last_close - year_low) / year_low
                    if 0 <= dist_from_low <= 0.05:
                        tags += ["BOUNCE_YEARLY_LOW", "Bounce Yearly Low"]

            last_rsi = col(daily_ind, "RSI", "rsi").iloc[-1]
            if last_rsi >= 70:
                tags += ["RSI_OVERBOUGHT", "RSI OB"]
            elif last_rsi <= 30:
                tags += ["RSI_OVERSOLD", "RSI OS"]

        daily_ext = find_local_extrema(daily_closes, lookback=14)

        # -------------------------------------------------------
        # INTRADAY DATA
        # -------------------------------------------------------
        ip = cache_path(f"{ticker}_intraday.csv")
        intr = None
        if is_cache_fresh(ip, 1):
            try:
                intr = pd.read_csv(ip, parse_dates=["Date"])
            except:
                intr = None

        if intr is None or intr.empty:
            intr = fetch_intraday(ticker, interval=INTRADAY_INTERVAL, days=INTRADAY_DAYS)
            if intr is not None and not intr.empty:
                try:
                    intr.to_csv(ip, index=False)
                except:
                    pass

        if intr is not None and not intr.empty:
            intr["Date"] = pd.to_datetime(intr["Date"], utc=True)

        intr_ind = compute_indicators(intr) if (intr is not None and not intr.empty) else pd.DataFrame()
        intr_closes = intr_ind["Close"].tolist() if not intr_ind.empty else []
        intr_ext = find_local_extrema(intr_closes, lookback=30)

        # -------------------------------------------------------
        # INTRADAY SIGNALS
        # -------------------------------------------------------
        intraday_signal = False
        intraday_bear_signal = False

        try:
            close = intr_ind["Close"].iloc[-1]
            ema9 = col(intr_ind, "EMA9", "ema9").iloc[-1]
            ema21 = col(intr_ind, "EMA21", "ema21").iloc[-1]
            rsi = col(intr_ind, "RSI", "rsi").iloc[-1]

            trend_up = ema9 > ema21
            trend_down = ema9 < ema21

            if close > ema9 * (1 + EMA_BUFFER) and trend_up and rsi >= RSI_BUY:
                intraday_signal = True
                tags += ["INTRADAY_SIGNAL", "Intraday Signal"]
                tags += ["INTRADAY_STRONG_TREND", "Intraday Strong Trend"] if rsi >= 60 else ["INTRADAY_WEAK_TREND", "Intraday Weak Trend"]

            if close < ema9 * (1 - EMA_BUFFER) and trend_down and rsi <= RSI_SELL:
                intraday_bear_signal = True
                tags += ["INTRADAY_BEAR_SIGNAL", "Intraday Bear Signal"]
        except:
            pass

        # -------------------------------------------------------
        # WEEKLY DATA
        # -------------------------------------------------------
        wp = cache_path(f"{ticker}_weekly.csv")
        weekly = None
        if is_cache_fresh(wp, 3):
            try:
                weekly = pd.read_csv(wp, parse_dates=["Date"])
            except:
                weekly = None

        if weekly is None or weekly.empty:
            weekly = fetch_weekly(ticker, days=WEEKLY_LOOKBACK_DAYS)
            if weekly is not None and not weekly.empty:
                try:
                    weekly.to_csv(wp, index=False)
                except:
                    pass

        if weekly is not None and not weekly.empty:
            weekly["Date"] = pd.to_datetime(weekly["Date"], utc=True)

        weekly_ind = compute_indicators(weekly) if (weekly is not None and not weekly.empty) else pd.DataFrame()
        weekly_closes = weekly_ind["Close"].tolist() if not weekly_ind.empty else []
        weekly_ext = find_local_extrema(weekly_closes, lookback=8)

        # -------------------------------------------------------
        # WEEKLY SIGNALS
        # -------------------------------------------------------
        weekly_signal = False
        weekly_bear_signal = False

        try:
            close = weekly_ind["Close"].iloc[-1]
            ema20 = col(weekly_ind, "EMA20", "ema20").iloc[-1]
            ema50 = col(weekly_ind, "EMA50", "ema50").iloc[-1]
            rsi = col(weekly_ind, "RSI", "rsi").iloc[-1]

            trend_up = ema20 > ema50
            trend_down = ema20 < ema50

            if close > ema20 * (1 + EMA_BUFFER) and trend_up and rsi >= RSI_BUY:
                weekly_signal = True
                tags += ["WEEKLY_SIGNAL", "Weekly Signal"]
                tags += ["WEEKLY_STRONG_TREND", "Weekly Strong Trend"] if rsi >= 60 else ["WEEKLY_WEAK_TREND", "Weekly Weak Trend"]

            if close < ema20 * (1 - EMA_BUFFER) and trend_down and rsi <= RSI_SELL:
                weekly_bear_signal = True
                tags += ["WEEKLY_BEAR_SIGNAL", "Weekly Bear Signal"]
        except:
            pass

        # -------------------------------------------------------
        # PULLBACK / REVERSAL / VOLATILITY
        # -------------------------------------------------------
        try:
            if daily_signal and col(daily_ind, "RSI", "rsi").iloc[-2] < 50 and col(daily_ind, "RSI", "rsi").iloc[-1] > 52:
                tags += ["PULLBACK_CONTINUATION", "Pullback Continuation"]
        except:
            pass

        try:
            rsi_prev = col(daily_ind, "RSI", "rsi").iloc[-2]
            rsi_now = col(daily_ind, "RSI", "rsi").iloc[-1]
            if rsi_prev < 30 and rsi_now > 35:
                tags += ["BULL_REVERSAL", "Bull Reversal"]
            if rsi_prev > 70 and rsi_now < 65:
                tags += ["BEAR_REVERSAL", "Bear Reversal"]
        except:
            pass

        try:
            atr = col(daily_ind, "ATR", "atr").iloc[-1]
            atr_prev = col(daily_ind, "ATR", "atr").iloc[-6]
            if atr > atr_prev * 1.35:
                tags += ["VOLATILITY_EXPANSION", "Volatility Expansion"]
        except:
            pass

        # -------------------------------------------------------
        # PATTERN RECOGNITION
        # -------------------------------------------------------
        if not intr_ind.empty:
            for p in reversed(intr_ext.get("peaks", [])):
                if p < 0 or p >= len(intr_ind):
                    continue
                if (len(intr_ind) - 1 - p) <= 2 and pivot_confirmable(intr_ind, p, "peak"):
                    tags += ["INTRADAY_TOP", "Intraday Top"]
                    break
            for p in reversed(intr_ext.get("troughs", [])):
                if p < 0 or p >= len(intr_ind):
                    continue
                if (len(intr_ind) - 1 - p) <= 2 and pivot_confirmable(intr_ind, p, "trough"):
                    tags += ["INTRADAY_BOTTOM", "Intraday Bottom"]
                    break

        for p in reversed(daily_ext.get("peaks", [])):
            if p < 0 or p >= len(daily_ind):
                continue
            dt = daily_ind["Date"].iloc[p]
            if dt >= recent_thresh_daily and (len(daily_ind) - 1 - p) <= 2 and pivot_confirmable(daily_ind, p, "peak"):
                tags += ["DAILY_TOP", "Daily Top"]
                break

        for p in reversed(daily_ext.get("troughs", [])):
            if p < 0 or p >= len(daily_ind):
                continue
            dt = daily_ind["Date"].iloc[p]
            if dt >= recent_thresh_daily and (len(daily_ind) - 1 - p) <= 2 and pivot_confirmable(daily_ind, p, "trough"):
                tags += ["DAILY_BOTTOM", "Daily Bottom"]
                break

        if len(daily_closes) >= 21:
            prev20_high = max(daily_closes[-21:-1])
            prev20_low = min(daily_closes[-21:-1])
            if last_close > prev20_high:
                tags += ["BREAKOUT_UP", "Breakout ↑"]
            if last_close < prev20_low:
                tags += ["BREAKOUT_DOWN", "Breakout ↓"]

        found_wk_top = False
        found_wk_bot = False
        if not weekly_ind.empty:
            for p in reversed(weekly_ext.get("peaks", [])):
                if weekly_ind["Date"].iloc[p] >= recent_thresh_weekly:
                    found_wk_top = True
                    break
            for p in reversed(weekly_ext.get("troughs", [])):
                if weekly_ind["Date"].iloc[p] >= recent_thresh_weekly:
                    found_wk_bot = True
                    break
            if found_wk_top:
                tags += ["RECENT_WEEKLY_TOP", "Recent W-Top"]
            if found_wk_bot:
                tags += ["RECENT_WEEKLY_BOTTOM", "Recent W-Bottom"]

        ("QUALITY_A_PLUS", "🏆 Quality A+"),
        ("QUALITY_A", "🏆 Quality A"),
        ("QUALITY_B", "🏆 Quality B"),

        ("OPTIONS_BULLISH", "📈 Opts Bullish"),
        ("OPTIONS_BEARISH", "📉 Opts Bearish"),
        ("UNUSUAL_OPTIONS", "⚡ Unusual Options"),

        intr_top = max([intr_closes[i] for i in intr_ext["peaks"]]) if intr_ext["peaks"] else None
        intr_bottom = min([intr_closes[i] for i in intr_ext["troughs"]]) if intr_ext["troughs"] else None

        # -------------------------------------------------------
        # FINAL BOOLEAN SYNC — FIXED
        # -------------------------------------------------------
        daily_signal = "DAILY_SIGNAL" in tags
        weekly_signal = "WEEKLY_SIGNAL" in tags
        intraday_signal = "INTRADAY_SIGNAL" in tags
        monthly_signal = "MONTHLY_SIGNAL" in tags
        quarterly_signal = "QUARTERLY_SIGNAL" in tags
        mtf_align = bool(daily_signal and weekly_signal and monthly_signal)

        if mtf_align:
            tags += ["MTF_ALIGN", "MTF Align"]

        # -------------------------------------------------------
        # SIGNAL CONFIDENCE
        # -------------------------------------------------------
        confidence = 0
        if daily_signal:
            confidence += 30
        if weekly_signal:
            confidence += 25
        if monthly_signal:
            confidence += 25
        if buy_score >= 70:
            confidence += 20
        confidence = min(100, confidence)

        if confidence >= 80:
            tags += ["HIGH_CONFIDENCE", "High Confidence"]
        elif confidence >= 60:
            tags += ["MEDIUM_CONFIDENCE", "Medium Confidence"]
        else:
            tags += ["LOW_CONFIDENCE", "Low Confidence"]

        # -------------------------------------------------------
        # CHART
        # -------------------------------------------------------
        try:
            chart_html = generate_light_interactive_chart(
                daily_ind,
                ticker,
                buy_score,
                extra_badges={
                    "AI": f"{int(buy_score)}",
                    "D": "YES" if daily_signal else "NO",
                    "W": "YES" if weekly_signal else "NO",
                    "I": "YES" if intraday_signal else "NO",
                    "M": "YES" if monthly_signal else "NO",
                },
            )
        except:
            chart_html = generate_light_interactive_chart(daily_ind, ticker, buy_score)

        # -------------------------------------------------------
        # RESULT
        # -------------------------------------------------------
        last_date_str = daily_ind["Date"].iloc[-1].strftime("%Y-%m-%d")

        result = {
            "ticker": ticker,
            "tags": sorted(set(tags)),
            "daily_df": daily_ind,
            "intraday_df": intr_ind,
            "daily_len": len(daily_ind),
            "intr_len": len(intr_ind),
            "daily_peaks": daily_ext["peaks"],
            "daily_troughs": daily_ext["troughs"],
            "intr_peaks": intr_ext["peaks"],
            "intr_troughs": intr_ext["troughs"],
            "intr_top": intr_top,
            "intr_bottom": intr_bottom,
            "earnings_date": earnings_date_dt,
            "trade_details": trade_details,
            "sector": sector,
            "market_cap": mcap,
            "market_cap_bucket": mcap_bucket,
            "last_close": last_close,
            "last_date": last_date_str,
            "buy_score": buy_score,
            "confidence": confidence,
            "daily_signal": daily_signal,
            "weekly_signal": weekly_signal,
            "intraday_signal": intraday_signal,
            "monthly_signal": monthly_signal,
            "quarterly_signal": quarterly_signal,
            "mtf_align": mtf_align,
            "chart_html": chart_html,
        }

        # EXPORT LAST INDICATORS
        result["rsi"] = float(col(daily_ind, "RSI", "rsi").iloc[-1]) if ("RSI" in daily_ind or "rsi" in daily_ind) else None
        result["ma5"] = float(daily_ind.get("MA5", daily_ind.get("ma5")).iloc[-1]) if ("MA5" in daily_ind or "ma5" in daily_ind) else None
        result["ma10"] = float(daily_ind.get("MA10", daily_ind.get("ma10")).iloc[-1]) if ("MA10" in daily_ind or "ma10" in daily_ind) else None
        result["ma20"] = float(daily_ind.get("MA20", daily_ind.get("ma20")).iloc[-1]) if ("MA20" in daily_ind or "ma20" in daily_ind) else None
        result["ma30"] = float(daily_ind.get("MA30", daily_ind.get("ma30")).iloc[-1]) if ("MA30" in daily_ind or "ma30" in daily_ind) else None

        if entry_data:
            result["entry_price"] = entry_data.get("price")
            result["entry_date"] = entry_data.get("date")

        """
            print(
                        result["ticker"],
                        result["tags"],
                        result["daily_signal"],
                        result["weekly_signal"],
                        result["intraday_signal"],
                        result["monthly_signal"],
                        result["quarterly_signal"],
                        result["mtf_align"],
                        result["confidence"],
                    )
     """
        return result

    except Exception as e:
        print("ERROR:", ticker, e)
        return None


def safe_float(x, default=None):
    try:
        if pd.isna(x): return default
        return float(x)
    except: return default
def detect_buying_opportunity(df: pd.DataFrame, timeframe: str) -> bool:
    if df is None or df.empty or len(df) < 50:
        return False

    last = df.iloc[-1]
    prev = df.iloc[-2]

    close = last["Close"]
    ema20 = last.get("EMA20") or last.get("ema20")
    ema50 = last.get("EMA50") or last.get("ema50")
    rsi = safe_float(last.get("RSI") or last.get("rsi"))
    atr = last.get("ATR")
    volume = last.get("Volume")
    vol_avg = df["Volume"].rolling(20).mean().iloc[-1] if "Volume" in df else None

    if any(pd.isna(x) for x in [ema20, ema50, close, rsi]):
        return False

    # --------------------------------------------------
    # 1️⃣ TREND FILTER
    # --------------------------------------------------
    if not (ema20 > ema50):
        return False

    # --------------------------------------------------
    # 2️⃣ TTM SQUEEZE DETECTION
    # --------------------------------------------------
    bb_upper = last.get("BB_upper")
    bb_lower = last.get("BB_lower")
    bb_mid = last.get("BB_mid")

    if atr and bb_mid is not None:
        kc_upper = bb_mid + atr * 1.5
        kc_lower = bb_mid - atr * 1.5
        squeeze_now = bb_upper < kc_upper and bb_lower > kc_lower
    else:
        squeeze_now = False

    squeeze_recent = False
    if len(df) >= 6:
        for i in range(2, 7):
            row = df.iloc[-i]
            bu = row.get("BB_upper")
            bl = row.get("BB_lower")
            bm = row.get("BB_mid")
            atr_i = row.get("ATR")
            if bu is not None and bl is not None and bm is not None and atr_i:
                ku = bm + atr_i * 1.5
                kl = bm - atr_i - atr_i * 1.5
                if bu < ku and bl > kl:
                    squeeze_recent = True
                    break

    # --------------------------------------------------
    # 3️⃣ SHORT SQUEEZE DETECTOR
    # --------------------------------------------------
    short_squeeze = False
    if volume and vol_avg and vol_avg > 0:
        vol_spike = volume > vol_avg * 2.0
        price_breakout = close > df["High"].rolling(20).max().iloc[-2]
        momentum = rsi >= 60 and close > prev["Close"]

        if vol_spike and price_breakout and momentum:
            short_squeeze = True

    # --------------------------------------------------
    # 4️⃣ BREAKOUT CONFIRMATION
    # --------------------------------------------------
    breakout = close > prev["Close"] and close > ema20
    rsi_ok = 45 <= rsi <= 72

    # --------------------------------------------------
    # 5️⃣ VOLATILITY EXPANSION
    # --------------------------------------------------
    vol_ok = True
    if atr and len(df) >= 10:
        atr_prev = df["ATR"].iloc[-6]
        if not pd.isna(atr_prev):
            vol_ok = atr > atr_prev * 1.3

    # --------------------------------------------------
    # ✅ FINAL BUY SIGNAL
    # --------------------------------------------------
    return bool((squeeze_recent and breakout and rsi_ok and vol_ok) or short_squeeze)

import plotly.graph_objects as go
def generate_light_interactive_chart(df: pd.DataFrame, ticker: str, ai_score: float, extra_badges=None) -> str:
    """
    Generates a Plotly HTML string for a powerful light-mode interactive chart.
    """
    if df is None or df.empty: return ""
    
    # Slice to last 150 days for clarity
    plot_df = df.tail(150).copy()
    
    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=plot_df['Date'],
        open=plot_df['Open'], high=plot_df['High'],
        low=plot_df['Low'], close=plot_df['Close'],
        name='Price'
    ))

    # EMA Overlays
    if 'ema20' in plot_df.columns:
        fig.add_trace(go.Scatter(x=plot_df['Date'], y=plot_df['ema20'], line=dict(color='#2563eb', width=1.5), name='EMA 20'))
    if 'ema50' in plot_df.columns:
        fig.add_trace(go.Scatter(x=plot_df['Date'], y=plot_df['ema50'], line=dict(color='#f59e0b', width=1.5), name='EMA 50'))

    # Dynamic Score Color
    score_color = "#10b981" if ai_score >= 70 else ("#f59e0b" if ai_score >= 50 else "#ef4444")

    # Light Mode Styling
    fig.update_layout(
        template="plotly_white",
        title=dict(text=f"{ticker} - AI Opportunity Score: {ai_score:.1f}%", font=dict(size=20)),
        yaxis_title="Price ($)",
        xaxis_rangeslider_visible=False,
        height=500,
        margin=dict(l=40, r=40, t=60, b=40),
        paper_bgcolor="white",
        plot_bgcolor="#f8fafc",
        annotations=[dict(
            x=0.02, y=0.95, xref="paper", yref="paper",
            text=f"AI SCORE: {ai_score:.1f}", showarrow=False,
            font=dict(size=18, color="white"),
            bgcolor=score_color, bordercolor=score_color, borderpad=6, opacity=0.9
        )]
    )
    return fig.to_html(full_html=False, include_plotlyjs='cdn')
def calculate_ai_score(df: pd.DataFrame) -> float:
    """
    Calculates a buying opportunity score (0-100) using multi-factor technical analysis.
    Higher scores = stronger buying opportunities.
    """
    if df is None or len(df) < 50: return 0.0
    
    score = 50.0 # Neutral starting point
    last = df.iloc[-1]
    
    # Factor 1: RSI (Oversold is good for buying)
    if last['RSI'] < 30: score += 25  # Strong Oversold
    elif last['RSI'] < 40: score += 12
    elif last['RSI'] > 75: score -= 20 # Overbought
    
    # Factor 2: EMA Alignment (Bullish trend)
    # Ensure EMA columns exist before checking
    if 'ema20' in df.columns and 'ema50' in df.columns:
        if last['Close'] > last['ema20'] > last['ema50']: 
            score += 15
        elif last['Close'] < last['ema50']: 
            score -= 10
    
    # Factor 3: Distance from Yearly Low (Mean Reversion)
    year_low = df['Close'].tail(252).min()
    if year_low > 0:
        dist_from_low = (last['Close'] - year_low) / year_low
        if dist_from_low < 0.05: score += 20 # "Bounce off Yearly Low" logic
    
    # Factor 4: Bollinger Band Position (Bounce off lower band)
    if 'BB_lower' in df.columns and last['Close'] <= last['BB_lower']: 
        score += 10
    
    return max(0.0, min(100.0, score))
def _inline_payload_js(div_id: str, chart_payload: Dict[str, Any], markers: List[Dict[str, Any]] = None, table_data: Dict[str, Any] = None) -> str:
    try:
        obj = {
            "data": chart_payload,
            "markers": markers or [],
            "tableData": table_data or {}
        }

        # --- AI SCORE METADATA ---
        if chart_payload:
            ai_vals = chart_payload.get("ai_score", [])
            if ai_vals:
                last_ai = ai_vals[-1]
                if last_ai is not None:
                    if last_ai >= 70:
                        ai_color = "green"
                    elif last_ai >= 45:
                        ai_color = "gold"
                    else:
                        ai_color = "red"
                else:
                    ai_color = "gray"
            else:
                ai_color = "gray"

            obj["meta"] = {
                "last_ai": last_ai if ai_vals else None,
                "ai_color": ai_color,
                "has_bb": bool(chart_payload.get("bb_upper")),
            }

        json_str = json.dumps(obj, separators=(',', ':'))
        return f"<script>window._tb_chart_payloads=window._tb_chart_payloads||{{}};window._tb_chart_payloads['{div_id}']={json_str};</script>"
    except Exception:
        return ""

def generate_html_page(page_type, data_groups, outpath, nav_link, source_info, timestamp_str,
                       report_js_template, existing_favorites=None):

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    page_title = {"universal": "Universal", "watchlist": "Watchlist", "sector": "Sector", "inbox": "Inbox Alerts"}.get(page_type, "Report")
    group_names = list(data_groups.keys())
    fav_json = json.dumps(existing_favorites or [])

    # ==========================================================
    # EMBEDDED CSS — PROFESSIONAL LIGHT SILVER THEME (LOGIC SAFE)
    # ==========================================================
    report_css = """
    :root {
      --bg-main: #f5f7fb;
      --bg-card: #ffffff;
      --bg-soft: #eef2f7;
      --bg-toolbar: rgba(255,255,255,0.92);
      --border-soft: #d9dee7;
      --border-strong: #c4cbd8;
      --text-main: #1f2937;
      --text-muted: #6b7280;
      --text-soft: #9ca3af;
      --primary: #2563eb;
      --primary-soft: #e8efff;
      --green: #16a34a;
      --green-soft: #dcfce7;
      --red: #dc2626;
      --red-soft: #fee2e2;
      --amber: #f59e0b;
      --amber-soft: #ffedd5;
      --purple: #7c3aed;
      --purple-soft: #ede9fe;
      --shadow-xs: 0 1px 2px rgba(0,0,0,0.04);
      --shadow-sm: 0 3px 8px rgba(0,0,0,0.06);
      --shadow-md: 0 8px 20px rgba(0,0,0,0.08);
      --radius-sm: 6px;
      --radius-md: 10px;
      --radius-lg: 14px;
      --font-main: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      padding: 0;
      font-family: var(--font-main);
      font-size: 13.5px;
      line-height: 1.45;
      background: linear-gradient(180deg, #f8fafc 0%, #eef2f7 100%);
      color: var(--text-main);
    }

    body {
      padding: 18px 20px 28px;
    }

    /* ------------------------------------------------------ */
    /* TOP TOOLBAR                                            */
    /* ------------------------------------------------------ */

    .top-toolbar {
      position: sticky;
      top: 0;
      z-index: 1000;
      background: linear-gradient(180deg, #ffffff 0%, #f4f7fb 100%);
      backdrop-filter: blur(10px);
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-md);
      padding: 10px 14px;
      margin-bottom: 12px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      box-shadow: var(--shadow-sm);
    }

    .brand {
      font-weight: 800;
      font-size: 1.1rem;
      letter-spacing: 0.2px;
      color: var(--primary);
      margin-right: 12px;
      white-space: nowrap;
    }

    .controls {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
    }

    /* ------------------------------------------------------ */
    /* BUTTONS                                                */
    /* ------------------------------------------------------ */

    .btn {
      cursor: pointer;
      padding: 5px 11px;
      border-radius: 999px;
      border: 1px solid var(--border-soft);
      background: linear-gradient(180deg, #ffffff 0%, #f2f5fa 100%);
      font-weight: 600;
      font-size: 0.72rem;
      letter-spacing: 0.2px;
      transition: all 0.15s ease;
      text-decoration: none;
      color: var(--text-main);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 4px;
      box-shadow: var(--shadow-xs);
      white-space: nowrap;
    }

    .btn:hover {
      background: linear-gradient(180deg, #ffffff 0%, #edf1f7 100%);
      border-color: var(--border-strong);
      transform: translateY(-0.5px);
      box-shadow: var(--shadow-sm);
    }

    .btn.primary {
      background: linear-gradient(180deg, #3b82f6 0%, #2563eb 100%);
      color: #ffffff;
      border-color: #2563eb;
      box-shadow: 0 3px 10px rgba(37,99,235,0.25);
    }

    .btn.active {
      background: linear-gradient(180deg, #2563eb 0%, #1d4ed8 100%);
      color: #ffffff;
      border-color: #1d4ed8;
      box-shadow: inset 0 2px 4px rgba(0,0,0,0.15);
    }

    /* ------------------------------------------------------ */
    /* FILTER AREA / SLIDERS                                  */
    /* ------------------------------------------------------ */

    .filter-area {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
    }

    input[type="range"] {
      accent-color: #2563eb;
      cursor: pointer;
    }

    select.btn {
      padding: 5px 10px;
    }

    /* ------------------------------------------------------ */
    /* LAYOUT CONTAINERS                                      */
    /* ------------------------------------------------------ */

    .container {
      width: 100%;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
      gap: 12px;
    }

    /* ------------------------------------------------------ */
    /* CARDS                                                  */
    /* ------------------------------------------------------ */

    .card {
      background: linear-gradient(180deg, #ffffff 0%, #f6f8fc 100%);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow-sm);
      padding: 14px 16px;
      margin-bottom: 12px;
      border: 1px solid var(--border-soft);
      transition: all 0.15s ease;
    }

    .card:hover {
      box-shadow: var(--shadow-md);
      border-color: var(--border-strong);
    }

    .group-card > h3 {
      margin: 2px 0 10px 0;
      font-size: 1rem;
      font-weight: 800;
      letter-spacing: 0.3px;
      color: #111827;
    }

    .signal_card.card {
      padding: 12px 14px;
      border-radius: var(--radius-lg);
      background: linear-gradient(180deg, #ffffff 0%, #f5f7fb 100%);
      box-shadow: var(--shadow-xs);
      border: 1px solid var(--border-soft);
      margin-bottom: 12px;
    }

    /* ------------------------------------------------------ */
    /* BADGES                                                 */
    /* ------------------------------------------------------ */

    .badge {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 999px;
      font-size: 0.62rem;
      font-weight: 800;
      letter-spacing: 0.25px;
      background: linear-gradient(180deg, #f1f3f8 0%, #e5e9f2 100%);
      color: #475569;
      margin-right: 4px;
      margin-bottom: 4px;
      border: 1px solid var(--border-soft);
      white-space: nowrap;
    }

    /* ------------------------------------------------------ */
    /* HEADER ROW / STATUS                                    */
    /* ------------------------------------------------------ */

    .header-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: space-between;
      align-items: center;
      font-size: 0.8rem;
    }

    .status {
      font-weight: 700;
      color: var(--text-muted);
    }

    .small {
      font-size: 0.72rem;
      color: var(--text-soft);
    }

    /* ------------------------------------------------------ */
    /* CHART AREA                                             */
    /* ------------------------------------------------------ */

    .chart-container {
      width: 100%;
      height: 100%;
      min-height: 300px;
      background: linear-gradient(180deg, #ffffff 0%, #f5f7fb 100%);
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-md);
      overflow: hidden;
      box-shadow: var(--shadow-xs);
    }

    .chart-controls {
      display: flex;
      gap: 6px;
      margin-top: 6px;
      flex-wrap: wrap;
    }

    .chart-table {
      margin-top: 6px;
      background: #ffffff;
      border: 1px solid var(--border-soft);
      border-radius: var(--radius-md);
      padding: 6px;
      font-size: 0.7rem;
      overflow-x: auto;
    }

    /* ------------------------------------------------------ */
    /* FOOTER / LOGS                                          */
    /* ------------------------------------------------------ */

    .footer-small {
      margin-top: 16px;
      font-size: 0.7rem;
      text-align: center;
      color: var(--text-soft);
    }

    details > summary {
      outline: none;
    }

    /* ------------------------------------------------------ */
    /* SCROLL TO TOP BUTTON                                   */
    /* ------------------------------------------------------ */

    #scrollTopBtn {
      display: none;
      position: fixed;
      bottom: 20px;
      right: 24px;
      z-index: 99;
      border: none;
      outline: none;
      background: linear-gradient(180deg, #3b82f6 0%, #2563eb 100%);
      color: white;
      cursor: pointer;
      padding: 10px 14px;
      border-radius: 10px;
      font-size: 0.8rem;
      font-weight: 800;
      box-shadow: 0 6px 18px rgba(37,99,235,0.35);
    }

    #scrollTopBtn:hover {
      background: linear-gradient(180deg, #2563eb 0%, #1d4ed8 100%);
    }

    /* ------------------------------------------------------ */
    /* SCROLLBAR                                              */
    /* ------------------------------------------------------ */

    ::-webkit-scrollbar {
      width: 9px;
      height: 9px;
    }

    ::-webkit-scrollbar-track {
      background: #f2f4f8;
    }

    ::-webkit-scrollbar-thumb {
      background: #cfd5df;
      border-radius: 6px;
    }

    ::-webkit-scrollbar-thumb:hover {
      background: #b8c0cc;
    }

    /* ------------------------------------------------------ */
    /* RESPONSIVE                                             */
    /* ------------------------------------------------------ */

    @media (max-width: 900px) {
      .grid {
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      }
    }

    @media (max-width: 520px) {
      .top-toolbar {
        flex-direction: column;
        align-items: stretch;
      }
      .controls {
        justify-content: flex-start;
      }
    }
    """

    parts = []
    parts.append("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>")
    parts.append(f"<title>TopBottom {page_title} {SCRIPT_VERSION} — {now_str}</title>")
    parts.append(f"<style>{report_css}</style></head><body>")

    # ---------------------------------------------------------
    # TOP NAV BAR
    # ---------------------------------------------------------
    parts.append("<div class='top-toolbar'><div class='brand'>TopBottom — " + SCRIPT_VERSION + "</div><div class='controls'>")
    if page_type == 'universal':
        parts.append("<button class='btn primary'>🌐 Universal</button>")
    elif 'univ_file' in nav_link:
        parts.append(f"<a href='{nav_link['univ_file']}' class='btn ghost'>🌐 Universal</a>")

    if 'watch_file' in nav_link:
        if page_type == 'watchlist':
            parts.append("<button class='btn primary'>⭐ Watchlist</button>")
        else:
            parts.append(f"<a href='{nav_link['watch_file']}' class='btn ghost'>⭐ Watchlist</a>")

    if 'inbox_file' in nav_link:
        if page_type == 'inbox':
            parts.append("<button class='btn primary'>📩 Inbox</button>")
        else:
            parts.append(f"<a href='{nav_link['inbox_file']}' class='btn ghost'>📩 Inbox</a>")

    if 'sector_file' in nav_link:
        if page_type == 'sector':
            parts.append("<button class='btn primary'>📊 Sector</button>")
        else:
            parts.append(f"<a href='{nav_link['sector_file']}' class='btn ghost'>📊 Sector</a>")

    if 'fav_file' in nav_link:
        parts.append(f"<a href='{nav_link['fav_file']}' class='btn ghost'>❤️ Favorites</a>")

    parts.append("</div><div style='width:16px'></div><div class='controls'><div class='filter-area'>")

    # ---------------------------------------------------------
    # GROUP FILTERS
    # ---------------------------------------------------------
    parts.append("<button class='btn' onclick=\"filterGroup('ALL')\">All</button>")
    for gname in sorted(group_names):
        parts.append(f"<button class='btn' data-filter='{gname}' data-text='{gname}' onclick=\"toggleTagButton(this,'{gname}')\">{gname}</button>")

    parts.append("<div style='width:12px; border-left:1px solid #ccc; margin:0 4px;'></div>")

    # ---------------------------------------------------------
    # AI SCORE SLIDER + STACK MODE
    # ---------------------------------------------------------
    parts.append("""
    <div style="display:flex;align-items:center;gap:6px;padding:2px 6px;border:1px solid #cbd5e0;border-radius:8px;background:#f8fafc;">
        <span style="font-size:0.8rem;font-weight:600;">AI ≥</span>
        <input type="range" id="aiScoreSlider" min="0" max="100" value="0" step="5" style="width:110px;" oninput="updateAIScoreFilter(this.value)">
        <span id="aiScoreVal" style="font-size:0.75rem;font-weight:700;">0</span>
    </div>
    <select id="stackModeSelect" class="btn" onchange="applyFilters()">
        <option value="AND" selected>AND</option>
        <option value="OR">OR</option>
    </select>
    """)

    parts.append("<div style='width:12px; border-left:1px solid #ccc; margin:0 4px;'></div>")

    # ---------------------------------------------------------
    # TECH FILTER BUTTONS
    # ---------------------------------------------------------
    tech_btns = [
        ("BUYING_OPPORTUNITY", "💰 Buying Opportunity"),
        ("DAILY_SIGNAL", "Daily Signal"),
        ("WEEKLY_SIGNAL", "Weekly Signal"),
        ("INTRADAY_SIGNAL", "Intraday Signal"),
        ("MONTHLY_SIGNAL", "Monthly Signal"),
        ("MTF_ALIGN", "MTF Align"),

        ("LARGE_CAP", "Large Cap"),
        ("MID_CAP", "Mid Cap"),
        ("SMALL_CAP", "Small Cap"),

        ("MA5_LT_MA10", "MA5 < MA10"),
        ("MA5_GT_MA10", "MA5 > MA10"),
        ("MA10_GT_MA20", "MA10 > MA20"),
        ("MA10_LT_MA20", "MA10 < MA20"),

        ("3_DAY_HIGH", "3D High"),
        ("3_DAY_LOW", "3D Low"),
        ("5_DAY_HIGH", "5D High"),
        ("5_DAY_LOW", "5D Low"),
        ("7_DAY_HIGH", "7D High"),
        ("7_DAY_LOW", "7D Low"),
        ("9_DAY_HIGH", "9D High"),
        ("9_DAY_LOW", "9D Low"),
        ("15_DAY_HIGH", "15D High"),
        ("15_DAY_LOW", "15D Low"),
        ("20_DAY_HIGH", "20D High"),
        ("20_DAY_LOW", "20D Low"),

        ("ENTRY_PLUS", "Entry +"),
        ("ENTRY_MINUS", "Entry −"),
        ("ENTRY_PLUS_1W", "Entry +1W"),
        ("ENTRY_PLUS_1M", "Entry +1M"),

        ("INTRADAY_TOP", "Intraday Top"),
        ("INTRADAY_BOTTOM", "Intraday Bottom"),
        ("DAILY_TOP", "Daily Top"),
        ("DAILY_BOTTOM", "Daily Bottom"),
        ("BOUNCE_YEARLY_LOW", "Bounce Yearly Low"),
        ("BUILDING_TREND", "Building Trend"),
        ("CROSS_ABOVE_20EMA", "Cross > 20EMA"),
        ("CROSS_BELOW_20EMA", "Cross < 20EMA"),
        ("BREAKOUT_UP", "Breakout ↑"),
        ("BREAKOUT_DOWN", "Breakout ↓"),

        ("MONTH_UP", "Month ↑"),
        ("MONTH_DOWN", "Month ↓"),
        ("YTD_UP", "YTD ↑"),
        ("YTD_DOWN", "YTD ↓"),
        ("QUARTER_UP", "Quarter ↑"),
        ("QUARTER_DOWN", "Quarter ↓"),

        ("RSI_OVERBOUGHT", "RSI OB"),
        ("RSI_OVERSOLD", "RSI OS"),

        ("UPCOMING_EARNINGS", "Upcoming E (15d)"),
        ("POST_EARNINGS", "Post E (15d)"),

        ("RECENT_WEEKLY_TOP", "Recent W-Top"),
        ("RECENT_WEEKLY_BOTTOM", "Recent W-Bottom"),

        ("QUALITY_A_PLUS", "🏆 Quality A+"),
        ("QUALITY_A", "🏆 Quality A"),
        ("QUALITY_B", "🏆 Quality B"),
        ("OPTIONS_BULLISH", "📈 Opts Bullish"),
        ("OPTIONS_BEARISH", "📉 Opts Bearish"),
        ("UNUSUAL_OPTIONS", "⚡ Unusual Options"),
    ]

    for tag_id, label in tech_btns:
        style = ""
        if any(x in tag_id for x in ['TOP', 'UP', 'HIGH', 'PLUS', 'ALIGN', 'BOUNCE', 'BUILDING', 'ABOVE']):
            style = "background-color:#f0fff4;color:#2f855a;border-color:#c6f6d5;"
        elif any(x in tag_id for x in ['BOTTOM', 'DOWN', 'LOW', 'MINUS', 'BELOW', 'OVERBOUGHT']):
            style = "background-color:#fff5f5;color:#c53030;border-color:#fecaca;"
        parts.append(
            f"<button class='btn' data-filter='{tag_id}' data-text='{label}' "
            f"onclick=\"toggleTagButton(this,'{tag_id}')\" style='{style}'>{label}</button>"
        )

    parts.append("</div><div style='margin-left:auto;display:flex;gap:8px;align-items:center'>")
    parts.append("<button class='btn' style='background:#22c55e;color:white;border:1px solid #16a34a;' onclick='exportFavorites()'>💾 Save Favorites DB</button>")
    parts.append("<select id='modeSelect' class='btn' onchange='setMode(this.value); updateFilterState();'><option value='STRICT' selected>STRICT</option><option value='NORMAL'>NORMAL</option><option value='LOOSE'>LOOSE</option></select>")
    parts.append("<button class='btn' onclick='downloadCSV()'>Download CSV</button><button class='btn' onclick='manualRefresh()'>🔄 Refresh</button><label class='small' style='margin-left:6px'>Auto-Refresh</label><input id='autoRefreshToggle' type='checkbox' onchange='toggleAutoRefresh(this.checked)'></div></div>")

    # ---------------------------------------------------------
    # HEADER
    # ---------------------------------------------------------
    parts.append("<div class='container' style='max-width: 95%;'><div class='card'><div class='header-row'><div><strong>Source:</strong> " + source_info + "</div><div id='statusMsg' class='status'>Mode: STRICT • Filters: none • Showing 0 of 0 stocks</div></div><div class='small'>Tip: Click '+ Add' on multiple stocks, then 'Save Favorites DB' to download. Next time you run the script, it will auto-import from Downloads.</div></div>")
    parts.append(f"<div id='view_content_area' data-page-type='{page_type}'><h2 style='margin-top:8px'>{page_title} Universe</h2>")
    parts.append("<div id='no_results_msg' class='card' style='display:none; color: var(--muted); text-align: center; padding: 30px;'>No stocks match the current filter combination.</div>")

    # ---------------------------------------------------------
    # BODY
    # ---------------------------------------------------------
    total_signals = sum(len(v) for v in data_groups.values())
    if not data_groups or total_signals == 0:
        parts.append(f"<div class='card'>No signals in {page_title}</div>")
    else:
        for tab, items in data_groups.items():
            if not items:
                continue
            group_name = tab
            safe_tab_id = "".join(c for c in tab if c.isalnum())
            parts.append(f"<div class='card group-card' id='group_card_{safe_tab_id}' data-group-name='{group_name}'><h3>{group_name} ({len(items)})</h3>")

            for idx, s in enumerate(items[:UNIVERSE_LIMIT]):
                ticker = s.get('ticker')
                tags_from_analysis = s.get('tags', [])

                # -------------------------
                # 🔥 FIX 1 — ADD EARNINGS TAGS FOR FILTERING
                # -------------------------
                earnings_date_dt = s.get('earnings_date')
                earnings_str = earnings_date_dt.strftime('%Y-%m-%d') if isinstance(earnings_date_dt, datetime) else ""

                all_tags = set(tags_from_analysis)
                all_tags.add(group_name)

                if earnings_str:
                    all_tags.add("UPCOMING_EARNINGS")
                    all_tags.add("HAS_EARNINGS")

                if page_type == 'sector' and s.get('sector') and s.get('sector') not in group_names:
                    all_tags.add(s['sector'])

                data_tags_str = ",".join(sorted(list(all_tags)))
                score = s.get("buy_score", 0)

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

                if earnings_str:
                    badges_html += f" <span class='badge' style='background:#f0f5ff; color:#434190; border:1px solid #c3dafe;'>🗓️ Earnings: {earnings_str}</span>"

                parts.append(
                    f"<div class='signal_card card' data-ticker='{ticker}' "
                    f"data-tags='{data_tags_str}' data-score='{score}'>"
                )

                last_close = s.get('last_close')
                last_date = s.get('last_date')
                add_button_html = ""
                if last_close and not pd.isna(last_close):
                    c_price = f"{last_close:.2f}"
                    c_date = last_date if last_date else datetime.now().strftime('%Y-%m-%d')
                    add_button_html = f"<button id='favbtn_{ticker}' class='btn' onclick=\"addToFavorite('{ticker}', '{c_price}', '{c_date}', this)\" style='font-size: 0.8rem; padding: 4px 8px; margin-left: 10px;'>+ Add to Favorite</button>"

                    weekly_sig = "YES" if "WEEKLY_SIGNAL" in tags_from_analysis else "NO"
                    daily_sig = "YES" if "DAILY_SIGNAL" in tags_from_analysis else "NO"
                    intr_sig = "YES" if "INTRADAY_SIGNAL" in tags_from_analysis else "NO"
                    ai_val = int(score or 0)

                    sig_html = (
                        f"<span class='badge' style='background:#eef2ff;color:#3730a3;'>W: {weekly_sig}</span>"
                        f"<span class='badge' style='background:#ecfeff;color:#0e7490;'>D: {daily_sig}</span>"
                        f"<span class='badge' style='background:#f0fdf4;color:#166534;'>I: {intr_sig}</span>"
                        f"<span class='badge' style='background:#fff7ed;color:#9a3412;'>AI: {ai_val}</span>"
                    )

                    parts.append(
                        f"<div style='display:flex;justify-content:space-between;align-items:center'>"
                        f"<div><a href='https://finviz.com/quote.ashx?t={ticker}&p=d' target='_blank' style='font-weight:700;color:var(--primary);font-size:1.1rem;'>{ticker}</a> {add_button_html} {sig_html} {badges_html}</div>"
                        f"</div>"
                    )

                trade_details = s.get('trade_details', {})
                if trade_details and tags_from_analysis:
                    details_html = f"<div style='border: 1px solid #e2e8f0; padding: 12px; border-radius: 8px; margin-top: 12px; background: #fdfdfd; font-size: 0.9em;'><h4 style='margin-top: 0; margin-bottom: 8px; color: var(--primary);'>Trade Strategy Analysis</h4>"
                    ordered_tags = sorted(tags_from_analysis, key=lambda x: "0" if "DAILY" in x or "BREAKOUT" in x else "1")
                    for tag in ordered_tags:
                        if tag in trade_details:
                            detail = trade_details[tag]
                            details_html += f"<div style='margin-bottom: 10px; border-left: 3px solid #cbd5e0; padding-left: 10px;'><div style='font-weight:bold;'>Signal: {tag}</div><p style='margin: 4px 0;'><strong>Technicals:</strong> {detail.get('desc', 'n/a')}</p><div style='display: flex; flex-wrap: wrap; gap: 20px;'><div><strong>Entry:</strong> {money(detail.get('entry'))}</div><div><strong style='color: #2f855a;'>Target:</strong> {money(detail.get('tp'))}</div><div><strong style='color: #c53030;'>Stop Loss:</strong> {money(detail.get('sl'))}</div></div></div>"
                    if earnings_str:
                        details_html += f"<p style='margin: 8px 0 0 0; color:#434190; font-size: 0.85em;'><strong>🗓️ Upcoming Earnings:</strong> {earnings_str}</p>"
                    details_html += "</div>"
                    parts.append(details_html)

                parts.append(generate_trend_html(s.get('daily_df')))

                intr_div = f"{page_type}_intr_{idx}_{safe_tab_id}"
                daily_div = f"{page_type}_daily_{idx}_{safe_tab_id}"

                parts.append(
                    f"<div class='grid' style='margin-top:8px'>"
                    f"<div><div id='{intr_div}' class='chart-container' style='height:{CHART_HEIGHT}px;min-width:240px'></div>"
                    f"<div class='chart-controls'><a href='https://stockanalysis.com/stocks/{ticker.lower()}/' target='_blank' class='btn'>📈 Forecast</a>"
                    f"<button class='btn' onclick=\"toggleTable('{intr_div}')\">📋 Toggle Table</button></div>"
                    f"<div id='{intr_div}_table' class='chart-table' style='display:none'></div></div>"
                    f"<div><div id='{daily_div}' class='chart-container' style='height:{CHART_HEIGHT}px;min-width:240px'></div>"
                    f"<div class='chart-controls'><a href='https://stockanalysis.com/stocks/{ticker.lower()}/' target='_blank' class='btn'>📈 Forecast</a>"
                    f"<button class='btn' onclick=\"toggleTable('{daily_div}')\">📋 Toggle Table</button></div>"
                    f"<div id='{daily_div}_table' class='chart-table' style='display:none'></div></div>"
                    f"</div></div>"
                )

                intr_payload = _df_to_payload(s.get('intraday_df'), MAX_HISTORY_INTRADAY)
                daily_payload = _df_to_payload(s.get('daily_df'), MAX_HISTORY_DAILY)

                # 🔥 FIX 2 — INJECT EARNINGS INTO PAYLOADS
                if earnings_str:
                    intr_payload["earnings"] = [earnings_str]
                    daily_payload["earnings"] = [earnings_str]
                else:
                    intr_payload["earnings"] = []
                    daily_payload["earnings"] = []

                # 🔥 ENSURE TABLE HAS RSI + MA5/10/20/30
                intr_table_data = _df_to_table_data(s.get('intraday_df'), TABLE_ROWS_INTRADAY,
                                                    extra_cols=["RSI", "MA5", "MA10", "MA20", "MA30"])
                daily_table_data = _df_to_table_data(s.get('daily_df'), TABLE_ROWS_DAILY,
                                                     extra_cols=["RSI", "MA5", "MA10", "MA20", "MA30"])

                def get_markers(dframe, peaks, troughs, length):
                    m = []
                    if dframe is not None and not dframe.empty:
                        try:
                            for p in (peaks or [])[-50:]:
                                if isinstance(p, int) and 0 <= p < length:
                                    m.append({"type": "peak", "pos": p, "price": float(dframe['Close'].iloc[p])})
                            for t in (troughs or [])[-50:]:
                                if isinstance(t, int) and 0 <= t < length:
                                    m.append({"type": "trough", "pos": t, "price": float(dframe['Close'].iloc[t])})
                        except:
                            pass
                    return m

                im = get_markers(s.get('intraday_df'), s.get('intr_peaks'), s.get('intr_troughs'), s.get('intr_len', 0))
                dm = get_markers(s.get('daily_df'), s.get('daily_peaks'), s.get('daily_troughs'), s.get('daily_len', 0))

                if intr_payload.get("labels"):
                    parts.append(_inline_payload_js(intr_div, intr_payload, im, intr_table_data))
                if daily_payload.get("labels"):
                    parts.append(_inline_payload_js(daily_div, daily_payload, dm, daily_table_data))

            parts.append("</div>")

    parts.append("</div></div>")

    logs = log_buffer.getvalue()[-30000:]
    parts.append(
        f"<div class='card'><details open><summary style='cursor: pointer; font-weight: bold; font-size: 1.25rem; margin-bottom: 10px;'>Latest Logs</summary>"
        f"<div style='font-family:monospace;background:#081025;color:#e6f1ff;padding:10px;border-radius:8px;white-space:pre-wrap;font-size:12px; margin-top: 10px; max-height: 400px; overflow-y: auto;'>{(logs or '').replace('<','&lt;').replace('>','&gt;')}</div></details></div>"
    )

    parts.append(f"<div class='footer-small'>Generated by TopBottom_Universe {SCRIPT_VERSION} — {now_str}</div><button onclick='scrollToTop()' id='scrollTopBtn' title='Go to top'>↑ Top</button>")

    # PLOTLY FALLBACK
    if os.path.exists(LOCAL_PLOTLY_FILE):
        with open(LOCAL_PLOTLY_FILE, "r", encoding="utf-8", errors="ignore") as f:
            parts.append("<script>" + f.read() + "</script>")
    else:
        parts.append("<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>")

    if os.path.exists(XLSX_JS_FILE):
        with open(XLSX_JS_FILE, "r", encoding="utf-8", errors="ignore") as f:
            parts.append("<script>" + f.read() + "</script>")
    else:
        parts.append("<script src='https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js'></script>")

    parts.append(f"<script>window.initialFavorites = {fav_json};</script>")
    
    # ---------------------------------------------------------
    # 🔥 CONSOLIDATED JAVASCRIPT LOGIC
    # ---------------------------------------------------------
    js_final = report_js_template.replace("%EAGER%", str(EAGER_RENDER_FIRST_N)) \
                                 .replace("%REF%", str(AUTO_REFRESH_MINUTES_DEFAULT)) \
                                 .replace("%HEIGHT%", str(CHART_HEIGHT)) \
                                 .replace("%TABLEROWS_DAILY%", str(TABLE_ROWS_DAILY)) \
                                 .replace("%TABLEROWS_INTRADAY%", str(TABLE_ROWS_INTRADAY))
    parts.append("<script>" + js_final + "</script>")

    try:
        with open(outpath, "w", encoding="utf-8") as f:
            f.write("\n".join(parts))
        logger.info("Saved HTML report: %s", outpath)
    except Exception as e:
        logging.error("Could not write HTML: %s", e)


# -------------------- HTML / JS --------------------
def _df_to_payload(df: pd.DataFrame, max_bars: int = 252) -> Dict[str, Any]:
    """
    High-Performance Payload Generator.
    - Uses vectorization for speed (no slow loops).
    - Rounds to 2 decimals to create 'Baby Size' output files.
    - Handles Volume as integers to save space.
    - Adds Bollinger Bands + AI Score line for charts.
    """
    if df is None or df.empty:
        return {}

    # 1. Slice efficiently
    d = df.iloc[-max_bars:]

    # 2. Fast Vectorized Cleaning
    def clean(series):
        return series.round(2).where(pd.notnull(series), None).tolist()

    # 3. Optimized Date Formatting
    try:
        is_intraday = False
        if len(d) > 1:
            diff = d['Date'].iloc[1] - d['Date'].iloc[0]
            if diff.total_seconds() < 86400:
                is_intraday = True

        fmt = '%Y-%m-%d %H:%M' if is_intraday else '%Y-%m-%d'
        labels = d['Date'].dt.strftime(fmt).tolist()
    except:
        labels = d['Date'].astype(str).tolist()

    payload = {
        "labels": labels,
        "open": clean(d['Open']),
        "high": clean(d['High']),
        "low": clean(d['Low']),
        "close": clean(d['Close']),
        "volume": d['Volume'].fillna(0).astype(int).tolist(),
    }

    # -----------------------
    # 🔥 Bollinger Bands (MATCH compute_indicators)
    # -----------------------
    if {"BB_upper", "BB_lower"}.issubset(d.columns):
        payload["bb_upper"] = clean(d["BB_upper"])
        payload["bb_lower"] = clean(d["BB_lower"])
        payload["bb_mid"]   = clean(d["BB_mid"]) if "BB_mid" in d.columns else []
    else:
        payload["bb_upper"] = []
        payload["bb_lower"] = []
        payload["bb_mid"]   = []

    # -----------------------
    # 🔥 AI Score Line
    # -----------------------
    if "AI_SCORE" in d.columns:
        payload["ai_score"] = (
            d["AI_SCORE"]
            .round(0)
            .where(pd.notnull(d["AI_SCORE"]), None)
            .tolist()
        )
    else:
        payload["ai_score"] = []

    return payload

def _df_to_table_data(df: pd.DataFrame, num_rows: int = 30, extra_cols: List[str] = None) -> Dict[str, Any]:
    """
    High-Performance Table Data Generator.
    - Reduces output size by strictly limiting rows and precision.
    - Includes RSI, SAR, VWAP, MA5/10/20/30.
    """
    if df is None or df.empty:
        return {}

    d = df.iloc[-num_rows:]

    def clean(series):
        return series.round(2).where(pd.notnull(series), None).tolist()

    # Fast Date Formatting
    try:
        labels = d['Date'].dt.strftime('%Y-%m-%d %H:%M').tolist()
    except:
        labels = d['Date'].astype(str).tolist()

    return {
        "labels": labels,
        "open": clean(d['Open']),
        "high": clean(d['High']),
        "low": clean(d['Low']),
        "close": clean(d['Close']),
        "volume": d['Volume'].fillna(0).astype(int).tolist(),

        # 🔥 NEW INDICATORS
        "rsi": clean(d['RSI']) if 'RSI' in d.columns else [],
        "sar": clean(d['SAR']) if 'SAR' in d.columns else [],
        "vwap": clean(d['VWAP']) if 'VWAP' in d.columns else [],
        "ma5": clean(d['ma5']) if 'ma5' in d.columns else [],
        "ma10": clean(d['ma10']) if 'ma10' in d.columns else [],
        "ma20": clean(d['ma20']) if 'ma20' in d.columns else [],
        "ma30": clean(d['ma30']) if 'ma30' in d.columns else [],
    }
def clean_output_directory(directory_path: str):
    """
    Safely removes all files and subdirectories within the specified directory.
    """
    if os.path.exists(directory_path):
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logger.error(f"Failed to delete {file_path}. Reason: {e}")



# ═══════════════════════════════════════════════════════════════════════════
# v2.0 — 7-LAYER QUALITY FILTER + DAILY MARKET INTELLIGENCE REPORTS
# Added on top of existing scanner. Does NOT change analyze_ticker() or HTML.
# ═══════════════════════════════════════════════════════════════════════════

import smtplib as _smtplib
from email.mime.multipart import MIMEMultipart as _MIMEMultipart
from email.mime.text import MIMEText as _MIMEText
import re as _re_mod

REPORT_2PM_HOUR = 14
REPORT_4PM_HOUR = 16
report_2pm_sent = False
report_4pm_sent = False
REPORT_11AM_HOUR = 11
REPORT_3PM_HOUR = 15
report_11am_sent = False
report_3pm_sent = False
_QF_SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
_QF_RECIPIENT_STR = os.getenv("RECIPIENT_EMAILS", EMAIL_ADDRESS)
_QF_RECIPIENTS = [a.strip() for a in _QF_RECIPIENT_STR.split(",") if a.strip()] if _QF_RECIPIENT_STR else []

QF_SECTOR_ETF_MAP = {
    "Technology":"XLK","Financial Services":"XLF","Financials":"XLF",
    "Healthcare":"XLV","Health Care":"XLV","Energy":"XLE","Industrials":"XLI",
    "Consumer Cyclical":"XLY","Consumer Defensive":"XLP","Utilities":"XLU",
    "Basic Materials":"XLB","Real Estate":"XLRE","Communication Services":"XLC",
}
QF_SECTOR_NAMES_UNIQUE = ["Technology","Financials","Health Care","Energy","Industrials",
    "Consumer Cyclical","Consumer Defensive","Utilities","Basic Materials","Real Estate","Communication Services"]

QF_TIER1 = {
    "AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","TSLA","BRK-B","UNH","V","JNJ","WMT","JPM",
    "PG","MA","HD","MRK","CVX","KO","DIS","MCD","CSCO","ABT","VZ","NKE","INTC","BA","CAT","GS",
    "XOM","LLY","ABBV","PEP","COST","AVGO","TMO","DHR","ACN","ADBE","CRM","TXN","NFLX","AMD",
    "CMCSA","NEE","PFE","PM","UPS","RTX","HON","LOW","QCOM","UNP","SPGI","ISRG","INTU","AMAT",
    "BKNG","GE","SYK","BLK","MDLZ","GILD","ADI","VRTX","REGN","LRCX","PANW","KLAC","AMT","SCHW",
    "SNPS","CDNS","ZTS","PGR","CB","CME","CI","CL","MCK","SLB","EOG","APD","PH","CMG","ITW",
    "CTAS","ORLY","FDX","PSA","EMR","MSI","PLTR","CRWD","SNOW","NET","DDOG","UBER","ABNB",
    "ARM","SMCI","MRVL","ON","COIN","SQ","SHOP","PYPL","NXPI","MCHP","KEYS","TDG",
}
QF_TIER3_KEYWORDS = {"TQQQ","SQQQ","SPXL","SPXS","UPRO","SPXU","SOXL","SOXS","FNGU","FNGD",
    "TNA","TZA","LABU","LABD","TECL","TECS","FAS","FAZ","ERX","ERY","NUGT","DUST","TMF","TMV",
    "DDM","SSO","SDS","UDOW","SDOW","NAIL","DPST","DRIP",
}


def qf_get_market_regime():
    """Layer 1: Market regime from SPY RSI + MACD + MA20."""
    try:
        spy = fetch_daily_csv("SPY", days=60)
        if spy is None or spy.empty or len(spy) < 25:
            return {"regime": "NEUTRAL", "spy_rsi": 50.0, "spy_close": 0, "spy_ma20": 0}
        c = spy["Close"].values.astype(float)
        deltas = np.diff(c)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        avg_g = np.mean(gains[-14:]); avg_l = np.mean(losses[-14:])
        rs = avg_g / avg_l if avg_l > 0 else 100
        rsi = 100 - (100 / (1 + rs))
        ma20 = float(np.mean(c[-20:]))
        last = float(c[-1])
        ema12 = pd.Series(c).ewm(span=12).mean().iloc[-1]
        ema26 = pd.Series(c).ewm(span=26).mean().iloc[-1]
        macd_val = ema12 - ema26
        bull = (1 if rsi > 55 else 0) + (1 if rsi > 65 else 0) + (1 if macd_val > 0 else 0) + (1 if last > ma20 else 0)
        bear = (1 if rsi < 45 else 0) + (1 if rsi < 35 else 0) + (1 if macd_val < 0 else 0) + (1 if last < ma20 else 0)
        regime = "BULLISH" if bull >= 3 else ("BEARISH" if bear >= 3 else "NEUTRAL")
        return {"regime": regime, "spy_rsi": round(float(rsi), 1), "spy_close": round(last, 2), "spy_ma20": round(ma20, 2)}
    except Exception as e:
        logger.warning("QF Regime failed: %s", e)
        return {"regime": "NEUTRAL", "spy_rsi": 50.0, "spy_close": 0, "spy_ma20": 0}


def qf_get_sector_strengths():
    """Layer 2: Sector relative strength vs SPY."""
    result = {}
    try:
        spy = fetch_daily_csv("SPY", days=15)
        if spy is None or len(spy) < 6: return result
        spy_c = spy["Close"].values.astype(float)
        spy_5d = (spy_c[-1] - spy_c[-6]) / spy_c[-6] * 100
        done_etfs = set()
        for sname in QF_SECTOR_NAMES_UNIQUE:
            etf = QF_SECTOR_ETF_MAP.get(sname)
            if not etf or etf in done_etfs: continue
            done_etfs.add(etf)
            try:
                sec = fetch_daily_csv(etf, days=15)
                if sec is None or len(sec) < 6:
                    result[sname] = {"strength": "NEUTRAL", "rs": 0.0}; continue
                sc = sec["Close"].values.astype(float)
                sec_5d = (sc[-1] - sc[-6]) / sc[-6] * 100
                rs = round(sec_5d - spy_5d, 3)
                strength = "STRONG" if rs > 0.15 else ("WEAK" if rs < -0.15 else "NEUTRAL")
                result[sname] = {"strength": strength, "rs": rs}
            except Exception:
                result[sname] = {"strength": "NEUTRAL", "rs": 0.0}
    except Exception as e:
        logger.warning("QF Sectors failed: %s", e)
    return result


def qf_multi_tf_score(r):
    """Layer 3: Multi-timeframe confirmation score."""
    s = 0
    if r.get("daily_signal"): s += 30
    if r.get("weekly_signal"): s += 25
    if r.get("intraday_signal"): s += 20
    if r.get("monthly_signal"): s += 15
    if r.get("mtf_align"): s += 10
    return min(s, 100)


def qf_volume_quality(r):
    """Layer 4: Volume quality check."""
    try:
        df = r.get("daily_df")
        if df is None or df.empty or len(df) < 21 or "Volume" not in df.columns:
            return {"rvol": 1.0, "vol_rising": False, "score": 10}
        vol = df["Volume"].values.astype(float)
        avg20 = float(np.mean(vol[-21:-1])) if np.mean(vol[-21:-1]) > 0 else 1.0
        rvol = float(vol[-1]) / avg20
        vol_rising = bool(len(vol) >= 3 and vol[-1] > vol[-2] > vol[-3])
        score = 30 if rvol > 2.0 else (25 if rvol > 1.5 else (18 if rvol > 1.2 else (10 if rvol > 1.0 else 5)))
        if vol_rising: score = min(30, score + 5)
        return {"rvol": round(rvol, 2), "vol_rising": vol_rising, "score": score}
    except Exception:
        return {"rvol": 1.0, "vol_rising": False, "score": 10}


def qf_reversal_quality(r, regime, sector_str):
    """Layer 5: Reversal Quality Gate (RQG)."""
    score = 0
    tags = set(r.get("tags", []))
    rsi_val = r.get("rsi")
    is_bottom = bool(tags & {"DAILY_BOTTOM","INTRADAY_BOTTOM","RECENT_WEEKLY_BOTTOM","BULL_REVERSAL","RSI_OVERSOLD","BOUNCE_YEARLY_LOW"})
    is_top = bool(tags & {"DAILY_TOP","INTRADAY_TOP","RECENT_WEEKLY_TOP","BEAR_REVERSAL","RSI_OVERBOUGHT"})
    if rsi_val is not None:
        if is_bottom and rsi_val < 35: score += 15
        elif is_top and rsi_val > 65: score += 15
        elif rsi_val < 40 or rsi_val > 60: score += 5
    try:
        df = r.get("daily_df")
        if df is not None and not df.empty:
            last = df.iloc[-1]; cl = float(last["Close"])
            for bbl_name in ["BB_lower","bb_lower","BBL_20_2.0"]:
                if bbl_name in df.columns:
                    bb_lo = last.get(bbl_name)
                    if bb_lo is not None and not pd.isna(bb_lo) and is_bottom and cl <= float(bb_lo) * 1.02:
                        score += 15
                    break
            for bbu_name in ["BB_upper","bb_upper","BBU_20_2.0"]:
                if bbu_name in df.columns:
                    bb_hi = last.get(bbu_name)
                    if bb_hi is not None and not pd.isna(bb_hi) and is_top and cl >= float(bb_hi) * 0.98:
                        score += 15
                    break
    except Exception: pass
    vq = qf_volume_quality(r)
    if vq["rvol"] > 1.5: score += 15
    elif vq["rvol"] > 1.2: score += 8
    reg = regime.get("regime", "NEUTRAL") if isinstance(regime, dict) else regime
    if is_bottom and reg == "BULLISH": score += 15
    elif is_bottom and reg == "NEUTRAL": score += 8
    elif is_top and reg == "BEARISH": score += 15
    elif is_top and reg == "NEUTRAL": score += 8
    if is_bottom and sector_str == "STRONG": score += 10
    elif is_top and sector_str == "WEAK": score += 10
    elif sector_str == "NEUTRAL": score += 5
    if tags & {"BULL_REVERSAL","BEAR_REVERSAL"}: score += 15
    if "PULLBACK_CONTINUATION" in tags: score += 10
    if "BREAKOUT_UP" in tags and is_bottom: score += 10
    if "BREAKOUT_DOWN" in tags and is_top: score += 10
    return min(score, 100)


def qf_get_tier(ticker):
    """Layer 6: Universe tiering."""
    t = ticker.upper().strip()
    if t in QF_TIER1: return "TIER_1"
    if t in QF_TIER3_KEYWORDS or "-USD" in t or "=F" in t: return "TIER_3"
    return "TIER_2"


def qf_compute_quality_score(r, regime, sector_strengths):
    """Layer 7: Master composite quality score."""
    sector = r.get("sector") or "Other"
    sec_info = sector_strengths.get(sector, {})
    sector_str = sec_info.get("strength", "NEUTRAL")
    tags = set(r.get("tags", []))
    is_bottom = bool(tags & {"DAILY_BOTTOM","INTRADAY_BOTTOM","RECENT_WEEKLY_BOTTOM","BULL_REVERSAL","RSI_OVERSOLD"})
    is_top = bool(tags & {"DAILY_TOP","INTRADAY_TOP","RECENT_WEEKLY_TOP","BEAR_REVERSAL","RSI_OVERBOUGHT"})
    signal_type = "BOTTOM" if is_bottom else ("TOP" if is_top else "NEUTRAL")
    mtf = qf_multi_tf_score(r)
    vol = qf_volume_quality(r)["score"] * (100.0/30.0)
    rqg = qf_reversal_quality(r, regime, sector_str)
    reg = regime.get("regime", "NEUTRAL") if isinstance(regime, dict) else regime
    if signal_type == "BOTTOM":
        reg_s = 100 if reg == "BULLISH" else (60 if reg == "NEUTRAL" else 20)
    elif signal_type == "TOP":
        reg_s = 100 if reg == "BEARISH" else (60 if reg == "NEUTRAL" else 20)
    else: reg_s = 50
    if signal_type == "BOTTOM":
        sec_s = 100 if sector_str == "STRONG" else (50 if sector_str == "NEUTRAL" else 20)
    elif signal_type == "TOP":
        sec_s = 100 if sector_str == "WEAK" else (50 if sector_str == "NEUTRAL" else 20)
    else: sec_s = 50
    tier = qf_get_tier(r.get("ticker", ""))
    tier_s = 100 if tier == "TIER_1" else (70 if tier == "TIER_2" else 40)
    total = round(min((mtf*0.25)+(vol*0.15)+(rqg*0.30)+(reg_s*0.15)+(sec_s*0.10)+(tier_s*0.05), 100), 1)
    grade = "A+" if total >= 75 else ("A" if total >= 60 else ("B" if total >= 45 else ("C" if total >= 30 else "F")))
    r["quality_score"] = total
    r["quality_grade"] = grade
    r["quality_passed"] = grade in ("A+", "A")
    r["quality_tier"] = tier
    r["quality_regime"] = reg
    r["quality_sector_str"] = sector_str
    r["quality_signal_type"] = signal_type
    return r


def qf_apply_quality_filter(results, regime, sector_strengths):
    """Apply 7-layer quality filter to all results. Sorts by score. Does NOT remove results."""
    for r in results:
        try: qf_compute_quality_score(r, regime, sector_strengths)
        except Exception as e:
            r["quality_score"]=0; r["quality_grade"]="F"; r["quality_passed"]=False
            logger.warning("QF scoring failed for %s: %s", r.get("ticker","?"), e)
    results.sort(key=lambda x: x.get("quality_score", 0), reverse=True)
    ap = [r for r in results if r.get("quality_grade") == "A+"]
    aa = [r for r in results if r.get("quality_grade") == "A"]
    bb = [r for r in results if r.get("quality_grade") == "B"]
    logger.info("  ═══ QUALITY FILTER RESULTS ═══")
    logger.info("  A+: %d | A: %d | B: %d | C/F: %d | Total: %d", len(ap), len(aa), len(bb), len(results)-len(ap)-len(aa)-len(bb), len(results))
    for r in (ap + aa)[:20]:
        logger.info("    [%s] %-6s Score=%.1f Signal=%-7s Regime=%-7s Sector=%-7s Tier=%s",
                     r.get("quality_grade","?"), r.get("ticker","?"), r.get("quality_score",0),
                     r.get("quality_signal_type","?"), r.get("quality_regime","?"),
                     r.get("quality_sector_str","?"), r.get("quality_tier","?"))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# DAILY MARKET INTELLIGENCE REPORT — 2 PM + 4 PM ET
# ═══════════════════════════════════════════════════════════════════════════

_RPT_CROSS_ASSETS = {
    "BTC-USD":"Bitcoin","ETH-USD":"Ethereum","SOL-USD":"Solana",
    "GLD":"Gold","SLV":"Silver","USO":"Oil","UNG":"NatGas",
    "COPX":"Copper","URA":"Uranium","LIT":"Lithium","TLT":"20Y Bonds","HYG":"High-Yield",
}
_RPT_SECULAR = [
    {"theme":"AI Infrastructure","tickers":["NVDA","AVGO","AMD","SMCI"],"etf":"SMH","prob":"85-90%","tf":"2025-2030"},
    {"theme":"Nuclear Renaissance","tickers":["CCJ","SMR","NNE","UEC"],"etf":"URA","prob":"75-85%","tf":"2025-2035"},
    {"theme":"Cybersecurity","tickers":["CRWD","PANW","FTNT","ZS"],"etf":"CIBR","prob":"80-90%","tf":"2025-2030"},
    {"theme":"Space Economy","tickers":["RKLB","ASTS","LUNR","PL"],"etf":"UFO","prob":"60-75%","tf":"2025-2035"},
    {"theme":"EV & Battery","tickers":["TSLA","RIVN","QS","ALB"],"etf":"LIT","prob":"70-80%","tf":"2025-2030"},
    {"theme":"Robotics","tickers":["ISRG","PATH","ABBV"],"etf":"ROBO","prob":"75-85%","tf":"2025-2030"},
    {"theme":"Clean Energy","tickers":["FSLR","ENPH","NEE"],"etf":"ICLN","prob":"65-80%","tf":"2025-2035"},
    {"theme":"Quantum Computing","tickers":["IONQ","RGTI","QBTS"],"etf":"QTUM","prob":"40-60%","tf":"2028-2040"},
]

def _rv(v, fmt="%.2f", na="N/A"):
    try: return fmt % float(v) if v is not None else na
    except: return na

def _rarrow(v):
    if v is None: return "&#8594;"
    return "&#9650;" if v > 0.3 else ("&#9660;" if v < -0.3 else "&#8594;")

def _rcolor(v):
    if v is None: return "#666"
    return "#27ae60" if v > 0 else ("#e74c3c" if v < 0 else "#f39c12")

def _r_rsi_sig(rsi):
    if rsi is None: return "N/A"
    if rsi > 70: return "Overbought"
    if rsi < 30: return "Oversold"
    if rsi > 60: return "Bullish"
    if rsi < 40: return "Bearish"
    return "Neutral"

def _r_proc(ticker):
    """Fetch and process a single ticker for the report."""
    info = {"price":None,"rsi":None,"trend_5d":None,"trend_20d":None,"rvol":None,"change_pct":None}
    try:
        df = fetch_daily_csv(ticker, days=30)
        if df is None or df.empty or len(df) < 6: return info
        c = df["Close"].values.astype(float)
        info["price"] = round(c[-1], 2)
        if len(c) >= 2: info["change_pct"] = round((c[-1]-c[-2])/c[-2]*100, 2)
        if len(c) >= 6: info["trend_5d"] = round((c[-1]-c[-6])/c[-6]*100, 2)
        if len(c) >= 21: info["trend_20d"] = round((c[-1]-c[-21])/c[-21]*100, 2)
        if len(c) >= 15:
            deltas = np.diff(c[-15:])
            g = np.where(deltas>0, deltas, 0); l2 = np.where(deltas<0, -deltas, 0)
            ag = np.mean(g); al2 = np.mean(l2)
            info["rsi"] = round(100 - 100/(1+(ag/al2 if al2 > 0 else 100)), 1)
        if "Volume" in df.columns and len(df) >= 21:
            vol = df["Volume"].values.astype(float)
            avg = np.mean(vol[-21:-1])
            if avg > 0: info["rvol"] = round(float(vol[-1])/avg, 2)
    except: pass
    return info


def tb_fetch_report_data(regime_data=None, sector_data=None, quality_results=None):
    """Fetch all data for the 10-section report."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    try:
        import pytz
        et = pytz.timezone("US/Eastern")
        now_et = datetime.now(et)
        ts = now_et.strftime("%Y-%m-%d %I:%M:%S %p ET")
        dt = now_et.strftime("%Y-%m-%d")
    except:
        ts = now.strftime("%Y-%m-%d %H:%M:%S UTC")
        dt = now.strftime("%Y-%m-%d")
    data = {"timestamp":ts,"date":dt,"macro":{},"sectors":{},"cross_assets":{},
            "key_levels":{},"secular_prices":{},"quality_results":quality_results or [],
            "regime":regime_data or {"regime":"NEUTRAL","spy_rsi":50},
            "sector_strengths":sector_data or {},"etf_liq_in":[],"etf_liq_out":[],
            "options_intel":[],"discovery":{},"congress":[]}
    logger.info("  REPORT: Fetching instrument data...")
    for t, name in [("SPY","S&P 500"),("QQQ","Nasdaq 100"),("IWM","Russell 2000")]:
        info = _r_proc(t); info["name"] = name; data["macro"][t] = info
    for t, name in [("^VIX","VIX"),("DX-Y.NYB","US Dollar"),("^TNX","10Y Yield"),("^IRX","3M Yield")]:
        info = _r_proc(t); info["name"] = name; data["macro"][t] = info
    done = set()
    for sname in QF_SECTOR_NAMES_UNIQUE:
        etf = QF_SECTOR_ETF_MAP.get(sname)
        if not etf or etf in done: continue
        done.add(etf)
        info = _r_proc(etf); info["name"] = sname
        ss = (sector_data or {}).get(sname, {})
        info["rs"] = ss.get("rs", 0); info["strength"] = ss.get("strength", "NEUTRAL")
        data["sectors"][etf] = info
    for t, name in _RPT_CROSS_ASSETS.items():
        info = _r_proc(t); info["name"] = name; data["cross_assets"][t] = info
    for th in _RPT_SECULAR:
        data["secular_prices"][th["etf"]] = _r_proc(th["etf"])
    for t in ["SPY","QQQ","BTC-USD","GLD","TLT"]:
        try:
            df = fetch_daily_csv(t, days=10)
            if df is not None and len(df) >= 5:
                data["key_levels"][t] = {"support":round(float(df["Low"].iloc[-5:].min()),2),
                    "resistance":round(float(df["High"].iloc[-5:].max()),2),
                    "current":round(float(df["Close"].iloc[-1]),2)}
        except: pass
    _thematic = {"SMH":"Semis","CIBR":"Cyber","URA":"Uranium","LIT":"EV","ARKK":"Innovation",
        "TAN":"Solar","ICLN":"Clean Energy","XBI":"Biotech","BITO":"Bitcoin","GDX":"Gold Miners","PAVE":"Infrastructure"}
    for etf, theme in _thematic.items():
        info = _r_proc(etf)
        rv = info.get("rvol"); t5 = info.get("trend_5d") or 0
        if rv and rv > 1.2 and t5 > 0: data["etf_liq_in"].append({"etf":etf,"theme":theme,"rvol":rv,"trend_5d":t5,"price":info.get("price")})
        elif rv and rv < 0.8 and t5 < 0: data["etf_liq_out"].append({"etf":etf,"theme":theme,"rvol":rv,"trend_5d":t5,"price":info.get("price")})

    # ── NEW: Section 9 — Options Intelligence ──
    try:
        opts_list = []
        for r in (quality_results or []):
            oi = r.get("options_intel")
            if oi and isinstance(oi, dict):
                opts_list.append({
                    "ticker": r.get("ticker", "?"),
                    "grade": r.get("quality_grade", "?"),
                    "last_close": r.get("last_close"),
                    "pcr": oi.get("pcr"),
                    "iv_calls": oi.get("iv_calls"),
                    "iv_puts": oi.get("iv_puts"),
                    "unusual": oi.get("unusual_activity", False),
                    "unusual_ratio": oi.get("unusual_ratio"),
                    "unusual_side": oi.get("unusual_side", "N/A"),
                    "max_pain": oi.get("max_pain"),
                    "smart_money": oi.get("smart_money_bias", "N/A"),
                    "options_score": oi.get("options_score", 0),
                })
        data["options_intel"] = sorted(opts_list, key=lambda x: x.get("options_score", 0), reverse=True)
    except Exception as e:
        logger.warning("REPORT: Options intel aggregation failed: %s", e)
        data["options_intel"] = []

    # ── NEW: Section 10 — Dynamic Stock Discovery ──
    try:
        data["discovery"] = fetch_dynamic_discovery()
    except Exception as e:
        logger.warning("REPORT: Dynamic discovery failed: %s", e)
        data["discovery"] = {}
    try:
        data["congress"] = fetch_congress_trades()
    except Exception as e:
        logger.warning("REPORT: Congress trades failed: %s", e)
        data["congress"] = []

    logger.info("  REPORT: Data fetch complete (10-section).")
    return data


def tb_generate_report_html(data):
    """Build the full 10-section HTML report."""
    ts = data.get("timestamp","?"); regime = data.get("regime",{})
    reg = regime.get("regime","NEUTRAL"); spy_rsi = regime.get("spy_rsi",50)
    rc = "#27ae60" if reg=="BULLISH" else ("#e74c3c" if reg=="BEARISH" else "#f39c12")
    css = "<style>body{font-family:'Segoe UI',Arial,sans-serif;margin:0;background:#f5f5f5}.ctr{max-width:900px;margin:0 auto;background:#fff}"
    css += ".hdr{background:linear-gradient(135deg,#1a1a2e,#0f3460);color:#fff;padding:25px 30px;text-align:center}.hdr h1{margin:0;font-size:20px}"
    css += ".sec{padding:18px 22px;border-bottom:1px solid #e0e0e0}.sec h2{color:#1a1a2e;font-size:16px;margin:0 0 10px;border-left:4px solid #0f3460;padding-left:10px}"
    css += "table{width:100%;border-collapse:collapse;font-size:11px;margin:6px 0}th{background:#1a1a2e;color:#fff;padding:6px;text-align:left;font-size:10px;text-transform:uppercase}"
    css += "td{padding:5px;border-bottom:1px solid #eee}tr:nth-child(even){background:#f8f9fa}.g{color:#27ae60;font-weight:bold}.r{color:#e74c3c;font-weight:bold}.y{color:#f39c12;font-weight:bold}"
    css += ".badge{display:inline-block;padding:2px 7px;border-radius:8px;font-size:9px;font-weight:bold;color:#fff}.bg{background:#27ae60}.br{background:#e74c3c}.by{background:#f39c12}.bb{background:#3498db}"
    css += ".ftr{background:#f0f0f0;padding:12px 20px;font-size:9px;color:#888}"
    css += ".highlight-row{background:#fffde7 !important}"
    css += "</style>"
    h = "<html><head>" + css + "</head><body><div class='ctr'>"
    h += "<div class='hdr'><h1>&#128202; Daily Market Intelligence Report</h1>"
    h += "<div style='color:#a0c4ff;font-size:12px;margin-top:4px'>TopBottom Scanner v2.0 — 10-Section Hedge Fund Format</div>"
    h += "<div style='color:#ccc;font-size:10px;margin-top:6px'>%s</div>" % ts
    h += "<div style='margin-top:8px'><span style='display:inline-block;padding:5px 14px;border-radius:5px;font-weight:bold;color:#fff;background:%s'>Regime: %s | SPY RSI: %.1f</span></div></div>" % (rc, reg, spy_rsi)

    # ══════════════════════════════════════════════════════════
    # SECTION 1: Macro Environment
    # ══════════════════════════════════════════════════════════
    h += "<div class='sec'><h2>&#128200; Section 1: Macro Environment</h2>"
    h += "<table><tr><th>Indicator</th><th>Price</th><th>Trend</th><th>5d Chg</th><th>RSI</th><th>Signal</th></tr>"
    for t, info in data.get("macro",{}).items():
        t5 = info.get("trend_5d"); rsi = info.get("rsi")
        h += "<tr><td><b>%s</b> <small>%s</small></td><td>%s</td><td>%s</td><td style='color:%s'>%s%%</td><td>%s</td><td>%s</td></tr>" % (
            t, info.get("name",""), _rv(info.get("price")), _rarrow(t5), _rcolor(t5), _rv(t5,"%+.2f"), _rv(rsi,"%.0f"), _r_rsi_sig(rsi))
    h += "</table></div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 2: Sector Rotation
    # ══════════════════════════════════════════════════════════
    h += "<div class='sec'><h2>&#128260; Section 2: Sector Rotation</h2>"
    h += "<table><tr><th>ETF</th><th>Sector</th><th>Price</th><th>5d</th><th>RS vs SPY</th><th>Rating</th></tr>"
    for etf, info in sorted(data.get("sectors",{}).items(), key=lambda x: x[1].get("rs",0), reverse=True):
        rs = info.get("rs",0); st = info.get("strength","NEUTRAL")
        bc = "bg" if st=="STRONG" else ("br" if st=="WEAK" else "by")
        h += "<tr><td><b>%s</b></td><td>%s</td><td>$%s</td><td style='color:%s'>%s%%</td><td style='color:%s;font-weight:bold'>%s%%</td><td><span class='badge %s'>%s</span></td></tr>" % (
            etf, info.get("name",""), _rv(info.get("price")), _rcolor(info.get("trend_5d")), _rv(info.get("trend_5d"),"%+.2f"), _rcolor(rs), _rv(rs,"%+.3f"), bc, st)
    h += "</table></div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 3: Cross-Asset Dashboard
    # ══════════════════════════════════════════════════════════
    h += "<div class='sec'><h2>&#127757; Section 3: Cross-Asset Dashboard</h2>"
    h += "<table><tr><th>Asset</th><th>Price</th><th>5d Chg</th><th>RSI</th><th>Signal</th></tr>"
    for t, info in data.get("cross_assets",{}).items():
        t5 = info.get("trend_5d")
        h += "<tr><td><b>%s</b> <small>%s</small></td><td>$%s</td><td style='color:%s'>%s%%</td><td>%s</td><td>%s</td></tr>" % (
            t, info.get("name",""), _rv(info.get("price")), _rcolor(t5), _rv(t5,"%+.2f"), _rv(info.get("rsi"),"%.0f"), _r_rsi_sig(info.get("rsi")))
    h += "</table></div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 4: Secular Growth Themes
    # ══════════════════════════════════════════════════════════
    h += "<div class='sec'><h2>&#128640; Section 4: Secular Growth Themes</h2>"
    h += "<table><tr><th>Theme</th><th>Tickers</th><th>ETF</th><th>Price</th><th>5d</th><th>Prob</th></tr>"
    for th in _RPT_SECULAR:
        ei = data["secular_prices"].get(th["etf"],{})
        h += "<tr><td><b>%s</b></td><td>%s</td><td>%s</td><td>$%s</td><td style='color:%s'>%s%%</td><td>%s</td></tr>" % (
            th["theme"],",".join(th["tickers"][:3]),th["etf"],_rv(ei.get("price") if ei else None),
            _rcolor(ei.get("trend_5d") if ei else None),_rv(ei.get("trend_5d") if ei else None,"%+.2f"),th["prob"])
    h += "</table></div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 5: Top Quality Stocks
    # ══════════════════════════════════════════════════════════
    qr = data.get("quality_results",[])
    h += "<div class='sec'><h2>&#128293; Section 5: Top Quality Stocks (7-Layer Filter)</h2>"
    if qr:
        h += "<table><tr><th>Grade</th><th>Ticker</th><th>Score</th><th>Signal</th><th>Sector</th><th>Regime</th><th>Tier</th><th>Price</th></tr>"
        for r in qr[:25]:
            gc = "bg" if r.get("quality_grade") in ("A+","A") else "by"
            h += "<tr><td><span class='badge %s'>%s</span></td><td><b>%s</b></td><td><b>%.1f</b></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>$%s</td></tr>" % (
                gc,r.get("quality_grade","?"),r.get("ticker","?"),r.get("quality_score",0),
                r.get("quality_signal_type","?"),r.get("quality_sector_str","?"),
                r.get("quality_regime","?"),r.get("quality_tier","?"),_rv(r.get("last_close")))
        h += "</table>"
    else: h += "<p style='color:#888'>No quality-filtered stocks available.</p>"
    h += "</div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 6: ETF Liquidity Flows
    # ══════════════════════════════════════════════════════════
    h += "<div class='sec'><h2>&#128176; Section 6: ETF Liquidity Flows</h2>"
    li_in = data.get("etf_liq_in",[]); li_out = data.get("etf_liq_out",[])
    if li_in:
        h += "<h3 style='color:#27ae60'>Money Flowing IN</h3><table><tr><th>ETF</th><th>Theme</th><th>RVOL</th><th>5d</th></tr>"
        for li in sorted(li_in, key=lambda x:x.get("rvol",0), reverse=True)[:10]:
            h += "<tr><td><b>%s</b></td><td>%s</td><td class='g'>%.2fx</td><td class='g'>%s%%</td></tr>" % (li["etf"],li["theme"],li["rvol"],_rv(li.get("trend_5d"),"%+.2f"))
        h += "</table>"
    if li_out:
        h += "<h3 style='color:#e74c3c'>Money Flowing OUT</h3><table><tr><th>ETF</th><th>Theme</th><th>RVOL</th><th>5d</th></tr>"
        for lo in sorted(li_out, key=lambda x:x.get("rvol",1))[:10]:
            h += "<tr><td><b>%s</b></td><td>%s</td><td class='r'>%.2fx</td><td class='r'>%s%%</td></tr>" % (lo["etf"],lo["theme"],lo["rvol"],_rv(lo.get("trend_5d"),"%+.2f"))
        h += "</table>"
    if not li_in and not li_out: h += "<p style='color:#888'>No unusual ETF flows detected.</p>"
    h += "</div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 7: Key Price Levels
    # ══════════════════════════════════════════════════════════
    h += "<div class='sec'><h2>&#127919; Section 7: Key Price Levels</h2>"
    h += "<table><tr><th>Instrument</th><th>Support</th><th>Current</th><th>Resistance</th><th>Position</th></tr>"
    for t, lv in data.get("key_levels",{}).items():
        s2=lv.get("support"); r2=lv.get("resistance"); c2=lv.get("current")
        if s2 and r2 and c2 and (r2-s2)>0:
            pp=(c2-s2)/(r2-s2)*100; pt="Near Support" if pp<30 else ("Near Resistance" if pp>70 else "Mid-Range"); pc="#27ae60" if pp<30 else ("#e74c3c" if pp>70 else "#f39c12")
        else: pt="N/A"; pc="#666"
        h += "<tr><td><b>%s</b></td><td class='g'>$%s</td><td><b>$%s</b></td><td class='r'>$%s</td><td style='color:%s;font-weight:bold'>%s</td></tr>" % (t,_rv(s2),_rv(c2),_rv(r2),pc,pt)
    h += "</table></div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 8: Multi-Timeframe Forecast
    # ══════════════════════════════════════════════════════════
    vix_i = data.get("macro",{}).get("^VIX",{}); vv = vix_i.get("price") or 20
    spy_i = data.get("macro",{}).get("SPY",{}); st20 = spy_i.get("trend_20d") or 0
    ss_data = data.get("sector_strengths",{})
    strong_s = sum(1 for s in ss_data.values() if s.get("strength")=="STRONG")
    weak_s = sum(1 for s in ss_data.values() if s.get("strength")=="WEAK")
    forecasts = []
    if reg=="BULLISH" and vv<20: forecasts.append(("1 Week","Bullish","75%","Momentum + low VIX","#27ae60"))
    elif reg=="BEARISH": forecasts.append(("1 Week","Bearish","65%","Negative momentum","#e74c3c"))
    else: forecasts.append(("1 Week","Neutral","55%","Mixed signals","#f39c12"))
    if st20>2: forecasts.append(("1 Month","Bullish","70%","Strong uptrend","#27ae60"))
    elif st20<-2: forecasts.append(("1 Month","Bearish","65%","Downtrend","#e74c3c"))
    else: forecasts.append(("1 Month","Neutral","50%","No clear trend","#f39c12"))
    if strong_s>weak_s+3: forecasts.append(("3 Months","Bullish","65%","%d strong vs %d weak sectors"%(strong_s,weak_s),"#27ae60"))
    else: forecasts.append(("3 Months","Neutral","50%","Rotation ongoing","#f39c12"))
    forecasts += [("6 Months","Cautiously Bullish","60%","Secular themes support","#27ae60"),("1 Year","Bullish","65%","Innovation cycle","#27ae60"),("3-5 Years","Strongly Bullish","80%","AI + energy transition","#27ae60")]
    h += "<div class='sec'><h2>&#128302; Section 8: Multi-Timeframe Forecast</h2>"
    h += "<table><tr><th>Timeframe</th><th>Direction</th><th>Confidence</th><th>Driver</th></tr>"
    for tf,dr,cf,dv,cl in forecasts:
        h += "<tr><td><b>%s</b></td><td style='color:%s;font-weight:bold'>%s</td><td>%s</td><td>%s</td></tr>" % (tf,cl,dr,cf,dv)
    h += "</table></div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 9: Options Intelligence (NEW)
    # ══════════════════════════════════════════════════════════
    opts = data.get("options_intel", [])
    h += "<div class='sec'><h2>&#128184; Section 9: Options Intelligence (A+/A Stocks)</h2>"
    if opts:
        h += "<table><tr><th>Ticker</th><th>Grade</th><th>Price</th><th>PCR</th><th>IV Calls</th><th>IV Puts</th><th>Unusual?</th><th>Ratio</th><th>Side</th><th>Max Pain</th><th>Smart Money</th><th>Opts Score</th></tr>"
        for o in opts[:30]:
            # PCR coloring
            pcr_val = o.get("pcr")
            if pcr_val is not None:
                pcr_cls = "r" if pcr_val > 1.0 else ("g" if pcr_val < 0.7 else "y")
                pcr_str = "%.2f" % pcr_val
            else:
                pcr_cls = ""; pcr_str = "N/A"
            # IV formatting
            iv_c = ("%.0f%%" % (o.get("iv_calls",0)*100)) if o.get("iv_calls") is not None else "N/A"
            iv_p = ("%.0f%%" % (o.get("iv_puts",0)*100)) if o.get("iv_puts") is not None else "N/A"
            # Unusual
            unusual = o.get("unusual", False)
            unusual_str = "<span class='badge bg'>YES</span>" if unusual else "No"
            # Unusual ratio & side
            ur = ("%.1fx" % o.get("unusual_ratio")) if o.get("unusual_ratio") else "N/A"
            us = o.get("unusual_side", "N/A")
            # Max pain
            mp = ("$%.2f" % o.get("max_pain")) if o.get("max_pain") else "N/A"
            # Smart money
            sm = o.get("smart_money", "N/A")
            if sm == "BULLISH": sm_html = "<span class='g'>BULLISH</span>"
            elif sm == "BEARISH": sm_html = "<span class='r'>BEARISH</span>"
            else: sm_html = "<span class='y'>%s</span>" % sm
            # Grade badge
            gc = "bg" if o.get("grade") in ("A+","A") else ("by" if o.get("grade")=="B" else "br")
            # Score
            os_val = o.get("options_score", 0)
            os_cls = "g" if os_val >= 60 else ("y" if os_val >= 40 else "r")
            h += "<tr>"
            h += "<td><b>%s</b></td>" % o.get("ticker","?")
            h += "<td><span class='badge %s'>%s</span></td>" % (gc, o.get("grade","?"))
            h += "<td>$%s</td>" % _rv(o.get("last_close"))
            h += "<td class='%s'>%s</td>" % (pcr_cls, pcr_str)
            h += "<td>%s</td><td>%s</td>" % (iv_c, iv_p)
            h += "<td>%s</td>" % unusual_str
            h += "<td>%s</td><td>%s</td>" % (ur, us)
            h += "<td>%s</td>" % mp
            h += "<td>%s</td>" % sm_html
            h += "<td class='%s'><b>%d</b></td>" % (os_cls, os_val)
            h += "</tr>"
        h += "</table>"
        # Summary stats
        bulls = sum(1 for o in opts if o.get("smart_money") == "BULLISH")
        bears = sum(1 for o in opts if o.get("smart_money") == "BEARISH")
        unusuals = sum(1 for o in opts if o.get("unusual"))
        h += "<div style='margin-top:8px;font-size:11px;color:#555'>"
        h += "<b>Summary:</b> %d stocks analyzed | " % len(opts)
        h += "<span class='g'>%d Bullish</span> | " % bulls
        h += "<span class='r'>%d Bearish</span> | " % bears
        h += "<span style='color:#e67e22'>%d Unusual Activity</span>" % unusuals
        h += "</div>"
    else:
        h += "<p style='color:#888'>No options intelligence data available. Options data is enriched for A+/A graded stocks only.</p>"
    h += "</div>"

    # ══════════════════════════════════════════════════════════
    # SECTION 10: Dynamic Stock Discovery (NEW)
    # ══════════════════════════════════════════════════════════
    disc = data.get("discovery", {})
    congress = data.get("congress", [])
    h += "<div class='sec'><h2>&#128269; Section 10: Dynamic Stock Discovery (Free Sources)</h2>"

    # 10a: Finviz Screener Signals
    signal_labels = {
        "unusual_volume": "Unusual Volume",
        "new_highs": "New 52-Week Highs",
        "most_active": "Most Active",
        "oversold": "Oversold (RSI)",
        "channel_up": "Channel Up (Trend)",
    }
    has_disc = False
    if disc and isinstance(disc, dict):
        for key in signal_labels:
            if disc.get(key):
                has_disc = True
                break
    if has_disc:
        h += "<h3 style='color:#0f3460'>&#128240; Finviz Screener Signals</h3>"
        h += "<table><tr><th>Signal Type</th><th>Count</th><th>Tickers (Top 15)</th></tr>"
        for key, label in signal_labels.items():
            tickers = disc.get(key, [])
            if isinstance(tickers, list) and tickers:
                display = ", ".join(tickers[:15])
                extra = " ..." if len(tickers) > 15 else ""
                h += "<tr><td><b>%s</b></td><td><span class='badge bb'>%d</span></td><td>%s%s</td></tr>" % (label, len(tickers), display, extra)
            else:
                h += "<tr><td>%s</td><td>0</td><td style='color:#aaa'>None detected</td></tr>" % label
        h += "</table>"
    else:
        h += "<p style='color:#888'>Finviz screener data not available for this run.</p>"

    # 10b: Congress Trades
    h += "<h3 style='color:#0f3460;margin-top:14px'>&#127963;&#65039; Recent Congress Purchases (30 Days)</h3>"
    if congress and isinstance(congress, list) and len(congress) > 0:
        # congress might be list of tickers or list of dicts
        if isinstance(congress[0], dict):
            congress_tickers = [c.get("ticker", c.get("symbol", "?")) for c in congress]
        else:
            congress_tickers = [str(c) for c in congress]
        display_ct = ", ".join(congress_tickers[:20])
        extra_ct = " ..." if len(congress_tickers) > 20 else ""
        h += "<p><b>%d tickers detected:</b> %s%s</p>" % (len(congress_tickers), display_ct, extra_ct)
    else:
        h += "<p style='color:#888'>No recent congressional trade data available.</p>"
    h += "</div>"

    # ══════════════════════════════════════════════════════════
    # FOOTER
    # ══════════════════════════════════════════════════════════
    h += "<div class='ftr'><p><b>Source:</b> TopBottom Scanner v2.0 | 10-Section Daily Intelligence + 7-Layer Quality Filter + Options Intel + Dynamic Discovery</p>"
    h += "<p><b>Disclaimer:</b> For informational purposes only. Not financial advice.</p></div>"
    h += "</div></body></html>"
    return h

def tb_send_email(subject, html_content):
    """Send HTML email using configured Gmail credentials."""
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD or not _QF_RECIPIENTS:
        logger.warning("  REPORT EMAIL: Not configured (missing EMAIL_ADDRESS/PASSWORD/RECIPIENTS)")
        return False
    try:
        msg = _MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = ", ".join(_QF_RECIPIENTS)
        msg.attach(_MIMEText(html_content, "html"))
        with _smtplib.SMTP_SSL(_QF_SMTP_SERVER, 465) as server:
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, _QF_RECIPIENTS, msg.as_string())
        logger.info("  REPORT EMAIL: Sent '%s' to %s", subject, ", ".join(_QF_RECIPIENTS))
        return True
    except Exception as e:
        logger.error("  REPORT EMAIL: Failed: %s", e)
        return False



_INSTITUTIONAL_PROMPT = """
Act as an elite institutional macro + thematic investment research desk specializing in:
* secular demand-transition analysis
* capital-cycle detection
* future bottleneck discovery
* institutional accumulation analysis
* liquidity-rotation analysis
* narrative-transition detection
* infrastructure dependency analysis
* operating leverage inflection analysis
* technical accumulation structure analysis
* long-duration thematic investing
* strategic industry evolution analysis
* hidden supplier discovery
* future infrastructure mapping
* asymmetric pre-rerating opportunity discovery

PRIMARY OBJECTIVE: Identify future institutional winners BEFORE mainstream discovery.

The objective is NOT to find current hype, already-crowded momentum trades, already-exploded secular winners, narrative-saturated sectors, or euphoric institutional positioning.

The objective IS to identify: future infrastructure bottlenecks, future strategic dependencies, emerging demand-transition beneficiaries, hidden suppliers, overlooked enabling technologies, second-order secular-growth winners, sectors BEFORE Wall Street fully reprices them.

DEMAND-TRANSITION DETECTION: Identify industries where structural demand behavior is changing, replacement cycles are beginning, infrastructure requirements are increasing, compute/energy/data/automation intensity is rising, supply chains are becoming constrained, enterprise spending priorities are shifting, government incentives are increasing, future capacity shortages may emerge.

CAPITAL-CYCLE ANALYSIS: Prioritize industries where years of underinvestment occurred, future supply shortages may emerge, barriers to entry are increasing, demand elasticity is underestimated, operating leverage could expand dramatically.

TECHNICAL ACCUMULATION: Prioritize stocks showing 6-36 month accumulation structures, volatility compression, quiet institutional accumulation, improving relative strength, constructive Stage-1 bases, early Stage-2 transitions, breakout proximity WITHOUT euphoric extension. Reject vertical/parabolic charts, crowded momentum.

FOR EACH STOCK PROVIDE: Company + ticker, Sector/theme, Why future demand may accelerate, Why this company may become strategically important, Current chart stage, Technical accumulation quality, Capital-cycle positioning, Institutional accumulation signs, Revenue/earnings inflection potential, Operating leverage potential, Why this is STILL EARLY, Future rerating catalysts, Probability of future institutional crowding, Why this is NOT already too late.

FINAL OUTPUT: Top future bottleneck providers, Top pre-rerating opportunities, Emerging sectors BEFORE mainstream narrative, Hidden infrastructure beneficiaries, Most attractive accumulation structures, Most underappreciated future-capex beneficiaries, Stocks with highest long-duration asymmetric upside, Stocks most likely to experience future institutional rerating.

Be extremely critical. Reject hype. Prioritize future demand transitions, infrastructure bottlenecks, early institutional accumulation, hidden strategic importance, capital-cycle acceleration, pre-rerating setups, long-duration asymmetry.
"""


def tb_generate_institutional_research_email(data):
    """Build the Institutional Research email: Prompt + Live Scanner Data + Options + Discovery for AI analysis."""
    ts = data.get("timestamp", "?")
    regime = data.get("regime", {})
    reg = regime.get("regime", "NEUTRAL")
    spy_rsi = regime.get("spy_rsi", 50)

    h = "<html><head><style>"
    h += "body{font-family:'Segoe UI',Arial,sans-serif;margin:0;background:#f5f5f5}"
    h += ".ctr{max-width:950px;margin:0 auto;background:#fff}"
    h += ".hdr{background:linear-gradient(135deg,#0a0a1a,#1a1a3e,#0a2a5e);color:#fff;padding:25px 30px;text-align:center}"
    h += ".hdr h1{margin:0;font-size:20px}"
    h += ".sec{padding:18px 22px;border-bottom:1px solid #e0e0e0}"
    h += ".sec h2{color:#1a1a2e;font-size:16px;margin:0 0 10px;border-left:4px solid #0a2a5e;padding-left:10px}"
    h += ".prompt-box{background:#f8f9fc;border:2px solid #c3d0e0;border-radius:8px;padding:18px;font-size:11px;color:#333;white-space:pre-wrap;max-height:400px;overflow-y:auto;margin:10px 0}"
    h += ".data-box{background:#0a0a1a;color:#e6f1ff;border-radius:8px;padding:18px;font-family:'Courier New',monospace;font-size:12px;white-space:pre-wrap;margin:10px 0}"
    h += ".tip{background:#fffbe6;border:1px solid #ffe58f;padding:12px;border-radius:6px;font-size:12px;margin:10px 0}"
    h += ".ftr{background:#f0f0f0;padding:12px 20px;font-size:9px;color:#888}"
    h += "</style></head><body><div class='ctr'>"

    # HEADER
    h += "<div class='hdr'><h1>&#128300; Institutional Research Scan</h1>"
    h += "<div style='color:#a0c4ff;font-size:13px;margin-top:6px'>Prompt + Live Scanner Data + Options Intel + Discovery — Ready for AI Analysis</div>"
    h += "<div style='color:#ccc;font-size:11px;margin-top:6px'>%s</div></div>" % ts

    # TIP
    h += "<div class='sec'><div class='tip'>"
    h += "<strong>&#128161; How to use this report:</strong><br>"
    h += "1. Select ALL text below (Ctrl+A) or copy the prompt + data sections<br>"
    h += "2. Paste into ChatGPT, Copilot, or Claude<br>"
    h += "3. The AI will analyze the REAL data below using the institutional research framework<br>"
    h += "4. You get deep analysis backed by actual market data — no hallucinated numbers"
    h += "</div></div>"

    # SECTION 1: THE PROMPT
    h += "<div class='sec'><h2>&#128203; Part 1: Institutional Research Prompt</h2>"
    h += "<div class='prompt-box'>%s</div></div>" % _INSTITUTIONAL_PROMPT.replace("<", "&lt;").replace(">", "&gt;")

    # SECTION 2: LIVE DATA
    h += "<div class='sec'><h2>&#128202; Part 2: Today's Live Scanner Data</h2>"
    h += "<p style='font-size:12px;color:#666'>Use this data to complete the analysis above. All data is real-time from yfinance.</p>"

    data_text = "=" * 60 + "\n"
    data_text += "TODAY'S LIVE MARKET DATA — %s\n" % ts
    data_text += "=" * 60 + "\n\n"

    # Market Regime
    data_text += "MARKET REGIME: %s | SPY RSI: %.1f\n" % (reg, spy_rsi)
    vix = data.get("macro", {}).get("^VIX", {})
    data_text += "VIX: %s | " % _rv(vix.get("price"), "%.1f")
    dxy = data.get("macro", {}).get("DX-Y.NYB", {})
    data_text += "DXY: %s | " % _rv(dxy.get("price"), "%.2f")
    tnx = data.get("macro", {}).get("^TNX", {})
    data_text += "10Y Yield: %s\n\n" % _rv(tnx.get("price"), "%.2f%%")

    # Macro
    data_text += "--- MACRO INDICES ---\n"
    for t, info in data.get("macro", {}).items():
        data_text += "  %-12s Price=%-10s 5d=%s%% RSI=%s Signal=%s\n" % (
            t, _rv(info.get("price")), _rv(info.get("trend_5d"), "%+.2f"),
            _rv(info.get("rsi"), "%.0f"), _r_rsi_sig(info.get("rsi")))
    data_text += "\n"

    # Sectors
    data_text += "--- SECTOR ROTATION (sorted by RS vs SPY) ---\n"
    for etf, info in sorted(data.get("sectors", {}).items(), key=lambda x: x[1].get("rs", 0), reverse=True):
        data_text += "  %-5s %-20s RS=%s%% Rating=%s Price=$%s\n" % (
            etf, info.get("name", ""), _rv(info.get("rs"), "%+.3f"),
            info.get("strength", "?"), _rv(info.get("price")))
    data_text += "\n"

    # Cross Assets
    data_text += "--- CROSS-ASSET DASHBOARD ---\n"
    for t, info in data.get("cross_assets", {}).items():
        data_text += "  %-12s %-12s Price=$%-10s 5d=%s%% RSI=%s\n" % (
            t, info.get("name", ""), _rv(info.get("price")),
            _rv(info.get("trend_5d"), "%+.2f"), _rv(info.get("rsi"), "%.0f"))
    data_text += "\n"

    # Quality Results
    data_text += "--- TOP QUALITY STOCKS (7-LAYER FILTER) ---\n"
    for r in data.get("quality_results", [])[:25]:
        data_text += "  [%s] %-6s Score=%.1f Signal=%-7s Sector=%-7s Tier=%s Price=$%s\n" % (
            r.get("quality_grade", "?"), r.get("ticker", "?"), r.get("quality_score", 0),
            r.get("quality_signal_type", "?"), r.get("quality_sector_str", "?"),
            r.get("quality_tier", "?"), _rv(r.get("last_close")))
    data_text += "\n"

    # ── NEW: Options Intelligence Context ──
    opts = data.get("options_intel", [])
    if opts:
        data_text += "--- OPTIONS INTELLIGENCE (A+/A STOCKS) ---\n"
        data_text += "  %-6s %-5s %-6s %-8s %-8s %-8s %-8s %-10s %-12s %-6s\n" % (
            "Ticker", "Grade", "PCR", "IV_Call", "IV_Put", "Unusual", "Ratio", "Max Pain", "SmartMoney", "Score")
        for o in opts[:20]:
            pcr_str = ("%.2f" % o["pcr"]) if o.get("pcr") is not None else "N/A"
            iv_c = ("%.0f%%" % (o.get("iv_calls",0)*100)) if o.get("iv_calls") is not None else "N/A"
            iv_p = ("%.0f%%" % (o.get("iv_puts",0)*100)) if o.get("iv_puts") is not None else "N/A"
            unusual_str = "YES" if o.get("unusual") else "No"
            ur = ("%.1fx" % o["unusual_ratio"]) if o.get("unusual_ratio") else "N/A"
            mp = ("$%.0f" % o["max_pain"]) if o.get("max_pain") else "N/A"
            sm = o.get("smart_money", "N/A")
            data_text += "  %-6s %-5s %-6s %-8s %-8s %-8s %-8s %-10s %-12s %-6d\n" % (
                o.get("ticker","?"), o.get("grade","?"), pcr_str, iv_c, iv_p,
                unusual_str, ur, mp, sm, o.get("options_score",0))
        data_text += "\n"

    # ── NEW: Dynamic Discovery Context ──
    disc = data.get("discovery", {})
    congress = data.get("congress", [])
    if disc or congress:
        data_text += "--- DYNAMIC STOCK DISCOVERY ---\n"
        signal_labels = {
            "unusual_volume": "Unusual Volume",
            "new_highs": "New 52-Week Highs",
            "most_active": "Most Active",
            "oversold": "Oversold (RSI)",
            "channel_up": "Channel Up (Trend)",
        }
        for key, label in signal_labels.items():
            tickers = disc.get(key, []) if isinstance(disc, dict) else []
            if tickers:
                data_text += "  %s: %s\n" % (label, ", ".join(tickers[:15]))
        if congress:
            if isinstance(congress[0], dict):
                ct = [c.get("ticker", c.get("symbol", "?")) for c in congress]
            else:
                ct = [str(c) for c in congress]
            data_text += "  Congress Purchases (30d): %s\n" % ", ".join(ct[:20])
        data_text += "\n"

    # Key Levels
    data_text += "--- KEY PRICE LEVELS ---\n"
    for t, lv in data.get("key_levels", {}).items():
        data_text += "  %-10s Support=$%-10s Current=$%-10s Resistance=$%-10s\n" % (
            t, _rv(lv.get("support")), _rv(lv.get("current")), _rv(lv.get("resistance")))

    h += "<div class='data-box'>%s</div></div>" % data_text

    # FOOTER
    h += "<div class='ftr'><p><b>Source:</b> TopBottom Scanner v2.0 | 10-Section Institutional Research + Options + Discovery</p>"
    h += "<p><b>Disclaimer:</b> For informational purposes only. Not financial advice.</p></div>"
    h += "</div></body></html>"
    return h

def tb_check_and_send_reports(regime_data=None, sector_data=None, quality_results=None):
    """Non-blocking report scheduler. Handles 11AM, 2PM, 3PM, 4PM reports."""
    global report_11am_sent, report_2pm_sent, report_3pm_sent, report_4pm_sent
    # Reliable ET time without requiring pytz
    try:
        import calendar as _cal
        _utc_now = datetime.now(timezone.utc)
        _month = _utc_now.month
        if 3 < _month < 11:
            _is_dst = True
        elif _month == 3:
            _ss = 14 - _cal.weekday(_utc_now.year, 3, 1)
            if _ss <= 0: _ss += 7
            _is_dst = _utc_now.day >= _ss
        elif _month == 11:
            _fs = 7 - _cal.weekday(_utc_now.year, 11, 1)
            if _fs <= 0: _fs += 7
            _is_dst = _utc_now.day < _fs
        else:
            _is_dst = False
        _offset = timedelta(hours=-4 if _is_dst else -5)
        now = _utc_now + _offset
    except Exception:
        now = datetime.now(timezone.utc)

    # 11:00 AM — Institutional Research Scan
    if not report_11am_sent and now.hour >= REPORT_11AM_HOUR:
        logger.info("  REPORT: 11 AM trigger — Institutional Research Scan...")
        try:
            rdata = tb_fetch_report_data(regime_data, sector_data, quality_results)
            html = tb_generate_institutional_research_email(rdata)
            tb_send_email("Institutional Research Scan — %s 11:00 AM" % rdata["date"], html)
            report_11am_sent = True
        except Exception as e:
            logger.exception("  REPORT: 11 AM failed: %s", e)

    # 2:00 PM — Mid-Day Market Intelligence
    if not report_2pm_sent and now.hour >= REPORT_2PM_HOUR:
        logger.info("  REPORT: 2 PM trigger — Mid-Day Intelligence...")
        try:
            rdata = tb_fetch_report_data(regime_data, sector_data, quality_results)
            html = tb_generate_report_html(rdata)
            tb_send_email("Mid-Day Market Intelligence — %s" % rdata["date"], html)
            report_2pm_sent = True
        except Exception as e:
            logger.exception("  REPORT: 2 PM failed: %s", e)

    # 3:00 PM — Pre-Close Institutional Research
    if not report_3pm_sent and now.hour >= REPORT_3PM_HOUR:
        logger.info("  REPORT: 3 PM trigger — Pre-Close Institutional Research...")
        try:
            rdata = tb_fetch_report_data(regime_data, sector_data, quality_results)
            html = tb_generate_institutional_research_email(rdata)
            tb_send_email("Pre-Close Institutional Research — %s 3:00 PM" % rdata["date"], html)
            report_3pm_sent = True
        except Exception as e:
            logger.exception("  REPORT: 3 PM failed: %s", e)

    # 4:00 PM — End-of-Day Report
    if not report_4pm_sent and now.hour >= REPORT_4PM_HOUR:
        logger.info("  REPORT: 4 PM trigger — EOD Report...")
        try:
            rdata = tb_fetch_report_data(regime_data, sector_data, quality_results)
            html = tb_generate_report_html(rdata)
            tb_send_email("End-of-Day Market Report — %s" % rdata["date"], html)
            report_4pm_sent = True
        except Exception as e:
            logger.exception("  REPORT: 4 PM failed: %s", e)



# ═══════════════════════════════════════════════════════════════════════════
# OPTIONS INTELLIGENCE — PCR, IV, Unusual Activity, Max Pain, Smart Money
# Runs ONLY on A+/A quality stocks to stay within yfinance rate limits
# ═══════════════════════════════════════════════════════════════════════════

def opt_fetch_options_intel(ticker):
    """Fetch options intelligence for a single ticker using yfinance."""
    intel = {"pcr":None,"iv_avg_call":None,"iv_avg_put":None,"iv_rank_label":None,
             "unusual_activity":False,"unusual_ratio":None,"unusual_side":None,
             "max_pain":None,"smart_money_bias":None,"options_score":None}
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        exps = tk.options
        if not exps: return intel
        chain = tk.option_chain(exps[0])
        calls = chain.calls; puts = chain.puts
        if calls.empty and puts.empty: return intel

        # 1. PUT/CALL RATIO
        call_vol = float(calls["volume"].fillna(0).sum()) if "volume" in calls.columns else 0
        put_vol = float(puts["volume"].fillna(0).sum()) if "volume" in puts.columns else 0
        if call_vol > 0: intel["pcr"] = round(put_vol / call_vol, 2)

        # 2. IMPLIED VOLATILITY
        if "impliedVolatility" in calls.columns:
            call_iv = calls["impliedVolatility"].dropna()
            put_iv = puts["impliedVolatility"].dropna() if "impliedVolatility" in puts.columns else pd.Series(dtype=float)
            if not call_iv.empty: intel["iv_avg_call"] = round(float(call_iv.mean())*100, 1)
            if not put_iv.empty: intel["iv_avg_put"] = round(float(put_iv.mean())*100, 1)
            avg_iv = intel["iv_avg_call"] or 0
            intel["iv_rank_label"] = "VERY HIGH" if avg_iv>80 else ("HIGH" if avg_iv>50 else ("MODERATE" if avg_iv>30 else "LOW"))

        # 3. UNUSUAL ACTIVITY (Volume vs Open Interest)
        if "volume" in calls.columns and "openInterest" in calls.columns:
            c_vol = float(calls["volume"].fillna(0).sum()); c_oi = float(calls["openInterest"].fillna(0).sum())
            p_vol = float(puts["volume"].fillna(0).sum()); p_oi = float(puts["openInterest"].fillna(0).sum())
            c_ratio = c_vol/c_oi if c_oi > 0 else 0; p_ratio = p_vol/p_oi if p_oi > 0 else 0
            if c_ratio > 3.0:
                intel["unusual_activity"]=True; intel["unusual_ratio"]=round(c_ratio,1); intel["unusual_side"]="CALLS"
            elif p_ratio > 3.0:
                intel["unusual_activity"]=True; intel["unusual_ratio"]=round(p_ratio,1); intel["unusual_side"]="PUTS"

        # 4. MAX PAIN
        try:
            if "openInterest" in calls.columns and "strike" in calls.columns:
                strikes = sorted(set(calls["strike"].dropna().tolist() + puts["strike"].dropna().tolist()))
                if len(strikes) > 2:
                    min_pain = float("inf"); pain_strike = strikes[len(strikes)//2]
                    for sk in strikes:
                        total = 0
                        c_itm = calls[calls["strike"] <= sk]
                        p_itm = puts[puts["strike"] >= sk]
                        if not c_itm.empty: total += float(((sk - c_itm["strike"]) * c_itm["openInterest"].fillna(0)).sum())
                        if not p_itm.empty: total += float(((p_itm["strike"] - sk) * p_itm["openInterest"].fillna(0)).sum())
                        if total < min_pain: min_pain = total; pain_strike = sk
                    intel["max_pain"] = round(float(pain_strike), 2)
        except: pass

        # 5. SMART MONEY BIAS
        pcr = intel["pcr"]; ua = intel["unusual_activity"]; us = intel["unusual_side"]
        if pcr is not None:
            if pcr > 1.5: intel["smart_money_bias"] = "BEARISH (heavy puts)"
            elif pcr > 1.2 and ua and us == "PUTS": intel["smart_money_bias"] = "BEARISH (unusual puts)"
            elif pcr < 0.5: intel["smart_money_bias"] = "BULLISH (heavy calls)"
            elif pcr < 0.8 and ua and us == "CALLS": intel["smart_money_bias"] = "BULLISH (unusual calls)"
            elif ua and us == "CALLS": intel["smart_money_bias"] = "BULLISH (call sweep)"
            elif ua and us == "PUTS": intel["smart_money_bias"] = "BEARISH (put sweep)"
            else: intel["smart_money_bias"] = "NEUTRAL"

        # 6. OPTIONS SCORE (0-100)
        os2 = 50
        if pcr is not None:
            if pcr > 1.5: os2 += 20
            elif pcr > 1.0: os2 += 10
            elif pcr < 0.5: os2 -= 15
        if ua and us == "CALLS": os2 += 15
        elif ua and us == "PUTS": os2 -= 10
        if (intel["iv_avg_call"] or 0) > 60: os2 += 10
        intel["options_score"] = max(0, min(100, os2))

    except Exception as e:
        logger.warning("  OPTIONS: Failed for %s: %s", ticker, e)
    return intel


def opt_enrich_results(results):
    """Add options intelligence to A+/A quality stocks only (rate-limit friendly)."""
    enriched = 0
    for r in results:
        if r.get("quality_grade", "F") not in ("A+", "A"):
            r["options_intel"] = None; continue
        ticker = r.get("ticker", "")
        if "-USD" in ticker or "=F" in ticker or "^" in ticker:
            r["options_intel"] = None; continue
        try:
            logger.info("  OPTIONS: Fetching %s (Grade %s)...", ticker, r.get("quality_grade"))
            intel = opt_fetch_options_intel(ticker)
            r["options_intel"] = intel; enriched += 1
            if intel.get("pcr") is not None:
                logger.info("    %s PCR=%.2f IV=%.1f%% Unusual=%s Bias=%s MaxPain=$%s",
                    ticker, intel["pcr"], intel.get("iv_avg_call") or 0,
                    ("%s %.1fx %s" % (intel["unusual_activity"], intel.get("unusual_ratio") or 0, intel.get("unusual_side") or "-")),
                    intel.get("smart_money_bias","?"), intel.get("max_pain","?"))
            time.sleep(0.5)
        except Exception as e:
            r["options_intel"] = None; logger.warning("  OPTIONS: Error %s: %s", ticker, e)
    logger.info("  OPTIONS: Enriched %d stocks with options data", enriched)
    return results


# ═══════════════════════════════════════════════════════════════════════════
# DYNAMIC STOCK DISCOVERY — Finviz Screeners + Congress Trades (FREE)
# These fetch the latest hot/active/unusual stocks daily — never hardcoded
# ═══════════════════════════════════════════════════════════════════════════

def fetch_finviz_signal(signal_id, max_tickers=50):
    """Fetch tickers from a Finviz screener signal page (free, no API key needed)."""
    tickers = []
    try:
        url = f"https://finviz.com/screener.ashx?v=111&s={signal_id}&o=-marketcap"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", "ignore")
        for df in pd.read_html(StringIO(html)):
            cols = [str(c).lower() for c in df.columns]
            if "ticker" in cols:
                raw = df[df.columns[cols.index("ticker")]].astype(str).str.strip().str.upper().tolist()
                valid = [t for t in raw if t != "TICKER" and 1 <= len(t) <= 6 and t.isalpha()]
                tickers.extend(valid[:max_tickers])
                break
    except Exception as e:
        logger.warning("  FINVIZ SIGNAL %s failed: %s", signal_id, e)
    return tickers


def fetch_dynamic_discovery():
    """Fetch hot stocks from multiple free Finviz screener signals."""
    discovery = {}
    signal_map = {
        "unusual_volume": "ta_unusualvolume",
        "new_highs": "ta_newhigh",
        "most_active": "ta_mostactive",
        "oversold": "ta_oversold",
        "channel_up": "ta_channelup",
    }
    for key, sig_id in signal_map.items():
        logger.info("  DISCOVERY: Fetching %s from Finviz...", key)
        tickers = fetch_finviz_signal(sig_id, max_tickers=30)
        discovery[key] = tickers
        logger.info("    Got %d tickers for %s", len(tickers), key)
        time.sleep(1.0 + random.random())
    return discovery


def fetch_congress_trades():
    """Fetch recent congress stock trades from free public dataset."""
    tickers = []
    try:
        url = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8", "ignore"))
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        seen = set()
        for trade in data:
            if trade.get("type", "").lower() == "purchase" and trade.get("transaction_date", "") >= cutoff:
                ticker = trade.get("ticker", "").strip().upper()
                if ticker and len(ticker) <= 6 and "--" not in ticker and ticker not in seen:
                    tickers.append(ticker)
                    seen.add(ticker)
        logger.info("  CONGRESS: Found %d recent purchase tickers (last 30 days)", len(tickers))
    except Exception as e:
        logger.warning("  CONGRESS TRADES fetch failed: %s", e)
    return tickers[:50]


    watchlist_tickers = set(t for group in watchmap_final.values() for t in group)
    favorite_tickers = set(t for group in favmap_final.values() for t in group)
    all_entry_data = watchdata_final.copy()
    all_entry_data.update(favdata_final) 

    inbox_map = parse_inbox()
    inbox_tickers = set(inbox_map.keys())
    all_entry_data.update({t: {'price': v['price'], 'date': v['date']} for t, v in inbox_map.items()})

    universe_map = build_universe()
    all_universe_tickers = set(t for group in universe_map.values() for t in group)

    if 'watchlist_tickers' not in locals(): watchlist_tickers = set()
    if 'favorite_tickers' not in locals(): favorite_tickers = set()
    if 'all_entry_data' not in locals(): all_entry_data = {}

    tickers_to_scan = all_universe_tickers.union(watchlist_tickers).union(favorite_tickers).union(inbox_tickers)
    logger.info("Total unique tickers to scan (including Inbox): %d", len(tickers_to_scan))

    q = queue.Queue()
    results = []
    for t in tickers_to_scan: q.put(t)
    
    def worker():
        while True:
            try:
                ticker = q.get_nowait()
            except queue.Empty:
                break
            try:
                entry_data = all_entry_data.get(ticker)
                result = analyze_ticker(ticker, entry_data=entry_data)
                if result:
                    tags = result.get('tags', [])
                    if tags or (ticker in watchlist_tickers) or (ticker in favorite_tickers) or (ticker in inbox_tickers):
                        results.append(result)
                    if "BUYING_OPPORTUNITY" in result.get("tags", []):
                        logger.info(f"Buying opportunity found: {ticker}")
            except Exception as e:
                logger.error(f"Worker error for {ticker}: {e}")
            finally:
                q.task_done()

    threads = []
    for _ in range(THREADS):
        t = threading.Thread(target=worker, daemon=True)
        t.start(); threads.append(t)
    q.join()

    # ═══════════════════════════════════════════════════════
    # v2.0 — 7-LAYER QUALITY FILTER
    # ═══════════════════════════════════════════════════════
    logger.info("=" * 65)
    logger.info("  Applying 7-Layer Quality Filter...")
    logger.info("=" * 65)
    _qf_regime = qf_get_market_regime()
    _qf_sectors = qf_get_sector_strengths()
    logger.info("  Market Regime: %s | SPY RSI: %.1f", _qf_regime.get("regime","?"), _qf_regime.get("spy_rsi", 0))
    for _sec_name, _sec_info in _qf_sectors.items():
        logger.info("    %s: %s (RS: %+.2f%%)", _sec_name, _sec_info.get("strength","?"), _sec_info.get("rs", 0))
    results = qf_apply_quality_filter(results, _qf_regime, _qf_sectors)
    _qf_passed = [r for r in results if r.get("quality_grade","F") in ("A+", "A")]
    _qf_bgrade = [r for r in results if r.get("quality_grade","F") == "B"]
    logger.info("  Quality: %d A+/A | %d B | %d total", len(_qf_passed), len(_qf_bgrade), len(results))

    # v2.0: Inject quality grade as filter tags (creates clickable buttons in filter bar)
    for _r in results:
        _qg = _r.get('quality_grade', 'F')
        if _qg == 'A+':
            _r['tags'] = sorted(set(_r.get('tags', []) + ['QUALITY_A_PLUS', '🏆 Quality A+']))
        elif _qg == 'A':
            _r['tags'] = sorted(set(_r.get('tags', []) + ['QUALITY_A', '🏆 Quality A']))
        elif _qg == 'B':
            _r['tags'] = sorted(set(_r.get('tags', []) + ['QUALITY_B', '🏆 Quality B']))

    # v2.0: Options Intelligence (A+/A stocks only — rate-limit friendly)
    logger.info("  Enriching A+/A stocks with options data...")
    results = opt_enrich_results(results)

    # v2.0: Inject options data as filter tags
    for _r in results:
        oi = _r.get('options_intel')
        if oi and oi.get('pcr') is not None:
            bias = oi.get('smart_money_bias', 'NEUTRAL') or 'NEUTRAL'
            if 'BULLISH' in bias:
                _r['tags'] = sorted(set(_r.get('tags', []) + ['OPTIONS_BULLISH', '📈 Opts Bullish']))
            elif 'BEARISH' in bias:
                _r['tags'] = sorted(set(_r.get('tags', []) + ['OPTIONS_BEARISH', '📉 Opts Bearish']))
            if oi.get('unusual_activity'):
                _r['tags'] = sorted(set(_r.get('tags', []) + ['UNUSUAL_OPTIONS', '⚡ Unusual Options']))


    
    groups_univ: Dict[str, List[Dict[str, Any]]] = {k:[] for k in universe_map.keys()}
    groups_wl: Dict[str, List[Dict[str, Any]]] = {k:[] for k in watchmap_final.keys()}
    groups_fav: Dict[str, List[Dict[str, Any]]] = {k:[] for k in favmap_final.keys()}
    groups_inbox: Dict[str, List[Dict[str, Any]]] = {"Recent Inbox Alerts": []} 
    groups_sector: Dict[str, List[Dict[str, Any]]] = {}

    # ----------------------------
    # ✅ DUPLICATE-SAFE APPENDER
    # ----------------------------
    def _append_unique(grouped_dict, group_name, record):
        ticker = record.get("ticker")
        if not ticker:
            return
        for r in grouped_dict[group_name]:
            if r.get("ticker") == ticker:
                return
        grouped_dict[group_name].append(record)

    # ----------------------------
    # ✅ GROUPING (FIXED)
    # ----------------------------
    for r in results:
        ticker = r['ticker']
        for cat, tickers in universe_map.items():
            if ticker in tickers and r.get('tags') and r.get('quality_grade', 'F') in ('A+', 'A', 'B'):
                _append_unique(groups_univ, cat, r)

        for cat, tickers in watchmap_final.items():
            if ticker in tickers:
                _append_unique(groups_wl, cat, r)

        for cat, tickers in favmap_final.items():
            if ticker in tickers:
                _append_unique(groups_fav, cat, r)

        if ticker in inbox_tickers and r.get('quality_grade', 'F') in ('A+', 'A', 'B'):
            _append_unique(groups_inbox, "Recent Inbox Alerts", r)

        if r.get('tags'):
            sector_name = r.get('sector') or "Other"
            if sector_name not in groups_sector:
                groups_sector[sector_name] = []
            _append_unique(groups_sector, sector_name, r)
    
    flagged_results = [r for r in results if r.get('tags')]
    if flagged_results:
        df_out = pd.DataFrame(flagged_results)
        df_out.sort_values(by=['ticker'], inplace=True)
        try: 
            if ENABLE_CSV_EXPORT: df_out.to_csv(OUT_CSV, index=False)
        except: pass

    # ---------------------------------------------------------
    # EMBEDDED JS TEMPLATE (REPAIRED)
    # ---------------------------------------------------------
    report_js_template = """
    console.log("TopBottom Report Loaded");
    
    // --- STATE ---
    window.currentFavorites = window.initialFavorites || [];
    window.aiScoreThreshold = 0;
    window.activeFilters = new Set();
    window.stackMode = 'AND';
    window._chartData = window._tb_chart_payloads || {};

    // --- TOGGLE TABLE ---
    window.toggleTable = function(divId) {
        var x = document.getElementById(divId + "_table");
        if (x.style.display === "none") {
            x.style.display = "block";
        } else {
            x.style.display = "none";
        }
    };
    
    // --- FILTER BUTTON LOGIC ---
    window.toggleTagButton = function(btn, tag) {
        if (tag === 'ALL') {
            window.activeFilters.clear();
            document.querySelectorAll('.filter-area .btn').forEach(b => b.classList.remove('active'));
        } else {
            if (window.activeFilters.has(tag)) {
                window.activeFilters.delete(tag);
                btn.classList.remove('active');
            } else {
                window.activeFilters.add(tag);
                btn.classList.add('active');
            }
        }
        window.applyFilters();
    };

    window.updateAIScoreFilter = function(val) {
        window.aiScoreThreshold = parseInt(val || 0);
        document.getElementById('aiScoreVal').innerText = val;
        window.applyFilters();
    };

    window.applyFilters = function() {
        var cards = document.querySelectorAll('.signal_card');
        var stackMode = document.getElementById('stackModeSelect') ? document.getElementById('stackModeSelect').value : 'AND';
        let visibleCount = 0;

        cards.forEach(function(card) {
            var tags = card.dataset.tags ? card.dataset.tags.split(',') : [];
            var score = parseFloat(card.dataset.score || 0);
            var tagMatch = true;

            if (window.activeFilters.size > 0) {
                const filters = Array.from(window.activeFilters);
                if (stackMode === "AND") {
                    tagMatch = filters.every(t => tags.includes(t));
                } else {
                    tagMatch = filters.some(t => tags.includes(t));
                }
            }

            var scoreMatch = score >= window.aiScoreThreshold;
            
            if (tagMatch && scoreMatch) {
                card.style.display = "";
                visibleCount++;
            } else {
                card.style.display = "none";
            }
        });

        window.updateFilterState(visibleCount, cards.length);
    };

    window.updateFilterState = function(visibleCount, totalCount) {
        var statusMsg = document.getElementById('statusMsg');
        var activeNames = Array.from(window.activeFilters);
        if (window.aiScoreThreshold > 0) activeNames.push("AI >= " + window.aiScoreThreshold);
        
        if (statusMsg) {
            statusMsg.innerHTML = `Mode: ${window.stackMode} • Filters: ${activeNames.length > 0 ? activeNames.join(', ') : 'None'} • Showing ${visibleCount} of ${totalCount}`;
        }

        // Update counts on buttons based on VISIBLE cards
        var allCards = Array.from(document.querySelectorAll('.signal_card'));
        var visibleCards = allCards.filter(c => c.style.display !== 'none');

        document.querySelectorAll('.filter-area .btn[data-filter]').forEach(function(btn) {
            var tag = btn.dataset.filter;
            var count = visibleCards.filter(c => (c.dataset.tags || "").split(',').includes(tag)).length;
            var baseText = btn.dataset.text || tag;
            btn.innerHTML = `${baseText} (${count})`;
        });
    };

    // --- FAVORITES LOGIC ---
    window.updateFavButtons = function() {
        window.currentFavorites.forEach(function(fav) {
            var btn = document.getElementById('favbtn_' + fav.Ticker);
            if (btn) { btn.innerHTML = "✅ Added"; btn.style.background = "#e2e8f0"; btn.style.color = "#333"; btn.disabled = true; }
        });
    };

    window.addToFavorite = function(ticker, price, date, btnElement) {
        if (window.currentFavorites.find(f => f.Ticker === ticker)) return;
        var newFav = { 'Ticker': ticker, 'EntryPrice': price, 'EntryDate': date };
        window.currentFavorites.push(newFav);
        if (btnElement) { btnElement.innerHTML = "✅ Added"; btnElement.disabled = true; }
    };
    
    window.exportFavorites = function() {
        if(typeof XLSX === 'undefined') { alert('XLSX library not loaded'); return; }
        var ws = XLSX.utils.json_to_sheet(window.currentFavorites);
        var wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Sheet1");
        XLSX.writeFile(wb, "favorites.xlsx");
    };

    // --- CHART RENDERING (LAZY LOAD) ---
    window.initCharts = function() {
        const observer = new IntersectionObserver((entries, obs) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const divId = entry.target.id;
                    if (divId && window._tb_chart_payloads && window._tb_chart_payloads[divId] && !entry.target.getAttribute('data-drawn')) {
                        drawChart(divId, window._tb_chart_payloads[divId]);
                        entry.target.setAttribute('data-drawn', 'true');
                        obs.unobserve(entry.target);
                    }
                }
            });
        });
        
        document.querySelectorAll('.chart-container').forEach(el => observer.observe(el));
    };

    window.drawChart = function(divId, payloadObj) {
        if(!payloadObj || !payloadObj.data) return;
        var d = payloadObj.data;
        
        var tracePrice = {
            x: d.labels,
            open: d.open, high: d.high, low: d.low, close: d.close,
            type: 'candlestick', name: 'Price'
        };
        
        var data = [tracePrice];
        
        if (d.bb_upper && d.bb_upper.length > 0) {
            data.push({ x: d.labels, y: d.bb_upper, type: 'scatter', mode: 'lines', line: {color: '#ccc', width: 1}, name: 'BB Upper' });
            data.push({ x: d.labels, y: d.bb_lower, type: 'scatter', mode: 'lines', line: {color: '#ccc', width: 1}, name: 'BB Lower', fill: 'tonexty', fillcolor: 'rgba(200,200,200,0.1)' });
        }
        
        var layout = {
            margin: {l: 35, r: 35, t: 10, b: 30},
            height: %HEIGHT%,
            xaxis: {rangeslider: {visible: false}, type: 'date'},
            yaxis: {autorange: true, fixedrange: false},
            dragmode: 'pan',
            showlegend: false
        };
        
        Plotly.newPlot(divId, data, layout, {displayModeBar: false});
    };

    window.manualRefresh = function() { location.reload(); };
    window.scrollToTop = function() { window.scrollTo({top: 0, behavior: 'smooth'}); };
    
    window.onscroll = function() {
        var btn = document.getElementById("scrollTopBtn");
        if (document.body.scrollTop > 300 || document.documentElement.scrollTop > 300) {
            btn.style.display = "block";
        } else {
            btn.style.display = "none";
        }
    };

    window.downloadCSV = function() {
        var cards = document.querySelectorAll('.signal_card');
        var csv = "Ticker,Tags,Score\\n";
        cards.forEach(function(card) {
            if(card.style.display !== 'none') {
                csv += card.dataset.ticker + ',"' + card.dataset.tags.replace(/,/g, ';') + '",' + card.dataset.score + "\\n";
            }
        });
        var blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        var link = document.createElement("a");
        var url = URL.createObjectURL(blob);
        link.setAttribute("href", url);
        link.setAttribute("download", "filtered_results.csv");
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };
    
    document.addEventListener('DOMContentLoaded', () => {
        window.updateFavButtons();
        window.applyFilters(); // Init counts
        window.initCharts();   // Init observer
    });
    """

    nav_links = {
        "univ_file": os.path.basename(OUT_HTML_UNIV),
        "watch_file": os.path.basename(OUT_HTML_WATCH),
        "sector_file": os.path.basename(OUT_HTML_SECTOR),
        "fav_file": os.path.basename(OUT_HTML_FAV),
        "inbox_file": os.path.basename(OUT_HTML_INBOX)
    }

    generate_html_page(page_type="universal", data_groups=groups_univ, outpath=OUT_HTML_UNIV, nav_link=nav_links, source_info="Universal", timestamp_str=TIMESTAMP, report_js_template=report_js_template, existing_favorites=current_favorites_list)
    if watchmap_final:
        generate_html_page(page_type="watchlist", data_groups=groups_wl, outpath=OUT_HTML_WATCH, nav_link=nav_links, source_info="Watchlist", timestamp_str=TIMESTAMP, report_js_template=report_js_template, existing_favorites=current_favorites_list)
    if ENABLE_Sector: generate_html_page(page_type="sector", data_groups=groups_sector, outpath=OUT_HTML_SECTOR, nav_link=nav_links, source_info="Sector", timestamp_str=TIMESTAMP, report_js_template=report_js_template, existing_favorites=current_favorites_list)
    generate_html_page(
        page_type="inbox", data_groups=groups_inbox, outpath=OUT_HTML_INBOX,
        nav_link=nav_links, source_info="Inbox Alerts", timestamp_str=TIMESTAMP,
        report_js_template=report_js_template, existing_favorites=current_favorites_list
    )

    if favmap_final:
        generate_favorites_tile_report(data_groups=groups_fav, outpath=OUT_HTML_FAV, nav_link=nav_links, source_info="Favorites", timestamp_str=TIMESTAMP, script_version=SCRIPT_VERSION, report_js_template=report_js_template)

    # ═══════════════════════════════════════════════════════
    # v2.0 — DAILY MARKET INTELLIGENCE REPORTS (2 PM + 4 PM)
    # ═══════════════════════════════════════════════════════
    try:
        tb_check_and_send_reports(regime_data=_qf_regime, sector_data=_qf_sectors, quality_results=_qf_passed)
    except Exception as rpt_err:
        logger.warning("  Report check failed: %s", rpt_err)

        
    try:
        target_open = OUT_HTML_INBOX if groups_inbox["Recent Inbox Alerts"] else OUT_HTML_UNIV
        if os.path.exists(target_open) and os.getenv("GITHUB_ACTIONS") != "true":
             webbrowser.open(f'file://{os.path.abspath(target_open)}')
             logger.info("Opened HTML report: %s", os.path.abspath(target_open))
    except Exception as e:
        logging.error(f"Worker error for {ticker}: {e}")
        logger.error("Failed to open browser: %s", e)

def main():
    auto_import_favorites_from_downloads()
    logger.info("--- MAINTENANCE: Clearing Cache Directory to force fresh data ---")
    clean_output_directory(CACHE_DIR)
    
    clean_output_directory(MASTER_OUTPUT_DIR)
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(CHARTS_DIR, exist_ok=True)
            
    watchmap_final = {}; watchdata_final = {}; favmap_final = {}; favdata_final = {}; current_favorites_list = [] 
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
        

def market_is_open():
    nyse = mcal.get_calendar("NYSE")
    now = pd.Timestamp.now(tz="America/New_York")
    sched = nyse.schedule(start_date=now.date(), end_date=now.date())
    if sched.empty: return False
    return sched.iloc[0]["market_open"] <= now <= sched.iloc[0]["market_close"]

if __name__ == "__main__":
    if not market_is_open(): logger.info("Market is currently CLOSED. Running in offline/review mode.")
    else: logger.info("Market is OPEN.")

    main()






