#!/usr/bin/env python3
# TopBottom_Universe - Favorites Tile Report Builder (Renamed from Watchlist)
# This file is imported by the main script to generate the tile-based favorites report.

import os, json, math
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np

# --- Copied Helpers from Main Script ---

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

def embed_local_plotly_text() -> str:
    """ (Copied) Embeds local plotly text if available. """
    try:
        if os.path.exists("plotly-latest.min.js"): # Assumes file is in same folder
            with open("plotly-latest.min.js", "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
    except Exception:
        pass
    return ""

# --- New Tile Report CSS ---

def get_tile_report_css() -> str:
    """ Contains the new CSS for the tile-based report. """
    return """
    :root {
        --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        --text-color: #2d3748;
        --text-muted: #718096;
        --text-dark: #1a202c;
        --bg-color: #f7fafc;
        --bg-white: #ffffff;
        --border-color: #e2e8f0;
        --green-500: #2f855a;
        --green-100: #f0fff4;
        --green-border: #c6f6d5;
        --red-500: #c53030;
        --red-100: #fff5f5;
        --red-border: #fecaca;
        --primary: #007bff;
    }
    body {
        font-family: var(--font-sans);
        background-color: var(--bg-color);
        color: var(--text-color);
        margin: 0;
        padding-top: 70px; /* Room for fixed toolbar */
    }
    .container {
        width: 100%;
        max-width: 95%;
        margin: 1rem auto;
        padding: 0 1rem;
        box-sizing: border-box;
    }
    .card {
        background: var(--bg-white);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        box-shadow: 0 1px 3px 0 rgba(0,0,0,0.1), 0 1px 2px 0 rgba(0,0,0,0.06);
        margin-bottom: 1rem;
    }
    h3 {
        font-size: 1.5rem;
        color: var(--text-dark);
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid var(--border-color);
    }
    
    /* --- Tile Grid --- */
    .tile-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
        gap: 1.25rem;
    }
    
    .stock-tile {
        background-color: var(--bg-white);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        overflow: hidden;
        display: flex;
        flex-direction: row;
        box-shadow: 0 1px 3px 0 rgba(0,0,0,0.05);
        transition: all 0.2s ease-in-out;
    }
    .stock-tile:hover {
        box-shadow: 0 4px 12px 0 rgba(0,0,0,0.08);
        transform: translateY(-2px);
    }
    
    .tile-border {
        flex-shrink: 0;
        width: 6px;
    }
    .tile-border-green { background-color: var(--green-500); }
    .tile-border-red { background-color: var(--red-500); }
    
    .tile-content {
        padding: 16px;
        width: 100%;
    }
    
    /* Header */
    .tile-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 1rem;
    }
    .tile-header-left {
        display: flex;
        flex-direction: column;
    }
    .tile-ticker {
        font-size: 2rem;
        font-weight: 700;
        color: var(--text-dark);
        line-height: 1;
        margin: 0;
    }
    .tile-category {
        font-size: 0.875rem;
        color: var(--text-muted);
        margin-top: 4px;
    }
    
    .tile-header-right {
        display: flex;
        flex-direction: column;
        align-items: flex-end;
    }
    .change-pill {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 0.9rem;
        font-weight: 600;
        line-height: 1;
    }
    .change-pill-green {
        background-color: var(--green-100);
        color: var(--green-500);
        border: 1px solid var(--green-border);
    }
    .change-pill-red {
        background-color: var(--red-100);
        color: var(--red-500);
        border: 1px solid var(--red-border);
    }
    
    /* Body */
    .tile-body {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1rem;
        margin-bottom: 1.25rem;
    }
    .tile-stat {
        display: flex;
        flex-direction: column;
    }
    .stat-label {
        font-size: 0.8rem;
        color: var(--text-muted);
        text-transform: uppercase;
        margin-bottom: 4px;
    }
    .stat-value {
        font-size: 1.25rem;
        font-weight: 600;
        color: var(--text-dark);
    }
    .stat-value-small {
        font-size: 1rem;
        font-weight: 500;
        color: var(--text-color);
    }
    
    /* Price Changes Table */
    .price-changes {
        font-size: 0.875rem;
        width: 100%;
        border-collapse: collapse;
        margin-bottom: 1.25rem;
    }
    .price-changes th, .price-changes td {
        text-align: center;
        padding: 8px;
        color: var(--text-muted);
        border: 1px solid var(--border-color);
    }
    .price-changes th {
        font-weight: 600;
        background-color: #fdfdfd;
    }
    
    /* 52-Week Range */
    .week-range {
        width: 100%;
    }
    .week-range-labels {
        display: flex;
        justify-content: space-between;
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--text-dark);
        margin-bottom: 6px;
    }
    .week-range-bar-outer {
        height: 10px;
        width: 100%;
        background-color: var(--border-color);
        border-radius: 5px;
        position: relative;
        overflow: hidden;
    }
    .week-range-bar-inner {
        height: 100%;
        background-color: var(--text-muted);
        border-radius: 5px 0 0 5px;
        position: relative;
    }
    .week-range-bar-inner::after { /* Current price indicator */
        content: '';
        display: block;
        width: 3px;
        height: 14px; /* Taller than bar */
        background-color: var(--text-dark);
        position: absolute;
        right: -1px; /* At the end of the inner bar */
        top: -2px; /* Centered */
    }
    
    /* --- Toolbar (Copied) --- */
    .top-toolbar {
        position: fixed; top: 0; left: 0; right: 0; z-index: 1000;
        background: var(--bg-white);
        border-bottom: 1px solid var(--border-color);
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        padding: 0 1rem;
        height: 60px;
        display: flex;
        align-items: center;
        color: var(--text-dark);
    }
    .brand { font-size: 1.25rem; font-weight: 700; }
    .controls { display: flex; align-items: center; gap: 8px; margin-left: 1.5rem; }
    .btn {
        display: inline-block;
        font-family: var(--font-sans);
        font-weight: 600;
        font-size: 0.875rem;
        text-decoration: none;
        padding: 8px 12px;
        border: 1px solid var(--border-color);
        border-radius: 6px;
        background: var(--bg-white);
        color: var(--text-color);
        cursor: pointer;
        transition: all 0.15s ease;
    }
    .btn:hover { background-color: #f7fafc; }
    .btn.primary { background-color: var(--primary); color: white; border-color: var(--primary); }
    .btn.ghost { background: transparent; border: 1px solid transparent; }
    .btn.ghost:hover { background: #f7fafc; border-color: var(--border-color); }
    .footer-small {
        text-align: center;
        padding: 20px;
        font-size: 0.8rem;
        color: var(--text-muted);
    }
    """

# --- Main Tile Report Function ---
# <-- MODIFIED: Renamed function
def generate_favorites_tile_report(
    data_groups: Dict[str,List[Dict[str,Any]]],
    outpath: str,
    nav_link: Dict[str, str], # Links to other pages
    source_info: str,
    timestamp_str: str,
    script_version: str,
    report_js_template: str # Pass in the script template
):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    page_title = "Favorites" # <-- MODIFIED: Page Title
    
    parts = []
    parts.append("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>")
    parts.append(f"<title>TopBottom {page_title} {script_version} — {now_str}</title>")
    
    # --- Embed Main CSS File ---
    try:
        with open('report_style.css', 'r', encoding='utf-8') as f:
            report_css = f.read()
        parts.append(f"<style>{report_css}</style>")
    except Exception:
        parts.append("<style>/* report_style.css not found */</style>")
        
    # --- Embed Tile CSS ---
    parts.append(f"<style>{get_tile_report_css()}</style>")
    parts.append("</head><body>")

    # --- Toolbar (Navigation) ---
    # <-- MODIFIED: Updated Nav Bar
    parts.append("<div class='top-toolbar'><div class='brand'>TopBottom — " + script_version + "</div>")
    parts.append("<div class='controls'>")

    if 'univ_file' in nav_link:
        parts.append(f"<a href='{nav_link['univ_file']}' class='btn ghost'>🌐 Universal</a>")
    
    if 'watch_file' in nav_link:
         parts.append(f"<a href='{nav_link['watch_file']}' class='btn ghost'>⭐ Watchlist</a>")
    
    if 'sector_file' in nav_link:
        parts.append(f"<a href='{nav_link['sector_file']}' class='btn ghost'>📊 Sector</a>")
        
    parts.append("<button class='btn primary'>❤️ Favorites</button>") # This is the active page

    parts.append("</div>") 
    parts.append("<div style='margin-left:auto;display:flex;gap:8px;align-items:center'>")
    parts.append("<button class='btn' onclick='exportFavorites()'>📤 Export Favorites</button>") # <-- MODIFIED
    parts.append("<button class='btn' onclick='window.location.reload()'>🔄 Refresh</button>")
    parts.append("</div></div>") 
    # <-- END MODIFIED SECTION -->

    # --- Page Content ---
    parts.append("<div class='container'>")
    
    total_signals = sum(len(v) for v in data_groups.values())
    if not data_groups or total_signals == 0:
        parts.append(f"<div class='card' style='padding: 2rem; text-align: center;'>Your Favorites list is empty.</div>")
        parts.append("<div class='card' style='padding: 2rem; text-align: center;'><strong>How to use:</strong><br>1. Browse the 'Universal' or 'Sector' reports and click '+ Add to Favorite' on any stock.<br>2. When done, click '📤 Export Favorites' to download your `favorites.xlsx` file.<br>3. Place this file in the same folder as the script.<br>4. Re-run the Python script. Your stocks will appear here.</div>")
    else:
        for group_name, items in data_groups.items():
            if not items: continue
            
            parts.append(f"<h3>{group_name} ({len(items)})</h3>")
            parts.append("<div class='tile-grid'>")
            
            for s in items:
                # --- 1. Data Extraction ---
                ticker = s.get('ticker', 'N/A')
                sector = s.get('sector', 'N/A')
                daily_df = s.get('daily_df')
                
                # These are passed in from the new load_watchlist_from_excel
                entry_price = s.get('entry_price') 
                entry_date = s.get('entry_date')
                
                today_price = None
                week_52_high = None
                week_52_low = None
                
                if daily_df is not None and not daily_df.empty:
                    try:
                        today_price = float(daily_df['Close'].iloc[-1])
                        # Use last 252 trading days for 52-week range
                        week_52_high = float(daily_df['Close'].tail(252).max())
                        week_52_low = float(daily_df['Close'].tail(252).min())
                    except Exception:
                        pass # Keep them as None
                
                # --- 2. Calculations ---
                overall_change_pct = None
                overall_change_val = None
                daily_change_pct = None
                
                if today_price and entry_price:
                    overall_change_val = today_price - entry_price
                    if entry_price != 0: # Avoid divide by zero
                        overall_change_pct = (overall_change_val / entry_price) * 100
                
                if daily_df is not None and len(daily_df) > 1:
                    try:
                        last_close = float(daily_df['Close'].iloc[-1])
                        prev_close = float(daily_df['Close'].iloc[-2])
                        if prev_close != 0: # Avoid divide by zero
                            daily_change_pct = ((last_close - prev_close) / prev_close) * 100
                    except Exception:
                        pass

                is_positive = overall_change_pct is not None and overall_change_pct >= 0
                border_class = 'tile-border-green' if is_positive else 'tile-border-red'
                change_class = 'change-pill-green' if is_positive else 'change-pill-red'
                
                range_pct = 0
                if week_52_high and week_52_low and today_price:
                    if (week_52_high - week_52_low) > 0:
                        range_pct = max(0, min(100, ((today_price - week_52_low) / (week_52_high - week_52_low)) * 100))

                # --- 3. Build Tile HTML ---
                parts.append(f"<div class='stock-tile'>")
                parts.append(f"  <div class='tile-border {border_class}'></div>")
                parts.append(f"  <div class='tile-content'>")
                
                # --- Header ---
                parts.append(f"    <div class='tile-header'>")
                parts.append(f"      <div class='tile-header-left'>")
                parts.append(f"        <a href='https://finviz.com/quote.ashx?t={ticker}&p=d' target='_blank' style='text-decoration:none;'><h2 class='tile-ticker'>{ticker}</h2></a>")
                parts.append(f"        <div class='tile-category'>Category: {sector}</div>")
                parts.append(f"      </div>")
                parts.append(f"      <div class='tile-header-right'>")
                parts.append(f"        <div class='change-pill {change_class}'>")
                parts.append(f"          Overall Change {'▲' if is_positive else '▼'} {safe_str(overall_change_pct)}%")
                parts.append(f"        </div>")
                parts.append(f"        <div class='stat-label' style='margin-top: 4px;'>Daily: {safe_str(daily_change_pct)}%</div>")
                parts.append(f"      </div>")
                parts.append(f"    </div>")
                
                # --- Body Stats ---
                parts.append(f"    <div class='tile-body'>")
                parts.append(f"      <div class='tile-stat'>")
                parts.append(f"        <div class='stat-label'>Today's Price</div>")
                parts.append(f"        <div class='stat-value'>{money(today_price)}</div>")
                parts.append(f"      </div>")
                parts.append(f"      <div class='tile-stat'>")
                parts.append(f"        <div class='stat-label'>Market Cap</div>")
                parts.append(f"        <div class='stat-value'>N/A</div>") # Data not fetched by script
                parts.append(f"      </div>")
                parts.append(f"      <div class='tile-stat'>")
                parts.append(f"        <div class='stat-label'>Entry Price</div>")
                parts.append(f"        <div class='stat-value stat-value-small'>{money(entry_price)}</div>")
                parts.append(f"      </div>")
                parts.append(f"      <div class='tile-stat'>")
                parts.append(f"        <div class='stat-label'>Entry Date</div>")
                parts.append(f"        <div class='stat-value stat-value-small'>{entry_date or 'N/A'}</div>")
                parts.append(f"      </div>")
                parts.append(f"    </div>")
                
                # --- Price Change (Mocked) ---
                parts.append(f"    <div class='stat-label'>Price Change (Trends)</div>")
                parts.append(f"    <table class='price-changes'>")
                parts.append(f"      <tr><th>3D</th><th>5D</th><th>7D</th><th>9D</th><th>15D</th><th>30D</th><th>60D</th><th>90D</th></tr>")
                parts.append(f"      <tr><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>")
                parts.append(f"    </table>")
                
                # --- 52-Week Range ---
                parts.append(f"    <div class='week-range'>")
                parts.append(f"      <div class='week-range-labels'>")
                parts.append(f"        <span>{money(week_52_low)}</span>")
                parts.append(f"        <span>52-Week Range (Top/Bottom)</span>")
                parts.append(f"        <span>{money(week_52_high)}</span>")
                parts.append(f"      </div>")
                parts.append(f"      <div class='week-range-bar-outer'>")
                parts.append(f"        <div class='week-range-bar-inner' style='width: {range_pct}%;'></div>")
                parts.append(f"      </div>")
                parts.append(f"    </div>")
                
                parts.append(f"  </div>") # content
                parts.append(f"</div>") # tile

            parts.append("</div>") # tile-grid
            
    parts.append("</div>") # container
    parts.append(f"<div class='footer-small'>Generated by TopBottom_Universe {script_version} — {now_str}</div>")

    # --- JS ---
    # We don't need Plotly for this page, but we DO need the 'xlsx' library for the export button
    parts.append("<script src='https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js'></script>")
    
    # We also need the report_script.js for the export function
    js_final = report_js_template
    # Replace dummy values, though they aren't used on this page
    js_final = js_final.replace("var EAGER_RENDER_COUNT = %EAGER%;", "var EAGER_RENDER_COUNT = 0;")
    js_final = js_final.replace("var AUTO_REFRESH_MINUTES = %REF%;", "var AUTO_REFRESH_MINUTES = 10;")
    js_final = js_final.replace("var CHART_HEIGHT = %HEIGHT%;", "var CHART_HEIGHT = 400;")
    js_final = js_final.replace("var TABLE_ROWS_DAILY = %TABLEROWS_DAILY%;", "var TABLE_ROWS_DAILY = 30;")
    js_final = js_final.replace("var TABLE_ROWS_INTRADAY = %TABLEROWS_INTRADAY%;", "var TABLE_ROWS_INTRADAY = 30;")
    parts.append("<script>" + js_final + "</script>")

    parts.append("</body></html>")

    html = "\n".join(parts)
    try:
        with open(outpath, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Saved Favorites Tile report: {outpath}")
    except Exception as e:
        print(f"Could not write HTML: {e}")