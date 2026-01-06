# ============================================================
# Deployment Runner for TopBottom Dashboard
# GitHub Pages + GitHub Actions compatible
# ============================================================

import os
import sys

# ---- Paths (GitHub-safe) ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOCS_DIR = os.path.join(BASE_DIR, "docs")
CACHE_DIR = os.path.join(BASE_DIR, "cache")

os.makedirs(DOCS_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# ---- Disable browser open on GitHub ----
def can_open_browser():
    return not os.getenv("GITHUB_ACTIONS")

# ---- Import YOUR original final script ----
# IMPORTANT: this file must exist exactly as named
from TopBottomwithwatchlist_report_builder_39__FINAL import main as dashboard_main

# ---- Run ----
if __name__ == "__main__":
    print("Starting Top/Bottom Dashboard (deployment mode)")
    dashboard_main()
    print("Dashboard update completed")

