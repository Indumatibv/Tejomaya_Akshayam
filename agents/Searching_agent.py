#!/usr/bin/env python
# python agents/Searching_agent.py
# =========================================================================
# CRITICAL FIX FOR WINDOWS - MUST BE AT THE VERY TOP OF THE SCRIPT
# =========================================================================
import sys
import asyncio
from matplotlib.pyplot import title
import nest_asyncio
import platform
import os
from pathlib import Path
import random
from nltk import data
# Apply Windows-specific event loop fix (must run before other asyncio use)
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    nest_asyncio.apply()
# =========================================================================

import logging
from urllib.parse import urljoin, parse_qs, unquote, urlparse
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import aiohttp
import base64
from datetime import datetime, timedelta
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import json

import unicodedata
import tempfile
import hashlib
import glob
import shutil
import requests
from langchain_community.llms import Ollama 
from selenium.common.exceptions import NoSuchWindowException, WebDriverException

try:
    import undetected_chromedriver as uc
    _UC_AVAILABLE = True
except ImportError:
    _UC_AVAILABLE = False
  
# ---------------------- Logging ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ---- GLOBAL TITLE TRACKING FOR BSE vs NSE DEDUP ----
BSE_TITLES_NORMALIZED = set()

#----mca-----
 

# ── Constants ──────────────────────────────────────────────
 
MCA_HOME_URL        = "https://www.mca.gov.in/"
MCA_DMS_BASE        = "https://www.mca.gov.in/bin/ebook/dms/getdocument"
MCA_DMS_PR_BASE     = "https://www.mca.gov.in/bin/dms/getdocument"   # Press Release endpoint
MCA_MAX_NAV_RETRIES = 5
MCA_MAX_DRV_RETRIES = 3   # retries when driver window dies on startup
MCA_DOMAIN_NAME     = "Companies Act"   # display name in Excel Verticals column
 
MCA_IGNORE_KEYWORDS = [
    "bid queries",
    "vacancy advertisement",
    "career notices",
    "corrigendum filling up post",
    "request for proposal",
]

#------------------------
def normalize_title_for_compare(title: str) -> str:
    """
    Normalize titles for cross-exchange comparison.
    - lowercase
    - remove extra spaces
    - strip punctuation
    """
    if not title:
        return ""

    title = unicodedata.normalize("NFKD", title)
    title = title.lower()
    title = re.sub(r'[^a-z0-9\s]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def safe_pdf_filename(title: str | None, pdf_url: str, max_base_len: int = 80) -> str:
    """
    Generates a filesystem-safe, collision-proof PDF filename.
    """
    if title:
        base = sanitize_filename(title).replace(".pdf", "")
    else:
        base = os.path.basename(urlparse(pdf_url).path).replace(".pdf", "")

    base = base[:max_base_len].rstrip("_")

    # stable short hash (URL-based)
    h = hashlib.sha1(pdf_url.encode("utf-8")).hexdigest()[:8]

    return f"{base}_{h}.pdf"

# -------- CONFIG --------

# Where PDFs should be stored (keep as-is: your Downloads path)
if platform.system() == "Windows":
    BASE_PATH = r"C:\Users\Admin\Desktop\Indu\Akshayam\Tejomaya_pdfs\Akshayam Data"
else:
    BASE_PATH = "/Users/admin/Downloads/Tejomaya_pdfs/Akshayam Data"

# Ensure base download folder exists
os.makedirs(BASE_PATH, exist_ok=True)

# Excel output goes into the repo data folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_OUTPUT = DATA_DIR / "Searching_agent_output.xlsx"

# -------- CLEAN PREVIOUS RUN OUTPUTS --------

def clean_previous_outputs():
    try:
        week_range_file = DATA_DIR / "week_range.json"
        excel_file = EXCEL_OUTPUT

        # Delete week_range.json
        if week_range_file.exists():
            week_range_file.unlink()
            logging.info("Deleted previous week_range.json")

        # Delete Searching_agent_output.xlsx
        if excel_file.exists():
            excel_file.unlink()
            logging.info("Deleted previous Searching_agent_output.xlsx")

    except Exception as e:
        logging.error("Error cleaning previous outputs: %s", e)


# Call this BEFORE anything starts
clean_previous_outputs()

# GLOBAL LIST FOR FINAL EXCEL
ALL_DOWNLOADED = []

# Excel file containing links
LINKS_EXCEL = DATA_DIR / "Links.xlsx"

# Only process these sheet names (categories)
PROCESS_SHEETS = ["SEBI", "Listed Companies", "IFSCA", "RBI", "IBBI", "ICAI", "Companies Act"]

# IBBI subdomains handled by IBBI v1 scraper
IBBI_1_SCRAPE = [
     "Notifications", "Circulars", "Regulations", "Acts", "Discussion Paper", "Guidelines" 
]

#---------------------------------------------------

def load_link_tasks_from_excel():
    tasks = []

    if not LINKS_EXCEL.exists():
        logging.error("Links Excel not found: %s", LINKS_EXCEL)
        return tasks

    xls = pd.ExcelFile(LINKS_EXCEL)

    for sheet in xls.sheet_names:
        if sheet not in PROCESS_SHEETS:
            logging.info("Skipping sheet (not in PROCESS_SHEETS): %s", sheet)
            continue

        df = pd.read_excel(LINKS_EXCEL, sheet_name=sheet)

        # Expect first column = SUBFOLDER
        # Second column = URL
        if df.shape[1] < 2:
            logging.warning("Invalid format in sheet: %s", sheet)
            continue

        subfolder_col = df.columns[0]
        link_col = df.columns[1]

        for _, row in df.iterrows():
            subfolder = str(row[subfolder_col]).strip()
            url = str(row[link_col]).strip()

            if not subfolder or not url or url.lower() == "nan":
                continue

            tasks.append({
                "category": sheet,    # sheet name = CATEGORY
                "subfolder": subfolder,
                "url": url
            })

    logging.info("Loaded %d link tasks from Excel", len(tasks))
    return tasks

def detect_aif_category(title: str) -> bool:
    aif_keywords = [
        "portfolio manager",
        "angel investor",
        "angel fund",
        "infrastructure investment trust",
        "invit",
        "real estate investment trust",
        "reit",
        "research analyst",
        "investment advisor",
        "alternative investment fund",
        "aif"
    ]

    title_lower = title.lower()
    return any(keyword in title_lower for keyword in aif_keywords)

def extract_detail_links_from_listing(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.select("a.points[href]"):
        detail_url = urljoin(base_url, a["href"])
        title = a.get_text(strip=True)
        links.append({"url": detail_url, "title": title})

    return links

def extract_sebi_pdf_from_iframe(iframe_src: str, page_url: str) -> str | None:
    if not iframe_src:
        return None

    iframe_src = urljoin(page_url, iframe_src)
    parsed = urlparse(iframe_src)
    qs = parse_qs(parsed.query)

    pdf = qs.get("file", [None])[0]
    if not pdf:
        return None

    return unquote(pdf)

def is_ignored_sebi_title(title: str) -> bool:
    """
    Returns True if SEBI title should be ignored based on business rules.
    - Most keywords: case-insensitive
    - KRAs / CRAs: case-sensitive (exact)
    """

    if not title:
        return False

    # Case-insensitive keywords
    ignore_keywords_ci = [
        "mutual fund",
        "niveshak shivir",
        "inauguration",
        "survey",
        "municipal bond",
        "contest",
        "campaign",
        "annual report",
        "newspaper advertisement"
    ]

    title_lower = title.lower()

    for kw in ignore_keywords_ci:
        if kw in title_lower:
            return True

    # Case-sensitive exact checks (DO NOT lowercase)
    if "KRAs" in title or "CRAs" in title or "KRA" in title or "CRA" in title:
        return True

    return False

def is_ignored_ifsca_title(title: str) -> bool:
    """
    Returns True if IFSCA title should be skipped.
    Case-insensitive keyword match.
    """
    if not title:
        return False

    ignore_keywords = [
        "career",
        "careers",
        "tender",
        "tenders",
    ]

    title_lower = title.lower()
    return any(kw in title_lower for kw in ignore_keywords)

#-------------------------------------------------------

# -------- WEEK RANGE LOGIC --------
def get_week_range(weeks_back: int = 0):
    today = datetime.today()
    this_monday = (today - timedelta(days=today.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    target_monday = this_monday - timedelta(weeks=weeks_back)
    target_sunday = (target_monday + timedelta(days=6)).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )
    if weeks_back == 0:
        target_sunday = today.replace(hour=23, minute=59, second=59, microsecond=999999)
    logging.info("Target range (%d week(s) back): %s -> %s", weeks_back, target_monday.date(), target_sunday.date())
    return target_monday, target_sunday


# ── Ignore filter ──────────────────────────────────────────
 
def is_ignored_mca_title(title: str) -> bool:
    if not title:
        return False
    t = unicodedata.normalize("NFKD", title).lower()
    t = re.sub(r"\s+", " ", t)
    return any(kw in t for kw in MCA_IGNORE_KEYWORDS)
 
 
# ── Driver builder ─────────────────────────────────────────
 
def _build_mca_driver():
    if _UC_AVAILABLE:
        opts = uc.ChromeOptions()
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        # return uc.Chrome(options=opts)
        return uc.Chrome(options=opts, version_main=149)
    else:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        opts = Options()
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        return webdriver.Chrome(options=opts)
 
 
# ── Navigation ─────────────────────────────────────────────
 
def _on_target_page(driver, url: str) -> bool:
    keyword = url.rstrip("/").split("/")[-1].split(".")[0]  # e.g. "notifications"
    return keyword in driver.current_url
 
 
def _navigate_mca(driver, target_url: str) -> bool:
    """
    Load MCA home (session), then navigate to target_url.
    Retries if redirected back to home.
    Raises NoSuchWindowException immediately so the caller can
    rebuild the driver.
    """
    logging.info("MCA: loading home to establish session...")
    driver.get(MCA_HOME_URL)   # may raise NoSuchWindowException on bad startup
    time.sleep(5)
 
    for attempt in range(1, MCA_MAX_NAV_RETRIES + 1):
        logging.info("MCA navigate attempt %d/%d -> %s",
                     attempt, MCA_MAX_NAV_RETRIES, target_url)
        driver.get(target_url)
 
        deadline = time.time() + 10
        while time.time() < deadline:
            if _on_target_page(driver, target_url):
                break
            time.sleep(0.5)
 
        if _on_target_page(driver, target_url):
            logging.info("MCA: landed on target page.")
            return True
 
        logging.warning("MCA: redirected — clearing cookies, retry...")
        driver.delete_all_cookies()
        driver.get(MCA_HOME_URL)
        time.sleep(5)
 
    logging.error("MCA: could not reach %s after %d attempts",
                  target_url, MCA_MAX_NAV_RETRIES)
    return False
 
 
# ── Wait for table rows (redirect-recovery) ────────────────
 
def _wait_for_mca_rows(driver, target_url: str, timeout: int = 90) -> list:
    logging.info("MCA: waiting up to %ds for table rows...", timeout)
    deadline = time.time() + timeout
    re_nav   = 0
 
    while time.time() < deadline:
        if not _on_target_page(driver, target_url):
            re_nav += 1
            remaining = int(deadline - time.time())
            logging.warning("MCA: redirected (#%d, %ds left) — re-navigating...",
                            re_nav, remaining)
            driver.delete_all_cookies()
            driver.get(MCA_HOME_URL)
            time.sleep(4)
            driver.get(target_url)
            settle = time.time() + 10
            while time.time() < settle:
                if _on_target_page(driver, target_url):
                    break
                time.sleep(0.5)
            time.sleep(3)
            continue
 
        rows = driver.find_elements(By.XPATH, "//table//tr[td]")
        valid = [
            r for r in rows
            if len(r.find_elements(By.TAG_NAME, "td")) >= 3
            and r.find_elements(By.TAG_NAME, "td")[0].text.strip()
        ]
        if valid:
            logging.info("MCA: table loaded with %d rows.", len(valid))
            return valid
 
        time.sleep(1)
 
    logging.error("MCA: timed out waiting for table rows.")
    return []
 
 
# ── Snapshot rows -> plain dicts (no live elements) ────────
 
def _snapshot_mca_rows(driver) -> list[dict]:
    rows = driver.find_elements(By.XPATH, "//table//tr[td]")
    results = []
 
    for row in rows:
        try:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 3:
                continue
 
            title    = cols[0].text.strip()
            date_str = cols[2].text.strip()   # DD/MM/YYYY
 
            if not title or title.lower() in ("particulars", "s.no", "sr.no", "#"):
                continue
 
            anchors = cols[0].find_elements(By.TAG_NAME, "a")
            if not anchors:
                continue
 
            val     = anchors[0].get_attribute("val")
            doc_cat = anchors[0].get_attribute("data-doccategory") or "Notifications"
 
            results.append({
                "title":    title,
                "date_str": date_str,
                "val":      val,
                "doc_cat":  doc_cat,
            })
 
        except Exception:
            continue
 
    logging.info("MCA: snapshot captured %d rows.", len(results))
    return results
 
 
# ── DMS URL builder ────────────────────────────────────────
 
def _mca_dms_url(val: str, doc_cat: str) -> str:
    doc_b64   = base64.b64encode(val.encode()).decode()
    timestamp = int(time.time() * 1000)
    return (
        f"{MCA_DMS_BASE}"
        f"?doc={doc_b64}"
        f"&docCategory={doc_cat}"
        f"&_={timestamp}"
    )
 
def _mca_pr_dms_url(data_value: str) -> str:
    """
    Press Release endpoint: /bin/dms/getdocument?mds=<data-value>&type=open
    data-value is already URL-encoded in the HTML — pass as-is.
    """
    return f"{MCA_DMS_PR_BASE}?mds={data_value}&type=open"
 
# ── PDF downloader ─────────────────────────────────────────
 
def _download_mca_pdf(val: str, doc_cat: str, filepath: str,
                      cookies: dict, user_agent: str,
                      page_url: str) -> bool:
    url = _mca_dms_url(val, doc_cat)
    headers = {
        "User-Agent":       user_agent,
        "Referer":          page_url,
        "Accept":           "*/*",
        "X-Requested-With": "XMLHttpRequest",
    }
 
    try:
        resp = requests.get(url, cookies=cookies, headers=headers,
                            timeout=30, stream=True)
        resp.raise_for_status()
 
        raw = b"".join(resp.iter_content(chunk_size=8192))
 
        if len(raw) < 100 or not raw.startswith(b"%PDF"):
            logging.warning(
                "MCA: invalid PDF (size=%d, first4=%s) for val=%s",
                len(raw), raw[:4], val
            )
            return False
 
        with open(filepath, "wb") as f:
            f.write(raw)
 
        logging.info("MCA: saved %s (%.1f KB)",
                     os.path.basename(filepath), len(raw) / 1024)
        return True
 
    except Exception as e:
        logging.error("MCA: download failed val=%s : %s", val, e)
        return False

def _download_mca_pr_pdf(data_value: str, filepath: str,
                          cookies: dict, user_agent: str,
                          page_url: str) -> bool:
    url = _mca_pr_dms_url(data_value)
    headers = {
        "User-Agent":       user_agent,
        "Referer":          page_url,
        "Accept":           "application/pdf,*/*",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        resp = requests.get(url, cookies=cookies, headers=headers,
                            timeout=30, stream=True)
        resp.raise_for_status()
        raw = b"".join(resp.iter_content(chunk_size=8192))

        if len(raw) < 100 or not raw.startswith(b"%PDF"):
            logging.warning(
                "MCA PR: invalid PDF (size=%d, first4=%s) for data_value=%s",
                len(raw), raw[:4], data_value
            )
            return False

        with open(filepath, "wb") as f:
            f.write(raw)

        logging.info("MCA PR: saved %s (%.1f KB)",
                     os.path.basename(filepath), len(raw) / 1024)
        return True

    except Exception as e:
        logging.error("MCA PR: download failed data_value=%s : %s", data_value, e)
        return False
     
# ── Pagination: click Next and wait for new rows ───────────
 
MCA_MAX_PAGES = 3   # how many pages to scrape per URL
 
def _click_next_page(driver, current_first_val: str) -> bool:
    """
    Click the DataTables Next button and wait for the table to refresh.
    Returns True if a new page loaded, False if disabled or failed.
    """
    try:
        # MCA uses a standard DataTables next button
        next_btn = driver.find_element(
            By.CSS_SELECTOR,
            "a.paginate_button.next, li.next a, #notificationCircularResultTable_next"
        )
        classes = next_btn.get_attribute("class") or ""
        if "disabled" in classes:
            logging.info("MCA: Next button disabled — no more pages.")
            return False
 
        next_btn.click()
 
        # Wait until first row val changes (new page loaded)
        deadline = time.time() + 15
        while time.time() < deadline:
            rows = driver.find_elements(By.XPATH, "//table//tr[td]")
            valid = [
                r for r in rows
                if len(r.find_elements(By.TAG_NAME, "td")) >= 3
                and r.find_elements(By.TAG_NAME, "td")[0].text.strip()
            ]
            if valid:
                try:
                    anchors = valid[0].find_elements(By.TAG_NAME, "a")
                    new_val = anchors[0].get_attribute("val") if anchors else None
                    if new_val and new_val != current_first_val:
                        logging.info("MCA: page turned (new first val=%s).", new_val)
                        return True
                except Exception:
                    pass
            time.sleep(0.5)
 
        logging.warning("MCA: page turn timed out — table did not refresh.")
        return False
 
    except Exception as e:
        logging.warning("MCA: Next button click failed: %s", e)
        return False
 
 
# ── Browser session: navigate + snapshot up to N pages ────
 
def _get_mca_snapshot(target_url: str) -> tuple[list[dict], dict, str]:
    """
    Builds a driver, navigates to target_url, snapshots up to
    MCA_MAX_PAGES pages of the DataTables listing, then quits.
 
    Retries the entire sequence up to MCA_MAX_DRV_RETRIES times
    if undetected_chromedriver's window dies on startup.
 
    Returns (all_rows, cookies, user_agent).
    """
    for drv_attempt in range(1, MCA_MAX_DRV_RETRIES + 1):
        driver = None
        try:
            logging.info("MCA: driver attempt %d/%d", drv_attempt, MCA_MAX_DRV_RETRIES)
            driver = _build_mca_driver()
 
            # Small warm-up pause — gives uc time to stabilise its tab
            time.sleep(2)
 
            landed = _navigate_mca(driver, target_url)
            if not landed:
                return [], {}, ""
 
            _wait_for_mca_rows(driver, target_url, timeout=90)
 
            all_rows = []
 
            for page_no in range(1, MCA_MAX_PAGES + 1):
                # Check we haven't been redirected mid-pagination
                if not _on_target_page(driver, target_url):
                    logging.warning("MCA: redirected during pagination at page %d — stopping.", page_no)
                    break
 
                page_rows = _snapshot_mca_rows(driver)
                logging.info("MCA: page %d — %d rows snapshotted.", page_no, len(page_rows))
                all_rows.extend(page_rows)
 
                if page_no == MCA_MAX_PAGES:
                    break   # reached limit, stop
 
                # Get first val before clicking next (to detect page change)
                first_val = page_rows[0]["val"] if page_rows else None
                if not _click_next_page(driver, first_val):
                    break   # no more pages
 
                time.sleep(1)   # brief pause after page turn
 
            cookies    = {c["name"]: c["value"] for c in driver.get_cookies()}
            user_agent = driver.execute_script("return navigator.userAgent;")
            logging.info("MCA: total rows across all pages: %d", len(all_rows))
            return all_rows, cookies, user_agent
 
        except (NoSuchWindowException, WebDriverException) as exc:
            logging.warning(
                "MCA: driver window lost on attempt %d/%d (%s). Retrying...",
                drv_attempt, MCA_MAX_DRV_RETRIES, exc
            )
            time.sleep(3)
 
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
 
    logging.error("MCA: all %d driver attempts failed.", MCA_MAX_DRV_RETRIES)
    return [], {}, ""

def _snapshot_mca_pr_rows(driver) -> list[dict]:
    """Snapshot Press Release rows using data-value attribute."""
    rows = driver.find_elements(By.XPATH, "//table//tr[td]")
    results = []

    for row in rows:
        try:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 2:
                continue

            title    = cols[0].text.strip()
            date_str = cols[1].text.strip()

            if not title or title.lower() in ("name", "title", "particulars"):
                continue

            # Press Release uses data-value (not val)
            anchor = None
            for col in cols:
                anchors = col.find_elements(By.CSS_SELECTOR, "a[data-value]")
                if anchors:
                    anchor = anchors[0]
                    break

            if not anchor:
                continue

            data_value = anchor.get_attribute("data-value") or ""

            results.append({
                "title":      title,
                "date_str":   date_str,
                "data_value": data_value,
            })

        except Exception:
            continue

    logging.info("MCA PR: snapshot captured %d rows.", len(results))
    return results

def _click_next_page_pr(driver, current_first_val: str) -> bool:
    """
    PR page uses a different DataTables Next button selector.
    Tries multiple known selectors before giving up.
    """
    PR_NEXT_SELECTORS = [
        "a.paginate_button.next",
        "li.next a",
        "#pressReleaseTable_next",
        "#tblPressRelease_next",
        "[id$='_next']",   # any DataTables next button (suffix match)
    ]
    try:
        next_btn = None
        for sel in PR_NEXT_SELECTORS:
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, sel)
                break
            except Exception:
                continue

        if not next_btn:
            logging.info("MCA PR: no Next button found — single page table.")
            return False

        classes = next_btn.get_attribute("class") or ""
        if "disabled" in classes:
            logging.info("MCA PR: Next button disabled — no more pages.")
            return False

        next_btn.click()

        deadline = time.time() + 15
        while time.time() < deadline:
            rows = driver.find_elements(By.XPATH, "//table//tr[td]")
            valid = [
                r for r in rows
                if len(r.find_elements(By.TAG_NAME, "td")) >= 2
                and r.find_elements(By.TAG_NAME, "td")[0].text.strip()
            ]
            if valid:
                try:
                    anchors = valid[0].find_elements(By.CSS_SELECTOR, "a[data-value]")
                    new_val = anchors[0].get_attribute("data-value") if anchors else None
                    if new_val and new_val != current_first_val:
                        logging.info("MCA PR: page turned.")
                        return True
                except Exception:
                    pass
            time.sleep(0.5)

        logging.warning("MCA PR: page turn timed out.")
        return False

    except Exception as e:
        logging.warning("MCA PR: Next button click failed: %s", e)
        return False
    
def _get_mca_pr_snapshot(target_url: str) -> tuple[list[dict], dict, str]:
    """
    Same driver/navigation logic as _get_mca_snapshot but calls
    _snapshot_mca_pr_rows (data-value based) instead of _snapshot_mca_rows.
    """
    for drv_attempt in range(1, MCA_MAX_DRV_RETRIES + 1):
        driver = None
        try:
            logging.info("MCA PR: driver attempt %d/%d", drv_attempt, MCA_MAX_DRV_RETRIES)
            driver = _build_mca_driver()
            time.sleep(2)

            landed = _navigate_mca(driver, target_url)
            if not landed:
                return [], {}, ""

            _wait_for_mca_rows(driver, target_url, timeout=90)

            all_rows = []

            for page_no in range(1, MCA_MAX_PAGES + 1):
                if not _on_target_page(driver, target_url):
                    logging.warning("MCA PR: redirected during pagination at page %d.", page_no)
                    break

                page_rows = _snapshot_mca_pr_rows(driver)
                logging.info("MCA PR: page %d — %d rows snapshotted.", page_no, len(page_rows))
                all_rows.extend(page_rows)

                if page_no == MCA_MAX_PAGES:
                    break

                # first_val = page_rows[0]["data_value"] if page_rows else None
                # # Reuse _click_next_page; pass data_value as the "current_first_val"
                # if not _click_next_page(driver, first_val):
                #     break
                first_val = page_rows[0]["data_value"] if page_rows else None
                if not _click_next_page_pr(driver, first_val):
                    break
                time.sleep(1)

            cookies    = {c["name"]: c["value"] for c in driver.get_cookies()}
            user_agent = driver.execute_script("return navigator.userAgent;")
            logging.info("MCA PR: total rows: %d", len(all_rows))
            return all_rows, cookies, user_agent

        except (NoSuchWindowException, WebDriverException) as exc:
            logging.warning("MCA PR: driver lost on attempt %d/%d (%s). Retrying...",
                            drv_attempt, MCA_MAX_DRV_RETRIES, exc)
            time.sleep(3)

        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    logging.error("MCA PR: all %d driver attempts failed.", MCA_MAX_DRV_RETRIES)
    return [], {}, ""
 
# ── Main scraper (called from scrape_generic_link) ────────
async def scrape_mca_press_release(task: dict, week_start: datetime, week_end: datetime):
    """
    Scraper for MCA Press Release pages.
    Uses data-value + mds endpoint (distinct from Notifications/Circulars).
    """
    category  = task["category"]
    subfolder = task["subfolder"]
    page_url  = task["url"]

    logging.info("MCA PR SCRAPER -> [%s > %s] %s", category, subfolder, page_url)

    rows, cookies, user_agent = _get_mca_pr_snapshot(page_url)

    if not rows:
        logging.warning("MCA PR: no rows captured — skipping.")
        return

    seen_titles = set()

    for row in rows:
        title      = row["title"]
        date_str   = row["date_str"]
        data_value = row["data_value"]

        if is_ignored_mca_title(title):
            logging.info("MCA PR: skipping (ignored): %s", title)
            continue

        dt = None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d %b %Y", "%d %B %Y"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                break
            except ValueError:
                pass
        if not dt:
            logging.warning("MCA PR: bad date %r for: %s", date_str, title)
            continue
        if not (week_start <= dt <= week_end):
            logging.info("MCA PR: outside week (%s): %s", dt.date(), title[:60])
            continue

        if not data_value:
            logging.warning("MCA PR: no data-value for: %s", title)
            continue

        year       = str(dt.year)
        month_full = dt.strftime("%B")

        save_dir = ensure_year_month_structure(
            BASE_PATH, category, subfolder, year, month_full
        )

        clean_title = title.split("|")[0].strip()
        title_key   = clean_title.lower().strip()

        if title_key in seen_titles:
            logging.info("MCA PR: duplicate skipped: %s", clean_title[:60])
            continue
        seen_titles.add(title_key)

        base_name  = sanitize_filename(clean_title).replace(".pdf", "")
        # Use last 8 chars of data_value hash for uniqueness
        import hashlib
        h          = hashlib.sha1(data_value.encode()).hexdigest()[:8]
        filename   = f"{base_name}_{h}.pdf"
        filepath   = os.path.join(save_dir, filename)

        ok = _download_mca_pr_pdf(data_value, filepath, cookies, user_agent, page_url)

        if ok:
            ALL_DOWNLOADED.append({
                "Verticals":   MCA_DOMAIN_NAME,
                "SubCategory": subfolder,
                "Year":        year,
                "Month":       month_full,
                "IssueDate":   dt.strftime("%Y-%m-%d"),
                "Title":       clean_title,
                "PDF_URL":     _mca_pr_dms_url(data_value),
                "File Name":   filename,
                "Path":        os.path.abspath(filepath),
            })
        else:
            logging.error("MCA PR: failed to download: %s", title)

    logging.info("MCA PR SCRAPER -> DONE [%s > %s]", category, subfolder)


async def scrape_mca(task: dict, week_start: datetime, week_end: datetime):

    """
    task = {
        "category":  "MCA",
        "subfolder": "Notifications",   # or "Circulars" / "Press Release"
        "url":       "https://www.mca.gov.in/content/mca/global/en/acts-rules/ebooks/notifications.html"
    }
    """
    if task["subfolder"].strip().lower() == "press release":
        return await scrape_mca_press_release(task, week_start, week_end)

    category  = task["category"]
    subfolder = task["subfolder"]
    page_url  = task["url"]
 
    logging.info("MCA SCRAPER -> [%s > %s] %s", category, subfolder, page_url)
 
    # ── Browser phase (retried automatically on window crash) ──
    rows, cookies, user_agent = _get_mca_snapshot(page_url)
 
    if not rows:
        logging.warning("MCA: no rows captured — skipping.")
        return
 
    # ── Offline phase: filter + download ──────────────────────
    seen_titles = set()   # dedup: skip duplicate titles in Excel
 
    for row in rows:
        title    = row["title"]
        date_str = row["date_str"]
        val      = row["val"]
        doc_cat  = row["doc_cat"]
 
        # Ignore filter
        if is_ignored_mca_title(title):
            logging.info("MCA: skipping (ignored): %s", title)
            continue
 
        # Parse date
        try:
            dt = datetime.strptime(date_str, "%d/%m/%Y")
        except Exception:
            logging.warning("MCA: bad date %r for: %s", date_str, title)
            continue
 
        # Week range filter
        if not (week_start <= dt <= week_end):
            logging.info("MCA: outside week (%s): %s", dt.date(), title[:60])
            continue
 
        if not val:
            logging.warning("MCA: no val for: %s", title)
            continue
 
        # Use data-doccategory as subfolder (auto-routes Notifications/Circulars/etc.)
        effective_subfolder = doc_cat if doc_cat else subfolder
 
        year       = str(dt.year)
        month_full = dt.strftime("%B")
 
        save_dir = ensure_year_month_structure(
            BASE_PATH, category, effective_subfolder, year, month_full
        )
 
        clean_title = title.split("|")[0].strip()
        title_key   = clean_title.lower().strip()
 
        # MCA sometimes lists same circular twice (e.g. English + Hindi versions)
        # Skip duplicates in Excel — each unique title recorded only once
        if title_key in seen_titles:
            logging.info("MCA: duplicate title skipped in Excel (val=%s): %s", val, clean_title[:60])
            continue
        seen_titles.add(title_key)
 
        # val suffix ensures unique filename even if same title appears twice
        base_name   = sanitize_filename(clean_title).replace(".pdf", "")
        filename    = f"{base_name}_{val[-6:]}.pdf"
        filepath    = os.path.join(save_dir, filename)
 
        ok = _download_mca_pdf(val, doc_cat, filepath, cookies, user_agent, page_url)
 
        if ok:
            ALL_DOWNLOADED.append({
                "Verticals":   MCA_DOMAIN_NAME,   # "Companies Act"
                "SubCategory": effective_subfolder,
                "Year":        year,
                "Month":       month_full,
                "IssueDate":   dt.strftime("%Y-%m-%d"),
                "Title":       clean_title,
                "PDF_URL":     _mca_dms_url(val, doc_cat),
                "File Name":   filename,
                "Path":        os.path.abspath(filepath),
            })
        else:
            logging.error("MCA: failed to download: %s", title)
 
    logging.info("MCA SCRAPER -> DONE [%s > %s]", category, subfolder)

# -------- HELPERS --------

def is_last_amended_title(title: str) -> bool:
    """
    Ignore SEBI amendment-only titles.
    Handles NBSPs, spacing, and punctuation variations.
    """
    if not title:
        return False

    # normalize unicode + spaces
    t = unicodedata.normalize("NFKD", title).lower()
    t = re.sub(r"\s+", " ", t)  # collapse all whitespace

    return (
        "last amended on" in t
        or "amended as on" in t
    )

def sanitize_filename(title: str, max_length: int = 100) -> str:
    # 1) Normalize unicode -> removes emojis, accents, fancy characters
    normalized = unicodedata.normalize("NFKD", title)
    ascii_text = normalized.encode("ascii", "ignore").decode()

    # 2) Replace all non-alphanumeric characters with _
    ascii_text = re.sub(r'[^A-Za-z0-9]+', '_', ascii_text)

    # 3) Remove repeated underscores
    ascii_text = re.sub(r'_+', '_', ascii_text)

    # 4) Remove leading/trailing underscores
    ascii_text = ascii_text.strip('_')

    # 5) Truncate safely
    if len(ascii_text) > max_length:
        ascii_text = ascii_text[:max_length]

    # 6) Guarantee filename exists
    if not ascii_text:
        ascii_text = "document"

    return ascii_text + ".pdf"

#-----------------------------------------------------

def ensure_year_month_structure(base_folder: str, category: str, subfolder: str, year: str, month_full: str) -> str:
    subfolder_path = os.path.join(base_folder, category, subfolder)
    year_path = os.path.join(subfolder_path, year)
    os.makedirs(year_path, exist_ok=True)
    month_path = os.path.join(year_path, month_full)
    os.makedirs(month_path, exist_ok=True)
    return month_path


async def download_pdf(session: aiohttp.ClientSession, pdf_url: str, save_dir: str, title: str | None = None) -> str | None:
    try:
        parsed = urlparse(pdf_url)
        qs = parse_qs(parsed.query)

        filename = qs.get("fileName", [None])[0]
        if filename:
            # Sanitize URL-provided filenames too — they can be very long
            filename = sanitize_filename(filename.replace(".pdf", "")[:80])
        else:
            filename = safe_pdf_filename(title, pdf_url)

        file_path = os.path.join(save_dir, filename)

        if os.path.exists(file_path):
            logging.warning("Overwriting existing file: %s", file_path)


        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,application/octet-stream,*/*",
        }

        # ---- RBI REFERER FIX ----
        if "rbidocs.rbi.org.in" in parsed.netloc:
            headers["Referer"] = "https://www.rbi.org.in/Scripts/NotificationUser.aspx"
        else:
            headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}"

        async with session.get(pdf_url, headers=headers, timeout=60) as resp:
            if resp.status != 200:
                logging.warning("IFSCA download failed (%s): %s", resp.status, pdf_url)
                return None

            data = await resp.read()
            content_type = resp.headers.get("Content-Type", "").lower()

            # HARD VALIDATION
            if not (
                data[:4] == b"%PDF"
                or "pdf" in content_type
                or "octet-stream" in content_type
            ):
                logging.error(
                    "Not a valid PDF. Content-Type=%s URL=%s",
                    content_type,
                    pdf_url,
                )
                return None

            os.makedirs(os.path.dirname(file_path), exist_ok=True)  # ← HERE ✓
            with open(file_path, "wb") as f:
                f.write(data)

            logging.info("Valid PDF saved -> %s", file_path)
            return file_path
        
    # except Exception as e:
    #     logging.exception("Error downloading PDF %s : %s", pdf_url, e)
    #     return None
    except Exception as e:
        logging.warning("aiohttp failed, retrying with requests: %s | %s", pdf_url, e)

        try:
            resp = requests.get(pdf_url, headers=headers, timeout=60)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "").lower()

            if not (
                resp.content[:4] == b"%PDF"
                or "pdf" in content_type
                or "octet-stream" in content_type
            ):
                logging.error(
                    "Fallback not a valid PDF. Content-Type=%s URL=%s",
                    content_type,
                    pdf_url,
                )
                return None

            os.makedirs(os.path.dirname(file_path), exist_ok=True)  # IMPORTANT

            with open(file_path, "wb") as f:
                f.write(resp.content)

            logging.info("Fallback PDF saved -> %s", file_path)
            return file_path

        except Exception as e2:
            logging.error("Requests fallback failed: %s | %s", pdf_url, e2)
            return None
#-----------------------------------------------------

async def direct_nse_pdf_download(pdf_url: str, save_path: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Referer": "https://www.nseindia.com/"
    }

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(pdf_url, timeout=30) as r:
                if r.status == 200:
                    data = await r.read()
                    with open(save_path, "wb") as f:
                        f.write(data)
                    logging.info("Direct NSE PDF downloaded: %s", save_path)
                    return True
                else:
                    logging.error("NSE PDF failed (%s): %s", r.status, pdf_url)
                    return False
    except Exception as e:
        logging.error("NSE Direct download error: %s", e)
        return False

async def scrape_nse(task, week_start, week_end):
    logging.info("NSE LISTED COMPANIES SCRAPER -> %s", task["url"])

    # 1) Crawl page using Crawl4AI
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=task["url"])

    soup = BeautifulSoup(result.html, "html.parser")
    rows = soup.select("table tbody tr")
    logging.info("NSE rows detected: %d", len(rows))

    if not rows:
        logging.error("No rows found on NSE page")
        return

    top_10 = rows[:10]
    logging.info("Processing top 10 NSE circulars")

    # -------- LOOP --------
    for row in top_10:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        title = cols[0].get_text(strip=True)
        normalized_title = normalize_title_for_compare(title)

        if normalized_title in BSE_TITLES_NORMALIZED:
            logging.info(
                "Skipping NSE circular (already downloaded from BSE): %s",
                title
            )
            continue

        # Extract date
        text = cols[1].get_text(" ", strip=True)
        date_match = re.search(r"\d{2}/\d{2}/\d{4}", text)
        if not date_match:
            logging.warning("Bad date format: %s", text)
            continue

        dt = datetime.strptime(date_match.group(), "%d/%m/%Y")

        # Week filter
        if not (week_start <= dt <= week_end):
            logging.info("Skipping %s (outside week)", dt.date())
            continue

        # Extract PDF viewer URL
        a = cols[1].find("a", href=True)
        if not a:
            logging.warning("No link for %s", title)
            continue

        pdf_url = a["href"]
        if pdf_url.startswith("//"):
            pdf_url = "https:" + pdf_url

        logging.info("NSE PDF URL: %s", pdf_url)

        # -------- DIRECT DOWNLOAD (NO SELENIUM) --------
        year = str(dt.year)
        month_full = dt.strftime("%B")

        save_dir = ensure_year_month_structure(
            BASE_PATH, task["category"], task["subfolder"], year, month_full
        )

        # filename = sanitize_filename(title)
        filename = safe_pdf_filename(title, pdf_url)

        file_path = os.path.join(save_dir, filename)

        success = await direct_nse_pdf_download(pdf_url, file_path)

        if not success:
            logging.error("NSE direct PDF failed: %s", pdf_url)
            continue

        logging.info("NSE PDF Saved -> %s", file_path)

        # Record in final Excel
        ALL_DOWNLOADED.append({
            "Verticals": task["category"],
            "SubCategory": task["subfolder"],
            "Year": year,
            "Month": month_full,
            "IssueDate": dt.strftime("%Y-%m-%d"),
            "Title": title,
            "PDF_URL": pdf_url,
            "File Name": filename,
            "Path": file_path
        })

    logging.info("NSE LISTED COMPANIES -> DONE")


# def get_latest_bse_pdf(existing, wait_seconds=15) -> str | None:
def get_latest_bse_pdf(download_dir, existing, wait_seconds=15) -> str | None:
    end_time = time.time() + wait_seconds

    while time.time() < end_time:
        # current = set(glob.glob(os.path.join(BSE_DOWNLOADS_DIR, "*.pdf")))
        current = set(glob.glob(os.path.join(download_dir, "*.pdf")))
        new_files = current - existing

        for f in new_files:
            if not f.endswith(".crdownload"):
                return f

        time.sleep(1)

    return None

def bse_get_pdf_url_from_detail_page(detail_url: str, driver) -> str | None:
    """
    Navigate to BSE detail page, use CDP to intercept the PDF network
    request triggered by clicking button.btnbr.
    """
    listing_url = driver.current_url
    captured_pdf_url = []

    try:
        # Enable CDP Network and listen for requests
        driver.execute_cdp_cmd("Network.enable", {})

        # Set up a JS-side interceptor using fetch/XHR monkey-patch BEFORE page loads
        driver.get(detail_url)
        time.sleep(5)

        # Inject JS to intercept window.open calls
        driver.execute_script("""
            window._bse_pdf_url = null;
            const _orig_open = window.open;
            window.open = function(url, ...args) {
                window._bse_pdf_url = url;
                return _orig_open.apply(this, arguments);
            };
            // Also intercept fetch
            const _orig_fetch = window.fetch;
            window.fetch = function(url, ...args) {
                if (typeof url === 'string' && url.toLowerCase().includes('pdf')) {
                    window._bse_pdf_url = url;
                }
                return _orig_fetch.apply(this, arguments);
            };
        """)

        try:
            pdf_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btnbr"))
            )
        except Exception:
            logging.warning("BSE detail: button.btnbr not found: %s", detail_url)
            return None

        existing_handles = set(driver.window_handles)

        # Click
        driver.execute_script("arguments[0].click();", pdf_button)
        time.sleep(4)

        # --- Strategy 1: check our window.open interceptor ---
        intercepted = driver.execute_script("return window._bse_pdf_url;")
        if intercepted:
            logging.info("BSE detail: intercepted window.open URL: %s", intercepted)
            if not intercepted.startswith("http"):
                intercepted = urljoin("https://www.bseindia.com", intercepted)
            return intercepted

        # --- Strategy 2: new tab opened ---
        new_handles = set(driver.window_handles) - existing_handles
        if new_handles:
            pdf_tab = new_handles.pop()
            driver.switch_to.window(pdf_tab)
            time.sleep(2)
            tab_url = driver.current_url
            logging.info("BSE detail: new tab URL: %s", tab_url)
            driver.close()
            driver.switch_to.window(list(existing_handles)[0])
            if tab_url and tab_url not in ("about:blank", detail_url):
                return tab_url

        # --- Strategy 3: check CDP network log for PDF requests ---
        try:
            logs = driver.execute_script("""
                return window.performance.getEntriesByType('resource')
                    .map(e => e.name);
            """)
            logging.info("BSE detail: all resource URLs: %s", logs[:10])
            for u in logs:
                if any(x in u.lower() for x in [".pdf", "getfile", "download", "circular"]):
                    logging.info("BSE detail: PDF from resource: %s", u)
                    return u
        except Exception as e:
            logging.warning("BSE: resource log failed: %s", e)

        # --- Strategy 4: check page source for any downloadable link ---
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tag in soup.select("embed, iframe, object"):
            src = tag.get("src", "") or tag.get("data", "")
            if src:
                return urljoin("https://www.bseindia.com", src)

        logging.warning("BSE detail: all strategies failed: %s", detail_url)
        return None

    except Exception as e:
        logging.warning("BSE detail page extraction failed: %s | %s", detail_url, e)
        return None

    finally:
        try:
            driver.execute_cdp_cmd("Network.disable", {})
        except Exception:
            pass
        try:
            if driver.current_url != listing_url:
                driver.get(listing_url)
                time.sleep(5)
        except Exception:
            pass

async def scrape_bse(task, week_start, week_end):
    logging.info("BSE SCRAPER (Angular) -> %s", task["url"])
    run_download_dir = tempfile.mkdtemp(prefix="bse_dl_")
    MAX_RETRIES = 3
    driver = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info("BSE: driver attempt %d/%d", attempt, MAX_RETRIES)

            # if _UC_AVAILABLE:
            #     opts = uc.ChromeOptions()
            #     opts.add_argument("--no-sandbox")
            #     opts.add_argument("--disable-dev-shm-usage")
            #     # driver = uc.Chrome(options=opts, version_main=147)
            #     driver = uc.Chrome(options=opts, version_main=148)
            if _UC_AVAILABLE:
                opts = uc.ChromeOptions()
                opts.add_argument("--no-sandbox")
                opts.add_argument("--disable-dev-shm-usage")
                # Force Chrome to download PDFs instead of opening in viewer
                prefs = {
                    "download.default_directory": run_download_dir,
                    "download.prompt_for_download": False,
                    "plugins.always_open_pdf_externally": True,
                    "download.directory_upgrade": True,
                }
                opts.add_experimental_option("prefs", prefs)
                driver = uc.Chrome(options=opts, version_main=148)
            else:
                opts = webdriver.ChromeOptions()
                opts.add_argument("--no-sandbox")
                opts.add_argument("--disable-dev-shm-usage")
                opts.add_argument("--disable-blink-features=AutomationControlled")
                opts.add_experimental_option("excludeSwitches", ["enable-automation"])
                opts.add_experimental_option("useAutomationExtension", False)
                driver = webdriver.Chrome(options=opts)

            time.sleep(2)  # let uc stabilise before touching the window

            driver.get(task["url"])
            logging.info("BSE: page loaded, waiting for Angular table...")

            time.sleep(5)  # let Angular fully render
            driver.execute_script("window.scrollTo(0, 300);")
            time.sleep(2)

            selectors = [
                "table tr td.tdcolumn",
                "table tr td",
                "tr td a[href*='bseindia.com/downloads']",
                "tbody tr",
            ]

            found = False
            for sel in selectors:
                try:
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                    )
                    logging.info("BSE: table found with selector: %s", sel)
                    found = True
                    break
                except Exception:
                    logging.warning("BSE: selector not found: %s", sel)
                    continue

            if not found:
                logging.warning("BSE: no selector matched — waiting 15s and trying anyway")
                time.sleep(15)

            page_src = driver.page_source
            logging.info("BSE: page source length: %d", len(page_src))

            if "tdcolumn" in page_src:
                logging.info("BSE: 'tdcolumn' found in page source — Angular rendered OK")
            elif "Circulars" in page_src:
                logging.info("BSE: 'Circulars' found but tdcolumn missing — partial render")
            else:
                logging.error("BSE: page source seems wrong — possible block or redirect")
                logging.info("BSE page snippet: %s", page_src[:2000])
                return

            soup = BeautifulSoup(page_src, "html.parser")
            rows = soup.select("table tr")
            logging.info("BSE: total rows found: %d", len(rows))

            async with aiohttp.ClientSession() as session:
                for row in rows:
                    cols = row.find_all("td", class_="tdcolumn")
                    if len(cols) < 2:
                        continue

                    a = cols[0].find("a", href=True)
                    if not a:
                        continue

                    # title = a.get_text(strip=True)
                    # pdf_url = a["href"]

                    title = a.get_text(strip=True)
                    pdf_url = a["href"]

                    if pdf_url.startswith("/"):
                        pdf_url = urljoin("https://www.bseindia.com", pdf_url)

                    actual_pdf_url = pdf_url

                    is_detail_page = (
                        "DispNewNoticesCirculars?page=" in pdf_url
                        or not pdf_url.lower().endswith(".pdf")
                    )

                    
                    # ---- DATE CHECK FIRST — skip before any Selenium work ----
                    date_text = cols[1].get_text(strip=True)
                    try:
                        dt = datetime.strptime(date_text.strip(), "%B %d, %Y")
                    except Exception:
                        logging.warning("BSE bad date: %s", date_text)
                        continue

                    if not (week_start <= dt <= week_end):
                        logging.info("BSE skipping outside week: %s | %s", dt.date(), title[:60])
                        continue

                    # ---- Only visit detail page if date is in range ----

                    if is_detail_page:
                        logging.info("BSE detail page detected: %s", pdf_url)

                        existing_downloads = set(
                            glob.glob(os.path.join(run_download_dir, "*.pdf"))
                        )

                        actual_pdf_url = bse_get_pdf_url_from_detail_page(pdf_url, driver)

                        logging.info("BSE detail: resolved URL = %r", actual_pdf_url)

                        if not actual_pdf_url:
                            logging.warning("BSE: could not extract PDF from detail page: %s", pdf_url)
                            continue

                        logging.info("BSE: resolved PDF URL: %s", actual_pdf_url)
                    normalized_title = normalize_title_for_compare(title)
                    if normalized_title in BSE_TITLES_NORMALIZED:
                        logging.info("BSE duplicate skipped: %s", title)
                        continue

                    year = str(dt.year)
                    month_full = dt.strftime("%B")

                    save_dir = ensure_year_month_structure(
                        BASE_PATH,
                        task["category"],
                        task["subfolder"],
                        year,
                        month_full
                    )

                    # For detail pages, Chrome already downloaded the PDF to Downloads
                    # For direct PDF links, use normal download
                    if is_detail_page:
                        # latest_pdf = get_latest_bse_pdf(wait_seconds=15)
                        # latest_pdf = get_latest_bse_pdf(existing_downloads,wait_seconds=15)
                        latest_pdf =get_latest_bse_pdf(run_download_dir,existing_downloads,wait_seconds=15)
                        if not latest_pdf:
                            logging.error("BSE: browser download not found for: %s", title)
                            continue

                        filename = safe_pdf_filename(title, actual_pdf_url)
                        final_path = os.path.join(save_dir, filename)
                        # shutil.move(latest_pdf, final_path)
                        # downloaded_path = final_path
                        shutil.move(latest_pdf, final_path)

                        downloaded_filename = os.path.basename(latest_pdf)

                        # if (
                        #     not actual_pdf_url
                        #     or "DispNewNoticesCirculars" in actual_pdf_url
                        #     or not actual_pdf_url.lower().endswith(".pdf")
                        # ):
                        #     actual_pdf_url = (
                        #         "https://www.bseindia.com/downloads/UploadDocs/Notices/"
                        #         + downloaded_filename
                        #     )
                        if (
                            not actual_pdf_url
                            or "DispNewNoticesCirculars" in actual_pdf_url
                            or not actual_pdf_url.lower().endswith(".pdf")
                        ):
                            actual_pdf_url = pdf_url
                        downloaded_path = final_path
                        logging.info("BSE: moved browser download -> %s", final_path)
                    else:
                        downloaded_path = await download_pdf(
                            session,
                            actual_pdf_url,
                            save_dir,
                            title
                        )
                        if not downloaded_path:
                            logging.error("BSE PDF failed: %s", pdf_url)
                            continue
                    filename = os.path.basename(downloaded_path)

                    ALL_DOWNLOADED.append({
                        "Verticals": task["category"],
                        "SubCategory": task["subfolder"],
                        "Year": year,
                        "Month": month_full,
                        "IssueDate": dt.strftime("%Y-%m-%d"),
                        "Title": title,
                        # "PDF_URL": pdf_url,
                        "PDF_URL": actual_pdf_url,
                        "File Name": filename,
                        "Path": os.path.abspath(downloaded_path)
                    })

                    BSE_TITLES_NORMALIZED.add(normalized_title)
                    logging.info("BSE downloaded: %s", filename)

            # success — break out of retry loop
            break

        except (NoSuchWindowException, WebDriverException) as exc:
            logging.warning("BSE: driver window lost on attempt %d/%d (%s). Retrying...",
                            attempt, MAX_RETRIES, exc)
            time.sleep(3)

        except (BrokenPipeError, OSError, ConnectionError) as exc:
            logging.warning("BSE: network/pipe error on attempt %d/%d (%s). Retrying...",
                            attempt, MAX_RETRIES, exc)
            time.sleep(5)

        except Exception as e:
            logging.exception("BSE scraper error: %s", e)
            break  # non-driver errors shouldn't retry
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None
            try:
                shutil.rmtree(run_download_dir, ignore_errors=True)
            except Exception:
                pass
    logging.info("BSE SCRAPER -> DONE")

async def scrape_sebi_informal_guidance(task, week_start, week_end):
    logging.info("SEBI INFORMAL GUIDANCE SCRAPER -> %s", task["url"])

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=task["url"])
    
    soup = BeautifulSoup(result.html, "html.parser")
    # 1. Get all rows from the listing table
    rows = soup.find_all("tr", class_=["odd", "even"])

    async with aiohttp.ClientSession() as session:
        for row in rows:
            # Extract Date from the first <td>
            date_td = row.find("td")
            if not date_td: continue
            
            date_text = date_td.get_text(strip=True)
            try:
                dt = datetime.strptime(date_text, "%b %d, %Y")
            except ValueError: continue

            # --- WEEK RANGE FILTER ---
            if dt < week_start: break  # SEBI is usually chronological
            if dt > week_end: continue

            # 2. Get Title and Detail Page Link
            link_tag = row.select_one("a.points")
            if not link_tag: continue
            
            detail_url = urljoin("https://www.sebi.gov.in", link_tag["href"])
            title = link_tag.get("title") or link_tag.get_text(strip=True)
            title = unicodedata.normalize("NFKD", title)

            # --- IGNORE LOGIC ---
            if is_ignored_sebi_title(title):
                logging.info("Skipping (Ignored Keyword): %s", title)
                continue

            # 3. OPEN DETAIL PAGE to find the actual PDF
            try:
                async with session.get(detail_url, timeout=30) as resp:
                    if resp.status != 200: continue
                    detail_html = await resp.text()
                
                detail_soup = BeautifulSoup(detail_html, "html.parser")
                # Get the complete title from the detail page
                for tag in detail_soup.find_all(["h1", "h2", "h3", "h4", "strong", "b", "p", "td"]):
                    text = tag.get_text(" ", strip=True)
                    if text.startswith("Request for Informal Guidance"):
                        title = text
                        break

                # Look for the specific link text you mentioned
                pdf_link_tag = detail_soup.find("a", string=re.compile("Informal Guidance Letter by SEBI", re.I))
                
                if pdf_link_tag and pdf_link_tag.get("href"):
                    pdf_url = urljoin(detail_url, pdf_link_tag["href"])
                else:
                    # Fallback to your existing iframe helper if the specific text isn't found
                    iframe = detail_soup.select_one("iframe")
                    pdf_url = extract_sebi_pdf_from_iframe(iframe.get("src"), detail_url) if iframe else None

                if not pdf_url:
                    logging.warning("No PDF link found for: %s", title)
                    continue

                # 4. DOWNLOAD AND RECORD
                year, month_full = str(dt.year), dt.strftime("%B")
                save_dir = ensure_year_month_structure(BASE_PATH, task["category"], task["subfolder"], year, month_full)
                
                downloaded_path = await download_pdf(session, pdf_url, save_dir, title)

                if downloaded_path:
                    ALL_DOWNLOADED.append({
                        "Verticals": task["category"],
                        "SubCategory": task["subfolder"],
                        "Year": year,
                        "Month": month_full,
                        "IssueDate": dt.strftime("%Y-%m-%d"),
                        "Title": title,
                        "PDF_URL": pdf_url,
                        "File Name": os.path.basename(downloaded_path),
                        "Path": os.path.abspath(downloaded_path)
                    })
            except Exception as e:
                logging.error("Error processing %s: %s", detail_url, e)

async def scrape_sebi(task, week_start, week_end):
    category = task["category"]
    subfolder = task["subfolder"]
    detail_url = task["url"]

    logging.info("SEBI Scraper -> [%s > %s]: %s", category, subfolder, detail_url)

    # ---- Crawl page ----
    async with AsyncWebCrawler() as crawler:
        try:
            detail_result = await crawler.arun(url=detail_url)
        except Exception as e:
            logging.exception("Crawler failed for %s : %s", detail_url, e)
            return

    soup_detail = BeautifulSoup(detail_result.html, "html.parser")

    # ---- Extract title ----
    if "title" not in task:
        title_elem = soup_detail.select_one("h1, h2, h3")
        if title_elem:
            task["title"] = title_elem.get_text(strip=True)
        else:
            logging.warning("No title found at %s", detail_url)
            task["title"] = "Untitled"
    
    # ---- SKIP "Last amended on" regulations ----
    if is_last_amended_title(task["title"]):
        logging.info(
            "Skipping regulation (Last amended on): %s",
            task["title"]
        )
        return

    # ---- SKIP non-relevant SEBI PDFs based on title ----
    if category == "SEBI" and is_ignored_sebi_title(task["title"]):
        logging.info(
            "Skipping SEBI document based on ignore list: %s",
            task["title"]
        )
        return

    # ---- Detect nested listing pages ----
    # if "doListing=yes" in detail_url:
    if "doListing" in detail_url:

        detail_links = extract_detail_links_from_listing(detail_result.html, detail_url)

        if not detail_links:
            logging.warning("No detail links inside listing: %s", detail_url)
            return

        logging.info("Found %d SEBI inner links in listing: %s", len(detail_links), detail_url)

        for item in detail_links:
            await scrape_sebi(
                {
                    "category": category,
                    "subfolder": subfolder,
                    "url": item["url"],
                    "title": item["title"]
                },
                week_start,
                week_end
            )

        return

    # ---- Extract date ----
    date_elem = soup_detail.select_one("h5")
    if not date_elem:
        logging.warning("No date found at %s", detail_url)
        return

    try:
        dt = datetime.strptime(date_elem.get_text(strip=True), "%b %d, %Y")
    except Exception:
        logging.warning("Invalid date format for %s", detail_url)
        return

    # ---- Week range filter ----
    if not (week_start <= dt <= week_end):
        logging.info("Skipping (out of weekly range): %s", dt.date())
        return

    year = str(dt.year)
    month_full = dt.strftime("%B")

    # ---- AIF Logic only for SEBI category ----
    original_category = category
    if original_category == "SEBI":
        if detect_aif_category(task["title"]):
            logging.info("AIF detected -> storing under AIF")
            category = "AIF"
        else:
            category = "SEBI"

    # ---- Folder Structure ----
    save_path = ensure_year_month_structure(
        BASE_PATH, category, subfolder, year, month_full
    )

    # ---- Detect PDF ----

    pdf_url = None

    iframe = soup_detail.select_one("iframe")
    if iframe:
        pdf_url = extract_sebi_pdf_from_iframe(
            iframe.get("src"),
            detail_url
        )

    # fallback: download button
    if not pdf_url:
        pdf_btn = soup_detail.select_one("button#download")
        if pdf_btn:
            pdf_url = detail_url.replace(".html", ".pdf")


    file_path = None

    # ---- Try direct PDF download ----
    if pdf_url:
        async with aiohttp.ClientSession() as session:
            # file_path = await download_pdf(session, pdf_url, save_path)
            file_path = await download_pdf(
                session,
                pdf_url,
                save_path,
                title=task["title"]
            )

    # ---- Fallback -> printToPDF ----
    if not file_path:
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless=new")
            driver = webdriver.Chrome(options=options)

            driver.get(detail_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.TAG_NAME, "body"))
            )

            pdf_data = base64.b64decode(
                driver.execute_cdp_cmd("Page.printToPDF", {"printBackground": True})["data"]
            )

            # filename = sanitize_filename(task["title"])
            filename = safe_pdf_filename(task["title"], detail_url)
            file_path = os.path.join(save_path, filename)

            with open(file_path, "wb") as f:
                f.write(pdf_data)

        except Exception:
            logging.exception("PrintToPDF fallback failed: %s", detail_url)
            file_path = None

        finally:
            try:
                driver.quit()
            except:
                pass

    # ---- Finally append to results ----
    if file_path:
        ALL_DOWNLOADED.append({
            "Verticals": category,
            "SubCategory": subfolder,
            "Year": year,
            "Month": month_full,
            "IssueDate": dt.strftime("%Y-%m-%d"),
            "Title": task["title"],
            "PDF_URL": pdf_url if pdf_url else "PrintToPDF",
            "File Name": os.path.basename(file_path),
            "Path": os.path.abspath(file_path)
        })


# -----------------------------------------------------

def is_ifsca_public_consultation(url: str) -> bool:
    """
    Detects IFSCA Public Consultation UI
    """
    return "ReportPublication/index" in url

async def scrape_ifsca_public_consultation(task, week_start, week_end):
    logging.info("IFSCA PUBLIC CONSULTATION SCRAPER -> %s", task["url"])

    chrome_opts = webdriver.ChromeOptions()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_opts)

    try:
        driver.get(task["url"])

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#tblReportFront tbody tr")
            )
        )

        async with aiohttp.ClientSession() as session:
            page_no = 1

            while True:
                logging.info("IFSCA Public Consultation -> page %d", page_no)

                soup = BeautifulSoup(driver.page_source, "html.parser")
                rows = soup.select("#tblReportFront tbody tr")

                if not rows:
                    break

                stop_pagination = False

                for row in rows:
                    try:
                        date_td = row.select_one('td[data-label="Date"] span')
                        title_td = row.select_one('td[data-label="Title"] span')
                        download_a = row.select_one('td[data-label="Download"] a[href]')

                        if not date_td or not title_td or not download_a:
                            continue

                        dt = datetime.strptime(
                            date_td.get_text(strip=True), "%d/%m/%Y"
                        )

                        # sorted DESC – once old, stop everything
                        if dt < week_start:
                            stop_pagination = True
                            continue

                        if not (week_start <= dt <= week_end):
                            continue

                        title = title_td.get_text(strip=True)

                        # GLOBAL IFSCA TITLE FILTER
                        if is_ignored_ifsca_title(title):
                            logging.info("Skipping IFSCA PC (filtered title): %s", title)
                            continue

                        pdf_url = urljoin("https://ifsca.gov.in", download_a["href"])

                        year = str(dt.year)
                        month_full = dt.strftime("%B")

                        save_dir = ensure_year_month_structure(
                            BASE_PATH,
                            task["category"],
                            task["subfolder"],
                            year,
                            month_full,
                        )

                        downloaded_path = await download_pdf(
                            session, pdf_url, save_dir, title
                        )

                        if downloaded_path:
                            ALL_DOWNLOADED.append({
                                "Verticals": task["category"],
                                "SubCategory": task["subfolder"],
                                "Year": year,
                                "Month": month_full,
                                "IssueDate": dt.strftime("%Y-%m-%d"),
                                "Title": title,
                                "PDF_URL": pdf_url,
                                "File Name": os.path.basename(downloaded_path),
                                "Path": os.path.abspath(downloaded_path),
                            })
                            logging.info("IFSCA Public Consultation downloaded: %s", title)

                    except Exception as e:
                        logging.warning("IFSCA PC row parse error: %s", e)

                if stop_pagination:
                    logging.info("Reached older Public Consultation records.")
                    break

                # pagination click
                try:
                    next_btn = driver.find_element(By.ID, "tblReportFront_next")
                    if "disabled" in next_btn.get_attribute("class"):
                        break

                    first_row_text = rows[0].get_text(strip=True)
                    next_btn.click()

                    WebDriverWait(driver, 15).until_not(
                        EC.text_to_be_present_in_element(
                            (By.CSS_SELECTOR, "#tblReportFront tbody tr"),
                            first_row_text,
                        )
                    )

                    page_no += 1

                except Exception:
                    break

    finally:
        driver.quit()
        logging.info("IFSCA PUBLIC CONSULTATION -> DONE")

async def scrape_ifsca(task, week_start, week_end):
    logging.info("IFSCA Scraper -> %s", task["url"])

    chrome_opts = webdriver.ChromeOptions()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_opts)

    try:
        driver.get(task["url"])

        # wait for DataTables rows (NOT just table)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#tblLegalFront tbody tr")
            )
        )

        async with aiohttp.ClientSession() as session:
            page_no = 1

            while True:
                logging.info("IFSCA scanning page %d", page_no)

                soup = BeautifulSoup(driver.page_source, "html.parser")
                rows = soup.select("#tblLegalFront tbody tr")

                logging.info("IFSCA rows found on page %d: %d", page_no, len(rows))

                if not rows:
                    break

                stop_pagination = False

                for row in rows:
                    is_discussion_paper = task["subfolder"] == "Discussion Paper"

                    try:
                        date_td = row.select_one('td[data-label="Date"]')
                        title_td = row.select_one('td[data-label="Title"]')
                        # download_a = row.select_one('td[data-label="Download"] a[href]')
                        if is_discussion_paper:
                            download_a = row.select_one('td[data-label="Discussion Paper"] a[href]')
                        else:
                            download_a = row.select_one('td[data-label="Download"] a[href]')

                        if not date_td or not title_td or not download_a:
                            continue

                        dt = datetime.strptime(
                            date_td.get_text(strip=True), "%d/%m/%Y"
                        )

                        # table sorted DESC -> once older, stop fully
                        if dt < week_start:
                            stop_pagination = True
                            continue

                        if not (week_start <= dt <= week_end):
                            continue

                        title = title_td.get_text(strip=True)

                        # IFSCA title filter
                        if is_ignored_ifsca_title(title):
                            logging.info("Skipping IFSCA (filtered title): %s", title)
                            continue

                        pdf_url = urljoin(
                            "https://ifsca.gov.in", download_a["href"]
                        )

                        year = str(dt.year)
                        month_full = dt.strftime("%B")

                        save_dir = ensure_year_month_structure(
                            BASE_PATH,
                            task["category"],
                            task["subfolder"],
                            year,
                            month_full,
                        )

                        downloaded_path = await download_pdf(
                            session, pdf_url, save_dir, title
                        )

                        if downloaded_path:
                            ALL_DOWNLOADED.append({
                                "Verticals": task["category"],
                                "SubCategory": task["subfolder"],
                                "Year": year,
                                "Month": month_full,
                                "IssueDate": dt.strftime("%Y-%m-%d"),
                                "Title": title,
                                "PDF_URL": pdf_url,
                                "File Name": os.path.basename(downloaded_path),
                                "Path": os.path.abspath(downloaded_path),
                            })
                            logging.info("IFSCA downloaded: %s", title)

                    except Exception as e:
                        logging.warning("IFSCA row parse error: %s", e)

                if stop_pagination:
                    logging.info("Reached older IFSCA records. Stopping pagination.")
                    break

                # click NEXT
                try:
                    next_btn = driver.find_element(By.ID, "tblLegalFront_next")
                    if "disabled" in next_btn.get_attribute("class"):
                        break

                    first_row_text = rows[0].get_text(strip=True)
                    next_btn.click()

                    WebDriverWait(driver, 15).until_not(
                        EC.text_to_be_present_in_element(
                            (By.CSS_SELECTOR, "#tblLegalFront tbody tr"),
                            first_row_text,
                        )
                    )

                    page_no += 1

                except Exception:
                    break

    finally:
        driver.quit()
        logging.info("IFSCA -> DONE")

def select_response_pdf(pdfs):

    if len(pdfs) == 1:
        return pdfs[0]

    llm = Ollama(
        model="mistral:latest",
        temperature=0.0
    )

    titles_text = "\n".join(
        [f"{i+1}. {p['title']}" for i, p in enumerate(pdfs)]
    )

    prompt = f"""
You are filtering IFSCA Informal Guidance documents.

The titles below belong to the same Informal Guidance case.

One document is usually:
- applicant request letter
- request letter
- query letter

Another document is usually:
- IFSCA response
- interpretative letter
- informal guidance
- clarification issued by IFSCA

Select ONLY the IFSCA response document.

Titles:
{titles_text}

Return ONLY JSON.

{{
    "selected_title":"exact title"
}}
"""

    response = llm.invoke(prompt).strip()

    if "```json" in response:
        response = response.split("```json")[1].split("```")[0].strip()

    data = json.loads(response)

    selected_title = data["selected_title"]

    for pdf in pdfs:
        if pdf["title"] == selected_title:
            return pdf

    return pdfs[0]


async def scrape_ifsca_informal_guidance(
    task,
    week_start,
    week_end
):
    logging.info(
        "IFSCA INFORMAL GUIDANCE SCRAPER STARTING -> %s",
        task["url"]
    )

    chrome_opts = webdriver.ChromeOptions()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_opts)

    try:

        wait = WebDriverWait(driver, 20)

        driver.get(task["url"])

        rows = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "tbody tr")
            )
        )

        logging.info(
            "IFSCA IG MAIN ROWS FOUND: %s",
            len(rows)
        )

        # --------------------------------------------------
        # SNAPSHOT ROW DATA
        # --------------------------------------------------

        row_data = []

        for row in rows:

            cols = row.find_elements(By.TAG_NAME, "td")

            if len(cols) < 3:
                continue

            try:
                dt = datetime.strptime(
                    cols[1].text.strip(),
                    "%d/%m/%Y"
                )
            except:
                continue

            title_link = cols[2].find_element(
                By.TAG_NAME,
                "a"
            )

            row_data.append({
                "dt": dt,
                "main_title": title_link.text.strip(),
                "detail_url": title_link.get_attribute(
                    "href"
                )
            })

        session = requests.Session()

        for cookie in driver.get_cookies():
            session.cookies.set(
                cookie["name"],
                cookie["value"]
            )

        # --------------------------------------------------
        # PROCESS SNAPSHOT DATA
        # --------------------------------------------------

        for item in row_data:

            dt = item["dt"]

            if not (
                week_start <= dt <= week_end
            ):
                continue

            main_title = item["main_title"]
            detail_url = item["detail_url"]

            logging.info(
                "Opening inner page -> %s",
                detail_url
            )

            driver.get(detail_url)

            pdf_links = wait.until(
                EC.presence_of_all_elements_located(
                    (
                        By.CSS_SELECTOR,
                        "tbody tr td:nth-child(3) a"
                    )
                )
            )

            pdf_data = []

            for pdf in pdf_links:

                pdf_data.append({
                    "title": pdf.text.strip(),
                    "getfile_url": pdf.get_attribute(
                        "href"
                    )
                })

            logging.info(
                "IFSCA IG PDF LINKS FOUND: %s",
                len(pdf_data)
            )

            for pdf in pdf_data:
                logging.info(
                    "PDF FOUND -> %s",
                    pdf["title"]
                )

            # --------------------------------------------------
            # LLM FILTER
            # --------------------------------------------------

            selected_pdf = select_response_pdf(
                pdf_data
            )

            logging.info(
                "LLM SELECTED -> %s",
                selected_pdf["title"]
            )

            # --------------------------------------------------
            # OPEN PDF PAGE
            # --------------------------------------------------

            driver.get(
                selected_pdf["getfile_url"]
            )

            iframe = wait.until(
                EC.presence_of_element_located(
                    (By.TAG_NAME, "iframe")
                )
            )

            real_pdf_url = iframe.get_attribute(
                "src"
            )

            year = str(dt.year)
            month_full = dt.strftime("%B")

            save_dir = ensure_year_month_structure(
                BASE_PATH,
                task["category"],
                task["subfolder"],
                year,
                month_full
            )

            filename = real_pdf_url.split(
                "fileName="
            )[-1]

            filepath = os.path.join(
                save_dir,
                filename
            )

            response = session.get(
                real_pdf_url,
                headers={
                    "Referer":
                        selected_pdf["getfile_url"],
                    "User-Agent":
                        "Mozilla/5.0"
                },
                timeout=60
            )

            if response.status_code != 200:
                logging.warning(
                    "Failed to download PDF -> %s",
                    real_pdf_url
                )
                continue

            with open(filepath, "wb") as f:
                f.write(response.content)

            ALL_DOWNLOADED.append({
                "Verticals":
                    task["category"],
                "SubCategory":
                    task["subfolder"],
                "Year":
                    year,
                "Month":
                    month_full,
                "IssueDate":
                    dt.strftime("%Y-%m-%d"),

                # MAIN TITLE
                "Title":
                    main_title,

                # RESPONSE PDF URL
                "PDF_URL":
                    real_pdf_url,

                "File Name":
                    filename,

                "Path":
                    os.path.abspath(filepath)
            })

            logging.info(
                "Downloaded IG response -> %s",
                filename
            )

    except Exception:
        logging.exception(
            "IFSCA Informal Guidance scraper failed"
        )

    finally:
        driver.quit()
# -----------------------------------------------------

def is_ignored_rbi_title(title: str) -> bool:
    """
    Returns True if RBI notification title should be skipped.
    Case-insensitive keyword match with normalization.
    """
    if not title:
        return False

    t = unicodedata.normalize("NFKD", title).lower()
    t = re.sub(r"\s+", " ", t)

    ignore_keywords = [
        # Auctions
        "auction",
        "auction results",

        # Money market
        "money market operations",
        "variable rate repo",

        # Securities related
        "conversion",
        "redemption",
        "state government securities",

        # Penalties / data
        "monetary penalty",
        "turnover data",

        # Publications
        "bulletin",
        "release of data",
    ]

    return any(kw in t for kw in ignore_keywords)


async def scrape_rbi(task, week_start, week_end):
    logging.info("RBI SCRAPER -> %s", task["url"])

    # ── Step 1: Crawl listing page ──────────────────────────
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=task["url"])

    soup = BeautifulSoup(result.html, "html.parser")
    rows = soup.select("table tr")

    if not rows:
        logging.warning("No RBI rows found")
        return

    # ── Step 2: Build session with proper headers ───────────
    connector = aiohttp.TCPConnector(ssl=False)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers,
        cookie_jar=aiohttp.CookieJar()      # ← explicit jar
    ) as session:

        # ── Step 3: Warm-up — MUST consume body so cookies stick ──
        try:
            async with session.get(
                "https://www.rbi.org.in/Scripts/NotificationUser.aspx",
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as warm:
                await warm.read()           # ← THIS was missing before
                logging.info(
                    "RBI warm-up done: status=%s cookies=%s",
                    warm.status,
                    [c.key for c in session.cookie_jar]
                )
        except Exception as e:
            logging.warning("RBI warm-up failed (continuing anyway): %s", e)

        # Human-like pause after landing on site
        await asyncio.sleep(random.uniform(2, 4))

        # ── Step 4: Parse rows ──────────────────────────────
        current_dt = None

        for row in rows:

            # ---- DATE HEADER ----
            date_h2 = row.select_one("h2.dop_header")
            if date_h2:
                text = date_h2.get_text(strip=True)
                try:
                    current_dt = datetime.strptime(text, "%b %d, %Y")
                except ValueError:
                    pass   # section heading — keep current_dt
                continue

            if not current_dt:
                continue

            # ---- WEEK FILTER ----
            if current_dt < week_start:
                continue
            if current_dt > week_end:
                continue

            # ---- TITLE & PDF LINK ----
            title_a = row.select_one("a.link2")
            pdf_a   = row.select_one("a[href*='rbidocs.rbi.org.in']")

            if not title_a or not pdf_a:
                continue

            title = unicodedata.normalize("NFKD", title_a.get_text(strip=True))

            if is_ignored_rbi_title(title):
                logging.info("Skipping RBI (filtered): %s", title)
                continue

            pdf_url    = pdf_a["href"]
            year       = str(current_dt.year)
            month_full = current_dt.strftime("%B")

            save_dir = ensure_year_month_structure(
                BASE_PATH, task["category"], task["subfolder"], year, month_full
            )

            # ── Step 5: Polite delay BEFORE every download ──
            delay = random.uniform(3, 7)
            logging.info("RBI: sleeping %.1fs before download...", delay)
            await asyncio.sleep(delay)

            downloaded_path = await download_pdf(session, pdf_url, save_dir, title)

            if downloaded_path:
                ALL_DOWNLOADED.append({
                    "Verticals":   task["category"],
                    "SubCategory": task["subfolder"],
                    "Year":        year,
                    "Month":       month_full,
                    "IssueDate":   current_dt.strftime("%Y-%m-%d"),
                    "Title":       title,
                    "PDF_URL":     pdf_url,
                    "File Name":   os.path.basename(downloaded_path),
                    "Path":        os.path.abspath(downloaded_path),
                })
                logging.info("RBI downloaded: %s", title)
            else:
                logging.warning("RBI download failed: %s", title)


#-----------------------------------------------------
def is_ignored_icai_title(title: str) -> bool:
    """
    Returns True if ICAI title should be skipped.
    Case-insensitive keyword match.
    """
    if not title:
        return False

    ignore_keywords = [
        "test",
        "results",
        "commencement of batch",
        "courses",
        "exams",
        "fees",
        "books",
        "exam",
        "result",
        "course",
        "book"
    ]

    t = title.lower()
    return any(kw in t for kw in ignore_keywords)

async def scrape_icai(task, week_start, week_end):
    logging.info("ICAI SCRAPER -> %s", task["url"])

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=task["url"])

    soup = BeautifulSoup(result.html, "html.parser")

    items = soup.select("li.list-group-item a[href]")

    if not items:
        logging.warning("No ICAI items found")
        return

    async with aiohttp.ClientSession() as session:
        for a in items:

            full_text = a.get_text(strip=True)
            # pdf_url = a["href"]
            pdf_url = urljoin(task["url"], a["href"])
            # --- Extract Date from (...) ---
            date_match = re.search(r"\((\d{2}-\d{2}-\d{4})\)", full_text)
            if not date_match:
                continue

            try:
                dt = datetime.strptime(date_match.group(1), "%d-%m-%Y")
            except Exception:
                continue

            # --- Week Filter ---
            if not (week_start <= dt <= week_end):
                continue

            # --- Remove date from title ---
            # title = re.sub(r"\(\d{2}-\d{2}-\d{4}\)", "", full_text).strip()
            title = re.sub(r"\(\d{2}-\d{2}-\d{4}\)", "", full_text)
            title = re.sub(r"\s+", " ", title).strip()
            title = unicodedata.normalize("NFKD", title)
            # --- Ignore filter ---
            if is_ignored_icai_title(title):
                logging.info("Skipping ICAI (filtered title): %s", title)
                continue

            year = str(dt.year)
            month_full = dt.strftime("%B")

            save_dir = ensure_year_month_structure(
                BASE_PATH,
                task["category"],
                task["subfolder"],
                year,
                month_full
            )

            downloaded_path = await download_pdf(
                session,
                pdf_url,
                save_dir,
                title
            )

            if downloaded_path:
                ALL_DOWNLOADED.append({
                    "Verticals": task["category"],
                    "SubCategory": task["subfolder"],
                    "Year": year,
                    "Month": month_full,
                    "IssueDate": dt.strftime("%Y-%m-%d"),
                    "Title": title,
                    "PDF_URL": pdf_url,
                    "File Name": os.path.basename(downloaded_path),
                    "Path": os.path.abspath(downloaded_path),
                })

                logging.info("ICAI downloaded: %s", title)


#-----------------------------------------------------
def is_ignored_ibbi_title(title: str) -> bool:
    """
    Returns True if IBBI title should be ignored.
    Case-insensitive keyword match.
    """
    if not title:
        return False

    ignore_keywords = [
        "approval of resolution plan",
        "annual publication",
        "conferences",
        "quarterly newsletter",
        "panel",
        "appointments",
        "invitation for expression of interest",
        "quiz",
        "provisional list",
        "research study",
        "reports",
    ]

    title_lower = title.lower()
    return any(kw in title_lower for kw in ignore_keywords)


async def scrape_ibbi_discussion_paper(task, week_start, week_end):
    logging.info("IBBI DISCUSSION PAPER SCRAPER -> %s", task["url"])

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=task["url"])

    soup = BeautifulSoup(result.html, "html.parser")

    # IBBI Discussion Paper tables often use 't-row' class or standard tr
    # We select all rows and filter them dynamically
    rows = soup.find_all("tr")

    if not rows:
        logging.warning("No IBBI Discussion Paper rows found")
        return

    async with aiohttp.ClientSession() as session:
        for row in rows:
            # IBBI uses <th> for data cells in some tables and <td> in others
            cells = row.find_all(["td", "th"])
            
            # Basic validation: need at least Date, Title, and Link columns
            if len(cells) < 3:
                continue

            # ---- CLEANING & HEADER SKIP ----
            # Get text from first cell to check if it's the header "Date"
            raw_date_cell = cells[0].get_text(" ", strip=True)
            if "date" in raw_date_cell.lower() or not raw_date_cell:
                continue

            # ---- DATE PARSING ----
            # Normalize ordinal dates: 19th -> 19
            raw_date = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', raw_date_cell)

            parsed = None
            for fmt in ("%d %B, %Y", "%d %b, %Y", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(raw_date, fmt)
                    break
                except ValueError:
                    pass

            if not parsed:
                logging.debug("Could not parse date: %s", raw_date)
                continue

            dt = parsed

            # Date Range Filter
            if not (week_start <= dt <= week_end):
                continue

            # ---- TITLE ----
            # Usually in the second cell (index 1)
            title = unicodedata.normalize(
                "NFKD",
                cells[1].get_text(" ", strip=True)
            )

            # ---- PDF URL EXTRACTION ----
           
            pdf_url = None

            # --------------------------------------------------
            # 1. Direct PDF link (MOST RELIABLE)
            # --------------------------------------------------
            download_a = row.select_one('a[href$=".pdf"], a[href*=".pdf"]')
            if download_a:
                href = download_a.get("href", "").strip()
                if href:
                    pdf_url = urljoin(task["url"], href)

            # --------------------------------------------------
            # 2. JavaScript onclick = newwindow1(...)
            # --------------------------------------------------
            if not pdf_url:
                download_a = row.select_one("a[onclick]")
                if download_a:
                    onclick = download_a.get("onclick", "")

                    m = re.search(
                        r"newwindow1\(['\"]([^'\"]+)['\"]\)",
                        onclick,
                        re.I,
                    )

                    if m:
                        pdf_url = urljoin(task["url"], m.group(1))

            # --------------------------------------------------
            # 3. data-href attribute
            # --------------------------------------------------
            if not pdf_url:
                a = row.select_one("[data-href]")
                if a:
                    pdf_url = urljoin(task["url"], a["data-href"])

            # --------------------------------------------------
            # 4. data-url attribute
            # --------------------------------------------------
            if not pdf_url:
                a = row.select_one("[data-url]")
                if a:
                    pdf_url = urljoin(task["url"], a["data-url"])

            # --------------------------------------------------
            # 5. Any anchor containing ".pdf"
            # --------------------------------------------------
            if not pdf_url:
                for a in row.find_all("a", href=True):
                    href = a["href"]
                    if ".pdf" in href.lower():
                        pdf_url = urljoin(task["url"], href)
                        break

            # --------------------------------------------------
            # 6. Last resort: regex anywhere in row HTML
            # --------------------------------------------------
            if not pdf_url:
                html = str(row)

                m = re.search(
                    r'https?://[^"\']+\.pdf|/[^"\']+\.pdf',
                    html,
                    re.I,
                )

                if m:
                    pdf_url = urljoin(task["url"], m.group(0))

            if not pdf_url:
                logging.warning("No PDF link found for: %s", title)
                continue


            # ---- DIRECTORY & DOWNLOAD ----
            year = str(dt.year)
            month_full = dt.strftime("%B")

            save_dir = ensure_year_month_structure(
                BASE_PATH,
                task["category"],
                task["subfolder"],
                year,
                month_full
            )

            downloaded_path = await download_pdf(
                session,
                pdf_url,
                save_dir,
                title
            )

            if downloaded_path:
                ALL_DOWNLOADED.append({
                    "Verticals": task["category"],
                    "SubCategory": task["subfolder"],
                    "Year": year,
                    "Month": month_full,
                    "IssueDate": dt.strftime("%Y-%m-%d"),
                    "Title": title,
                    "PDF_URL": pdf_url,
                    "File Name": os.path.basename(downloaded_path),
                    "Path": os.path.abspath(downloaded_path),
                })

                logging.info("IBBI Discussion Paper downloaded: %s", title)

async def scrape_ibbi_1(task, week_start, week_end):
    logging.info("IBBI CIRCULARS SCRAPER -> %s", task["url"])

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=task["url"])


    soup = BeautifulSoup(result.html, "html.parser")

    # Discussion Paper is NOT a row-based listing
    if task["subfolder"] == "Discussion Paper":
        return await scrape_ibbi_discussion_paper(task, week_start, week_end)

    # All other IBBI sections
    rows = soup.select("table tbody tr")

    if not rows:
        logging.warning("No IBBI rows found for subfolder: %s", task["subfolder"])
        return

    async with aiohttp.ClientSession() as session:

        for row in rows:
            tds = row.find_all("td")
            if len(tds) < 3:
                continue

            raw_date = tds[1].get_text(" ", strip=True)

            # normalize ordinal dates: 19th -> 19, 21st -> 21, etc
            raw_date = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', raw_date)

            parsed = None
            for fmt in ("%d %b, %Y", "%d %B, %Y"):
                try:
                    parsed = datetime.strptime(raw_date, fmt)
                    break
                except ValueError:
                    pass

            if not parsed:
                continue

            dt = parsed

            if not (week_start <= dt <= week_end):
                continue

            # ---- TITLE ----
            title_td = tds[2]
            title = unicodedata.normalize(
                "NFKD",
                title_td.get_text(" ", strip=True)
            )

            if is_ignored_ibbi_title(title):
                logging.info("Skipping IBBI (filtered title): %s", title)
                continue


            pdf_url = None

            # --------------------------------------------------
            # 1. Direct PDF link (MOST RELIABLE)
            # --------------------------------------------------
            download_a = row.select_one('a[href$=".pdf"], a[href*=".pdf"]')
            if download_a:
                href = download_a.get("href", "").strip()
                if href:
                    pdf_url = urljoin(task["url"], href)

            # --------------------------------------------------
            # 2. JavaScript onclick = newwindow1(...)
            # --------------------------------------------------
            if not pdf_url:
                download_a = row.select_one("a[onclick]")
                if download_a:
                    onclick = download_a.get("onclick", "")

                    m = re.search(
                        r"newwindow1\(['\"]([^'\"]+)['\"]\)",
                        onclick,
                        re.I,
                    )

                    if m:
                        pdf_url = urljoin(task["url"], m.group(1))

            # --------------------------------------------------
            # 3. data-href attribute
            # --------------------------------------------------
            if not pdf_url:
                a = row.select_one("[data-href]")
                if a:
                    pdf_url = urljoin(task["url"], a["data-href"])

            # --------------------------------------------------
            # 4. data-url attribute
            # --------------------------------------------------
            if not pdf_url:
                a = row.select_one("[data-url]")
                if a:
                    pdf_url = urljoin(task["url"], a["data-url"])

            # --------------------------------------------------
            # 5. Any anchor containing ".pdf"
            # --------------------------------------------------
            if not pdf_url:
                for a in row.find_all("a", href=True):
                    href = a["href"]
                    if ".pdf" in href.lower():
                        pdf_url = urljoin(task["url"], href)
                        break

            # --------------------------------------------------
            # 6. Last resort: regex anywhere in row HTML
            # --------------------------------------------------
            if not pdf_url:
                html = str(row)

                m = re.search(
                    r'https?://[^"\']+\.pdf|/[^"\']+\.pdf',
                    html,
                    re.I,
                )

                if m:
                    pdf_url = urljoin(task["url"], m.group(0))

            if not pdf_url:
                logging.warning("No PDF link found for: %s", title)
                continue


            year = str(dt.year)
            month_full = dt.strftime("%B")

            save_dir = ensure_year_month_structure(
                BASE_PATH,
                task["category"],
                task["subfolder"],
                year,
                month_full
            )

            downloaded_path = await download_pdf(
                session,
                pdf_url,
                save_dir,
                title
            )

            if downloaded_path:
                ALL_DOWNLOADED.append({
                    "Verticals": task["category"],
                    "SubCategory": task["subfolder"],
                    "Year": year,
                    "Month": month_full,
                    "IssueDate": dt.strftime("%Y-%m-%d"),
                    "Title": title,
                    "PDF_URL": pdf_url,
                    "File Name": os.path.basename(downloaded_path),
                    "Path": os.path.abspath(downloaded_path),
                })

                logging.info("IBBI downloaded: %s", title)

#-----------------------------------------------------

async def scrape_generic_link(task, week_start, week_end):
    category = task["category"]
    subfolder = task["subfolder"]
    url = task["url"]

    logging.info("Processing [%s > %s] => %s", category, subfolder, url)

    # SEBI website (current logic)

    if category == "SEBI":
        # Check if this specific link is for Informal Guidance
        if "Informal Guidance" in subfolder:
            return await scrape_sebi_informal_guidance(task, week_start, week_end)
        else:
            # Fallback to your existing SEBI scraper for other subfolders
            return await scrape_sebi(task, week_start, week_end)

    # if category == "SEBI":
    #     return await scrape_sebi(task, week_start, week_end)

    if category == "IFSCA":

        # SPECIAL CASE: Informal Guidance with LLM filtering
        if "informal guidance" in subfolder.lower():
            return await scrape_ifsca_informal_guidance(task, week_start, week_end)
        
        # SPECIAL CASE: Public Consultation
        if is_ifsca_public_consultation(task["url"]):
            return await scrape_ifsca_public_consultation(task, week_start, week_end)

        # DEFAULT: Notifications / Circulars / Others
        return await scrape_ifsca(task, week_start, week_end)


    # LISTED COMPANIES (NSE/BSE logic)
    if category == "Listed Companies":
        
        # NSE
        if "nse" in subfolder.lower():
            return await scrape_nse(task, week_start, week_end)

        # BSE
        if "bse" in subfolder.lower():
            return await scrape_bse(task, week_start, week_end)

        logging.warning("No scraper defined for subfolder: %s", subfolder)
        return

    if category == "IBBI":

        if subfolder in IBBI_1_SCRAPE:
            return await scrape_ibbi_1(task, week_start, week_end)

        logging.warning("No IBBI scraper mapped for subfolder: %s", subfolder)
        return

    if category == "RBI":
        return await scrape_rbi(task, week_start, week_end)

    if category == "ICAI":
        return await scrape_icai(task, week_start, week_end)
    
    if category == "Companies Act":
        return await scrape_mca(task, week_start, week_end)
    
    logging.warning("Unknown category: %s", category)

#---------------------------------------------------------------------

async def main():
    weeks_back = 0 # 0=this week, 1=last week, 2=two weeks back (week= this week monday to next sunday)
    week_start, week_end = get_week_range(weeks_back)

    tasks = load_link_tasks_from_excel()
    if not tasks:
        logging.info("No tasks found in Excel.")
        return

    for task in tasks:
        await scrape_generic_link(task, week_start, week_end)

    if ALL_DOWNLOADED:
        df = pd.DataFrame(ALL_DOWNLOADED)
        df.to_excel(EXCEL_OUTPUT, index=False)
        # Write week range for parsing agent
        week_info = {
            "week_start": week_start.strftime("%Y-%m-%d"),
            "week_end": week_end.strftime("%Y-%m-%d")
        }

        with open(DATA_DIR / "week_range.json", "w") as f:
            json.dump(week_info, f)

        logging.info("FINAL EXCEL GENERATED: %s", EXCEL_OUTPUT)
    else:
        logging.info("No PDFs downloaded for this week.")

#-------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception:
        logging.exception("Fatal error in searching_agent")