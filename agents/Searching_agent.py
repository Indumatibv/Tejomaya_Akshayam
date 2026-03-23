#!/usr/bin/env python
# agents/searching_agent.py
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

import hashlib


# ---------------------- Logging ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ---- GLOBAL TITLE TRACKING FOR BSE vs NSE DEDUP ----
BSE_TITLES_NORMALIZED = set()

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
    BASE_PATH = r"C:\Users\Admin\Desktop\Indu\Tejomaya\Tejomaya_pdfs\Akshayam Data"
else:
    BASE_PATH = "/Users/admin/Downloads/Tejomaya_pdfs/Akshayam Data"

# Ensure base download folder exists
os.makedirs(BASE_PATH, exist_ok=True)

# Excel output goes into the repo data folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_OUTPUT = DATA_DIR / "Searching_agent_output.xlsx"

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

#----mca-----
 
import base64
import os
import re
import time
import requests
import unicodedata
from datetime import datetime
 
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchWindowException, WebDriverException
 
try:
    import undetected_chromedriver as uc
    _UC_AVAILABLE = True
except ImportError:
    _UC_AVAILABLE = False
 
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
        return uc.Chrome(options=opts)
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
 
 
# ── Snapshot rows → plain dicts (no live elements) ────────
 
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

        # try:
        #     dt = datetime.strptime(date_str, "%d/%m/%Y")
        # except Exception:
        #     logging.warning("MCA PR: bad date %r for: %s", date_str, title)
        #     continue
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

# async def download_pdf(session: aiohttp.ClientSession, pdf_url: str, save_path: str) -> str | None:
#     try:
#         filename = os.path.basename(urlparse(pdf_url).path) or sanitize_filename("downloaded.pdf")
#         file_path = os.path.join(save_path, filename)
#         if os.path.exists(file_path):
#             logging.info("Skipping download (exists): %s", file_path)
#             return file_path

#         async with session.get(pdf_url, timeout=30) as resp:
#             if resp.status == 200:
#                 content = await resp.read()
#                 with open(file_path, "wb") as f:
#                     f.write(content)
#                 logging.info("Downloaded PDF: %s", file_path)
#                 return file_path
#             else:
#                 logging.warning("Failed PDF download (%s) for %s", resp.status, pdf_url)
#     except Exception as e:
#         logging.exception("Error downloading PDF %s : %s", pdf_url, e)
#     return None

async def download_pdf(session: aiohttp.ClientSession, pdf_url: str, save_dir: str, title: str | None = None) -> str | None:
    try:
        parsed = urlparse(pdf_url)
        qs = parse_qs(parsed.query)

        # filename = qs.get("fileName", [None])[0]
        # if not filename:
        #     filename = sanitize_filename(title or "document")

        # filename = qs.get("fileName", [None])[0]

        # if not filename:
        #     if title:
        #         filename = sanitize_filename(title)
        #     else:
        #         filename = os.path.basename(urlparse(pdf_url).path)

        # if not filename.lower().endswith(".pdf"):
        #     filename += ".pdf"

        # if not filename.lower().endswith(".pdf"):
        #     filename += ".pdf"


        filename = qs.get("fileName", [None])[0]

        if not filename:
            filename = safe_pdf_filename(title, pdf_url)

        file_path = os.path.join(save_dir, filename)

        if os.path.exists(file_path):
            logging.warning("Overwriting existing file: %s", file_path)

        # headers = {
        #     "User-Agent": "Mozilla/5.0",
        #     "Accept": "application/pdf",
        #     "Referer": "https://ifsca.gov.in/",
        # }
        # headers = {
        #     "User-Agent": "Mozilla/5.0",
        #     "Accept": "application/pdf",
        #     "Referer": urlparse(pdf_url).scheme + "://" + urlparse(pdf_url).netloc,
        # }

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
            headers["Referer"] = "https://www.rbi.org.in/"
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

            with open(file_path, "wb") as f:
                f.write(data)

            logging.info("Valid PDF saved -> %s", file_path)
            return file_path

    except Exception as e:
        logging.exception("Error downloading PDF %s : %s", pdf_url, e)
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


async def scrape_bse(task, week_start, week_end):
    logging.info("BSE LISTED COMPANIES SCRAPER -> %s", task["url"])

    # Configure Chrome with custom download folder
    chrome_opts = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": BASE_PATH,     # PDF auto saved here
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True
    }
    chrome_opts.add_experimental_option("prefs", prefs)
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--window-size=1920,1080")

    chrome_opts.add_argument(
    "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=chrome_opts)

    # Load listing page
    # driver.get(task["url"])
    # time.sleep(3)

    # soup = BeautifulSoup(driver.page_source, "html.parser")
    # # rows = soup.select("tr.ng-scope")
    # rows = soup.select("tr.ng-scope, tr[ng-repeat]")

    driver.get(task["url"])

    # WebDriverWait(driver, 20).until(
    #     # EC.presence_of_element_located((By.CSS_SELECTOR, "tr[ng-repeat]"))
    #     EC.presence_of_element_located((By.CSS_SELECTOR, "tr[ng-repeat], tr.ng-scope"))
    # )

    # time.sleep(1)

    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "tr[ng-repeat]"))
    )
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.select("tr[ng-repeat]")

    if not rows:
        logging.error("No BSE rows found after JS load")
        driver.quit()
        return

    logging.info("Processing TOP 10 BSE Circulars")
    top_10 = rows[:10]

    for row in top_10:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        title_elem = cols[0].find("a", href=True)
        if not title_elem:
            continue

        title = title_elem.get_text(strip=True)
        detail_link = urljoin("https://www.bseindia.com", title_elem["href"])

        # Parse Issue Date
        date_text = cols[1].get_text(strip=True)
        try:
            dt = datetime.strptime(date_text, "%B %d, %Y")
        except:
            logging.warning("Cannot parse date: %s", date_text)
            continue

        if not (week_start <= dt <= week_end):
            logging.info("Skipping (outside week): %s", dt.date())
            continue

        logging.info("Opening detail page: %s", detail_link)
        driver.get(detail_link)
        # time.sleep(2)
        # WebDriverWait(driver, 10).until(
        #     EC.presence_of_element_located((By.CSS_SELECTOR, "td#tc52 a"))
        # )

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        time.sleep(2)

        detail_soup = BeautifulSoup(driver.page_source, "html.parser")

        year = str(dt.year)
        month_full = dt.strftime("%B")

        save_dir = ensure_year_month_structure(
            BASE_PATH, task["category"], task["subfolder"], year, month_full
        )
        # filename = sanitize_filename(title)
        filename = safe_pdf_filename(title, detail_link)

        final_path = os.path.join(save_dir, filename)

        # ---- CHECK FOR ATTACHMENT ----
        attach = detail_soup.select_one("td#tc52 a[href]")
        if attach:
            pdf_url = urljoin("https://www.bseindia.com", attach["href"])
            logging.info("Attachment -> %s", pdf_url)

            # CLICK USING SELENIUM (IMPORTANT)
            try:
                link = driver.find_element(By.CSS_SELECTOR, "td#tc52 a")
                link.click()
                time.sleep(3)   # allow browser to download

                # Now move the latest downloaded file into final_path
                dl_folder = BASE_PATH
                downloaded_file = sorted(
                    [os.path.join(dl_folder, f) for f in os.listdir(dl_folder)],
                    key=os.path.getmtime
                )[-1]

                os.rename(downloaded_file, final_path)
                logging.info("Downloaded via click -> %s", final_path)

                ALL_DOWNLOADED.append({
                    "Verticals": task["category"],
                    "SubCategory": task["subfolder"],
                    "Year": year,
                    "Month": month_full,
                    "IssueDate": dt.strftime("%Y-%m-%d"),
                    "Title": title,
                    "PDF_URL": pdf_url,
                    "File Name": filename,
                    "Path": final_path
                })
                BSE_TITLES_NORMALIZED.add(normalize_title_for_compare(title))

                driver.get(task["url"])
                time.sleep(1)
                continue

            except Exception as e:
                logging.error("Selenium click download failed: %s", e)

        # ---- NO ATTACHMENTS -> printToPDF fallback ----
        logging.info("Using printToPDF fallback")

        try:
            pdf_data = driver.execute_cdp_cmd("Page.printToPDF", {"printBackground": True})
            with open(final_path, "wb") as f:
                f.write(base64.b64decode(pdf_data["data"]))

            logging.info("Saved printToPDF -> %s", final_path)

            ALL_DOWNLOADED.append({
                "Verticals": task["category"],
                "SubCategory": task["subfolder"],
                "Year": year,
                "Month": month_full,
                "IssueDate": dt.strftime("%Y-%m-%d"),
                "Title": title,
                # "PDF_URL": "PrintToPDF",
                "PDF_URL": detail_link,   # use detail page when no direct attachment
                "File Name": filename,
                "Path": final_path
            })
            BSE_TITLES_NORMALIZED.add(normalize_title_for_compare(title))

        except Exception as e:
            logging.error("printToPDF failed: %s", e)

        driver.get(task["url"])
        time.sleep(1)

    driver.quit()
    logging.info("BSE LISTED COMPANIES -> DONE")

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
        "auction",
        "auction results",
        "money market operations conversion",
        "money market",
        "redemption",
        "state government securities",
        "monetary penalty turnover data",
        "monetary penalty"
    ]

    return any(kw in t for kw in ignore_keywords)
async def scrape_rbi(task, week_start, week_end):
    logging.info("RBI SCRAPER -> %s", task["url"])

    # 1. Use Crawl4AI to get the HTML (Keep this as is)
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=task["url"])

    soup = BeautifulSoup(result.html, "html.parser")
    rows = soup.select("table tr")

    if not rows:
        logging.warning("No RBI rows found")
        return

    current_dt = None
    
    # Use a persistent session for the whole RBI task
    connector = aiohttp.TCPConnector(ssl=False) # Helps with some Mac SSL handshake issues
    async with aiohttp.ClientSession(connector=connector) as session:
        # First, hit the main page to establish a session/cookie
        # await session.get("https://www.rbi.org.in/Scripts/NotificationUser.aspx")

        async with session.get("https://www.rbi.org.in/Scripts/NotificationUser.aspx"):
            pass

        for row in rows:
            # -------- DATE HEADER --------
            # date_h2 = row.select_one("h2.dop_header")
            # if date_h2:
            #     try:
            #         # RBI Date format: "Feb 03, 2026"
            #         current_dt = datetime.strptime(date_h2.get_text(strip=True), "%b %d, %Y")
            #     except Exception:
            #         current_dt = None
            #     continue

            date_h2 = row.select_one("h2.dop_header")
            if date_h2:
                text = date_h2.get_text(strip=True)

                # Try parsing as RBI date
                try:
                    parsed_dt = datetime.strptime(text, "%b %d, %Y")
                    current_dt = parsed_dt
                except ValueError:
                    # This is a section heading like:
                    # "Banker and Debt Manager to Government"
                    # Do NOT reset current_dt
                    pass

                continue

            if not current_dt:
                continue

            # -------- WEEK FILTER --------
            if current_dt < week_start:
                continue
            if current_dt > week_end:
                continue

            # -------- TITLE & PDF LINK --------
            title_a = row.select_one("a.link2")
            pdf_a = row.select_one("a[href*='rbidocs.rbi.org.in']")
            
            if not title_a or not pdf_a:
                continue

            title = unicodedata.normalize("NFKD", title_a.get_text(strip=True))
            
            if is_ignored_rbi_title(title):
                logging.info("Skipping RBI (filtered title): %s", title)
                continue

            pdf_url = pdf_a["href"]
            year = str(current_dt.year)
            month_full = current_dt.strftime("%B")

            save_dir = ensure_year_month_structure(
                BASE_PATH, task["category"], task["subfolder"], year, month_full
            )

            # Use the specialized download logic
            downloaded_path = await download_pdf(session, pdf_url, save_dir, title)

            if downloaded_path:
                ALL_DOWNLOADED.append({
                    "Verticals": task["category"],
                    "SubCategory": task["subfolder"],
                    "Year": year,
                    "Month": month_full,
                    "IssueDate": current_dt.strftime("%Y-%m-%d"),
                    "Title": title,
                    "PDF_URL": pdf_url,
                    "File Name": os.path.basename(downloaded_path),
                    "Path": os.path.abspath(downloaded_path)
                })
                logging.info("RBI Successfully downloaded: %s", title)

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

# async def scrape_ibbi_discussion_paper(task, week_start, week_end):
#     logging.info("IBBI DISCUSSION PAPER SCRAPER -> %s", task["url"])

#     async with AsyncWebCrawler() as crawler:
#         result = await crawler.arun(url=task["url"])

#     soup = BeautifulSoup(result.html, "html.parser")

#     # IBBI Discussion Papers are DIRECTLY in the table (no detail pages)
#     rows = soup.select("table tbody tr")

#     if not rows:
#         logging.warning("No IBBI Discussion Paper rows found")
#         return

#     async with aiohttp.ClientSession() as session:
#         for row in rows:
#             tds = row.find_all("td")
#             if len(tds) < 3:
#                 continue

#             # ---- DATE ----
#             raw_date = tds[1].get_text(" ", strip=True)
#             raw_date = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', raw_date)

#             parsed = None
#             for fmt in ("%d %B, %Y", "%d %b, %Y"):
#                 try:
#                     parsed = datetime.strptime(raw_date, fmt)
#                     break
#                 except ValueError:
#                     pass

#             if not parsed:
#                 continue

#             dt = parsed

#             if not (week_start <= dt <= week_end):
#                 continue

#             # ---- TITLE ----
#             title = unicodedata.normalize(
#                 "NFKD",
#                 tds[2].get_text(" ", strip=True)
#             )

#             # ---- PDF URL (onclick=newwindow1) ----
#             download_a = row.select_one("a[onclick*='newwindow1']")
#             if not download_a:
#                 continue

#             onclick = download_a.get("onclick", "")
#             m = re.search(r"newwindow1\(['\"]([^'\"]+\.pdf)['\"]\)", onclick)
#             if not m:
#                 continue

#             pdf_url = m.group(1)
#             if pdf_url.startswith("/"):
#                 pdf_url = urljoin(task["url"], pdf_url)

#             year = str(dt.year)
#             month_full = dt.strftime("%B")

#             save_dir = ensure_year_month_structure(
#                 BASE_PATH,
#                 task["category"],
#                 task["subfolder"],
#                 year,
#                 month_full
#             )

#             downloaded_path = await download_pdf(
#                 session,
#                 pdf_url,
#                 save_dir,
#                 title
#             )

#             if downloaded_path:
#                 ALL_DOWNLOADED.append({
#                     "Verticals": task["category"],
#                     "SubCategory": task["subfolder"],
#                     "Year": year,
#                     "Month": month_full,
#                     "IssueDate": dt.strftime("%Y-%m-%d"),
#                     "Title": title,
#                     "PDF_URL": pdf_url,
#                     "File Name": os.path.basename(downloaded_path),
#                     "Path": os.path.abspath(downloaded_path),
#                 })

#                 logging.info("IBBI Discussion Paper downloaded: %s", title)

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
            # Discussion papers often have the link in the 3rd or 4th cell
            download_a = row.select_one("a[onclick*='newwindow1']")
            if not download_a:
                continue

            onclick = download_a.get("onclick", "")
            # Regex handles both single quotes and double quotes in the JS function
            m = re.search(r"newwindow1\(['\"]([^'\"]+\.pdf)['\"]\)", onclick)
            if not m:
                continue

            pdf_url = m.group(1)
            if pdf_url.startswith("/"):
                pdf_url = urljoin(task["url"], pdf_url)

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

    # soup = BeautifulSoup(result.html, "html.parser")
    # rows = soup.select("table tbody tr")

    # if not rows:
    #     logging.warning("No IBBI rows found")
    #     return

    soup = BeautifulSoup(result.html, "html.parser")

    # if task["subfolder"] == "Discussion Paper":
    #     # Discussion Paper pages do NOT use table/tbody consistently
    #     rows = soup.select("tr")
    # else:
    #     rows = soup.select("table tbody tr")

    # if not rows:
    #     logging.warning("No IBBI rows found for subfolder: %s", task["subfolder"])
    #     return

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

            # ---- DATE ----
            # try:
            #     dt = datetime.strptime(
            #         tds[1].get_text(strip=True),
            #         "%d %b, %Y"
            #     )
            # except Exception:
            #     continue

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

            # ---- PDF URL (onclick anywhere in row) ----
            download_a = row.select_one("a[onclick]")
            if not download_a:
                continue

            onclick = download_a.get("onclick", "")
            m = re.search(r"newwindow1\('([^']+\.pdf)'\)", onclick)
            if not m:
                continue

            pdf_url = m.group(1)
            if pdf_url.startswith("/"):
                pdf_url = urljoin(task["url"], pdf_url)

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