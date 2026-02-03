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
PROCESS_SHEETS = ["SEBI", "Listed Companies", "IFSCA", "IBBI"]

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
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/pdf",
            "Referer": urlparse(pdf_url).scheme + "://" + urlparse(pdf_url).netloc,
        }

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
                "⏭ Skipping NSE circular (already downloaded from BSE): %s",
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

    driver = webdriver.Chrome(options=chrome_opts)

    # Load listing page
    driver.get(task["url"])
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.select("tr.ng-scope")

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
                link = driver.find_element("css selector", "td#tc52 a")
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
            "⏭ Skipping regulation (Last amended on): %s",
            task["title"]
        )
        return

    # ---- SKIP non-relevant SEBI PDFs based on title ----
    if category == "SEBI" and is_ignored_sebi_title(task["title"]):
        logging.info(
            "⏭ Skipping SEBI document based on ignore list: %s",
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
    # pdf_url = None
    # iframe = soup_detail.select_one("iframe")
    # pdf_btn = soup_detail.select_one("button#download")

    # if iframe and "file=" in iframe.get("src", ""):
    #     pdf_url = iframe["src"].split("file=")[-1]
    #     if not pdf_url.startswith("http"):
    #         pdf_url = urljoin(detail_url, pdf_url)

    # elif pdf_btn:
    #     pdf_url = detail_url.replace(".html", ".pdf")

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
                            logging.info("⏭ Skipping IFSCA PC (filtered title): %s", title)
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
        return await scrape_sebi(task, week_start, week_end)

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

    logging.warning("Unknown category: %s", category)

#---------------------------------------------------------------------

async def main():
    weeks_back = 25 # 0=this week, 1=last week, 2=two weeks back (week= this week monday to next sunday)
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