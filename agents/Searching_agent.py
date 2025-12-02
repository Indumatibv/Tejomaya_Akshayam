#!/usr/bin/env python
# agents/searching_agent.py
# =========================================================================
# CRITICAL FIX FOR WINDOWS - MUST BE AT THE VERY TOP OF THE SCRIPT
# =========================================================================
import sys
import asyncio
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
from urllib.parse import urljoin, urlparse
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

# ---------------------- Logging ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# -------- CONFIG --------
# BASE_URL = "https://www.sebi.gov.in"
# LISTING_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=6&ssid=23&smid=0"
# CATEGORY = "SEBI"
# SUBFOLDER = "Press Release"

# Where PDFs should be stored (keep as-is: your Downloads path)
if platform.system() == "Windows":
    BASE_PATH = r"C:\Users\Admin\Downloads\Tejomaya_pdfs\Akshayam Data"
else:
    BASE_PATH = "/Users/admin/Downloads/Tejomaya_pdfs/Akshayam Data"

# Ensure base download folder exists
os.makedirs(BASE_PATH, exist_ok=True)

# Excel output goes into the repo data folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_OUTPUT = DATA_DIR / "weekly_sebi_downloads.xlsx"

# GLOBAL LIST FOR FINAL EXCEL
ALL_DOWNLOADED = []

#--------------------------------------------------scaling the downloads for testing purpose
# Excel file containing links
LINKS_EXCEL = DATA_DIR / "Links.xlsx"

# Only process these sheet names (categories)
PROCESS_SHEETS = ["SEBI", "RBI"]  # <-- modify based on your file

#---------------------------------------------------

#-------------------------------------------------------scaling the downloads for testing purpose
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
    logging.info("Target range (%d week(s) back): %s → %s", weeks_back, target_monday.date(), target_sunday.date())
    return target_monday, target_sunday


# -------- HELPERS --------
def sanitize_filename(title: str, max_length: int = 120) -> str:
    filename = re.sub(r'[\/\\\:\*\?"<>\|]', '_', title).strip()
    filename = filename.replace(" ", "_")

    # Trim filename to safe length
    if len(filename) > max_length:
        filename = filename[:max_length]

    return filename + ".pdf"
#-----------------------------------------------------

def ensure_year_month_structure(base_folder: str, category: str, subfolder: str, year: str, month_full: str) -> str:
    subfolder_path = os.path.join(base_folder, category, subfolder)
    year_path = os.path.join(subfolder_path, year)
    os.makedirs(year_path, exist_ok=True)
    month_path = os.path.join(year_path, month_full)
    os.makedirs(month_path, exist_ok=True)
    return month_path

async def download_pdf(session: aiohttp.ClientSession, pdf_url: str, save_path: str) -> str | None:
    try:
        filename = os.path.basename(urlparse(pdf_url).path) or sanitize_filename("downloaded.pdf")
        file_path = os.path.join(save_path, filename)
        if os.path.exists(file_path):
            logging.info("Skipping download (exists): %s", file_path)
            return file_path

        async with session.get(pdf_url, timeout=30) as resp:
            if resp.status == 200:
                content = await resp.read()
                with open(file_path, "wb") as f:
                    f.write(content)
                logging.info("Downloaded PDF: %s", file_path)
                return file_path
            else:
                logging.warning("Failed PDF download (%s) for %s", resp.status, pdf_url)
    except Exception as e:
        logging.exception("Error downloading PDF %s : %s", pdf_url, e)
    return None

async def scrape_generic_link(task, week_start, week_end):
    category = task["category"]
    subfolder = task["subfolder"]
    detail_url = task["url"]

    logging.info("Processing [%s > %s]: %s", category, subfolder, detail_url)

    async with AsyncWebCrawler() as crawler:
        try:
            detail_result = await crawler.arun(url=detail_url)
        except Exception as e:
            logging.exception("Crawler failed for %s : %s", detail_url, e)
            return

    soup_detail = BeautifulSoup(detail_result.html, "html.parser")
    # ----- Ensure title exists -----
    if "title" not in task:
        # Try extracting title from detail page (h1/h2/h3)
        title_elem = soup_detail.select_one("h1, h2, h3")
        if title_elem:
            task["title"] = title_elem.get_text(strip=True)
        else:
            logging.warning("No title found at %s", detail_url)
            task["title"] = "Untitled"

        # ---------- Detect listing page and extract inner detail links ----------
    if "doListing=yes" in detail_url:
        detail_links = extract_detail_links_from_listing(detail_result.html, detail_url)

        if not detail_links:
            logging.warning("No detail links found in listing page: %s", detail_url)
            return

        logging.info("Found %d detail links inside listing: %s", len(detail_links), detail_url)

        # Recursively process each detail link

        for item in detail_links:
            await scrape_generic_link(
                {
                    "category": category,
                    "subfolder": subfolder,
                    "url": item["url"],
                    "title": item["title"]
                },
                week_start,
                week_end
            )

        return  # IMPORTANT: Do NOT continue processing listing page itself

    # ---------- Extract date ----------
    date_elem = soup_detail.select_one("h5")
    if not date_elem:
        logging.warning("No date found at %s", detail_url)
        return

    try:
        dt = datetime.strptime(date_elem.get_text(strip=True), "%b %d, %Y")
    except:
        logging.warning("Invalid date format for %s", detail_url)
        return

    # ---------- Week filter ----------
    if not (week_start <= dt <= week_end):
        logging.info("Skipping (out of week): %s", dt.date())
        return

    year = str(dt.year)
    month_full = dt.strftime("%B")

    # ---------- AIF Logic ----------
    original_category = category  # preserve original

    if original_category == "SEBI":
        if detect_aif_category(task["title"]):
            logging.info("AIF match detected → Storing under AIF")
            category = "AIF"
        else:
            category = "SEBI"

    save_path = ensure_year_month_structure(BASE_PATH, category, subfolder, year, month_full)

    # ---------- Detect PDF links ----------
    pdf_url = None
    iframe = soup_detail.select_one("iframe")
    pdf_btn = soup_detail.select_one("button#download")

    if iframe and "file=" in iframe.get("src", ""):
        pdf_url = iframe["src"].split("file=")[-1]
        if not pdf_url.startswith("http"):
            pdf_url = urljoin(detail_url, pdf_url)
    elif pdf_btn:
        pdf_url = detail_url.replace(".html", ".pdf")

    file_path = None

    if pdf_url:
        async with aiohttp.ClientSession() as session:
            file_path = await download_pdf(session, pdf_url, save_path)

    # ---------- If no PDF → printToPDF fallback ----------
    if not file_path:
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless=new")
            driver = webdriver.Chrome(options=options)
            driver.get(detail_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.TAG_NAME, "body"))
            )
            pdf = driver.execute_cdp_cmd("Page.printToPDF", {"printBackground": True})
            pdf_data = base64.b64decode(pdf["data"])
            #filename = sanitize_filename(Path(detail_url).stem)
            filename = sanitize_filename(task["title"])
            file_path = os.path.join(save_path, filename)
            with open(file_path, "wb") as f:
                f.write(pdf_data)
        except:
            logging.exception("Fallback printToPDF failed: %s", detail_url)
        finally:
            try:
                driver.quit()
            except:
                pass

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

#---------------------------------------------------------------------

async def main():
    weeks_back = 6   # 0=this week, 1=last week, 2=two weeks back (week= this week monday to next sunday)
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
