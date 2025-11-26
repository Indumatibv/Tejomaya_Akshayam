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
BASE_URL = "https://www.sebi.gov.in"
LISTING_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=6&ssid=23&smid=0"
CATEGORY = "SEBI"
SUBFOLDER = "Press Release"

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
def sanitize_filename(title: str) -> str:
    filename = re.sub(r'[\/\\\:\*\?"<>\|]', '_', title)
    filename = filename.replace(" ", "_")
    return filename + ".pdf"

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

def sync_fetch_links(page_number: int) -> list[dict]:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new" if sys.platform != "win32" else "--headless")
    # improve reliability on some systems
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)

    titles_and_urls = []
    try:
        driver.get(LISTING_URL)
        WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.points")))
        if page_number > 1:
            page_index = page_number - 1
            driver.execute_script(f"searchFormNewsList('n', '{page_index}');")
            WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.points")))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for link in soup.select("a.points[href]"):
            detail_url = link.get("href")
            title = link.get_text(strip=True)
            if detail_url:
                titles_and_urls.append({"title": title, "detail_url": urljoin(BASE_URL, detail_url)})
    except Exception as e:
        logging.exception("Error fetching listing page links: %s", e)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return titles_and_urls


# -------- MAIN SCRAPER --------
async def scrape_sebi_page(page_number: int, week_start: datetime, week_end: datetime):
    logging.info("Processing SEBI Page %d", page_number)
    links_data = await asyncio.to_thread(sync_fetch_links, page_number)
    if not links_data:
        logging.warning("No titles found on page %d", page_number)
        return

    async with AsyncWebCrawler() as crawler:
        # iterate synchronously through found links; each uses crawler/arun for the detail page
        for link_data in links_data:
            title = link_data.get("title")
            detail_url = link_data.get("detail_url")
            logging.info("Opening: %s", detail_url)

            try:
                detail_result = await crawler.arun(url=detail_url)
            except Exception as e:
                logging.exception("Crawler failed for %s : %s", detail_url, e)
                continue

            soup_detail = BeautifulSoup(detail_result.html, "html.parser")
            date_elem = soup_detail.select_one("h5")
            if not date_elem:
                logging.debug("No date element found for %s; skipping", detail_url)
                continue

            date_str = date_elem.get_text(strip=True)
            try:
                dt = datetime.strptime(date_str, "%b %d, %Y")
            except Exception:
                logging.warning("Invalid date string '%s' at %s; skipping", date_str, detail_url)
                continue

            # WEEK FILTER
            if not (week_start <= dt <= week_end):
                logging.debug("Skipping (not in week): %s", dt.date())
                continue

            logging.info("MATCH (in week): %s", dt.date())

            year = str(dt.year)
            month_full = dt.strftime("%B")
            save_path = ensure_year_month_structure(BASE_PATH, CATEGORY, SUBFOLDER, year, month_full)

            # PDF extraction
            pdf_url = None
            iframe = soup_detail.select_one("iframe")
            pdf_button = soup_detail.select_one("button#download")

            if iframe and "file=" in iframe.get("src", ""):
                pdf_url = iframe["src"].split("file=")[-1]
                if not pdf_url.startswith("http"):
                    pdf_url = urljoin(BASE_URL, pdf_url)
            elif pdf_button:
                pdf_url = detail_url.replace(".html", ".pdf")

            file_path = None

            if pdf_url:
                try:
                    async with aiohttp.ClientSession() as session:
                        file_path = await download_pdf(session, pdf_url, save_path)
                except Exception:
                    logging.exception("aiohttp session failed for %s", pdf_url)

            if not file_path:
                # Print-to-PDF fallback using chrome headless (CDP)
                options = webdriver.ChromeOptions()
                options.add_argument("--headless=new" if sys.platform != "win32" else "--headless")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                driver = webdriver.Chrome(options=options)
                try:
                    driver.get(detail_url)
                    # allow page to settle
                    WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "body")))
                    pdf = driver.execute_cdp_cmd("Page.printToPDF", {"printBackground": True})
                    pdf_data = base64.b64decode(pdf["data"])
                    file_path = os.path.join(save_path, sanitize_filename(title or "printed"))
                    with open(file_path, "wb") as f:
                        f.write(pdf_data)
                    logging.info("Printed to PDF: %s", file_path)
                except Exception as e:
                    logging.exception("Print-to-PDF failed for %s : %s", detail_url, e)
                    file_path = None
                finally:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            if file_path:
                # ALL_DOWNLOADED.append({
                #     "Verticals": CATEGORY,
                #     "SubCategory": SUBFOLDER,
                #     "Year": year,
                #     "Month": month_full,
                #     "File Name": os.path.basename(file_path),
                #     "Path": os.path.abspath(file_path)
                # })
                ALL_DOWNLOADED.append({
                    "Verticals": CATEGORY,
                    "SubCategory": SUBFOLDER,
                    "Year": year,
                    "Month": month_full,
                    "IssueDate": dt.strftime("%Y-%m-%d"),
                    "Title": title,
                    "PDF_URL": pdf_url if pdf_url else "Generated via print",
                    "File Name": os.path.basename(file_path),
                    "Path": os.path.abspath(file_path)
                })

            else:
                logging.warning("No PDF produced for %s", detail_url)


# -------- MAIN PROGRAM --------
async def main():
    weeks_back = 1   # 0=this week, 1=last week (change as needed)
    week_start, week_end = get_week_range(weeks_back)

    # scrape first 2 pages (configurable)
    for page in range(1, 3):
        await scrape_sebi_page(page, week_start, week_end)

    # FINAL EXCEL
    if ALL_DOWNLOADED:
        try:
            df = pd.DataFrame(ALL_DOWNLOADED)
            df.to_excel(EXCEL_OUTPUT, index=False)
            logging.info("FINAL EXCEL GENERATED: %s", EXCEL_OUTPUT)
        except Exception as e:
            logging.exception("Failed to write excel %s : %s", EXCEL_OUTPUT, e)
    else:
        logging.info("No PDFs found for this week.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception:
        logging.exception("Fatal error in searching_agent")
