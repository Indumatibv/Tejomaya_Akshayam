#!/usr/bin/env python
# =========================================================================
# CRITICAL FIX FOR WINDOWS
# =========================================================================
import sys
import asyncio
import nest_asyncio
import platform
import os

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    nest_asyncio.apply()
# =========================================================================

import aiohttp
import pandas as pd
from datetime import datetime
import re
from urllib.parse import urljoin

# ---------------- CONFIG -----------------
HOME_URL = "https://www.nseindia.com"
API_URL = "https://www.nseindia.com/api/circulars?types=Equity"

CATEGORY = "Listed Companies"
SUBFOLDER = "Circular-NSE"

if platform.system() == "Windows":
    BASE_PATH = r"C:\Users\Admin\Downloads\Tejomaya_pdfs\Akshayam Data"
else:
    BASE_PATH = "/Users/admin/Downloads/Tejomaya_pdfs/Akshayam Data"


# ---------------- UTIL -----------------

def ensure_year_month_structure(base, cat, sub, year, month):
    path = os.path.join(base, cat, sub, year, month)
    os.makedirs(path, exist_ok=True)
    return path

def sanitize_filename(title):
    title = re.sub(r'[\\/*?:"<>|]', "_", title)
    return title.replace(" ", "_") + ".pdf"


async def download_pdf(session, pdf_url, save_dir, filename):
    filepath = os.path.join(save_dir, filename)

    if os.path.exists(filepath):
        print(f"⏭️ Already exists: {filepath}")
        return filepath

    try:
        async with session.get(pdf_url) as resp:
            if resp.status == 200:
                with open(filepath, "wb") as f:
                    f.write(await resp.read())
                print(f"📥 Downloaded: {filepath}")
                return filepath
            else:
                print(f"❌ Failed {pdf_url}, status={resp.status}")
    except Exception as e:
        print(f"⚠️ Error: {e}")

    return None


# ---------------- SCRAPER -----------------

async def scrape_nse():
    print("\n🚀 Starting NSE Circular Scraper using API ...")

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/companies-listing/circular-for-listed-companies-equity-market"
    }

    async with aiohttp.ClientSession(headers=headers) as session:

        # 1️⃣ MUST Load homepage to get cookies
        await session.get(HOME_URL)

        # 2️⃣ Now call circulars API
        resp = await session.get(API_URL)

        if resp.status != 200:
            print(f"❌ API failed: {resp.status}")
            return

        data = await resp.json()

    circulars = data.get("data", [])
    print(f"✔ Circulars fetched: {len(circulars)}")

    if not circulars:
        print("❌ API returned empty data")
        return

    top_10 = circulars[:10]
    extracted = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for item in top_10:
            title = item.get("subject", "").strip()
            date_str = item.get("date", "").strip()
            pdf_url = item.get("pdfUrl", "")

            if not pdf_url:
                continue

            # Parse date
            try:
                dt = datetime.strptime(date_str, "%d-%b-%Y")
                year = str(dt.year)
                month = dt.strftime("%B")
            except:
                year, month = "UnknownYear", "UnknownMonth"

            save_dir = ensure_year_month_structure(BASE_PATH, CATEGORY, SUBFOLDER, year, month)
            filename = sanitize_filename(title)

            pdf_path = await download_pdf(session, pdf_url, save_dir, filename)

            extracted.append({
                "Title": title,
                "Date": date_str,
                "PDF_URL": pdf_url,
                "PDF_Path": pdf_path
            })

    # Save Excel
    df = pd.DataFrame(extracted)
    excel_path = os.path.join(BASE_PATH, "NSE_Listed_Companies_Top10.xlsx")
    df.to_excel(excel_path, index=False)

    print(f"\n✅ Excel saved: {excel_path}")
    print("🎉 DONE!\n")


if __name__ == "__main__":
    asyncio.run(scrape_nse())
