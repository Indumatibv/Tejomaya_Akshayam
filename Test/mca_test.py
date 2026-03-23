#!/usr/bin/env python3
"""
MCA Notifications Bulk Downloader — Selenium Download Manager approach
"""

import os
import re
import time
import shutil
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────
PAGE_URL    = "https://www.mca.gov.in/content/mca/global/en/acts-rules/ebooks/notifications.html"
BASE_URL    = "https://www.mca.gov.in"
DOWNLOAD_DIR = Path("mca_notifications").resolve()
DELAY       = 2  # seconds between downloads

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ─── Helpers ──────────────────────────────────────────────
def sanitize_filename(name: str) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    name = re.sub(r'\s+', " ", name).strip()
    return name[:200]


def wait_for_download(download_dir: Path, timeout: int = 30) -> bool:
    """Wait until no .crdownload/.part files remain."""
    end = time.time() + timeout
    while time.time() < end:
        tmp_files = list(download_dir.glob("*.crdownload")) + list(download_dir.glob("*.part"))
        if not tmp_files:
            return True
        time.sleep(0.5)
    return False


def get_downloaded_file(download_dir: Path, before_files: set) -> Path | None:
    """Return the newly downloaded file by comparing before/after."""
    after_files = set(download_dir.iterdir())
    new_files = after_files - before_files
    new_files = {f for f in new_files if not f.suffix in [".crdownload", ".part"]}
    return next(iter(new_files), None)


# ─── Build Driver ─────────────────────────────────────────
def build_driver() -> WebDriver:
    options = Options()
    # options.add_argument("--headless")  # ← comment out if issues persist
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/122.0.0.0 Safari/537.36")

    # Tell Chrome to auto-download PDFs to our folder (skip the PDF viewer)
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,   # don't open PDFs in browser
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    driver: WebDriver = webdriver.Chrome(options=options)
    return driver


# ─── Extract Links ────────────────────────────────────────
def extract_links(html: str) -> list[tuple[str, str]]:
    from urllib.parse import urljoin
    soup = BeautifulSoup(html, "html.parser")
    links = []
    seen: set[str] = set()

    for a_tag in soup.find_all("a", href=True):
        raw_href = a_tag.get("href")
        if not isinstance(raw_href, str):
            continue
        href = raw_href.strip()
        if not any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xlsx", ".zip"]):
            continue
        full_url = urljoin(BASE_URL, href)
        if full_url in seen:
            continue
        seen.add(full_url)
        text = a_tag.get_text(strip=True) or href.split("/")[-1]
        filename = sanitize_filename(text)
        ext = os.path.splitext(href.split("?")[0])[-1]
        if not filename.lower().endswith(ext.lower()):
            filename += ext
        links.append((full_url, filename))

    return links


# ─── Main ─────────────────────────────────────────────────
def main():
    print(f"📄 Launching browser: {PAGE_URL}\n")
    driver = build_driver()

    try:
        driver.get(PAGE_URL)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "a"))
        )
        time.sleep(3)

        links = extract_links(driver.page_source)
        print(f"📦 Found {len(links)} document(s). Downloading to '{DOWNLOAD_DIR}'...\n")

        success, failed = 0, 0

        for i, (file_url, desired_name) in enumerate(links, 1):
            print(f"[{i}/{len(links)}] {desired_name[:80]}...")
            before = set(DOWNLOAD_DIR.iterdir())

            # Navigate to the PDF URL — Chrome will auto-download it
            driver.get(file_url)
            time.sleep(1)  # let download start

            if wait_for_download(DOWNLOAD_DIR, timeout=30):
                new_file = get_downloaded_file(DOWNLOAD_DIR, before)
                if new_file:
                    # Rename to our desired descriptive filename
                    target = DOWNLOAD_DIR / desired_name
                    counter = 1
                    stem, suffix = os.path.splitext(target)
                    while target.exists():
                        target = Path(f"{stem}_{counter}{suffix}")
                        counter += 1
                    new_file.rename(target)
                    print(f"  ✅ Saved: {target.name}")
                    success += 1
                else:
                    print(f"  ⚠️  Download triggered but file not found")
                    failed += 1
            else:
                print(f"  ❌ Timed out waiting for download")
                failed += 1

            # Go back to the listing page to maintain session context
            driver.get(PAGE_URL)
            time.sleep(DELAY)

    finally:
        driver.quit()

    print(f"\n✅ Done! {success} downloaded, {failed} failed.")
    print(f"📁 Files saved in: {DOWNLOAD_DIR}")


if __name__ == "__main__":
    main()