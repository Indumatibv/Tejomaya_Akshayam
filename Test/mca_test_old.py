#!/usr/bin/env python3
"""
MCA Notifications Downloader — Navigation + Click-based (FINAL FIX)
"""

import os
import re
import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# ─── Config ───────────────────────────────────────────────
HOME_URL = "https://www.mca.gov.in/"
DOWNLOAD_DIR = Path("mca_notifications").resolve()
DELAY = 2

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ─── Helpers ──────────────────────────────────────────────
def sanitize_filename(name: str) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:200]


def wait_for_download(download_dir: Path, timeout: int = 40) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        tmp = list(download_dir.glob("*.crdownload")) + list(download_dir.glob("*.part"))
        if not tmp:
            return True
        time.sleep(0.5)
    return False


def get_downloaded_file(download_dir: Path, before_files: set):
    after = set(download_dir.iterdir())
    new_files = after - before_files
    new_files = [f for f in new_files if f.suffix not in [".crdownload", ".part"]]
    return new_files[0] if new_files else None


# ─── Driver ───────────────────────────────────────────────
def build_driver() -> WebDriver:
    options = Options()

    # Keep non-headless
    # options.add_argument("--headless")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-popup-blocking")

    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True,
    }

    options.add_experimental_option("prefs", prefs)

    return webdriver.Chrome(options=options)


# ─── Navigation (KEY FIX) ─────────────────────────────────
def navigate_to_notifications(driver):
    wait = WebDriverWait(driver, 20)

    driver.get(HOME_URL)

    # Click "Acts & Rules"
    acts_rules = wait.until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Acts & Rules"))
    )
    acts_rules.click()

    time.sleep(2)

    # Click "Notifications"
    notifications = wait.until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Notifications"))
    )
    notifications.click()

    time.sleep(3)


# ─── Extract Elements ─────────────────────────────────────
def extract_elements(driver):
    elements = driver.find_elements(By.TAG_NAME, "a")
    valid = []
    seen = set()

    for el in elements:
        href = el.get_attribute("href")
        text = el.text.strip()

        if not href:
            continue

        if not any(ext in href.lower() for ext in [".pdf", ".doc", ".docx", ".xlsx", ".zip"]):
            continue

        if href in seen:
            continue
        seen.add(href)

        filename = sanitize_filename(text or href.split("/")[-1])

        ext = os.path.splitext(href.split("?")[0])[-1]
        if ext and not filename.lower().endswith(ext.lower()):
            filename += ext

        valid.append((el, filename))

    return valid


# ─── Click Download ───────────────────────────────────────
def click_and_download(driver, element, desired_name):
    before = set(DOWNLOAD_DIR.iterdir())

    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    time.sleep(0.5)

    original_tabs = driver.window_handles

    try:
        element.click()
    except:
        driver.execute_script("arguments[0].click();", element)

    time.sleep(1)

    if len(driver.window_handles) > len(original_tabs):
        driver.switch_to.window(driver.window_handles[-1])

    if wait_for_download(DOWNLOAD_DIR):
        new_file = get_downloaded_file(DOWNLOAD_DIR, before)
        if new_file:
            target = DOWNLOAD_DIR / desired_name

            counter = 1
            stem, suffix = os.path.splitext(target)
            while target.exists():
                target = Path(f"{stem}_{counter}{suffix}")
                counter += 1

            new_file.rename(target)
            print(f"  Saved: {target.name}")
            return True

    print("  Failed")
    return False


# ─── Main ─────────────────────────────────────────────────
def main():
    driver = build_driver()

    try:
        print("Navigating MCA properly...")
        navigate_to_notifications(driver)

        elements = extract_elements(driver)
        print(f"Found {len(elements)} files\n")

        success, failed = 0, 0

        for i, (el, name) in enumerate(elements, 1):
            print(f"[{i}/{len(elements)}] {name[:80]}")

            if click_and_download(driver, el, name):
                success += 1
            else:
                failed += 1

            # Close extra tabs
            while len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])

            time.sleep(DELAY)

    finally:
        driver.quit()

    print("\nDone")
    print(f"Success: {success}")
    print(f"Failed: {failed}")
    print(f"Saved in: {DOWNLOAD_DIR}")


if __name__ == "__main__":
    main()