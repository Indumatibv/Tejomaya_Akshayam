#!/usr/bin/env python3

import time
import os
import base64
import requests
from selenium.webdriver.common.by import By

try:
    import undetected_chromedriver as uc
    USE_UC = True
    print("✅ Using undetected-chromedriver")
except ImportError:
    USE_UC = False
    print("⚠️  undetected-chromedriver not found, using standard selenium")

HOME_URL          = "https://www.mca.gov.in/"
NOTIFICATIONS_URL = "https://www.mca.gov.in/content/mca/global/en/acts-rules/ebooks/notifications.html"

# Correct DMS endpoint (intercepted from browser network traffic)
# val attribute must be base64-encoded before sending as ?doc=
DMS_ENDPOINT = "https://www.mca.gov.in/bin/ebook/dms/getdocument"

MAX_NAV_RETRIES = 5
SAVE_DIR = os.getcwd()


# ──────────────────────────────────────────────
# Driver
# ──────────────────────────────────────────────

def build_driver():
    if USE_UC:
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        return uc.Chrome(options=options)
    else:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        return webdriver.Chrome(options=options)


# ──────────────────────────────────────────────
# Navigation
# ──────────────────────────────────────────────

def on_notifications_page(driver) -> bool:
    return "notifications" in driver.current_url


def navigate_to_notifications(driver) -> bool:
    print("Loading home page to establish session...")
    driver.get(HOME_URL)
    time.sleep(5)

    for attempt in range(1, MAX_NAV_RETRIES + 1):
        print(f"Attempt {attempt}/{MAX_NAV_RETRIES}: navigating to notifications page...")
        driver.get(NOTIFICATIONS_URL)

        deadline = time.time() + 10
        while time.time() < deadline:
            if on_notifications_page(driver):
                break
            time.sleep(0.5)

        print(f"  URL after settle: {driver.current_url}")

        if on_notifications_page(driver):
            print("  ✅ Landed on notifications page.")
            return True

        print("  ⚠️  Redirected — clearing cookies and retrying...")
        driver.delete_all_cookies()
        driver.get(HOME_URL)
        time.sleep(5)

    print("❌ Could not reach notifications page.")
    return False


# ──────────────────────────────────────────────
# Wait for table with redirect recovery
# ──────────────────────────────────────────────

def wait_for_rows(driver, timeout=90):
    print(f"Waiting up to {timeout}s for table data (with redirect recovery)...")
    deadline = time.time() + timeout
    re_nav = 0

    while time.time() < deadline:
        if not on_notifications_page(driver):
            re_nav += 1
            remaining = int(deadline - time.time())
            print(f"  ↩️  Redirected (#{re_nav}, {remaining}s left). Re-navigating...")
            driver.delete_all_cookies()
            driver.get(HOME_URL)
            time.sleep(4)
            driver.get(NOTIFICATIONS_URL)
            settle = time.time() + 10
            while time.time() < settle:
                if on_notifications_page(driver):
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
            print(f"  ✅ Table loaded with {len(valid)} rows.")
            return valid
        time.sleep(1)

    print("  ❌ Timed out waiting for table.")
    return []


# ──────────────────────────────────────────────
# Snapshot rows → plain dicts
# ──────────────────────────────────────────────

def snapshot_rows(driver, count=3) -> list[dict]:
    rows = driver.find_elements(By.XPATH, "//table//tr[td]")
    results = []

    for row in rows:
        try:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 3:
                continue

            title = cols[0].text.strip()
            date  = cols[2].text.strip()

            if not title or title.lower() in ("particulars", "s.no", "sr.no", "#"):
                continue

            anchors = cols[0].find_elements(By.TAG_NAME, "a")
            val = anchors[0].get_attribute("val") if anchors else None

            results.append({"title": title, "date": date, "val": val})

            if len(results) >= count:
                break

        except Exception:
            continue

    return results


# ──────────────────────────────────────────────
# Build DMS URL (val must be base64 encoded)
# Intercepted pattern:
#   GET /bin/ebook/dms/getdocument
#       ?doc=<base64(val)>
#       &docCategory=Notifications
#       &_=<unix_timestamp_ms>
# ──────────────────────────────────────────────

def build_dms_url(val: str) -> str:
    doc_b64 = base64.b64encode(val.encode()).decode()
    timestamp = int(time.time() * 1000)
    return (
        f"{DMS_ENDPOINT}"
        f"?doc={doc_b64}"
        f"&docCategory=Notifications"
        f"&_={timestamp}"
    )


# ──────────────────────────────────────────────
# Download
# ──────────────────────────────────────────────

def get_selenium_cookies(driver) -> dict:
    return {c["name"]: c["value"] for c in driver.get_cookies()}


def sanitize_filename(title: str) -> str:
    title = title.split("|")[0].strip()
    for ch in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']:
        title = title.replace(ch, "_")
    return title[:120]


def download_pdf(driver, val: str, filepath: str) -> bool:
    url = build_dms_url(val)
    print(f"   🌐 URL: {url}")

    cookies    = get_selenium_cookies(driver)
    user_agent = driver.execute_script("return navigator.userAgent;")

    headers = {
        "User-Agent":       user_agent,
        "Referer":          NOTIFICATIONS_URL,
        "Accept":           "*/*",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        resp = requests.get(url, cookies=cookies, headers=headers, timeout=30, stream=True)
        resp.raise_for_status()

        ct   = resp.headers.get("Content-Type", "")
        size = resp.headers.get("Content-Length", "?")
        print(f"   ℹ️  Content-Type: {ct!r}  Content-Length: {size}")

        raw = b"".join(resp.iter_content(chunk_size=8192))

        if len(raw) < 100:
            print(f"   ⚠️  Response too small ({len(raw)} bytes). Body: {raw[:200]}")
            return False

        if not raw.startswith(b"%PDF"):
            print(f"   ⚠️  Not a PDF. First 200 bytes: {raw[:200]}")
            return False

        with open(filepath, "wb") as f:
            f.write(raw)

        print(f"   💾 Saved: {os.path.basename(filepath)} ({len(raw)/1024:.1f} KB)")
        return True

    except Exception as e:
        print(f"   ❌ Download failed: {e}")
        return False


# ──────────────────────────────────────────────
# Main orchestration
# ──────────────────────────────────────────────

def extract_and_download(driver, count=3):
    rows = wait_for_rows(driver, timeout=90)
    if not rows:
        print("No rows found.")
        return

    print("  📋 Snapshotting row data...")
    notifications = snapshot_rows(driver, count=count)

    if not notifications:
        print("Could not snapshot rows.")
        return

    print(f"\nTop {len(notifications)} Notifications:\n" + "=" * 50)

    for i, notif in enumerate(notifications):
        title = notif["title"]
        date  = notif["date"]
        val   = notif["val"]

        print(f"{i+1}. {title}")
        print(f"   Date : {date}")
        print(f"   Val  : {val}  →  base64: {base64.b64encode(val.encode()).decode() if val else 'N/A'}")

        if not val:
            print("   ⚠️  No val — cannot download.\n")
            continue

        filename = sanitize_filename(title) + ".pdf"
        filepath = os.path.join(SAVE_DIR, filename)

        ok = download_pdf(driver, val, filepath)
        if not ok:
            print("   ❌ Could not download PDF.")
        print()


def main():
    driver = build_driver()
    try:
        if not navigate_to_notifications(driver):
            print("Aborting.")
            return
        extract_and_download(driver, count=3)
    finally:
        driver.quit()


if __name__ == "__main__":
    main()