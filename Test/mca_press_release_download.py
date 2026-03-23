#!/usr/bin/env python3
"""
MCA Press Release downloader.
Endpoint: /bin/dms/getdocument?mds=<data-value>&type=open
data-value is already URL-encoded in the HTML — pass it as-is in the query string.
"""

import time
import os
import requests
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchWindowException, WebDriverException

try:
    import undetected_chromedriver as uc
    USE_UC = True
    print("✅ Using undetected-chromedriver")
except ImportError:
    USE_UC = False

HOME_URL = "https://www.mca.gov.in/"
PR_URL   = "https://www.mca.gov.in/content/mca/global/en/notifications-tender/news-updates/press-release.html"
DMS_BASE = "https://www.mca.gov.in/bin/dms/getdocument"

MAX_DRV_RETRIES = 3
MAX_NAV_RETRIES = 5
SAVE_DIR        = os.getcwd()


def build_driver():
    if USE_UC:
        opts = uc.ChromeOptions()
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        return uc.Chrome(options=opts)
    else:
        from selenium import webdriver
        return webdriver.Chrome()


def on_pr_page(driver) -> bool:
    return "press-release" in driver.current_url


def navigate_to_pr(driver) -> bool:
    print("Loading home to establish session...")
    driver.get(HOME_URL)
    time.sleep(5)

    for attempt in range(1, MAX_NAV_RETRIES + 1):
        print(f"Attempt {attempt}/{MAX_NAV_RETRIES}...")
        driver.get(PR_URL)

        deadline = time.time() + 10
        while time.time() < deadline:
            if on_pr_page(driver):
                break
            time.sleep(0.5)

        if on_pr_page(driver):
            print(f"  ✅ On PR page.")
            return True

        print(f"  ⚠️  Redirected — retrying...")
        driver.delete_all_cookies()
        driver.get(HOME_URL)
        time.sleep(5)

    return False


def wait_for_rows(driver, timeout=90) -> list:
    print(f"Waiting up to {timeout}s for rows...")
    deadline = time.time() + timeout
    re_nav = 0

    while time.time() < deadline:
        if not on_pr_page(driver):
            re_nav += 1
            remaining = int(deadline - time.time())
            print(f"  ↩️  Redirected (#{re_nav}, {remaining}s left)...")
            driver.delete_all_cookies()
            driver.get(HOME_URL)
            time.sleep(4)
            driver.get(PR_URL)
            settle = time.time() + 10
            while time.time() < settle:
                if on_pr_page(driver):
                    break
                time.sleep(0.5)
            time.sleep(3)
            continue

        rows = driver.find_elements(By.XPATH, "//table//tr[td]")
        valid = [r for r in rows
                 if len(r.find_elements(By.TAG_NAME, "td")) >= 2
                 and r.find_elements(By.TAG_NAME, "td")[0].text.strip()]
        if valid:
            print(f"  ✅ {len(valid)} rows.")
            return valid
        time.sleep(1)

    print("  ❌ Timed out.")
    return []


def snapshot_rows(driver, count=3) -> list[dict]:
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
            anchor = None
            for col in cols:
                anchors = col.find_elements(By.CSS_SELECTOR, "a[data-value]")
                if anchors:
                    anchor = anchors[0]
                    break
            if not anchor:
                continue
            # data-value is URL-encoded in the HTML e.g. "pfP4Gkc7e%2FOB0p6rQQBfwQ%3D%3D"
            data_value = anchor.get_attribute("data-value") or ""
            results.append({
                "title":      title,
                "date_str":   date_str,
                "data_value": data_value,
            })
            if len(results) >= count:
                break
        except Exception:
            continue
    print(f"  📋 {len(results)} rows snapshotted.")
    return results


def build_dms_url(data_value: str) -> str:
    """
    Endpoint: /bin/dms/getdocument?mds=<data-value>&type=open
    data-value is already URL-encoded — pass it directly in the query string
    so it gets correctly decoded by the server.
    """
    return f"{DMS_BASE}?mds={data_value}&type=open"


def sanitize_filename(title: str) -> str:
    import re, unicodedata
    title = title.split("|")[0].strip()
    t = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
    t = re.sub(r'[^A-Za-z0-9]+', '_', t)
    t = re.sub(r'_+', '_', t).strip('_')
    return (t[:100] or "document") + ".pdf"


def download_pdf(data_value: str, filepath: str, cookies: dict, user_agent: str) -> bool:
    url = build_dms_url(data_value)
    print(f"   🌐 URL: {url}")

    headers = {
        "User-Agent":       user_agent,
        "Referer":          PR_URL,
        "Accept":           "application/pdf,*/*",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        resp = requests.get(url, cookies=cookies, headers=headers, timeout=30, stream=True)
        resp.raise_for_status()
        raw = b"".join(resp.iter_content(chunk_size=8192))

        ct = resp.headers.get("Content-Type", "")
        print(f"   ℹ️  Content-Type: {ct!r}  Size: {len(raw)} bytes")

        if len(raw) < 100 or not raw.startswith(b"%PDF"):
            print(f"   ⚠️  Not a valid PDF. First 100 bytes: {raw[:100]}")
            return False

        with open(filepath, "wb") as f:
            f.write(raw)

        print(f"   💾 Saved: {os.path.basename(filepath)} ({len(raw)/1024:.1f} KB)")
        return True

    except Exception as e:
        print(f"   ❌ Download failed: {e}")
        return False


def get_snapshot() -> tuple[list[dict], dict, str]:
    for drv_attempt in range(1, MAX_DRV_RETRIES + 1):
        driver = None
        try:
            print(f"\nDriver attempt {drv_attempt}/{MAX_DRV_RETRIES}")
            driver = build_driver()
            time.sleep(2)

            if not navigate_to_pr(driver):
                return [], {}, ""

            wait_for_rows(driver, timeout=90)
            rows       = snapshot_rows(driver, count=3)
            cookies    = {c["name"]: c["value"] for c in driver.get_cookies()}
            user_agent = driver.execute_script("return navigator.userAgent;")
            return rows, cookies, user_agent

        except (NoSuchWindowException, WebDriverException) as e:
            print(f"⚠️  Driver lost: {e}. Retrying...")
            time.sleep(3)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    print("❌ All driver attempts failed.")
    return [], {}, ""


def main():
    rows, cookies, user_agent = get_snapshot()

    if not rows:
        print("No rows captured.")
        return

    print(f"\nTop {len(rows)} Press Releases:\n" + "=" * 50)

    for i, notif in enumerate(rows):
        title      = notif["title"]
        date_str   = notif["date_str"]
        data_value = notif["data_value"]

        print(f"{i+1}. {title}")
        print(f"   Date       : {date_str}")
        print(f"   data-value : {data_value}")

        filename = sanitize_filename(title)
        filepath = os.path.join(SAVE_DIR, filename)

        download_pdf(data_value, filepath, cookies, user_agent)
        print()


if __name__ == "__main__":
    main()