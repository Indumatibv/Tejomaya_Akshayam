#!/usr/bin/env python3

import time
import os
from selenium.webdriver.common.by import By

try:
    import undetected_chromedriver as uc
    USE_UC = True
    print("✅ Using undetected-chromedriver")
except ImportError:
    USE_UC = False

HOME_URL = "https://www.mca.gov.in/"
NOTIFICATIONS_URL = "https://www.mca.gov.in/content/mca/global/en/acts-rules/ebooks/notifications.html"
MAX_NAV_RETRIES = 5


def build_driver():
    if USE_UC:
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        return uc.Chrome(options=options)
    else:
        from selenium import webdriver
        return webdriver.Chrome()


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

    return False


def wait_for_rows(driver, timeout=90):
    print(f"Waiting up to {timeout}s for table rows...")
    deadline = time.time() + timeout
    re_nav = 0

    while time.time() < deadline:
        if not on_notifications_page(driver):
            re_nav += 1
            print(f"  ↩️  Redirected (#{re_nav}). Re-navigating...")
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

    return []


def dump_html(driver):
    """
    Dump the full innerHTML of each cell in the first 3 valid rows.
    This tells us exactly where the PDF link is hidden.
    """
    rows = driver.find_elements(By.XPATH, "//table//tr[td]")
    valid = [
        r for r in rows
        if len(r.find_elements(By.TAG_NAME, "td")) >= 3
        and r.find_elements(By.TAG_NAME, "td")[0].text.strip()
        and r.find_elements(By.TAG_NAME, "td")[0].text.strip().lower()
           not in ("particulars", "s.no", "sr.no", "#")
    ]

    print("\n" + "=" * 80)
    print("RAW HTML DUMP — first 3 valid rows")
    print("=" * 80)

    for i, row in enumerate(valid[:3]):
        cols = row.find_elements(By.TAG_NAME, "td")
        print(f"\n{'─'*40} Row {i+1} {'─'*40}")
        for j, col in enumerate(cols):
            print(f"\n  [col {j}] text     : {repr(col.text.strip())}")
            print(f"  [col {j}] innerHTML:\n{col.get_attribute('innerHTML').strip()}")

    # Also dump the entire table's outerHTML so we can see all attributes
    print("\n\n" + "=" * 80)
    print("FULL TABLE outerHTML (first 4000 chars)")
    print("=" * 80)
    try:
        table = driver.find_element(By.TAG_NAME, "table")
        print(table.get_attribute("outerHTML")[:4000])
    except Exception as e:
        print(f"Could not get table: {e}")


def main():
    driver = build_driver()
    try:
        if not navigate_to_notifications(driver):
            print("Could not reach notifications page.")
            return

        rows = wait_for_rows(driver, timeout=90)
        if not rows:
            print("No rows found.")
            return

        dump_html(driver)

    finally:
        driver.quit()


if __name__ == "__main__":
    main()