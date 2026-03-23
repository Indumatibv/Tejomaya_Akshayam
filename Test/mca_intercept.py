#!/usr/bin/env python3
"""
This script intercepts ALL network requests made when MCA's JS
handles the dmslink click — so we can see the exact URL, method,
headers, and response that delivers the PDF.
"""

import time
import json
from selenium.webdriver.common.by import By

try:
    import undetected_chromedriver as uc
    USE_UC = True
    print("✅ Using undetected-chromedriver")
except ImportError:
    USE_UC = False

HOME_URL          = "https://www.mca.gov.in/"
NOTIFICATIONS_URL = "https://www.mca.gov.in/content/mca/global/en/acts-rules/ebooks/notifications.html"
MAX_NAV_RETRIES   = 5


def build_driver():
    if USE_UC:
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        # Enable performance logging to capture network events
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        return uc.Chrome(options=options)
    else:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument("--no-sandbox")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        return webdriver.Chrome(options=options)


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

        if on_notifications_page(driver):
            print(f"  ✅ Landed on notifications page.")
            return True

        print("  ⚠️  Redirected — retrying...")
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


def get_network_events(driver) -> list:
    """Extract all network request/response events from performance log."""
    events = []
    try:
        logs = driver.get_log("performance")
        for entry in logs:
            msg = json.loads(entry["message"])["message"]
            if msg["method"] in (
                "Network.requestWillBeSent",
                "Network.responseReceived",
                "Network.loadingFinished",
                "Network.loadingFailed",
            ):
                events.append(msg)
    except Exception as e:
        print(f"  ⚠️  Could not get performance log: {e}")
    return events


def click_first_link_and_intercept(driver):
    """
    Enable CDP network monitoring, click the first dmslink,
    wait for network activity, then dump all captured requests.
    """
    # Enable CDP Network domain
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        print("  ✅ CDP Network monitoring enabled.")
    except Exception as e:
        print(f"  ⚠️  CDP enable failed: {e}")

    # Clear existing performance logs by reading them
    try:
        driver.get_log("performance")
    except Exception:
        pass

    # Find the first dmslink anchor
    rows = wait_for_rows(driver, timeout=90)
    if not rows:
        print("No rows found.")
        return

    anchors = driver.find_elements(By.CSS_SELECTOR, "a.dmslink")
    if not anchors:
        print("No dmslink anchors found.")
        return

    first = anchors[0]
    val   = first.get_attribute("val")
    text  = first.text.strip()
    print(f"\nClicking: {text[:80]}")
    print(f"Val: {val}")
    print("Intercepting network traffic...\n")

    # Click via JS to trigger MCA's onclick handler
    driver.execute_script("arguments[0].click();", first)

    # Wait for any network activity to settle
    time.sleep(6)

    # Collect network events
    events = get_network_events(driver)

    print("=" * 80)
    print("NETWORK EVENTS CAPTURED:")
    print("=" * 80)

    seen_urls = set()
    for ev in events:
        method = ev["method"]
        params = ev.get("params", {})

        if method == "Network.requestWillBeSent":
            req  = params.get("request", {})
            url  = req.get("url", "")
            meth = req.get("method", "")
            hdrs = req.get("headers", {})
            body = req.get("postData", "")

            if url in seen_urls:
                continue
            seen_urls.add(url)

            print(f"\n→ REQUEST: [{meth}] {url}")
            if body:
                print(f"  POST body: {body}")
            # Print relevant headers
            for h in ["Content-Type", "X-Requested-With", "Referer", "Accept", "Cookie"]:
                if h in hdrs:
                    val_h = hdrs[h]
                    if h == "Cookie":
                        val_h = val_h[:80] + "..."  # truncate cookies
                    print(f"  {h}: {val_h}")

        elif method == "Network.responseReceived":
            resp = params.get("response", {})
            url  = resp.get("url", "")
            status = resp.get("status", "")
            ct   = resp.get("headers", {}).get("content-type", resp.get("headers", {}).get("Content-Type", ""))
            cl   = resp.get("headers", {}).get("content-length", resp.get("headers", {}).get("Content-Length", "?"))
            loc  = resp.get("headers", {}).get("location", resp.get("headers", {}).get("Location", ""))

            print(f"\n← RESPONSE: {status} {url}")
            print(f"   Content-Type: {ct}  Content-Length: {cl}")
            if loc:
                print(f"   Location (redirect): {loc}")

    print("\n" + "=" * 80)
    print(f"Total events captured: {len(events)}")
    print(f"Current URL after click: {driver.current_url}")
    print(f"Window count: {len(driver.window_handles)}")


def main():
    driver = build_driver()
    try:
        if not navigate_to_notifications(driver):
            print("Could not reach notifications page.")
            return
        click_first_link_and_intercept(driver)
    finally:
        driver.quit()


if __name__ == "__main__":
    main()