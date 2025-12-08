from playwright.sync_api import sync_playwright

API_URL = "https://www.mca.gov.in/bin/mca/getEBookNotifications"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    # shared container (no nonlocal required)
    store = {"data": None}

    # response handler
    def handle_response(response):
        if response.url == API_URL and response.status == 200:
            try:
                store["data"] = response.json()
            except:
                pass

    page.on("response", handle_response)

    print("Loading MCA Notifications Page…")

    page.goto(
        "https://www.mca.gov.in/content/mca/global/en/acts-rules/ebooks/notifications.html",
        wait_until="networkidle",
        timeout=90000
    )

    # wait for JS API call
    page.wait_for_timeout(3000)

    if not store["data"]:
        raise Exception("❌ Could not capture MCA API")

    print("✅ Fetched MCA Notifications\n")

    for item in store["data"]:
        print({
            "title": item.get("title"),
            "date": item.get("date"),
            "pdf": item.get("downloadLink"),
            "size": item.get("size"),
        })

    browser.close()
