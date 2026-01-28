import asyncio
from playwright.async_api import async_playwright

TARGET = "https://www.mca.gov.in/content/mca/global/en/acts-rules/ebooks/notifications.html"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir="/tmp/mca-profile-2",
            headless=False,
            viewport={"width": 1920, "height": 1080},
            timezone_id="Asia/Kolkata",
            args=["--disable-blink-features=AutomationControlled"]
        )

        page = await browser.new_page()
        await page.goto(TARGET, wait_until="networkidle")
        await page.wait_for_timeout(4000)

        print("PAGE TITLE:", await page.title())

        pdf_links = await page.query_selector_all(
            'a[href*="getdocument"]'
        )

        print("TOTAL PDF LINKS FOUND:", len(pdf_links))
        print("-" * 60)

        for i, link in enumerate(pdf_links[:5], start=1):
            container = await link.evaluate_handle(
                "el => el.closest('tr, div, li')"
            )
            text = await container.evaluate("el => el.innerText")
            print(f"[{i}]")
            print(text.strip())
            print("-" * 60)

        await browser.close()

asyncio.run(main())
