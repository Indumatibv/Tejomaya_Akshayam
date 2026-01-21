from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json
import time

URL = "https://www.mca.gov.in/content/mca/global/en/application-history.html"

def scrape_mca_page(url: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,   # IMPORTANT: keep visible
            args=["--start-maximized"]
        )

        context = browser.new_context(
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/143.0.0.0 Safari/537.36"
            )
        )

        page = context.new_page()

        print("Opening MCA page...")
        page.goto(url, wait_until="networkidle")

        # Give MCA JS time
        time.sleep(5)

        print("Page title:", page.title())

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        blocks = soup.select("div.cmp-text")

        paragraphs = []
        for b in blocks:
            text = b.get_text(" ", strip=True)
            if text:
                paragraphs.append(text)

        browser.close()

        return paragraphs


if __name__ == "__main__":
    content = scrape_mca_page(URL)

    print(f"\nExtracted {len(content)} blocks\n")
    for i, para in enumerate(content, 1):
        print(f"[{i}] {para}")
        print("-" * 90)
