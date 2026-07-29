"""Retake screenshots for BPM, UAT Dashboard, and Product Lifecycle with new data."""
import asyncio
from playwright.async_api import async_playwright

BASE = "http://localhost:8080"
OUT  = "email_screenshots"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            device_scale_factor=2,
        )
        page = await context.new_page()

        # Login
        print("Logging in ...")
        await page.goto(f"{BASE}/login", wait_until="networkidle", timeout=15000)
        await page.wait_for_timeout(1000)
        await page.fill('input[placeholder="e.g. eng_manager"]', "prog_director", timeout=5000)
        await page.fill('input[type="password"]', "kpmg1234", timeout=3000)
        await page.click('button[type="submit"]', timeout=3000)
        await page.wait_for_timeout(3000)
        print("  -> logged in")

        pages = [
            ("06_BPM",              "/enterprise/bpm"),
            ("09_UAT_Dashboard",    "/uat"),
            ("12_Product_Lifecycle","/product-dashboard"),
        ]

        for name, path in pages:
            print(f"Capturing: {name} ...")
            await page.goto(f"{BASE}{path}", wait_until="networkidle", timeout=15000)
            await page.wait_for_timeout(3000)
            await page.screenshot(path=f"{OUT}/{name}.png", full_page=True)
            print(f"  -> saved {name}.png")

        await browser.close()
        print("\nDone!")

if __name__ == "__main__":
    asyncio.run(main())
