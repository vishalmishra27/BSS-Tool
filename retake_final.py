"""Retake UAT + BPM Tasks tab screenshots."""
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

        # UAT Dashboard
        print("Capturing: UAT Dashboard ...")
        await page.goto(f"{BASE}/uat", wait_until="networkidle", timeout=15000)
        await page.wait_for_timeout(2500)
        await page.screenshot(path=f"{OUT}/09_UAT_Dashboard.png", full_page=True)
        print("  -> saved")

        # BPM — Tasks tab
        print("Capturing: BPM Tasks tab ...")
        await page.goto(f"{BASE}/enterprise/bpm", wait_until="networkidle", timeout=15000)
        await page.wait_for_timeout(2000)
        # Click the Tasks tab
        try:
            await page.click('text=Tasks', timeout=3000)
            await page.wait_for_timeout(2000)
        except:
            pass
        await page.screenshot(path=f"{OUT}/06b_BPM_Tasks.png", full_page=True)
        print("  -> saved")

        # BPM — Users tab
        print("Capturing: BPM Users tab ...")
        try:
            await page.click('text=Users', timeout=3000)
            await page.wait_for_timeout(2000)
        except:
            pass
        await page.screenshot(path=f"{OUT}/06c_BPM_Users.png", full_page=True)
        print("  -> saved")

        await browser.close()
        print("\nDone!")

if __name__ == "__main__":
    asyncio.run(main())
