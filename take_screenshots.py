"""
Automated screenshot capture for BSS Tool — email attachments.
Uses Playwright to navigate each page and take full-page screenshots.

Usage:
  1. Start backend:   cd backend && python app.py
  2. Start frontend:  cd frontend && npm run dev
  3. Run this:        python take_screenshots.py
"""

import asyncio
from playwright.async_api import async_playwright

BASE = "http://localhost:8080"
OUT  = "email_screenshots"

USERNAME = "prog_director"
PASSWORD = "kpmg1234"

PAGES = [
    ("01_Login_Page",               "/login",                  False),
    ("02_Landing_Page",             "/",                       True),
    ("03_Transformation_Dashboard", "/dashboard",              True),
    ("04_Milestone_Tracker",        "/milestones",             True),
    ("05_Workflow_Tracker",         "/workflow",               True),
    ("06_BPM",                      "/enterprise/bpm",         True),
    ("07_Migration_Summary",        "/enterprise/bpm-summary", True),
    ("08_Network_IO",               "/enterprise/network",     True),
    ("09_UAT_Dashboard",            "/uat",                    True),
    ("10_UAT_Automation",           "/uat-automation",         True),
    ("11_Reconciliation_Engine",    "/reconciliation",         True),
    ("12_Product_Lifecycle",        "/product-dashboard",      True),
    ("13_AI_Agents",                "/agent/about",            True),
    ("14_Document_Intelligence",    "/agent/ocr",              True),
    ("15_Audit_Log",                "/audit-log",              True),
]


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            device_scale_factor=2,
        )
        page = await context.new_page()

        # 1. Login page screenshot (before auth)
        print("Capturing: 01_Login_Page ...")
        await page.goto(f"{BASE}/login", wait_until="networkidle", timeout=15000)
        await page.wait_for_timeout(1500)
        await page.screenshot(path=f"{OUT}/01_Login_Page.png", full_page=True)
        print("  -> saved 01_Login_Page.png")

        # 2. Log in
        print("Logging in as prog_director ...")
        try:
            await page.fill('input[placeholder="e.g. eng_manager"]', USERNAME, timeout=5000)
            await page.fill('input[type="password"]', PASSWORD, timeout=3000)
            await page.click('button[type="submit"]', timeout=3000)
            await page.wait_for_timeout(3000)
            print("  -> logged in")
        except Exception as e:
            print(f"  -> login note: {e}")

        # 3. Capture all pages
        for name, path, needs_auth in PAGES:
            if name == "01_Login_Page":
                continue

            print(f"Capturing: {name} ...")
            try:
                await page.goto(f"{BASE}{path}", wait_until="networkidle", timeout=15000)
                await page.wait_for_timeout(2500)

                await page.screenshot(path=f"{OUT}/{name}.png", full_page=True)
                print(f"  -> saved {name}.png")
            except Exception as e:
                print(f"  -> FAILED {name}: {e}")

        await browser.close()
        print(f"\nAll screenshots saved to ./{OUT}/")


if __name__ == "__main__":
    asyncio.run(main())
