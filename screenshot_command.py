import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
        page = await ctx.new_page()
        await page.goto("http://localhost:8080/login", wait_until="networkidle", timeout=15000)
        await page.wait_for_timeout(1000)
        await page.fill('input[placeholder="e.g. eng_manager"]', "prog_director", timeout=5000)
        await page.fill('input[type="password"]', "kpmg1234", timeout=3000)
        await page.click('button[type="submit"]', timeout=3000)
        await page.wait_for_timeout(3000)
        await page.goto("http://localhost:8080/agent/command", wait_until="networkidle", timeout=15000)
        await page.wait_for_timeout(2000)
        await page.screenshot(path="email_screenshots/16_Command_Agent.png", full_page=True)
        print("Saved 16_Command_Agent.png")
        await browser.close()

asyncio.run(main())
