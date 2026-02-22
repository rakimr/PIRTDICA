import asyncio
from playwright.async_api import async_playwright

CHARTS = [
    ("player-shot", "Mark Williams", "articles/chart_mark_williams_shots.png"),
    ("player-shot", "LaMelo Ball", "articles/chart_lamelo_ball_shots.png"),
    ("player-shot", "Jalen Green", "articles/chart_jalen_green_shots.png"),
    ("player-shot", "OG Anunoby", "articles/chart_og_anunoby_shots.png"),
    ("team-defense-shot", "POR", "articles/chart_por_defense_shots.png"),
    ("team-defense-shot", "WAS", "articles/chart_was_defense_shots.png"),
    ("team-defense-shot", "CHI", "articles/chart_chi_defense_shots.png"),
    ("def-scheme", "POR", "articles/chart_por_defense_scheme.png"),
    ("def-scheme", "WAS", "articles/chart_was_defense_scheme.png"),
    ("def-scheme", "CHI", "articles/chart_chi_defense_scheme.png"),
]

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path="/nix/store/qa9cnw4v5xkxyip6mb9kxqfq1z4x2dx1-chromium-138.0.7204.100/bin/chromium",
            args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page(viewport={"width": 1200, "height": 900}, device_scale_factor=2)

        for chart_type, target, output_path in CHARTS:
            url = f"http://localhost:5000/chart-screenshot/{chart_type}/{target}"
            print(f"Capturing {chart_type}/{target} -> {output_path}")
            await page.goto(url, wait_until="networkidle")
            await page.wait_for_timeout(500)

            container = page.locator("#chart-container")
            await container.screenshot(path=output_path)
            print(f"  Saved {output_path}")

        await browser.close()
        print("\nAll 10 charts captured!")

asyncio.run(main())
