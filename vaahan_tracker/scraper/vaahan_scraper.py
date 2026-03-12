"""
Vaahan Dashboard Scraper

Scrapes maker-wise vehicle registration data from the Vaahan dashboard
(https://vahan.parivahan.gov.in/vahan4dashboard/).

The dashboard is a JSF/PrimeFaces application. This scraper:
1. First attempts to discover and use underlying XHR/API endpoints
2. Falls back to full Playwright browser automation if API approach fails

Handles anti-bot measures with randomized delays, stealth mode, and retry logic.
"""

import asyncio
import csv
import json
import logging
import random
import sys
from datetime import datetime
from pathlib import Path

import yaml

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    async_playwright = None
    PlaywrightTimeout = TimeoutError
    logging.getLogger(__name__).warning(
        "Playwright not installed. Run: pip install playwright && playwright install chromium"
    )

try:
    from scraper.network_interceptor import NetworkInterceptor
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scraper.network_interceptor import NetworkInterceptor

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def generate_month_range(start: str, end: str) -> list[dict]:
    """Generate list of {year, month, label} dicts from YYYY-MM range."""
    months = []
    start_dt = datetime.strptime(start, "%Y-%m")
    end_dt = datetime.strptime(end, "%Y-%m")
    current = start_dt
    while current <= end_dt:
        months.append({
            "year": current.year,
            "month": current.month,
            "label": current.strftime("%b-%y"),
            "value": current.strftime("%Y-%m"),
        })
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


async def random_delay(min_s: float, max_s: float) -> None:
    delay = random.uniform(min_s, max_s)
    logger.debug(f"Waiting {delay:.1f}s")
    await asyncio.sleep(delay)


class VaahanScraper:
    """Playwright-based scraper for the Vaahan dashboard."""

    DASHBOARD_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/"
    REPORT_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"

    def __init__(self, config: dict):
        self.config = config
        self.scraper_cfg = config["scraper"]
        self.interceptor = NetworkInterceptor()
        self.raw_data_dir = BASE_DIR / config["paths"]["raw_data"]
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    async def scrape_all(self) -> list[dict]:
        """Main entry point: scrape all configured months."""
        if async_playwright is None:
            raise RuntimeError("Playwright is not installed. Run: pip install playwright && playwright install chromium")
        months = generate_month_range(
            self.scraper_cfg["start_month"],
            self.scraper_cfg["end_month"],
        )

        # If rescraping, always include last N months regardless
        rescrape_n = self.scraper_cfg.get("rescrape_last_months", 2)
        logger.info(f"Will scrape {len(months)} months (re-scraping last {rescrape_n})")

        all_data = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.scraper_cfg.get("headless", True),
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            # Stealth: override navigator.webdriver
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """)

            page = await context.new_page()
            await self.interceptor.attach(page)

            # Initial page load
            try:
                logger.info(f"Loading dashboard: {self.DASHBOARD_URL}")
                await page.goto(
                    self.DASHBOARD_URL,
                    timeout=self.scraper_cfg["page_load_timeout_ms"],
                    wait_until="networkidle",
                )
                await random_delay(2, 4)
            except PlaywrightTimeout:
                logger.warning("Dashboard initial load timed out, continuing anyway")

            # Try to navigate to maker-wise report view
            try:
                await self._navigate_to_maker_report(page)
            except Exception as e:
                logger.error(f"Failed to navigate to maker report: {e}")
                # Export intercepted endpoints for manual analysis
                self.interceptor.export_captured(
                    self.raw_data_dir / "intercepted_endpoints.json"
                )
                await browser.close()
                return all_data

            # Scrape each month × category
            for month_info in months:
                for category in self.config["vehicle_categories"]:
                    for attempt in range(self.scraper_cfg["max_retries"]):
                        try:
                            data = await self._scrape_month_category(
                                page, month_info, category
                            )
                            if data:
                                all_data.extend(data)
                                self._save_raw(month_info, category, data)
                            break
                        except PlaywrightTimeout:
                            wait = self.scraper_cfg["retry_backoff_base"] ** (attempt + 1)
                            logger.warning(
                                f"Timeout scraping {month_info['label']} / {category}, "
                                f"retry {attempt + 1}/{self.scraper_cfg['max_retries']} "
                                f"after {wait}s"
                            )
                            await asyncio.sleep(wait)
                        except Exception as e:
                            logger.error(
                                f"Error scraping {month_info['label']} / {category}: {e}"
                            )
                            if attempt == self.scraper_cfg["max_retries"] - 1:
                                logger.error("Max retries reached, skipping")
                            await asyncio.sleep(2)

                    await random_delay(
                        self.scraper_cfg["min_delay_seconds"],
                        self.scraper_cfg["max_delay_seconds"],
                    )

            # Export intercepted endpoints
            self.interceptor.export_captured(
                self.raw_data_dir / "intercepted_endpoints.json"
            )
            await browser.close()

        logger.info(f"Scraping complete. Total records: {len(all_data)}")
        return all_data

    async def _navigate_to_maker_report(self, page) -> None:
        """Navigate to the maker-wise registration report view."""
        logger.info("Navigating to maker-wise report...")

        # The Vaahan dashboard has multiple tabs/views. We need the
        # "Maker Wise" or "Top Maker" view which shows OEM registration counts.
        # Try direct navigation first
        try:
            await page.goto(
                self.REPORT_URL,
                timeout=self.scraper_cfg["page_load_timeout_ms"],
                wait_until="networkidle",
            )
            await random_delay(2, 3)
        except PlaywrightTimeout:
            logger.warning("Report view load timed out")

        # Look for maker-related navigation elements
        maker_selectors = [
            "text=Maker",
            "text=maker",
            "text=Top Maker",
            "text=MAKER",
            "[id*='maker']",
            "[class*='maker']",
            "a:has-text('Maker')",
            "button:has-text('Maker')",
            ".ui-menuitem-link:has-text('Maker')",
        ]

        for selector in maker_selectors:
            try:
                element = page.locator(selector).first
                if await element.is_visible(timeout=3000):
                    await element.click()
                    await random_delay(2, 3)
                    logger.info(f"Clicked maker navigation: {selector}")
                    return
            except Exception:
                continue

        logger.warning(
            "Could not find maker-specific navigation. "
            "Will attempt to scrape from current view."
        )

    async def _scrape_month_category(
        self, page, month_info: dict, category: str
    ) -> list[dict]:
        """Scrape registration data for a specific month and vehicle category."""
        logger.info(f"Scraping {month_info['label']} / {category}")

        # Select month/year filter
        await self._select_month_year(page, month_info)
        await random_delay(1, 2)

        # Select vehicle category
        await self._select_category(page, category)
        await random_delay(1, 2)

        # Wait for table to update
        await self._wait_for_table_update(page)

        # Extract table data
        return await self._extract_table_data(page, month_info, category)

    async def _select_month_year(self, page, month_info: dict) -> None:
        """Select the month and year from dropdown filters."""
        month_names = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ]
        month_name = month_names[month_info["month"] - 1]
        year_str = str(month_info["year"])

        # Try common dropdown patterns for PrimeFaces
        year_selectors = [
            f"select[id*='year'] option[value='{year_str}']",
            f"select[id*='Year'] option[value='{year_str}']",
            f"[id*='year']",
            f"[id*='Year']",
        ]
        month_selectors = [
            f"select[id*='month'] option[value='{month_info['month']}']",
            f"select[id*='Month'] option[value='{month_info['month']}']",
            f"[id*='month']",
            f"[id*='Month']",
        ]

        # Try selecting year
        for sel in year_selectors:
            try:
                elem = page.locator(sel).first
                if await elem.is_visible(timeout=2000):
                    if "select" in sel.lower():
                        await page.select_option(sel.split(" option")[0], year_str)
                    else:
                        await elem.click()
                        await random_delay(0.5, 1)
                        # Try to find and click the year value
                        year_option = page.locator(f"text={year_str}").first
                        if await year_option.is_visible(timeout=2000):
                            await year_option.click()
                    logger.debug(f"Selected year: {year_str}")
                    break
            except Exception:
                continue

        await random_delay(0.5, 1)

        # Try selecting month
        for sel in month_selectors:
            try:
                elem = page.locator(sel).first
                if await elem.is_visible(timeout=2000):
                    if "select" in sel.lower():
                        await page.select_option(
                            sel.split(" option")[0], str(month_info["month"])
                        )
                    else:
                        await elem.click()
                        await random_delay(0.5, 1)
                        month_option = page.locator(f"text={month_name}").first
                        if await month_option.is_visible(timeout=2000):
                            await month_option.click()
                    logger.debug(f"Selected month: {month_name}")
                    break
            except Exception:
                continue

    async def _select_category(self, page, category: str) -> None:
        """Select the vehicle category filter."""
        category_selectors = [
            f"select[id*='category'] option:has-text('{category}')",
            f"select[id*='Category'] option:has-text('{category}')",
            f"select[id*='vchCat'] option:has-text('{category}')",
            f"[id*='category']",
            f"[id*='vchCat']",
        ]

        for sel in category_selectors:
            try:
                elem = page.locator(sel).first
                if await elem.is_visible(timeout=2000):
                    if "select" in sel.lower() and "option" in sel:
                        select_sel = sel.split(" option")[0]
                        await page.select_option(select_sel, label=category)
                    else:
                        await elem.click()
                        await random_delay(0.5, 1)
                        cat_option = page.locator(f"text={category}").first
                        if await cat_option.is_visible(timeout=2000):
                            await cat_option.click()
                    logger.debug(f"Selected category: {category}")
                    break
            except Exception:
                continue

    async def _wait_for_table_update(self, page) -> None:
        """Wait for the data table to finish updating after filter change."""
        # Wait for any loading indicators to disappear
        loading_selectors = [
            ".ui-blockui",
            ".ui-loading",
            "[class*='loading']",
            "[class*='spinner']",
        ]
        for sel in loading_selectors:
            try:
                elem = page.locator(sel).first
                if await elem.is_visible(timeout=1000):
                    await elem.wait_for(state="hidden", timeout=30000)
            except Exception:
                pass

        # Also wait for network idle
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

    async def _extract_table_data(
        self, page, month_info: dict, category: str
    ) -> list[dict]:
        """Extract OEM registration data from the visible table."""
        records = []

        # Common table selectors for PrimeFaces data tables
        table_selectors = [
            "table[id*='groupTable'] tbody tr",
            "table[id*='dataTable'] tbody tr",
            ".ui-datatable tbody tr",
            "table.dataTable tbody tr",
            "#dataTable tbody tr",
            "table tbody tr",
        ]

        rows = None
        for sel in table_selectors:
            try:
                candidate = page.locator(sel)
                count = await candidate.count()
                if count > 0:
                    rows = candidate
                    logger.debug(f"Found {count} rows with selector: {sel}")
                    break
            except Exception:
                continue

        if not rows:
            logger.warning(f"No table rows found for {month_info['label']} / {category}")
            return records

        row_count = await rows.count()
        for i in range(row_count):
            row = rows.nth(i)
            cells = row.locator("td")
            cell_count = await cells.count()

            if cell_count < 2:
                continue

            try:
                # Typical layout: [S.No, Maker/OEM Name, Registration Count, ...]
                # or [Maker/OEM Name, Registration Count]
                cell_texts = []
                for j in range(cell_count):
                    text = (await cells.nth(j).text_content() or "").strip()
                    cell_texts.append(text)

                # Heuristic: find the OEM name (longest text) and count (numeric)
                oem_name = None
                reg_count = None

                for text in cell_texts:
                    clean = text.replace(",", "").strip()
                    if clean.isdigit() and int(clean) > 0:
                        if reg_count is None or int(clean) > reg_count:
                            reg_count = int(clean)
                    elif len(text) > 2 and not text.isdigit():
                        # Skip serial numbers
                        if text.replace(".", "").isdigit():
                            continue
                        if oem_name is None or len(text) > len(oem_name):
                            oem_name = text

                if oem_name and reg_count is not None:
                    records.append({
                        "month": month_info["value"],
                        "month_label": month_info["label"],
                        "oem_raw": oem_name.upper().strip(),
                        "category_raw": category,
                        "units": reg_count,
                        "fuel_type": "ALL",  # Will be refined if fuel filter active
                        "scraped_at": datetime.now().isoformat(),
                    })
            except Exception as e:
                logger.debug(f"Error parsing row {i}: {e}")
                continue

        logger.info(
            f"Extracted {len(records)} records for {month_info['label']} / {category}"
        )
        return records

    def _save_raw(self, month_info: dict, category: str, data: list[dict]) -> None:
        """Save raw scraped data for audit trail."""
        safe_cat = category.replace(" ", "_").replace("(", "").replace(")", "")
        filename = f"raw_{month_info['value']}_{safe_cat}.json"
        filepath = self.raw_data_dir / filename
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        logger.debug(f"Saved raw data: {filepath}")


class VaahanAPIClient:
    """
    Direct API client for Vaahan dashboard endpoints.

    If the network interceptor discovers usable XHR endpoints, this client
    can hit them directly without browser automation — much faster and more
    reliable than Playwright scraping.
    """

    def __init__(self, config: dict):
        self.config = config
        self.base_url = "https://vahan.parivahan.gov.in/vahan4dashboard/"
        self.session = None
        self._endpoints = None

    async def discover_endpoints(self) -> dict | None:
        """Use a headless browser session to discover API endpoints."""
        if async_playwright is None:
            raise RuntimeError("Playwright is not installed. Run: pip install playwright && playwright install chromium")
        interceptor = NetworkInterceptor()
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await interceptor.attach(page)

            try:
                await page.goto(
                    self.base_url,
                    timeout=self.config["scraper"]["page_load_timeout_ms"],
                    wait_until="networkidle",
                )
                await asyncio.sleep(5)

                # Click around to trigger AJAX calls
                clickables = page.locator("a, button, .ui-menuitem-link")
                count = await clickables.count()
                for i in range(min(count, 10)):
                    try:
                        elem = clickables.nth(i)
                        if await elem.is_visible(timeout=1000):
                            await elem.click()
                            await asyncio.sleep(2)
                    except Exception:
                        continue

            except Exception as e:
                logger.error(f"Endpoint discovery failed: {e}")
            finally:
                await browser.close()

        form_endpoints = interceptor.get_form_post_endpoints()
        api_endpoints = interceptor.get_api_endpoints()

        if form_endpoints or api_endpoints:
            self._endpoints = {
                "form": [
                    {"url": ep.url, "post_data": ep.post_data}
                    for ep in form_endpoints
                ],
                "api": [
                    {"url": ep.url, "method": ep.method}
                    for ep in api_endpoints
                ],
            }
            logger.info(
                f"Discovered {len(form_endpoints)} form + "
                f"{len(api_endpoints)} API endpoints"
            )
            return self._endpoints

        logger.warning("No usable endpoints discovered")
        return None


async def run_scraper() -> list[dict]:
    """Run the full scraping pipeline."""
    config = load_config()

    # First try API discovery
    logger.info("Attempting API endpoint discovery...")
    api_client = VaahanAPIClient(config)
    try:
        endpoints = await api_client.discover_endpoints()
    except Exception as e:
        logger.warning(f"API endpoint discovery failed: {e}")
        endpoints = None

    if endpoints:
        logger.info("API endpoints found — saving for future direct access")
        ep_path = BASE_DIR / config["paths"]["raw_data"] / "discovered_endpoints.json"
        ep_path.parent.mkdir(parents=True, exist_ok=True)
        ep_path.write_text(json.dumps(endpoints, indent=2))
    else:
        logger.info("No API endpoints found, using full browser scraping")

    # Fall back to browser scraping
    scraper = VaahanScraper(config)
    data = await scraper.scrape_all()

    # Also save combined raw output as CSV
    if data:
        csv_path = BASE_DIR / config["paths"]["raw_data"] / "all_raw_data.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        logger.info(f"Saved combined raw CSV: {csv_path}")

    return data


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    data = asyncio.run(run_scraper())
    print(f"\nScraping complete. {len(data)} total records collected.")


if __name__ == "__main__":
    main()
