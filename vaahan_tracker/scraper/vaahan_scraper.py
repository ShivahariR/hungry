"""
Vaahan Dashboard Scraper

Scrapes maker-wise vehicle registration data from the Vaahan dashboard
(https://vahan.parivahan.gov.in/vahan4dashboard/) by triggering the
built-in PrimeFaces Excel export.

Approach:
  1. Navigate to the reportview page
  2. Select Year, X-Axis (e.g. "Month Wise"), Y-Axis ("Maker"), and State
  3. Click Refresh to load the data table
  4. Click the Excel download link (groupingTable:xls)
  5. Parse the downloaded XLSX into structured records
"""

import asyncio
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None
    logging.getLogger(__name__).warning(
        "Playwright not installed. Run: pip install playwright && playwright install chromium"
    )

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def select_primefaces_dropdown(page, label_selector: str, item_text: str) -> bool:
    """Select an item from a PrimeFaces dropdown by clicking the label and choosing an item."""
    logger.debug(f"Setting dropdown {label_selector} to '{item_text}'")
    try:
        page.wait_for_selector(label_selector, timeout=5000)
        page.click(label_selector)
        time.sleep(0.5)

        item_locator = page.locator("li").filter(has_text=item_text)
        if item_locator.count() > 0:
            item_locator.first.click()
            time.sleep(0.5)
            return True
        else:
            logger.warning(f"Item '{item_text}' not found in dropdown {label_selector}")
            return False
    except Exception as e:
        logger.warning(f"Exception selecting dropdown {label_selector}: {e}")
        return False


def get_all_states(page) -> list[str]:
    """Get all state names from the State dropdown."""
    logger.info("Fetching list of all states...")
    label_selectors = [
        "label#j_idt45_label",
        "label#j_idt41_label",
        "label:has-text('Select State')",
    ]
    label_selector = None
    for selector in label_selectors:
        if page.locator(selector).count() > 0:
            label_selector = selector
            break

    if not label_selector:
        logger.error("Could not find State dropdown label")
        return []

    page.click(label_selector)
    time.sleep(1)

    states = page.locator("li.ui-selectonemenu-item").all_inner_texts()
    page.click("body")
    time.sleep(0.5)

    states = [s.strip() for s in states if s.strip() and "Select State" not in s]
    logger.info(f"Found {len(states)} states")
    return states


def find_and_click(page, selectors: list[str], description: str) -> bool:
    """Try multiple selectors and click the first one found."""
    for selector in selectors:
        try:
            if page.locator(selector).count() > 0:
                page.click(selector)
                logger.debug(f"Clicked {description}: {selector}")
                return True
        except Exception:
            continue
    logger.warning(f"Could not find {description}")
    return False


def goto_with_retry(page, url: str, timeout: int = 90000, max_retries: int = 3) -> bool:
    """Navigate to URL with retry logic and exponential backoff."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Navigating to {url} (attempt {attempt + 1}/{max_retries})")
            page.goto(url, timeout=timeout)
            page.wait_for_load_state("networkidle", timeout=timeout)
            logger.info("Successfully loaded dashboard")
            return True
        except Exception as e:
            logger.warning(f"Navigation attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 5
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
    return False


def parse_vahan_excel(filepath: str, x_axis: str, y_axis: str, state: str, year: str) -> pd.DataFrame:
    """
    Parse the multi-row-header Excel downloaded from the Vahan dashboard.

    The XLSX has:
    - Row 0: title/empty
    - Rows 1-3: multi-level headers
    - Row 4+: data

    Returns a long-format DataFrame.
    """
    df = pd.read_excel(filepath, header=None)

    if len(df) <= 4:
        logger.warning(f"Excel file too short ({len(df)} rows), no data")
        return pd.DataFrame()

    # Build flattened headers from rows 1-3
    rows = [
        df.iloc[1].fillna("").astype(str).tolist(),
        df.iloc[2].fillna("").astype(str).tolist(),
        df.iloc[3].fillna("").astype(str).tolist(),
    ]
    headers = []
    for i in range(len(rows[0])):
        parts = []
        for r in rows:
            val = r[i].strip()
            if val and not val.startswith("Unnamed") and val not in parts:
                parts.append(val)
        headers.append("_".join(parts) if parts else f"Col_{i}")

    df_cleaned = df.iloc[4:].copy()
    df_cleaned.columns = headers
    df_cleaned = df_cleaned.dropna(how="all", axis=0)

    # Find the TOTAL column to exclude it
    total_patterns = ["TOTAL", "GRAND TOTAL", "TOTAL_TOTAL"]
    total_index = len(headers)
    for i, h in enumerate(headers):
        if i > 1 and any(tp in h.upper() for tp in total_patterns):
            total_index = i
            break

    # Strip axis prefixes from header names
    prefixes_to_strip = [
        f"{x_axis}_", f"{x_axis.upper()}_",
        "Month Wise_", "Vehicle Category Group_",
        "Fuel_", "Maker_", "Norms_", "Vehicle Class_", "Vehicle Category_",
        "FOUR WHEELER_", "TWO WHEELER_", "THREE WHEELER_",
    ]
    cleaned_headers = []
    for h in headers:
        ch = h
        modified = True
        while modified:
            modified = False
            for pref in prefixes_to_strip:
                if ch.startswith(pref):
                    ch = ch.replace(pref, "", 1)
                    modified = True
                    break
        cleaned_headers.append(ch)

    df_cleaned.columns = cleaned_headers
    s_col, y_col = cleaned_headers[0], cleaned_headers[1]
    x_cols = [c for c in cleaned_headers[2:total_index] if not c.startswith("Col_")]

    if not x_cols:
        logger.warning("No data columns found in Excel")
        return pd.DataFrame()

    df_long = df_cleaned.melt(
        id_vars=[s_col, y_col],
        value_vars=x_cols,
        var_name=x_axis,
        value_name="Value",
    )
    df_long["State"] = state
    df_long["Year"] = str(year)
    df_long = df_long[[s_col, y_col, "State", "Year", x_axis, "Value"]]

    return df_long


class VaahanScraper:
    """Scrapes Vahan dashboard by triggering PrimeFaces Excel exports."""

    REPORT_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"

    DOWNLOAD_SELECTORS = [
        "a[id='groupingTable:xls']",
        "a[id='vchgroupTable:xls']",
    ]
    REFRESH_SELECTORS = [
        "button#j_idt75",
        "button#j_idt72",
        "button:has-text('Refresh')",
    ]
    STATE_LABEL_SELECTORS = [
        "label#j_idt45_label",
        "label#j_idt41_label",
        "div:has-text('State:') + div label",
    ]

    def __init__(self, config: dict):
        self.config = config
        self.scraper_cfg = config["scraper"]
        self.raw_data_dir = BASE_DIR / config["paths"]["raw_data"]
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

    def scrape_all(self) -> list[dict]:
        """Main entry: scrape all configured years/states using Excel downloads."""
        if sync_playwright is None:
            raise RuntimeError(
                "Playwright not installed. Run: pip install playwright && playwright install chromium"
            )

        # Determine years to scrape from config month range
        start_year = int(self.scraper_cfg["start_month"].split("-")[0])
        end_year = int(self.scraper_cfg["end_month"].split("-")[0])
        years = list(range(start_year, end_year + 1))

        # X/Y axis configurations for maker-wise data
        scrape_configs = self.config.get("scrape_axes", [
            {"x_axis": "Month Wise", "y_axis": "Maker"},
        ])

        states_filter = self.config.get("states", None)
        all_india_only = self.config.get("all_india_only", True)

        all_data = []

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.scraper_cfg.get("headless", True),
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            # Stealth overrides
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """)

            page = context.new_page()
            goto_with_retry(page, self.REPORT_URL, timeout=self.scraper_cfg["page_load_timeout_ms"])

            # Discover states if needed
            if all_india_only:
                states_to_scrape = None  # Will use whatever default state is set
            elif states_filter:
                states_to_scrape = states_filter
            else:
                states_to_scrape = get_all_states(page)

            for axis_cfg in scrape_configs:
                x_axis = axis_cfg["x_axis"]
                y_axis = axis_cfg["y_axis"]
                logger.info(f"Scraping with X-Axis={x_axis}, Y-Axis={y_axis}")

                # Select axis variables
                select_primefaces_dropdown(page, "label#xaxisVar_label", x_axis)
                select_primefaces_dropdown(page, "label#yaxisVar_label", y_axis)

                for year in years:
                    logger.info(f"Processing year {year}")
                    select_primefaces_dropdown(page, "label#selectedYear_label", str(year))

                    if states_to_scrape:
                        state_list = states_to_scrape
                    else:
                        state_list = [None]  # Use default/All India

                    for state in state_list:
                        state_name = state or "ALL_INDIA"
                        logger.info(f"Processing state: {state_name}, year: {year}")

                        try:
                            if state:
                                find_and_click_state = False
                                for sel in self.STATE_LABEL_SELECTORS:
                                    if page.locator(sel).count() > 0:
                                        select_primefaces_dropdown(page, sel, state)
                                        find_and_click_state = True
                                        break
                                if not find_and_click_state:
                                    logger.warning(f"Could not find State dropdown for {state}")
                                    continue

                            # Click Refresh
                            if not find_and_click(page, self.REFRESH_SELECTORS, "Refresh button"):
                                logger.error("Refresh button not found, skipping")
                                continue

                            page.wait_for_load_state("networkidle")
                            time.sleep(self.scraper_cfg.get("table_load_wait", 5))

                            # Download Excel
                            data = self._download_and_parse_excel(
                                page, x_axis, y_axis, state_name, str(year)
                            )
                            if data is not None and not data.empty:
                                records = self._dataframe_to_records(data, x_axis, y_axis, year)
                                all_data.extend(records)
                                self._save_raw_excel_data(data, x_axis, y_axis, state_name, year)
                                logger.info(f"Collected {len(records)} records for {state_name}/{year}")
                            else:
                                logger.warning(f"No data for {state_name}/{year}")

                        except Exception as e:
                            logger.error(f"Failed to process {state_name}/{year}: {e}")

                        # Delay between requests
                        time.sleep(self.scraper_cfg.get("min_delay_seconds", 2))

            browser.close()

        logger.info(f"Scraping complete. Total records: {len(all_data)}")
        return all_data

    def _download_and_parse_excel(
        self, page, x_axis: str, y_axis: str, state: str, year: str
    ) -> pd.DataFrame | None:
        """Click the Excel download button and parse the result."""
        # Find the download button
        download_selector = None
        for sel in self.DOWNLOAD_SELECTORS:
            if page.locator(sel).count() > 0:
                download_selector = sel
                break

        if not download_selector:
            # Wait a bit more and retry
            logger.debug("Download button not found, waiting...")
            time.sleep(3)
            for sel in self.DOWNLOAD_SELECTORS:
                if page.locator(sel).count() > 0:
                    download_selector = sel
                    break

        if not download_selector:
            logger.warning("Excel download button not found")
            return None

        try:
            with page.expect_download(timeout=30000) as download_info:
                page.click(download_selector)
            download = download_info.value

            temp_path = str(self.raw_data_dir / f"temp_{int(time.time())}.xlsx")
            download.save_as(temp_path)

            df = parse_vahan_excel(temp_path, x_axis, y_axis, state, year)

            # Clean up temp file
            try:
                os.remove(temp_path)
            except OSError:
                pass

            return df

        except Exception as e:
            logger.error(f"Excel download/parse failed: {e}")
            return None

    def _dataframe_to_records(
        self, df: pd.DataFrame, x_axis: str, y_axis: str, year: int
    ) -> list[dict]:
        """
        Convert the long-format DataFrame into our standard record format.

        When x_axis="Month Wise" and y_axis="Maker", rows look like:
          S.No | Maker | State | Year | Month Wise | Value
        """
        records = []
        for _, row in df.iterrows():
            try:
                value = row.get("Value")
                if pd.isna(value) or value == "" or value == 0:
                    continue

                # Try to get numeric value
                units = int(float(str(value).replace(",", "")))
                if units <= 0:
                    continue

                # Determine month from the x_axis column if it's "Month Wise"
                month_val = None
                if x_axis == "Month Wise":
                    month_raw = str(row.get(x_axis, "")).strip()
                    month_val = self._parse_month(month_raw, year)

                # The y_axis column contains maker/category name
                y_col_name = df.columns[1] if len(df.columns) > 1 else y_axis
                oem_or_category = str(row.get(y_col_name, "")).strip()

                if not oem_or_category or oem_or_category.upper() in ("TOTAL", "GRAND TOTAL"):
                    continue

                record = {
                    "month": month_val or f"{year}-01",
                    "oem_raw": oem_or_category.upper() if y_axis == "Maker" else "",
                    "category_raw": oem_or_category if y_axis != "Maker" else "ALL",
                    "units": units,
                    "fuel_type": "ALL",
                    "scraped_at": datetime.now().isoformat(),
                }

                # If x_axis is Fuel, capture fuel type
                if x_axis == "Fuel":
                    record["fuel_type"] = str(row.get(x_axis, "ALL")).strip()

                records.append(record)
            except (ValueError, TypeError) as e:
                logger.debug(f"Skipping row: {e}")
                continue

        return records

    def _parse_month(self, month_str: str, year: int) -> str:
        """Parse month string (e.g. 'January', 'Jan', 'JAN') to YYYY-MM format."""
        month_map = {
            "JAN": "01", "JANUARY": "01",
            "FEB": "02", "FEBRUARY": "02",
            "MAR": "03", "MARCH": "03",
            "APR": "04", "APRIL": "04",
            "MAY": "05",
            "JUN": "06", "JUNE": "06",
            "JUL": "07", "JULY": "07",
            "AUG": "08", "AUGUST": "08",
            "SEP": "09", "SEPTEMBER": "09",
            "OCT": "10", "OCTOBER": "10",
            "NOV": "11", "NOVEMBER": "11",
            "DEC": "12", "DECEMBER": "12",
        }
        month_upper = month_str.upper().strip()
        mm = month_map.get(month_upper)
        if mm:
            return f"{year}-{mm}"
        # Try partial match
        for key, val in month_map.items():
            if key in month_upper or month_upper in key:
                return f"{year}-{val}"
        return f"{year}-01"

    def _save_raw_excel_data(
        self, df: pd.DataFrame, x_axis: str, y_axis: str, state: str, year: int
    ) -> None:
        """Save the parsed Excel data as CSV for audit trail."""
        safe_x = x_axis.replace(" ", "_")
        safe_y = y_axis.replace(" ", "_")
        safe_state = state.replace(" ", "_")
        filename = f"excel_{safe_x}_{safe_y}_{safe_state}_{year}.csv"
        filepath = self.raw_data_dir / filename
        df.to_csv(filepath, index=False)
        logger.debug(f"Saved raw Excel data: {filepath}")


def run_scraper() -> list[dict]:
    """Run the full scraping pipeline."""
    config = load_config()
    scraper = VaahanScraper(config)
    data = scraper.scrape_all()

    # Save combined raw output as CSV
    if data:
        raw_dir = BASE_DIR / config["paths"]["raw_data"]
        csv_path = raw_dir / "all_raw_data.csv"
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
    data = run_scraper()
    print(f"\nScraping complete. {len(data)} total records collected.")


if __name__ == "__main__":
    main()
