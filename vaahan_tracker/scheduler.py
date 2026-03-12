"""
Vaahan Tracker Scheduler

Cron-ready monthly job that:
1. Scrapes new month's data (+ re-scrapes last 2 months for revisions)
2. Runs the cleaning pipeline
3. Generates updated analytics JSON
4. Sends optional Slack/Telegram notifications

Designed to run on the 16th of each month (Vaahan data typically
updates by 10th-15th of the following month).

Crontab entry:
    0 6 16 * * cd /path/to/vaahan_tracker && python scheduler.py

Or use systemd timer, AWS Lambda, GCP Cloud Functions, etc.
"""

import asyncio
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

import yaml

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent


def load_config() -> dict:
    with open(BASE_DIR / "config.yaml") as f:
        return yaml.safe_load(f)


def get_scrape_range(config: dict) -> tuple[str, str]:
    """
    Determine the month range to scrape.

    On a scheduled run, scrape only the most recent month plus
    the last N months for revisions.
    """
    rescrape_n = config["scraper"].get("rescrape_last_months", 2)
    today = date.today()

    # Data for month M is available around the 10th-15th of month M+1
    # So if we're running on the 16th, the latest available data is for last month
    if today.month == 1:
        latest_year, latest_month = today.year - 1, 12
    else:
        latest_year, latest_month = today.year, today.month - 1

    # Go back rescrape_n months
    start_year, start_month = latest_year, latest_month
    for _ in range(rescrape_n):
        if start_month == 1:
            start_year -= 1
            start_month = 12
        else:
            start_month -= 1

    start = f"{start_year:04d}-{start_month:02d}"
    end = f"{latest_year:04d}-{latest_month:02d}"
    return start, end


def send_slack_notification(webhook_url: str, message: str) -> bool:
    """Send notification via Slack webhook."""
    if not webhook_url:
        return False

    payload = json.dumps({"text": message}).encode("utf-8")
    req = Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except URLError as e:
        logger.error(f"Slack notification failed: {e}")
        return False


def send_telegram_notification(bot_token: str, chat_id: str, message: str) -> bool:
    """Send notification via Telegram bot."""
    if not bot_token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }).encode("utf-8")
    req = Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except URLError as e:
        logger.error(f"Telegram notification failed: {e}")
        return False


def notify(config: dict, message: str) -> None:
    """Send notifications via configured channels."""
    notif = config.get("notifications", {})

    if notif.get("slack_webhook_url"):
        send_slack_notification(notif["slack_webhook_url"], message)

    if notif.get("telegram_bot_token") and notif.get("telegram_chat_id"):
        send_telegram_notification(
            notif["telegram_bot_token"],
            notif["telegram_chat_id"],
            message,
        )


async def run_scheduled() -> None:
    """Run the full scheduled pipeline."""
    config = load_config()

    start_range, end_range = get_scrape_range(config)
    logger.info(f"Scheduled run: scraping {start_range} to {end_range}")

    # Override config with computed range
    config["scraper"]["start_month"] = start_range
    config["scraper"]["end_month"] = end_range

    # 1. Scrape
    from scraper.vaahan_scraper import VaahanScraper
    scraper = VaahanScraper(config)
    raw_data = await scraper.scrape_all()

    if not raw_data:
        msg = f"Vaahan Tracker: No data scraped for {start_range} to {end_range}"
        logger.warning(msg)
        notify(config, msg)
        return

    # 2. Run pipeline
    from pipeline.vaahan_pipeline import process_raw_data, save_clean_data
    df_new = process_raw_data(raw_data)

    # Merge with existing historical data
    clean_dir = BASE_DIR / "data" / "clean"
    existing_parquet = clean_dir / "vaahan_clean.parquet"

    if existing_parquet.exists():
        import pandas as pd
        df_existing = pd.read_parquet(existing_parquet)
        # Remove months we're re-scraping from existing data
        months_to_replace = df_new["month"].unique()
        df_existing = df_existing[~df_existing["month"].isin(months_to_replace)]
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.sort_values(
            ["month", "category", "oem_normalized"]
        ).reset_index(drop=True)
    else:
        df_combined = df_new

    save_clean_data(df_combined, clean_dir)

    # 3. Generate analytics JSON
    from analytics.export_dashboard import export_dashboard_json
    dashboard = export_dashboard_json(df_combined)

    # 4. Notify
    n_months = df_new["month"].nunique()
    n_oems = df_new["oem_normalized"].nunique()
    total_units = df_new["units"].sum()
    msg = (
        f"*Vaahan Tracker Update*\n"
        f"Scraped {n_months} month(s): {start_range} to {end_range}\n"
        f"OEMs: {n_oems} | Total registrations: {total_units:,}\n"
        f"Dashboard JSON updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    logger.info(msg)
    notify(config, msg)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if "--full" in sys.argv:
        # Full historical scrape (uses config.yaml start/end dates)
        logger.info("Running full historical scrape...")
        from scraper.vaahan_scraper import main as scraper_main
        scraper_main()
        from pipeline.vaahan_pipeline import main as pipeline_main
        pipeline_main()
        from analytics.export_dashboard import main as export_main
        export_main()
    else:
        # Scheduled incremental run
        asyncio.run(run_scheduled())


if __name__ == "__main__":
    main()
