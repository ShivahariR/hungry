#!/usr/bin/env python3
"""
Global Handover Dashboard
=========================
Senior Macro Strategist tool for an Indian Long/Short Equity Fund.

Synthesizes global and domestic data into a pre-market briefing
designed for a 9:00 AM IST session, filtered through the lens
of Indian Cyclicals (Autos, Cap-Goods, Defense, Metals, Energy,
Chemicals, Logistics).

Usage:
    python dashboard.py              # Full terminal dashboard
    python dashboard.py --markdown   # Export as markdown file
    python dashboard.py --section us # Run a single section
    python dashboard.py --no-fetch   # Use cached/sample data
"""

import argparse
import sys
import os
import json
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Global Handover Dashboard - Indian Cyclicals-First Pre-Market Briefing"
    )
    parser.add_argument(
        "--markdown", "-m",
        action="store_true",
        help="Export dashboard as a markdown file",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output file path for markdown export (default: dashboard_YYYYMMDD.md)",
    )
    parser.add_argument(
        "--section", "-s",
        choices=["us", "adr", "commodities", "commentary", "fo", "all"],
        default="all",
        help="Run a specific section only",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Skip live data fetch; use sample/cached data",
    )
    parser.add_argument(
        "--cache-dir",
        default=".cache",
        help="Directory for caching fetched data",
    )
    return parser.parse_args()


def load_sample_data():
    """Return sample data for testing without network access."""
    return {
        "us": {
            "indices": {
                "S&P 500": {"close": 5321.41, "change_pct": 0.74, "significant": True},
                "Nasdaq": {"close": 16742.39, "change_pct": 1.12, "significant": True},
                "Dow Jones": {"close": 39512.84, "change_pct": 0.32, "significant": False},
                "Russell 2000": {"close": 2085.34, "change_pct": -0.21, "significant": False},
            },
            "factors": {
                "Value (IWD)": {"ticker": "IWD", "close": 178.23, "change_pct": 0.85},
                "Growth (IWF)": {"ticker": "IWF", "close": 342.11, "change_pct": 0.42},
                "Industrials (XLI)": {"ticker": "XLI", "close": 125.67, "change_pct": 1.23},
                "Defense (ITA)": {"ticker": "ITA", "close": 142.89, "change_pct": 0.92},
                "Semis (SOXX)": {"ticker": "SOXX", "close": 231.45, "change_pct": 1.56},
                "Materials (XLB)": {"ticker": "XLB", "close": 89.34, "change_pct": 0.67},
                "Energy (XLE)": {"ticker": "XLE", "close": 92.18, "change_pct": -0.34},
            },
            "rotation": {"rotation": "Value leading (Cyclical-friendly)", "spread": 0.43},
            "readthroughs": [
                {
                    "us_etf": "Industrials (XLI)", "us_change_pct": 1.23,
                    "direction": "Bullish", "indian_sector": "Capital Goods / Industrials",
                    "indian_proxies": ["L&T", "Siemens", "ABB India"],
                    "logic": "US industrial strength signals global capex cycle",
                },
                {
                    "us_etf": "Defense (ITA)", "us_change_pct": 0.92,
                    "direction": "Bullish", "indian_sector": "Defense",
                    "indian_proxies": ["HAL", "BEL", "Bharat Dynamics"],
                    "logic": "US defense spending trends benefit allied ecosystem",
                },
                {
                    "us_etf": "Semis (SOXX)", "us_change_pct": 1.56,
                    "direction": "Bullish", "indian_sector": "Electronics / Cap Goods",
                    "indian_proxies": ["Dixon Tech", "Kaynes Tech", "Tata Elxsi"],
                    "logic": "Semi cycle leads electronics capex",
                },
            ],
        },
        "adr": {
            "fx_rate": 83.42,
            "spreads": [
                {"name": "Tata Motors", "adr_ticker": "TTM", "adr_close_usd": 24.56,
                 "adr_inr_equiv": 2049.19, "nse_close_inr": 2023.45, "spread_pct": 1.27,
                 "significant": True, "direction": "Premium"},
                {"name": "Infosys", "adr_ticker": "INFY", "adr_close_usd": 18.92,
                 "adr_inr_equiv": 1578.31, "nse_close_inr": 1562.10, "spread_pct": 1.04,
                 "significant": True, "direction": "Premium"},
                {"name": "HDFC Bank", "adr_ticker": "HDB", "adr_close_usd": 62.34,
                 "adr_inr_equiv": 5200.50, "nse_close_inr": 5215.80, "spread_pct": -0.29,
                 "significant": False, "direction": "Discount"},
            ],
        },
        "commodities": {
            "prices": {
                "Brent Crude": {"ticker": "BZ=F", "price": 82.45, "change_pct": 1.82, "significant": True},
                "Gold": {"ticker": "GC=F", "price": 2378.90, "change_pct": 0.34, "significant": False},
                "Copper": {"ticker": "HG=F", "price": 4.52, "change_pct": 2.14, "significant": True},
                "Aluminium": {"ticker": "ALI=F", "price": 2567.00, "change_pct": 1.23, "significant": False},
                "Natural Gas": {"ticker": "NG=F", "price": 2.89, "change_pct": -3.21, "significant": True},
            },
            "india_implications": [
                {"commodity": "Brent Crude", "move": "+1.82%", "impact": "Negative",
                 "sectors": "Paints, Aviation, Tyres", "detail": "Input cost pressure", "significant": True},
                {"commodity": "Brent Crude", "move": "+1.82%", "impact": "Positive",
                 "sectors": "ONGC, Oil India, Reliance", "detail": "Higher realizations", "significant": True},
                {"commodity": "Copper", "move": "+2.14%", "impact": "Positive",
                 "sectors": "Hindalco (Novelis), Hindustan Copper", "detail": "Better realizations", "significant": True},
            ],
        },
        "commentary": {
            "earnings_scan": {
                "scanned_companies": ["Caterpillar", "Cummins", "Lockheed Martin"],
                "keyword_list": ["India", "Asia demand", "capex cycle"],
                "findings": [
                    {"company": "Caterpillar", "relevance": "Capital Goods",
                     "indian_read": "L&T, Cummins India", "source": "Q4 Earnings Call"},
                ],
                "note": "Wire to transcript API for live data",
            },
            "news_scan": {"queries": [], "results": []},
        },
        "fo": {
            "stocks": [
                {"stock": "TATAMOTORS", "sector": "Autos", "close": 2023.45,
                 "change_pct": 3.21, "vol_ratio": 2.34, "impact": "HIGH",
                 "opening_prediction": "Gap-Up likely"},
                {"stock": "HAL", "sector": "Defense", "close": 4521.30,
                 "change_pct": 2.15, "vol_ratio": 1.87, "impact": "HIGH",
                 "opening_prediction": "Gap-Up likely"},
                {"stock": "TATASTEEL", "sector": "Metals", "close": 167.85,
                 "change_pct": 1.45, "vol_ratio": 1.62, "impact": "MEDIUM",
                 "opening_prediction": "Mild positive"},
                {"stock": "LT", "sector": "Capital Goods", "close": 3456.20,
                 "change_pct": -1.89, "vol_ratio": 1.78, "impact": "MEDIUM",
                 "opening_prediction": "Gap-Down likely"},
            ],
            "summary": {
                "total_scanned": 45, "high_impact_count": 2,
                "gap_up_count": 2, "gap_down_count": 1,
            },
        },
    }


def save_cache(data, cache_dir):
    """Save fetched data to cache."""
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"dashboard_{datetime.now().strftime('%Y%m%d')}.json")

    # Convert to serializable format
    serializable = {}
    for key, val in data.items():
        serializable[key] = val

    try:
        with open(cache_file, "w") as f:
            json.dump(serializable, f, indent=2, default=str)
    except Exception:
        pass


def main():
    args = parse_args()

    from modules.renderer import (
        render_header, render_us_markets, render_adr_spreads,
        render_commodities, render_commentary, render_fo_scan,
        render_risk_map, render_footer, generate_markdown_report,
        console,
    )

    render_header()

    if args.no_fetch:
        console.print("\n  [dim italic]Using sample data (--no-fetch mode)[/dim italic]\n")
        data = load_sample_data()
        us_data = data["us"]
        adr_data = data["adr"]
        commodity_data = data["commodities"]
        commentary_data = data["commentary"]
        fo_data = data["fo"]
    else:
        from modules.us_markets import get_us_markets_section
        from modules.adr_spreads import get_adr_section
        from modules.commodities import get_commodities_section
        from modules.corporate_commentary import get_commentary_section
        from modules.nse_fo import get_fo_section

        sections_to_run = args.section

        console.print("\n  [dim]Fetching live market data...[/dim]\n")

        if sections_to_run in ("all", "us"):
            console.print("  [dim]>> US Markets & Factor ETFs...[/dim]")
            us_data = get_us_markets_section()
        else:
            us_data = load_sample_data()["us"]

        if sections_to_run in ("all", "adr"):
            console.print("  [dim]>> ADR Spreads & FX...[/dim]")
            adr_data = get_adr_section()
        else:
            adr_data = load_sample_data()["adr"]

        if sections_to_run in ("all", "commodities"):
            console.print("  [dim]>> Commodities...[/dim]")
            commodity_data = get_commodities_section()
        else:
            commodity_data = load_sample_data()["commodities"]

        if sections_to_run in ("all", "commentary"):
            console.print("  [dim]>> Corporate Commentary...[/dim]")
            commentary_data = get_commentary_section()
        else:
            commentary_data = load_sample_data()["commentary"]

        if sections_to_run in ("all", "fo"):
            console.print("  [dim]>> NSE F&O Scan...[/dim]")
            fo_data = get_fo_section()
        else:
            fo_data = load_sample_data()["fo"]

        console.print()

    # Render all sections
    render_us_markets(us_data)
    render_adr_spreads(adr_data)
    render_commodities(commodity_data)
    render_commentary(commentary_data)
    render_fo_scan(fo_data)
    render_risk_map(us_data, commodity_data, fo_data)
    render_footer()

    # Cache data
    all_data = {
        "us": us_data, "adr": adr_data, "commodities": commodity_data,
        "commentary": commentary_data, "fo": fo_data,
    }
    save_cache(all_data, args.cache_dir)

    # Markdown export
    if args.markdown:
        md_content = generate_markdown_report(
            us_data, adr_data, commodity_data, commentary_data, fo_data,
        )
        output_path = args.output or f"dashboard_{datetime.now().strftime('%Y%m%d')}.md"
        with open(output_path, "w") as f:
            f.write(md_content)
        console.print(f"\n  [green]Markdown exported to: {output_path}[/green]\n")


if __name__ == "__main__":
    main()
