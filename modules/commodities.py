"""
Commodities Module
- Fetches commodity prices (LME proxies, Brent, metals)
- Maps moves to specific Indian sector margin impacts
"""

import yfinance as yf
from config import (
    COMMODITY_TICKERS, COMMODITY_INDIA_IMPACT,
    COMMODITY_MOVE_SIGNIFICANT_PCT,
)


def fetch_commodity_data():
    """Fetch latest commodity prices and daily changes."""
    results = {}
    tickers = list(COMMODITY_TICKERS.values())
    data = yf.download(tickers, period="5d", group_by="ticker", progress=False)

    for name, ticker in COMMODITY_TICKERS.items():
        try:
            if len(tickers) == 1:
                closes = data["Close"].dropna()
            else:
                closes = data[ticker]["Close"].dropna()
            if len(closes) >= 2:
                prev, last = closes.iloc[-2], closes.iloc[-1]
                chg = ((last - prev) / prev) * 100
                results[name] = {
                    "ticker": ticker,
                    "price": round(float(last), 2),
                    "change_pct": round(float(chg), 2),
                    "significant": abs(float(chg)) >= COMMODITY_MOVE_SIGNIFICANT_PCT,
                }
        except Exception:
            results[name] = {
                "ticker": ticker, "price": None,
                "change_pct": None, "significant": False,
            }
    return results


def map_commodity_to_india(commodity_data):
    """
    For each commodity with a significant move, look up the
    India impact map and return actionable implications.
    """
    implications = []
    for name, data in commodity_data.items():
        ticker = data.get("ticker")
        chg = data.get("change_pct")
        if ticker not in COMMODITY_INDIA_IMPACT or chg is None:
            continue

        direction = "up" if chg > 0 else "down"
        for mapping in COMMODITY_INDIA_IMPACT[ticker]:
            if mapping["direction"] == direction:
                implications.append({
                    "commodity": name,
                    "move": f"{'+' if chg > 0 else ''}{chg}%",
                    "impact": mapping["impact"],
                    "sectors": mapping["sectors"],
                    "detail": mapping["detail"],
                    "significant": data["significant"],
                })
    return implications


def get_commodities_section():
    """Main entry: returns commodity data and India implications."""
    commodity_data = fetch_commodity_data()
    implications = map_commodity_to_india(commodity_data)
    return {
        "prices": commodity_data,
        "india_implications": implications,
    }
