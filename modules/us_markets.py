"""
US Markets Module
- Fetches US index and factor ETF data
- Detects Value vs Growth rotation
- Maps US sector moves to Indian cyclical read-throughs
"""

import yfinance as yf
from datetime import datetime, timedelta
from config import (
    US_INDICES, US_FACTOR_ETFS, US_TO_INDIA_READTHROUGH,
    INDEX_MOVE_SIGNIFICANT_PCT,
)


def fetch_us_index_data():
    """Fetch latest US index closes and daily change."""
    results = {}
    tickers = list(US_INDICES.values())
    data = yf.download(tickers, period="2d", group_by="ticker", progress=False)

    for name, ticker in US_INDICES.items():
        try:
            if len(tickers) == 1:
                closes = data["Close"]
            else:
                closes = data[ticker]["Close"]
            if len(closes) >= 2:
                prev, last = closes.iloc[-2], closes.iloc[-1]
                chg = ((last - prev) / prev) * 100
                results[name] = {
                    "close": round(last, 2),
                    "change_pct": round(chg, 2),
                    "significant": abs(chg) >= INDEX_MOVE_SIGNIFICANT_PCT,
                }
        except Exception:
            results[name] = {"close": None, "change_pct": None, "significant": False}
    return results


def fetch_factor_data():
    """Fetch factor/style ETF data to detect rotation."""
    results = {}
    tickers = list(US_FACTOR_ETFS.values())
    data = yf.download(tickers, period="2d", group_by="ticker", progress=False)

    for name, ticker in US_FACTOR_ETFS.items():
        try:
            if len(tickers) == 1:
                closes = data["Close"]
            else:
                closes = data[ticker]["Close"]
            if len(closes) >= 2:
                prev, last = closes.iloc[-2], closes.iloc[-1]
                chg = ((last - prev) / prev) * 100
                results[name] = {
                    "ticker": ticker,
                    "close": round(last, 2),
                    "change_pct": round(chg, 2),
                }
        except Exception:
            results[name] = {"ticker": ticker, "close": None, "change_pct": None}
    return results


def detect_factor_rotation(factor_data):
    """Determine if Value or Growth is leading."""
    value = factor_data.get("Value (IWD)", {}).get("change_pct")
    growth = factor_data.get("Growth (IWF)", {}).get("change_pct")

    if value is None or growth is None:
        return {"rotation": "Data unavailable", "spread": None}

    spread = round(value - growth, 2)
    if spread > 0.3:
        rotation = "Value leading (Cyclical-friendly)"
    elif spread < -0.3:
        rotation = "Growth leading (Defensive tilt)"
    else:
        rotation = "Neutral / No clear rotation"

    return {"rotation": rotation, "spread": spread}


def generate_readthroughs(factor_data):
    """Map US sector ETF moves to Indian cyclical implications."""
    readthroughs = []
    for etf_ticker, mapping in US_TO_INDIA_READTHROUGH.items():
        # Find the factor entry for this ticker
        etf_entry = None
        for name, data in factor_data.items():
            if data.get("ticker") == etf_ticker:
                etf_entry = data
                etf_name = name
                break

        if etf_entry and etf_entry.get("change_pct") is not None:
            chg = etf_entry["change_pct"]
            direction = "Bullish" if chg > 0 else "Bearish" if chg < 0 else "Flat"
            readthroughs.append({
                "us_etf": etf_name,
                "us_change_pct": chg,
                "direction": direction,
                "indian_sector": mapping["sector"],
                "indian_proxies": mapping["indian_proxies"],
                "logic": mapping["logic"],
            })
    return readthroughs


def get_us_markets_section():
    """Main entry: returns full US markets analysis dict."""
    index_data = fetch_us_index_data()
    factor_data = fetch_factor_data()
    rotation = detect_factor_rotation(factor_data)
    readthroughs = generate_readthroughs(factor_data)

    return {
        "indices": index_data,
        "factors": factor_data,
        "rotation": rotation,
        "readthroughs": readthroughs,
    }
