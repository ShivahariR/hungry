"""
ADR Spread Calculator
- Fetches US ADR closing prices and previous NSE cash close
- Calculates implied opening gap adjusted for FX
"""

import yfinance as yf
from config import ADR_MAPPINGS, FX_TICKER, ADR_SPREAD_SIGNIFICANT_PCT


def fetch_fx_rate():
    """Fetch latest USDINR rate."""
    try:
        data = yf.download(FX_TICKER, period="2d", progress=False)
        if len(data) >= 1:
            return round(data["Close"].iloc[-1], 2)
    except Exception:
        pass
    return None


def calculate_adr_spreads():
    """
    For each ADR:
    - Get US ADR close (in USD)
    - Get previous NSE close (in INR)
    - Convert ADR price to INR equivalent
    - Calculate spread %
    """
    fx_rate = fetch_fx_rate()
    if fx_rate is None:
        return {"fx_rate": None, "spreads": [], "error": "Could not fetch USDINR rate"}

    spreads = []
    for adr_ticker, info in ADR_MAPPINGS.items():
        try:
            # Fetch ADR data (US close)
            adr_data = yf.download(adr_ticker, period="2d", progress=False)
            # Fetch NSE data (previous cash close)
            nse_data = yf.download(info["nse"], period="5d", progress=False)

            if len(adr_data) < 1 or len(nse_data) < 1:
                continue

            adr_close_usd = adr_data["Close"].iloc[-1]
            nse_close_inr = nse_data["Close"].iloc[-1]

            # Convert ADR to INR equivalent
            # Most Indian ADRs represent 1 share, but some have ratios
            adr_inr_equivalent = adr_close_usd * fx_rate

            # Calculate spread
            spread_pct = ((adr_inr_equivalent - nse_close_inr) / nse_close_inr) * 100

            spreads.append({
                "name": info["name"],
                "adr_ticker": adr_ticker,
                "adr_close_usd": round(float(adr_close_usd), 2),
                "adr_inr_equiv": round(float(adr_inr_equivalent), 2),
                "nse_close_inr": round(float(nse_close_inr), 2),
                "spread_pct": round(float(spread_pct), 2),
                "significant": abs(float(spread_pct)) >= ADR_SPREAD_SIGNIFICANT_PCT,
                "direction": "Premium" if spread_pct > 0 else "Discount",
            })
        except Exception:
            continue

    # Sort by absolute spread descending
    spreads.sort(key=lambda x: abs(x["spread_pct"]), reverse=True)

    return {"fx_rate": fx_rate, "spreads": spreads}


def get_adr_section():
    """Main entry: returns ADR spread analysis."""
    return calculate_adr_spreads()
