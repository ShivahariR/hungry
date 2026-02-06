"""
NSE F&O Module
- Categorizes F&O news by impact (High/Medium/Low)
- Predicts opening behavior for cyclical stocks
- Focuses on non-BFSI space
"""

import yfinance as yf
from config import NSE_FO_CYCLICALS


def fetch_fo_stock_data():
    """
    Fetch recent price data for NSE F&O cyclical stocks.
    Uses previous session data to analyze momentum and gaps.
    """
    results = {}
    for sector, stocks in NSE_FO_CYCLICALS.items():
        sector_data = []
        for stock in stocks:
            ticker = f"{stock}.NS"
            try:
                data = yf.download(ticker, period="5d", progress=False)
                if len(data) >= 2:
                    last_close = float(data["Close"].iloc[-1])
                    prev_close = float(data["Close"].iloc[-2])
                    day_chg = ((last_close - prev_close) / prev_close) * 100

                    high = float(data["High"].iloc[-1])
                    low = float(data["Low"].iloc[-1])
                    vol = int(data["Volume"].iloc[-1])
                    avg_vol = int(data["Volume"].mean())

                    vol_ratio = vol / avg_vol if avg_vol > 0 else 1.0

                    sector_data.append({
                        "stock": stock,
                        "sector": sector,
                        "close": round(last_close, 2),
                        "change_pct": round(day_chg, 2),
                        "high": round(high, 2),
                        "low": round(low, 2),
                        "volume": vol,
                        "vol_ratio": round(vol_ratio, 2),
                    })
            except Exception:
                continue
        results[sector] = sector_data
    return results


def categorize_impact(stock_data):
    """
    Categorize each stock's news/move impact:
    - High: >3% move OR volume ratio >2x
    - Medium: 1-3% move OR volume ratio 1.5-2x
    - Low: <1% move and normal volume
    """
    categorized = []
    for sector, stocks in stock_data.items():
        for s in stocks:
            chg = abs(s["change_pct"])
            vol_r = s["vol_ratio"]

            if chg > 3.0 or vol_r > 2.0:
                impact = "HIGH"
            elif chg > 1.0 or vol_r > 1.5:
                impact = "MEDIUM"
            else:
                impact = "LOW"

            # Predict opening behavior
            if s["change_pct"] > 1.5:
                opening = "Gap-Up likely"
            elif s["change_pct"] < -1.5:
                opening = "Gap-Down likely"
            elif s["change_pct"] > 0.5:
                opening = "Mild positive"
            elif s["change_pct"] < -0.5:
                opening = "Mild negative"
            else:
                opening = "Flat open"

            categorized.append({
                "stock": s["stock"],
                "sector": s["sector"],
                "close": s["close"],
                "change_pct": s["change_pct"],
                "vol_ratio": s["vol_ratio"],
                "impact": impact,
                "opening_prediction": opening,
            })

    # Sort: HIGH impact first, then by absolute change
    priority = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    categorized.sort(key=lambda x: (priority.get(x["impact"], 3), -abs(x["change_pct"])))

    return categorized


def get_fo_section():
    """Main entry: returns F&O analysis for cyclical stocks."""
    stock_data = fetch_fo_stock_data()
    categorized = categorize_impact(stock_data)

    # Summary stats
    high_impact = [s for s in categorized if s["impact"] == "HIGH"]
    gap_ups = [s for s in categorized if "Gap-Up" in s["opening_prediction"]]
    gap_downs = [s for s in categorized if "Gap-Down" in s["opening_prediction"]]

    return {
        "stocks": categorized,
        "summary": {
            "total_scanned": len(categorized),
            "high_impact_count": len(high_impact),
            "gap_up_count": len(gap_ups),
            "gap_down_count": len(gap_downs),
        },
    }
