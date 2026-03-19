"""
US Recession Probability Simulator — FRED Historical Data Pull
================================================================
Pulls 10 years of historical data for all indicators needed to
calibrate the MiroFish multi-agent recession simulation.
SETUP:
  pip install fredapi pandas
  Get free FRED API key: https://fred.stlouisfed.org/docs/api/api_key.html
  Export: export FRED_API_KEY=your_key_here
USAGE:
  python pull_fred_history.py
  # Outputs: fred_historical_data.parquet, fred_historical_data.csv
"""
import os
import sys
import json
import time
from datetime import datetime, timedelta

try:
    from fredapi import Fred
    import pandas as pd
except ImportError:
    print("Install required packages: pip install fredapi pandas pyarrow")
    sys.exit(1)

FRED_API_KEY = os.environ.get("FRED_API_KEY", "YOUR_KEY_HERE")
START_DATE = "2016-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = "recession_sim_data"

# ── Series definitions: {fred_id: {name, category, frequency, transform}} ──
SERIES = {
    # ═══ OIL / ENERGY ═══
    "DCOILBRENTEU":  {"name": "Brent Crude Spot", "cat": "oil", "freq": "D", "transform": "level"},
    "DCOILWTICO":    {"name": "WTI Crude Spot", "cat": "oil", "freq": "D", "transform": "level"},
    "GASREGW":       {"name": "US Regular Gasoline Retail Weekly", "cat": "oil", "freq": "W", "transform": "level"},
    "MCRFPUS2":      {"name": "US Crude Oil Production", "cat": "oil", "freq": "M", "transform": "level"},
    "DPCERG3M086SBEA": {"name": "Energy Goods+Services PCE Share", "cat": "oil", "freq": "M", "transform": "level"},

    # ═══ LABOR MARKET ═══
    "ICSA":          {"name": "Initial Claims SA", "cat": "labor", "freq": "W", "transform": "level"},
    "CCSA":          {"name": "Continuing Claims SA", "cat": "labor", "freq": "W", "transform": "level"},
    "PAYEMS":        {"name": "Total Nonfarm Payrolls", "cat": "labor", "freq": "M", "transform": "diff"},
    "UNRATE":        {"name": "Unemployment Rate", "cat": "labor", "freq": "M", "transform": "level"},
    "CES0500000003": {"name": "Avg Hourly Earnings Private", "cat": "labor", "freq": "M", "transform": "yoy_pct"},
    "JTSJOL":        {"name": "JOLTS Job Openings", "cat": "labor", "freq": "M", "transform": "level"},
    "JTSQUR":        {"name": "JOLTS Quits Rate", "cat": "labor", "freq": "M", "transform": "level"},
    "AWHMAN":        {"name": "Avg Weekly Hours Manufacturing", "cat": "labor", "freq": "M", "transform": "level"},
    "SAHMREALTIME":  {"name": "Sahm Rule Recession Indicator", "cat": "labor", "freq": "M", "transform": "level"},
    "LNS13025703":   {"name": "Long-Term Unemployed (27+ weeks)", "cat": "labor", "freq": "M", "transform": "level"},
    "UEMPMEAN":      {"name": "Mean Duration of Unemployment", "cat": "labor", "freq": "M", "transform": "level"},

    # ═══ CREDIT / FINANCIAL CONDITIONS ═══
    "BAMLH0A0HYM2":  {"name": "ICE BofA HY OAS", "cat": "credit", "freq": "D", "transform": "level"},
    "BAMLH0A1HYBB":  {"name": "ICE BofA BB OAS", "cat": "credit", "freq": "D", "transform": "level"},
    "BAMLH0A2HYB":   {"name": "ICE BofA B OAS", "cat": "credit", "freq": "D", "transform": "level"},
    "BAMLH0A3HYC":   {"name": "ICE BofA CCC OAS", "cat": "credit", "freq": "D", "transform": "level"},
    "BAMLC0A0CM":    {"name": "ICE BofA IG OAS", "cat": "credit", "freq": "D", "transform": "level"},
    "DGS10":         {"name": "10Y Treasury Yield", "cat": "credit", "freq": "D", "transform": "level"},
    "DGS2":          {"name": "2Y Treasury Yield", "cat": "credit", "freq": "D", "transform": "level"},
    "DGS3MO":        {"name": "3M Treasury Yield", "cat": "credit", "freq": "D", "transform": "level"},
    "T10Y2Y":        {"name": "10Y-2Y Spread", "cat": "credit", "freq": "D", "transform": "level"},
    "T10Y3M":        {"name": "10Y-3M Spread", "cat": "credit", "freq": "D", "transform": "level"},
    "DFF":           {"name": "Effective Fed Funds Rate", "cat": "credit", "freq": "D", "transform": "level"},
    "NFCI":          {"name": "Chicago Fed NFCI", "cat": "credit", "freq": "W", "transform": "level"},
    "ANFCI":         {"name": "Chicago Fed Adjusted NFCI", "cat": "credit", "freq": "W", "transform": "level"},
    "STLFSI2":       {"name": "St. Louis Fed Financial Stress", "cat": "credit", "freq": "W", "transform": "level"},
    "VIXCLS":        {"name": "VIX Close", "cat": "credit", "freq": "D", "transform": "level"},

    # ═══ FED POLICY ═══
    "DFEDTARU":      {"name": "Fed Funds Target Upper", "cat": "fed", "freq": "D", "transform": "level"},
    "DFEDTARL":      {"name": "Fed Funds Target Lower", "cat": "fed", "freq": "D", "transform": "level"},
    "WALCL":         {"name": "Fed Balance Sheet Total Assets", "cat": "fed", "freq": "W", "transform": "level"},
    "PCEPILFE":      {"name": "Core PCE Price Index", "cat": "fed", "freq": "M", "transform": "yoy_pct"},

    # ═══ DEMAND / ACTIVITY ═══
    "RSXFS":         {"name": "Real Retail Sales ex Food Services", "cat": "demand", "freq": "M", "transform": "mom_pct"},
    "INDPRO":        {"name": "Industrial Production Index", "cat": "demand", "freq": "M", "transform": "mom_pct"},
    "TOTALSA":       {"name": "Total Vehicle Sales SAAR", "cat": "demand", "freq": "M", "transform": "level"},
    "HOUST":         {"name": "Housing Starts SAAR", "cat": "demand", "freq": "M", "transform": "level"},
    "PERMIT":        {"name": "Building Permits SAAR", "cat": "demand", "freq": "M", "transform": "level"},
    "W875RX1":       {"name": "Real Personal Income ex Transfers", "cat": "demand", "freq": "M", "transform": "mom_pct"},
    "USSLIND":       {"name": "Conference Board LEI", "cat": "demand", "freq": "M", "transform": "mom_pct"},
    "PCEC96":        {"name": "Real PCE", "cat": "demand", "freq": "M", "transform": "mom_pct"},
    "PSAVERT":       {"name": "Personal Savings Rate", "cat": "demand", "freq": "M", "transform": "level"},

    # ═══ INFLATION ═══
    "CPIAUCSL":      {"name": "CPI All Urban Consumers SA", "cat": "inflation", "freq": "M", "transform": "yoy_pct"},
    "CPILFESL":      {"name": "Core CPI SA", "cat": "inflation", "freq": "M", "transform": "yoy_pct"},
    "PCEPI":         {"name": "PCE Price Index", "cat": "inflation", "freq": "M", "transform": "yoy_pct"},
    "PPIFIS":        {"name": "PPI Final Demand", "cat": "inflation", "freq": "M", "transform": "yoy_pct"},
    "MICH":          {"name": "Michigan 1Y Inflation Expectations", "cat": "inflation", "freq": "M", "transform": "level"},
    "EXPINF5YR":     {"name": "Michigan 5Y Inflation Expectations", "cat": "inflation", "freq": "M", "transform": "level"},
    "T5YIE":         {"name": "5Y Breakeven Inflation", "cat": "inflation", "freq": "D", "transform": "level"},
    "T10YIE":        {"name": "10Y Breakeven Inflation", "cat": "inflation", "freq": "D", "transform": "level"},

    # ═══ SENTIMENT ═══
    "UMCSENT":       {"name": "Michigan Consumer Sentiment", "cat": "sentiment", "freq": "M", "transform": "level"},
}


def pull_all_series(api_key: str) -> dict[str, pd.Series]:
    fred = Fred(api_key=api_key)
    results = {}
    total = len(SERIES)
    for i, (sid, meta) in enumerate(SERIES.items(), 1):
        print(f"  [{i:02d}/{total}] {sid:20s} — {meta['name']}...", end=" ", flush=True)
        try:
            s = fred.get_series(sid, observation_start=START_DATE, observation_end=END_DATE)
            s.name = sid
            results[sid] = s
            print(f"OK ({len(s)} obs)")
        except Exception as e:
            print(f"FAILED: {e}")
        if i % 10 == 0:
            time.sleep(1)  # rate limit courtesy
    return results


def build_monthly_panel(raw: dict[str, pd.Series]) -> pd.DataFrame:
    """Resample all series to monthly (last obs) and join into a single DataFrame."""
    monthly = {}
    for sid, s in raw.items():
        try:
            ms = s.resample("ME").last().dropna()
            monthly[sid] = ms
        except Exception:
            monthly[sid] = s
    df = pd.DataFrame(monthly)
    df.index.name = "date"
    return df


def compute_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Add transformed columns (YoY%, MoM%, diff) based on SERIES config."""
    out = df.copy()
    for sid, meta in SERIES.items():
        if sid not in out.columns:
            continue
        t = meta["transform"]
        col = out[sid]
        if t == "yoy_pct":
            out[f"{sid}_yoy"] = col.pct_change(12) * 100
        elif t == "mom_pct":
            out[f"{sid}_mom"] = col.pct_change(1) * 100
        elif t == "diff":
            out[f"{sid}_diff"] = col.diff()
    return out


def generate_recession_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create derived features useful for recession probability modeling."""
    feat = pd.DataFrame(index=df.index)

    # Yield curve: 10Y-2Y and 10Y-3M (already have these, but ensure they're present)
    if "T10Y2Y" in df.columns:
        feat["yield_curve_2s10s"] = df["T10Y2Y"]
    if "T10Y3M" in df.columns:
        feat["yield_curve_3m10y"] = df["T10Y3M"]

    # Sahm Rule
    if "SAHMREALTIME" in df.columns:
        feat["sahm_rule"] = df["SAHMREALTIME"]

    # HY spread z-score (rolling 2Y)
    if "BAMLH0A0HYM2" in df.columns:
        hy = df["BAMLH0A0HYM2"]
        feat["hy_oas_zscore_24m"] = (hy - hy.rolling(24).mean()) / hy.rolling(24).std()

    # ISM New Orders minus Inventories (if available via ISM data — FRED NAPM series)
    if "NAPM" in df.columns:
        feat["ism_pmi"] = df["NAPM"]

    # Real fed funds rate
    if "DFF" in df.columns and "CPILFESL_yoy" in df.columns:
        feat["real_fed_funds"] = df["DFF"] - df["CPILFESL_yoy"]

    # Claims momentum (4wk vs 26wk MA)
    if "ICSA" in df.columns:
        feat["claims_4w_vs_26w"] = (
            df["ICSA"].rolling(4).mean() / df["ICSA"].rolling(26).mean() - 1
        ) * 100

    # Consumer sentiment deviation from trend
    if "UMCSENT" in df.columns:
        feat["sentiment_deviation"] = (
            df["UMCSENT"] - df["UMCSENT"].rolling(36).mean()
        )

    # Oil shock indicator: MoM pct change in Brent
    if "DCOILBRENTEU" in df.columns:
        feat["brent_mom_pct"] = df["DCOILBRENTEU"].pct_change() * 100
        feat["brent_3m_pct"] = df["DCOILBRENTEU"].pct_change(3) * 100

    # LEI momentum
    if "USSLIND" in df.columns:
        feat["lei_6m_change_pct"] = df["USSLIND"].pct_change(6) * 100

    # NFP 3-month average
    if "PAYEMS_diff" in df.columns:
        feat["nfp_3m_avg"] = df["PAYEMS_diff"].rolling(3).mean()

    return feat


def main():
    if FRED_API_KEY == "YOUR_KEY_HERE":
        print("=" * 60)
        print("ERROR: Set your FRED API key!")
        print("  export FRED_API_KEY=your_key_here")
        print("  Get one free at: https://fred.stlouisfed.org/docs/api/api_key.html")
        print("=" * 60)
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"FRED Historical Data Pull for Recession Simulator")
    print(f"Period: {START_DATE} to {END_DATE}")
    print(f"Series: {len(SERIES)}")
    print(f"{'='*60}\n")

    # 1. Pull raw data
    print("Step 1: Pulling raw series from FRED...")
    raw = pull_all_series(FRED_API_KEY)
    print(f"\n  Successfully pulled {len(raw)}/{len(SERIES)} series.\n")

    # 2. Save raw daily/weekly data as-is
    print("Step 2: Saving raw time series...")
    for sid, s in raw.items():
        s.to_csv(f"{OUTPUT_DIR}/raw_{sid}.csv", header=True)
    print(f"  Saved {len(raw)} raw CSV files to {OUTPUT_DIR}/\n")

    # 3. Build monthly panel
    print("Step 3: Building monthly panel...")
    monthly = build_monthly_panel(raw)
    print(f"  Panel shape: {monthly.shape}\n")

    # 4. Compute transforms
    print("Step 4: Computing transforms (YoY%, MoM%, diffs)...")
    transformed = compute_transforms(monthly)
    print(f"  Transformed shape: {transformed.shape}\n")

    # 5. Generate recession features
    print("Step 5: Generating recession probability features...")
    features = generate_recession_features(transformed)
    print(f"  Features shape: {features.shape}\n")

    # 6. Merge and save
    print("Step 6: Saving outputs...")
    full = pd.concat([transformed, features], axis=1)

    full.to_csv(f"{OUTPUT_DIR}/fred_monthly_panel.csv")
    print(f"  → {OUTPUT_DIR}/fred_monthly_panel.csv")

    try:
        full.to_parquet(f"{OUTPUT_DIR}/fred_monthly_panel.parquet")
        print(f"  → {OUTPUT_DIR}/fred_monthly_panel.parquet")
    except Exception:
        print("  (parquet export skipped — install pyarrow: pip install pyarrow)")

    features.to_csv(f"{OUTPUT_DIR}/recession_features.csv")
    print(f"  → {OUTPUT_DIR}/recession_features.csv")

    # 7. Summary stats
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"  Total series pulled:   {len(raw)}")
    print(f"  Monthly panel columns: {transformed.shape[1]}")
    print(f"  Recession features:    {features.shape[1]}")
    print(f"  Date range:            {full.index.min()} to {full.index.max()}")
    print(f"  Output directory:      {OUTPUT_DIR}/")
    print(f"\nReady to seed MiroFish simulation.")
    print(f"{'='*60}\n")

    # 8. Export series metadata
    meta_export = {}
    for sid, m in SERIES.items():
        meta_export[sid] = {**m, "pulled": sid in raw, "obs_count": len(raw.get(sid, []))}
    with open(f"{OUTPUT_DIR}/series_metadata.json", "w") as f:
        json.dump(meta_export, f, indent=2)
    print(f"  → {OUTPUT_DIR}/series_metadata.json\n")


if __name__ == "__main__":
    main()
