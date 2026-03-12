"""
Vaahan Analytics Layer

Computes market share, growth rates, EV penetration, and other metrics
from cleaned registration data.

Metrics computed:
- Market share % by category per month
- EV penetration % by category
- YoY growth % per OEM
- MoM growth % per OEM
- Share delta (bps change) month-over-month
- 3-month rolling average volumes
- FYTD cumulative volumes and share
"""

import logging
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent


def load_clean_data(clean_dir: Path | None = None) -> pd.DataFrame:
    """Load cleaned parquet data."""
    if clean_dir is None:
        clean_dir = BASE_DIR / "data" / "clean"

    parquet_path = clean_dir / "vaahan_clean.parquet"
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)

    csv_path = clean_dir / "vaahan_clean.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path)

    raise FileNotFoundError(f"No clean data found in {clean_dir}")


def month_to_fy(month_str: str) -> str:
    """Convert YYYY-MM to fiscal year label (e.g., '2023-04' -> 'FY24')."""
    year, month = int(month_str[:4]), int(month_str[5:7])
    fy = year + 1 if month >= 4 else year
    return f"FY{fy % 100:02d}"


def month_to_sort_key(month_str: str) -> str:
    """Return sortable key for YYYY-MM string."""
    return month_str


def compute_market_share(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute market share % by category per month.

    Returns DataFrame with columns:
        month, category, oem_normalized, units, total_category, share_pct
    """
    # Total units per category per month
    totals = (
        df.groupby(["month", "category"])["units"]
        .sum()
        .reset_index()
        .rename(columns={"units": "total_category"})
    )

    # OEM units per category per month
    oem_data = (
        df.groupby(["month", "category", "oem_normalized", "ticker", "listed"])["units"]
        .sum()
        .reset_index()
    )

    merged = oem_data.merge(totals, on=["month", "category"])
    merged["share_pct"] = (merged["units"] / merged["total_category"] * 100).round(2)

    return merged.sort_values(["month", "category", "share_pct"], ascending=[True, True, False])


def compute_ev_penetration(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute EV penetration % = EV registrations / total registrations, by category.

    Returns DataFrame with columns:
        month, category, total_units, ev_units, ev_penetration_pct
    """
    # Total by category per month
    totals = (
        df.groupby(["month", "category"])["units"]
        .sum()
        .reset_index()
        .rename(columns={"units": "total_units"})
    )

    # EV only
    ev_totals = (
        df[df["is_ev"]]
        .groupby(["month", "category"])["units"]
        .sum()
        .reset_index()
        .rename(columns={"units": "ev_units"})
    )

    merged = totals.merge(ev_totals, on=["month", "category"], how="left")
    merged["ev_units"] = merged["ev_units"].fillna(0).astype(int)
    merged["ev_penetration_pct"] = (
        merged["ev_units"] / merged["total_units"] * 100
    ).round(2)

    return merged.sort_values(["month", "category"])


def compute_yoy_growth(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute year-over-year growth % per OEM per category.

    Compares each month to the same month in the previous year.
    """
    oem_monthly = (
        df.groupby(["month", "category", "oem_normalized", "ticker", "listed"])["units"]
        .sum()
        .reset_index()
    )

    oem_monthly["year"] = oem_monthly["month"].str[:4].astype(int)
    oem_monthly["month_num"] = oem_monthly["month"].str[5:7].astype(int)

    # Self-join: current month to same month previous year
    prev = oem_monthly.copy()
    prev["year"] = prev["year"] + 1
    prev = prev.rename(columns={"units": "units_prev_year", "month": "month_prev"})

    merged = oem_monthly.merge(
        prev[["year", "month_num", "category", "oem_normalized", "units_prev_year"]],
        on=["year", "month_num", "category", "oem_normalized"],
        how="left",
    )

    merged["yoy_growth_pct"] = np.where(
        merged["units_prev_year"] > 0,
        ((merged["units"] - merged["units_prev_year"]) / merged["units_prev_year"] * 100).round(1),
        np.nan,
    )

    return merged.drop(columns=["year", "month_num"]).sort_values(["month", "category", "oem_normalized"])


def compute_mom_growth(df: pd.DataFrame) -> pd.DataFrame:
    """Compute month-over-month growth % and share delta (bps)."""
    oem_monthly = (
        df.groupby(["month", "category", "oem_normalized", "ticker", "listed"])["units"]
        .sum()
        .reset_index()
    )

    # Compute share first
    totals = (
        oem_monthly.groupby(["month", "category"])["units"]
        .sum()
        .reset_index()
        .rename(columns={"units": "total_category"})
    )
    oem_monthly = oem_monthly.merge(totals, on=["month", "category"])
    oem_monthly["share_pct"] = (oem_monthly["units"] / oem_monthly["total_category"] * 100).round(2)

    # Sort and compute MoM
    oem_monthly = oem_monthly.sort_values(["category", "oem_normalized", "month"])

    oem_monthly["units_prev_month"] = oem_monthly.groupby(
        ["category", "oem_normalized"]
    )["units"].shift(1)
    oem_monthly["share_prev_month"] = oem_monthly.groupby(
        ["category", "oem_normalized"]
    )["share_pct"].shift(1)

    oem_monthly["mom_growth_pct"] = np.where(
        oem_monthly["units_prev_month"] > 0,
        (
            (oem_monthly["units"] - oem_monthly["units_prev_month"])
            / oem_monthly["units_prev_month"]
            * 100
        ).round(1),
        np.nan,
    )

    # Share delta in basis points
    oem_monthly["share_delta_bps"] = (
        (oem_monthly["share_pct"] - oem_monthly["share_prev_month"]) * 100
    ).round(0)

    return oem_monthly.sort_values(["month", "category", "oem_normalized"])


def compute_rolling_avg(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """Compute N-month rolling average volumes per OEM per category."""
    oem_monthly = (
        df.groupby(["month", "category", "oem_normalized"])["units"]
        .sum()
        .reset_index()
        .sort_values(["category", "oem_normalized", "month"])
    )

    oem_monthly[f"units_rolling_{window}m"] = (
        oem_monthly.groupby(["category", "oem_normalized"])["units"]
        .transform(lambda x: x.rolling(window, min_periods=1).mean().round(0))
    )

    return oem_monthly


def compute_fytd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute fiscal-year-to-date cumulative volumes and share.

    Indian FY starts April. FYTD for Aug-24 = cumulative Apr-24 to Aug-24.
    """
    oem_monthly = (
        df.groupby(["month", "category", "oem_normalized", "ticker", "listed"])["units"]
        .sum()
        .reset_index()
    )

    oem_monthly["fy"] = oem_monthly["month"].apply(month_to_fy)
    oem_monthly = oem_monthly.sort_values(["fy", "category", "oem_normalized", "month"])

    # Cumulative sum within each FY
    oem_monthly["fytd_units"] = oem_monthly.groupby(
        ["fy", "category", "oem_normalized"]
    )["units"].cumsum()

    # FYTD category totals for share
    cat_fytd = (
        oem_monthly.groupby(["fy", "month", "category"])["fytd_units"]
        .transform("sum")
    )
    # Use the latest cumulative total per category per FY-month
    fytd_totals = (
        oem_monthly.groupby(["fy", "month", "category"])["units"]
        .sum()
        .groupby(level=["fy", "category"])
        .cumsum()
        .reset_index()
        .rename(columns={"units": "fytd_category_total"})
    )

    oem_monthly = oem_monthly.merge(
        fytd_totals, on=["fy", "month", "category"], how="left"
    )
    oem_monthly["fytd_share_pct"] = (
        oem_monthly["fytd_units"] / oem_monthly["fytd_category_total"] * 100
    ).round(2)

    return oem_monthly


def compute_all_analytics(
    df: pd.DataFrame, listed_only: bool = False
) -> dict[str, pd.DataFrame]:
    """
    Compute all analytics from clean data.

    Args:
        df: Cleaned registration DataFrame
        listed_only: If True, filter to listed companies only

    Returns:
        Dict of metric name -> DataFrame
    """
    if listed_only:
        df = df[df["listed"]].copy()
        logger.info(f"Filtered to listed companies: {df['oem_normalized'].nunique()} OEMs")

    results = {}

    logger.info("Computing market share...")
    results["market_share"] = compute_market_share(df)

    logger.info("Computing EV penetration...")
    results["ev_penetration"] = compute_ev_penetration(df)

    logger.info("Computing YoY growth...")
    results["yoy_growth"] = compute_yoy_growth(df)

    logger.info("Computing MoM growth and share delta...")
    results["mom_growth"] = compute_mom_growth(df)

    logger.info("Computing 3-month rolling averages...")
    results["rolling_avg"] = compute_rolling_avg(df, window=3)

    logger.info("Computing FYTD cumulative volumes...")
    results["fytd"] = compute_fytd(df)

    return results


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    df = load_clean_data()
    results = compute_all_analytics(df)

    for name, result_df in results.items():
        print(f"\n{'='*60}")
        print(f"{name.upper()} ({len(result_df)} rows)")
        print(f"{'='*60}")
        print(result_df.head(10).to_string())


if __name__ == "__main__":
    main()
