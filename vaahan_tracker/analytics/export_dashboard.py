"""
Dashboard JSON Exporter

Converts analytics DataFrames into the JSON format expected by the
React dashboard frontend.

Output contract:
{
  "last_updated": "2025-02-15",
  "categories": [...],
  "monthly_data": { category: [ { month, OEM1: count, OEM2: count, ... } ] },
  "ev_data": { category: [ { month, OEM1: count, ... } ] },
  "oem_meta": { OEM: { ticker, category, listed, color } },
  "market_share": { category: [ { month, OEM1: share%, ... } ] },
  "ev_penetration": { category: [ { month, total, ev, penetration_pct } ] },
  "yoy_growth": { category: [ { month, OEM1: growth%, ... } ] },
  "fytd": { category: [ { month, fy, OEM1: units, ... } ] },
  "fy_prior_avg": { category: { OEM: avg_monthly_units } }
}
"""

import json
import logging
from datetime import date
from pathlib import Path

import pandas as pd

from analytics.vaahan_analytics import (
    compute_all_analytics,
    load_clean_data,
    month_to_fy,
)

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
MAPPING_PATH = BASE_DIR / "pipeline" / "oem_mapping.json"


def load_oem_mapping() -> dict:
    with open(MAPPING_PATH) as f:
        return json.load(f)


def format_month_label(month_str: str) -> str:
    """Convert '2024-04' to 'Apr-24'."""
    months = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    parts = month_str.split("-")
    year = parts[0][2:]  # '2024' -> '24'
    month_idx = int(parts[1]) - 1
    return f"{months[month_idx]}-{year}"


def pivot_to_monthly_dict(
    df: pd.DataFrame, value_col: str = "units", oem_col: str = "oem_normalized"
) -> dict[str, list[dict]]:
    """
    Pivot DataFrame into {category: [{month, OEM1: val, OEM2: val, ...}]} format.
    """
    result = {}

    for category in df["category"].unique():
        cat_df = df[df["category"] == category]
        pivot = cat_df.pivot_table(
            index="month", columns=oem_col, values=value_col, aggfunc="sum"
        ).fillna(0)

        records = []
        for month_str in sorted(pivot.index):
            row = {"month": format_month_label(str(month_str))}
            for oem in pivot.columns:
                val = pivot.loc[month_str, oem]
                row[oem] = int(val) if value_col == "units" else round(float(val), 2)
            records.append(row)

        result[category] = records

    return result


def build_oem_meta(df: pd.DataFrame) -> dict:
    """Build OEM metadata for the dashboard."""
    mapping = load_oem_mapping()
    oem_mappings = mapping.get("oem_mappings", {})

    # Build reverse lookup: normalized name -> info
    meta = {}
    seen_normalized = set()

    for raw_name, info in oem_mappings.items():
        normalized = info["normalized"]
        if normalized in seen_normalized:
            continue
        seen_normalized.add(normalized)

        # Find which categories this OEM appears in from the data
        oem_cats = df[df["oem_normalized"] == normalized]["category"].unique().tolist()
        primary_category = info.get("categories", oem_cats)
        if isinstance(primary_category, list) and primary_category:
            primary_category = primary_category[0]

        meta[normalized] = {
            "ticker": info.get("ticker"),
            "category": primary_category,
            "listed": info.get("listed", False),
            "color": info.get("color", "#94a3b8"),
        }

    # Add any OEMs found in data but not in mapping
    for oem in df["oem_normalized"].unique():
        if oem not in meta:
            oem_cats = df[df["oem_normalized"] == oem]["category"].unique().tolist()
            meta[oem] = {
                "ticker": None,
                "category": oem_cats[0] if oem_cats else "Other",
                "listed": False,
                "color": "#94a3b8",
            }

    return meta


def compute_fy_prior_avg(df: pd.DataFrame) -> dict:
    """
    Compute prior FY average monthly volumes per OEM per category.
    Used as baseline for current FY comparison.
    """
    df = df.copy()
    df["fy"] = df["month"].apply(month_to_fy)

    # Get the two most recent FYs
    fys = sorted(df["fy"].unique())
    if len(fys) < 2:
        return {}

    prior_fy = fys[-2]  # Second to last FY

    prior_df = df[df["fy"] == prior_fy]
    n_months = prior_df["month"].nunique()

    if n_months == 0:
        return {}

    result = {}
    for category in prior_df["category"].unique():
        cat_df = prior_df[prior_df["category"] == category]
        oem_totals = cat_df.groupby("oem_normalized")["units"].sum()
        result[category] = {
            oem: round(units / n_months)
            for oem, units in oem_totals.items()
        }

    return result


def export_dashboard_json(
    df: pd.DataFrame | None = None,
    output_dir: Path | None = None,
    listed_only: bool = False,
) -> dict:
    """
    Generate the complete dashboard JSON.

    Args:
        df: Cleaned DataFrame (loads from disk if None)
        output_dir: Output directory (defaults to data/output)
        listed_only: Filter to listed companies only

    Returns:
        The dashboard JSON dict
    """
    if df is None:
        df = load_clean_data()

    if output_dir is None:
        output_dir = BASE_DIR / "data" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    analytics = compute_all_analytics(df, listed_only=listed_only)

    # Build the dashboard JSON
    categories = sorted(df["category"].unique().tolist())
    # Ensure "Electric Vehicles" is included as a virtual category
    if "Electric Vehicles" not in categories:
        categories.append("Electric Vehicles")

    ev_df = df[df["is_ev"]].copy()

    dashboard = {
        "last_updated": date.today().isoformat(),
        "categories": categories,
        "monthly_data": pivot_to_monthly_dict(df, "units"),
        "ev_data": pivot_to_monthly_dict(ev_df, "units") if not ev_df.empty else {},
        "oem_meta": build_oem_meta(df),
        "market_share": pivot_to_monthly_dict(
            analytics["market_share"], "share_pct"
        ),
        "ev_penetration": {},
        "yoy_growth": {},
        "fytd": {},
        "fy_prior_avg": compute_fy_prior_avg(df),
    }

    # EV penetration per category
    ev_pen = analytics["ev_penetration"]
    for category in ev_pen["category"].unique():
        cat_data = ev_pen[ev_pen["category"] == category]
        dashboard["ev_penetration"][category] = [
            {
                "month": format_month_label(str(row["month"])),
                "total": int(row["total_units"]),
                "ev": int(row["ev_units"]),
                "penetration_pct": float(row["ev_penetration_pct"]),
            }
            for _, row in cat_data.iterrows()
        ]

    # YoY growth pivoted
    yoy = analytics["yoy_growth"]
    yoy_clean = yoy.dropna(subset=["yoy_growth_pct"])
    if not yoy_clean.empty:
        dashboard["yoy_growth"] = pivot_to_monthly_dict(
            yoy_clean, "yoy_growth_pct"
        )

    # FYTD
    fytd = analytics["fytd"]
    if not fytd.empty:
        for category in fytd["category"].unique():
            cat_data = fytd[fytd["category"] == category]
            # Get latest month per FY
            latest = cat_data.sort_values("month").groupby(
                ["fy", "oem_normalized"]
            ).last().reset_index()
            pivot = latest.pivot_table(
                index=["fy", "month"],
                columns="oem_normalized",
                values="fytd_units",
                aggfunc="sum",
            ).fillna(0)

            records = []
            for (fy, month), row in pivot.iterrows():
                rec = {"month": format_month_label(str(month)), "fy": fy}
                for oem in pivot.columns:
                    rec[oem] = int(row[oem])
                records.append(rec)
            dashboard["fytd"][category] = records

    # Write output
    output_path = output_dir / "dashboard.json"
    with open(output_path, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)
    logger.info(f"Dashboard JSON exported: {output_path}")

    # Also export a compact version (no indent, smaller file)
    compact_path = output_dir / "dashboard.min.json"
    with open(compact_path, "w") as f:
        json.dump(dashboard, f, separators=(",", ":"), default=str)
    logger.info(f"Compact JSON exported: {compact_path}")

    return dashboard


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    dashboard = export_dashboard_json()
    print(f"\nDashboard JSON exported with {len(dashboard['categories'])} categories")
    print(f"OEMs tracked: {len(dashboard['oem_meta'])}")


if __name__ == "__main__":
    main()
