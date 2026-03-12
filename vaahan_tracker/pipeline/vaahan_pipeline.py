"""
Vaahan Data Pipeline

Cleans and normalizes raw scraped registration data:
- Normalizes OEM names using oem_mapping.json
- Maps OEMs to tickers, categories, listed status
- Classifies EV/hybrid status
- Outputs monthly parquet/CSV with standardized schema
"""

import csv
import json
import logging
from pathlib import Path

import pandas as pd

from pipeline.ev_classifier import classify_ev

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
MAPPING_PATH = Path(__file__).parent / "oem_mapping.json"


def load_oem_mapping() -> dict:
    with open(MAPPING_PATH) as f:
        return json.load(f)


def normalize_oem_name(raw_name: str, mapping: dict) -> dict:
    """
    Look up raw OEM name in mapping. Returns normalized info or a default.

    Returns dict with keys: normalized, ticker, listed, color
    """
    raw_upper = raw_name.upper().strip()
    oem_mappings = mapping.get("oem_mappings", {})

    # Direct lookup
    for key, info in oem_mappings.items():
        if key.upper() == raw_upper:
            return {
                "oem_normalized": info["normalized"],
                "ticker": info.get("ticker"),
                "listed": info.get("listed", False),
                "color": info.get("color", "#94a3b8"),
            }

    # Fuzzy: check if raw name contains a known key or vice versa
    for key, info in oem_mappings.items():
        key_upper = key.upper()
        if key_upper in raw_upper or raw_upper in key_upper:
            return {
                "oem_normalized": info["normalized"],
                "ticker": info.get("ticker"),
                "listed": info.get("listed", False),
                "color": info.get("color", "#94a3b8"),
            }

    # Unknown OEM — return cleaned-up version
    normalized = raw_name.strip().title()
    # Remove common suffixes
    for suffix in [" Ltd", " Limited", " Pvt", " Private", " India"]:
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)].strip()
    return {
        "oem_normalized": normalized,
        "ticker": None,
        "listed": False,
        "color": "#94a3b8",
    }


def map_category(raw_category: str, mapping: dict) -> str:
    """Map raw Vaahan category to display name."""
    cat_map = mapping.get("category_mapping", {})
    return cat_map.get(raw_category, raw_category)


def process_raw_data(raw_data: list[dict]) -> pd.DataFrame:
    """
    Process raw scraped records into clean DataFrame.

    Input records expected to have:
        month, oem_raw, category_raw, units, fuel_type

    Output DataFrame columns:
        month, oem, oem_normalized, ticker, listed, category,
        fuel_type, is_ev, is_hybrid, units
    """
    if not raw_data:
        logger.warning("No raw data to process")
        return pd.DataFrame()

    mapping = load_oem_mapping()
    processed = []

    for record in raw_data:
        oem_raw = record.get("oem_raw", "").strip()
        if not oem_raw:
            continue

        oem_info = normalize_oem_name(oem_raw, mapping)
        ev_info = classify_ev(
            record.get("fuel_type", "ALL"),
            oem_raw,
        )

        processed.append({
            "month": record["month"],
            "oem": oem_raw,
            "oem_normalized": oem_info["oem_normalized"],
            "ticker": oem_info["ticker"],
            "listed": oem_info["listed"],
            "category": map_category(record.get("category_raw", ""), mapping),
            "fuel_type": record.get("fuel_type", "ALL"),
            "is_ev": ev_info["is_ev"],
            "is_hybrid": ev_info["is_hybrid"],
            "units": int(record.get("units", 0)),
        })

    df = pd.DataFrame(processed)

    # Ensure month is string in YYYY-MM format
    if not df.empty:
        df["month"] = df["month"].astype(str)
        # Sort by month, category, OEM
        df = df.sort_values(["month", "category", "oem_normalized"]).reset_index(
            drop=True
        )

    logger.info(f"Processed {len(df)} records from {len(raw_data)} raw records")
    return df


def load_raw_data_from_files(raw_dir: Path) -> list[dict]:
    """Load all raw JSON files from the raw data directory."""
    all_data = []
    for json_file in sorted(raw_dir.glob("raw_*.json")):
        with open(json_file) as f:
            data = json.load(f)
            all_data.extend(data)
        logger.debug(f"Loaded {len(data)} records from {json_file.name}")

    # Also try CSV
    csv_file = raw_dir / "all_raw_data.csv"
    if csv_file.exists() and not all_data:
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["units"] = int(row.get("units", 0))
                all_data.append(row)
        logger.debug(f"Loaded {len(all_data)} records from CSV")

    logger.info(f"Total raw records loaded: {len(all_data)}")
    return all_data


def save_clean_data(df: pd.DataFrame, output_dir: Path, fmt: str = "both") -> None:
    """Save cleaned data as parquet and/or CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if fmt in ("parquet", "both"):
        parquet_path = output_dir / "vaahan_clean.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Saved parquet: {parquet_path}")

    if fmt in ("csv", "both"):
        csv_path = output_dir / "vaahan_clean.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved CSV: {csv_path}")


def run_pipeline(raw_dir: Path | None = None, output_dir: Path | None = None) -> pd.DataFrame:
    """Run the full cleaning pipeline."""
    if raw_dir is None:
        raw_dir = BASE_DIR / "data" / "raw"
    if output_dir is None:
        output_dir = BASE_DIR / "data" / "clean"

    raw_data = load_raw_data_from_files(raw_dir)
    df = process_raw_data(raw_data)

    if not df.empty:
        save_clean_data(df, output_dir)

        # Log summary stats
        logger.info(f"Months covered: {df['month'].nunique()}")
        logger.info(f"OEMs found: {df['oem_normalized'].nunique()}")
        logger.info(f"Categories: {df['category'].unique().tolist()}")
        logger.info(f"Listed OEMs: {df[df['listed']]['oem_normalized'].nunique()}")
        logger.info(f"EV records: {df['is_ev'].sum()}")

    return df


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    df = run_pipeline()
    if not df.empty:
        print(f"\nPipeline complete. {len(df)} clean records.")
        print(f"\nSample:\n{df.head(10).to_string()}")
    else:
        print("\nNo data to process. Run the scraper first.")


if __name__ == "__main__":
    main()
