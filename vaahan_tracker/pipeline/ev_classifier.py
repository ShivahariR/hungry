"""
EV classifier for Vaahan registration data.

Classifies vehicles as EV based on fuel type and OEM identity.
Handles both explicit EV fuel types and EV pure-play OEMs.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Fuel types that definitively indicate electric vehicles
EV_FUEL_TYPES = {
    "ELECTRIC(BOV)",
    "ELECTRIC",
    "BATTERY",
    "ELECTRIC(BATT)",
    "PURE EV",
    "BEV",
}

# Hybrid fuel types — tracked separately, not classified as pure EV
HYBRID_FUEL_TYPES = {
    "PETROL/HYBRID",
    "DIESEL/HYBRID",
    "HYBRID",
    "PHEV",
    "PLUG-IN HYBRID",
    "STRONG HYBRID",
    "MILD HYBRID",
}

MAPPING_PATH = Path(__file__).parent / "oem_mapping.json"


def load_ev_pure_plays() -> set[str]:
    """Load OEM names marked as EV pure-play from mapping."""
    with open(MAPPING_PATH) as f:
        mapping = json.load(f)

    pure_plays = set()
    for raw_name, info in mapping.get("oem_mappings", {}).items():
        if info.get("ev_pure_play", False):
            pure_plays.add(raw_name.upper())
            pure_plays.add(info["normalized"].upper())
    return pure_plays


_ev_pure_plays: set[str] | None = None


def _get_pure_plays() -> set[str]:
    global _ev_pure_plays
    if _ev_pure_plays is None:
        _ev_pure_plays = load_ev_pure_plays()
    return _ev_pure_plays


def is_ev_fuel_type(fuel_type: str) -> bool:
    """Check if fuel type indicates an electric vehicle."""
    return fuel_type.upper().strip() in EV_FUEL_TYPES


def is_hybrid_fuel_type(fuel_type: str) -> bool:
    """Check if fuel type indicates a hybrid vehicle."""
    return fuel_type.upper().strip() in HYBRID_FUEL_TYPES


def is_ev_oem(oem_name: str) -> bool:
    """Check if OEM is a pure-play EV manufacturer."""
    return oem_name.upper().strip() in _get_pure_plays()


def classify_ev(fuel_type: str, oem_raw: str) -> dict:
    """
    Classify a record's EV status.

    Returns dict with:
        is_ev: bool — True if this is an electric vehicle
        is_hybrid: bool — True if this is a hybrid
        ev_source: str — How EV was determined ('fuel_type', 'oem', 'both', or None)
    """
    ft_upper = fuel_type.upper().strip()
    ev_by_fuel = ft_upper in EV_FUEL_TYPES
    hybrid = ft_upper in HYBRID_FUEL_TYPES
    ev_by_oem = is_ev_oem(oem_raw)

    if ev_by_fuel and ev_by_oem:
        source = "both"
    elif ev_by_fuel:
        source = "fuel_type"
    elif ev_by_oem:
        # OEM is EV pure-play, so even if fuel_type says ALL, classify as EV
        source = "oem"
    else:
        source = None

    return {
        "is_ev": ev_by_fuel or ev_by_oem,
        "is_hybrid": hybrid,
        "ev_source": source,
    }
