#!/usr/bin/env python3
"""
Test the theory: does Reliance Industries (RELIANCE, NSE) drift LOWER after its AGM?

Approach
--------
For each historical AGM we anchor at T0 = the closing price on the AGM trading day
(the last trading session on or before the AGM date). We then measure the forward
return of the stock over a series of holding windows (1 day ... 6 months of trading
days). To separate stock-specific drift from the broad market, we also measure the
NIFTY 50 return over the identical window and report the EXCESS (RIL minus NIFTY)
return. A negative average excess return after the AGM would support the theory.

Data
----
- data/reliance_daily.csv : RELIANCE daily OHLCV, fully split/bonus adjusted
                            (source: BennyThadikaran/eod2_data, NSE EOD).
- data/nifty50_index.csv  : NIFTY 50 index daily close (benchmark).

Prices are adjusted for the 1:1 bonuses of Sep-2017 and Oct-2024 (verified: the
series is continuous across both record dates), so returns are directly comparable
across years. The series is NOT dividend-adjusted, but RIL's dividend yield (~0.3%)
is negligible over the windows studied.
"""

import csv
import os
from datetime import date, datetime
from statistics import mean, median

HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# AGM dates (NSE calendar). 2013-2025 verified against contemporaneous press /
# RIL notices; 2012 is approximate (lower confidence) and flagged below.
# ---------------------------------------------------------------------------
AGM_DATES = [
    ("2012-06-07", "approx"),   # 2012 - lower confidence
    ("2013-06-06", "ok"),
    ("2014-06-18", "ok"),
    ("2015-06-12", "ok"),
    ("2016-09-01", "ok"),
    ("2017-07-21", "ok"),
    ("2018-07-05", "ok"),
    ("2019-08-12", "ok"),
    ("2020-07-15", "ok"),
    ("2021-06-24", "ok"),
    ("2022-08-29", "ok"),
    ("2023-08-28", "ok"),
    ("2024-08-29", "ok"),
    ("2025-08-29", "ok"),
]

# Holding windows in TRADING days, with friendly labels.
HORIZONS = [
    (1,   "1 day"),
    (3,   "3 days"),
    (5,   "1 week"),
    (10,  "2 weeks"),
    (21,  "1 month"),
    (42,  "2 months"),
    (63,  "3 months"),
    (126, "6 months"),
]


def load_series(path, date_col, close_col):
    """Return (sorted list of dates, dict date->close)."""
    dates, close = [], {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            d = datetime.strptime(row[date_col], "%Y-%m-%d").date()
            try:
                c = float(row[close_col])
            except (ValueError, KeyError):
                continue
            dates.append(d)
            close[d] = c
    dates.sort()
    return dates, close


def anchor_index(dates, agm):
    """Index of the last trading day on or before the AGM date."""
    lo, hi, res = 0, len(dates) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if dates[mid] <= agm:
            res = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return res


def fwd_return(dates, close, agm, h):
    """Forward simple return from AGM-day close over h trading days."""
    i0 = anchor_index(dates, agm)
    if i0 is None or i0 + h >= len(dates):
        return None
    return close[dates[i0 + h]] / close[dates[i0]] - 1.0


def pct(x):
    return f"{x*100:+6.2f}%" if x is not None else "   n/a "


def main():
    rdates, rclose = load_series(os.path.join(HERE, "data/reliance_daily.csv"),
                                 "Date", "Close")
    ndates, nclose = load_series(os.path.join(HERE, "data/nifty50_index.csv"),
                                 "Date", "Close")
    print(f"RELIANCE daily series : {rdates[0]} -> {rdates[-1]}  ({len(rdates)} sessions)")
    print(f"NIFTY50 benchmark     : {ndates[0]} -> {ndates[-1]}  ({len(ndates)} sessions)")
    print(f"AGMs tested           : {len(AGM_DATES)} ({AGM_DATES[0][0][:4]}-{AGM_DATES[-1][0][:4]})\n")

    have_bench = ndates[0]  # NIFTY only starts 2007-09

    # Collect per-(AGM,horizon) absolute and excess returns.
    abs_ret = {h: [] for h, _ in HORIZONS}
    exc_ret = {h: [] for h, _ in HORIZONS}

    # ---- Per-AGM detail (absolute RIL return) ----
    head = "AGM date      " + "".join(f"{lbl:>10}" for _, lbl in HORIZONS)
    print("ABSOLUTE RELIANCE RETURN FROM AGM-DAY CLOSE")
    print(head)
    print("-" * len(head))
    for ds, flag in AGM_DATES:
        agm = datetime.strptime(ds, "%Y-%m-%d").date()
        line = f"{ds}{'*' if flag=='approx' else ' '} "
        for h, _ in HORIZONS:
            r = fwd_return(rdates, rclose, agm, h)
            if r is not None:
                abs_ret[h].append(r)
            line += f"{pct(r):>10}"
        print(line)

    # ---- Per-AGM detail (excess vs NIFTY) ----
    print("\nEXCESS RETURN (RELIANCE minus NIFTY50) FROM AGM-DAY CLOSE")
    print(head)
    print("-" * len(head))
    for ds, flag in AGM_DATES:
        agm = datetime.strptime(ds, "%Y-%m-%d").date()
        line = f"{ds}{'*' if flag=='approx' else ' '} "
        for h, _ in HORIZONS:
            rr = fwd_return(rdates, rclose, agm, h)
            nn = fwd_return(ndates, nclose, agm, h) if agm >= have_bench else None
            e = (rr - nn) if (rr is not None and nn is not None) else None
            if e is not None:
                exc_ret[h].append(e)
            line += f"{pct(e):>10}"
        print(line)

    # ---- Aggregates ----
    def summarize(title, store):
        print(f"\n{title}")
        print(f"{'window':>9} {'n':>4} {'mean':>9} {'median':>9} {'% down':>8} {'worst':>9} {'best':>9}")
        for h, lbl in HORIZONS:
            xs = store[h]
            if not xs:
                continue
            ndown = sum(1 for x in xs if x < 0)
            print(f"{lbl:>9} {len(xs):>4} {mean(xs)*100:>8.2f}% {median(xs)*100:>8.2f}% "
                  f"{ndown/len(xs)*100:>6.0f}% {min(xs)*100:>8.2f}% {max(xs)*100:>8.2f}%")

    print("\n" + "=" * 70)
    summarize("SUMMARY - ABSOLUTE RELIANCE RETURN AFTER AGM", abs_ret)
    summarize("SUMMARY - EXCESS RETURN vs NIFTY50 AFTER AGM", exc_ret)
    print("\n* 2012 AGM date approximate (lower confidence).")
    print("'% down' = share of AGMs where the return over that window was negative.")


if __name__ == "__main__":
    main()
