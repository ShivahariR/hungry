#!/usr/bin/env python3
"""Visualise the post-AGM drift study (writes post_agm_drift.png)."""
import csv
import os
from datetime import datetime
from statistics import mean, median

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from analyze import (AGM_DATES, HORIZONS, load_series, fwd_return)

HERE = os.path.dirname(os.path.abspath(__file__))

rdates, rclose = load_series(os.path.join(HERE, "data/reliance_daily.csv"), "Date", "Close")
ndates, nclose = load_series(os.path.join(HERE, "data/nifty50_index.csv"), "Date", "Close")
bench_start = ndates[0]

labels = [lbl for _, lbl in HORIZONS]
abs_mean, abs_med, exc_mean, exc_med, pct_down = [], [], [], [], []
for h, _ in HORIZONS:
    a, e = [], []
    for ds, _f in AGM_DATES:
        agm = datetime.strptime(ds, "%Y-%m-%d").date()
        r = fwd_return(rdates, rclose, agm, h)
        n = fwd_return(ndates, nclose, agm, h) if agm >= bench_start else None
        if r is not None:
            a.append(r)
        if r is not None and n is not None:
            e.append(r - n)
    abs_mean.append(mean(a) * 100); abs_med.append(median(a) * 100)
    exc_mean.append(mean(e) * 100); exc_med.append(median(e) * 100)
    pct_down.append(sum(1 for x in e if x < 0) / len(e) * 100)

x = np.arange(len(labels))
fig, axes = plt.subplots(1, 3, figsize=(16, 5))

ax = axes[0]
w = 0.38
ax.bar(x - w/2, abs_mean, w, label="mean", color="#2b8cbe")
ax.bar(x + w/2, abs_med, w, label="median", color="#a6bddb")
ax.axhline(0, color="k", lw=0.8)
ax.set_title("RELIANCE absolute return after AGM\n(2012-2025, 14 AGMs)")
ax.set_ylabel("return (%)"); ax.set_xticks(x); ax.set_xticklabels(labels, rotation=45, ha="right")
ax.legend(); ax.grid(axis="y", alpha=0.3)

ax = axes[1]
ax.bar(x - w/2, exc_mean, w, label="mean", color="#e34a33")
ax.bar(x + w/2, exc_med, w, label="median", color="#fdbb84")
ax.axhline(0, color="k", lw=0.8)
ax.set_title("Excess return vs NIFTY 50 after AGM\n(stock-specific drift)")
ax.set_ylabel("excess return (%)"); ax.set_xticks(x); ax.set_xticklabels(labels, rotation=45, ha="right")
ax.legend(); ax.grid(axis="y", alpha=0.3)

ax = axes[2]
ax.bar(x, pct_down, 0.6, color="#756bb1")
ax.axhline(50, color="k", lw=0.8, ls="--", label="coin-flip (50%)")
ax.set_title("Share of AGMs with NEGATIVE excess return")
ax.set_ylabel("% of AGMs down vs NIFTY"); ax.set_ylim(0, 100)
ax.set_xticks(x); ax.set_xticklabels(labels, rotation=45, ha="right")
ax.legend(); ax.grid(axis="y", alpha=0.3)

fig.suptitle("Does Reliance drift lower after its AGM?  — yes, but only for a few days, and mainly vs the market",
             fontsize=13, y=1.02)
fig.tight_layout()
out = os.path.join(HERE, "post_agm_drift.png")
fig.savefig(out, dpi=130, bbox_inches="tight")
print("wrote", out)
