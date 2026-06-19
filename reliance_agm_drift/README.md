# reliance_agm_drift

Empirical test of the trader folklore that **Reliance Industries (RELIANCE, NSE)
drifts lower after its Annual General Meeting (AGM)**, checked across multiple
holding windows (1 day → 6 months) and benchmarked against the NIFTY 50.

**TL;DR:** the "sell-the-AGM" dip is real but small and only lasts a few trading
days (mainly *vs the market*); there is no sustained downward drift — 1–6 months
later RIL is usually higher. Full write-up in [RESULTS.md](RESULTS.md).

```
data/reliance_daily.csv   RELIANCE daily OHLCV, split/bonus adjusted (NSE EOD)
data/nifty50_index.csv    NIFTY 50 index daily close (benchmark)
analyze.py                per-AGM tables + aggregate summary
make_chart.py             writes post_agm_drift.png
RESULTS.md                findings, tables, interpretation, sources
```

Run:

```bash
python3 analyze.py
python3 make_chart.py
```
