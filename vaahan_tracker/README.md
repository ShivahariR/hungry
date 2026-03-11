# Vaahan Registration Tracker

Python pipeline for scraping and analyzing India's vehicle registration data from the [Vaahan Dashboard](https://vahan.parivahan.gov.in/vahan4dashboard/) (MoRTH).

Tracks monthly retail registrations by OEM, vehicle category, and fuel type — the ground-truth demand signal for Indian auto OEMs.

## Features

- **Scraper**: Playwright-based scraper with network interception for API endpoint discovery
- **Pipeline**: OEM name normalization, ticker mapping, EV classification
- **Analytics**: Market share, YoY/MoM growth, EV penetration, FYTD volumes, rolling averages
- **Dashboard Export**: JSON output matching React frontend contract
- **Scheduler**: Cron-ready monthly updates with Slack/Telegram notifications

## Coverage

| Category | Key OEMs Tracked |
|----------|-----------------|
| **Passenger Vehicles** | Maruti Suzuki (MARUTI), Tata Motors (TATAMOTORS), Mahindra (M&M), Hyundai (HYUNDAI), Kia, Toyota, MG Motor |
| **Commercial Vehicles** | Tata Motors, Ashok Leyland (ASHOKLEY), Eicher/VECV (EICHERMOT), Force Motors (FORCEMOT), SML Isuzu, Olectra |
| **Two Wheelers** | Hero MotoCorp (HEROMOTOCO), Honda 2W, TVS (TVSMOTOR), Bajaj Auto (BAJAJ-AUTO), Royal Enfield (EICHERMOT), Ola Electric (OLAELEC) |
| **Three Wheelers** | Bajaj Auto, Mahindra, Piaggio, TVS |
| **EV Pure-Plays** | Ola Electric, Ather Energy, Olectra, BYD, Revolt, Ampere |

## Setup

```bash
cd vaahan_tracker
pip install -r requirements.txt
playwright install chromium
```

## Usage

### Full Historical Scrape
```bash
python scheduler.py --full
```

### Scheduled Monthly Run (incremental)
```bash
python scheduler.py
```

### Individual Components
```bash
# Scraper only
python -m scraper.vaahan_scraper

# Pipeline only (requires raw data)
python -m pipeline.vaahan_pipeline

# Analytics + export only (requires clean data)
python -m analytics.export_dashboard
```

### Crontab
```
0 6 16 * * cd /path/to/vaahan_tracker && python scheduler.py >> /var/log/vaahan.log 2>&1
```

## Output

Dashboard JSON at `data/output/dashboard.json`:

```json
{
  "last_updated": "2025-02-15",
  "categories": ["Passenger Vehicles", "Commercial Vehicles", "Two Wheelers", "Three Wheelers", "Electric Vehicles"],
  "monthly_data": { ... },
  "ev_data": { ... },
  "oem_meta": { ... },
  "market_share": { ... },
  "ev_penetration": { ... },
  "yoy_growth": { ... },
  "fytd": { ... },
  "fy_prior_avg": { ... }
}
```

## Project Structure

```
vaahan_tracker/
├── scraper/
│   ├── vaahan_scraper.py      # Playwright-based scraper
│   └── network_interceptor.py # XHR endpoint discovery
├── pipeline/
│   ├── vaahan_pipeline.py     # Clean + normalize
│   ├── oem_mapping.json       # OEM → ticker/category mapping
│   └── ev_classifier.py       # Fuel type → EV flag logic
├── analytics/
│   ├── vaahan_analytics.py    # Market share, YoY, penetration
│   └── export_dashboard.py    # JSON export for React frontend
├── data/
│   ├── raw/                   # Raw scraped HTML/JSON
│   ├── clean/                 # Normalized parquet
│   └── output/                # Dashboard-ready JSON
├── scheduler.py               # Cron-ready monthly runner
├── config.yaml                # Configuration
└── requirements.txt
```

## Notes

- Vaahan data typically updates by the 10th-15th of the following month
- The scraper always re-scrapes the last 2 months to capture data revisions
- EV volumes are tracked both within parent categories and as a separate "Electric Vehicles" category
- Raw scraped data is preserved for audit trail
