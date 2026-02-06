"""
Configuration for the Global Handover Dashboard.
Maps global signals to Indian cyclical sectors.
"""

# --- Indian Cyclical Sectors of Interest ---
CYCLICAL_SECTORS = [
    "Autos",
    "Capital Goods",
    "Defense",
    "Metals",
    "Energy",
    "Chemicals",
    "Logistics",
]

# --- US Index Tickers ---
US_INDICES = {
    "S&P 500": "^GSPC",
    "Nasdaq": "^IXIC",
    "Dow Jones": "^DJI",
    "Russell 2000": "^RUT",
}

# --- US Factor / Style ETFs for rotation detection ---
US_FACTOR_ETFS = {
    "Value (IWD)": "IWD",
    "Growth (IWF)": "IWF",
    "Industrials (XLI)": "XLI",
    "Defense (ITA)": "ITA",
    "Semis (SOXX)": "SOXX",
    "Materials (XLB)": "XLB",
    "Energy (XLE)": "XLE",
}

# --- US-to-India Read-Through Map ---
# Maps US sector ETFs to Indian cyclical counterparts
US_TO_INDIA_READTHROUGH = {
    "XLI": {
        "sector": "Capital Goods / Industrials",
        "indian_proxies": ["L&T", "Siemens", "ABB India", "Cummins India"],
        "logic": "US industrial strength signals global capex cycle -> Indian capgoods benefit",
    },
    "ITA": {
        "sector": "Defense",
        "indian_proxies": ["HAL", "BEL", "Bharat Dynamics", "Data Patterns"],
        "logic": "US defense spending trends -> allied defense ecosystem tailwinds",
    },
    "SOXX": {
        "sector": "Electronics / Cap Goods",
        "indian_proxies": ["Dixon Tech", "Kaynes Tech", "Tata Elxsi"],
        "logic": "Semi cycle leads electronics capex -> Indian EMS/design plays",
    },
    "XLB": {
        "sector": "Metals / Chemicals",
        "indian_proxies": ["Tata Steel", "Hindalco", "SRF", "Navin Fluorine"],
        "logic": "US materials demand -> global commodity pricing -> Indian margin impact",
    },
    "XLE": {
        "sector": "Energy",
        "indian_proxies": ["ONGC", "Oil India", "GAIL", "Reliance"],
        "logic": "US energy sector direction -> crude/gas pricing -> Indian energy cos",
    },
}

# --- ADR Mappings (US ADR ticker -> NSE ticker) ---
ADR_MAPPINGS = {
    "INFY": {"nse": "INFY.NS", "name": "Infosys"},
    "WIT": {"nse": "WIPRO.NS", "name": "Wipro"},
    "HDB": {"nse": "HDFCBANK.NS", "name": "HDFC Bank"},
    "IBN": {"nse": "ICICIBANK.NS", "name": "ICICI Bank"},
    "TTM": {"nse": "TATAMOTORS.NS", "name": "Tata Motors"},
    "RDY": {"nse": "DRREDDY.NS", "name": "Dr. Reddy's"},
    "VEDL": {"nse": "VEDL.NS", "name": "Vedanta"},
    "SIFY": {"nse": "SIFY.NS", "name": "Sify Tech"},
}

# --- Commodity Tickers and India Impact Map ---
COMMODITY_TICKERS = {
    "Brent Crude": "BZ=F",
    "WTI Crude": "CL=F",
    "Gold": "GC=F",
    "Silver": "SI=F",
    "Copper": "HG=F",
    "Aluminium": "ALI=F",
    "Natural Gas": "NG=F",
}

COMMODITY_INDIA_IMPACT = {
    "BZ=F": [
        {"direction": "up", "impact": "Negative", "sectors": "Paints, Aviation, Tyres", "detail": "Input cost pressure on crude-linked sectors"},
        {"direction": "up", "impact": "Positive", "sectors": "ONGC, Oil India, Reliance", "detail": "Higher realizations for upstream E&P"},
        {"direction": "down", "impact": "Positive", "sectors": "Paints, Aviation, Tyres", "detail": "Margin tailwind from lower crude"},
        {"direction": "down", "impact": "Negative", "sectors": "ONGC, Oil India", "detail": "Lower realizations for upstream"},
    ],
    "HG=F": [
        {"direction": "up", "impact": "Positive", "sectors": "Hindalco (Novelis), Hindustan Copper", "detail": "Higher commodity price = better realizations"},
        {"direction": "up", "impact": "Negative", "sectors": "Cable cos, Capital Goods (input cost)", "detail": "Higher input costs for copper consumers"},
    ],
    "ALI=F": [
        {"direction": "up", "impact": "Positive", "sectors": "Hindalco, NALCO, Vedanta", "detail": "Aluminium producers benefit from higher LME"},
        {"direction": "up", "impact": "Negative", "sectors": "Auto ancillaries, Packaging", "detail": "Input cost headwind"},
    ],
    "GC=F": [
        {"direction": "up", "impact": "Positive", "sectors": "Titan, Kalyan Jewellers, Senco Gold", "detail": "Higher gold = inventory gains + revenue growth"},
    ],
    "NG=F": [
        {"direction": "up", "impact": "Negative", "sectors": "Fertilizers, City Gas (IGL, MGL, Gujarat Gas)", "detail": "Higher gas cost = margin compression"},
        {"direction": "up", "impact": "Positive", "sectors": "ONGC, Oil India (gas segment)", "detail": "Higher APM/spot gas realization"},
    ],
}

# --- Global Companies to scan for India/Asia commentary ---
GLOBAL_EARNINGS_WATCHLIST = [
    "Caterpillar", "Cummins", "Tesla", "GE Aerospace", "Honeywell",
    "3M", "Siemens AG", "ABB Ltd", "Schneider Electric",
    "BASF", "Dow Inc", "Rio Tinto", "BHP", "Glencore",
    "Lockheed Martin", "RTX Corp", "Boeing", "Airbus",
    "Apple", "TSMC", "Samsung", "Foxconn",
]

INDIA_KEYWORDS = [
    "India", "Indian", "Asia demand", "Asia capex", "capex cycle",
    "emerging market", "South Asia", "Make in India", "PLI scheme",
    "Indian defense", "Indian infrastructure",
]

# --- NSE F&O Universe: Non-BFSI Cyclicals Focus ---
NSE_FO_CYCLICALS = {
    "Autos": ["TATAMOTORS", "M&M", "MARUTI", "BAJAJ-AUTO", "HEROMOTOCO", "EICHERMOT", "ASHOKLEY", "BHARATFORG", "MOTHERSON"],
    "Capital Goods": ["LT", "SIEMENS", "ABB", "HAVELLS", "CUMMINSIND", "THERMAX", "BEL", "BHEL"],
    "Defense": ["HAL", "BEL", "SOLARINDS", "BDL"],
    "Metals": ["TATASTEEL", "HINDALCO", "JSWSTEEL", "VEDL", "NATIONALUM", "NMDC", "COALINDIA"],
    "Energy": ["RELIANCE", "ONGC", "BPCL", "IOC", "GAIL", "PETRONET"],
    "Chemicals": ["SRF", "PIIND", "ATUL", "NAVINFLUOR", "DEEPAKNTR", "UPL"],
    "Logistics": ["CONCOR", "ADANIPORTS"],
}

# --- FX Pair for ADR spread calculation ---
FX_TICKER = "USDINR=X"

# --- Thresholds ---
ADR_SPREAD_SIGNIFICANT_PCT = 1.0  # Flag ADR spreads > 1%
COMMODITY_MOVE_SIGNIFICANT_PCT = 1.5  # Flag commodity moves > 1.5%
INDEX_MOVE_SIGNIFICANT_PCT = 0.5  # Flag index moves > 0.5%
