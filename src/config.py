import os
import tomllib
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
CREDENTIALS_PATH = BASE_DIR / "credentials" / "gen-lang-client-0815236933-340089633139.json"
DATA_DIR = BASE_DIR / "data" / "filings"
DB_PATH = BASE_DIR / "data" / "insights.db"
JSONL_PATH = BASE_DIR / "data" / "insights.jsonl"
LOG_PATH = BASE_DIR / "logs" / "pipeline.log"

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv(BASE_DIR / ".env")

# Gemini Developer API
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables. Please create a .env file (see .env.example)")

GEMINI_MODEL = "gemini-3-flash-preview"
GEMINI_MODEL_FALLBACKS = ["gemini-2.5-flash", "gemini-2.0-flash-001"]

HTTPS_PROXY = os.getenv("HTTPS_PROXY", "http://127.0.0.1:7890")

SEC_BASE_URL = "https://data.sec.gov"
SEC_CIK_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_RATE_LIMIT = 8
VERTEX_WORKERS = 6
SEC_WORKERS = 8

# ── Load from stocks.toml ──────────────────────────────────
_TOML_PATH = BASE_DIR / "stocks.toml"
with open(_TOML_PATH, "rb") as _f:
    _cfg = tomllib.load(_f)

YEARS: list[int] = _cfg["years"]

# Flatten tiers → {ticker: tier_name}, deduplicate (first occurrence wins)
COMPANIES: dict[str, list[str]] = {
    tier: data["tickers"] for tier, data in _cfg["companies"].items()
}
ALL_TICKERS: dict[str, str] = {}
for _tier, _tickers in COMPANIES.items():
    for _t in _tickers:
        ALL_TICKERS.setdefault(_t, _tier)

FOREIGN_FILERS: set[str] = set(_cfg.get("foreign_filers") or [])
IPO_YEAR_FLOOR: dict[str, int] = _cfg.get("ipo_year_floor", {})
