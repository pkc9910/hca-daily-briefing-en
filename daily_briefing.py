#!/usr/bin/env python3
"""
HCA Daily Briefing — Nasdaq Copenhagen News Scraper

Scrapes Nasdaq Nordic RSS feed for news about companies listed on Nasdaq Copenhagen
Main Market. Uses Claude to summarize and rank news by investor impact. Sends an
HTML email briefing daily via Gmail SMTP.
"""

import argparse
import datetime
import json
import logging
import os
import re
import smtplib
import sys
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import anthropic
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Constants ---
# Nasdaq News API for Copenhagen company announcements
NASDAQ_NEWS_API_URL = (
    "https://api.news.eu.nasdaq.com/news/query.action"
    "?type=json&showAttachments=true&showCns498LookupButton=true"
    "&showCompany=true&countResults=true"
    "&market=Main%20Market%2C%20Copenhagen"
    "&limit=100&start=0&offset=0"
)
TICKERS_CACHE_FILE = Path("tickers_cache.json")
HCA_COVERED_COMPANIES_FILE = Path("hca_covered_companies.json")
ANALYSIS_SYSTEM_PROMPT_FILE = Path("analysis_system_prompt.md")
INDERES_STYLE_PROMPT_FILE = Path("inderes_style_prompt.md")
TICKERS_CACHE_MAX_AGE_DAYS = 7
NEWS_LOOKBACK_HOURS = 48
CLAUDE_MODEL_TRIAGE = "claude-haiku-4-5-20251001"    # Fast pass: classify & rank
CLAUDE_MODEL_ANALYSIS = "claude-sonnet-4-6"           # Deep pass: detailed summaries
CLAUDE_MODEL_REWRITE = "claude-sonnet-4-6"            # Pass 3: Inderes-style rewrite
REQUEST_TIMEOUT = 30
MIN_TICKER_THRESHOLD = 40  # If fewer tickers scraped, use seed list instead
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8",
}

# Nasdaq Nordic API endpoints for instrument lists
NASDAQ_API_URL = (
    "https://api.nasdaq.com/api/nordic/instruments/shares"
    "?exchange=CSE&locale=en"
)
NASDAQ_DATAFEED_URL = (
    "http://www.nasdaqomxnordic.com/webproxy/DataFeedProxy.aspx"
    "?SubSystem=Prices&Action=GetInstruments&Exchange=CSE&instType=S"
)

# Impact-level colours for HTML email
IMPACT_COLOURS = {
    1: {"bg": "#fde8e8", "border": "#e53e3e", "label": "Highly Impactful"},
    2: {"bg": "#fef3e2", "border": "#dd6b20", "label": "Significant"},
    3: {"bg": "#fefce8", "border": "#d69e2e", "label": "Moderate"},
    4: {"bg": "#ebf8ff", "border": "#3182ce", "label": "Low Impact"},
    5: {"bg": "#f7fafc", "border": "#a0aec0", "label": "Minimal"},
}

# Hardcoded seed list — fallback when APIs return insufficient data.
# Covers C25 index and major Copenhagen Main Market companies.
SEED_TICKERS = {
    # C25 Index constituents
    "NOVO B": "Novo Nordisk",
    "DSV": "DSV",
    "ORSTED": "Orsted",
    "VWS": "Vestas Wind Systems",
    "CARL B": "Carlsberg",
    "MAERSK A": "A.P. Moller - Maersk",
    "MAERSK B": "A.P. Moller - Maersk",
    "PNDORA": "Pandora",
    "COLO B": "Coloplast",
    "GMAB": "Genmab",
    "DANSKE": "Danske Bank",
    "TRYG": "Tryg",
    "DEMANT": "Demant",
    "NSIS B": "Novonesis",
    "GN": "GN Store Nord",
    "ISS": "ISS",
    "AMBU B": "Ambu",
    "FLS": "FLSmidth",
    "NETC": "Netcompany Group",
    "RBREW": "Royal Unibrew",
    "BAVA": "Bavarian Nordic",
    "ROCK B": "Rockwool",
    "ZEAL": "Zealand Pharma",
    "DFDS": "DFDS",
    "JYSK": "Jyske Bank",
    # Large Cap
    "NDA DK": "Nordea",
    "SYDB": "Sydbank",
    "TOP": "Topdanmark",
    "LUN": "Lundbeck",
    "NKT": "NKT",
    "DNORD": "D/S Norden",
    "ALMB": "ALM. Brand",
    "NNIT": "NNIT",
    "SCHOUW": "Schouw",
    "STER": "Scandinavian Tobacco Group",
    # Mid Cap
    "CBRAIN": "cBrain",
    "RILBA": "Ringkjoebing Landbobank",
    "SPAR": "Spar Nord Bank",
    "GREEN": "Green Hydrogen Systems",
    "BNORDIK": "BankNordik",
    "HARB B": "Harboes Bryggeri",
    "PAAL B": "Per Aarsleff Holding",
    "TIGO": "Millicom International Cellular",
    "WIRTEK": "Wirtek",
    "SIM": "SimCorp",
    # Additional Main Market companies
    "ASTRO": "Astralis",
    "AG": "Agat Ejendomme",
    "AOJ": "Andersen og Jensen",
    "ARCTIC": "Arctic Seafood Group",
    "ATLA": "Atlantic Petroleum",
    "BIOPOR": "Bioporto",
    "BNORD": "Brodrene A O Johansen",
    "BRDRF": "Brodrene Hartmann",
    "CEMAT": "Cemat",
    "CHEMM": "ChemoMetec",
    "CICAN": "Cicor Technologies",
    "COLUM": "Columbus",
    "CONFRZ": "Conferize",
    "CPHI": "Copenhagen Infrastructure Partners",
    "DANT": "Dantax",
    "EAC": "East Asiatic Company",
    "ESOFT": "EG A/S",
    "ENSI": "Ennogie",
    "ERRIA": "Erria",
    "FFARMS": "FirstFarms",
    "FLUG": "Flugger",
    "FOM": "Fom Technologies",
    "GABR": "Gabriel Holding",
    "GYLD": "Gyldendal",
    "HARP": "Harpoon Therapeutics",
    "HCAND": "HC Andersen Capital",
    "HH": "H H International",
    "HOVE": "Hove",
    "HTH": "H Lundbeck",
    "HUSCO": "Huscompagniet",
    "HYDR": "Hydrogen Pro",
    "IC": "IC Group",
    "IHF": "Investeringsforeningen",
    "KEM": "Kemp Lauritzen",
    "KRIS": "Krist. Gerhard Jebsen",
    "LASB": "Lassen Bros",
    "LUXOR": "Luxor",
    "MATAS": "Matas",
    "MONDO": "Mondo",
    "MOVI": "Movinn",
    "MT": "MT Hojgaard Holding",
    "NLFSK": "Nilfisk",
    "NNRD": "NunaMinerals",
    "NORD": "Nordzucker",
    "NORTHM": "North Media",
    "NRTC": "Nortic",
    "NUNA": "Nunaminerals",
    "OCEA": "Oceana Group",
    "ORPHAZ": "Orphazyme",
    "PENG": "Penguin Random House",
    "PHLI": "Phil and Teds",
    "PRIM": "Prime Office",
    "PROSP": "Prosperity Resources",
    "QNTH": "Quntha",
    "RAVN": "Ravn IT",
    "RILB": "Ringkjobing Landbobank",
    "RISMA": "Risma Systems",
    "ROVSN": "Rovsing",
    "RTXS": "RTX",
    "SANIST": "Sanistal",
    "SCAN": "Scandinavian Investment Group",
    "SCHB": "Schur",
    "SCHO": "Solar",
    "SHP": "Shp Group",
    "SKAKO": "Skako",
    "SPNO": "Spno",
    "SOLAR B": "Solar",
    "SPEAS": "SP Group",
    "SSB": "SSB Holding",
    "STG": "Strategisk Invest",
    "STIBY": "Stiby",
    "TCM": "TCM Group",
    "THERAP": "Theravance Biopharma",
    "TIVOLI": "Tivoli",
    "TKHO": "Torm",
    "TOBY": "Tobii",
    "TOPSOE": "Topsoe",
    "TORM A": "Torm",
    "UIE": "United International Enterprises",
    "VELO": "Velocys",
    "VIROG": "Virogates",
    "VLT": "Vlaamse Televisie Maatschappij",
    "WDHL": "William Demant Holding",
    "WEST": "Vestjysk Bank",
    "WINT": "Wintershall",
    "XTER": "Xtera Communications",
}


# ─────────────────────────────────────────────────────────────────────────────
# Ticker management
# ─────────────────────────────────────────────────────────────────────────────

def load_tickers() -> dict[str, str]:
    """Return {ticker_symbol: company_name} for Nasdaq Copenhagen Main Market.

    Uses a local JSON cache that is refreshed weekly. Falls back through
    multiple data sources if the primary API is unavailable.
    """
    # Check cache freshness
    if TICKERS_CACHE_FILE.exists():
        try:
            data = json.loads(TICKERS_CACHE_FILE.read_text())
            cached_at = datetime.datetime.fromisoformat(data["cached_at"])
            age = datetime.datetime.now(datetime.timezone.utc) - cached_at
            if age.days < TICKERS_CACHE_MAX_AGE_DAYS:
                logger.info(
                    "Using cached tickers (%d companies, cached %s)",
                    len(data["tickers"]),
                    data["cached_at"],
                )
                return data["tickers"]
            logger.info("Ticker cache expired (%d days old), refreshing.", age.days)
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Corrupt ticker cache, will re-scrape: %s", exc)

    # Try primary source
    tickers = _scrape_nasdaq_api()

    # Fallback to DataFeedProxy
    if len(tickers) < MIN_TICKER_THRESHOLD:
        if tickers:
            logger.warning(
                "Nasdaq API returned only %d tickers (below threshold %d), trying fallback.",
                len(tickers), MIN_TICKER_THRESHOLD
            )
        scraped = _scrape_datafeed_proxy()
        if len(scraped) > len(tickers):
            tickers = scraped

    # Final fallback: use seed list if still below threshold
    if len(tickers) < MIN_TICKER_THRESHOLD:
        logger.warning(
            "Scraped only %d tickers (below threshold %d). Using seed list (%d companies).",
            len(tickers), MIN_TICKER_THRESHOLD, len(SEED_TICKERS)
        )
        # Merge any scraped tickers with the seed list for maximum coverage
        tickers = {**SEED_TICKERS, **tickers}

    # Persist cache
    _save_ticker_cache(tickers)
    return tickers


def _scrape_nasdaq_api() -> dict[str, str]:
    """Primary: Nasdaq API for Nordic instruments."""
    logger.info("Scraping tickers from Nasdaq API …")
    try:
        resp = requests.get(
            NASDAQ_API_URL, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        payload = resp.json()

        tickers: dict[str, str] = {}
        # The API nests instruments differently depending on version.
        # Try common structures.
        rows = (
            payload.get("data", {}).get("table", {}).get("rows", [])
            or payload.get("data", {}).get("rows", [])
            or payload.get("instruments", [])
            or payload.get("data", [])
        )
        for row in rows:
            symbol = (
                row.get("symbol") or row.get("Symbol") or row.get("ticker") or ""
            ).strip()
            name = (
                row.get("name") or row.get("Name") or row.get("companyName") or ""
            ).strip()
            if symbol and name:
                tickers[symbol] = name

        if tickers:
            logger.info("Nasdaq API returned %d tickers.", len(tickers))
        else:
            logger.warning("Nasdaq API returned 0 tickers (unexpected format).")
        return tickers

    except Exception as exc:
        logger.warning("Nasdaq API failed: %s", exc)
        return {}


def _scrape_datafeed_proxy() -> dict[str, str]:
    """Fallback: Nasdaq Nordic DataFeedProxy (pipe-delimited text)."""
    logger.info("Trying DataFeedProxy fallback …")
    try:
        resp = requests.get(
            NASDAQ_DATAFEED_URL, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        text = resp.text

        tickers: dict[str, str] = {}
        for line in text.splitlines():
            # Skip header/metadata lines starting with @
            if line.startswith("@") or not line.strip():
                continue
            parts = line.split(";")
            if len(parts) < 3:
                parts = line.split("|")
            if len(parts) >= 3:
                symbol = parts[1].strip() if len(parts) > 1 else ""
                name = parts[2].strip() if len(parts) > 2 else ""
                if not symbol:
                    symbol = parts[0].strip()
                if symbol and name:
                    tickers[symbol] = name

        if tickers:
            logger.info("DataFeedProxy returned %d tickers.", len(tickers))
        else:
            logger.warning("DataFeedProxy returned 0 tickers.")
        return tickers

    except Exception as exc:
        logger.warning("DataFeedProxy failed: %s", exc)
        return {}


def _save_ticker_cache(tickers: dict[str, str]) -> None:
    data = {
        "cached_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "tickers": tickers,
    }
    TICKERS_CACHE_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    logger.info("Saved %d tickers to cache.", len(tickers))


# ─────────────────────────────────────────────────────────────────────────────
# HCA Covered Companies
# ─────────────────────────────────────────────────────────────────────────────

def load_hca_covered_companies() -> dict:
    """Load HCA covered companies from JSON config file.

    Returns a dict with:
    - 'tickers': set of all tickers (uppercase)
    - 'names': set of company names and aliases (lowercase)
    - 'companies': list of original company entries
    """
    if not HCA_COVERED_COMPANIES_FILE.exists():
        logger.warning("HCA covered companies file not found: %s", HCA_COVERED_COMPANIES_FILE)
        return {"tickers": set(), "names": set(), "companies": []}

    try:
        data = json.loads(HCA_COVERED_COMPANIES_FILE.read_text())
        companies = data.get("companies", [])

        tickers = set()
        names = set()

        for company in companies:
            # Add all tickers
            for ticker in company.get("tickers", []):
                tickers.add(ticker.upper())
            # Add company name and aliases
            names.add(company.get("name", "").lower())
            for alias in company.get("aliases", []):
                names.add(alias.lower())

        logger.info("Loaded %d HCA covered companies (%d tickers, %d name variants)",
                    len(companies), len(tickers), len(names))
        return {"tickers": tickers, "names": names, "companies": companies}

    except (json.JSONDecodeError, KeyError) as exc:
        logger.warning("Failed to load HCA covered companies: %s", exc)
        return {"tickers": set(), "names": set(), "companies": []}


def is_hca_company(item: dict, hca_data: dict) -> bool:
    """Check if a news item relates to an HCA covered company."""
    # Check ticker match
    ticker = item.get("ticker", "").upper()
    if ticker and ticker in hca_data["tickers"]:
        return True

    # Check company name match
    company = item.get("company", "").lower()
    if company:
        # Exact match
        if company in hca_data["names"]:
            return True
        # Partial match (company name contains or is contained by HCA name)
        for hca_name in hca_data["names"]:
            if len(hca_name) >= 4 and (hca_name in company or company in hca_name):
                return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Market Data (Yahoo Finance API)
# ─────────────────────────────────────────────────────────────────────────────

# Market instruments grouped by category for the overview table.
# Each entry: (yahoo_ticker, display_name, category, format_type)
# format_type: "price" = show as number, "pct" = show as percentage
MARKET_INSTRUMENTS = [
    # Equities
    ("ACWI", "MSCI World ACWI (in DKK)", "Equities", "price"),
    ("^GSPC", "SP500**", "Equities", "price"),
    ("^STOXX", "Euro. Stoxx600", "Equities", "price"),
    ("^OMXC25", "OMX C25", "Equities", "price"),
    # Commodities
    ("BZ=F", "Oil (Brent)", "Commodities", "price"),
    ("GC=F", "Gold", "Commodities", "price"),
    # Rates
    ("^TNX", "US 10-Year", "Rates", "pct"),
    ("DK10Y.BOND", "DK 10-Year", "Rates", "pct"),
    # FX
    ("USDDKK=X", "USDDKK", "FX", "price"),
]

YAHOO_FINANCE_API_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _fetch_ytd_change(symbol: str, headers: dict) -> float | None:
    """Fetch year-to-date percentage change for a symbol from Yahoo Finance.

    Uses range=ytd to get the first trading day close of the year vs current price.
    """
    try:
        url = YAHOO_FINANCE_API_URL.format(symbol=symbol)
        resp = requests.get(
            url, headers=headers, timeout=10,
            params={"range": "ytd", "interval": "1d"},
        )
        resp.raise_for_status()
        data = resp.json()

        result = data.get("chart", {}).get("result", [])
        if not result:
            return None

        meta = result[0].get("meta", {})
        current_price = meta.get("regularMarketPrice")
        chart_prev_close = meta.get("chartPreviousClose")

        if current_price is not None and chart_prev_close is not None and chart_prev_close != 0:
            return ((current_price - chart_prev_close) / chart_prev_close) * 100
        return None

    except Exception as exc:
        logger.debug("YTD fetch failed for %s: %s", symbol, exc)
        return None


def fetch_market_indices() -> list[dict]:
    """Fetch market data for all instruments from Yahoo Finance API.

    Returns list of dicts with: name, category, format_type, price, day_change_pct, ytd_pct
    """
    logger.info("Fetching market data from Yahoo Finance...")
    indices = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    for ticker_symbol, display_name, category, fmt_type in MARKET_INSTRUMENTS:
        try:
            url = YAHOO_FINANCE_API_URL.format(symbol=ticker_symbol)
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            result = data.get("chart", {}).get("result", [])
            if result:
                meta = result[0].get("meta", {})
                current_price = meta.get("regularMarketPrice")
                prev_close = meta.get("previousClose") or meta.get("chartPreviousClose")

                day_change_pct = None
                if current_price is not None and prev_close is not None and prev_close != 0:
                    day_change_pct = ((current_price - prev_close) / prev_close) * 100

                ytd_pct = _fetch_ytd_change(ticker_symbol, headers)

                indices.append({
                    "name": display_name,
                    "category": category,
                    "format_type": fmt_type,
                    "price": current_price,
                    "day_change_pct": day_change_pct,
                    "ytd_pct": ytd_pct,
                })
                logger.info(
                    "  %s: %s (day: %s, ytd: %s)",
                    display_name,
                    f"{current_price:.2f}" if current_price else "N/A",
                    f"{day_change_pct:.2f}%" if day_change_pct is not None else "N/A",
                    f"{ytd_pct:.2f}%" if ytd_pct is not None else "N/A",
                )
            else:
                logger.warning("No data returned for %s", ticker_symbol)
                indices.append({
                    "name": display_name,
                    "category": category,
                    "format_type": fmt_type,
                    "price": None,
                    "day_change_pct": None,
                    "ytd_pct": None,
                })

        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", ticker_symbol, exc)
            indices.append({
                "name": display_name,
                "category": category,
                "format_type": fmt_type,
                "price": None,
                "day_change_pct": None,
                "ytd_pct": None,
            })

    return indices


# ─────────────────────────────────────────────────────────────────────────────
# News fetching
# ─────────────────────────────────────────────────────────────────────────────

def fetch_news(tickers: dict[str, str]) -> list[dict]:
    """Fetch company news from Nasdaq Copenhagen News API.

    The API returns announcements from companies listed on Nasdaq Copenhagen Main Market.
    The tickers parameter is used for enriching company information if needed.
    """
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        hours=NEWS_LOOKBACK_HOURS
    )

    # Fetch from Nasdaq News API (primary source)
    news_items = _fetch_nasdaq_news_api(cutoff)

    # Enrich with ticker info from our list if API doesn't provide it
    ticker_lookup = {name.lower(): ticker for ticker, name in tickers.items()}
    for item in news_items:
        if item.get("ticker") == "N/A" and item.get("company"):
            company_lower = item["company"].lower()
            # Try exact match first
            if company_lower in ticker_lookup:
                item["ticker"] = ticker_lookup[company_lower]
            else:
                # Try partial match
                for name, ticker in ticker_lookup.items():
                    if name in company_lower or company_lower in name:
                        item["ticker"] = ticker
                        break

    logger.info(
        "Found %d news items from Nasdaq Copenhagen (cutoff %s).",
        len(news_items),
        cutoff.isoformat(),
    )

    return news_items


def _clean_html(text: str) -> str:
    """Strip HTML tags from text."""
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean[:1000]  # Truncate to save tokens


def _fetch_nasdaq_news_api(cutoff: datetime.datetime) -> list[dict]:
    """Fetch company news directly from Nasdaq Nordic News API.

    Returns list of news items with ticker, company, title, description, published, link.
    """
    logger.info("Fetching news from Nasdaq News API: %s", NASDAQ_NEWS_API_URL[:80])

    try:
        resp = requests.get(
            NASDAQ_NEWS_API_URL,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        # Debug: log the raw response structure
        logger.info("API response keys: %s", list(data.keys()) if isinstance(data, dict) else type(data))

        # The API returns nested structure: {"results": {"item": [...]}, "count": N}
        results_container = data.get("results", {})

        # Handle different possible structures
        if isinstance(results_container, dict):
            # Try common nested keys: item, items, news, data
            results = (
                results_container.get("item", []) or
                results_container.get("items", []) or
                results_container.get("news", []) or
                results_container.get("data", [])
            )
            logger.info("Results container keys: %s", list(results_container.keys())[:10])
        elif isinstance(results_container, list):
            results = results_container
        else:
            results = []

        # Ensure results is a list
        if not isinstance(results, list):
            logger.warning("Results is not a list, got: %s", type(results).__name__)
            results = [results] if results else []

        logger.info("Nasdaq API returned %d news items", len(results))

        # Debug: log first result structure
        if results and len(results) > 0:
            sample = results[0]
            if isinstance(sample, dict):
                logger.info("Sample item keys: %s", list(sample.keys())[:15])
                # Log a sample of the values to understand structure
                for key in list(sample.keys())[:5]:
                    val = sample[key]
                    val_str = str(val)[:100] if val else "None"
                    logger.info("  %s: %s", key, val_str)
            else:
                logger.info("Sample item (not dict): type=%s value=%s", type(sample).__name__, str(sample)[:200])

        matched: list[dict] = []
        seen_titles: set[str] = set()

        for item in results:
            # Parse the news item - adapt to actual API response structure
            # Common field names: headline, title, company, symbol, ticker, published, releaseTime
            title = (
                item.get("headline", "") or
                item.get("title", "") or
                item.get("messageTitle", "")
            ).strip()

            if not title:
                continue

            # Skip duplicates
            title_norm = title.lower()
            if title_norm in seen_titles:
                continue

            # Parse publication date
            pub_str = (
                item.get("releaseTime", "") or
                item.get("published", "") or
                item.get("publishedDate", "") or
                item.get("time", "")
            )
            pub_date = _parse_nasdaq_date(pub_str)

            # Filter by cutoff time
            if pub_date and pub_date < cutoff:
                continue

            # Get company/ticker info
            company = (
                item.get("company", "") or
                item.get("companyName", "") or
                item.get("issuer", "")
            ).strip()

            ticker = (
                item.get("symbol", "") or
                item.get("ticker", "") or
                item.get("cnsCode", "")
            ).strip()

            # Get description/summary
            description = (
                item.get("summary", "") or
                item.get("description", "") or
                item.get("messageText", "") or
                item.get("text", "")
            ).strip()

            # Get link to press release
            link = (
                item.get("link", "") or
                item.get("url", "") or
                item.get("messageUrl", "")
            ).strip()

            # Build the cns link if we have a cnsCode
            if not link and item.get("cnsCode"):
                link = f"https://www.nasdaq.com/press-release/{item.get('cnsCode')}"

            # Only include if we have meaningful content
            if title and (company or ticker):
                seen_titles.add(title_norm)
                matched.append({
                    "ticker": ticker or "N/A",
                    "company": company or "Unknown",
                    "title": title,
                    "description": _clean_html(description) if description else title,
                    "published": pub_date.isoformat() if pub_date else "Unknown",
                    "link": link,
                })

        logger.info("Nasdaq API: %d news items after filtering (cutoff: %s)",
                    len(matched), cutoff.isoformat())
        return matched

    except requests.exceptions.RequestException as exc:
        logger.warning("Nasdaq News API request failed: %s", exc)
        return []
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        logger.warning("Nasdaq News API response parsing failed: %s", exc)
        return []


def _parse_nasdaq_date(date_str: str) -> datetime.datetime | None:
    """Parse various date formats from Nasdaq API."""
    if not date_str:
        return None

    # Try ISO format first
    try:
        # Handle formats like "2024-02-13T09:00:00Z" or "2024-02-13T09:00:00+00:00"
        dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt
    except ValueError:
        pass

    # Try common formats
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
        "%d/%m/%Y %H:%M",
    ]
    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(date_str, fmt)
            return dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue

    logger.debug("Could not parse date: %s", date_str)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Claude summarisation — Two-pass analysis
# ─────────────────────────────────────────────────────────────────────────────

def _load_system_prompt() -> str:
    """Load the analyst system prompt from file, with inline fallback."""
    if ANALYSIS_SYSTEM_PROMPT_FILE.exists():
        prompt = ANALYSIS_SYSTEM_PROMPT_FILE.read_text(encoding="utf-8").strip()
        if prompt:
            logger.info("Loaded analysis system prompt from %s (%d chars)",
                        ANALYSIS_SYSTEM_PROMPT_FILE, len(prompt))
            return prompt
    logger.warning("System prompt file not found, using inline fallback.")
    return (
        "You are a senior equity research analyst specialising in Nordic equities, "
        "with deep expertise in Nasdaq Copenhagen-listed companies. You work for "
        "HC Andersen Capital (HCA), a Danish investment advisory firm. "
        "Analyse news through the lens of earnings impact, strategic significance, "
        "catalyst timing, and sector read-through. Be concise but substantive."
    )


def _load_inderes_prompt() -> str:
    """Load the Inderes morning news style prompt from file, with inline fallback."""
    if INDERES_STYLE_PROMPT_FILE.exists():
        prompt = INDERES_STYLE_PROMPT_FILE.read_text(encoding="utf-8").strip()
        if prompt:
            logger.info("Loaded Inderes style prompt from %s (%d chars)",
                        INDERES_STYLE_PROMPT_FILE, len(prompt))
            return prompt
    logger.warning("Inderes style prompt file not found, using inline fallback.")
    return (
        "Write short, sharp morning news in English in the Inderes style. "
        "Use professional financial language with common abbreviations (EBITDA, EBIT, m, bn). "
        "The tone is neutral and fact-based. Never mention individuals by name. "
        "NEVER use bullet points or headings — write flowing prose. "
        "ALWAYS include comparison figures in parentheses for key metrics."
    )


def _parse_claude_json(response_text: str) -> dict | list:
    """Extract and parse JSON from a Claude response, handling code fences."""
    json_text = response_text.strip()

    # Handle markdown code fences — both complete and truncated
    if json_text.startswith("```"):
        lines = json_text.split("\n", 1)
        if len(lines) > 1:
            json_text = lines[1]
        if "```" in json_text:
            json_text = json_text.rsplit("```", 1)[0]
        json_text = json_text.strip()

    try:
        return json.loads(json_text)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse Claude JSON: %s", exc)
        logger.error("Raw response:\n%s", response_text[:500])
        raise RuntimeError("Claude returned invalid JSON. See logs.") from exc


def _build_items_text(news_items: list[dict]) -> tuple[str, dict]:
    """Build compact text representation + metadata lookup for news items."""
    item_metadata = {}
    items_text = ""
    for i, item in enumerate(news_items, 1):
        item_metadata[i] = {
            "link": item.get("link", ""),
            "published": item.get("published", ""),
        }
        items_text += (
            f"[{i}] {item['ticker']} — {item['company']}\n"
            f"Headline: {item['title']}\n"
            f"Snippet: {item['description'][:300]}\n"
            f"Date: {item['published']}\n\n"
        )
    return items_text, item_metadata


# ── Few-shot example for consistent output quality ──

FEW_SHOT_EXAMPLE = """
EXAMPLE INPUT:
[1] NOVO B — Novo Nordisk
Headline: Novo Nordisk reports Q4 2025 results above consensus, raises FY2026 guidance
Snippet: Revenue grew 28% YoY driven by GLP-1 franchise. Operating profit exceeded consensus by 4%. Company raises full-year 2026 revenue guidance to DKK 290-300bn from DKK 275-285bn.
Date: 2026-02-05T07:00:00+00:00

[2] DANSKE — Danske Bank
Headline: Danske Bank announces new share buyback programme of DKK 5bn
Snippet: The Board of Directors has approved a new share buyback programme of up to DKK 5 billion to be executed over the next 12 months, subject to regulatory approval.
Date: 2026-02-05T08:30:00+00:00

[3] BAVA — Bavarian Nordic
Headline: Bavarian Nordic publishes financial calendar for 2026
Snippet: Bavarian Nordic A/S today published its financial calendar for 2026. Annual Report 2025 will be published on 26 February 2026.
Date: 2026-02-05T09:00:00+00:00

EXAMPLE OUTPUT:
{
  "items": [
    {
      "id": 1,
      "ticker": "NOVO B",
      "company": "Novo Nordisk",
      "headline": "Novo Nordisk reports Q4 2025 results above consensus, raises FY2026 guidance",
      "impact": 1,
      "summary": "Novo Nordisk delivered a strong beat with Q4 revenue up 28% YoY, driven by continued GLP-1 momentum, and operating profit 4% above consensus. The raised FY2026 guidance to DKK 290-300bn (from 275-285bn) signals management confidence in sustained demand and should drive consensus estimate upgrades of 3-5%, supporting further multiple expansion for the C25 heavyweight."
    },
    {
      "id": 2,
      "ticker": "DANSKE",
      "company": "Danske Bank",
      "headline": "Danske Bank announces new share buyback programme of DKK 5bn",
      "impact": 2,
      "summary": "The DKK 5bn buyback programme represents approximately 3% of Danske Bank's market cap and signals robust excess capital generation. This shareholder-friendly capital return should provide ongoing share price support and may prompt consensus to revise capital return forecasts upward, with the programme likely to be EPS-accretive by 2-3% on an annualised basis."
    },
    {
      "id": 3,
      "ticker": "BAVA",
      "company": "Bavarian Nordic",
      "headline": "Bavarian Nordic publishes financial calendar for 2026",
      "impact": 5,
      "summary": "Routine publication of the 2026 financial calendar with no new information beyond previously communicated reporting dates. No impact on estimates or investment thesis."
    }
  ]
}
"""


def _pass1_triage(news_items: list[dict], items_text: str,
                  system_prompt: str) -> list[dict]:
    """Pass 1 (Haiku): Rapidly classify and rank all news items by impact.

    Returns list of dicts with id, ticker, company, headline, impact (1-5).
    """
    logger.info("Pass 1 — Triage: sending %d items to %s …",
                len(news_items), CLAUDE_MODEL_TRIAGE)

    # Compact few-shot for triage only (no summaries to save tokens)
    triage_example = """
EXAMPLE:
Input: "Novo Nordisk reports Q4 2025 results above consensus, raises FY2026 guidance" → impact: 1
Input: "Danske Bank announces new share buyback programme of DKK 5bn" → impact: 2
Input: "Lundbeck appoints new CFO" → impact: 3
Input: "Coloplast publishes Annual Report 2025" → impact: 4
Input: "Bavarian Nordic publishes financial calendar for 2026" → impact: 5
"""

    prompt = f"""Classify these {len(news_items)} news items about Nasdaq Copenhagen-listed companies.

For EACH item, assign an impact score from 1 to 5 using this calibration:
- 1 = Highly impactful (>5% expected share price move: profit warnings, major M&A, CEO departures, material earnings surprises)
- 2 = Significant (2-5% move: results above/below expectations, meaningful strategic announcements, dividend changes)
- 3 = Moderate (1-2% move: in-line results, minor contract wins, board changes, routine updates)
- 4 = Low impact (<1% move: routine filings, annual reports when results known, minor updates)
- 5 = Minimal/Administrative (no price impact: calendar updates, AGM notices, routine insider trades)

{triage_example}

Return a JSON array where each element has ONLY these 5 fields (no other fields):
- "id": the item number
- "ticker": ticker symbol
- "company": company name
- "headline": original headline (verbatim)
- "impact": integer 1-5

Do NOT include summaries or any other fields. Keep output compact.
Order by impact (1 first). Respond ONLY with the JSON array.

NEWS ITEMS:
{items_text}"""

    client = anthropic.Anthropic()
    message = client.messages.create(
        model=CLAUDE_MODEL_TRIAGE,
        max_tokens=8192,
        system=system_prompt,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()
    logger.info("Pass 1 done: %d chars, input=%d output=%d tokens",
                len(response_text),
                message.usage.input_tokens, message.usage.output_tokens)

    result = _parse_claude_json(response_text)
    items = result if isinstance(result, list) else result.get("items", [])
    items.sort(key=lambda x: x.get("impact", 5))
    return items


def _pass2_deep_analysis(triaged_items: list[dict], items_text: str,
                         system_prompt: str) -> dict:
    """Pass 2 (Sonnet): Deep analysis on items with impact 1-3.

    Generates detailed investor-focused summaries and the day's editorial focus.
    Returns dict with "days_focus" and "items" (now with "summary" field added).
    """
    # Only send impact 1-3 items for deep analysis (skip 4-5)
    worthy_items = [i for i in triaged_items if i.get("impact", 5) <= 3]
    skipped = [i for i in triaged_items if i.get("impact", 5) > 3]

    if not worthy_items:
        logger.info("Pass 2 — No items with impact 1-3, skipping deep analysis.")
        return {"days_focus": "", "items": triaged_items}

    logger.info("Pass 2 — Deep analysis: sending %d items (impact 1-3) to %s …",
                len(worthy_items), CLAUDE_MODEL_ANALYSIS)

    # Build focused text for only the worthy items
    worthy_ids = {i["id"] for i in worthy_items}
    worthy_text = ""
    for line_block in items_text.split("\n\n"):
        if not line_block.strip():
            continue
        # Check if this block's ID is in worthy set
        for wid in worthy_ids:
            if line_block.strip().startswith(f"[{wid}]"):
                worthy_text += line_block + "\n\n"
                break

    prompt = f"""Analyse these {len(worthy_items)} important news items about companies listed on Nasdaq Copenhagen.
They have already been classified as impact 1-3 (most significant).

Write ALL output in English.

Return a JSON object with two keys:

1. "days_focus": An editorial paragraph of 3-4 sentences summarising the day's most important investment themes and market-moving events. Write in a professional, analytical tone suitable for institutional investors. Identify overarching trends, sector movements and significant corporate actions. Be specific — mention company names and quantify where possible.

2. "items": An array where EACH of the {len(worthy_items)} news items has:
   - "id": the item number (must match input)
   - "ticker": ticker symbol
   - "company": company name
   - "headline": the headline (keep as-is or clean up if needed)
   - "impact": the pre-assigned impact score (keep as-is)
   - "summary": 2-3 sentence analysis in English explaining why this matters for investors. Include: likely impact on consensus estimates or valuation, strategic significance, catalyst timing and any sector read-through. Be specific and quantitative where possible.

Here is an example of the expected quality and style:
{FEW_SHOT_EXAMPLE}

Sort by impact (1 first). Return ONLY the JSON object.

NEWS ITEMS:
{worthy_text}"""

    client = anthropic.Anthropic()
    message = client.messages.create(
        model=CLAUDE_MODEL_ANALYSIS,
        max_tokens=16384,
        system=system_prompt,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()
    logger.info("Pass 2 done: %d chars, input=%d output=%d tokens",
                len(response_text),
                message.usage.input_tokens, message.usage.output_tokens)

    result = _parse_claude_json(response_text)

    if isinstance(result, list):
        days_focus = ""
        analysed_items = result
    else:
        days_focus = result.get("days_focus", "")
        analysed_items = result.get("items", [])

    # Merge: use deep-analysed items for 1-3, keep triage-only for 4-5
    analysed_by_id = {i["id"]: i for i in analysed_items}
    merged = []
    for item in triaged_items:
        iid = item["id"]
        if iid in analysed_by_id:
            merged.append(analysed_by_id[iid])
        else:
            # Impact 4-5: add a minimal summary from triage
            item.setdefault("summary", "Routine announcement with minimal impact for investors.")
            merged.append(item)

    merged.sort(key=lambda x: x.get("impact", 5))

    return {"days_focus": days_focus, "items": merged}


def _pass3_inderes_rewrite(top_stories: list[dict], hca_items: list[dict],
                           days_focus: str) -> dict:
    """Pass 3 (Sonnet): Rewrite top stories and HCA client news in Inderes style.

    Takes the already-analysed items from Pass 2 and rewrites their summaries
    in Danish Inderes morning news style. Also rewrites the days_focus editorial.

    Returns dict with:
    - "days_focus": Danish editorial paragraph
    - "top_stories": items with rewritten summaries
    - "hca_items": items with rewritten summaries
    """
    items_to_rewrite = top_stories + hca_items
    if not items_to_rewrite:
        logger.info("Pass 3 — No items to rewrite, skipping Inderes rewrite.")
        return {"days_focus": days_focus, "top_stories": top_stories, "hca_items": hca_items}

    logger.info("Pass 3 — Inderes rewrite: sending %d items (%d top + %d HCA) to %s …",
                len(items_to_rewrite), len(top_stories), len(hca_items), CLAUDE_MODEL_REWRITE)

    # Load Inderes style prompt as system prompt
    inderes_prompt = _load_inderes_prompt()

    # Build compact input for the rewrite
    items_input = ""
    for item in items_to_rewrite:
        section = "TOP STORY" if item.get("impact", 5) == 1 else "HCA CLIENT NEWS"
        items_input += (
            f"[{item.get('id', '?')}] ({section}) {item.get('ticker', '')} — {item.get('company', '')}\n"
            f"Headline: {item.get('headline', '')}\n"
            f"Analysis: {item.get('summary', '')}\n"
            f"Link: {item.get('link', '')}\n\n"
        )

    prompt = f"""Rewrite the following {len(items_to_rewrite)} company news items in the Inderes morning news style.

You will receive news items with an analysis. Rewrite EACH news item in English in the Inderes style
described in your instructions. Preserve all facts and key figures from the analysis.

Also rewrite "days_focus" as a short English editorial summary (3-4 sentences) of the day's
most important themes.

CURRENT DAY'S FOCUS:
{days_focus}

NEWS ITEMS TO REWRITE:
{items_input}

Return a JSON object with:
1. "days_focus": The English editorial summary
2. "items": An array where EACH news item has:
   - "id": the item number (keep as-is)
   - "headline": headline in English (short and precise)
   - "summary": The rewritten English Inderes-style news text

Return ONLY the JSON object, no other text."""

    client = anthropic.Anthropic()
    message = client.messages.create(
        model=CLAUDE_MODEL_REWRITE,
        max_tokens=16384,
        system=inderes_prompt,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()
    logger.info("Pass 3 done: %d chars, input=%d output=%d tokens",
                len(response_text),
                message.usage.input_tokens, message.usage.output_tokens)

    result = _parse_claude_json(response_text)

    if isinstance(result, list):
        rewritten_days_focus = days_focus  # keep original if parsing fails
        rewritten_items = result
    else:
        rewritten_days_focus = result.get("days_focus", days_focus)
        rewritten_items = result.get("items", [])

    # Build lookup by ID for rewritten fields
    rewritten_by_id = {i["id"]: i for i in rewritten_items}

    # Apply rewritten summaries and headlines back to original items
    rewritten_top = []
    for item in top_stories:
        iid = item.get("id")
        if iid in rewritten_by_id:
            rw = rewritten_by_id[iid]
            if rw.get("summary"):
                item["summary"] = rw["summary"]
            if rw.get("headline"):
                item["headline"] = rw["headline"]
        rewritten_top.append(item)

    rewritten_hca = []
    for item in hca_items:
        iid = item.get("id")
        if iid in rewritten_by_id:
            rw = rewritten_by_id[iid]
            if rw.get("summary"):
                item["summary"] = rw["summary"]
            if rw.get("headline"):
                item["headline"] = rw["headline"]
        rewritten_hca.append(item)

    logger.info("Pass 3 complete: %d/%d items rewritten in Inderes style.",
                len(rewritten_by_id), len(items_to_rewrite))

    return {
        "days_focus": rewritten_days_focus,
        "top_stories": rewritten_top,
        "hca_items": rewritten_hca,
    }


def generate_briefing(news_items: list[dict]) -> dict:
    """Two-pass analysis of news items using Claude.

    Pass 1 (Haiku): Fast classification and impact ranking of all items.
    Pass 2 (Sonnet): Deep investor-focused analysis of impact 1-3 items only.

    Returns a dict with:
    - "days_focus": 3-4 sentence editorial on the day's themes
    - "items": ranked and summarised items as a list of dicts
    """
    logger.info("Starting two-pass analysis of %d news items …", len(news_items))

    # Load analyst system prompt
    system_prompt = _load_system_prompt()

    # Build items text and metadata
    items_text, item_metadata = _build_items_text(news_items)

    # ── Pass 1: Triage with Haiku ──
    triaged = _pass1_triage(news_items, items_text, system_prompt)

    # ── Pass 2: Deep analysis with Sonnet ──
    result = _pass2_deep_analysis(triaged, items_text, system_prompt)
    items = result.get("items", [])

    # Add link and published back from metadata
    for item in items:
        item_id = item.get("id")
        if item_id and item_id in item_metadata:
            item["link"] = item_metadata[item_id]["link"]
            item["published"] = item_metadata[item_id]["published"]

    # Ensure sorted by impact
    items.sort(key=lambda x: x.get("impact", 5))

    logger.info("Two-pass analysis complete: %d items scored, %d with deep analysis.",
                len(items), len([i for i in items if i.get("impact", 5) <= 3]))

    return {
        "days_focus": result.get("days_focus", ""),
        "items": items,
    }


# ─────────────────────────────────────────────────────────────────────────────
# HTML email formatting
# ─────────────────────────────────────────────────────────────────────────────

# HCA logo - using text for now (can be replaced with image URL later)
HCA_LOGO_HTML = '<span style="font-size:24px; font-weight:700; color:#ffffff; letter-spacing:1px;">HCA</span>'

# Disclaimer text
DISCLAIMER_TEXT = (
    "Disclaimer: HC Andersen Capital receives payment from mentioned companies "
    "under DigitalIR/corporate visibility subscription agreements."
)


def _format_change_cell(value: float | None, as_absolute: bool = False) -> str:
    """Format a change value as a colored HTML cell content.

    If as_absolute is True, display as absolute value (e.g. -0.02) instead of percentage.
    """
    if value is None:
        return '-'
    color = "#22c55e" if value >= 0 else "#ef4444"
    if as_absolute:
        sign = "" if value < 0 else ""
        return f'<span style="color:{color};">{value:+.2f}</span>'
    sign = "+" if value >= 0 else ""
    return f'<span style="color:{color};">{sign}{value:.1f}%</span>'


def _format_market_indices_table(indices: list[dict]) -> str:
    """Format market data as a grouped HTML table with 5 columns:
    Category, Instrument, Latest Price, Day Change, YTD*
    """
    # Group instruments by category while preserving order
    categories: list[str] = []
    cat_items: dict[str, list[dict]] = {}
    for idx in indices:
        cat = idx.get("category", "")
        if cat not in cat_items:
            categories.append(cat)
            cat_items[cat] = []
        cat_items[cat].append(idx)

    rows = ""
    for cat in categories:
        items = cat_items[cat]
        for i, idx in enumerate(items):
            name = idx.get("name", "")
            price = idx.get("price")
            fmt_type = idx.get("format_type", "price")
            day_pct = idx.get("day_change_pct")
            ytd_pct = idx.get("ytd_pct")

            # Format price column
            if price is not None:
                if fmt_type == "pct":
                    price_str = f"{price:.1f}%"
                else:
                    # Use Danish-style formatting: dot as thousands sep, comma as decimal
                    price_str = f"{price:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
            else:
                price_str = "-"

            # Format day change — bonds/FX use absolute values, rest use pct
            is_abs = fmt_type == "pct" and ("10-Year" in name or name == "USDDKK")
            day_str = _format_change_cell(day_pct, as_absolute=False)

            # Format YTD
            ytd_str = _format_change_cell(ytd_pct, as_absolute=False)

            # Category cell: only on first row of each category, with rowspan
            cat_cell = ""
            if i == 0:
                rowspan = len(items)
                cat_cell = (
                    f'<td rowspan="{rowspan}" style="padding:6px 10px; font-size:12px; '
                    f'font-weight:700; color:#1e3a5f; background:#f0f4f8; '
                    f'border-bottom:1px solid #e5e7eb; border-right:1px solid #e5e7eb; '
                    f'vertical-align:middle;">{cat}</td>'
                )

            rows += f"""
        <tr>
          {cat_cell}
          <td style="padding:5px 10px; font-size:12px; color:#374151; border-bottom:1px solid #e5e7eb;">
            {name}
          </td>
          <td align="right" style="padding:5px 10px; font-size:12px; font-weight:600; color:#1f2937; border-bottom:1px solid #e5e7eb;">
            {price_str}
          </td>
          <td align="right" style="padding:5px 10px; font-size:12px; font-weight:600; border-bottom:1px solid #e5e7eb;">
            {day_str}
          </td>
          <td align="right" style="padding:5px 10px; font-size:12px; font-weight:600; border-bottom:1px solid #e5e7eb;">
            {ytd_str}
          </td>
        </tr>
            """

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #e5e7eb; border-radius:6px; overflow:hidden;">
      <tr style="background:#1e3a5f;">
        <td style="padding:7px 10px; font-size:11px; font-weight:600; color:#ffffff; text-transform:uppercase;">Category</td>
        <td style="padding:7px 10px; font-size:11px; font-weight:600; color:#ffffff; text-transform:uppercase;">Instrument</td>
        <td align="right" style="padding:7px 10px; font-size:11px; font-weight:600; color:#ffffff; text-transform:uppercase;">Latest Price</td>
        <td align="right" style="padding:7px 10px; font-size:11px; font-weight:600; color:#ffffff; text-transform:uppercase;">Day Change</td>
        <td align="right" style="padding:7px 10px; font-size:11px; font-weight:600; color:#ffffff; text-transform:uppercase;">YTD*</td>
      </tr>
      {rows}
    </table>
    <div style="font-size:10px; color:#9ca3af; padding-top:4px;">* Year-to-date (%)</div>
    """


def _format_top_story(item: dict) -> str:
    """Format a top story (Impact 1) with full summary."""
    impact = item.get("impact", 1)
    colours = IMPACT_COLOURS.get(impact, IMPACT_COLOURS[1])

    published = item.get("published", "")
    timestamp_display = ""
    if published and published != "Unknown":
        try:
            dt = datetime.datetime.fromisoformat(published.replace("Z", "+00:00"))
            timestamp_display = dt.strftime("%d %b %H:%M")
        except (ValueError, TypeError):
            timestamp_display = published[:16] if len(published) > 16 else published

    link = item.get("link", "")
    link_html = ""
    if link:
        link_html = f'<a href="{link}" style="color:#2563eb; text-decoration:none; font-size:12px;">Read more →</a>'

    return f"""
    <tr>
      <td style="padding:16px; border-left:4px solid {colours['border']}; background:{colours['bg']}; margin-bottom:12px;">
        <table width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td style="font-size:11px; font-weight:600; color:{colours['border']}; text-transform:uppercase; padding-bottom:4px;">
              {colours['label']}
            </td>
            <td align="right" style="font-size:12px; color:#6b7280;">
              {item.get('ticker', '')} &bull; {timestamp_display}
            </td>
          </tr>
          <tr>
            <td colspan="2" style="font-size:16px; font-weight:700; color:#111827; padding-bottom:8px; line-height:1.3;">
              {item.get('company', '')} — {item.get('headline', '')}
            </td>
          </tr>
          <tr>
            <td colspan="2" style="font-size:14px; color:#374151; line-height:1.6; padding-bottom:8px;">
              {item.get('summary', '')}
            </td>
          </tr>
          <tr>
            <td colspan="2">{link_html}</td>
          </tr>
        </table>
      </td>
    </tr>
    <tr><td style="height:12px;"></td></tr>
    """


def _format_brief_item(item: dict) -> str:
    """Format a brief news item (Impact 2-3) with shorter display."""
    link = item.get("link", "")
    headline = item.get("headline", "")
    if link:
        headline_html = f'<a href="{link}" style="color:#1f2937; text-decoration:none;">{headline}</a>'
    else:
        headline_html = headline

    # Format timestamp
    published = item.get("published", "")
    timestamp_display = ""
    if published and published != "Unknown":
        try:
            dt = datetime.datetime.fromisoformat(published.replace("Z", "+00:00"))
            timestamp_display = dt.strftime("%d %b %H:%M")
        except (ValueError, TypeError):
            timestamp_display = published[:16] if len(published) > 16 else published

    return f"""
    <tr>
      <td style="padding:10px 12px; border-bottom:1px solid #f3f4f6;">
        <table width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td style="font-size:12px; font-weight:600; color:#6b7280; padding-bottom:2px;">
              {item.get('ticker', '')} — {item.get('company', '')}
            </td>
            <td align="right" style="font-size:11px; color:#9ca3af; padding-bottom:2px;">
              {timestamp_display}
            </td>
          </tr>
          <tr>
            <td colspan="2" style="font-size:14px; font-weight:500; color:#1f2937; line-height:1.4;">
              {headline_html}
            </td>
          </tr>
          <tr>
            <td colspan="2" style="font-size:13px; color:#4b5563; line-height:1.5; padding-top:4px;">
              {item.get('summary', '')[:200]}
            </td>
          </tr>
        </table>
      </td>
    </tr>
    """


def format_briefing_html(
    top_stories: list[dict],
    hca_items: list[dict],
    other_items: list[dict],
    days_focus: str,
    market_indices: list[dict],
    date_str: str,
) -> str:
    """Build the HTML email body with new format structure.

    Sections:
    1. Header with logo, date, edition number
    2. Market overview (4 indices)
    3. Day's Focus editorial
    4. Top Stories (Impact 1)
    5. HCA Client News (Impact 2-3)
    6. Stock News (Impact 2-3, non-HCA)
    7. Disclaimer & Footer
    """
    # Format edition as date
    try:
        dt = datetime.datetime.fromisoformat(date_str)
        edition_display = dt.strftime("%d %B %Y")
    except ValueError:
        edition_display = date_str

    # Current time for footer
    now = datetime.datetime.now()
    footer_time = now.strftime("%H:%M %d.%m.%Y")

    # Market overview section
    market_section = ""
    if market_indices:
        market_table = _format_market_indices_table(market_indices)
        market_section = f"""
        <tr>
          <td style="padding:20px 24px 12px 24px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
              <tr>
                <td style="font-size:14px; font-weight:600; color:#374151; padding-bottom:12px;">
                  Market Overview
                </td>
              </tr>
              <tr>
                <td>{market_table}</td>
              </tr>
            </table>
          </td>
        </tr>
        """

    # Day's Focus section
    focus_section = ""
    if days_focus:
        focus_section = f"""
        <tr>
          <td style="padding:16px 24px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#eff6ff; border-radius:6px; border-left:4px solid #3b82f6;">
              <tr>
                <td style="padding:16px;">
                  <div style="font-size:13px; font-weight:600; color:#1e40af; text-transform:uppercase; padding-bottom:8px;">
                    Day's Focus
                  </div>
                  <div style="font-size:14px; color:#1e3a5f; line-height:1.6;">
                    {days_focus}
                  </div>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        """

    # Top Stories section (Impact 1)
    top_stories_section = ""
    if top_stories:
        top_stories_html = "".join(_format_top_story(item) for item in top_stories)
        top_stories_section = f"""
        <tr>
          <td style="padding:16px 24px 8px 24px;">
            <div style="font-size:16px; font-weight:700; color:#111827; padding-bottom:4px;">
              Top Stories
            </div>
            <div style="font-size:12px; color:#6b7280; padding-bottom:12px;">
              High impact news for investors
            </div>
          </td>
        </tr>
        <tr>
          <td style="padding:0 24px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
              {top_stories_html}
            </table>
          </td>
        </tr>
        """

    # HCA Client News section
    hca_section = ""
    if hca_items:
        hca_items_html = "".join(_format_brief_item(item) for item in hca_items)
        hca_section = f"""
        <tr>
          <td style="padding:20px 24px 8px 24px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f0fdf4; border:1px solid #86efac; border-radius:6px;">
              <tr>
                <td style="padding:12px 16px; background:#22c55e; border-radius:5px 5px 0 0;">
                  <span style="font-size:14px; font-weight:700; color:#ffffff;">
                    HCA Client News
                  </span>
                  <span style="float:right; font-size:11px; color:#dcfce7;">
                    {len(hca_items)} items
                  </span>
                </td>
              </tr>
              <tr>
                <td style="padding:8px;">
                  <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    {hca_items_html}
                  </table>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        """

    # Stock News section (other Impact 2-3 items)
    aktie_section = ""
    if other_items:
        aktie_items_html = "".join(_format_brief_item(item) for item in other_items)
        aktie_section = f"""
        <tr>
          <td style="padding:20px 24px 8px 24px;">
            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #e5e7eb; border-radius:6px;">
              <tr>
                <td style="padding:12px 16px; background:#f9fafb; border-radius:5px 5px 0 0; border-bottom:1px solid #e5e7eb;">
                  <span style="font-size:14px; font-weight:600; color:#374151;">
                    Stock News
                  </span>
                  <span style="float:right; font-size:11px; color:#6b7280;">
                    {len(other_items)} items
                  </span>
                </td>
              </tr>
              <tr>
                <td style="padding:8px;">
                  <table width="100%" cellpadding="0" cellspacing="0" border="0">
                    {aktie_items_html}
                  </table>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        """

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; background:#f3f4f6; font-family:Arial, Helvetica, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f3f4f6;">
    <tr>
      <td align="center" style="padding:24px 16px;">
        <table width="640" cellpadding="0" cellspacing="0" border="0" style="background:#ffffff; border-radius:8px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.1);">

          <!-- Header -->
          <tr>
            <td style="background:#1e3a5f; padding:20px 24px;">
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="width:180px;">
                    {HCA_LOGO_HTML}
                  </td>
                  <td align="right" style="vertical-align:middle;">
                    <div style="font-size:20px; font-weight:700; color:#ffffff;">
                      Daily Briefing
                    </div>
                    <div style="font-size:13px; color:#93c5fd; padding-top:2px;">
                      {edition_display}
                    </div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          {market_section}
          {focus_section}
          {top_stories_section}
          {hca_section}
          {aktie_section}

          <!-- Disclaimer & Footer -->
          <tr>
            <td style="padding:20px 24px; background:#f9fafb; border-top:1px solid #e5e7eb;">
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="font-size:11px; color:#6b7280; line-height:1.5; padding-bottom:12px;">
                    {DISCLAIMER_TEXT}
                  </td>
                </tr>
                <tr>
                  <td style="font-size:11px; color:#9ca3af; border-top:1px solid #e5e7eb; padding-top:12px;">
                    Philip Coombes &bull; {footer_time}
                  </td>
                </tr>
              </table>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

    return html


def format_no_news_html(date_str: str) -> str:
    """HTML email for days with no matching news."""
    try:
        dt = datetime.datetime.fromisoformat(date_str)
        edition_display = dt.strftime("%d %B %Y")
    except ValueError:
        edition_display = date_str

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0; padding:0; background:#f3f4f6; font-family:Arial, Helvetica, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f3f4f6;">
    <tr>
      <td align="center" style="padding:24px 16px;">
        <table width="640" cellpadding="0" cellspacing="0" border="0"
               style="background:#ffffff; border-radius:8px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.1);">
          <tr>
            <td style="background:#1e3a5f; padding:20px 24px;">
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="width:180px;">
                    {HCA_LOGO_HTML}
                  </td>
                  <td align="right" style="vertical-align:middle;">
                    <div style="font-size:20px; font-weight:700; color:#ffffff;">
                      Daily Briefing
                    </div>
                    <div style="font-size:13px; color:#93c5fd; padding-top:2px;">
                      {edition_display}
                    </div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="padding:48px 24px; text-align:center;">
              <p style="font-size:16px; color:#4b5563; margin:0;">
                No relevant Nasdaq Copenhagen news in the past 48 hours.
              </p>
            </td>
          </tr>
          <tr>
            <td style="padding:16px 24px; background:#f9fafb; border-top:1px solid #e5e7eb;">
              <div style="font-size:11px; color:#6b7280;">{DISCLAIMER_TEXT}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def format_failure_html(date_str: str, error: str) -> str:
    """HTML email for script failures."""
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0; padding:0; background:#fff5f5; font-family:Arial, Helvetica, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fff5f5;">
    <tr>
      <td align="center" style="padding:24px 16px;">
        <table width="600" cellpadding="0" cellspacing="0" border="0"
               style="background:#ffffff; border-radius:8px; overflow:hidden;">
          <tr>
            <td style="background:#c53030; padding:20px 24px;">
              <span style="font-size:20px; font-weight:700; color:#ffffff;">
                HCA Briefing — FAILURE
              </span>
              <span style="float:right; font-size:14px; color:#fed7d7;">{date_str}</span>
            </td>
          </tr>
          <tr>
            <td style="padding:24px;">
              <p style="font-size:14px; color:#c53030; font-weight:600;">
                The daily briefing script failed with the following error:
              </p>
              <pre style="background:#fff5f5; border:1px solid #feb2b2; padding:12px;
                          border-radius:4px; font-size:13px; color:#742a2a;
                          white-space:pre-wrap; word-break:break-word;">
{error}
              </pre>
              <p style="font-size:13px; color:#718096; margin-top:16px;">
                Check the GitHub Actions run for full logs.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# Email delivery
# ─────────────────────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str) -> None:
    """Send an HTML email via Gmail SMTP."""
    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_password = os.environ.get("SMTP_PASSWORD", "").strip()
    recipients_raw = os.environ.get("BRIEFING_RECIPIENTS", "").strip()

    if not all([smtp_user, smtp_password, recipients_raw]):
        raise RuntimeError(
            "Missing email configuration. Set SMTP_USER, SMTP_PASSWORD, "
            "and BRIEFING_RECIPIENTS environment variables."
        )

    # Handle both comma-separated and newline-separated email lists
    # Also strip any whitespace/newlines from each address
    recipients_raw = recipients_raw.replace("\n", ",").replace("\r", ",")
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    logger.info("Sending email to %s via %s …", recipients, smtp_user)
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipients, msg.as_string())

    logger.info("Email sent successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="HCA Daily Briefing")
    parser.add_argument(
        "--email", action="store_true", help="Send briefing via email"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and analyse but do not send email (print HTML to stdout)",
    )
    args = parser.parse_args()

    today = datetime.date.today().isoformat()

    try:
        # 1. Load tickers and HCA covered companies
        tickers = load_tickers()
        logger.info("Loaded %d Copenhagen tickers.", len(tickers))

        hca_data = load_hca_covered_companies()

        # 2. Fetch market indices from Yahoo Finance
        market_indices = fetch_market_indices()

        # 3. Fetch news from Nasdaq API
        news_items = fetch_news(tickers)

        if not news_items:
            logger.info("No matching news found.")
            subject = f"HCA Daily Briefing — {today} (0 items)"
            html = format_no_news_html(today)

            if args.dry_run:
                print(html)
            elif args.email:
                send_email(subject, html)
            return

        # 4. Summarise with Claude (returns dict with days_focus and items)
        briefing_result = generate_briefing(news_items)
        days_focus = briefing_result.get("days_focus", "")
        ranked = briefing_result.get("items", [])

        # 5. Organize news by new structure:
        #    - Impact 1: Top Stories (all items)
        #    - Impact 2-3 HCA companies: HCA Client News
        #    - Impact 2-3 other: Stock News
        #    - Impact 4-5: Ignored

        top_stories = []      # Impact 1
        hca_items = []        # Impact 2-3, HCA covered
        other_items = []      # Impact 2-3, non-HCA

        for item in ranked:
            impact = item.get("impact", 5)

            # Skip impact 4-5
            if impact >= 4:
                continue

            if impact == 1:
                # All impact 1 go to top stories
                top_stories.append(item)
            elif impact in (2, 3):
                # Impact 2-3: separate HCA vs other
                if is_hca_company(item, hca_data):
                    hca_items.append(item)
                else:
                    other_items.append(item)

        logger.info(
            "Organized news: %d top stories, %d HCA client, %d other (ignored %d low-impact)",
            len(top_stories),
            len(hca_items),
            len(other_items),
            len([i for i in ranked if i.get("impact", 5) >= 4]),
        )

        # 5b. Pass 3: Rewrite top stories + HCA client news in Inderes style (Danish)
        if top_stories or hca_items:
            try:
                rewrite_result = _pass3_inderes_rewrite(top_stories, hca_items, days_focus)
                top_stories = rewrite_result.get("top_stories", top_stories)
                hca_items = rewrite_result.get("hca_items", hca_items)
                days_focus = rewrite_result.get("days_focus", days_focus)
            except Exception as exc:
                logger.warning("Pass 3 (Inderes rewrite) failed, using original summaries: %s", exc)

        # 6. Format HTML with new structure
        total_shown = len(top_stories) + len(hca_items) + len(other_items)
        subject = f"HCA Daily Briefing — {today}"
        if top_stories:
            subject += f" ({len(top_stories)} top stories)"

        html = format_briefing_html(
            top_stories=top_stories,
            hca_items=hca_items,
            other_items=other_items,
            days_focus=days_focus,
            market_indices=market_indices,
            date_str=today,
        )

        # 7. Save briefing to output directory (for artifacts)
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"briefing_{today}.html"
        output_file.write_text(html)
        logger.info("Saved briefing to %s", output_file)

        # 8. Deliver
        if args.dry_run:
            print(html)
        elif args.email:
            send_email(subject, html)

        logger.info("Done. %d items shown in briefing.", total_shown)

    except Exception as exc:
        logger.error("FATAL: %s", exc, exc_info=True)

        # Attempt to send failure notification
        if args.email:
            try:
                fail_subject = f"HCA Briefing FAILED — {today}"
                fail_html = format_failure_html(today, str(exc))
                send_email(fail_subject, fail_html)
            except Exception as mail_exc:
                logger.error("Could not send failure email: %s", mail_exc)

        sys.exit(1)


if __name__ == "__main__":
    main()
