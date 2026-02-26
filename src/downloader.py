import asyncio
import logging
from pathlib import Path

import httpx

from . import config

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "FinResearchBot contact@research.local",
    "Accept-Encoding": "gzip, deflate",
}

_cik_cache: dict[str, str] = {}
_cik_lock: asyncio.Lock | None = None


def _get_cik_lock() -> asyncio.Lock:
    global _cik_lock
    if _cik_lock is None:
        _cik_lock = asyncio.Lock()
    return _cik_lock


async def _load_cik_map(client: httpx.AsyncClient) -> dict[str, str]:
    if _cik_cache:
        return _cik_cache
    async with _get_cik_lock():
        if _cik_cache:  # re-check after acquiring lock
            return _cik_cache
        resp = await client.get(
            config.SEC_CIK_URL,
            headers=_HEADERS,
        )
        resp.raise_for_status()
        data = resp.json()
        for entry in data.values():
            ticker = entry["ticker"].upper()
            cik = str(entry["cik_str"]).zfill(10)
            _cik_cache[ticker] = cik
    return _cik_cache


async def _get_filing_url(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    cik: str,
    ticker: str,
    year: int,
) -> str | None:
    form_type = "20-F" if ticker in config.FOREIGN_FILERS else "10-K"
    url = f"{config.SEC_BASE_URL}/submissions/CIK{cik}.json"
    async with semaphore:
        resp = await client.get(url, headers=_HEADERS)
    resp.raise_for_status()
    submissions = resp.json()

    filings = submissions.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    dates = filings.get("filingDate", [])
    accessions = filings.get("accessionNumber", [])
    primary_docs = filings.get("primaryDocument", [])

    # 10-K for fiscal year YYYY is filed between Jan-Apr of YYYY+1
    # 20-F has extended deadline to Jun 30 of YYYY+1
    start = f"{year + 1}-01-01"
    end = f"{year + 1}-06-30" if ticker in config.FOREIGN_FILERS else f"{year + 1}-04-30"

    # Helper to search in a filings dict
    def _search_in_filings(forms_list, dates_list, accessions_list, docs_list):
        for form, date, accession, doc in zip(forms_list, dates_list, accessions_list, docs_list):
            if form not in (form_type, f"{form_type}/A"):
                continue
            if not (start <= date <= end):
                continue
            acc_clean = accession.replace("-", "")
            try:
                cik_int = int(cik)
            except ValueError:
                logger.error(f"[{ticker}] Malformed CIK: {cik!r}")
                return None
            return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{doc}"
        return None

    # Search in recent
    result = _search_in_filings(forms, dates, accessions, primary_docs)
    if result:
        return result

    # Fallback: check paginated files
    files = submissions.get("filings", {}).get("files", [])
    for file_info in files:
        file_name = file_info.get("name")
        if not file_name:
            continue

        # Check if this file's date range overlaps with our target window
        file_from = file_info.get("filingFrom", "")
        file_to = file_info.get("filingTo", "")
        if file_to < start or file_from > end:
            continue  # No overlap

        # Fetch the paginated file
        file_url = f"{config.SEC_BASE_URL}/submissions/{file_name}"
        async with semaphore:
            file_resp = await client.get(file_url, headers=_HEADERS)
        if file_resp.status_code != 200:
            continue

        file_data = file_resp.json()
        result = _search_in_filings(
            file_data.get("form", []),
            file_data.get("filingDate", []),
            file_data.get("accessionNumber", []),
            file_data.get("primaryDocument", []),
        )
        if result:
            return result

    logger.debug(f"[{ticker}/{year}] No {form_type} found in submissions (checked {len(files)} paginated files)")
    return None


async def download_filing(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    ticker: str,
    year: int,
) -> Path | None:
    floor = config.IPO_YEAR_FLOOR.get(ticker)
    if floor and year < floor:
        logger.debug(f"[{ticker}/{year}] Skipped: pre-IPO year")
        return None

    dest_dir = config.DATA_DIR / ticker / str(year)
    dest = dest_dir / "filing.htm"
    if dest.exists() and dest.stat().st_size > 1024:
        logger.debug(f"[{ticker}/{year}] Cache hit")
        return dest

    cik_map = await _load_cik_map(client)
    cik = cik_map.get(ticker.upper())
    if not cik:
        logger.warning(f"[{ticker}] CIK not found")
        return None

    filing_url = await _get_filing_url(client, semaphore, cik, ticker, year)
    if not filing_url:
        logger.warning(f"[{ticker}/{year}] Filing URL not found")
        return None

    async with semaphore:
        resp = await client.get(filing_url, headers=_HEADERS, follow_redirects=True)
    if resp.status_code != 200:
        logger.warning(f"[{ticker}/{year}] HTTP {resp.status_code} for {filing_url}")
        return None

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(resp.content)
    logger.info(f"[{ticker}/{year}] Downloaded {len(resp.content):,} bytes")
    return dest


from contextlib import asynccontextmanager
from collections.abc import AsyncIterator


@asynccontextmanager
async def build_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(timeout=60.0, limits=httpx.Limits(max_connections=20)) as client:
        yield client
