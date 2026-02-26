import asyncio
import logging
from typing import Any

import httpx

from . import config, downloader, parser
from .api_client import GeminiClient
from .storage import Storage

logger = logging.getLogger(__name__)


async def run(gemini: GeminiClient, storage: Storage) -> None:
    done = storage.get_done_set()

    # Build work list: all (ticker, year) not yet extracted, ordered by year desc then tier
    work_items: list[tuple[str, str, int]] = []
    for year in config.YEARS:
        for tier_name, tickers in config.COMPANIES.items():
            for ticker in tickers:
                if (ticker, year) not in done:
                    work_items.append((ticker, tier_name, year))

    logger.info(f"Pipeline starting: {len(work_items)} items pending")

    work_q: asyncio.Queue[tuple[str, str, int] | None] = asyncio.Queue()
    api_q: asyncio.Queue[tuple[str, str, int, dict[str, Any]] | None] = asyncio.Queue(maxsize=30)

    for item in work_items:
        await work_q.put(item)
    for _ in range(config.SEC_WORKERS):
        await work_q.put(None)  # sentinel

    sec_semaphore = asyncio.Semaphore(config.SEC_RATE_LIMIT)

    async def download_worker():
        async with downloader.build_client() as client:
            while True:
                item = await work_q.get()
                if item is None:
                    break  # sentinel: no task_done, queue join not used
                ticker, tier, year = item
                try:
                    path = await downloader.download_filing(client, sec_semaphore, ticker, year)
                    if path is None:
                        storage.log_status(ticker, year, "no_filing")
                        continue

                    sections = parser.extract_sections(path)
                    storage.log_status(ticker, year, "parsed")
                    await api_q.put((ticker, tier, year, sections))
                except Exception as e:
                    logger.error(f"[{ticker}/{year}] Download/parse error: {e}")
                    storage.log_status(ticker, year, "failed", str(e))
                finally:
                    work_q.task_done()

    async def api_worker():
        while True:
            item = await api_q.get()
            if item is None:
                api_q.task_done()
                break
            ticker, tier, year, sections = item
            try:
                mda = sections.get("mda_text")
                risk = sections.get("risk_text")
                ai_flag = parser.has_ai_exposure(mda or "", risk or "")
                form_type = "20-F" if ticker in config.FOREIGN_FILERS else "10-K"

                insights = await gemini.extract(
                    ticker=ticker,
                    year=year,
                    mda=mda,
                    risk=risk,
                    ai_flag=ai_flag,
                    form_type=form_type,
                )
                storage.upsert_insight(
                    ticker=ticker,
                    tier=tier,
                    year=year,
                    data=insights,
                    mda_chars=sections.get("mda_chars", 0),
                    risk_chars=sections.get("risk_chars", 0),
                    model_name=gemini.model_name,
                )
                storage.append_jsonl(ticker, year, insights)
                storage.log_status(ticker, year, "extracted")
                logger.info(f"[{ticker}/{year}] Extracted OK (sentiment={insights.get('mda_sentiment_score')})")
            except Exception as e:
                logger.error(f"[{ticker}/{year}] API worker error: {e}")
                storage.log_status(ticker, year, "failed", str(e))
            finally:
                api_q.task_done()

    # Launch workers
    dl_tasks = [asyncio.create_task(download_worker()) for _ in range(config.SEC_WORKERS)]
    api_tasks = [asyncio.create_task(api_worker()) for _ in range(config.VERTEX_WORKERS)]

    try:
        # Wait for all downloads to finish
        await asyncio.gather(*dl_tasks)
    finally:
        # Always signal API workers, even if download stage crashed
        for _ in range(config.VERTEX_WORKERS):
            await api_q.put(None)

    # Wait for API workers
    await asyncio.gather(*api_tasks)

    # Summary
    done_now = storage.get_done_set()
    newly = len(done_now) - len(done)
    logger.info(f"Pipeline complete. New extractions: {newly} | Total in DB: {len(done_now)}")
