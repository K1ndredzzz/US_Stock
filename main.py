import asyncio
import logging
import os
import sys

from src import config
from src.api_client import probe_and_build_client
from src.storage import Storage
from src import orchestrator

# Apply proxy before any SDK initialization
os.environ.setdefault("HTTPS_PROXY", config.HTTPS_PROXY)
os.environ.setdefault("HTTP_PROXY", config.HTTPS_PROXY)


def _setup_logging() -> None:
    config.LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s %(levelname)-8s %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(config.LOG_PATH), encoding="utf-8"),
        ],
    )


async def _main() -> None:
    _setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=== US Stock Earnings Insight Pipeline ===")
    logger.info(f"Companies: {len(config.ALL_TICKERS)} | Years: {config.YEARS}")

    logger.info("Probing Vertex AI model availability...")
    gemini = await probe_and_build_client()

    storage = Storage()
    try:
        await orchestrator.run(gemini, storage)
    finally:
        storage.close()
        logger.info(f"Results: {config.DB_PATH}")
        logger.info(f"Backup:  {config.JSONL_PATH}")


if __name__ == "__main__":
    asyncio.run(_main())
