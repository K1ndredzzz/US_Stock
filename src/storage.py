import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from . import config

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS filing_insights (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker                   TEXT NOT NULL,
    tier                     TEXT NOT NULL,
    fiscal_year              INTEGER NOT NULL,
    filing_type              TEXT DEFAULT '10-K',
    processed_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ai_investment_focus      TEXT,
    ai_monetization_status   TEXT,
    capex_guidance_tone      TEXT,
    china_exposure_risk      TEXT,
    supply_chain_bottlenecks TEXT,
    restructuring_plans      TEXT,
    efficiency_initiatives   TEXT,
    mda_sentiment_score      INTEGER,
    macro_concerns           TEXT,
    growing_segments         TEXT,
    shrinking_segments       TEXT,
    mda_char_count           INTEGER,
    risk_char_count          INTEGER,
    extraction_model         TEXT,
    UNIQUE(ticker, fiscal_year)
);

CREATE TABLE IF NOT EXISTS processing_log (
    ticker          TEXT NOT NULL,
    fiscal_year     INTEGER NOT NULL,
    status          TEXT NOT NULL,
    error_message   TEXT,
    attempts        INTEGER DEFAULT 0,
    last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_tier_year  ON filing_insights(tier, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_sentiment  ON filing_insights(mda_sentiment_score);
CREATE INDEX IF NOT EXISTS idx_china_risk ON filing_insights(china_exposure_risk);
"""


def _to_str(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


class Storage:
    def __init__(self):
        config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        config.JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(config.DB_PATH), check_same_thread=False)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._jsonl = open(config.JSONL_PATH, "a", encoding="utf-8")
        logger.info(f"Storage initialized: {config.DB_PATH}")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def log_status(
        self,
        ticker: str,
        year: int,
        status: str,
        error: str | None = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO processing_log (ticker, fiscal_year, status, error_message, attempts)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(ticker, fiscal_year) DO UPDATE SET
                status = excluded.status,
                error_message = excluded.error_message,
                attempts = processing_log.attempts + 1,
                last_attempt_at = CURRENT_TIMESTAMP
            """,
            (ticker, year, status, error),
        )
        self._conn.commit()

    def upsert_insight(
        self,
        ticker: str,
        tier: str,
        year: int,
        data: dict[str, Any],
        mda_chars: int = 0,
        risk_chars: int = 0,
        model_name: str = "",
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO filing_insights (
                ticker, tier, fiscal_year, filing_type,
                ai_investment_focus, ai_monetization_status, capex_guidance_tone,
                china_exposure_risk, supply_chain_bottlenecks,
                restructuring_plans, efficiency_initiatives,
                mda_sentiment_score, macro_concerns,
                growing_segments, shrinking_segments,
                mda_char_count, risk_char_count, extraction_model
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(ticker, fiscal_year) DO UPDATE SET
                filing_type              = excluded.filing_type,
                ai_investment_focus      = excluded.ai_investment_focus,
                ai_monetization_status   = excluded.ai_monetization_status,
                capex_guidance_tone      = excluded.capex_guidance_tone,
                china_exposure_risk      = excluded.china_exposure_risk,
                supply_chain_bottlenecks = excluded.supply_chain_bottlenecks,
                restructuring_plans      = excluded.restructuring_plans,
                efficiency_initiatives   = excluded.efficiency_initiatives,
                mda_sentiment_score      = excluded.mda_sentiment_score,
                macro_concerns           = excluded.macro_concerns,
                growing_segments         = excluded.growing_segments,
                shrinking_segments       = excluded.shrinking_segments,
                mda_char_count           = excluded.mda_char_count,
                risk_char_count          = excluded.risk_char_count,
                extraction_model         = excluded.extraction_model,
                processed_at             = CURRENT_TIMESTAMP
            """,
            (
                ticker, tier, year, data.get("filing_type", "10-K"),
                _to_str(data.get("ai_investment_focus")),
                _to_str(data.get("ai_monetization_status")),
                _to_str(data.get("capex_guidance_tone")),
                _to_str(data.get("china_exposure_risk")),
                _to_str(data.get("supply_chain_bottlenecks")),
                _to_str(data.get("restructuring_plans")),
                _to_str(data.get("efficiency_initiatives")),
                data.get("mda_sentiment_score"),
                _to_str(data.get("macro_concerns")),
                _to_str(data.get("growing_segments")),
                _to_str(data.get("shrinking_segments")),
                mda_chars, risk_chars, model_name,
            ),
        )
        self._conn.commit()

    def append_jsonl(self, ticker: str, year: int, data: dict[str, Any]) -> None:
        self._jsonl.write(json.dumps(data, ensure_ascii=False) + "\n")
        self._jsonl.flush()

    def get_done_set(self) -> set[tuple[str, int]]:
        rows = self._conn.execute(
            "SELECT ticker, fiscal_year FROM processing_log WHERE status = 'extracted'"
        ).fetchall()
        return {(r[0], r[1]) for r in rows}

    def close(self) -> None:
        self._conn.close()
        self._jsonl.close()
