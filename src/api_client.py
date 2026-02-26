import asyncio
import json
import logging
import random
import re
from typing import Any

from google import genai
from google.genai import types

from . import config

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are a financial analyst AI specialized in extracting structured intelligence from SEC 10-K annual filings. Your output is consumed programmatically — JSON validity is a hard requirement.

EXTRACTION RULES:
1. Ground every field strictly in the source text. Do not infer or fabricate.
2. If evidence for a field is absent, use null (not empty string, not "N/A").
3. mda_sentiment_score: integer 1-10 based on management tone. 1=deeply cautionary, 10=highly optimistic.
4. capex_guidance_tone: exactly one of ["aggressive", "conservative", "reducing"]. Use "conservative" when ambiguous.
5. macro_concerns: exactly 3 items. Pad with null if fewer signals found.
6. All string fields: concise (under 120 words), factual, third-person.
7. Output ONLY the JSON object. No preamble, no explanation, no markdown fences."""

_USER_TEMPLATE = """Analyze the following SEC filing sections and return a single JSON object.

COMPANY: {ticker}
FISCAL YEAR: {year}
FILING TYPE: {form_type}
HAS AI EXPOSURE: {ai_flag}

---BEGIN ITEM 7 (MD&A)---
{mda}
---END ITEM 7---

---BEGIN ITEM 1A (RISK FACTORS)---
{risk}
---END ITEM 1A---

REQUIRED JSON SCHEMA:
{{
  "ticker": "{ticker}",
  "year": {year},
  "filing_type": "{form_type}",
  "ai_investment_focus": null,
  "ai_monetization_status": null,
  "capex_guidance_tone": "conservative",
  "china_exposure_risk": null,
  "supply_chain_bottlenecks": null,
  "restructuring_plans": null,
  "efficiency_initiatives": null,
  "mda_sentiment_score": 5,
  "macro_concerns": [null, null, null],
  "growing_segments": null,
  "shrinking_segments": null
}}

Return ONLY the JSON object."""

_NO_AI_NOTE = "\nNOTE: This company has no apparent AI exposure. Set ai_investment_focus and ai_monetization_status to null."

_CAPEX_VALUES = {"aggressive", "conservative", "reducing"}
_MAX_RETRIES = 4


def _build_null_skeleton(ticker: str, year: int, form_type: str) -> dict[str, Any]:
    return {
        "ticker": ticker.upper(),
        "year": year,
        "filing_type": form_type,
        "ai_investment_focus": None,
        "ai_monetization_status": None,
        "capex_guidance_tone": "conservative",
        "china_exposure_risk": None,
        "supply_chain_bottlenecks": None,
        "restructuring_plans": None,
        "efficiency_initiatives": None,
        "mda_sentiment_score": 5,
        "macro_concerns": [None, None, None],
        "growing_segments": None,
        "shrinking_segments": None,
    }


def _validate(data: dict[str, Any], ticker: str, year: int, form_type: str) -> dict[str, Any]:
    required = [
        "ai_investment_focus", "ai_monetization_status", "capex_guidance_tone",
        "china_exposure_risk", "supply_chain_bottlenecks", "restructuring_plans",
        "efficiency_initiatives", "mda_sentiment_score", "macro_concerns",
        "growing_segments", "shrinking_segments",
    ]
    missing = [f for f in required if f not in data]
    if missing:
        raise ValueError(f"Missing fields: {missing}")

    if isinstance(data.get("year"), str):
        data["year"] = int(data["year"])
    if isinstance(data.get("mda_sentiment_score"), str):
        data["mda_sentiment_score"] = int(data["mda_sentiment_score"])

    if data.get("capex_guidance_tone") not in _CAPEX_VALUES:
        data["capex_guidance_tone"] = "conservative"

    score = data.get("mda_sentiment_score", 5)
    data["mda_sentiment_score"] = max(1, min(10, int(score)))

    mc = data.get("macro_concerns")
    if not isinstance(mc, list):
        data["macro_concerns"] = [None, None, None]
    elif len(mc) < 3:
        data["macro_concerns"] = (mc + [None, None, None])[:3]
    else:
        data["macro_concerns"] = mc[:3]

    data["ticker"] = ticker.upper()
    data["year"] = year
    data["filing_type"] = form_type
    return data


def _parse_response(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r'^```(?:json)?\n?', '', text)
        text = re.sub(r'\n?```$', '', text.strip())
    return json.loads(text)


class GeminiClient:
    def __init__(self, client: genai.Client, model_name: str):
        self.model_name = model_name
        self._client = client
        logger.info(f"Initialized Gemini client with model: {model_name}")

    async def extract(
        self,
        ticker: str,
        year: int,
        mda: str | None,
        risk: str | None,
        ai_flag: bool,
        form_type: str = "10-K",
    ) -> dict[str, Any]:
        mda_text = mda or "(not available)"
        risk_text = risk or "(not available)"
        ai_note = "" if ai_flag else _NO_AI_NOTE

        user_prompt = _USER_TEMPLATE.format(
            ticker=ticker,
            year=year,
            form_type=form_type,
            ai_flag="YES" if ai_flag else "NO",
            mda=mda_text,
            risk=risk_text,
        ) + ai_note

        gen_config = types.GenerateContentConfig(
            system_instruction=_SYSTEM_PROMPT,
            temperature=0.1,
            top_p=0.8,
            max_output_tokens=2048,
            response_mime_type="application/json",
        )

        last_err: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            prompt = user_prompt
            if attempt > 0 and last_err:
                prompt += f"\n\nPrevious attempt failed: {last_err}. Return ONLY valid JSON."

            try:
                resp = await self._client.aio.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=gen_config,
                )
                data = _parse_response(resp.text)
                return _validate(data, ticker, year, form_type)

            except Exception as e:
                err_str = str(e).lower()
                last_err = e
                if "quota" in err_str or "resource_exhausted" in err_str or "429" in err_str:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"[{ticker}/{year}] Rate limit, retry in {wait:.1f}s")
                    await asyncio.sleep(wait)
                elif "unavailable" in err_str or "503" in err_str:
                    wait = 5 * (attempt + 1)
                    logger.warning(f"[{ticker}/{year}] Service unavailable, retry in {wait}s")
                    await asyncio.sleep(wait)
                elif isinstance(e, (json.JSONDecodeError, ValueError)):
                    logger.warning(f"[{ticker}/{year}] Parse error attempt {attempt+1}: {e}")
                    await asyncio.sleep(2)
                else:
                    logger.warning(f"[{ticker}/{year}] Error attempt {attempt+1}: {e}")
                    await asyncio.sleep(3)

        logger.error(f"[{ticker}/{year}] All {_MAX_RETRIES} attempts failed, using null skeleton")
        return _build_null_skeleton(ticker, year, form_type)


async def probe_and_build_client() -> GeminiClient:
    client = genai.Client(api_key=config.GEMINI_API_KEY)
    model_priority = [config.GEMINI_MODEL] + config.GEMINI_MODEL_FALLBACKS
    for model_name in model_priority:
        try:
            resp = await client.aio.models.generate_content(
                model=model_name,
                contents="Return the word OK.",
                config=types.GenerateContentConfig(max_output_tokens=5),
            )
            logger.info(f"Model probe succeeded: {model_name} → '{resp.text.strip()}'")
            return GeminiClient(client, model_name)
        except Exception as e:
            logger.warning(f"Model {model_name} unavailable: {e}")
    raise RuntimeError("No Gemini model available")
