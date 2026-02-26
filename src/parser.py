import re
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

_ITEM_7 = re.compile(
    r'(?:^|\n)(?:ITEM|Item)\.?\s+7(?!A)[.\s\-—]*(?:MANAGEMENT[\'S]*\s+DISCUSSION|MD&A)?[.\s]*\n',
    re.IGNORECASE | re.MULTILINE,
)
_ITEM_7A = re.compile(
    r'(?:^|\n)(?:ITEM|Item)\s+7A[.\s]*(?:QUANTITATIVE|Quantitative)?[.\s]*\n',
    re.IGNORECASE | re.MULTILINE,
)
_ITEM_8 = re.compile(
    r'(?:^|\n)(?:ITEM|Item)\s+8[.\s]*(?:FINANCIAL\s+STATEMENTS?|Financial\s+Statements?)?[.\s]*\n',
    re.IGNORECASE | re.MULTILINE,
)
_ITEM_1A = re.compile(
    r'(?:^|\n)(?:ITEM|Item)\s+1A[.\s]*(?:RISK\s+FACTORS?|Risk\s+Factors?)?[.\s]*\n',
    re.IGNORECASE | re.MULTILINE,
)
_ITEM_2 = re.compile(
    r'(?:^|\n)(?:ITEM|Item)\s+2[.\s]*(?:PROPERTIES?|Properties?)?[.\s]*\n',
    re.IGNORECASE | re.MULTILINE,
)

_MAX_MDA_CHARS = 280_000   # ~80k tokens at 3.5 chars/token
_MAX_RISK_CHARS = 140_000  # ~40k tokens at 3.5 chars/token


def _strip_html(raw: bytes) -> str:
    soup = BeautifulSoup(raw, "lxml")
    for tag in soup(["script", "style", "table"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]{2,}', ' ', text)
    return text.strip()


def _best_match(text: str, pattern: re.Pattern) -> re.Match | None:
    matches = list(pattern.finditer(text))
    if not matches:
        return None
    for m in reversed(matches):
        if len(text[m.end():]) > 2000:
            return m
    return matches[-1]


def _extract_between(
    text: str,
    start_pat: re.Pattern,
    end_pats: list[re.Pattern],
) -> Optional[str]:
    m = _best_match(text, start_pat)
    if not m:
        return None
    start = m.end()
    end = len(text)
    for ep in end_pats:
        em = ep.search(text, start)
        if em and em.start() < end:
            end = em.start()
    return text[start:end].strip() or None


def _smart_truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    marker = "\n\n[...TRUNCATED...]\n\n"
    budget = max_chars - len(marker)
    front = int(budget * 0.70)
    rear = budget - front
    return text[:front] + marker + text[-rear:]


def extract_sections(filepath: Path) -> dict[str, Optional[str]]:
    raw = filepath.read_bytes()
    text = _strip_html(raw)

    # Skip TOC region (capped at 50k chars to avoid over-skipping)
    toc_skip = min(len(text) // 7, 50_000)
    body = text[toc_skip:]

    mda = _extract_between(body, _ITEM_7, [_ITEM_7A, _ITEM_8])
    risk = _extract_between(body, _ITEM_1A, [_ITEM_2, _ITEM_7])

    return {
        "mda_text": _smart_truncate(mda, _MAX_MDA_CHARS) if mda else None,
        "risk_text": _smart_truncate(risk, _MAX_RISK_CHARS) if risk else None,
        "mda_chars": len(mda) if mda else 0,
        "risk_chars": len(risk) if risk else 0,
    }


_AI_TERMS = frozenset([
    "artificial intelligence", "machine learning", "generative ai",
    "large language model", "llm", "neural network", "gpu cluster",
    "ai infrastructure", "foundation model", "deep learning",
])


def has_ai_exposure(mda: str, risk: str, threshold: int = 3) -> bool:
    combined = ((mda or "") + (risk or "")).lower()
    return sum(1 for t in _AI_TERMS if t in combined) >= threshold
