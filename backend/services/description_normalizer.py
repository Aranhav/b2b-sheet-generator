"""Normalize product descriptions for Gaia classification caching.

Two-tier normalization:
  1. Regex: fast, deterministic — strips quantities, prices, references, special chars
  2. LLM (Claude Haiku): semantic — extracts clean trade-standard product name

The LLM step is optional (async, may fail) and produces a superior description
for Gaia classification. The regex result is always used as the cache key.
"""
from __future__ import annotations

import logging
import re

import anthropic

from backend.config import ANTHROPIC_API_KEY, LLM_MODEL_TEXT

logger = logging.getLogger(__name__)

_QTY_PATTERN = re.compile(
    r"\b\d+\s*(?:pcs?|pieces?|units?|nos?|sets?|pairs?|kgs?|gms?|lbs?|mts?|ltrs?)\b",
    re.IGNORECASE,
)
_PRICE_PATTERN = re.compile(
    r"(?:(?:USD|INR|EUR|GBP|\$|₹|€|£)\s*[\d,]+(?:\.\d+)?)|(?:[\d,]+(?:\.\d+)?\s*(?:USD|INR|EUR|GBP))",
    re.IGNORECASE,
)
_REF_PATTERN = re.compile(
    r"(?:PO|REF|SO|SKU|ITEM|LOT|BATCH)\s*[#:]\s*\S+",
    re.IGNORECASE,
)
_SPECIAL_CHARS = re.compile(r"[^\w\s-]")
_MULTI_SPACE = re.compile(r"\s+")

_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    return _client


def normalize_description(raw: str) -> str:
    """Clean a product description for classification caching (regex only).

    Strips quantities, prices, reference numbers, and special characters
    to produce a stable key suitable for cache lookups.
    """
    if not raw:
        return ""

    text = raw.strip()

    # Strip quantities/units
    text = _QTY_PATTERN.sub("", text)

    # Strip price/currency
    text = _PRICE_PATTERN.sub("", text)

    # Strip reference numbers
    text = _REF_PATTERN.sub("", text)

    # Strip special characters (keep hyphens, letters, digits, spaces)
    text = _SPECIAL_CHARS.sub(" ", text)

    # Collapse whitespace, lowercase, trim
    text = _MULTI_SPACE.sub(" ", text).strip().lower()

    return text


_LLM_NORMALIZE_PROMPT = """\
You are a customs classification assistant. Given a raw product description from a commercial invoice, extract the clean product name suitable for tariff classification.

Rules:
- Return ONLY the clean product name, nothing else
- Remove quantities, weights, sizes, prices, SKU codes, packaging info (zipper, pouch, tin, box)
- Remove marketplace prefixes (WM = Walmart, AM = Amazon) that are concatenated with the product name
- Split concatenated words (e.g., "WMHIBISCUS" → "Hibiscus")
- Keep material/fabric type (cotton, rayon, polyester, silk)
- Keep garment/product type (kurti, dress, top, tea, spice)
- Keep gender/age qualifiers (women's, girls, men's, kids)
- Expand abbreviations: PTB = pyramid tea bags, PTB TIN = tea in tin
- Use proper English title case
- Be concise: aim for 2-6 words

Examples:
"BUTTERFLY PEA 60g LOOSE ZIPPER ( HERBAL TEA )" → "Butterfly Pea Herbal Tea"
"WMHIBISCUS ROSE FLOWER TEA 100PTB ZIPPER" → "Hibiscus Rose Flower Tea"
"BLUE TEA - Hibiscus Cinnamon Herbal Tea - 100 Tea Bags || CAFFEINE FREE || Tangy & Spicy Tea - Eco-Conscious Ziplock Pouch" → "Hibiscus Cinnamon Herbal Tea"
"INDIAN KURTI WOMEN'S/ GIRLS POLYSTER" → "Women's Polyester Kurti"
"INDIAN DRESS WOMEN'S/ GIRLS COTTON" → "Women's Cotton Dress"
"Girls Cotton Kurti" → "Girls Cotton Kurti"
"""


async def llm_normalize_description(raw: str) -> str | None:
    """Use Claude Haiku to extract a clean product name from a raw description.

    Returns the LLM-cleaned description or None on failure.
    Falls back gracefully — callers should use regex-normalized version if this fails.
    """
    if not raw or not ANTHROPIC_API_KEY:
        return None

    try:
        client = _get_client()
        resp = await client.messages.create(
            model=LLM_MODEL_TEXT,
            max_tokens=100,
            system=_LLM_NORMALIZE_PROMPT,
            messages=[{"role": "user", "content": raw.strip()}],
        )
        result = resp.content[0].text.strip()
        if result:
            logger.debug("LLM normalize: '%s' → '%s'", raw[:60], result)
            return result
        return None
    except Exception:
        logger.warning("LLM normalize failed for '%s'", raw[:60], exc_info=True)
        return None


async def llm_normalize_batch(descriptions: list[str]) -> dict[str, str]:
    """Batch-normalize descriptions via Claude. One LLM call for up to ~20 items.

    Returns a mapping of raw_description → cleaned_description.
    Only includes successful normalizations.
    """
    if not descriptions or not ANTHROPIC_API_KEY:
        return {}

    # Build a numbered list for a single LLM call
    numbered = "\n".join(f"{i+1}. {d.strip()}" for i, d in enumerate(descriptions))
    prompt = (
        f"Normalize each product description below. "
        f"Return one clean product name per line, numbered to match.\n\n{numbered}"
    )

    try:
        client = _get_client()
        resp = await client.messages.create(
            model=LLM_MODEL_TEXT,
            max_tokens=1000,
            system=_LLM_NORMALIZE_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()

        # Parse numbered lines: "1. Clean Name" or "1: Clean Name"
        result: dict[str, str] = {}
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            # Match "1. Foo" or "1: Foo" or "1) Foo"
            m = re.match(r"(\d+)[.):\s]+(.+)", line)
            if m:
                idx = int(m.group(1)) - 1
                cleaned = m.group(2).strip()
                if 0 <= idx < len(descriptions) and cleaned:
                    result[descriptions[idx]] = cleaned

        logger.info("LLM batch normalize: %d/%d succeeded", len(result), len(descriptions))
        return result
    except Exception:
        logger.warning("LLM batch normalize failed", exc_info=True)
        return {}
