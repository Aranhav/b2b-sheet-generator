"""Normalize product descriptions for Gaia classification caching."""
from __future__ import annotations

import re


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


def normalize_description(raw: str) -> str:
    """Clean a product description for classification caching.

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
