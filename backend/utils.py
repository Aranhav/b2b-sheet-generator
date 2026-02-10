"""Shared utilities for the B2B Booking Agent backend."""
from __future__ import annotations


def normalize_name(name: str | None) -> str:
    """Normalize a company name for matching.

    Uppercases, strips whitespace, removes common legal suffixes
    (PVT LTD, INC, LLC, etc.), honorific prefixes (M/S, MESSRS),
    and proprietor name patterns ("NAME1 M/S NAME2" → "NAME2").
    Used by both the grouper and seller-intelligence modules.
    """
    if not name:
        return ""
    import re
    name = name.upper().strip()
    # Strip "M/S" or "MESSRS" prefix (with optional period/space)
    name = re.sub(r"^M/S\.?\s+", "", name)
    name = re.sub(r"^MESSRS\.?\s+", "", name)
    # Strip proprietor patterns: "COMPANY M/S PERSON NAME" → "COMPANY"
    name = re.sub(r"\s+M/S\s+.*$", "", name)
    for suffix in [
        " PVT LTD", " PRIVATE LIMITED", " LTD", " LIMITED",
        " INC", " INC.", " LLC", " LLP", " CO.", " CORP",
        " CORPORATION", " & CO", " AND CO",
    ]:
        name = name.replace(suffix, "")
    return name.strip()
