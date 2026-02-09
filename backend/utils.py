"""Shared utilities for the B2B Booking Agent backend."""
from __future__ import annotations


def normalize_name(name: str | None) -> str:
    """Normalize a company name for matching.

    Uppercases, strips whitespace, and removes common legal suffixes
    (PVT LTD, INC, LLC, etc.).  Used by both the grouper and
    seller-intelligence modules.
    """
    if not name:
        return ""
    name = name.upper().strip()
    for suffix in [
        " PVT LTD", " PRIVATE LIMITED", " LTD", " LIMITED",
        " INC", " INC.", " LLC", " LLP", " CO.", " CORP",
        " CORPORATION", " & CO", " AND CO",
    ]:
        name = name.replace(suffix, "")
    return name.strip()
