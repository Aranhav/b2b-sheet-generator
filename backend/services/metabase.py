"""Metabase REST API client for querying the Xindus production database.

Session-based auth (POST /api/session → token, cached 12 days).
Mirrors the pattern in src/lib/metabase.ts on the frontend.
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

METABASE_URL = (os.getenv("METABASE_URL") or "").rstrip("/")
METABASE_USERNAME = os.getenv("METABASE_USERNAME") or ""
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD") or ""
METABASE_DB_ID = int(os.getenv("METABASE_DB_ID") or "2")

_session_token: str | None = None
_session_expiry: float = 0

# Browser-like headers to avoid Cloudflare blocks
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def escape_sql(value: str) -> str:
    """Escape a string for safe use in MySQL single-quoted literals."""
    return re.sub(
        r"[\0\x08\x09\x1a\n\r\"'\\%_]",
        lambda m: {
            "\0": "\\0", "\x08": "\\b", "\x09": "\\t", "\x1a": "\\z",
            "\n": "\\n", "\r": "\\r", '"': '\\"', "'": "\\'",
            "\\": "\\\\", "%": "\\%", "_": "\\_",
        }.get(m.group(), m.group()),
        value,
    )


async def _get_session() -> str:
    """Get or refresh the Metabase session token."""
    global _session_token, _session_expiry

    if _session_token and time.time() < _session_expiry:
        return _session_token

    if not METABASE_URL or not METABASE_USERNAME or not METABASE_PASSWORD:
        raise RuntimeError(
            "Metabase credentials not configured. "
            "Set METABASE_URL, METABASE_USERNAME, METABASE_PASSWORD."
        )

    async with httpx.AsyncClient(timeout=15, headers=_BROWSER_HEADERS) as client:
        res = await client.post(
            f"{METABASE_URL}/api/session",
            json={"username": METABASE_USERNAME, "password": METABASE_PASSWORD},
        )
        res.raise_for_status()
        data = res.json()

    _session_token = data["id"]
    # Metabase sessions last 14 days; refresh after 12
    _session_expiry = time.time() + 12 * 24 * 60 * 60
    return _session_token


async def query_metabase(sql: str, _retry: bool = True) -> list[dict[str, Any]]:
    """Execute a native SQL query and return rows as list of dicts."""
    session = await _get_session()

    async with httpx.AsyncClient(timeout=15, headers=_BROWSER_HEADERS) as client:
        res = await client.post(
            f"{METABASE_URL}/api/dataset",
            headers={
                "Content-Type": "application/json",
                "X-Metabase-Session": session,
            },
            json={
                "database": METABASE_DB_ID,
                "type": "native",
                "native": {"query": sql},
            },
        )

        if not res.is_success:
            text = res.text
            is_block = "Cloudflare" in text or "<!DOCTYPE" in text
            if (res.status_code in (401, 403) or is_block) and _retry:
                global _session_token, _session_expiry
                _session_token = None
                _session_expiry = 0
                return await query_metabase(sql, _retry=False)
            raise RuntimeError(f"Metabase query failed ({res.status_code}): {text[:200]}")

        result = res.json()

    if result.get("error"):
        raise RuntimeError(f"Metabase query error: {result['error']}")

    cols = result.get("data", {}).get("cols", [])
    rows = result.get("data", {}).get("rows", [])

    col_names = [c["name"] for c in cols]
    return [{col_names[i]: row[i] for i in range(len(col_names))} for row in rows]
