"""Xindus UAT API client — authenticate and submit shipments."""
from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from backend.config import XINDUS_UAT_URL, XINDUS_UAT_USERNAME, XINDUS_UAT_PASSWORD

logger = logging.getLogger(__name__)

# In-memory token cache
_token: str | None = None
_token_expires: float = 0


async def _authenticate() -> str:
    """Login to Xindus UAT and return a Bearer token (cached)."""
    global _token, _token_expires

    if _token and time.time() < _token_expires:
        return _token

    if not XINDUS_UAT_USERNAME or not XINDUS_UAT_PASSWORD:
        raise RuntimeError("XINDUS_UAT_USERNAME / XINDUS_UAT_PASSWORD not configured")

    url = f"{XINDUS_UAT_URL}/xos/api/auth/login"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json={
            "username": XINDUS_UAT_USERNAME,
            "password": XINDUS_UAT_PASSWORD,
        })

    if resp.status_code != 200:
        raise RuntimeError(f"Xindus auth failed ({resp.status_code}): {resp.text[:200]}")

    data = resp.json()
    _token = data.get("access_token") or data.get("token")
    if not _token:
        raise RuntimeError(f"No access_token in auth response: {list(data.keys())}")

    # Cache for 55 minutes (tokens typically last 1h)
    _token_expires = time.time() + 55 * 60
    logger.info("Xindus UAT auth successful, token cached")
    return _token


def _clear_token() -> None:
    """Clear cached token (used on 401 retry)."""
    global _token, _token_expires
    _token = None
    _token_expires = 0


async def submit_shipment(
    payload: dict[str, Any],
    consignor_id: int | None = None,
) -> tuple[int, dict[str, Any]]:
    """Submit a shipment to Xindus UAT.

    Returns (http_status, response_body).
    Retries once on 401 with a fresh token.
    """
    url = f"{XINDUS_UAT_URL}/xos/api/partner/shipment"
    if consignor_id:
        url += f"?consignor_id={consignor_id}"

    for attempt in range(2):
        token = await _authenticate()

        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )

        if resp.status_code == 401 and attempt == 0:
            logger.warning("Xindus returned 401, refreshing token and retrying")
            _clear_token()
            continue

        try:
            body = resp.json()
        except Exception:
            body = {"raw_response": resp.text[:2000]}

        return resp.status_code, body

    # Should not reach here, but just in case
    return 500, {"error": "Failed after retry"}
