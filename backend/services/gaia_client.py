"""Gaia Dynamics API client for autonomous tariff classification.

Uses the /product/classification/tariff-code/autonomous endpoint which
returns IHSN code, confidence, and inline tariff_detail with duty rates
in a single call — no separate tariff lookup needed.

Correct request format (discovered via testing):
  POST /product/classification/tariff-code/autonomous
  {
    "input": {"name": "...", "description": "..."},
    "destination_country": "US"
  }

Response shape (key fields inside "data"):
  {
    "data": {
      "best_guess_code": "7419.80.15.00",
      "confidence": "MEDIUM",          # STRING not float
      "suggested_description": "...",
      "tariff_detail": {
        "rate_of_duty": {"general": "3%", "special": "", "other": ""}
      },
      "alternatives": [...],
      "rulings": [...]
    }
  }
"""
from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from backend.config import GAIA_API_URL, GAIA_API_KEY, GAIA_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

_DUTY_PERCENT_RE = re.compile(r"([\d.]+)\s*%")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {GAIA_API_KEY}",
        "Content-Type": "application/json",
    }


def parse_duty_rate(general_duty: str | None) -> float | None:
    """Extract numeric duty % from Gaia's rate_of_duty.general string.

    Examples: "3%" → 3.0, "5.5%" → 5.5, "Free" → 0.0, "" → None
    """
    if not general_duty:
        return None
    if general_duty.strip().lower() == "free":
        return 0.0
    m = _DUTY_PERCENT_RE.search(general_duty)
    if m:
        try:
            return float(m.group(1))
        except (ValueError, TypeError):
            return None
    return None


async def classify_autonomous(
    name: str,
    description: str,
    destination_country: str = "US",
) -> dict[str, Any] | None:
    """Classify a product autonomously via Gaia.

    Returns the inner "data" dict with keys: best_guess_code, confidence,
    suggested_description, tariff_detail — or None on failure.

    The response already includes tariff_detail inline, so no separate
    tariff lookup call is needed.
    """
    if not GAIA_API_KEY:
        logger.warning("GAIA_API_KEY not set — skipping classification")
        return None

    url = f"{GAIA_API_URL}/product/classification/tariff-code/autonomous"
    body = {
        "input": {
            "name": name,
            "description": description,
        },
        "destination_country": destination_country,
    }

    try:
        async with httpx.AsyncClient(timeout=GAIA_TIMEOUT_SECONDS) as client:
            resp = await client.post(url, json=body, headers=_headers())
            resp.raise_for_status()
            payload = resp.json()

            # Response is wrapped: {"data": {...actual classification...}}
            data = payload.get("data") or payload
            code = data.get("best_guess_code", "")
            confidence = data.get("confidence", "")

            # Extract duty rate from inline tariff_detail
            tariff_detail = data.get("tariff_detail") or {}
            rod = tariff_detail.get("rate_of_duty") or {}
            duty_rate = parse_duty_rate(rod.get("general"))

            logger.info(
                "Gaia classify: '%s' → %s (confidence=%s, duty=%s)",
                name[:60],
                code,
                confidence,
                duty_rate,
            )
            return data
    except Exception:
        logger.warning("Gaia classify failed for '%s'", name[:60], exc_info=True)
        return None
