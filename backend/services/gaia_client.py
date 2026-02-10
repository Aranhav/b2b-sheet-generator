"""Gaia Dynamics API client for tariff classification and duty lookup.

Two-step flow (matching XOS pattern):
  1. classify_autonomous() → IHSN code, confidence, base duty from inline tariff_detail
  2. get_tariff_detail() → full tariff breakdown with tariff_scenario (reciprocal tariffs)

Cumulative duty = base_rate + SUM(scenario.value WHERE is_additional=true)

Request format for classification:
  POST /product/classification/tariff-code/autonomous
  {"input": {"name": "...", "description": "..."}, "destination_country": "US"}

Tariff detail lookup:
  GET /product/tariff-detail/{destination}/{code}/{origin}?include=pga_flags,remedy_flags
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


def calculate_cumulative_duty(
    tariff_data: dict[str, Any],
) -> tuple[float | None, float | None, list[dict[str, Any]]]:
    """Calculate cumulative duty from tariff-detail response (XOS formula).

    Returns (base_rate, cumulative_rate, scenario_summaries).

    XOS formula:
      duty_rate = tariff_base[0].rules[0].value
                + SUM(scenario.value WHERE scenario.tariff.is_additional=true)
    """
    # Extract base rate from tariff_base
    tariff_base = tariff_data.get("tariff_base") or []
    base_rate: float | None = None
    if tariff_base:
        rules = tariff_base[0].get("rules") or []
        if rules:
            rule = rules[0]
            kind = rule.get("kind", "")
            if kind == "percent":
                try:
                    base_rate = float(rule.get("value", 0))
                except (ValueError, TypeError):
                    pass
            elif kind == "free" or str(rule.get("value", "")).strip().lower() == "free":
                base_rate = 0.0
        # Fallback: parse from description string
        if base_rate is None:
            desc = tariff_base[0].get("description") or ""
            base_rate = parse_duty_rate(desc)

    if base_rate is None:
        return None, None, []

    # Sum additional tariff scenarios (only is_additional=true)
    scenarios = tariff_data.get("tariff_scenario") or []
    additional_sum = 0.0
    scenario_summaries: list[dict[str, Any]] = []

    for s in scenarios:
        tariff_info = s.get("tariff") or {}
        is_additional = tariff_info.get("is_additional", False)
        is_rumored = tariff_info.get("is_rumored", False)
        value = s.get("value", 0) or 0

        summary = {
            "title": tariff_info.get("title", ""),
            "value": value,
            "is_additional": is_additional,
            "is_rumored": is_rumored,
            "tariff_code": tariff_info.get("tariff_code", ""),
            "tariff_category": tariff_info.get("tariff_category", ""),
        }
        scenario_summaries.append(summary)

        if is_additional and value:
            additional_sum += float(value)

    cumulative = round(base_rate + additional_sum, 2)

    logger.info(
        "Duty calculation: base=%.2f + additional=%.2f = cumulative=%.2f (%d scenarios)",
        base_rate, additional_sum, cumulative, len(scenario_summaries),
    )

    return base_rate, cumulative, scenario_summaries


async def classify_autonomous(
    name: str,
    description: str,
    destination_country: str = "US",
) -> dict[str, Any] | None:
    """Classify a product autonomously via Gaia.

    Returns the inner "data" dict with keys: best_guess_code, confidence,
    suggested_description, tariff_detail — or None on failure.
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

            data = payload.get("data") or payload
            code = data.get("best_guess_code", "")
            confidence = data.get("confidence", "")

            logger.info(
                "Gaia classify: '%s' → %s (confidence=%s)",
                name[:60], code, confidence,
            )
            return data
    except Exception:
        logger.warning("Gaia classify failed for '%s'", name[:60], exc_info=True)
        return None


async def get_tariff_detail(
    destination_country: str,
    tariff_code: str,
    origin_country: str = "IN",
) -> dict[str, Any] | None:
    """Get full tariff detail including scenarios, PGA flags, and remedy flags.

    The tariff_code should be the 10-digit HTS code (dots are stripped).
    Returns the inner "data" dict or None on failure.
    """
    if not GAIA_API_KEY:
        return None

    # Strip dots from HTS code (e.g. "6109.10.00.12" → "6109100012")
    code_clean = tariff_code.replace(".", "")
    if not code_clean:
        return None

    url = (
        f"{GAIA_API_URL}/product/tariff-detail"
        f"/{destination_country}/{code_clean}/{origin_country}"
        f"?include=pga_flags,remedy_flags"
    )

    try:
        async with httpx.AsyncClient(timeout=GAIA_TIMEOUT_SECONDS) as client:
            resp = await client.get(url, headers=_headers())
            resp.raise_for_status()
            payload = resp.json()

            data = payload.get("data") or payload
            logger.info(
                "Gaia tariff detail: %s/%s/%s → %d scenarios",
                destination_country, code_clean, origin_country,
                len(data.get("tariff_scenario") or []),
            )
            return data
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.info(
                "Gaia tariff detail not found: %s/%s/%s (404)",
                destination_country, code_clean, origin_country,
            )
        else:
            logger.warning(
                "Gaia tariff detail failed: %s/%s/%s (%d)",
                destination_country, code_clean, origin_country,
                e.response.status_code,
            )
        return None
    except Exception:
        logger.warning(
            "Gaia tariff detail failed: %s/%s/%s",
            destination_country, code_clean, origin_country,
            exc_info=True,
        )
        return None
