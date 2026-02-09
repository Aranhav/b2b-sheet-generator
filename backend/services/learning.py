"""Learning service: retrieve few-shot examples from correction history.

Queries the corrections table for recent corrections matching a field path,
and formats them as examples that can be injected into extraction prompts.
"""
from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from backend import db

logger = logging.getLogger(__name__)


async def get_few_shot_examples(
    field_path: str,
    limit: int = 3,
    seller_id: UUID | None = None,
) -> list[dict[str, Any]]:
    """Retrieve recent corrections for a field to use as few-shot examples.

    Parameters
    ----------
    field_path:
        Dot-separated field path, e.g. "shipper_address.city"
    limit:
        Maximum number of examples to return.
    seller_id:
        When provided, corrections are scoped to this seller.

    Returns
    -------
    List of dicts with keys: field_path, original_value, corrected_value, file_context
    """
    try:
        rows = await db.get_corrections_for_field(
            field_path, limit=limit, seller_id=seller_id,
        )
        return rows
    except Exception:
        logger.warning("Failed to fetch corrections for %s", field_path, exc_info=True)
        return []


def format_correction_context(
    corrections: list[dict[str, Any]],
) -> str:
    """Format corrections as a text block for injection into LLM prompts.

    Returns an empty string if no corrections are available.
    """
    if not corrections:
        return ""

    lines = ["\nCommon corrections for similar documents:"]
    for c in corrections:
        orig = c.get("original_value")
        corrected = c.get("corrected_value")
        if isinstance(orig, str):
            try:
                orig = json.loads(orig)
            except (json.JSONDecodeError, TypeError):
                pass
        if isinstance(corrected, str):
            try:
                corrected = json.loads(corrected)
            except (json.JSONDecodeError, TypeError):
                pass

        lines.append(
            f"- Field '{c['field_path']}': AI extracted '{orig}' but correct was '{corrected}'"
        )

    return "\n".join(lines)


async def build_extraction_hint(
    field_paths: list[str] | None = None,
    seller_id: UUID | None = None,
) -> str:
    """Build a prompt hint from recent corrections across key fields.

    If field_paths is None, uses a default set of commonly corrected fields.
    When seller_id is provided, corrections are scoped to that seller.
    """
    if field_paths is None:
        field_paths = [
            "shipper_address.name",
            "shipper_address.city",
            "receiver_address.name",
            "receiver_address.city",
            "receiver_address.zip",
            "invoice_number",
            "shipping_currency",
        ]

    all_corrections = []
    for fp in field_paths:
        examples = await get_few_shot_examples(fp, limit=2, seller_id=seller_id)
        all_corrections.extend(examples)

    return format_correction_context(all_corrections)


async def get_correction_stats() -> list[dict[str, Any]]:
    """Return per-field correction frequency stats.

    Returns list of {field_path, correction_count, latest_at}.
    """
    try:
        return await db.get_correction_stats(limit=20)
    except Exception:
        logger.warning("Failed to fetch correction stats", exc_info=True)
        return []
