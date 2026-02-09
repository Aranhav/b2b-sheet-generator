"""Document type classification using Claude Haiku.

Classifies each uploaded PDF as one of:
  invoice, packing_list, certificate, bill_of_lading, purchase_order, other
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from functools import partial
from typing import Any

import anthropic

from backend.config import ANTHROPIC_API_KEY, LLM_MODEL_TEXT, LLM_MODEL_VISION

logger = logging.getLogger(__name__)

_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

DOCUMENT_TYPES = [
    "invoice",
    "packing_list",
    "certificate",
    "bill_of_lading",
    "purchase_order",
    "other",
]

_CLASSIFICATION_PROMPT = """\
You are classifying documents from Indian export shipments.

Based on the content, classify this document as exactly ONE of:
- invoice: Commercial invoice with line items, unit prices, and totals
- packing_list: Packing list with box dimensions, weights, and item quantities
- certificate: Certificate of Origin, phytosanitary cert, test report, or any certification document
- bill_of_lading: Shipping document with container numbers, BL numbers, vessel info
- purchase_order: Buyer's purchase order with PO numbers and order details
- other: Unknown or unclassifiable document type

Return ONLY a valid JSON object (no markdown fences, no commentary):
{
  "document_type": "invoice",
  "confidence": 0.95,
  "reason": "Contains line items with descriptions, quantities, unit prices, and HSN codes"
}
"""


def _classify_sync(
    pages_data: list[dict[str, Any]],
    is_vision: bool,
) -> dict[str, Any]:
    """Synchronous classification call."""
    if is_vision:
        import base64
        content: list[dict[str, Any]] = [
            {"type": "text", "text": "Classify this document:"},
        ]
        # Use only first 2 pages for classification (enough to determine type)
        for page in pages_data[:2]:
            image_bytes = page.get("image_bytes")
            if image_bytes:
                b64_data = base64.standard_b64encode(image_bytes).decode("ascii")
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": b64_data,
                    },
                })
        model = LLM_MODEL_VISION
    else:
        # Concatenate first 2 pages of text
        text_parts = []
        for page in pages_data[:2]:
            text = page.get("text", "")
            if text:
                text_parts.append(f"--- PAGE {page.get('page_num', '?')} ---\n{text}")
        content = "Classify this document:\n\n" + "\n\n".join(text_parts)
        model = LLM_MODEL_TEXT

    response = _client.messages.create(
        model=model,
        max_tokens=256,
        system=_CLASSIFICATION_PROMPT,
        messages=[{"role": "user", "content": content}],
    )

    raw = response.content[0].text.strip()
    # Strip markdown fences if present
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)```", raw, re.DOTALL)
    if fence_match:
        raw = fence_match.group(1).strip()

    result = json.loads(raw)

    # Validate document type
    doc_type = result.get("document_type", "other").lower()
    if doc_type not in DOCUMENT_TYPES:
        doc_type = "other"

    return {
        "document_type": doc_type,
        "confidence": float(result.get("confidence", 0.5)),
        "reason": result.get("reason", ""),
    }


async def classify_document(
    pages_data: list[dict[str, Any]],
    is_vision: bool = False,
) -> dict[str, Any]:
    """Classify a document's type using the first few pages.

    Returns dict with keys: document_type, confidence, reason.
    """
    if not pages_data:
        return {"document_type": "other", "confidence": 0.0, "reason": "No pages"}

    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None,
            partial(_classify_sync, pages_data, is_vision),
        )
        logger.info(
            "Classified as %s (confidence=%.2f): %s",
            result["document_type"],
            result["confidence"],
            result["reason"][:80],
        )
        return result
    except Exception:
        logger.exception("Classification failed, defaulting to 'other'")
        return {"document_type": "other", "confidence": 0.0, "reason": "Classification error"}
