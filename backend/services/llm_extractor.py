"""LLM-based structured data extraction from PDF content.

Sends extracted text (or rendered page images) to Anthropic Claude for
structured extraction of invoice and packing-list data from Indian export
documents.

Uses two separate LLM calls to handle large packing lists:
  - Call 1 (Invoice): Haiku 4.5, extracts line items, addresses, amounts
  - Call 2 (Packing List): Sonnet 4.5 with higher token limit, extracts
    destinations array + all boxes using a flat (no confidence wrapper)
    schema to minimize token usage
Both calls run concurrently via asyncio.gather, then results are merged.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from functools import partial
from typing import Any

import anthropic

from backend.config import (
    ANTHROPIC_API_KEY,
    LLM_MAX_TOKENS_INVOICE,
    LLM_MAX_TOKENS_PACKING_LIST,
    LLM_MODEL_PACKING_LIST,
    LLM_MODEL_TEXT,
    LLM_MODEL_VISION,
)
from backend.models.extraction import (
    Address,
    Box,
    BoxItem,
    ConfidenceValue,
    Destination,
    ExtractionResult,
    InvoiceData,
    LineItem,
    PackingListData,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Anthropic client (synchronous -- wrapped in asyncio below)
# ---------------------------------------------------------------------------
_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ---------------------------------------------------------------------------
# System prompts -- one per extraction call
# ---------------------------------------------------------------------------

_INVOICE_PROMPT = """\
You are extracting data from Indian export invoices for cross-border shipments.

INSTRUCTIONS:
- Extract ONLY invoice data: line items, addresses, amounts. Ignore packing list / box data.
- If a field is not found in the document, set value to null and confidence to 0.0
- For HS codes, extract the full 8-digit code. If only 6 digits visible, pad with 00.
- All weights must be in kilograms. Convert if in grams or pounds.
- All prices should be in USD. If in INR, note the original currency but keep the value as-is.
- Rate your confidence 0.0-1.0 for each field based on how clearly it appears in the document.

Return ONLY a valid JSON object (no markdown fences, no commentary) conforming to \
the exact schema below. Every leaf value must be a {"value": ..., "confidence": ...} \
object unless it is a list.

JSON SCHEMA:
{
  "invoice": {
    "invoice_number": {"value": "...", "confidence": 0.95},
    "invoice_date": {"value": "...", "confidence": 0.90},
    "currency": {"value": "USD", "confidence": 0.99},
    "total_amount": {"value": 12500.00, "confidence": 0.97},
    "exporter": {
      "name": {"value": "...", "confidence": 0.9},
      "address": {"value": "...", "confidence": 0.85},
      "city": {"value": "...", "confidence": 0.85},
      "state": {"value": "...", "confidence": 0.85},
      "zip_code": {"value": "...", "confidence": 0.85},
      "country": {"value": "...", "confidence": 0.85},
      "phone": {"value": "...", "confidence": 0.70},
      "email": {"value": "...", "confidence": 0.70}
    },
    "consignee": { "name": {...}, "address": {...}, "city": {...}, "state": {...}, "zip_code": {...}, "country": {...}, "phone": {...}, "email": {...} },
    "ship_to": { "name": {...}, "address": {...}, "city": {...}, "state": {...}, "zip_code": {...}, "country": {...}, "phone": {...}, "email": {...} },
    "ior": { "name": {...}, "address": {...}, "city": {...}, "state": {...}, "zip_code": {...}, "country": {...}, "phone": {...}, "email": {...} },
    "line_items": [
      {
        "description": {"value": "...", "confidence": 0.94},
        "hs_code_origin": {"value": "74181000", "confidence": 0.85},
        "hs_code_destination": {"value": "74181000", "confidence": 0.80},
        "quantity": {"value": 500, "confidence": 0.96},
        "unit_price_usd": {"value": 12.50, "confidence": 0.95},
        "total_price_usd": {"value": 6250.00, "confidence": 0.97},
        "unit_weight_kg": {"value": 0.35, "confidence": 0.80},
        "igst_percent": {"value": 18, "confidence": 0.70}
      }
    ]
  }
}
"""

_PACKING_LIST_PROMPT = """\
You are extracting packing list data from Indian export shipment documents.

CRITICAL INSTRUCTIONS:
- Extract packing list data: destinations, boxes, weights, dimensions.
- ALSO extract the reference fields at the top level (invoice_number, exporter_name, consignee_name) \
if they appear anywhere on the document -- these are needed to match this packing list to its invoice.
- Extract ALL boxes from the document. Do NOT truncate or summarize. Every single box must appear.
- If boxes ship to multiple destinations/warehouses, list each destination in the "destinations" array and reference it from each box via "destination_id".
- If all boxes go to the same destination, still include it as a single entry in "destinations".
- Use FLAT values (plain strings/numbers) to save space -- do NOT use {"value":..., "confidence":...} wrappers.
- All weights must be in kilograms. Convert if in grams or pounds.
- All dimensions must be in centimeters. Convert if in inches (multiply by 2.54).

Return ONLY a valid JSON object (no markdown fences, no commentary).

JSON SCHEMA:
{
  "invoice_number": "WFS-042025-26",
  "exporter_name": "Redplum Pvt Ltd",
  "consignee_name": "WALMART",
  "total_boxes": 105,
  "total_net_weight_kg": 450.5,
  "total_gross_weight_kg": 520.0,
  "destinations": [
    {
      "id": "D1",
      "name": "Amazon FBA FTW1",
      "address": "33333 Lyndon B Johnson Fwy",
      "city": "Dallas",
      "state": "TX",
      "zip_code": "75241",
      "country": "US",
      "phone": "",
      "email": ""
    }
  ],
  "boxes": [
    {
      "box_number": 1,
      "destination_id": "D1",
      "length_cm": 40,
      "width_cm": 30,
      "height_cm": 25,
      "gross_weight_kg": 5.2,
      "net_weight_kg": 4.8,
      "items": [
        {"description": "Copper Bottle 750ml", "quantity": 10}
      ]
    }
  ]
}
"""

_RETRY_PROMPT = """\
Your previous response was not valid JSON. Please try again.
Return ONLY a raw JSON object (no markdown code fences, no extra text) \
following the schema I described earlier. If you are unsure about a field, \
set its value to null.
"""

# ---------------------------------------------------------------------------
# Content builders
# ---------------------------------------------------------------------------


def _build_text_user_message(pages_data: list[dict[str, Any]], purpose: str) -> str:
    """Assemble a single user-message string from text-based page data."""
    parts: list[str] = [f"Extract {purpose} data from the following document pages:\n"]

    for page in pages_data:
        page_num = page.get("page_num", "?")
        text = page.get("text", "")
        tables = page.get("tables", [])

        parts.append(f"\n--- PAGE {page_num} ---\n")
        if text:
            parts.append(text)

        if tables:
            for t_idx, table in enumerate(tables):
                parts.append(f"\n[Table {t_idx + 1} on page {page_num}]")
                for row in table:
                    parts.append(" | ".join(str(c) if c else "" for c in row))

    return "\n".join(parts)


def _build_vision_content_blocks(
    pages_data: list[dict[str, Any]], purpose: str
) -> list[dict[str, Any]]:
    """Build Claude content blocks for the vision model."""
    blocks: list[dict[str, Any]] = []

    blocks.append({
        "type": "text",
        "text": f"Extract {purpose} data from the following scanned document pages:",
    })

    for page in pages_data:
        image_bytes: bytes | None = page.get("image_bytes")
        if not image_bytes:
            continue

        b64_data = base64.standard_b64encode(image_bytes).decode("ascii")
        blocks.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": b64_data,
                },
            }
        )
        blocks.append(
            {"type": "text", "text": f"(Page {page.get('page_num', '?')})"}
        )

    return blocks


def _extract_json_from_response(raw: str) -> dict[str, Any]:
    """Parse JSON from a Claude response, tolerating markdown fences."""
    text = raw.strip()

    # Strip markdown code fences if present.
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1).strip()

    return json.loads(text)


# ---------------------------------------------------------------------------
# Model mapping helpers
# ---------------------------------------------------------------------------


def _parse_confidence_value(raw: Any) -> ConfidenceValue:
    """Convert a raw dict to a ConfidenceValue, tolerating missing keys."""
    if raw is None:
        return ConfidenceValue(value=None, confidence=0.0)
    if isinstance(raw, dict):
        return ConfidenceValue(
            value=raw.get("value"),
            confidence=float(raw.get("confidence", 0.0)),
        )
    # If the LLM returned a bare value instead of an object, wrap it.
    return ConfidenceValue(value=raw, confidence=0.5)


def _wrap_flat_value(raw: Any, default_confidence: float = 0.8) -> ConfidenceValue:
    """Wrap a flat (non-confidence-wrapped) value from the packing list LLM."""
    if raw is None:
        return ConfidenceValue(value=None, confidence=0.0)
    if isinstance(raw, dict) and "value" in raw:
        # LLM included confidence wrapper anyway -- use it
        return _parse_confidence_value(raw)
    return ConfidenceValue(value=raw, confidence=default_confidence)


def _parse_address(raw: Any) -> Address:
    if not raw or not isinstance(raw, dict):
        return Address()
    return Address(
        name=_parse_confidence_value(raw.get("name")),
        address=_parse_confidence_value(raw.get("address")),
        city=_parse_confidence_value(raw.get("city")),
        state=_parse_confidence_value(raw.get("state")),
        zip_code=_parse_confidence_value(raw.get("zip_code")),
        country=_parse_confidence_value(raw.get("country")),
        phone=_parse_confidence_value(raw.get("phone")),
        email=_parse_confidence_value(raw.get("email")),
    )


def _parse_flat_address(raw: Any) -> Address:
    """Parse an address from the flat packing list schema (no confidence wrappers)."""
    if not raw or not isinstance(raw, dict):
        return Address()
    return Address(
        name=_wrap_flat_value(raw.get("name")),
        address=_wrap_flat_value(raw.get("address")),
        city=_wrap_flat_value(raw.get("city")),
        state=_wrap_flat_value(raw.get("state")),
        zip_code=_wrap_flat_value(raw.get("zip_code")),
        country=_wrap_flat_value(raw.get("country")),
        phone=_wrap_flat_value(raw.get("phone")),
        email=_wrap_flat_value(raw.get("email")),
    )


def _parse_line_item(raw: dict[str, Any]) -> LineItem:
    return LineItem(
        description=_parse_confidence_value(raw.get("description")),
        hs_code_origin=_parse_confidence_value(raw.get("hs_code_origin")),
        hs_code_destination=_parse_confidence_value(raw.get("hs_code_destination")),
        quantity=_parse_confidence_value(raw.get("quantity")),
        unit_price_usd=_parse_confidence_value(raw.get("unit_price_usd")),
        total_price_usd=_parse_confidence_value(raw.get("total_price_usd")),
        unit_weight_kg=_parse_confidence_value(raw.get("unit_weight_kg")),
        igst_percent=_parse_confidence_value(raw.get("igst_percent")),
    )


def _parse_box_item(raw: dict[str, Any]) -> BoxItem:
    return BoxItem(
        description=_parse_confidence_value(raw.get("description")),
        quantity=_parse_confidence_value(raw.get("quantity")),
    )


def _parse_flat_box_item(raw: dict[str, Any]) -> BoxItem:
    """Parse a box item from the flat packing list schema."""
    return BoxItem(
        description=_wrap_flat_value(raw.get("description")),
        quantity=_wrap_flat_value(raw.get("quantity")),
    )


def _parse_box(raw: dict[str, Any]) -> Box:
    items_raw = raw.get("items") or []
    return Box(
        box_number=_parse_confidence_value(raw.get("box_number")),
        length_cm=_parse_confidence_value(raw.get("length_cm")),
        width_cm=_parse_confidence_value(raw.get("width_cm")),
        height_cm=_parse_confidence_value(raw.get("height_cm")),
        gross_weight_kg=_parse_confidence_value(raw.get("gross_weight_kg")),
        net_weight_kg=_parse_confidence_value(raw.get("net_weight_kg")),
        items=[_parse_box_item(i) for i in items_raw if isinstance(i, dict)],
        destination_id=_parse_confidence_value(raw.get("destination_id")),
    )


def _parse_flat_box(raw: dict[str, Any]) -> Box:
    """Parse a box from the flat packing list schema (no confidence wrappers)."""
    items_raw = raw.get("items") or []
    return Box(
        box_number=_wrap_flat_value(raw.get("box_number")),
        length_cm=_wrap_flat_value(raw.get("length_cm")),
        width_cm=_wrap_flat_value(raw.get("width_cm")),
        height_cm=_wrap_flat_value(raw.get("height_cm")),
        gross_weight_kg=_wrap_flat_value(raw.get("gross_weight_kg")),
        net_weight_kg=_wrap_flat_value(raw.get("net_weight_kg")),
        items=[_parse_flat_box_item(i) for i in items_raw if isinstance(i, dict)],
        destination_id=_wrap_flat_value(raw.get("destination_id")),
    )


def _parse_invoice(raw: dict[str, Any] | None) -> InvoiceData:
    if not raw or not isinstance(raw, dict):
        return InvoiceData()

    line_items_raw = raw.get("line_items") or []
    return InvoiceData(
        invoice_number=_parse_confidence_value(raw.get("invoice_number")),
        invoice_date=_parse_confidence_value(raw.get("invoice_date")),
        currency=_parse_confidence_value(raw.get("currency")),
        total_amount=_parse_confidence_value(raw.get("total_amount")),
        exporter=_parse_address(raw.get("exporter")),
        consignee=_parse_address(raw.get("consignee")),
        ship_to=_parse_address(raw.get("ship_to")),
        ior=_parse_address(raw.get("ior")),
        line_items=[_parse_line_item(li) for li in line_items_raw if isinstance(li, dict)],
    )


def _parse_packing_list(raw: dict[str, Any] | None) -> PackingListData:
    if not raw or not isinstance(raw, dict):
        return PackingListData()

    boxes_raw = raw.get("boxes") or []
    return PackingListData(
        total_boxes=_parse_confidence_value(raw.get("total_boxes")),
        total_net_weight_kg=_parse_confidence_value(raw.get("total_net_weight_kg")),
        total_gross_weight_kg=_parse_confidence_value(raw.get("total_gross_weight_kg")),
        boxes=[_parse_box(b) for b in boxes_raw if isinstance(b, dict)],
    )


def _parse_flat_packing_list(raw: dict[str, Any] | None) -> PackingListData:
    """Parse packing list from the flat schema (no confidence wrappers on boxes)."""
    if not raw or not isinstance(raw, dict):
        return PackingListData()

    boxes_raw = raw.get("boxes") or []
    destinations_raw = raw.get("destinations") or []

    destinations: list[Destination] = []
    for d in destinations_raw:
        if not isinstance(d, dict):
            continue
        destinations.append(Destination(
            id=str(d.get("id", "")),
            name=_wrap_flat_value(d.get("name")),
            address=_parse_flat_address(d),
        ))

    return PackingListData(
        total_boxes=_wrap_flat_value(raw.get("total_boxes")),
        total_net_weight_kg=_wrap_flat_value(raw.get("total_net_weight_kg")),
        total_gross_weight_kg=_wrap_flat_value(raw.get("total_gross_weight_kg")),
        boxes=[_parse_flat_box(b) for b in boxes_raw if isinstance(b, dict)],
        destinations=destinations,
    )


def _resolve_box_receivers(
    packing_list: PackingListData,
    fallback_receiver: Address,
) -> None:
    """Resolve each box's receiver address from destinations.

    Mutates boxes in-place, setting ``box.receiver`` to the matching
    destination address, or the fallback (ship_to / consignee) if no match.
    """
    dest_map: dict[str, Address] = {}
    for dest in packing_list.destinations:
        dest_map[dest.id] = dest.address

    for box in packing_list.boxes:
        dest_id = box.destination_id.value
        if dest_id and str(dest_id) in dest_map:
            box.receiver = dest_map[str(dest_id)]
        else:
            box.receiver = fallback_receiver


def _compute_overall_confidence(result: ExtractionResult) -> float:
    """Compute a weighted average confidence across all extracted fields."""
    confidences: list[float] = []

    # Invoice-level fields
    inv = result.invoice
    for cv in [inv.invoice_number, inv.invoice_date, inv.currency, inv.total_amount]:
        if cv.value is not None:
            confidences.append(cv.confidence)

    # Line items
    for li in inv.line_items:
        for cv in [li.description, li.quantity, li.unit_price_usd, li.total_price_usd]:
            if cv.value is not None:
                confidences.append(cv.confidence)

    # Packing list
    pl = result.packing_list
    for cv in [pl.total_boxes, pl.total_net_weight_kg, pl.total_gross_weight_kg]:
        if cv.value is not None:
            confidences.append(cv.confidence)

    for box in pl.boxes:
        for cv in [box.box_number, box.gross_weight_kg, box.net_weight_kg]:
            if cv.value is not None:
                confidences.append(cv.confidence)

    if not confidences:
        return 0.0
    return round(sum(confidences) / len(confidences), 3)


# ---------------------------------------------------------------------------
# Core LLM calls (synchronous, to be run in executor)
# ---------------------------------------------------------------------------


def _call_llm(
    model: str,
    system_prompt: str,
    user_content: str | list[dict[str, Any]],
    max_tokens: int,
) -> str:
    """Generic synchronous LLM call."""
    response = _client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[{"role": "user", "content": user_content}],
    )
    return response.content[0].text


def _call_retry(
    model: str,
    system_prompt: str,
    original_message: Any,
    max_tokens: int,
) -> str:
    """Retry with a simpler follow-up prompt asking for valid JSON."""
    messages = [
        {"role": "user", "content": original_message},
        {
            "role": "assistant",
            "content": "I apologize, let me provide the correct JSON.",
        },
        {"role": "user", "content": _RETRY_PROMPT},
    ]
    response = _client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=messages,
    )
    return response.content[0].text


# ---------------------------------------------------------------------------
# Individual extraction functions
# ---------------------------------------------------------------------------


async def _extract_invoice(
    pages_data: list[dict[str, Any]],
    is_vision: bool,
    correction_hints: str = "",
) -> dict[str, Any]:
    """Call 1: Extract invoice data (line items, addresses, amounts)."""
    loop = asyncio.get_running_loop()

    if is_vision:
        user_content: str | list[dict[str, Any]] = _build_vision_content_blocks(
            pages_data, "invoice"
        )
        model = LLM_MODEL_VISION
    else:
        user_content = _build_text_user_message(pages_data, "invoice")
        model = LLM_MODEL_TEXT

    system_prompt = _INVOICE_PROMPT
    if correction_hints:
        system_prompt += f"\n\nCommon corrections to watch for:\n{correction_hints}"

    raw_response = await loop.run_in_executor(
        None,
        partial(_call_llm, model, system_prompt, user_content, LLM_MAX_TOKENS_INVOICE),
    )
    logger.info("Invoice LLM response: %d chars from %s", len(raw_response), model)

    try:
        return _extract_json_from_response(raw_response)
    except (json.JSONDecodeError, ValueError) as first_err:
        logger.warning("Invoice JSON parse failed (%s), retrying", first_err)
        retry_response = await loop.run_in_executor(
            None,
            partial(_call_retry, model, system_prompt, user_content, LLM_MAX_TOKENS_INVOICE),
        )
        return _extract_json_from_response(retry_response)


async def _extract_packing_list(
    pages_data: list[dict[str, Any]],
    is_vision: bool,
    correction_hints: str = "",
) -> dict[str, Any]:
    """Call 2: Extract packing list data (destinations + all boxes, flat schema)."""
    loop = asyncio.get_running_loop()

    # Packing list always uses the higher-capacity model
    if is_vision:
        user_content: str | list[dict[str, Any]] = _build_vision_content_blocks(
            pages_data, "packing list"
        )
    else:
        user_content = _build_text_user_message(pages_data, "packing list")

    model = LLM_MODEL_PACKING_LIST

    system_prompt = _PACKING_LIST_PROMPT
    if correction_hints:
        system_prompt += f"\n\nCommon corrections to watch for:\n{correction_hints}"

    raw_response = await loop.run_in_executor(
        None,
        partial(_call_llm, model, system_prompt, user_content, LLM_MAX_TOKENS_PACKING_LIST),
    )
    logger.info("Packing list LLM response: %d chars from %s", len(raw_response), model)

    try:
        return _extract_json_from_response(raw_response)
    except (json.JSONDecodeError, ValueError) as first_err:
        logger.warning("Packing list JSON parse failed (%s), retrying", first_err)
        retry_response = await loop.run_in_executor(
            None,
            partial(_call_retry, model, system_prompt, user_content, LLM_MAX_TOKENS_PACKING_LIST),
        )
        return _extract_json_from_response(retry_response)


def _merge_results(
    invoice_data: dict[str, Any],
    packing_data: dict[str, Any],
) -> ExtractionResult:
    """Merge invoice and packing list LLM outputs into a single ExtractionResult."""
    warnings: list[str] = []

    invoice = _parse_invoice(invoice_data.get("invoice"))
    packing_list = _parse_flat_packing_list(packing_data)

    # Determine fallback receiver for boxes without a destination match
    fallback = invoice.ship_to
    if all(
        getattr(fallback, f).value in (None, "")
        for f in ["name", "address", "city", "country"]
    ):
        fallback = invoice.consignee

    # Resolve per-box receiver addresses
    _resolve_box_receivers(packing_list, fallback)

    # Check for unresolved destination IDs
    dest_ids = {d.id for d in packing_list.destinations}
    unresolved = set()
    for box in packing_list.boxes:
        did = box.destination_id.value
        if did and str(did) not in dest_ids:
            unresolved.add(str(did))
    if unresolved:
        warnings.append(
            f"Boxes reference unknown destination IDs: {', '.join(sorted(unresolved))}"
        )

    result = ExtractionResult(
        status="review_needed",
        invoice=invoice,
        packing_list=packing_list,
        warnings=warnings,
    )

    result.overall_confidence = _compute_overall_confidence(result)

    if result.overall_confidence >= 0.90 and not warnings:
        result.status = "completed"

    return result


# ---------------------------------------------------------------------------
# Public async interface
# ---------------------------------------------------------------------------


async def extract_from_text(
    pages_data: list[dict[str, Any]],
    doc_type: str = "AUTO",
) -> ExtractionResult:
    """Extract structured invoice / packing-list data from PDF pages via Claude.

    Runs two concurrent LLM calls:
      1. Invoice extraction (Haiku 4.5, 8K tokens)
      2. Packing list extraction (Sonnet 4.5, 16K tokens, flat schema)

    Parameters
    ----------
    pages_data:
        List of page dicts as returned by ``pdf_processor.process_pdf()["pages"]``.
        Each dict has ``page_num``, ``text``, ``tables``, ``image_bytes``.
    doc_type:
        One of ``"INVOICE"``, ``"PACKING_LIST"``, ``"COMBINED"``, or ``"AUTO"``
        (auto-detect). Kept for API compatibility but both calls always
        receive all pages.

    Returns
    -------
    ExtractionResult
        Validated pydantic model with invoice data, packing-list data,
        confidence scores, and any warnings/errors.
    """
    if not pages_data:
        logger.warning("extract_from_text called with empty pages_data")
        return ExtractionResult(
            status="failed",
            errors=["No pages to extract data from."],
        )

    is_vision = any(p.get("image_bytes") for p in pages_data)

    # Run both extractions concurrently; return_exceptions=True so one
    # failure doesn't discard the other's result.
    results = await asyncio.gather(
        _extract_invoice(pages_data, is_vision),
        _extract_packing_list(pages_data, is_vision),
        return_exceptions=True,
    )

    invoice_data: dict[str, Any] | Exception = results[0]
    packing_data: dict[str, Any] | Exception = results[1]
    gather_errors: list[str] = []

    if isinstance(invoice_data, Exception):
        logger.exception("Invoice extraction failed: %s", invoice_data)
        gather_errors.append(f"Invoice extraction failed: {invoice_data}")
        invoice_data = {}

    if isinstance(packing_data, Exception):
        logger.exception("Packing list extraction failed: %s", packing_data)
        gather_errors.append(f"Packing list extraction failed: {packing_data}")
        packing_data = {}

    # If both calls failed, return early with a clear error
    if len(gather_errors) == 2:
        return ExtractionResult(
            status="failed",
            errors=gather_errors,
        )

    # ------------------------------------------------------------------
    # Merge results from both calls
    # ------------------------------------------------------------------
    try:
        result = _merge_results(invoice_data, packing_data)
        # Propagate partial-failure warnings
        if gather_errors:
            result.warnings = gather_errors + result.warnings
            result.status = "review_needed"
        logger.info(
            "Extraction complete -- status=%s, confidence=%.2f, boxes=%d, destinations=%d",
            result.status,
            result.overall_confidence,
            len(result.packing_list.boxes),
            len(result.packing_list.destinations),
        )
        return result
    except Exception as exc:
        logger.exception("Error merging LLM results into ExtractionResult")
        return ExtractionResult(
            status="failed",
            errors=[f"Error mapping LLM output to data model: {exc}"],
        )
