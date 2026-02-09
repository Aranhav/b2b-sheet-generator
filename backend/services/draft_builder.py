"""Draft shipment builder: merge per-file extractions into Xindus B2B format.

Takes a group of classified/extracted files and produces a shipment_data
dict that matches the Xindus B2BShipmentCreateRequestDTO exactly, along with
per-field confidence scores.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------


def _cv(data: Any, *paths: str) -> str | None:
    """Extract a value from nested dicts, handling ConfidenceValue wrappers."""
    for path in paths:
        obj = data
        for key in path.split("."):
            if not isinstance(obj, dict):
                obj = None
                break
            obj = obj.get(key)
        if obj is None:
            continue
        if isinstance(obj, dict) and "value" in obj:
            val = obj["value"]
        else:
            val = obj
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def _cv_float(data: Any, *paths: str) -> float | None:
    """Extract a numeric value."""
    val = _cv(data, *paths)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _cv_int(data: Any, *paths: str) -> int | None:
    """Extract an integer value."""
    val = _cv(data, *paths)
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _confidence(data: Any, *paths: str) -> float:
    """Extract the confidence score for a field."""
    for path in paths:
        obj = data
        for key in path.split("."):
            if not isinstance(obj, dict):
                obj = None
                break
            obj = obj.get(key)
        if isinstance(obj, dict) and "confidence" in obj:
            return float(obj["confidence"])
    return 0.0


# ---------------------------------------------------------------------------
# Address mapping → Xindus AddressRequestDTO
# ---------------------------------------------------------------------------

_EMPTY_ADDRESS: dict[str, Any] = {
    "name": "",
    "email": "",
    "phone": "",
    "address": "",
    "city": "",
    "zip": "",
    "district": "",
    "state": "",
    "country": "",
    "extension_number": "",
    "eori_number": "",
    "contact_name": "",
    "contact_phone": "",
    "warehouse_id": None,
    "type": None,
}


def _map_address(addr_data: dict[str, Any] | None) -> dict[str, Any]:
    """Map an extracted address to Xindus AddressRequestDTO format."""
    if not addr_data or not isinstance(addr_data, dict):
        return dict(_EMPTY_ADDRESS)
    return {
        "name": _cv(addr_data, "name") or "",
        "email": _cv(addr_data, "email") or "",
        "phone": _cv(addr_data, "phone") or "",
        "address": _cv(addr_data, "address") or "",
        "city": _cv(addr_data, "city") or "",
        "zip": _cv(addr_data, "zip_code", "zip") or "",
        "district": _cv(addr_data, "district") or "",
        "state": _cv(addr_data, "state") or "",
        "country": _cv(addr_data, "country") or "",
        "extension_number": _cv(addr_data, "extension_number") or "",
        "eori_number": _cv(addr_data, "eori_number") or "",
        "contact_name": _cv(addr_data, "contact_name") or "",
        "contact_phone": _cv(addr_data, "contact_phone") or "",
        "warehouse_id": _cv(addr_data, "warehouse_id") or None,
        "type": _cv(addr_data, "type") or None,
    }


def _address_confidence(addr_data: dict[str, Any] | None) -> dict[str, float]:
    """Get per-field confidence for an address."""
    if not addr_data or not isinstance(addr_data, dict):
        return {}
    scores = {}
    for field in ["name", "address", "city", "state", "zip_code", "country", "phone", "email"]:
        c = _confidence(addr_data, field)
        if c > 0:
            # Map zip_code confidence key to "zip" for new format
            key = "zip" if field == "zip_code" else field
            scores[key] = c
    return scores


# ---------------------------------------------------------------------------
# Box / product mapping → Xindus ShipmentBoxRequestDTO
# ---------------------------------------------------------------------------


def _map_boxes(
    packing_data: dict[str, Any] | None,
    line_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Map extracted packing list boxes to Xindus ShipmentBoxRequestDTO format."""
    boxes_out = []

    if not packing_data:
        return boxes_out

    boxes = packing_data.get("boxes", [])
    destinations = packing_data.get("destinations", [])

    # Build destination lookup
    dest_map: dict[str, dict[str, Any]] = {}
    for d in destinations:
        did = d.get("id", "")
        if isinstance(did, dict):
            did = did.get("value", "")
        dest_map[str(did)] = d

    for box in boxes:
        dest_id = _cv(box, "destination_id") or ""
        dest = dest_map.get(dest_id, {})
        receiver = _map_address(dest) if dest else dict(_EMPTY_ADDRESS)

        # Map box items → Xindus ShipmentBoxItemRequestDTO
        items = []
        for bi in box.get("items", []):
            desc = _cv(bi, "description") or ""
            qty = _cv_int(bi, "quantity") or 0

            # Try to find matching line item for price/HS code
            matched_li = _find_matching_line_item(desc, line_items)

            items.append({
                "description": desc,
                "quantity": qty,
                "weight": _cv_float(matched_li, "unit_weight_kg") if matched_li else None,
                "unit_price": _cv_float(matched_li, "unit_price_usd") if matched_li else None,
                "total_price": _cv_float(matched_li, "total_price_usd") if matched_li else None,
                "ehsn": _cv(matched_li, "hs_code_origin") if matched_li else "",
                "ihsn": _cv(matched_li, "hs_code_destination") if matched_li else "",
                "country_of_origin": _cv(matched_li, "country_of_origin") or "",
                "category": "",
                "market_place": "",
                "igst_amount": _cv_float(matched_li, "igst_percent") if matched_li else None,
                "duty_rate": None,
                "vat_rate": None,
                "unit_fob_value": None,
                "fob_value": None,
                "listing_price": None,
                "cogs_value": None,
                "insurance": None,
                "remarks": "",
            })

        boxes_out.append({
            "box_id": str(_cv_int(box, "box_number") or (len(boxes_out) + 1)),
            "weight": _cv_float(box, "gross_weight_kg") or 0,
            "width": _cv_float(box, "width_cm") or 0,
            "length": _cv_float(box, "length_cm") or 0,
            "height": _cv_float(box, "height_cm") or 0,
            "uom": "cm",
            "has_battery": False,
            "remarks": "",
            "receiver_address": receiver,
            "shipment_box_items": items,
        })

    return boxes_out


def _find_matching_line_item(
    description: str,
    line_items: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Find the best matching invoice line item by description similarity."""
    if not description or not line_items:
        return None

    desc_lower = description.lower()
    best_match = None
    best_score = 0

    for li in line_items:
        li_desc = _cv(li, "description") or ""
        li_lower = li_desc.lower()
        if not li_lower:
            continue

        # Simple containment check
        if desc_lower in li_lower or li_lower in desc_lower:
            score = 100
        else:
            # Word overlap score
            words_a = set(desc_lower.split())
            words_b = set(li_lower.split())
            common = words_a & words_b
            if words_a or words_b:
                score = len(common) * 100 // max(len(words_a), len(words_b), 1)
            else:
                score = 0

        if score > best_score:
            best_score = score
            best_match = li

    return best_match if best_score >= 40 else None


def _map_products(line_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map invoice line items to Xindus ProductDetailDTO format."""
    products = []
    for li in line_items:
        products.append({
            "product_description": _cv(li, "description") or "",
            "hsn_code": _cv(li, "hs_code_origin", "hs_code_destination") or "",
            "value": _cv_float(li, "total_price_usd") or 0,
        })
    return products


def _check_multi_address(boxes: list[dict[str, Any]]) -> bool:
    """Check if boxes have different receiver addresses (multi-destination)."""
    if len(boxes) <= 1:
        return False
    first_addr = boxes[0].get("receiver_address", {})
    first_key = (
        first_addr.get("address", ""),
        first_addr.get("city", ""),
        first_addr.get("zip", ""),
    )
    for box in boxes[1:]:
        addr = box.get("receiver_address", {})
        key = (addr.get("address", ""), addr.get("city", ""), addr.get("zip", ""))
        if key != first_key and any(key):
            return True
    return False


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------


def build_draft_shipment(
    file_group: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a Xindus-format shipment draft from a group of extracted files.

    Parameters
    ----------
    file_group:
        List of file dicts with keys: id, file_type, extracted_data

    Returns
    -------
    Tuple of (shipment_data, confidence_scores)
    """
    # Separate files by type
    invoices = [f for f in file_group if f.get("file_type") == "invoice"]
    packing_lists = [f for f in file_group if f.get("file_type") == "packing_list"]

    # Use primary invoice and packing list
    invoice_data = {}
    packing_data = {}

    if invoices:
        raw = invoices[0].get("extracted_data", {})
        invoice_data = raw.get("invoice", raw)

    if packing_lists:
        packing_data = packing_lists[0].get("extracted_data", {})

    # Extract line items for cross-referencing with boxes
    line_items = invoice_data.get("line_items", [])

    # Build mapped components
    shipper_address = _map_address(invoice_data.get("exporter"))
    receiver_address = _map_address(
        invoice_data.get("ship_to") or invoice_data.get("consignee")
    )
    billing_address = _map_address(invoice_data.get("consignee"))
    ior_address = _map_address(invoice_data.get("ior"))
    shipment_boxes = _map_boxes(packing_data, line_items)
    product_details = _map_products(line_items)

    # Determine multi-address delivery
    multi_addr = _check_multi_address(shipment_boxes)

    # Detect country from receiver
    country = receiver_address.get("country", "") or ""

    # Build shipment data matching B2BShipmentCreateRequestDTO exactly
    shipment_data = {
        # Shipment method & clearance
        "shipping_method": "AN",
        "origin_clearance_type": "",
        "destination_clearance_type": "",
        "terms_of_trade": "",
        "purpose_of_booking": "",
        "tax_type": "",
        "amazon_fba": False,
        "multi_address_destination_delivery": multi_addr,
        "country": country,

        # Addresses
        "shipper_address": shipper_address,
        "receiver_address": receiver_address,
        "billing_address": billing_address,
        "ior_address": ior_address,

        # Boxes and products
        "shipment_boxes": shipment_boxes,
        "product_details": product_details,

        # Invoice / financial
        "invoice_date": _cv(invoice_data, "invoice_date") or "",
        "shipping_currency": _cv(invoice_data, "currency") or "USD",
        "billing_currency": "",

        # References
        "export_reference": "",
        "shipment_references": "",
        "exporter_category": "",
        "marketplace": "",

        # Logistics options
        "self_drop": False,
        "self_origin_clearance": False,
        "self_destination_clearance": False,
        "port_of_entry": "",
        "destination_cha": "",

        # Extra metadata (not in DTO but useful for UI)
        "invoice_number": _cv(invoice_data, "invoice_number") or "",
        "total_amount": _cv_float(invoice_data, "total_amount") or 0,
        "total_boxes": _cv_int(packing_data, "total_boxes") or len(packing_data.get("boxes", [])),
        "total_gross_weight_kg": _cv_float(packing_data, "total_gross_weight_kg") or 0,
        "total_net_weight_kg": _cv_float(packing_data, "total_net_weight_kg") or 0,
    }

    # Build confidence map
    confidence_scores = {
        "shipper_address": _address_confidence(invoice_data.get("exporter")),
        "receiver_address": _address_confidence(
            invoice_data.get("ship_to") or invoice_data.get("consignee")
        ),
        "billing_address": _address_confidence(invoice_data.get("consignee")),
        "ior_address": _address_confidence(invoice_data.get("ior")),
        "invoice_number": _confidence(invoice_data, "invoice_number"),
        "invoice_date": _confidence(invoice_data, "invoice_date"),
        "shipping_currency": _confidence(invoice_data, "currency"),
        "total_amount": _confidence(invoice_data, "total_amount"),
    }

    # Overall confidence: average of top-level fields
    all_confidences = [
        v for v in confidence_scores.values()
        if isinstance(v, (int, float)) and v > 0
    ]
    confidence_scores["_overall"] = (
        round(sum(all_confidences) / len(all_confidences), 3)
        if all_confidences else 0.0
    )

    logger.info(
        "Built draft: %d boxes, %d products, overall confidence=%.2f",
        len(shipment_data.get("shipment_boxes", [])),
        len(shipment_data.get("product_details", [])),
        confidence_scores.get("_overall", 0),
    )

    return shipment_data, confidence_scores
