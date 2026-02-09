"""Per-seller intelligence: match sellers, apply defaults, harvest learnings.

Zero extra LLM tokens — all logic is DB lookups and dict merging.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from thefuzz import fuzz

from backend import db
from backend.utils import normalize_name

logger = logging.getLogger(__name__)

# Fuzzy match threshold for seller name matching (0-100)
_FUZZY_THRESHOLD = 85

# Fields that are stable across shipments and stored as seller defaults.
_DEFAULT_FIELDS: list[str] = [
    "shipping_method",
    "origin_clearance_type",
    "destination_clearance_type",
    "terms_of_trade",
    "purpose_of_booking",
    "tax_type",
    "exporter_category",
    "shipping_currency",
    "billing_currency",
    "amazon_fba",
    "self_drop",
    "self_origin_clearance",
    "self_destination_clearance",
    "port_of_entry",
    "destination_cha",
    "export_reference",
    "marketplace",
]

# Address objects stored as defaults
_DEFAULT_ADDRESS_FIELDS: list[str] = [
    "billing_address",
    "ior_address",
]


# ---------------------------------------------------------------------------
# Match or create seller
# ---------------------------------------------------------------------------


async def match_or_create_seller(
    shipper_name: str,
    shipper_address: dict[str, Any] | None = None,
) -> tuple[UUID, dict[str, Any] | None]:
    """Look up a seller by name, or create a new profile.

    Returns (seller_id, defaults_dict_or_None).
    defaults is None for newly-created sellers.
    """
    norm = normalize_name(shipper_name)
    if not norm:
        # No usable name — create with raw name
        seller_id = await db.upsert_seller(
            name=shipper_name or "Unknown",
            normalized_name=shipper_name or "UNKNOWN",
            shipper_address=shipper_address,
        )
        return seller_id, None

    # 1. Exact match on normalized_name
    seller = await db.get_seller_by_normalized_name(norm)
    if seller:
        defaults = _parse_jsonb(seller.get("defaults"))
        return seller["id"], defaults if defaults else None

    # 2. Fuzzy fallback — scan all sellers
    all_sellers = await db.get_all_sellers()
    best_match: dict[str, Any] | None = None
    best_ratio = 0

    for s in all_sellers:
        s_norm = s.get("normalized_name", "")
        ratio = fuzz.ratio(norm, s_norm)
        if ratio >= _FUZZY_THRESHOLD and ratio > best_ratio:
            best_ratio = ratio
            best_match = s

    if best_match:
        defaults = _parse_jsonb(best_match.get("defaults"))
        return best_match["id"], defaults if defaults else None

    # 3. New seller — create
    seller_id = await db.upsert_seller(
        name=shipper_name,
        normalized_name=norm,
        shipper_address=shipper_address,
    )
    return seller_id, None


# ---------------------------------------------------------------------------
# Apply seller defaults to blank fields
# ---------------------------------------------------------------------------


def apply_seller_defaults(
    shipment_data: dict[str, Any],
    seller_defaults: dict[str, Any],
    shipper_address: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fill blank fields in shipment_data from seller defaults.

    Only fills fields that are empty/missing/falsy in the extracted data.
    Returns the mutated shipment_data.
    """
    # Scalar default fields
    for field in _DEFAULT_FIELDS:
        if field in seller_defaults and _is_blank(shipment_data.get(field)):
            shipment_data[field] = seller_defaults[field]

    # Address defaults (billing_address, ior_address)
    for addr_field in _DEFAULT_ADDRESS_FIELDS:
        default_addr = seller_defaults.get(addr_field)
        if not default_addr or not isinstance(default_addr, dict):
            continue

        current_addr = shipment_data.get(addr_field)
        if not current_addr or not isinstance(current_addr, dict):
            shipment_data[addr_field] = default_addr
        else:
            # Fill individual blank address fields
            for key, val in default_addr.items():
                if _is_blank(current_addr.get(key)):
                    current_addr[key] = val

    # Optionally apply stored shipper_address to blank shipper fields
    if shipper_address and isinstance(shipper_address, dict):
        current_shipper = shipment_data.get("shipper_address")
        if isinstance(current_shipper, dict):
            for key, val in shipper_address.items():
                if _is_blank(current_shipper.get(key)):
                    current_shipper[key] = val

    return shipment_data


# ---------------------------------------------------------------------------
# Extract defaults from an approved shipment
# ---------------------------------------------------------------------------


def extract_defaults_from_shipment(
    shipment_data: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Pull default-worthy fields from final shipment data.

    Returns (defaults_dict, shipper_address_dict).
    """
    defaults: dict[str, Any] = {}

    for field in _DEFAULT_FIELDS:
        val = shipment_data.get(field)
        if not _is_blank(val):
            defaults[field] = val

    for addr_field in _DEFAULT_ADDRESS_FIELDS:
        val = shipment_data.get(addr_field)
        if val and isinstance(val, dict) and any(v for v in val.values() if not _is_blank(v)):
            defaults[addr_field] = val

    shipper_address = shipment_data.get("shipper_address", {})
    if not isinstance(shipper_address, dict):
        shipper_address = {}

    return defaults, shipper_address


# ---------------------------------------------------------------------------
# Harvest: update seller profile after approval
# ---------------------------------------------------------------------------


async def harvest_seller_defaults(
    seller_id: UUID,
    shipment_data: dict[str, Any],
) -> None:
    """Update seller defaults from an approved shipment and increment count."""
    defaults, shipper_address = extract_defaults_from_shipment(shipment_data)

    # Merge with existing defaults (new values overwrite)
    seller = await db.get_seller(seller_id)
    if seller:
        existing = _parse_jsonb(seller.get("defaults")) or {}
        existing.update(defaults)
        defaults = existing

    await db.update_seller_defaults(seller_id, defaults, shipper_address or None)
    await db.increment_seller_shipment_count(seller_id)

    logger.info(
        "Harvested %d default fields for seller %s",
        len(defaults),
        seller_id,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_blank(val: Any) -> bool:
    """Check if a value is blank/empty/falsy (but 0 and False are valid)."""
    if val is None:
        return True
    if isinstance(val, str) and not val.strip():
        return True
    if isinstance(val, dict) and not val:
        return True
    if isinstance(val, list) and not val:
        return True
    return False


def _parse_jsonb(val: Any) -> dict[str, Any] | None:
    """Parse a JSONB value that may be a string or already a dict."""
    import json
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return None
    return None
