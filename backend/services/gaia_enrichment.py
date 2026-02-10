"""Orchestrate Gaia classification and tariff enrichment for draft items.

Pipeline (parallel dedup approach):
  1. Collect ALL items across ALL boxes
  2. Normalize each description (deterministic: same input → same output)
  3. Deduplicate by normalized description (track original→normalized mapping)
  4. Batch check DB cache for ALL distinct hashes in one query
  5. For cache misses, call Gaia in PARALLEL via asyncio.gather()
  6. Fan results back to ALL items sharing the same normalized description
  7. Cache all new results in DB

Rules:
- EHSN: keep document-extracted value if non-empty; fall back to Gaia code
- IHSN: always from Gaia (documents don't have import codes)
- duty_rate: always from Gaia tariff detail (inline in autonomous response)
- Cache by normalized description + destination + origin country
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from backend import db
from backend.services import gaia_client
from backend.services.gaia_client import parse_duty_rate
from backend.services.description_normalizer import normalize_description

logger = logging.getLogger(__name__)


# ── Data structures ──────────────────────────────────────────────────────

@dataclass
class ItemRef:
    """Reference to a specific item inside a box (by index)."""
    box_idx: int
    item_idx: int
    original_description: str


@dataclass
class DescriptionGroup:
    """A group of items sharing the same normalized description."""
    normalized: str
    desc_hash: str
    items: list[ItemRef] = field(default_factory=list)


@dataclass
class GaiaResult:
    """Parsed Gaia classification result ready to apply to items."""
    ihsn: str = ""
    ehsn_fallback: str = ""
    duty_rate: float | None = None
    confidence: str = ""
    gaia_response: dict[str, Any] | None = None


# ── Helpers ──────────────────────────────────────────────────────────────

def _hash_key(normalized_desc: str, destination: str, origin: str) -> str:
    """Deterministic cache key from normalized description + countries."""
    raw = f"{normalized_desc}|{destination}|{origin}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _parse_gaia_response(data: dict[str, Any]) -> GaiaResult:
    """Extract usable fields from a Gaia autonomous classification response."""
    code = data.get("best_guess_code") or ""
    confidence = data.get("confidence") or ""

    tariff_detail = data.get("tariff_detail") or {}
    rod = tariff_detail.get("rate_of_duty") or {}
    duty_rate = parse_duty_rate(rod.get("general"))

    return GaiaResult(
        ihsn=code,
        ehsn_fallback=code,  # Use as EHSN only if document didn't provide one
        duty_rate=duty_rate,
        confidence=confidence,
        gaia_response=data,
    )


def _parse_cached_row(row: dict[str, Any]) -> GaiaResult:
    """Convert a DB cache row into a GaiaResult."""
    return GaiaResult(
        ihsn=row.get("ihsn") or "",
        ehsn_fallback=row.get("ehsn") or "",
        duty_rate=row.get("duty_rate"),
        confidence=row.get("confidence") or "",
        gaia_response=row.get("classification_response"),
    )


def _apply_result(item: dict[str, Any], result: GaiaResult, norm_desc: str) -> None:
    """Apply a Gaia result to a shipment item dict, following the rules."""
    item["ihsn"] = result.ihsn
    if not item.get("ehsn"):
        item["ehsn"] = result.ehsn_fallback
    if result.duty_rate is not None:
        item["duty_rate"] = result.duty_rate
    item["gaia_classified"] = True
    item["gaia_description"] = norm_desc


# ── Main enrichment (parallel dedup) ────────────────────────────────────

async def enrich_items_with_gaia(
    shipment_data: dict[str, Any],
    destination_country: str = "US",
    origin_country: str = "IN",
) -> dict[str, Any]:
    """Enrich all box items with Gaia IHSN, duty%, and optionally EHSN.

    Uses parallel dedup approach:
    - Deduplicates descriptions so each unique product is classified once
    - Checks DB cache in a single batch query
    - Calls Gaia in parallel for all cache misses
    - Fans results back to all items sharing the same description

    Modifies shipment_data in-place and returns it.
    """
    boxes = shipment_data.get("shipment_boxes") or []
    if not boxes:
        return shipment_data

    # ── Step 1: Collect ALL items + normalize + deduplicate ──
    groups: dict[str, DescriptionGroup] = {}  # keyed by desc_hash

    for b_idx, box in enumerate(boxes):
        items = box.get("shipment_box_items") or []
        for i_idx, item in enumerate(items):
            description = item.get("description") or ""
            if not description.strip():
                continue

            norm_desc = normalize_description(description)
            if not norm_desc:
                continue

            desc_hash = _hash_key(norm_desc, destination_country, origin_country)

            if desc_hash not in groups:
                groups[desc_hash] = DescriptionGroup(
                    normalized=norm_desc,
                    desc_hash=desc_hash,
                )
            groups[desc_hash].items.append(
                ItemRef(box_idx=b_idx, item_idx=i_idx, original_description=description)
            )

    if not groups:
        return shipment_data

    distinct_count = len(groups)
    total_items = sum(len(g.items) for g in groups.values())
    logger.info(
        "Gaia enrichment: %d total items → %d distinct descriptions",
        total_items, distinct_count,
    )

    # ── Step 2: Batch cache check for ALL distinct hashes ──
    all_hashes = list(groups.keys())
    cached_rows: dict[str, dict[str, Any]] = {}
    try:
        cached_rows = await db.get_gaia_classifications_batch(
            all_hashes, destination_country, origin_country,
        )
    except Exception:
        logger.warning("Gaia batch cache lookup failed", exc_info=True)

    cache_hits = len(cached_rows)
    cache_misses = distinct_count - cache_hits
    logger.info(
        "Gaia cache: %d hits, %d misses out of %d distinct",
        cache_hits, cache_misses, distinct_count,
    )

    # ── Step 3: Apply cached results immediately ──
    results: dict[str, GaiaResult] = {}  # keyed by desc_hash
    miss_hashes: list[str] = []

    for desc_hash, group in groups.items():
        if desc_hash in cached_rows:
            results[desc_hash] = _parse_cached_row(cached_rows[desc_hash])
        else:
            miss_hashes.append(desc_hash)

    # ── Step 4: Call Gaia in PARALLEL for all cache misses ──
    if miss_hashes:
        async def _classify_one(h: str) -> tuple[str, GaiaResult | None]:
            g = groups[h]
            data = await gaia_client.classify_autonomous(
                name=g.normalized,
                description=g.normalized,
                destination_country=destination_country,
            )
            if data:
                return (h, _parse_gaia_response(data))
            return (h, None)

        # Run all Gaia calls concurrently
        tasks = [_classify_one(h) for h in miss_hashes]
        gaia_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in gaia_results:
            if isinstance(result, Exception):
                logger.warning("Gaia parallel call failed: %s", result)
                continue
            h, parsed = result
            if parsed:
                results[h] = parsed

                # Cache the new result
                g = groups[h]
                try:
                    await db.upsert_gaia_classification(
                        desc_hash=h,
                        normalized_description=g.normalized,
                        destination=destination_country,
                        origin=origin_country,
                        ehsn=parsed.ehsn_fallback,
                        ihsn=parsed.ihsn,
                        duty_rate=parsed.duty_rate,
                        confidence=parsed.confidence,
                        classification_response=parsed.gaia_response,
                        tariff_response=None,  # tariff is inline in classification_response
                    )
                except Exception:
                    logger.warning("Failed to cache Gaia result for hash %s", h[:12], exc_info=True)

    # ── Step 5: Fan results back to ALL items ──
    applied = 0
    for desc_hash, group in groups.items():
        result = results.get(desc_hash)
        if not result:
            continue
        for ref in group.items:
            item = boxes[ref.box_idx]["shipment_box_items"][ref.item_idx]
            _apply_result(item, result, group.normalized)
            applied += 1

    logger.info(
        "Gaia enrichment complete: %d/%d items enriched (%d distinct descriptions)",
        applied, total_items, len(results),
    )

    return shipment_data


# ── Manual re-classify endpoint helper ───────────────────────────────────

async def classify_draft_items(draft_id: UUID) -> dict[str, Any] | None:
    """Load a draft from DB, enrich its items with Gaia, and save back.

    Used by the manual "Classify with Gaia" endpoint.
    Returns the updated shipment_data or None on failure.
    """
    draft = await db.get_draft(draft_id)
    if not draft:
        return None

    # Use corrected_data if available, else shipment_data
    sd = draft.get("corrected_data") or draft.get("shipment_data") or {}
    if isinstance(sd, str):
        sd = json.loads(sd)

    # Determine destination country from receiver address
    receiver = sd.get("receiver_address") or {}
    dest_country = receiver.get("country") or "US"
    dest_country = dest_country.strip().upper()[:2] if dest_country else "US"

    # Determine origin country from shipper address
    shipper = sd.get("shipper_address") or {}
    origin_country = shipper.get("country") or "IN"
    origin_country = origin_country.strip().upper()[:2] if origin_country else "IN"

    # Enrich
    enriched = await enrich_items_with_gaia(sd, dest_country, origin_country)

    # Save back as corrected_data
    await db.update_draft_corrections(draft_id, enriched)

    return enriched
