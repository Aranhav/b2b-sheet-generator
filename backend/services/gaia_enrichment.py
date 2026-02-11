"""Orchestrate Gaia classification and tariff enrichment for draft items.

Pipeline (parallel dedup approach with two-step tariff lookup):
  1. Collect ALL items across ALL boxes
  2. Normalize each description (deterministic: same input → same output)
  3. Deduplicate by normalized description (track original→normalized mapping)
  4. Batch check DB cache for ALL distinct hashes in one query
  5. For cache misses:
     a. Call classify_autonomous() in PARALLEL → get IHSN codes
     b. Call get_tariff_detail() in PARALLEL → get tariff scenarios + cumulative duty
  6. Fan results back to ALL items sharing the same normalized description
  7. Cache all new results in DB

Duty calculation (XOS-exact formula):
  base_rate = SUM(tariff_base[*].rules[*].value WHERE kind="percent")
  scenario_sum = SUM(tariff_scenario[*].value WHERE is_approved AND is_additional AND value>=0)
  cumulative_duty = base_rate + scenario_sum

Confidence gate (XOS-exact):
  Only apply IHSN and tariff data when confidence is HIGH or MEDIUM.
  LOW or missing confidence → skip IHSN, duty_rate, tariff_scenarios.

Rules:
- EHSN: keep document-extracted value if non-empty; fall back to Gaia code
- IHSN: from Gaia (only when confidence is HIGH/MEDIUM)
- duty_rate: cumulative duty from tariff-detail (only when confidence is HIGH/MEDIUM)
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
from backend.config import normalize_country_code
from backend.services import gaia_client
from backend.services.gaia_client import parse_duty_rate, calculate_cumulative_duty
from backend.services.description_normalizer import normalize_description, llm_normalize_batch

logger = logging.getLogger(__name__)

# XOS confidence gate: only apply IHSN and tariff data for these confidence levels.
# LOW or missing confidence → skip tariff application entirely.
_ALLOWED_CONFIDENCES = {"HIGH", "MEDIUM"}


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
    llm_normalized: str = ""  # Claude-enhanced description for Gaia


@dataclass
class GaiaResult:
    """Parsed Gaia classification + tariff result ready to apply to items."""
    ihsn: str = ""
    ehsn_fallback: str = ""
    duty_rate: float | None = None
    base_duty_rate: float | None = None
    confidence: str = ""
    tariff_scenarios: list[dict[str, Any]] = field(default_factory=list)
    remedy_flags: dict[str, bool] = field(default_factory=dict)
    gaia_response: dict[str, Any] | None = None
    tariff_response: dict[str, Any] | None = None


# ── Helpers ──────────────────────────────────────────────────────────────

def _hash_key(normalized_desc: str, destination: str, origin: str) -> str:
    """Deterministic cache key from normalized description + countries."""
    raw = f"{normalized_desc}|{destination}|{origin}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _parse_gaia_response(
    classification_data: dict[str, Any],
    tariff_data: dict[str, Any] | None,
) -> GaiaResult:
    """Build a GaiaResult from classification + tariff-detail responses."""
    code = classification_data.get("best_guess_code") or ""
    confidence = classification_data.get("confidence") or ""

    # Try cumulative duty from tariff-detail endpoint first
    base_rate: float | None = None
    cumulative_rate: float | None = None
    scenario_summaries: list[dict[str, Any]] = []
    remedy_flags: dict[str, bool] = {}

    if tariff_data:
        base_rate, cumulative_rate, scenario_summaries = calculate_cumulative_duty(tariff_data)

        # Extract remedy flags
        for flag in tariff_data.get("flags") or []:
            if flag.get("name") == "remedy":
                val = flag.get("value") or {}
                remedy_flags = {
                    "add_risk": bool(val.get("possible_add_required_indicator")),
                    "cvd_risk": bool(val.get("possible_cvd_duty_required_indicator")),
                }

    # Fallback: if tariff-detail failed, use base rate from autonomous inline tariff_detail
    if cumulative_rate is None:
        inline_td = classification_data.get("tariff_detail") or {}
        rod = inline_td.get("rate_of_duty") or {}
        fallback_rate = parse_duty_rate(rod.get("general"))
        if fallback_rate is not None:
            base_rate = fallback_rate
            cumulative_rate = fallback_rate  # No scenarios available, base = cumulative

    return GaiaResult(
        ihsn=code,
        ehsn_fallback=code,
        duty_rate=cumulative_rate,
        base_duty_rate=base_rate,
        confidence=confidence,
        tariff_scenarios=scenario_summaries,
        remedy_flags=remedy_flags,
        gaia_response=classification_data,
        tariff_response=tariff_data,
    )


def _parse_cached_row(row: dict[str, Any]) -> GaiaResult:
    """Convert a DB cache row into a GaiaResult."""
    tariff_resp = row.get("tariff_response")

    # Reconstruct scenarios and remedy from cached tariff_response
    base_rate: float | None = None
    scenario_summaries: list[dict[str, Any]] = []
    remedy_flags: dict[str, bool] = {}

    if tariff_resp and isinstance(tariff_resp, dict):
        base_rate, _, scenario_summaries = calculate_cumulative_duty(tariff_resp)
        for flag in tariff_resp.get("flags") or []:
            if flag.get("name") == "remedy":
                val = flag.get("value") or {}
                remedy_flags = {
                    "add_risk": bool(val.get("possible_add_required_indicator")),
                    "cvd_risk": bool(val.get("possible_cvd_duty_required_indicator")),
                }

    return GaiaResult(
        ihsn=row.get("ihsn") or "",
        ehsn_fallback=row.get("ehsn") or "",
        duty_rate=row.get("duty_rate"),
        base_duty_rate=base_rate,
        confidence=row.get("confidence") or "",
        tariff_scenarios=scenario_summaries,
        remedy_flags=remedy_flags,
        gaia_response=row.get("classification_response"),
        tariff_response=tariff_resp,
    )


def _apply_result(item: dict[str, Any], result: GaiaResult, norm_desc: str) -> None:
    """Apply a Gaia result to a shipment box item dict, following XOS rules.

    Confidence gate (XOS-exact): only apply IHSN and tariff data when
    confidence is HIGH or MEDIUM. LOW/missing → skip tariff application.
    """
    # Always mark as classified and store confidence + description
    item["gaia_classified"] = True
    item["gaia_description"] = norm_desc
    if result.confidence:
        item["hsn_confidence"] = result.confidence

    # Confidence gate: skip IHSN and tariff data for LOW/missing confidence
    conf = (result.confidence or "").upper()
    if conf not in _ALLOWED_CONFIDENCES:
        logger.debug("Skipping tariff for '%s' (confidence=%s)", norm_desc[:40], conf)
        return

    item["ihsn"] = result.ihsn
    if not item.get("ehsn"):
        item["ehsn"] = result.ehsn_fallback
    if result.duty_rate is not None:
        item["duty_rate"] = result.duty_rate
    if result.base_duty_rate is not None:
        item["base_duty_rate"] = result.base_duty_rate
    if result.tariff_scenarios:
        item["tariff_scenarios"] = result.tariff_scenarios
    if result.remedy_flags:
        item["remedy_flags"] = result.remedy_flags


def _apply_result_to_product(product: dict[str, Any], result: GaiaResult, norm_desc: str) -> None:
    """Apply a Gaia result to a product_details item (customs summary).

    product_details uses hsn_code (export HSN) and ihsn (import HSN).
    Confidence gate (XOS-exact): skip IHSN and tariff for LOW/missing.
    """
    # Always mark as classified and store confidence
    product["gaia_classified"] = True
    product["gaia_description"] = norm_desc
    if result.confidence:
        product["hsn_confidence"] = result.confidence

    # Confidence gate
    conf = (result.confidence or "").upper()
    if conf not in _ALLOWED_CONFIDENCES:
        return

    product["ihsn"] = result.ihsn
    if not product.get("hsn_code"):
        product["hsn_code"] = result.ehsn_fallback
    if result.duty_rate is not None:
        product["duty_rate"] = result.duty_rate
    if result.base_duty_rate is not None:
        product["base_duty_rate"] = result.base_duty_rate
    if result.tariff_scenarios:
        product["tariff_scenarios"] = result.tariff_scenarios


# ── Main enrichment (parallel dedup) ────────────────────────────────────

async def enrich_items_with_gaia(
    shipment_data: dict[str, Any],
    destination_country: str = "US",
    origin_country: str = "IN",
) -> dict[str, Any]:
    """Enrich all box items with Gaia IHSN, cumulative duty%, and optionally EHSN.

    Two-step lookup per unique description:
      1. classify_autonomous() → IHSN code
      2. get_tariff_detail() → tariff scenarios → cumulative duty

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

    # ── Step 3b: LLM-normalize descriptions for cache misses (one batch call) ──
    if miss_hashes:
        miss_originals: list[str] = []
        for h in miss_hashes:
            g = groups[h]
            # Use the first item's original description for LLM normalization
            raw = g.items[0].original_description if g.items else g.normalized
            miss_originals.append(raw)

        llm_map = await llm_normalize_batch(miss_originals)

        # Assign LLM-normalized descriptions to groups
        for h, raw in zip(miss_hashes, miss_originals):
            g = groups[h]
            g.llm_normalized = llm_map.get(raw, "")
            if g.llm_normalized:
                logger.info(
                    "LLM normalize: '%s' → '%s' (regex: '%s')",
                    raw[:50], g.llm_normalized, g.normalized[:50],
                )

    # ── Step 4: Classify + tariff lookup in PARALLEL for cache misses ──
    if miss_hashes:
        async def _classify_and_lookup(h: str) -> tuple[str, GaiaResult | None]:
            """Two-step: classify → tariff detail → cumulative duty."""
            g = groups[h]

            # Use LLM-normalized description for Gaia if available, else regex
            gaia_desc = g.llm_normalized or g.normalized

            # Step 4a: Classify to get IHSN code
            classification = await gaia_client.classify_autonomous(
                name=gaia_desc,
                description=gaia_desc,
                destination_country=destination_country,
            )
            if not classification:
                return (h, None)

            # Step 4b: Get tariff detail for the classified code
            ihsn_code = classification.get("best_guess_code") or ""
            tariff_detail = None
            if ihsn_code:
                tariff_detail = await gaia_client.get_tariff_detail(
                    destination_country=destination_country,
                    tariff_code=ihsn_code,
                    origin_country=origin_country,
                )

            return (h, _parse_gaia_response(classification, tariff_detail))

        # Run all two-step lookups concurrently
        tasks = [_classify_and_lookup(h) for h in miss_hashes]
        gaia_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in gaia_results:
            if isinstance(result, Exception):
                logger.warning("Gaia parallel call failed: %s", result)
                continue
            h, parsed = result
            if parsed:
                results[h] = parsed

                # Cache the new result (with cumulative duty and tariff_response)
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
                        tariff_response=parsed.tariff_response,
                        llm_normalized=g.llm_normalized or None,
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

    # ── Step 6: Also enrich product_details[] (customs summary) ──
    product_details = shipment_data.get("product_details") or []
    products_enriched = 0
    for product in product_details:
        desc = product.get("product_description") or ""
        if not desc.strip():
            continue
        norm_desc = normalize_description(desc)
        if not norm_desc:
            continue
        desc_hash = _hash_key(norm_desc, destination_country, origin_country)
        result = results.get(desc_hash)
        if result:
            _apply_result_to_product(product, result, norm_desc)
            products_enriched += 1

    logger.info(
        "Gaia enrichment complete: %d/%d box items, %d/%d products enriched (%d distinct)",
        applied, total_items, products_enriched, len(product_details), len(results),
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
    dest_country = normalize_country_code(dest_country, "US")

    # Determine origin country from shipper address
    shipper = sd.get("shipper_address") or {}
    origin_country = shipper.get("country") or "IN"
    origin_country = normalize_country_code(origin_country, "IN")

    # Enrich
    enriched = await enrich_items_with_gaia(sd, dest_country, origin_country)

    # Save back as corrected_data
    await db.update_draft_corrections(draft_id, enriched)

    return enriched
