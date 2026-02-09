"""Shipment grouping: cluster extracted files into shipment groups.

Uses deterministic matching on key fields (invoice number, seller name,
consignee, dates, destination country) with fuzzy string matching.
Falls back to filename pattern matching when extracted fields are sparse.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any

from thefuzz import fuzz

from backend.utils import normalize_name as _normalize_name

logger = logging.getLogger(__name__)

# Minimum fuzzy match ratio to consider names equivalent
_FUZZY_THRESHOLD = 85

# Maximum days apart for dates to be considered "same shipment"
_DATE_RANGE_DAYS = 7


# ---------------------------------------------------------------------------
# Field extraction helpers
# ---------------------------------------------------------------------------


def _get_value(data: dict[str, Any], *paths: str) -> str | None:
    """Walk a nested dict to extract a value. Handles ConfidenceValue wrappers."""
    for path in paths:
        obj = data
        for key in path.split("."):
            if not isinstance(obj, dict):
                obj = None
                break
            obj = obj.get(key)

        if obj is None:
            continue

        # Handle ConfidenceValue wrapper
        if isinstance(obj, dict) and "value" in obj:
            val = obj["value"]
        else:
            val = obj

        if val is not None and str(val).strip():
            return str(val).strip()

    return None


def _parse_date(date_str: str | None) -> datetime | None:
    """Try to parse a date string in common formats."""
    if not date_str:
        return None

    for fmt in [
        "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y",
        "%d.%m.%Y", "%Y/%m/%d", "%d %b %Y", "%d %B %Y",
        "%B %d, %Y", "%b %d, %Y",
    ]:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------


# Regex to extract identifier-like tokens from filenames (invoice nums, codes)
# Matches: letter-prefix + digit sequences (WFS-042025-26, USA-14, INVOICE-1)
_IDENTIFIER_RE = re.compile(
    r"[A-Za-z]+[\-/]?\d[\w\-/]*|\d[\w\-/]*[A-Za-z]+[\w\-/]*"
)


def _filename_identifiers(filename: str) -> set[str]:
    """Extract identifier-like tokens from a filename.

    E.g. "01. INVOICE_WFS-042025-26 - inv.pdf" → {"WFS-042025-26"}
         "AASHIRWAD GARMENTS USA-14 (1).xlsx - INVOICE-1.pdf" → {"USA-14", "INVOICE-1"}
    """
    stem = filename.rsplit(".", 1)[0] if "." in filename else filename
    tokens = _IDENTIFIER_RE.findall(stem)
    return {t.upper() for t in tokens if len(t) >= 3}


class _FileMetadata:
    """Extracted metadata from a single file for grouping purposes."""

    def __init__(self, file_id: str, file_type: str, extracted_data: dict[str, Any], filename: str = ""):
        self.file_id = file_id
        self.file_type = file_type
        self.data = extracted_data or {}
        self.filename = filename
        self.filename_ids = _filename_identifiers(filename) if filename else set()

        # Extract key fields depending on document type
        if file_type == "invoice":
            inv = self.data.get("invoice", self.data)
            self.invoice_number = _get_value(inv, "invoice_number")
            self.po_number = _get_value(inv, "po_number", "purchase_order_number")
            self.seller_name = _normalize_name(
                _get_value(inv, "exporter.name", "seller.name", "shipper.name")
            )
            self.buyer_name = _normalize_name(
                _get_value(inv, "consignee.name", "buyer.name", "importer.name")
            )
            self.date_str = _get_value(inv, "invoice_date", "date")
            self.date = _parse_date(self.date_str)
            self.dest_country = _get_value(
                inv,
                "ship_to.country",
                "consignee.country",
            )
            self.container_number = _get_value(inv, "container_number", "bl_number")
        elif file_type == "packing_list":
            pl = self.data
            self.invoice_number = _get_value(
                pl, "invoice_number", "invoice_ref",
            )
            self.po_number = _get_value(pl, "po_number")
            self.seller_name = _normalize_name(
                _get_value(pl, "exporter_name", "exporter.name", "shipper.name")
            )
            self.buyer_name = _normalize_name(
                _get_value(pl, "consignee_name", "consignee.name")
            )
            self.date_str = _get_value(pl, "date", "packing_date")
            self.date = _parse_date(self.date_str)
            dest = pl.get("destinations", [])
            self.dest_country = dest[0].get("country", None) if dest else None
            if isinstance(self.dest_country, dict):
                self.dest_country = self.dest_country.get("value")
            self.container_number = _get_value(pl, "container_number", "bl_number")
        else:
            self.invoice_number = _get_value(self.data, "invoice_number", "reference_number")
            self.po_number = _get_value(self.data, "po_number")
            self.seller_name = _normalize_name(_get_value(self.data, "exporter.name", "issuer.name"))
            self.buyer_name = _normalize_name(_get_value(self.data, "consignee.name", "applicant.name"))
            self.date_str = _get_value(self.data, "date", "issue_date")
            self.date = _parse_date(self.date_str)
            self.dest_country = _get_value(self.data, "country", "destination_country")
            self.container_number = _get_value(self.data, "container_number", "bl_number")


def _match_score(file_meta: _FileMetadata, group_metas: list[_FileMetadata]) -> float:
    """Compute a matching score between a file and a group (0.0 to 1.0).

    Uses weighted signals:
      - Invoice/PO number exact match: 0.35
      - Seller name fuzzy match: 0.25
      - Buyer name fuzzy match: 0.15
      - Date proximity: 0.10
      - Destination country: 0.10
      - Container/BL number: 0.05
    """
    best_score = 0.0

    for gm in group_metas:
        score = 0.0

        # 1. Invoice / PO number exact match (strongest signal)
        if file_meta.invoice_number and gm.invoice_number:
            if file_meta.invoice_number.upper() == gm.invoice_number.upper():
                score += 0.35
        elif file_meta.po_number and gm.po_number:
            if file_meta.po_number.upper() == gm.po_number.upper():
                score += 0.35

        # 2. Seller/exporter name fuzzy match
        if file_meta.seller_name and gm.seller_name:
            ratio = fuzz.ratio(file_meta.seller_name, gm.seller_name)
            if ratio >= _FUZZY_THRESHOLD:
                score += 0.25 * (ratio / 100.0)

        # 3. Buyer/consignee name fuzzy match
        if file_meta.buyer_name and gm.buyer_name:
            ratio = fuzz.ratio(file_meta.buyer_name, gm.buyer_name)
            if ratio >= _FUZZY_THRESHOLD:
                score += 0.15 * (ratio / 100.0)

        # 4. Date proximity
        if file_meta.date and gm.date:
            delta = abs((file_meta.date - gm.date).days)
            if delta <= _DATE_RANGE_DAYS:
                score += 0.10 * (1.0 - delta / _DATE_RANGE_DAYS)

        # 5. Destination country
        if file_meta.dest_country and gm.dest_country:
            if file_meta.dest_country.upper() == gm.dest_country.upper():
                score += 0.10

        # 6. Container / BL number
        if file_meta.container_number and gm.container_number:
            if file_meta.container_number.upper() == gm.container_number.upper():
                score += 0.05

        # 7. Filename identifier overlap (fallback when LLM fields are sparse)
        if file_meta.filename_ids and gm.filename_ids:
            overlap = file_meta.filename_ids & gm.filename_ids
            if overlap:
                # Strong signal: shared identifier tokens in filenames
                score += 0.30

        best_score = max(best_score, score)

    return best_score


# Minimum score to assign a file to an existing group
_GROUP_THRESHOLD = 0.30


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def group_files_into_shipments(
    files: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Group extracted files into shipment clusters.

    Parameters
    ----------
    files:
        List of dicts, each with keys:
            id (str/UUID), file_type (str), extracted_data (dict)

    Returns
    -------
    List of shipment groups, each a dict with:
        file_ids: list of file IDs in the group
        reason: human-readable explanation of grouping
        primary_invoice: the extracted_data from the primary invoice (if any)
    """
    if not files:
        return []

    # Build metadata for each file
    metas = []
    for f in files:
        meta = _FileMetadata(
            file_id=str(f["id"]),
            file_type=f.get("file_type", "other"),
            extracted_data=f.get("extracted_data", {}),
            filename=f.get("filename", ""),
        )
        metas.append(meta)

    # Greedy grouping: try to assign each file to an existing group
    groups: list[list[_FileMetadata]] = []

    for meta in metas:
        best_group_idx = -1
        best_score = 0.0

        for idx, group in enumerate(groups):
            score = _match_score(meta, group)
            if score > best_score and score >= _GROUP_THRESHOLD:
                best_score = score
                best_group_idx = idx

        if best_group_idx >= 0:
            groups[best_group_idx].append(meta)
            logger.debug(
                "Grouped '%s' (%s) with group %d (score=%.2f, inv=%s, seller=%s, fname_ids=%s)",
                meta.filename, meta.file_type, best_group_idx, best_score,
                meta.invoice_number, meta.seller_name, meta.filename_ids,
            )
        else:
            groups.append([meta])
            logger.debug(
                "New group %d for '%s' (%s): inv=%s, seller=%s, fname_ids=%s",
                len(groups) - 1, meta.filename, meta.file_type,
                meta.invoice_number, meta.seller_name, meta.filename_ids,
            )

    # Build output
    result = []
    for group in groups:
        file_ids = [m.file_id for m in group]

        # Generate grouping reason
        reasons = []
        invoices = [m for m in group if m.file_type == "invoice"]
        if invoices and invoices[0].invoice_number:
            reasons.append(f"Invoice #{invoices[0].invoice_number}")
        if invoices and invoices[0].seller_name:
            reasons.append(f"Seller: {invoices[0].seller_name}")
        if not reasons:
            names = [m.seller_name or m.buyer_name for m in group if m.seller_name or m.buyer_name]
            if names:
                reasons.append(f"Party: {names[0]}")

        reason = " | ".join(reasons) if reasons else f"{len(file_ids)} file(s) grouped"

        result.append({
            "file_ids": file_ids,
            "reason": reason,
        })

    logger.info(
        "Grouped %d files into %d shipment(s): %s",
        len(metas),
        len(result),
        [r["reason"][:60] for r in result],
    )
    return result
