"""Xindus UAT API client — authenticate and submit B2B shipments.

Uses the Express-Shipment API (multipart: Excel + JSON) which creates
proper B2B shipments (b2b_shipment=1, source=B2B).
"""
from __future__ import annotations

import json
import logging
import time
from io import BytesIO
from typing import Any

import httpx
from openpyxl import Workbook
from openpyxl.styles import Alignment

from backend.config import XINDUS_UAT_URL, XINDUS_UAT_USERNAME, XINDUS_UAT_PASSWORD

logger = logging.getLogger(__name__)

# In-memory token cache
_token: str | None = None
_token_expires: float = 0


async def _authenticate() -> str:
    """Login to Xindus UAT and return a Bearer token (cached)."""
    global _token, _token_expires

    if _token and time.time() < _token_expires:
        return _token

    if not XINDUS_UAT_USERNAME or not XINDUS_UAT_PASSWORD:
        raise RuntimeError("XINDUS_UAT_USERNAME / XINDUS_UAT_PASSWORD not configured")

    url = f"{XINDUS_UAT_URL}/xos/api/auth/login"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json={
            "username": XINDUS_UAT_USERNAME,
            "password": XINDUS_UAT_PASSWORD,
        })

    if resp.status_code != 200:
        raise RuntimeError(f"Xindus auth failed ({resp.status_code}): {resp.text[:200]}")

    data = resp.json()
    # Token is nested: {"data": [{"access_token": "..."}]}
    token_data = (data.get("data") or [{}])[0] if isinstance(data.get("data"), list) else data
    _token = token_data.get("access_token") or data.get("access_token") or data.get("token")
    if not _token:
        raise RuntimeError(f"No access_token in auth response: {data}")

    # Cache for 55 minutes (tokens typically last 1h)
    _token_expires = time.time() + 55 * 60
    logger.info("Xindus UAT auth successful, token cached")
    return _token


def _clear_token() -> None:
    """Clear cached token (used on 401 retry)."""
    global _token, _token_expires
    _token = None
    _token_expires = 0


def _normalize_hs(val: Any) -> str | None:
    """Normalize an HS/HTS code string for Excel.

    Keeps as STRING to preserve leading zeros — Xindus Java parser
    uses getCellString() which handles both STRING and NUMERIC cells.
    Export HSN (ehsn) must be exactly 8 characters for validation.
    Returns None for empty/missing values (cell left blank).
    """
    if val is None or val == "":
        return None
    s = str(val).replace(".", "").replace(" ", "").strip()
    return s if s else None


def _build_excel(shipment_data: dict[str, Any]) -> bytes:
    """Generate XpressB2B Excel for Xindus Express-Shipment API.

    Headers, sheet name, and layout must match the official Xindus
    "FBA Split shipment" template exactly.
    - Single-address (multiAddressDestinationDelivery=false): 12 columns
    - Multi-address  (multiAddressDestinationDelivery=true):  21 columns
    First item row of each box has all box-level columns filled;
    subsequent items leave box-level columns empty (None).
    """
    # Detect mode from the payload (camelCase key from frontend)
    multi_addr = shipment_data.get("multiAddressDestinationDelivery", False)
    if not multi_addr:
        multi_addr = shipment_data.get("multi_address_destination_delivery", False)

    # ── Column headers — must match Xindus template EXACTLY ──
    if multi_addr:
        columns = [
            "Box Number (R)",
            "Receiver Name (R)",
            "Receiver Address (R)",
            "Receiver City (R)",
            "Receiver Zip (R)",
            "Receiver State (R)",
            "Receiver Country (R)",
            "Receiver Phone Number (R)",
            "Receiver Extension No (O)",
            "Receiver Email (R)",
            "Box Length(cms)(R)",
            "Box Width(cms)(R)",
            "Box Height(cms)(R)",
            "Box Weight(kgs)(R)",
            "Item Description (R)",
            "Item Qty (R)",
            "Item Unit Weight Kg (O)",
            "HS Code(Origin)(R)",
            "HS Code(Destination) (O)",
            "Item Unit Price (R)",
            "IGST % (For GST taxtype)",
        ]
        box_col_count = 14  # columns 1-14 are box-level
    else:
        columns = [
            "Box Number (R)",
            "Box Length(cms)(R)",
            "Box Width(cms)(R)",
            "Box Height(cms)(R)",
            "Box Weight(kgs)(R)",
            "Item Description (R)",
            "Item Qty (R)",
            "Item Unit Weight Kg (O)",
            "HS Code(Origin)(R)",
            "HS Code(Destination) (O)",
            "Item Unit Price (R)",
            "IGST % (For GST taxtype)",
        ]
        box_col_count = 5  # columns 1-5 are box-level

    wb = Workbook()
    ws = wb.active
    ws.title = "FBA Split shipment"

    # Header row
    for ci, h in enumerate(columns, start=1):
        ws.cell(row=1, column=ci, value=h)

    # Fallback receiver from top-level
    top_recv = shipment_data.get("receiverAddress") or shipment_data.get("receiver_address") or {}
    boxes = (shipment_data.get("shipmentBoxes")
             or shipment_data.get("shipment_boxes")
             or [])

    current_row = 2
    for box in boxes:
        items = (box.get("shipmentBoxItems")
                 or box.get("shipment_box_items")
                 or [{}])

        box_id = box.get("boxId", box.get("box_id", 0))

        for j, item in enumerate(items):
            is_first = j == 0

            if multi_addr:
                recv = box.get("receiverAddress") or box.get("receiver_address") or {}
                if not recv.get("name"):
                    recv = top_recv
                if is_first:
                    row_data: list[Any] = [
                        box_id,
                        recv.get("name", ""),
                        recv.get("address", ""),
                        recv.get("city", ""),
                        recv.get("zip", ""),
                        recv.get("state", ""),
                        recv.get("country", ""),
                        recv.get("phone", ""),
                        recv.get("extensionNumber", recv.get("extension_number", "")),
                        recv.get("email", ""),
                        box.get("length", 0),
                        box.get("width", 0),
                        box.get("height", 0),
                        box.get("weight", 0),
                    ]
                else:
                    row_data = [None] * box_col_count
            else:
                if is_first:
                    row_data = [
                        box_id,
                        box.get("length", 0),
                        box.get("width", 0),
                        box.get("height", 0),
                        box.get("weight", 0),
                    ]
                else:
                    row_data = [None] * box_col_count

            # Item-level columns (same for both formats).
            # HS codes kept as STRINGS to preserve leading zeros —
            # Xindus getCellString() handles both STRING and NUMERIC cells,
            # but ehsn validation requires exactly 8 characters.
            row_data.extend([
                item.get("description", "") or None,
                item.get("quantity", 0),
                item.get("weight", 0) or None,
                _normalize_hs(item.get("ehsn", "")),
                _normalize_hs(item.get("ihsn", "")),
                item.get("unitPrice", item.get("unit_price", 0)),
                item.get("igst", item.get("igst_amount", 0)) or None,
            ])

            for ci, val in enumerate(row_data, start=1):
                if val is None:
                    continue
                cell = ws.cell(row=current_row, column=ci, value=val)
                cell.alignment = Alignment(vertical="center", wrap_text=True)

            current_row += 1

    ws.freeze_panes = "A2"

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


async def upload_document(
    file_data: bytes,
    filename: str,
    docs_type: str = "FULFIL_LABEL",
) -> str:
    """Upload a file to Xindus CDN via POST /xos/api/file.

    Returns the CDN URL string. Xindus stores files at ucdn.xindus.net.
    """
    token = await _authenticate()
    url = f"{XINDUS_UAT_URL}/xos/api/file"

    files = [("file", (filename, file_data, "application/pdf"))]
    data_fields = {
        "docs_type": docs_type,
        "reference_string": "0",
        "is_private": "false",
    }

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            url,
            files=files,
            data=data_fields,
            headers={"Authorization": f"Bearer {token}"},
        )

    if resp.status_code == 401:
        # Retry with fresh token
        _clear_token()
        token = await _authenticate()
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                url,
                files=files,
                data=data_fields,
                headers={"Authorization": f"Bearer {token}"},
            )

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Xindus file upload failed ({resp.status_code}): {resp.text[:300]}")

    body = resp.json()
    raw_data = body.get("data")
    if isinstance(raw_data, list) and raw_data:
        cdn_url = raw_data[0]
        logger.info("Xindus file upload OK: %s → %s", filename, cdn_url)
        return cdn_url
    raise RuntimeError(f"No URL in Xindus file upload response: {body}")


async def submit_b2b_shipment(
    shipment_data: dict[str, Any],
    consignor_id: int | None = None,
) -> tuple[int, dict[str, Any]]:
    """Submit a B2B shipment to Xindus UAT via the Express-Shipment API.

    This uses multipart form data (Excel + JSON) which creates proper B2B
    shipments (b2b_shipment=1, source=B2B) unlike the Partner API.

    Returns (http_status, response_body).
    Retries once on 401 with a fresh token.
    """
    # 1. Generate Excel from shipment boxes
    excel_bytes = _build_excel(shipment_data)

    # 2. Build the express-shipment JSON payload (flat format, no shipment_config wrapper)
    json_payload = json.dumps(shipment_data)

    # 3. Build URL
    url = f"{XINDUS_UAT_URL}/xos/api/express-shipment/create"
    if consignor_id:
        url += f"?consignor_id={consignor_id}"

    for attempt in range(2):
        token = await _authenticate()

        # Both parts need explicit Content-Type headers for Spring Boot:
        # - box_details_file: Excel with spreadsheet MIME type
        # - create_shipment_data: JSON blob with application/json
        files = [
            ("box_details_file", ("uploadedFile.xlsx", excel_bytes,
                                  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
            ("create_shipment_data", (None, json_payload.encode("utf-8"),
                                      "application/json")),
        ]

        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                url,
                files=files,
                headers={"Authorization": f"Bearer {token}"},
            )

        if resp.status_code == 401 and attempt == 0:
            logger.warning("Xindus returned 401, refreshing token and retrying")
            _clear_token()
            continue

        try:
            body = resp.json()
        except Exception:
            body = {"raw_response": resp.text[:2000]}

        logger.info("Xindus express-shipment response: %d %s",
                     resp.status_code, str(body)[:300])
        return resp.status_code, body

    return 500, {"error": "Failed after retry"}
