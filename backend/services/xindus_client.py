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


def _build_excel(shipment_data: dict[str, Any]) -> bytes:
    """Generate XpressB2B 21-column Excel from shipment_data in memory."""
    COLUMNS = [
        "Box Number",
        "Receiver Name",
        "Receiver Address",
        "Receiver City",
        "Receiver Zip",
        "Receiver State",
        "Receiver Country",
        "Receiver Phone Number",
        "Receiver Extension No",
        "Receiver Email",
        "Box Length (cms)",
        "Box Width (cms)",
        "Box Height (cms)",
        "Box Weight (kgs)",
        "Item Description",
        "Item Qty",
        "Item Unit Weight Kg",
        "HS Code (Origin)",
        "HS Code (Destination)",
        "Item Unit Price",
        "IGST % (For GST taxtype)",
    ]
    BOX_COL_COUNT = 14

    wb = Workbook()
    ws = wb.active
    ws.title = "XpressB2B Multi Address"

    # Header row
    for ci, h in enumerate(COLUMNS, start=1):
        ws.cell(row=1, column=ci, value=h)

    # Fallback receiver from top-level
    top_recv = shipment_data.get("receiver_address", {}) or {}
    boxes = shipment_data.get("shipment_boxes", []) or []

    current_row = 2
    for box in boxes:
        items = box.get("shipment_box_items", []) or [{}]
        recv = box.get("receiver_address", {}) or {}
        if not recv.get("name"):
            recv = top_recv

        for j, item in enumerate(items):
            is_first = j == 0
            if is_first:
                row_data = [
                    box.get("box_id", ""),
                    recv.get("name", ""),
                    recv.get("address", ""),
                    recv.get("city", ""),
                    recv.get("zip", ""),
                    recv.get("state", ""),
                    recv.get("country", ""),
                    recv.get("phone", ""),
                    recv.get("extension_number", ""),
                    recv.get("email", ""),
                    box.get("length", ""),
                    box.get("width", ""),
                    box.get("height", ""),
                    box.get("weight", ""),
                ]
            else:
                row_data = [""] * BOX_COL_COUNT

            # Item-level columns (15-21)
            row_data.extend([
                item.get("description", ""),
                item.get("quantity", ""),
                item.get("weight", ""),
                item.get("ehsn", ""),
                item.get("ihsn", ""),
                item.get("unit_price", ""),
                item.get("igst_amount", ""),
            ])

            for ci, val in enumerate(row_data, start=1):
                cell = ws.cell(row=current_row, column=ci, value=val if val else "")
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

    if resp.status_code != 200:
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
