"""Router for downloading generated output files.

Endpoints
---------
GET  /api/download/{job_id}/multi           -- XpressB2B Multi Address Excel sheet.
GET  /api/download/{job_id}/simplified      -- Simplified Template Excel sheet.
GET  /api/download/{job_id}/result          -- Raw extraction result as JSON.
GET  /api/download/{job_id}/xindus_single   -- Xindus single-address (12 col) sheet.
GET  /api/download/{job_id}/xindus_multi    -- Xindus multi-address (21 col) sheet.
POST /api/download/generate-xindus          -- Generate Xindus Excel from extraction JSON.
"""
from __future__ import annotations

import json
import logging
import os

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response

from backend.config import UPLOAD_DIR

logger = logging.getLogger(__name__)

router = APIRouter(tags=["export"])

# ---------------------------------------------------------------------------
# File name mapping
# ---------------------------------------------------------------------------
_FILE_MAP: dict[str, tuple[str, str]] = {
    "multi": (
        "XpressB2BMultiAddressSheet.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    "simplified": (
        "SimplifiedTemplate.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    "b2b_shipment": (
        "B2BShipment.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    "result": (
        "extraction_result.json",
        "application/json",
    ),
}


def _resolve_file(job_id: str, file_key: str) -> tuple[str, str, str]:
    """Resolve a file key to an absolute path, media type, and download name.

    Raises ``HTTPException(404)`` when the job directory or file does not
    exist.
    """
    if file_key not in _FILE_MAP:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown file type '{file_key}'. "
                   f"Valid options: {', '.join(sorted(_FILE_MAP))}.",
        )

    filename, media_type = _FILE_MAP[file_key]
    job_dir = os.path.join(UPLOAD_DIR, job_id)

    if not os.path.isdir(job_dir):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    file_path = os.path.join(job_dir, filename)

    if not os.path.isfile(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"Output file '{filename}' has not been generated for job '{job_id}'.",
        )

    return file_path, media_type, filename


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/download/{job_id}/multi")
async def download_multi_address(job_id: str) -> FileResponse:
    """Download the XpressB2B Multi Address Excel sheet for the given job."""
    file_path, media_type, filename = _resolve_file(job_id, "multi")
    logger.info("Serving multi-address sheet for job %s: %s", job_id, file_path)
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename,
    )


@router.get("/api/download/{job_id}/simplified")
async def download_simplified(job_id: str) -> FileResponse:
    """Download the Simplified Template Excel sheet for the given job."""
    file_path, media_type, filename = _resolve_file(job_id, "simplified")
    logger.info("Serving simplified template for job %s: %s", job_id, file_path)
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename,
    )


@router.get("/api/download/{job_id}/b2b_shipment")
async def download_b2b_shipment(job_id: str) -> FileResponse:
    """Download the B2B Shipment block-format Excel sheet for the given job."""
    file_path, media_type, filename = _resolve_file(job_id, "b2b_shipment")
    logger.info("Serving B2B shipment sheet for job %s: %s", job_id, file_path)
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename,
    )


@router.get("/api/download/{job_id}/result")
async def download_result(job_id: str) -> FileResponse:
    """Download the raw extraction result JSON for the given job."""
    file_path, media_type, filename = _resolve_file(job_id, "result")
    logger.info("Serving extraction JSON for job %s: %s", job_id, file_path)
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename,
    )


# ---------------------------------------------------------------------------
# Xindus-format downloads (generated on-the-fly from extraction result)
# ---------------------------------------------------------------------------

def _cv(field) -> str:
    """Extract value from a ConfidenceValue dict."""
    if isinstance(field, dict):
        return str(field.get("value", "") or "")
    return str(field or "")


def _extraction_to_shipment_data(result: dict, multi_address: bool) -> dict:
    """Map extraction result JSON to the shipment_data format _build_excel() expects."""
    invoice = result.get("invoice", {})
    packing = result.get("packing_list", {})

    # Extract addresses
    def _addr(a: dict) -> dict:
        return {
            "name": _cv(a.get("name", "")),
            "email": _cv(a.get("email", "")),
            "phone": _cv(a.get("phone", "")),
            "address": _cv(a.get("address", "")),
            "city": _cv(a.get("city", "")),
            "state": _cv(a.get("state", "")),
            "zip": _cv(a.get("zip_code", a.get("zip", ""))),
            "country": _cv(a.get("country", "")),
        }

    exporter = invoice.get("exporter", {})
    consignee = invoice.get("consignee", {})
    ship_to = invoice.get("ship_to", {})

    # Use ship_to as receiver if it has data, else consignee
    recv = ship_to if _cv(ship_to.get("name", "")) else consignee

    # Build line items lookup for matching
    line_items = invoice.get("line_items", [])

    # Build shipment boxes
    boxes_data = packing.get("boxes", [])
    destinations = packing.get("destinations", [])
    shipment_boxes = []

    for i, box in enumerate(boxes_data):
        # Build items for this box
        box_items_raw = box.get("items", [])
        items = []
        if box_items_raw:
            for bi in box_items_raw:
                items.append({
                    "description": _cv(bi.get("description", "")),
                    "quantity": _cv(bi.get("quantity", 1)),
                    "weight": _cv(bi.get("unit_weight_kg", 0)),
                    "ehsn": _cv(bi.get("hs_code_origin", "")),
                    "ihsn": _cv(bi.get("hs_code_destination", "")),
                    "unit_price": _cv(bi.get("unit_price_usd", 0)),
                    "igst_amount": _cv(bi.get("igst_percent", 0)),
                })
        else:
            # No box items — use all line items
            for li in line_items:
                items.append({
                    "description": _cv(li.get("description", "")),
                    "quantity": _cv(li.get("quantity", 1)),
                    "weight": _cv(li.get("unit_weight_kg", 0)),
                    "ehsn": _cv(li.get("hs_code_origin", "")),
                    "ihsn": _cv(li.get("hs_code_destination", "")),
                    "unit_price": _cv(li.get("unit_price_usd", 0)),
                    "igst_amount": _cv(li.get("igst_percent", 0)),
                })

        # Get receiver for this box from destinations
        box_receiver = {}
        dest_id = _cv(box.get("destination_id", ""))
        if dest_id and destinations:
            for d in destinations:
                if d.get("id") == dest_id:
                    box_receiver = _addr(d.get("address", d))
                    break

        shipment_boxes.append({
            "box_id": i + 1,
            "length": _cv(box.get("length_cm", 0)),
            "width": _cv(box.get("width_cm", 0)),
            "height": _cv(box.get("height_cm", 0)),
            "weight": _cv(box.get("gross_weight_kg", 0)),
            "receiver_address": box_receiver,
            "shipment_box_items": items,
        })

    # If no boxes from packing, create one box with all line items
    if not shipment_boxes and line_items:
        items = []
        for li in line_items:
            items.append({
                "description": _cv(li.get("description", "")),
                "quantity": _cv(li.get("quantity", 1)),
                "weight": _cv(li.get("unit_weight_kg", 0)),
                "ehsn": _cv(li.get("hs_code_origin", "")),
                "ihsn": _cv(li.get("hs_code_destination", "")),
                "unit_price": _cv(li.get("unit_price_usd", 0)),
                "igst_amount": _cv(li.get("igst_percent", 0)),
            })
        shipment_boxes.append({
            "box_id": 1,
            "length": 0, "width": 0, "height": 0, "weight": 0,
            "receiver_address": _addr(recv),
            "shipment_box_items": items,
        })

    return {
        "multiAddressDestinationDelivery": multi_address,
        "shipper_address": _addr(exporter),
        "receiver_address": _addr(recv),
        "shipment_boxes": shipment_boxes,
    }


@router.get("/api/download/{job_id}/xindus_single")
async def download_xindus_single(job_id: str) -> Response:
    """Download Xindus single-address (12-col) Excel sheet."""
    return await _xindus_download(job_id, multi_address=False)


@router.get("/api/download/{job_id}/xindus_multi")
async def download_xindus_multi(job_id: str) -> Response:
    """Download Xindus multi-address (21-col) Excel sheet."""
    return await _xindus_download(job_id, multi_address=True)


async def _xindus_download(job_id: str, multi_address: bool) -> Response:
    """Generate Xindus-format Excel on-the-fly from extraction result on disk."""
    from backend.services.xindus_client import _build_excel

    job_dir = os.path.join(UPLOAD_DIR, job_id)
    if not os.path.isdir(job_dir):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    result_path = os.path.join(job_dir, "extraction_result.json")
    if not os.path.isfile(result_path):
        raise HTTPException(status_code=404, detail="Extraction result not found.")

    with open(result_path) as f:
        result = json.load(f)

    shipment_data = _extraction_to_shipment_data(result, multi_address)
    excel_bytes = _build_excel(shipment_data)

    suffix = "Multi" if multi_address else "Single"
    filename = f"Xindus_{suffix}_{job_id[:8]}.xlsx"

    return Response(
        content=excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# POST endpoint — generate Xindus Excel from extraction JSON (no filesystem)
# ---------------------------------------------------------------------------

@router.post("/api/download/generate-xindus")
async def generate_xindus_excel(
    request: Request,
    format: str = Query("single", regex="^(single|multi)$"),
) -> Response:
    """Generate Xindus-format Excel from POSTed extraction result JSON.

    Unlike the GET endpoints above, this does NOT depend on the job
    existing on disk — the caller sends the full extraction result in
    the request body.  This makes it resilient to Railway redeploys.
    """
    from backend.services.xindus_client import _build_excel

    body = await request.json()
    result = body.get("extraction_result") or body
    multi_address = format == "multi"

    shipment_data = _extraction_to_shipment_data(result, multi_address)
    excel_bytes = _build_excel(shipment_data)

    suffix = "Multi" if multi_address else "Single"
    filename = f"Xindus_{suffix}_Address.xlsx"

    return Response(
        content=excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
