"""Router for downloading generated output files.

Endpoints
---------
GET /api/download/{job_id}/multi      -- XpressB2B Multi Address Excel sheet.
GET /api/download/{job_id}/simplified  -- Simplified Template Excel sheet.
GET /api/download/{job_id}/result      -- Raw extraction result as JSON.
"""
from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

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
