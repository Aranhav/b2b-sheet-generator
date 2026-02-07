"""Router for PDF upload, extraction, and human-review updates.

Endpoints
---------
POST /api/extract         -- Upload one or more PDFs, extract structured data,
                             generate Excel output files.
GET  /api/jobs/{job_id}   -- Retrieve the status and result of a prior extraction.
POST /api/jobs/{job_id}/update -- Patch a single extracted field after human review
                                  and re-generate output files.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Any

import aiofiles
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from backend.config import ALLOWED_EXTENSIONS, MAX_FILE_SIZE_MB, UPLOAD_DIR
from backend.exporters.b2b_shipment import generate_b2b_shipment
from backend.exporters.simplified_template import generate_simplified_template
from backend.exporters.xpressb2b_multi import generate_multi_address
from backend.models.extraction import ExtractionResult, JobStatus
from backend.services.llm_extractor import extract_from_text
from backend.services.pdf_processor import process_pdf

logger = logging.getLogger(__name__)

router = APIRouter(tags=["extraction"])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_file(file: UploadFile) -> None:
    """Validate file extension and declared content type.

    Raises ``HTTPException(422)`` on validation failure.
    """
    if not file.filename:
        raise HTTPException(status_code=422, detail="Uploaded file has no filename.")

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=422,
            detail=f"File '{file.filename}' has unsupported extension '{ext}'. "
                   f"Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )


async def _read_and_validate_size(file: UploadFile) -> bytes:
    """Read the full upload into memory and enforce the size limit.

    Returns the raw bytes.  Raises ``HTTPException(413)`` when the file
    exceeds ``MAX_FILE_SIZE_MB``.
    """
    contents = await file.read()
    max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    if len(contents) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"File '{file.filename}' exceeds the {MAX_FILE_SIZE_MB} MB limit "
                   f"({len(contents) / (1024 * 1024):.1f} MB).",
        )
    return contents


def _compute_overall_confidence(result: ExtractionResult) -> float:
    """Compute a weighted average confidence across all extracted fields.

    Returns a value between 0.0 and 1.0.
    """
    scores: list[float] = []

    # Invoice-level fields
    inv = result.invoice
    for field in (
        inv.invoice_number,
        inv.invoice_date,
        inv.currency,
        inv.total_amount,
    ):
        if field.value is not None:
            scores.append(field.confidence)

    # Address fields (exporter, consignee, ship_to, ior)
    for addr in (inv.exporter, inv.consignee, inv.ship_to, inv.ior):
        for field in (
            addr.name,
            addr.address,
            addr.city,
            addr.state,
            addr.zip_code,
            addr.country,
            addr.phone,
            addr.email,
        ):
            if field.value is not None:
                scores.append(field.confidence)

    # Line items
    for item in inv.line_items:
        for field in (
            item.description,
            item.hs_code_origin,
            item.hs_code_destination,
            item.quantity,
            item.unit_price_usd,
            item.total_price_usd,
            item.unit_weight_kg,
            item.igst_percent,
        ):
            if field.value is not None:
                scores.append(field.confidence)

    # Packing list fields
    pl = result.packing_list
    for field in (pl.total_boxes, pl.total_net_weight_kg, pl.total_gross_weight_kg):
        if field.value is not None:
            scores.append(field.confidence)

    for box in pl.boxes:
        for field in (
            box.box_number,
            box.length_cm,
            box.width_cm,
            box.height_cm,
            box.gross_weight_kg,
            box.net_weight_kg,
        ):
            if field.value is not None:
                scores.append(field.confidence)
        for bi in box.items:
            for field in (bi.description, bi.quantity):
                if field.value is not None:
                    scores.append(field.confidence)

    if not scores:
        return 0.0
    return round(sum(scores) / len(scores), 4)


def _run_validation(result: ExtractionResult) -> ExtractionResult:
    """Run business-logic validation and populate ``warnings`` / ``errors``.

    Mutates and returns the same ``ExtractionResult`` instance.
    """
    warnings: list[str] = []
    errors: list[str] = []

    inv = result.invoice

    # Must have at least one line item
    if not inv.line_items:
        errors.append("No line items were extracted from the invoice.")

    # Invoice number is typically required
    if not inv.invoice_number.value:
        warnings.append("Invoice number could not be extracted.")

    # Consignee name
    if not inv.consignee.name.value:
        warnings.append("Consignee name is missing.")

    # Exporter name
    if not inv.exporter.name.value:
        warnings.append("Exporter/shipper name is missing.")

    # Check for low-confidence fields (below 0.5)
    low_conf_count = 0
    for item in inv.line_items:
        for field in (
            item.description,
            item.hs_code_origin,
            item.quantity,
            item.unit_price_usd,
            item.total_price_usd,
        ):
            if field.value is not None and field.confidence < 0.5:
                low_conf_count += 1

    if low_conf_count > 0:
        warnings.append(
            f"{low_conf_count} line-item field(s) have confidence below 50%. "
            "Please review highlighted fields."
        )

    # Packing list cross-checks
    pl = result.packing_list
    if pl.total_boxes.value and pl.boxes:
        try:
            expected = int(pl.total_boxes.value)
            actual = len(pl.boxes)
            if expected != actual:
                warnings.append(
                    f"Total boxes declared ({expected}) does not match "
                    f"extracted box count ({actual})."
                )
        except (ValueError, TypeError):
            pass

    # Destination ID validation
    if pl.destinations:
        dest_ids = {d.id for d in pl.destinations}
        unresolved_boxes = []
        for box in pl.boxes:
            did = box.destination_id.value
            if did and str(did) not in dest_ids:
                unresolved_boxes.append(str(box.box_number.value or "?"))
        if unresolved_boxes:
            warnings.append(
                f"Box(es) {', '.join(unresolved_boxes[:10])} reference unknown destination IDs."
            )

        # Check boxes without any receiver resolved
        no_receiver = [
            str(box.box_number.value or "?")
            for box in pl.boxes
            if box.receiver is None
        ]
        if no_receiver:
            warnings.append(
                f"{len(no_receiver)} box(es) have no resolved receiver address."
            )

    result.warnings = warnings
    result.errors = errors

    # Determine status
    result.overall_confidence = _compute_overall_confidence(result)
    if errors:
        result.status = "failed"
    elif result.overall_confidence < 0.7 or warnings:
        result.status = "review_needed"
    else:
        result.status = "completed"

    return result


def _set_nested_value(obj: Any, path_parts: list[str], value: Any) -> None:
    """Traverse a nested Pydantic model / list using dot-separated path parts
    and set the leaf attribute.

    Example path_parts: ["invoice", "line_items", "0", "hs_code_origin", "value"]
    """
    current = obj
    for part in path_parts[:-1]:
        if part.isdigit():
            idx = int(part)
            if not isinstance(current, list) or idx >= len(current):
                raise ValueError(f"Index {idx} out of range for list of length {len(current) if isinstance(current, list) else 'N/A'}")
            current = current[idx]
        elif isinstance(current, dict):
            if part not in current:
                raise ValueError(f"Key '{part}' not found in dict")
            current = current[part]
        else:
            if not hasattr(current, part):
                raise ValueError(f"Attribute '{part}' not found on {type(current).__name__}")
            current = getattr(current, part)

    leaf = path_parts[-1]
    if isinstance(current, dict):
        current[leaf] = value
    elif hasattr(current, leaf):
        setattr(current, leaf, value)
    else:
        raise ValueError(f"Cannot set attribute '{leaf}' on {type(current).__name__}")


async def _save_json(path: str, data: dict) -> None:
    """Write a dict as formatted JSON to disk."""
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, indent=2, ensure_ascii=False, default=str))


async def _load_json(path: str) -> dict:
    """Read and parse a JSON file from disk."""
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        content = await f.read()
    return json.loads(content)


def _generate_outputs(result: ExtractionResult, job_dir: str) -> tuple[str, str, str]:
    """Generate all Excel output files and return their paths."""
    multi_path = os.path.join(job_dir, "XpressB2BMultiAddressSheet.xlsx")
    simplified_path = os.path.join(job_dir, "SimplifiedTemplate.xlsx")
    b2b_shipment_path = os.path.join(job_dir, "B2BShipment.xlsx")

    generate_multi_address(result, multi_path)
    generate_simplified_template(result, simplified_path)
    generate_b2b_shipment(result, b2b_shipment_path)

    return multi_path, simplified_path, b2b_shipment_path


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

def _apply_post_processing(
    result: ExtractionResult,
    output_currency: str,
    exchange_rate: float | None,
    sync_hs_codes: bool,
) -> None:
    """Apply user-selected post-processing options to the extraction result.

    Mutates the result in-place.
    """
    inv = result.invoice

    # --- HS Code Sync ---
    if sync_hs_codes:
        for item in inv.line_items:
            origin = item.hs_code_origin
            dest = item.hs_code_destination
            if origin.value and not dest.value:
                dest.value = origin.value
                dest.confidence = origin.confidence * 0.9
            elif dest.value and not origin.value:
                origin.value = dest.value
                origin.confidence = dest.confidence * 0.9

    # --- Currency conversion ---
    if output_currency == "auto" or not exchange_rate or exchange_rate <= 0:
        return

    detected = str(inv.currency.value or "USD").upper()
    target = output_currency.upper()

    if detected == target:
        return  # No conversion needed

    rate = exchange_rate

    # Convert line item prices
    for item in inv.line_items:
        if item.unit_price_usd.value is not None:
            try:
                item.unit_price_usd.value = round(float(item.unit_price_usd.value) * rate, 2)
            except (ValueError, TypeError):
                pass
        if item.total_price_usd.value is not None:
            try:
                item.total_price_usd.value = round(float(item.total_price_usd.value) * rate, 2)
            except (ValueError, TypeError):
                pass

    # Convert total amount
    if inv.total_amount.value is not None:
        try:
            inv.total_amount.value = round(float(inv.total_amount.value) * rate, 2)
        except (ValueError, TypeError):
            pass

    # Update currency field to reflect the output currency
    inv.currency.value = target


@router.post("/api/extract", response_model=JobStatus)
async def extract_from_pdfs(
    files: list[UploadFile] = File(..., description="One or more PDF files to process"),
    output_currency: str = Form("auto"),
    exchange_rate: str = Form(""),
    sync_hs_codes: str = Form("true"),
) -> JobStatus:
    """Upload PDF invoices / packing lists, extract structured data, and
    generate XpressB2B bulk-upload Excel sheets.
    """
    if not files:
        raise HTTPException(status_code=422, detail="At least one PDF file is required.")

    # ---- 1. Validate all files up-front ----
    for f in files:
        _validate_file(f)

    # ---- 2. Generate job ID and create directories ----
    job_id = str(uuid.uuid4())
    job_dir = os.path.join(UPLOAD_DIR, job_id)
    input_dir = os.path.join(job_dir, "input")
    os.makedirs(input_dir, exist_ok=True)
    logger.info("Created job %s  --  output dir: %s", job_id, job_dir)

    # ---- 3. Read, validate size, and save files ----
    file_data: list[tuple[str, bytes]] = []
    for f in files:
        contents = await _read_and_validate_size(f)
        safe_name = os.path.basename(f.filename or "upload.pdf")
        save_path = os.path.join(input_dir, safe_name)
        async with aiofiles.open(save_path, "wb") as out:
            await out.write(contents)
        file_data.append((safe_name, contents))
        logger.info("Saved input file: %s (%d bytes)", safe_name, len(contents))

    # ---- 4. Extract text/tables from each PDF ----
    all_pages: list[dict[str, Any]] = []
    for filename, pdf_bytes in file_data:
        try:
            pdf_result = await asyncio.to_thread(process_pdf, pdf_bytes)
            pages = pdf_result.get("pages", [])
            # Tag each page with its source filename for traceability
            for page in pages:
                page["source_file"] = filename
            all_pages.extend(pages)
            logger.info(
                "Extracted %d page(s) from '%s' (text_based=%s)",
                len(pages),
                filename,
                pdf_result.get("is_text_based"),
            )
        except Exception:
            logger.exception("Failed to process PDF '%s'", filename)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to extract data from '{filename}'. Please check the file is a valid PDF.",
            )

    if not all_pages:
        raise HTTPException(
            status_code=422,
            detail="No pages could be extracted from the uploaded PDF(s).",
        )

    # ---- 5. LLM-based structured extraction ----
    try:
        extraction_result: ExtractionResult = await extract_from_text(all_pages)
        extraction_result.job_id = job_id
    except Exception:
        logger.exception("LLM extraction failed for job %s", job_id)
        raise HTTPException(
            status_code=500,
            detail="AI extraction failed. Please try again or contact support.",
        )

    # ---- 6. Apply post-processing options ----
    parsed_rate: float | None = None
    if exchange_rate:
        try:
            parsed_rate = float(exchange_rate)
        except ValueError:
            pass
    _apply_post_processing(
        extraction_result,
        output_currency=output_currency,
        exchange_rate=parsed_rate,
        sync_hs_codes=sync_hs_codes.lower() == "true",
    )

    # ---- 7. Validation and confidence scoring ----
    extraction_result = _run_validation(extraction_result)
    logger.info(
        "Job %s validation complete: status=%s, confidence=%.2f, warnings=%d, errors=%d",
        job_id,
        extraction_result.status,
        extraction_result.overall_confidence,
        len(extraction_result.warnings),
        len(extraction_result.errors),
    )

    # ---- 8. Generate output Excel files ----
    try:
        await asyncio.to_thread(_generate_outputs, extraction_result, job_dir)
    except Exception:
        logger.exception("Excel generation failed for job %s", job_id)
        extraction_result.warnings.append(
            "Excel file generation encountered an error. "
            "You can still download the extraction JSON."
        )

    # ---- 9. Persist extraction result as JSON ----
    result_path = os.path.join(job_dir, "extraction_result.json")
    await _save_json(result_path, extraction_result.model_dump())

    # ---- 10. Build and return response ----
    job_status = JobStatus(
        job_id=job_id,
        status=extraction_result.status,
        progress=100,
        message="Extraction complete.",
        result=extraction_result,
        multi_address_download=f"/api/download/{job_id}/multi",
        simplified_download=f"/api/download/{job_id}/simplified",
    )

    logger.info("Job %s finished successfully.", job_id)
    return job_status


@router.get("/api/jobs/{job_id}", response_model=JobStatus)
async def get_job(job_id: str) -> JobStatus:
    """Retrieve the status and extraction result of a prior job."""
    result_path = os.path.join(UPLOAD_DIR, job_id, "extraction_result.json")

    if not os.path.isfile(result_path):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    try:
        data = await _load_json(result_path)
        extraction_result = ExtractionResult(**data)
    except Exception:
        logger.exception("Failed to read extraction result for job %s", job_id)
        raise HTTPException(
            status_code=500, detail="Failed to load job data. The result file may be corrupted."
        )

    return JobStatus(
        job_id=job_id,
        status=extraction_result.status,
        progress=100,
        message="Loaded from disk.",
        result=extraction_result,
        multi_address_download=f"/api/download/{job_id}/multi",
        simplified_download=f"/api/download/{job_id}/simplified",
    )


@router.post("/api/jobs/{job_id}/update", response_model=JobStatus)
async def update_job_field(job_id: str, body: dict) -> JobStatus:
    """Update a single extracted field after human review.

    Request body::

        {
            "field_path": "invoice.line_items.0.hs_code_origin.value",
            "new_value": "74182000"
        }

    After the update the extraction JSON is re-saved and both Excel output
    files are regenerated.
    """
    field_path: str | None = body.get("field_path")
    new_value = body.get("new_value")

    if not field_path:
        raise HTTPException(status_code=422, detail="'field_path' is required in the request body.")

    job_dir = os.path.join(UPLOAD_DIR, job_id)
    result_path = os.path.join(job_dir, "extraction_result.json")

    if not os.path.isfile(result_path):
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    # Load current result
    try:
        data = await _load_json(result_path)
        extraction_result = ExtractionResult(**data)
    except Exception:
        logger.exception("Failed to load job %s for update", job_id)
        raise HTTPException(status_code=500, detail="Failed to load job data.")

    # Apply the update
    path_parts = field_path.split(".")
    try:
        _set_nested_value(extraction_result, path_parts, new_value)
    except (ValueError, IndexError, AttributeError) as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid field path '{field_path}': {exc}",
        )

    logger.info("Job %s: updated '%s' -> %r", job_id, field_path, new_value)

    # Re-validate
    extraction_result = _run_validation(extraction_result)

    # Re-generate Excel outputs
    try:
        await asyncio.to_thread(_generate_outputs, extraction_result, job_dir)
    except Exception:
        logger.exception("Excel re-generation failed for job %s after update", job_id)
        extraction_result.warnings.append(
            "Excel regeneration failed after field update."
        )

    # Persist updated result
    await _save_json(result_path, extraction_result.model_dump())

    return JobStatus(
        job_id=job_id,
        status=extraction_result.status,
        progress=100,
        message=f"Field '{field_path}' updated successfully.",
        result=extraction_result,
        multi_address_download=f"/api/download/{job_id}/multi",
        simplified_download=f"/api/download/{job_id}/simplified",
    )
