"""B2B Booking Agent API router.

Endpoints:
  POST   /api/agent/upload            Upload files, create batch, start extraction
  GET    /api/agent/jobs/{id}/stream   SSE progress stream
  GET    /api/agent/batches/{id}       Get batch with all drafts
  GET    /api/agent/drafts/{id}        Get single draft details
  PATCH  /api/agent/drafts/{id}        Apply corrections
  POST   /api/agent/drafts/{id}/approve  Push to Xindus API
  POST   /api/agent/drafts/{id}/archive Archive a draft
  POST   /api/agent/drafts/{id}/delete  Permanently delete a draft
  DELETE /api/agent/drafts/{id}        Soft-delete (reject) draft
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, UploadFile, File
from sse_starlette.sse import EventSourceResponse

from backend import db, storage
from backend.config import ALLOWED_EXTENSIONS, MAX_FILE_SIZE_MB
from backend.models.agent import (
    ActiveBatch,
    ActiveBatchesResponse,
    ApprovalResponse,
    BatchResponse,
    CorrectionRequest,
    DraftDetail,
    DraftsListResponse,
    DraftSummary,
    FileInfo,
    SellerProfile,
    SellersListResponse,
    SSEProgress,
    UploadResponse,
)
from backend.services.classifier import classify_document
from backend.services.draft_builder import build_draft_shipment
from backend.services.grouper import group_files_into_shipments
from backend.services.pdf_processor import process_pdf

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/agent", tags=["agent"])

# ---------------------------------------------------------------------------
# In-memory job progress tracking (lightweight; no Redis needed)
# ---------------------------------------------------------------------------

_job_queues: dict[str, asyncio.Queue] = {}


async def _send_progress(job_id: str, batch_id: UUID, event: SSEProgress) -> None:
    """Push an SSE event to the job's queue and persist step to DB."""
    q = _job_queues.get(job_id)
    if q:
        q.put_nowait(event)

    # Persist step + progress to DB for recovery after disconnect
    if event.step not in ("complete", "error"):
        progress_data: dict[str, Any] = {
            "completed": event.completed,
            "total": event.total,
        }
        if event.file:
            progress_data["file"] = event.file
        if event.shipments_found is not None:
            progress_data["shipments_found"] = event.shipments_found
        try:
            await db.update_batch_progress(batch_id, event.step, progress_data)
        except Exception:
            logger.warning("Failed to persist batch progress", exc_info=True)


# ---------------------------------------------------------------------------
# POST /api/agent/upload
# ---------------------------------------------------------------------------


@router.post("/upload", response_model=UploadResponse)
async def upload_files(files: list[UploadFile] = File(...)):
    """Accept file uploads, create a batch, and start async extraction."""
    if not files:
        raise HTTPException(400, "No files uploaded")

    # Validate files
    for f in files:
        ext = "." + f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
        if ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(400, f"Unsupported file type: {f.filename}")

    # Create batch
    batch_id = await db.create_batch(file_count=len(files))
    job_id = str(batch_id)

    # Create progress queue
    _job_queues[job_id] = asyncio.Queue()

    # Read file bytes and store records
    file_records: list[dict[str, Any]] = []
    for f in files:
        content = await f.read()

        # Check file size
        if len(content) > MAX_FILE_SIZE_MB * 1024 * 1024:
            raise HTTPException(400, f"File too large: {f.filename} (max {MAX_FILE_SIZE_MB}MB)")

        # Store in S3 / local
        s3_key = f"uploads/{batch_id}/{f.filename}"
        file_url = await storage.upload_file(s3_key, content)

        # Create DB record
        file_id = await db.create_file(batch_id, f.filename, file_url)
        file_records.append({
            "id": file_id,
            "filename": f.filename,
            "content": content,
        })

    # Start extraction in background
    asyncio.create_task(_run_extraction_pipeline(job_id, batch_id, file_records))

    return UploadResponse(batch_id=batch_id, file_count=len(files))


# ---------------------------------------------------------------------------
# Extraction pipeline (runs in background)
# ---------------------------------------------------------------------------


async def _run_extraction_pipeline(
    job_id: str,
    batch_id: UUID,
    file_records: list[dict[str, Any]],
) -> None:
    """Full extraction pipeline: classify → extract → group → build drafts."""
    total = len(file_records)

    try:
        # ── Step 1: Process PDFs and classify ──────────────────────
        extracted_files: list[dict[str, Any]] = []

        for idx, fr in enumerate(file_records):
            await _send_progress(job_id, batch_id, SSEProgress(
                step="classifying",
                file=fr["filename"],
                completed=idx,
                total=total,
            ))

            # Process PDF (text extraction / image rendering)
            pdf_result = await asyncio.to_thread(process_pdf, fr["content"])
            pages = pdf_result.get("pages", [])
            is_vision = any(p.get("image_bytes") for p in pages)

            # Classify document type
            classification = await classify_document(pages, is_vision=is_vision)
            file_type = classification["document_type"]

            await db.update_file_extraction(
                fr["id"],
                file_type=file_type,
                page_count=len(pages),
            )

            extracted_files.append({
                "id": fr["id"],
                "filename": fr["filename"],
                "file_type": file_type,
                "pages": pages,
                "is_vision": is_vision,
                "classification": classification,
            })

        await _send_progress(job_id, batch_id, SSEProgress(
            step="classifying",
            completed=total,
            total=total,
        ))

        # ── Step 2: Extract data per file ──────────────────────────
        from backend.services.llm_extractor import _extract_invoice, _extract_packing_list
        from backend.services.learning import build_extraction_hint

        # Build correction hints from past user corrections (self-improvement)
        try:
            correction_hints = await build_extraction_hint()
        except Exception:
            logger.warning("Failed to build extraction hints, continuing without", exc_info=True)
            correction_hints = ""

        for idx, ef in enumerate(extracted_files):
            await _send_progress(job_id, batch_id, SSEProgress(
                step="extracting",
                file=ef["filename"],
                completed=idx,
                total=total,
            ))

            pages = ef["pages"]
            is_vision = ef["is_vision"]
            file_type = ef["file_type"]

            try:
                if file_type == "invoice":
                    raw_data = await _extract_invoice(pages, is_vision, correction_hints=correction_hints)
                elif file_type == "packing_list":
                    raw_data = await _extract_packing_list(pages, is_vision, correction_hints=correction_hints)
                else:
                    # For certificates and other types, try invoice extraction
                    # to capture any identifiable metadata
                    raw_data = await _extract_invoice(pages, is_vision, correction_hints=correction_hints)
            except Exception:
                logger.exception("Extraction failed for %s", ef["filename"])
                raw_data = {}

            ef["extracted_data"] = raw_data

            # Compute a simple confidence from extracted data
            confidence = _estimate_confidence(raw_data, file_type)

            await db.update_file_extraction(
                ef["id"],
                extracted_data=raw_data,
                confidence=confidence,
            )

        await _send_progress(job_id, batch_id, SSEProgress(
            step="extracting",
            completed=total,
            total=total,
        ))

        # ── Step 3: Group files into shipments ─────────────────────
        await _send_progress(job_id, batch_id, SSEProgress(step="grouping", total=total))

        group_input = [
            {
                "id": ef["id"],
                "file_type": ef["file_type"],
                "extracted_data": ef.get("extracted_data", {}),
                "filename": ef.get("filename", ""),
            }
            for ef in extracted_files
        ]
        shipment_groups = group_files_into_shipments(group_input)

        await _send_progress(job_id, batch_id, SSEProgress(
            step="grouping",
            shipments_found=len(shipment_groups),
            total=total,
        ))

        # ── Step 4: Build draft shipments + seller intelligence ────
        from backend.services.seller_intelligence import (
            match_or_create_seller,
            apply_seller_defaults,
        )

        await _send_progress(job_id, batch_id, SSEProgress(step="building_drafts", total=len(shipment_groups)))

        for g_idx, group in enumerate(shipment_groups):
            # Gather file data for this group
            group_files = []
            group_file_ids = []
            for ef in extracted_files:
                if str(ef["id"]) in group["file_ids"]:
                    group_files.append({
                        "id": ef["id"],
                        "file_type": ef["file_type"],
                        "extracted_data": ef.get("extracted_data", {}),
                    })
                    group_file_ids.append(ef["id"])

            # Build Xindus-format draft
            shipment_data, confidence_scores = build_draft_shipment(group_files)

            # ── Seller intelligence: match/create + apply defaults ──
            seller_id = None
            try:
                shipper_name = (shipment_data.get("shipper_address") or {}).get("name", "")
                shipper_addr = shipment_data.get("shipper_address")
                if shipper_name:
                    seller_id, seller_defaults = await match_or_create_seller(
                        shipper_name, shipper_addr,
                    )
                    if seller_defaults:
                        shipment_data = apply_seller_defaults(
                            shipment_data, seller_defaults, shipper_addr,
                        )
                        logger.info(
                            "Applied seller defaults for '%s' (seller %s)",
                            shipper_name, seller_id,
                        )
            except Exception:
                logger.warning("Seller intelligence failed, continuing without", exc_info=True)

            # Save draft to DB
            draft_id = await db.create_draft(
                batch_id=batch_id,
                shipment_data=shipment_data,
                confidence_scores=confidence_scores,
                grouping_reason=group["reason"],
                seller_id=seller_id,
            )

            # Link files to draft
            await db.link_files_to_draft(draft_id, group_file_ids)

        # ── Done ───────────────────────────────────────────────────
        await db.update_batch_status(batch_id, "review")

        await _send_progress(job_id, batch_id, SSEProgress(
            step="complete",
            batch_id=str(batch_id),
            shipments_found=len(shipment_groups),
            completed=total,
            total=total,
        ))

    except Exception as exc:
        logger.exception("Extraction pipeline failed for batch %s", batch_id)
        await db.update_batch_status(batch_id, "failed")
        await _send_progress(job_id, batch_id, SSEProgress(
            step="error",
            message=str(exc),
        ))
    finally:
        # Signal end of stream
        q = _job_queues.get(job_id)
        if q:
            q.put_nowait(None)


def _estimate_confidence(raw_data: dict[str, Any], file_type: str) -> float:
    """Quick confidence estimate from extracted data."""
    if not raw_data:
        return 0.0

    confidences = []

    def _collect(obj: Any) -> None:
        if isinstance(obj, dict):
            if "confidence" in obj and "value" in obj:
                confidences.append(float(obj["confidence"]))
            else:
                for v in obj.values():
                    _collect(v)
        elif isinstance(obj, list):
            for item in obj:
                _collect(item)

    _collect(raw_data)
    return round(sum(confidences) / len(confidences), 3) if confidences else 0.5


# ---------------------------------------------------------------------------
# GET /api/agent/jobs/{job_id}/stream
# ---------------------------------------------------------------------------


@router.get("/jobs/{job_id}/stream")
async def stream_job_progress(job_id: str):
    """SSE endpoint streaming extraction progress events."""
    q = _job_queues.get(job_id)
    if not q:
        raise HTTPException(404, f"Job {job_id} not found or already completed")

    async def event_generator():
        while True:
            event = await q.get()
            if event is None:
                break
            yield {
                "event": "progress",
                "data": event.model_dump_json(),
            }
        # Cleanup
        _job_queues.pop(job_id, None)

    return EventSourceResponse(event_generator())


# ---------------------------------------------------------------------------
# GET /api/agent/batches/active  (in-flight jobs for persistent status)
# ---------------------------------------------------------------------------


@router.get("/batches/active", response_model=ActiveBatchesResponse)
async def get_active_batches():
    """Return all processing batches with their current step and progress."""
    rows = await db.get_active_batches()
    batches = []
    for r in rows:
        sp = r.get("step_progress") or {}
        if isinstance(sp, str):
            import json as _json
            sp = _json.loads(sp)
        batches.append(ActiveBatch(
            id=r["id"],
            status=r["status"],
            current_step=r.get("current_step"),
            step_progress=sp,
            file_count=r.get("file_count", 0),
            created_at=r.get("created_at"),
        ))
    return ActiveBatchesResponse(batches=batches)


# ---------------------------------------------------------------------------
# GET /api/agent/drafts  (list all drafts with optional status filter)
# ---------------------------------------------------------------------------


@router.get("/drafts", response_model=DraftsListResponse)
async def list_drafts(
    status: str | None = None,
    exclude_status: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    """List all drafts, optionally filtered by status."""
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200
    if offset < 0:
        offset = 0

    drafts_raw, total = await db.get_all_drafts(
        status=status, exclude_status=exclude_status, limit=limit, offset=offset
    )

    # Pre-load seller shipment counts for all drafts with seller_id
    seller_counts: dict[str, int] = {}
    seller_ids_to_load = {d.get("seller_id") for d in drafts_raw if d.get("seller_id")}
    for sid in seller_ids_to_load:
        seller_row = await db.get_seller(sid)
        if seller_row:
            seller_counts[str(sid)] = seller_row.get("shipment_count", 0)

    drafts = []
    for d in drafts_raw:
        sd = d.get("shipment_data") or {}
        if isinstance(sd, str):
            sd = json.loads(sd)

        draft_file_ids = await db.get_draft_file_ids(d["id"])
        d_seller_id = d.get("seller_id")

        drafts.append(DraftSummary(
            id=d["id"],
            status=d["status"],
            file_count=len(draft_file_ids),
            grouping_reason=d.get("grouping_reason"),
            confidence_scores=_parse_jsonb(d.get("confidence_scores")),
            shipper_name=_nested_get(sd, "shipper_address", "name"),
            receiver_name=_nested_get(sd, "receiver_address", "name"),
            box_count=sd.get("total_boxes") or len(sd.get("shipment_boxes", [])),
            total_value=sd.get("total_amount"),
            invoice_number=sd.get("invoice_number"),
            created_at=d.get("created_at"),
            seller_id=d_seller_id,
            seller_shipment_count=seller_counts.get(str(d_seller_id)) if d_seller_id else None,
        ))

    return DraftsListResponse(drafts=drafts, total=total)


# ---------------------------------------------------------------------------
# GET /api/agent/batches/{batch_id}
# ---------------------------------------------------------------------------


@router.get("/batches/{batch_id}", response_model=BatchResponse)
async def get_batch(batch_id: UUID):
    """Get batch metadata with all draft shipments."""
    batch = await db.get_batch(batch_id)
    if not batch:
        raise HTTPException(404, "Batch not found")

    drafts_raw = await db.get_drafts_for_batch(batch_id)
    files_raw = await db.get_files_for_batch(batch_id)

    # Build draft summaries
    drafts = []
    for d in drafts_raw:
        sd = d.get("shipment_data") or {}
        if isinstance(sd, str):
            sd = json.loads(sd)

        # Count files linked to this draft
        draft_file_ids = await db.get_draft_file_ids(d["id"])

        drafts.append(DraftSummary(
            id=d["id"],
            status=d["status"],
            file_count=len(draft_file_ids),
            grouping_reason=d.get("grouping_reason"),
            confidence_scores=_parse_jsonb(d.get("confidence_scores")),
            shipper_name=_nested_get(sd, "shipper_address", "name"),
            receiver_name=_nested_get(sd, "receiver_address", "name"),
            box_count=sd.get("total_boxes") or len(sd.get("shipment_boxes", [])),
            total_value=sd.get("total_amount"),
            invoice_number=sd.get("invoice_number"),
            created_at=d.get("created_at"),
        ))

    files = [
        FileInfo(
            id=f["id"],
            filename=f["filename"],
            file_type=f.get("file_type"),
            page_count=f.get("page_count"),
            confidence=f.get("confidence"),
            processed_at=f.get("processed_at"),
        )
        for f in files_raw
    ]

    return BatchResponse(
        id=batch["id"],
        status=batch["status"],
        file_count=batch["file_count"],
        created_at=batch.get("created_at"),
        completed_at=batch.get("completed_at"),
        drafts=drafts,
        files=files,
    )


# ---------------------------------------------------------------------------
# GET /api/agent/drafts/{draft_id}
# ---------------------------------------------------------------------------


@router.get("/drafts/{draft_id}", response_model=DraftDetail)
async def get_draft(draft_id: UUID):
    """Get full draft details including shipment data and associated files."""
    draft = await db.get_draft(draft_id)
    if not draft:
        raise HTTPException(404, "Draft not found")

    files_raw = await db.get_draft_files(draft_id)
    files = [
        FileInfo(
            id=f["id"],
            filename=f["filename"],
            file_type=f.get("file_type"),
            page_count=f.get("page_count"),
            confidence=f.get("confidence"),
            processed_at=f.get("processed_at"),
        )
        for f in files_raw
    ]

    # Load seller profile if available
    seller_id = draft.get("seller_id")
    seller_profile = None
    if seller_id:
        seller_row = await db.get_seller(seller_id)
        if seller_row:
            seller_profile = SellerProfile(
                id=seller_row["id"],
                name=seller_row["name"],
                normalized_name=seller_row["normalized_name"],
                defaults=_parse_jsonb(seller_row.get("defaults")) or {},
                shipper_address=_parse_jsonb(seller_row.get("shipper_address")) or {},
                shipment_count=seller_row.get("shipment_count", 0),
                created_at=seller_row.get("created_at"),
                updated_at=seller_row.get("updated_at"),
            )

    return DraftDetail(
        id=draft["id"],
        batch_id=draft["batch_id"],
        status=draft["status"],
        shipment_data=_parse_jsonb(draft.get("shipment_data")) or {},
        confidence_scores=_parse_jsonb(draft.get("confidence_scores")),
        grouping_reason=draft.get("grouping_reason"),
        corrected_data=_parse_jsonb(draft.get("corrected_data")),
        xindus_scancode=draft.get("xindus_scancode"),
        files=files,
        created_at=draft.get("created_at"),
        seller_id=seller_id,
        seller=seller_profile,
    )


# ---------------------------------------------------------------------------
# PATCH /api/agent/drafts/{draft_id}
# ---------------------------------------------------------------------------


@router.patch("/drafts/{draft_id}", response_model=DraftDetail)
async def apply_corrections(draft_id: UUID, body: CorrectionRequest):
    """Apply field corrections to a draft shipment."""
    draft = await db.get_draft(draft_id)
    if not draft:
        raise HTTPException(404, "Draft not found")

    if draft["status"] not in ("pending_review", "review"):
        raise HTTPException(400, f"Draft is {draft['status']}, cannot apply corrections")

    # Start with existing corrected_data or shipment_data
    current_data = _parse_jsonb(draft.get("corrected_data")) or _parse_jsonb(draft.get("shipment_data")) or {}

    # Get seller_id from draft for scoped corrections
    draft_seller_id = draft.get("seller_id")

    # Apply each correction
    for correction in body.corrections:
        # Special case: seller_id updates the DB column, not the shipment JSON
        if correction.field_path == "seller_id":
            new_seller_id = correction.new_value
            if new_seller_id:
                try:
                    seller_uuid = UUID(str(new_seller_id))
                    await db.update_draft_seller(draft_id, seller_uuid)
                    draft_seller_id = seller_uuid
                except (ValueError, TypeError):
                    logger.warning("Invalid seller_id value: %s", new_seller_id)
            else:
                await db.update_draft_seller(draft_id, None)
                draft_seller_id = None
            continue

        _set_nested(current_data, correction.field_path, correction.new_value)

        # Store correction for learning (scoped to seller)
        await db.create_correction(
            draft_id=draft_id,
            field_path=correction.field_path,
            original_value=correction.old_value,
            corrected_value=correction.new_value,
            seller_id=draft_seller_id,
        )

    # Save updated data
    await db.update_draft_corrections(draft_id, current_data)

    return await get_draft(draft_id)


# ---------------------------------------------------------------------------
# POST /api/agent/drafts/{draft_id}/approve
# ---------------------------------------------------------------------------


@router.post("/drafts/{draft_id}/approve", response_model=ApprovalResponse)
async def approve_draft(draft_id: UUID):
    """Approve a draft and push to Xindus B2B API (placeholder)."""
    draft = await db.get_draft(draft_id)
    if not draft:
        raise HTTPException(404, "Draft not found")

    if draft["status"] not in ("pending_review",):
        raise HTTPException(400, f"Draft is {draft['status']}, cannot approve")

    # Use corrected_data if available, otherwise shipment_data
    final_data = _parse_jsonb(draft.get("corrected_data")) or _parse_jsonb(draft.get("shipment_data")) or {}

    # TODO: Phase 3 — Push to Xindus API
    # For now, mark as approved with a placeholder scancode
    await db.update_draft_status(draft_id, "approved")

    # Harvest seller defaults from approved shipment
    seller_id = draft.get("seller_id")
    if seller_id:
        try:
            from backend.services.seller_intelligence import harvest_seller_defaults
            await harvest_seller_defaults(seller_id, final_data)
        except Exception:
            logger.warning("Failed to harvest seller defaults", exc_info=True)

    return ApprovalResponse(
        success=True,
        draft_id=draft_id,
        xindus_scancode=None,
        message="Draft approved. Xindus API integration coming in Phase 3.",
    )


# ---------------------------------------------------------------------------
# DELETE /api/agent/drafts/{draft_id}
# ---------------------------------------------------------------------------


@router.delete("/drafts/{draft_id}")
async def reject_draft(draft_id: UUID):
    """Soft-delete (reject) a draft shipment."""
    draft = await db.get_draft(draft_id)
    if not draft:
        raise HTTPException(404, "Draft not found")

    await db.update_draft_status(draft_id, "rejected")
    return {"success": True, "message": "Draft rejected"}


# ---------------------------------------------------------------------------
# POST /api/agent/drafts/{draft_id}/archive
# ---------------------------------------------------------------------------


@router.post("/drafts/{draft_id}/archive")
async def archive_draft(draft_id: UUID):
    """Archive a draft shipment."""
    draft = await db.get_draft(draft_id)
    if not draft:
        raise HTTPException(404, "Draft not found")
    if draft["status"] == "archived":
        raise HTTPException(400, "Draft is already archived")
    await db.update_draft_status(draft_id, "archived")
    return {"success": True, "message": "Draft archived"}


# ---------------------------------------------------------------------------
# POST /api/agent/drafts/{draft_id}/delete
# ---------------------------------------------------------------------------


@router.post("/drafts/{draft_id}/delete")
async def permanently_delete_draft(draft_id: UUID):
    """Permanently delete a draft shipment from the database."""
    draft = await db.get_draft(draft_id)
    if not draft:
        raise HTTPException(404, "Draft not found")
    if draft["status"] not in ("pending_review", "rejected"):
        raise HTTPException(400, f"Cannot delete draft with status '{draft['status']}'")
    await db.delete_draft_permanent(draft_id)
    return {"success": True, "message": "Draft permanently deleted"}


# ---------------------------------------------------------------------------
# GET /api/agent/sellers  (list all sellers)
# GET /api/agent/sellers/match?name=...  (match a seller by name)
# ---------------------------------------------------------------------------


@router.get("/sellers", response_model=SellersListResponse)
async def list_sellers():
    """List all known seller profiles."""
    rows = await db.get_all_sellers()
    sellers = [
        SellerProfile(
            id=r["id"],
            name=r["name"],
            normalized_name=r["normalized_name"],
            defaults=_parse_jsonb(r.get("defaults")) or {},
            shipper_address=_parse_jsonb(r.get("shipper_address")) or {},
            shipment_count=r.get("shipment_count", 0),
            created_at=r.get("created_at"),
            updated_at=r.get("updated_at"),
        )
        for r in rows
    ]
    return SellersListResponse(sellers=sellers)


@router.get("/sellers/match", response_model=SellerProfile)
async def match_seller(name: str):
    """Match a seller by name (exact + fuzzy)."""
    from backend.services.seller_intelligence import match_or_create_seller

    if not name.strip():
        raise HTTPException(400, "name parameter required")

    seller_id, _ = await match_or_create_seller(name)
    seller_row = await db.get_seller(seller_id)
    if not seller_row:
        raise HTTPException(404, "Seller not found")

    return SellerProfile(
        id=seller_row["id"],
        name=seller_row["name"],
        normalized_name=seller_row["normalized_name"],
        defaults=_parse_jsonb(seller_row.get("defaults")) or {},
        shipper_address=_parse_jsonb(seller_row.get("shipper_address")) or {},
        shipment_count=seller_row.get("shipment_count", 0),
        created_at=seller_row.get("created_at"),
        updated_at=seller_row.get("updated_at"),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_jsonb(val: Any) -> dict[str, Any] | None:
    """Parse a JSONB value that may be a string or already a dict."""
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


def _nested_get(data: dict[str, Any], *keys: str) -> str | None:
    """Safely get a nested value from a dict."""
    obj = data
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    if obj is None:
        return None
    return str(obj) if obj else None


def _set_nested(data: dict[str, Any], path: str, value: Any) -> None:
    """Set a value in a nested dict using dot-separated path.

    Supports array indices: "shipment_boxes.0.gross_weight_kg"
    """
    keys = path.split(".")
    obj = data
    for key in keys[:-1]:
        if key.isdigit():
            idx = int(key)
            if isinstance(obj, list) and 0 <= idx < len(obj):
                obj = obj[idx]
            else:
                return
        else:
            if not isinstance(obj, dict):
                return
            if key not in obj:
                obj[key] = {}
            obj = obj[key]

    final_key = keys[-1]
    if final_key.isdigit() and isinstance(obj, list):
        idx = int(final_key)
        if 0 <= idx < len(obj):
            obj[idx] = value
    elif isinstance(obj, dict):
        obj[final_key] = value
