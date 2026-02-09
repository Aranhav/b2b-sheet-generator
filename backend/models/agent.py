"""Pydantic models for the B2B Booking Agent endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class CorrectionItem(BaseModel):
    """A single field correction submitted by a reviewer."""
    field_path: str = Field(..., description="Dot-separated path, e.g. 'shipper_address.city'")
    old_value: Any = None
    new_value: Any = None


class CorrectionRequest(BaseModel):
    """PATCH body for applying corrections to a draft."""
    corrections: list[CorrectionItem] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class UploadResponse(BaseModel):
    batch_id: UUID
    file_count: int


class FileInfo(BaseModel):
    id: UUID
    filename: str
    file_type: Optional[str] = None
    page_count: Optional[int] = None
    confidence: Optional[float] = None
    processed_at: Optional[datetime] = None


class SellerProfile(BaseModel):
    """Per-seller profile with accumulated defaults."""
    id: UUID
    name: str
    normalized_name: str
    defaults: dict[str, Any] = Field(default_factory=dict)
    shipper_address: dict[str, Any] = Field(default_factory=dict)
    shipment_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class DraftSummary(BaseModel):
    id: UUID
    status: str
    file_count: int = 0
    grouping_reason: Optional[str] = None
    confidence_scores: Optional[dict[str, Any]] = None
    shipper_name: Optional[str] = None
    receiver_name: Optional[str] = None
    box_count: Optional[int] = None
    total_value: Optional[float] = None
    invoice_number: Optional[str] = None
    created_at: Optional[datetime] = None
    seller_id: Optional[UUID] = None
    seller_shipment_count: Optional[int] = None


class DraftDetail(BaseModel):
    id: UUID
    batch_id: UUID
    status: str
    shipment_data: dict[str, Any]
    confidence_scores: Optional[dict[str, Any]] = None
    grouping_reason: Optional[str] = None
    corrected_data: Optional[dict[str, Any]] = None
    xindus_scancode: Optional[str] = None
    files: list[FileInfo] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    seller_id: Optional[UUID] = None
    seller: Optional[SellerProfile] = None


class BatchResponse(BaseModel):
    id: UUID
    status: str
    file_count: int
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    drafts: list[DraftSummary] = Field(default_factory=list)
    files: list[FileInfo] = Field(default_factory=list)


class ApprovalResponse(BaseModel):
    success: bool
    draft_id: UUID
    xindus_scancode: Optional[str] = None
    message: str = ""


class DraftsListResponse(BaseModel):
    """Response for GET /api/agent/drafts list endpoint."""
    drafts: list[DraftSummary] = Field(default_factory=list)
    total: int = 0


class SellersListResponse(BaseModel):
    """Response for GET /api/agent/sellers list endpoint."""
    sellers: list[SellerProfile] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# SSE event models
# ---------------------------------------------------------------------------


class SSEProgress(BaseModel):
    """Payload for SSE progress events."""
    step: str  # classifying, extracting, grouping, building_drafts, complete, error
    file: Optional[str] = None
    completed: int = 0
    total: int = 0
    batch_id: Optional[str] = None
    shipments_found: Optional[int] = None
    message: Optional[str] = None


class ActiveBatch(BaseModel):
    """A batch currently being processed (for persistent job status)."""
    id: UUID
    status: str
    current_step: Optional[str] = None
    step_progress: dict[str, Any] = Field(default_factory=dict)
    file_count: int = 0
    created_at: Optional[datetime] = None


class ActiveBatchesResponse(BaseModel):
    """Response for GET /api/agent/batches/active."""
    batches: list[ActiveBatch] = Field(default_factory=list)
