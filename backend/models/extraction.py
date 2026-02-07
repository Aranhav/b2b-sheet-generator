"""Pydantic models for the extraction pipeline."""
from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, Field


class ConfidenceValue(BaseModel):
    value: Optional[str | int | float] = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class LineItem(BaseModel):
    description: ConfidenceValue = ConfidenceValue()
    hs_code_origin: ConfidenceValue = ConfidenceValue()
    hs_code_destination: ConfidenceValue = ConfidenceValue()
    quantity: ConfidenceValue = ConfidenceValue()
    unit_price_usd: ConfidenceValue = ConfidenceValue()
    total_price_usd: ConfidenceValue = ConfidenceValue()
    unit_weight_kg: ConfidenceValue = ConfidenceValue()
    igst_percent: ConfidenceValue = ConfidenceValue()


class BoxItem(BaseModel):
    description: ConfidenceValue = ConfidenceValue()
    quantity: ConfidenceValue = ConfidenceValue()


class Address(BaseModel):
    name: ConfidenceValue = ConfidenceValue()
    address: ConfidenceValue = ConfidenceValue()
    city: ConfidenceValue = ConfidenceValue()
    state: ConfidenceValue = ConfidenceValue()
    zip_code: ConfidenceValue = ConfidenceValue()
    country: ConfidenceValue = ConfidenceValue()
    phone: ConfidenceValue = ConfidenceValue()
    email: ConfidenceValue = ConfidenceValue()


class Box(BaseModel):
    box_number: ConfidenceValue = ConfidenceValue()
    length_cm: ConfidenceValue = ConfidenceValue()
    width_cm: ConfidenceValue = ConfidenceValue()
    height_cm: ConfidenceValue = ConfidenceValue()
    gross_weight_kg: ConfidenceValue = ConfidenceValue()
    net_weight_kg: ConfidenceValue = ConfidenceValue()
    items: list[BoxItem] = []
    destination_id: ConfidenceValue = ConfidenceValue()
    receiver: Optional[Address] = None


class Destination(BaseModel):
    id: str = ""
    name: ConfidenceValue = ConfidenceValue()
    address: Address = Address()


class InvoiceData(BaseModel):
    invoice_number: ConfidenceValue = ConfidenceValue()
    invoice_date: ConfidenceValue = ConfidenceValue()
    currency: ConfidenceValue = ConfidenceValue()
    total_amount: ConfidenceValue = ConfidenceValue()
    exporter: Address = Address()
    consignee: Address = Address()
    ship_to: Address = Address()
    ior: Address = Address()
    line_items: list[LineItem] = []


class PackingListData(BaseModel):
    total_boxes: ConfidenceValue = ConfidenceValue()
    total_net_weight_kg: ConfidenceValue = ConfidenceValue()
    total_gross_weight_kg: ConfidenceValue = ConfidenceValue()
    boxes: list[Box] = []
    destinations: list[Destination] = []


class ExtractionResult(BaseModel):
    job_id: str = ""
    status: str = "processing"  # processing | review_needed | completed | failed
    overall_confidence: float = 0.0
    invoice: InvoiceData = InvoiceData()
    packing_list: PackingListData = PackingListData()
    warnings: list[str] = []
    errors: list[str] = []


class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: int = 0
    message: str = ""
    result: Optional[ExtractionResult] = None
    multi_address_download: Optional[str] = None
    simplified_download: Optional[str] = None
