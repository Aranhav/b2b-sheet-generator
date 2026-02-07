"""PDF text and table extraction service.

Uses PyMuPDF (fitz) for scanned-PDF detection and image rendering,
and pdfplumber for high-fidelity text + table extraction from
text-based PDFs.
"""
from __future__ import annotations

import io
import logging
from typing import Any

import fitz  # PyMuPDF
import pdfplumber

logger = logging.getLogger(__name__)

# Minimum characters per page to consider a PDF "text-based".
_TEXT_THRESHOLD = 50


def _is_text_based(pdf_bytes: bytes) -> bool:
    """Return True if every page in the PDF contains meaningful text.

    A page is considered text-bearing when its extracted character count
    exceeds ``_TEXT_THRESHOLD``.  If *any* page falls below the threshold
    the entire document is treated as scanned / image-based so that the
    vision pipeline can handle it.
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for page in doc:
            text = page.get_text("text") or ""
            if len(text.strip()) <= _TEXT_THRESHOLD:
                doc.close()
                return False
        doc.close()
        return True
    except Exception:
        logger.exception("Error during text-based detection with PyMuPDF")
        return False


def _extract_text_pages(pdf_bytes: bytes) -> list[dict[str, Any]]:
    """Extract text and tables from a text-based PDF using pdfplumber.

    Returns a list of page dicts, one per page:
        {
            "page_num": int,       # 1-indexed
            "text": str,
            "tables": list,        # list of tables, each table is list[list[str|None]]
            "image_bytes": None,
        }
    """
    pages: list[dict[str, Any]] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for idx, page in enumerate(pdf.pages):
                page_text = page.extract_text() or ""
                raw_tables = page.extract_tables() or []

                # Normalise table cells so downstream consumers always see
                # ``str | None`` (pdfplumber may return other types on
                # malformed cells).
                cleaned_tables: list[list[list[str | None]]] = []
                for table in raw_tables:
                    cleaned_rows: list[list[str | None]] = []
                    for row in table:
                        cleaned_row = [
                            str(cell).strip() if cell is not None else None
                            for cell in row
                        ]
                        cleaned_rows.append(cleaned_row)
                    cleaned_tables.append(cleaned_rows)

                pages.append(
                    {
                        "page_num": idx + 1,
                        "text": page_text,
                        "tables": cleaned_tables,
                        "image_bytes": None,
                    }
                )
    except Exception:
        logger.exception("Error extracting text pages with pdfplumber")

    return pages


def _extract_image_pages(pdf_bytes: bytes) -> list[dict[str, Any]]:
    """Render each page of a scanned PDF to a PNG image using PyMuPDF.

    Returns a list of page dicts:
        {
            "page_num": int,       # 1-indexed
            "text": "",
            "tables": [],
            "image_bytes": bytes,  # PNG image data
        }
    """
    pages: list[dict[str, Any]] = []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for idx, page in enumerate(doc):
            # Render at 2x zoom (144 DPI) for legible OCR / vision input.
            zoom = 2.0
            matrix = fitz.Matrix(zoom, zoom)
            pixmap = page.get_pixmap(matrix=matrix)
            png_bytes: bytes = pixmap.tobytes(output="png")

            pages.append(
                {
                    "page_num": idx + 1,
                    "text": "",
                    "tables": [],
                    "image_bytes": png_bytes,
                }
            )
        doc.close()
    except Exception:
        logger.exception("Error rendering scanned pages with PyMuPDF")

    return pages


def process_pdf(file_bytes: bytes) -> dict[str, Any]:
    """Main entry point -- process a PDF file and return structured results.

    Parameters
    ----------
    file_bytes:
        Raw bytes of the uploaded PDF file.

    Returns
    -------
    dict with keys:
        ``is_text_based`` (bool) -- whether the PDF contained extractable text.
        ``pages`` (list[dict])   -- per-page extraction results.
            Each page dict contains:
                ``page_num``    (int)
                ``text``        (str)
                ``tables``      (list[list[list[str|None]]])
                ``image_bytes`` (bytes | None)
    """
    if not file_bytes:
        logger.warning("process_pdf called with empty file bytes")
        return {"is_text_based": False, "pages": []}

    text_based = _is_text_based(file_bytes)
    logger.info(
        "PDF classification: %s (%d bytes)",
        "text-based" if text_based else "scanned/image",
        len(file_bytes),
    )

    if text_based:
        pages = _extract_text_pages(file_bytes)
    else:
        pages = _extract_image_pages(file_bytes)

    logger.info("Extracted %d page(s) from PDF", len(pages))
    return {"is_text_based": text_based, "pages": pages}
