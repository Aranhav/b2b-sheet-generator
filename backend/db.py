"""PostgreSQL database layer for the B2B Booking Agent.

Uses asyncpg for async connection pooling and provides CRUD helpers
for upload batches, files, draft shipments, and corrections.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS upload_batches (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status TEXT NOT NULL DEFAULT 'processing',
  file_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS uploaded_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id UUID NOT NULL REFERENCES upload_batches(id) ON DELETE CASCADE,
  filename TEXT NOT NULL,
  file_url TEXT NOT NULL DEFAULT '',
  file_type TEXT,
  page_count INT,
  extracted_data JSONB,
  confidence FLOAT,
  processed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS draft_shipments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id UUID NOT NULL REFERENCES upload_batches(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending_review',
  shipment_data JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence_scores JSONB,
  grouping_reason TEXT,
  reviewed_at TIMESTAMPTZ,
  corrected_data JSONB,
  xindus_scancode TEXT,
  pushed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS draft_shipment_files (
  draft_id UUID NOT NULL REFERENCES draft_shipments(id) ON DELETE CASCADE,
  file_id UUID NOT NULL REFERENCES uploaded_files(id) ON DELETE CASCADE,
  PRIMARY KEY (draft_id, file_id)
);

CREATE TABLE IF NOT EXISTS corrections (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  draft_id UUID NOT NULL REFERENCES draft_shipments(id) ON DELETE CASCADE,
  field_path TEXT NOT NULL,
  original_value JSONB,
  corrected_value JSONB,
  file_context TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_drafts_batch ON draft_shipments(batch_id);
CREATE INDEX IF NOT EXISTS idx_drafts_status ON draft_shipments(status);
CREATE INDEX IF NOT EXISTS idx_files_batch ON uploaded_files(batch_id);
CREATE INDEX IF NOT EXISTS idx_corrections_field ON corrections(field_path);
CREATE INDEX IF NOT EXISTS idx_corrections_draft ON corrections(draft_id);
CREATE INDEX IF NOT EXISTS idx_shipment_data ON draft_shipments USING GIN (shipment_data);

-- Per-seller intelligence table
CREATE TABLE IF NOT EXISTS sellers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  normalized_name TEXT NOT NULL UNIQUE,
  defaults JSONB NOT NULL DEFAULT '{}'::jsonb,
  shipper_address JSONB NOT NULL DEFAULT '{}'::jsonb,
  shipment_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sellers_normalized ON sellers(normalized_name);

-- Add seller_id FK to existing tables (idempotent)
DO $$ BEGIN
  ALTER TABLE draft_shipments ADD COLUMN seller_id UUID REFERENCES sellers(id);
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE corrections ADD COLUMN seller_id UUID REFERENCES sellers(id);
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS idx_drafts_seller ON draft_shipments(seller_id);
CREATE INDEX IF NOT EXISTS idx_corrections_seller ON corrections(seller_id);

-- Step tracking columns for persistent job status (idempotent)
DO $$ BEGIN
  ALTER TABLE upload_batches ADD COLUMN current_step TEXT;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

DO $$ BEGIN
  ALTER TABLE upload_batches ADD COLUMN step_progress JSONB DEFAULT '{}'::jsonb;
EXCEPTION WHEN duplicate_column THEN NULL; END $$;

-- Shipping methods lookup table
CREATE TABLE IF NOT EXISTS shipping_methods (
  code TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  is_b2b BOOLEAN NOT NULL DEFAULT false,
  active BOOLEAN NOT NULL DEFAULT true
);

INSERT INTO shipping_methods (code, name, is_b2b, active) VALUES
  ('AN', 'Xindus B2B Express', true, true)
ON CONFLICT (code) DO NOTHING;
"""

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


async def init_db() -> None:
    """Create the connection pool and ensure the schema exists."""
    global _pool
    if not DATABASE_URL:
        logger.warning("DATABASE_URL not set -- agent features will be unavailable")
        return

    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with _pool.acquire() as conn:
        await conn.execute(_SCHEMA_SQL)
    logger.info("Database pool created and schema initialized")

    # Backfill seller_id for existing drafts that predate seller intelligence
    await _backfill_seller_ids()


async def _backfill_seller_ids() -> None:
    """One-time backfill: match existing drafts (seller_id IS NULL) to sellers."""
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT id, shipment_data FROM draft_shipments WHERE seller_id IS NULL"
    )
    if not rows:
        return

    logger.info("Backfilling seller_id for %d existing drafts...", len(rows))

    # Lazy import to avoid circular deps at module level
    from backend.services.seller_intelligence import match_or_create_seller

    for row in rows:
        try:
            sd = row["shipment_data"]
            if isinstance(sd, str):
                sd = json.loads(sd)
            shipper_addr = sd.get("shipper_address") or {}
            shipper_name = shipper_addr.get("name", "")
            if not shipper_name:
                continue

            seller_id, _ = await match_or_create_seller(shipper_name, shipper_addr)
            await pool.execute(
                "UPDATE draft_shipments SET seller_id = $1 WHERE id = $2",
                seller_id,
                row["id"],
            )
        except Exception:
            logger.warning(
                "Failed to backfill seller for draft %s", row["id"], exc_info=True
            )

    # Also backfill corrections: inherit seller_id from their parent draft
    await pool.execute("""
        UPDATE corrections c
        SET seller_id = ds.seller_id
        FROM draft_shipments ds
        WHERE c.draft_id = ds.id
          AND c.seller_id IS NULL
          AND ds.seller_id IS NOT NULL
    """)

    logger.info("Seller backfill complete")


async def close_db() -> None:
    """Drain and close the connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


def get_pool() -> asyncpg.Pool:
    """Return the active pool or raise if not initialized."""
    if _pool is None:
        raise RuntimeError("Database not initialized -- call init_db() first")
    return _pool


# ---------------------------------------------------------------------------
# Helpers: Upload batches
# ---------------------------------------------------------------------------


async def create_batch(file_count: int) -> UUID:
    pool = get_pool()
    row = await pool.fetchrow(
        "INSERT INTO upload_batches (file_count) VALUES ($1) RETURNING id",
        file_count,
    )
    return row["id"]


async def get_batch(batch_id: UUID) -> dict[str, Any] | None:
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM upload_batches WHERE id = $1", batch_id
    )
    return dict(row) if row else None


async def update_batch_status(batch_id: UUID, status: str) -> None:
    pool = get_pool()
    extra = ", completed_at = NOW()" if status in ("completed", "review") else ""
    # Clear step tracking on terminal states
    if status in ("completed", "review", "failed"):
        extra += ", current_step = NULL, step_progress = '{}'::jsonb"
    await pool.execute(
        f"UPDATE upload_batches SET status = $1{extra} WHERE id = $2",
        status,
        batch_id,
    )


async def update_batch_progress(
    batch_id: UUID, step: str, progress_data: dict[str, Any]
) -> None:
    """Persist the current extraction step and progress to the batch row."""
    pool = get_pool()
    await pool.execute(
        """UPDATE upload_batches
           SET current_step = $2, step_progress = $3::jsonb
           WHERE id = $1""",
        batch_id,
        step,
        json.dumps(progress_data),
    )


async def get_active_batches() -> list[dict[str, Any]]:
    """Return all batches with status = 'processing' (i.e. in-flight jobs)."""
    pool = get_pool()
    rows = await pool.fetch(
        """SELECT id, status, current_step, step_progress, file_count, created_at
           FROM upload_batches
           WHERE status = 'processing'
           ORDER BY created_at DESC"""
    )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Helpers: Uploaded files
# ---------------------------------------------------------------------------


async def create_file(
    batch_id: UUID,
    filename: str,
    file_url: str = "",
) -> UUID:
    pool = get_pool()
    row = await pool.fetchrow(
        """INSERT INTO uploaded_files (batch_id, filename, file_url)
           VALUES ($1, $2, $3) RETURNING id""",
        batch_id,
        filename,
        file_url,
    )
    return row["id"]


async def update_file_extraction(
    file_id: UUID,
    *,
    file_type: str | None = None,
    page_count: int | None = None,
    extracted_data: dict[str, Any] | None = None,
    confidence: float | None = None,
) -> None:
    pool = get_pool()
    await pool.execute(
        """UPDATE uploaded_files
           SET file_type = COALESCE($2, file_type),
               page_count = COALESCE($3, page_count),
               extracted_data = COALESCE($4::jsonb, extracted_data),
               confidence = COALESCE($5, confidence),
               processed_at = NOW()
           WHERE id = $1""",
        file_id,
        file_type,
        page_count,
        json.dumps(extracted_data) if extracted_data else None,
        confidence,
    )


async def get_files_for_batch(batch_id: UUID) -> list[dict[str, Any]]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT * FROM uploaded_files WHERE batch_id = $1 ORDER BY created_at",
        batch_id,
    )
    return [dict(r) for r in rows]


async def get_file(file_id: UUID) -> dict[str, Any] | None:
    pool = get_pool()
    row = await pool.fetchrow("SELECT * FROM uploaded_files WHERE id = $1", file_id)
    return dict(row) if row else None


async def unlink_file_from_draft(draft_id: UUID, file_id: UUID) -> None:
    pool = get_pool()
    await pool.execute(
        "DELETE FROM draft_shipment_files WHERE draft_id = $1 AND file_id = $2",
        draft_id, file_id,
    )


# ---------------------------------------------------------------------------
# Helpers: Draft shipments
# ---------------------------------------------------------------------------


async def create_draft(
    batch_id: UUID,
    shipment_data: dict[str, Any],
    confidence_scores: dict[str, Any] | None = None,
    grouping_reason: str | None = None,
    seller_id: UUID | None = None,
) -> UUID:
    pool = get_pool()
    row = await pool.fetchrow(
        """INSERT INTO draft_shipments (batch_id, shipment_data, confidence_scores, grouping_reason, seller_id)
           VALUES ($1, $2::jsonb, $3::jsonb, $4, $5) RETURNING id""",
        batch_id,
        json.dumps(shipment_data),
        json.dumps(confidence_scores) if confidence_scores else None,
        grouping_reason,
        seller_id,
    )
    return row["id"]


async def link_files_to_draft(draft_id: UUID, file_ids: list[UUID]) -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO draft_shipment_files (draft_id, file_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            [(draft_id, fid) for fid in file_ids],
        )


async def get_draft(draft_id: UUID) -> dict[str, Any] | None:
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM draft_shipments WHERE id = $1", draft_id
    )
    return dict(row) if row else None


async def get_drafts_for_batch(batch_id: UUID) -> list[dict[str, Any]]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT * FROM draft_shipments WHERE batch_id = $1 ORDER BY created_at",
        batch_id,
    )
    return [dict(r) for r in rows]


async def get_draft_file_ids(draft_id: UUID) -> list[UUID]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT file_id FROM draft_shipment_files WHERE draft_id = $1", draft_id
    )
    return [r["file_id"] for r in rows]


async def get_draft_files(draft_id: UUID) -> list[dict[str, Any]]:
    pool = get_pool()
    rows = await pool.fetch(
        """SELECT f.* FROM uploaded_files f
           JOIN draft_shipment_files dsf ON f.id = dsf.file_id
           WHERE dsf.draft_id = $1 ORDER BY f.created_at""",
        draft_id,
    )
    return [dict(r) for r in rows]


async def update_draft_status(draft_id: UUID, status: str) -> None:
    pool = get_pool()
    await pool.execute(
        "UPDATE draft_shipments SET status = $1 WHERE id = $2",
        status,
        draft_id,
    )


async def update_draft_corrections(
    draft_id: UUID,
    corrected_data: dict[str, Any],
) -> None:
    pool = get_pool()
    await pool.execute(
        """UPDATE draft_shipments
           SET corrected_data = $2::jsonb, reviewed_at = NOW()
           WHERE id = $1""",
        draft_id,
        json.dumps(corrected_data),
    )


async def delete_draft_permanent(draft_id: UUID) -> None:
    """Permanently delete a draft shipment from the database."""
    pool = get_pool()
    await pool.execute("DELETE FROM draft_shipments WHERE id = $1", draft_id)


async def update_draft_seller(draft_id: UUID, seller_id: UUID | None) -> None:
    """Update the seller_id column on a draft shipment."""
    pool = get_pool()
    await pool.execute(
        "UPDATE draft_shipments SET seller_id = $1 WHERE id = $2",
        seller_id,
        draft_id,
    )


async def update_draft_shipment_data(
    draft_id: UUID,
    shipment_data: dict[str, Any],
    confidence_scores: dict[str, Any] | None = None,
) -> None:
    """Replace shipment_data, clear corrected_data, and optionally update confidence."""
    pool = get_pool()
    await pool.execute(
        """UPDATE draft_shipments
           SET shipment_data = $2::jsonb,
               corrected_data = NULL,
               confidence_scores = COALESCE($3::jsonb, confidence_scores),
               reviewed_at = NOW()
           WHERE id = $1""",
        draft_id,
        json.dumps(shipment_data),
        json.dumps(confidence_scores) if confidence_scores else None,
    )


async def update_draft_pushed(draft_id: UUID, scancode: str) -> None:
    pool = get_pool()
    await pool.execute(
        """UPDATE draft_shipments
           SET status = 'pushed', xindus_scancode = $2, pushed_at = NOW()
           WHERE id = $1""",
        draft_id,
        scancode,
    )


# ---------------------------------------------------------------------------
# Helpers: Corrections
# ---------------------------------------------------------------------------


async def create_correction(
    draft_id: UUID,
    field_path: str,
    original_value: Any,
    corrected_value: Any,
    file_context: str | None = None,
    seller_id: UUID | None = None,
) -> UUID:
    pool = get_pool()
    row = await pool.fetchrow(
        """INSERT INTO corrections (draft_id, field_path, original_value, corrected_value, file_context, seller_id)
           VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6) RETURNING id""",
        draft_id,
        field_path,
        json.dumps(original_value),
        json.dumps(corrected_value),
        file_context,
        seller_id,
    )
    return row["id"]


async def get_corrections_for_field(
    field_path: str,
    limit: int = 5,
    seller_id: UUID | None = None,
) -> list[dict[str, Any]]:
    pool = get_pool()
    if seller_id is not None:
        rows = await pool.fetch(
            """SELECT field_path, original_value, corrected_value, file_context
               FROM corrections
               WHERE field_path = $1 AND seller_id = $3
               ORDER BY created_at DESC
               LIMIT $2""",
            field_path,
            limit,
            seller_id,
        )
    else:
        rows = await pool.fetch(
            """SELECT field_path, original_value, corrected_value, file_context
               FROM corrections
               WHERE field_path = $1
               ORDER BY created_at DESC
               LIMIT $2""",
            field_path,
            limit,
        )
    return [dict(r) for r in rows]


async def get_corrections_for_draft(draft_id: UUID) -> list[dict[str, Any]]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT * FROM corrections WHERE draft_id = $1 ORDER BY created_at",
        draft_id,
    )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Helpers: List all drafts (with status filter)
# ---------------------------------------------------------------------------


async def get_all_drafts(
    status: str | None = None,
    exclude_status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Get all drafts with optional status filter and pagination.

    Returns (drafts, total_count).
    """
    pool = get_pool()

    conditions: list[str] = []
    args: list[Any] = []
    arg_idx = 1

    if status:
        conditions.append(f"ds.status = ${arg_idx}")
        args.append(status)
        arg_idx += 1
    elif exclude_status:
        conditions.append(f"ds.status != ${arg_idx}")
        args.append(exclude_status)
        arg_idx += 1

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # Get total count — use ds alias consistently
    count_row = await pool.fetchrow(
        f"SELECT COUNT(*) as cnt FROM draft_shipments ds {where}",
        *args,
    )
    total = count_row["cnt"] if count_row else 0

    # Get paginated results
    args_page = list(args)
    query = f"""
        SELECT ds.*, ub.file_count as batch_file_count
        FROM draft_shipments ds
        LEFT JOIN upload_batches ub ON ds.batch_id = ub.id
        {where}
        ORDER BY ds.created_at DESC
        LIMIT ${arg_idx} OFFSET ${arg_idx + 1}
    """
    args_page.extend([limit, offset])

    rows = await pool.fetch(query, *args_page)
    return [dict(r) for r in rows], total


# ---------------------------------------------------------------------------
# Helpers: Correction stats (for learning)
# ---------------------------------------------------------------------------


async def get_correction_stats(
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Get per-field correction frequency stats.

    Returns list of {field_path, correction_count, latest_at}.
    """
    pool = get_pool()
    rows = await pool.fetch(
        """SELECT field_path,
                  COUNT(*) as correction_count,
                  MAX(created_at) as latest_at
           FROM corrections
           GROUP BY field_path
           ORDER BY correction_count DESC
           LIMIT $1""",
        limit,
    )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Helpers: Sellers (per-seller intelligence)
# ---------------------------------------------------------------------------


async def upsert_seller(
    name: str,
    normalized_name: str,
    shipper_address: dict[str, Any] | None = None,
    defaults: dict[str, Any] | None = None,
) -> UUID:
    """Create a seller or return existing id if normalized_name already exists."""
    pool = get_pool()
    row = await pool.fetchrow(
        """INSERT INTO sellers (name, normalized_name, shipper_address, defaults)
           VALUES ($1, $2, $3::jsonb, $4::jsonb)
           ON CONFLICT (normalized_name) DO UPDATE
             SET shipper_address = COALESCE($3::jsonb, sellers.shipper_address),
                 updated_at = NOW()
           RETURNING id""",
        name,
        normalized_name,
        json.dumps(shipper_address or {}),
        json.dumps(defaults or {}),
    )
    return row["id"]


async def get_seller_by_normalized_name(
    normalized_name: str,
) -> dict[str, Any] | None:
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM sellers WHERE normalized_name = $1",
        normalized_name,
    )
    return dict(row) if row else None


async def get_seller(seller_id: UUID) -> dict[str, Any] | None:
    pool = get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM sellers WHERE id = $1", seller_id
    )
    return dict(row) if row else None


async def get_all_sellers() -> list[dict[str, Any]]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT * FROM sellers ORDER BY shipment_count DESC, updated_at DESC"
    )
    return [dict(r) for r in rows]


async def update_seller_defaults(
    seller_id: UUID,
    defaults: dict[str, Any],
    shipper_address: dict[str, Any] | None = None,
) -> None:
    pool = get_pool()
    if shipper_address is not None:
        await pool.execute(
            """UPDATE sellers
               SET defaults = $2::jsonb,
                   shipper_address = $3::jsonb,
                   updated_at = NOW()
               WHERE id = $1""",
            seller_id,
            json.dumps(defaults),
            json.dumps(shipper_address),
        )
    else:
        await pool.execute(
            """UPDATE sellers
               SET defaults = $2::jsonb,
                   updated_at = NOW()
               WHERE id = $1""",
            seller_id,
            json.dumps(defaults),
        )


async def increment_seller_shipment_count(seller_id: UUID) -> None:
    pool = get_pool()
    await pool.execute(
        """UPDATE sellers
           SET shipment_count = shipment_count + 1,
               updated_at = NOW()
           WHERE id = $1""",
        seller_id,
    )


# ---------------------------------------------------------------------------
# Shipping methods
# ---------------------------------------------------------------------------


async def get_shipping_methods(b2b_only: bool = False) -> list[dict]:
    pool = get_pool()
    if b2b_only:
        rows = await pool.fetch(
            "SELECT code, name FROM shipping_methods WHERE active = true AND is_b2b = true ORDER BY name"
        )
    else:
        rows = await pool.fetch(
            "SELECT code, name FROM shipping_methods WHERE active = true ORDER BY name"
        )
    return [dict(r) for r in rows]
