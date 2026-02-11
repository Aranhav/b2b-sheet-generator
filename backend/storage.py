"""S3-compatible object storage for Railway Buckets.

Provides upload / download / delete helpers for storing uploaded PDFs.
Falls back gracefully when S3 credentials are not configured (local dev).
"""
from __future__ import annotations

import io
import logging
import os

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "b2b-agent-uploads")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client

    if not S3_ENDPOINT or not S3_ACCESS_KEY:
        logger.warning("S3 credentials not configured -- storage will use local fallback")
        return None

    _client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=BotoConfig(signature_version="s3v4"),
    )
    logger.info("S3 client initialized: endpoint=%s, bucket=%s", S3_ENDPOINT, S3_BUCKET)
    return _client


# ---------------------------------------------------------------------------
# Local fallback (for development without S3)
# ---------------------------------------------------------------------------

_LOCAL_STORAGE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "output",
    "agent_uploads",
)


def _ensure_local_dir(key: str) -> str:
    """Ensure local directory exists and return full path."""
    full_path = os.path.join(_LOCAL_STORAGE_DIR, key)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    return full_path


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def upload_file(key: str, data: bytes, content_type: str = "application/pdf") -> str:
    """Upload a file to S3 (or local fallback). Returns the storage URL/path."""
    client = _get_client()

    if client is None:
        path = _ensure_local_dir(key)
        with open(path, "wb") as f:
            f.write(data)
        logger.info("Local upload: %s (%d bytes)", path, len(data))
        return f"local://{path}"

    try:
        client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        url = f"{S3_ENDPOINT}/{S3_BUCKET}/{key}"
        logger.info("S3 upload: %s (%d bytes)", key, len(data))
        return url
    except ClientError:
        logger.exception("S3 upload failed for key=%s", key)
        raise


async def download_file(key: str) -> bytes:
    """Download a file from S3 (or local fallback)."""
    client = _get_client()

    if client is None:
        path = os.path.join(_LOCAL_STORAGE_DIR, key)
        with open(path, "rb") as f:
            return f.read()

    try:
        response = client.get_object(Bucket=S3_BUCKET, Key=key)
        return response["Body"].read()
    except ClientError:
        logger.exception("S3 download failed for key=%s", key)
        raise


async def delete_file(key: str) -> None:
    """Delete a file from S3 (or local fallback)."""
    client = _get_client()

    if client is None:
        path = os.path.join(_LOCAL_STORAGE_DIR, key)
        if os.path.exists(path):
            os.remove(path)
        return

    try:
        client.delete_object(Bucket=S3_BUCKET, Key=key)
        logger.info("S3 delete: %s", key)
    except ClientError:
        logger.exception("S3 delete failed for key=%s", key)


def generate_presigned_url(key: str, expires_in: int = 3600) -> str:
    """Generate a presigned URL for an S3 object (valid for expires_in seconds)."""
    client = _get_client()
    if client is None:
        return ""
    try:
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": key},
            ExpiresIn=expires_in,
        )
    except ClientError:
        logger.exception("Failed to generate presigned URL for key=%s", key)
        return ""


def s3_key_from_url(file_url: str) -> str:
    """Extract the S3 key from a full file URL."""
    prefix = f"{S3_ENDPOINT}/{S3_BUCKET}/"
    if file_url.startswith(prefix):
        return file_url[len(prefix):]
    # Fallback: try to extract after bucket name
    if f"/{S3_BUCKET}/" in file_url:
        return file_url.split(f"/{S3_BUCKET}/", 1)[1]
    return ""


async def file_exists(key: str) -> bool:
    """Check if a file exists in S3 (or local fallback)."""
    client = _get_client()

    if client is None:
        path = os.path.join(_LOCAL_STORAGE_DIR, key)
        return os.path.exists(path)

    try:
        client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError:
        return False
