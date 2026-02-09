import os
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
MAX_FILE_SIZE_MB = 20
ALLOWED_EXTENSIONS = {".pdf"}
LLM_MODEL_TEXT = "claude-haiku-4-5-20251001"
LLM_MODEL_VISION = "claude-sonnet-4-5-20250929"
LLM_MODEL_PACKING_LIST = "claude-sonnet-4-5-20250929"
LLM_MAX_TOKENS_INVOICE = 8192
LLM_MAX_TOKENS_PACKING_LIST = 16384

# Database (PostgreSQL on Railway)
DATABASE_URL = os.getenv("DATABASE_URL", "")

# S3-compatible object storage (Railway Buckets)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "b2b-agent-uploads")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# Xindus B2B API (Phase 3)
XINDUS_API_URL = os.getenv("XINDUS_API_URL", "")
