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

# Gaia Dynamics (tariff classification)
GAIA_API_URL = os.getenv("GAIA_API_URL", "https://platform-api.gaiadynamics.ai")
GAIA_API_KEY = os.getenv("GAIA_API_KEY", "")
GAIA_TIMEOUT_SECONDS = int(os.getenv("GAIA_TIMEOUT_SECONDS", "30"))

# Xindus B2B API (Phase 3)
XINDUS_API_URL = os.getenv("XINDUS_API_URL", "")


# ── Country code normalization ──────────────────────────────────────────
# Maps common country names/variants to ISO 3166-1 alpha-2 codes.
# Only includes countries we commonly encounter in B2B shipments.

_COUNTRY_NAME_MAP: dict[str, str] = {
    "united states": "US", "united states of america": "US", "usa": "US",
    "india": "IN", "bharat": "IN",
    "united kingdom": "GB", "great britain": "GB", "england": "GB",
    "canada": "CA", "australia": "AU", "germany": "DE", "deutschland": "DE",
    "france": "FR", "japan": "JP", "china": "CN", "south korea": "KR",
    "korea": "KR", "singapore": "SG", "hong kong": "HK",
    "united arab emirates": "AE", "uae": "AE", "dubai": "AE",
    "saudi arabia": "SA", "netherlands": "NL", "holland": "NL",
    "new zealand": "NZ", "mexico": "MX", "brazil": "BR",
    "italy": "IT", "spain": "ES", "sweden": "SE", "norway": "NO",
    "denmark": "DK", "finland": "FI", "switzerland": "CH",
    "belgium": "BE", "austria": "AT", "ireland": "IE",
    "poland": "PL", "portugal": "PT", "czech republic": "CZ",
    "south africa": "ZA", "malaysia": "MY", "thailand": "TH",
    "indonesia": "ID", "philippines": "PH", "vietnam": "VN",
    "bangladesh": "BD", "sri lanka": "LK", "nepal": "NP",
    "pakistan": "PK", "israel": "IL", "turkey": "TR", "egypt": "EG",
    "nigeria": "NG", "kenya": "KE", "ghana": "GH",
    "colombia": "CO", "argentina": "AR", "chile": "CL", "peru": "PE",
    "taiwan": "TW",
}


def normalize_country_code(raw: str, default: str = "US") -> str:
    """Convert a country name or code to a 2-letter ISO code.

    Handles: "United States" → "US", "IN" → "IN", "usa" → "US", etc.
    """
    if not raw:
        return default
    cleaned = raw.strip()
    # Already a 2-letter code
    if len(cleaned) == 2:
        return cleaned.upper()
    # Try name lookup (case-insensitive)
    code = _COUNTRY_NAME_MAP.get(cleaned.lower())
    if code:
        return code
    # Last resort: if it's a 3-letter code like "USA" or "GBR", try first 2
    if len(cleaned) == 3 and cleaned.isalpha():
        maybe = _COUNTRY_NAME_MAP.get(cleaned.lower())
        return maybe or cleaned[:2].upper()
    return default
