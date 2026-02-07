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
