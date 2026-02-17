"""
Centralized constants for the DCL backend.

Every value reads from an environment variable with the current hardcoded
value as default, so existing deployments require zero configuration changes.
"""
import os
from datetime import datetime, timezone

# --- API Version ---
API_VERSION = os.getenv("DCL_API_VERSION", "2.0.0")

# --- Farm API ---
FARM_API_URL = os.getenv("FARM_API_URL", "https://autonomos.farm")

# --- Confidence Thresholds (heuristic mapper) ---
CONFIDENCE_POSITIVE_PATTERN = float(os.getenv("DCL_CONFIDENCE_POSITIVE_PATTERN", "0.95"))
CONFIDENCE_EXACT_FIELD = float(os.getenv("DCL_CONFIDENCE_EXACT_FIELD", "0.95"))
CONFIDENCE_PARTIAL_FIELD = float(os.getenv("DCL_CONFIDENCE_PARTIAL_FIELD", "0.75"))
CONFIDENCE_SYNONYM = float(os.getenv("DCL_CONFIDENCE_SYNONYM", "0.70"))
CONFIDENCE_CONCEPT_IN_NAME = float(os.getenv("DCL_CONFIDENCE_CONCEPT_IN_NAME", "0.80"))
CONFIDENCE_CONTEXT_BOOST = float(os.getenv("DCL_CONFIDENCE_CONTEXT_BOOST", "0.05"))
CONFIDENCE_CONTEXT_CAP = float(os.getenv("DCL_CONFIDENCE_CONTEXT_CAP", "0.95"))
CONFIDENCE_SEMANTIC_AMOUNT = float(os.getenv("DCL_CONFIDENCE_SEMANTIC_AMOUNT", "0.65"))
CONFIDENCE_SEMANTIC_ID = float(os.getenv("DCL_CONFIDENCE_SEMANTIC_ID", "0.60"))

# --- Confidence Thresholds (mapping service) ---
CONFIDENCE_CONCEPT_MATCH = float(os.getenv("DCL_CONFIDENCE_CONCEPT_MATCH", "0.95"))
CONFIDENCE_EXAMPLE_EXACT = float(os.getenv("DCL_CONFIDENCE_EXAMPLE_EXACT", "0.90"))
CONFIDENCE_EXAMPLE_PARTIAL = float(os.getenv("DCL_CONFIDENCE_EXAMPLE_PARTIAL", "0.75"))
CONFIDENCE_DEFAULT = float(os.getenv("DCL_CONFIDENCE_DEFAULT", "0.60"))

# --- Confidence Thresholds (RAG / LLM) ---
RAG_CONFIDENCE_THRESHOLD = float(os.getenv("DCL_RAG_CONFIDENCE_THRESHOLD", "0.75"))
LLM_VALIDATION_THRESHOLD = float(os.getenv("DCL_LLM_VALIDATION_THRESHOLD", "0.80"))

# --- Trust Scores ---
TRUST_SCORE_FALLBACK = int(os.getenv("DCL_TRUST_SCORE_FALLBACK", "60"))

# --- LLM Configuration ---
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "gpt-4o-mini")

# --- Pinecone Configuration ---
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "dcl-mapping-lessons")
PINECONE_DIMENSION = int(os.getenv("PINECONE_DIMENSION", "1536"))
PINECONE_CLOUD = os.getenv("PINECONE_CLOUD", "aws")
PINECONE_REGION = os.getenv("PINECONE_REGION", "us-east-1")

# --- Cache TTLs (seconds) ---
ONTOLOGY_CACHE_TTL = float(os.getenv("DCL_ONTOLOGY_CACHE_TTL", "300.0"))
MAPPINGS_CACHE_TTL = float(os.getenv("DCL_MAPPINGS_CACHE_TTL", "60.0"))
SCHEMA_CACHE_TTL = float(os.getenv("DCL_SCHEMA_CACHE_TTL", "300.0"))

# --- Connection Pool ---
POOL_RETRY_COOLDOWN = float(os.getenv("DCL_POOL_RETRY_COOLDOWN", "30.0"))

# --- CORS ---
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")


def utc_now() -> str:
    """Return current UTC time as ISO-8601 string. Single format everywhere."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
