"""
Centralized constants for the DCL backend.

Every value reads from an environment variable with the current hardcoded
value as default, so existing deployments require zero configuration changes.
"""
import os

# --- Confidence Thresholds ---
RAG_CONFIDENCE_THRESHOLD = float(os.getenv("DCL_RAG_CONFIDENCE_THRESHOLD", "0.75"))
LLM_VALIDATION_THRESHOLD = float(os.getenv("DCL_LLM_VALIDATION_THRESHOLD", "0.80"))

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
