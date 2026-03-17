"""
COFA Mapping Writer
====================
Converts Maestra's structured COFA mapping output into semantic triples
and writes them to Postgres via TripleStore.

Deterministic — no LLM calls. Idempotent via run_id deactivation.
"""

import json
import uuid

from backend.db.triple_store import TripleStore
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Fixed namespace for deterministic canonical_id generation
_COFA_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

# Map Maestra's mapping_basis to DB-allowed resolution_method values.
# DB constraint: deterministic | fuzzy | manual | NULL
_RESOLUTION_METHOD_MAP = {
    "exact_match": "deterministic",
    "deterministic": "deterministic",
    "semantic_similarity": "fuzzy",
    "fuzzy_match": "fuzzy",
    "fuzzy": "fuzzy",
    "manual_override": "manual",
    "manual": "manual",
}


def _resolve_method(basis: str | None) -> str | None:
    """Convert mapping_basis to a DB-valid resolution_method."""
    if not basis:
        return None
    return _RESOLUTION_METHOD_MAP.get(basis)


def _confidence_tier(score: float) -> str:
    """Map numeric confidence to tier label."""
    if score >= 0.9:
        return "exact"
    if score >= 0.8:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


def _validate(data: dict) -> list[str]:
    """Validate input data. Returns list of error messages (empty = valid)."""
    errors: list[str] = []

    for field in ("engagement_id", "acquirer_entity_id", "target_entity_id", "tenant_id", "run_id"):
        val = data.get(field)
        if not val or not isinstance(val, str) or not val.strip():
            errors.append(f"Required field '{field}' is missing or empty")

    mappings = data.get("mappings")
    if not mappings or not isinstance(mappings, list) or len(mappings) == 0:
        errors.append("'mappings' must be a non-empty list")
        return errors  # No point validating individual mappings

    for i, m in enumerate(mappings):
        ua = m.get("unified_account")
        if not ua or not isinstance(ua, str) or not ua.strip():
            errors.append(f"mappings[{i}]: 'unified_account' is missing or empty")

        acq = m.get("acquirer_account")
        tgt = m.get("target_account")
        if not acq and not tgt:
            errors.append(
                f"mappings[{i}]: at least one of 'acquirer_account' or 'target_account' must be non-null"
            )

        conf = m.get("confidence")
        if conf is None:
            errors.append(f"mappings[{i}]: 'confidence' is required")
        elif not isinstance(conf, (int, float)) or conf < 0.0 or conf > 1.0:
            errors.append(f"mappings[{i}]: 'confidence' must be between 0.0 and 1.0, got {conf}")

    return errors


def _build_mapping_triples(data: dict) -> list[dict]:
    """Build triples for mapping entries."""
    triples: list[dict] = []
    engagement_id = data["engagement_id"]
    acquirer_id = data["acquirer_entity_id"]
    target_id = data["target_entity_id"]
    tenant_id = data["tenant_id"]
    run_id = data["run_id"]

    for m in data["mappings"]:
        unified = m["unified_account"]
        acq_acct = m.get("acquirer_account")
        tgt_acct = m.get("target_account")
        confidence = m["confidence"]
        tier = _confidence_tier(confidence)
        basis = _resolve_method(m.get("mapping_basis"))
        canonical = str(uuid.uuid5(_COFA_NAMESPACE, f"{engagement_id}:{unified}"))
        concept = f"cofa_mapping.{unified}"

        # Acquirer-side triple
        if acq_acct:
            triples.append({
                "tenant_id": tenant_id,
                "entity_id": acquirer_id,
                "concept": concept,
                "property": "mapping_target",
                "value": tgt_acct or "N/A",
                "period": None,
                "currency": None,
                "unit": None,
                "source_system": "maestra",
                "source_table": None,
                "source_field": "cofa_unification",
                "pipe_id": None,
                "run_id": run_id,
                "confidence_score": confidence,
                "confidence_tier": tier,
                "canonical_id": canonical,
                "resolution_method": basis,
                "resolution_confidence": None,
            })

        # Target-side triple
        if tgt_acct:
            triples.append({
                "tenant_id": tenant_id,
                "entity_id": target_id,
                "concept": concept,
                "property": "mapping_source",
                "value": acq_acct or "N/A",
                "period": None,
                "currency": None,
                "unit": None,
                "source_system": "maestra",
                "source_table": None,
                "source_field": "cofa_unification",
                "pipe_id": None,
                "run_id": run_id,
                "confidence_score": confidence,
                "confidence_tier": tier,
                "canonical_id": canonical,
                "resolution_method": basis,
                "resolution_confidence": None,
            })

    return triples


def _build_conflict_triples(data: dict) -> list[dict]:
    """Build triples for conflict entries."""
    triples: list[dict] = []
    acquirer_id = data["acquirer_entity_id"]
    tenant_id = data["tenant_id"]
    run_id = data["run_id"]

    for c in data.get("conflicts", []):
        conflict_id = c.get("conflict_id", "unknown")
        concept = f"cofa_conflict.{conflict_id}"

        property_fields = [
            "conflict_type", "severity", "dollar_impact", "description",
            "acquirer_treatment", "target_treatment", "resolution_status",
        ]
        for prop in property_fields:
            val = c.get(prop)
            if val is None:
                continue
            triples.append({
                "tenant_id": tenant_id,
                "entity_id": acquirer_id,
                "concept": concept,
                "property": prop,
                "value": val,
                "period": None,
                "currency": None,
                "unit": None,
                "source_system": "maestra",
                "source_table": None,
                "source_field": "cofa_unification",
                "pipe_id": None,
                "run_id": run_id,
                "confidence_score": 1.0,
                "confidence_tier": "exact",
                "canonical_id": None,
                "resolution_method": None,
                "resolution_confidence": None,
            })

    return triples


def _build_unified_account_triples(data: dict) -> list[dict]:
    """Build triples for unified account entries."""
    triples: list[dict] = []
    tenant_id = data["tenant_id"]
    run_id = data["run_id"]

    for ua in data.get("unified_accounts", []):
        account_name = ua.get("account_name", "unknown")
        concept = f"cofa_unified.{account_name}"

        prop_map = {
            "account_type": ua.get("account_type"),
            "hierarchy_parent": ua.get("hierarchy_parent"),
            "source_entities": ",".join(ua["source_entities"]) if ua.get("source_entities") else None,
        }

        for prop, val in prop_map.items():
            if val is None:
                continue
            triples.append({
                "tenant_id": tenant_id,
                "entity_id": "combined",
                "concept": concept,
                "property": prop,
                "value": val,
                "period": None,
                "currency": None,
                "unit": None,
                "source_system": "maestra",
                "source_table": None,
                "source_field": "cofa_unification",
                "pipe_id": None,
                "run_id": run_id,
                "confidence_score": 1.0,
                "confidence_tier": "exact",
                "canonical_id": None,
                "resolution_method": None,
                "resolution_confidence": None,
            })

    return triples


def write_cofa_mapping(data: dict) -> dict:
    """Write COFA mapping triples to semantic_triples. Returns result dict."""
    # Validate input
    errors = _validate(data)
    if errors:
        return {
            "status": "error",
            "errors": errors,
            "triple_count": 0,
            "mapping_count": 0,
        }

    run_id = data["run_id"]
    acquirer_id = data["acquirer_entity_id"]
    target_id = data["target_entity_id"]
    store = TripleStore()

    # Deactivate ALL prior COFA triples for this entity pair — prevents
    # accumulation across runs (each run_id is unique, so run-scoped
    # deactivation alone would leave old runs' triples active).
    deactivated = store.deactivate_cofa_triples([acquirer_id, target_id])
    if deactivated:
        logger.info(f"[COFA] Deactivated {deactivated} prior COFA triples for entities [{acquirer_id}, {target_id}]")

    # Build all triples
    mapping_triples = _build_mapping_triples(data)
    conflict_triples = _build_conflict_triples(data)
    unified_triples = _build_unified_account_triples(data)

    all_triples = mapping_triples + conflict_triples + unified_triples

    if not all_triples:
        return {
            "status": "error",
            "errors": ["No triples generated from input data"],
            "triple_count": 0,
            "mapping_count": len(data.get("mappings", [])),
        }

    # Write to DB
    inserted = store.insert_triples(all_triples)

    logger.info(
        f"[COFA] Wrote {inserted} triples "
        f"(mappings={len(mapping_triples)}, conflicts={len(conflict_triples)}, "
        f"unified={len(unified_triples)}) for run_id={run_id}"
    )

    return {
        "status": "success",
        "triple_count": inserted,
        "mapping_count": len(data["mappings"]),
        "conflict_count": len(data.get("conflicts", [])),
        "unified_account_count": len(data.get("unified_accounts", [])),
        "breakdown": {
            "mapping_triples": len(mapping_triples),
            "conflict_triples": len(conflict_triples),
            "unified_triples": len(unified_triples),
        },
        "run_id": run_id,
    }
