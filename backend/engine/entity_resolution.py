"""
Entity Resolution Service - Matches entities across systems.

v1 scope: Companies/Customers ONLY.

Two-pass approach:
- Pass 1: Deterministic matching on shared keys (email, domain, tax_id)
- Pass 2: LLM-assisted fuzzy matching with confidence scoring

Human confirmation required for all fuzzy matches. Every merge is reversible.

Data source: data/entity_test_scenarios.json -> entity_fragmentation
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


ALLOWED_ENTITY_TYPES = {"company", "customer"}

SCENARIO_FILE = Path("data/entity_test_scenarios.json")


class SourceRecord(BaseModel):
    """A record from a source system."""
    source_system: str
    record_id: str
    entity_type: str = "company"
    name: str
    field_values: Dict[str, Any] = Field(default_factory=dict)


class CanonicalEntity(BaseModel):
    """A unified entity with a global ID."""
    dcl_global_id: str
    entity_type: str = "company"
    canonical_name: str
    source_records: List[SourceRecord] = Field(default_factory=list)
    golden_record: Optional[Dict[str, Any]] = None
    created_at: str = ""
    updated_at: str = ""


class MatchCandidate(BaseModel):
    """A proposed match between two source records."""
    id: str
    record_a: SourceRecord
    record_b: SourceRecord
    match_type: str  # "deterministic" or "fuzzy" or "llm_assisted"
    confidence: float = Field(ge=0.0, le=1.0)
    shared_keys: List[str] = Field(default_factory=list)
    status: str = "pending"  # "pending", "confirmed", "rejected"
    dcl_global_id: Optional[str] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[str] = None


class MergeHistoryEntry(BaseModel):
    """Audit trail entry for entity merges/splits."""
    id: str
    action: str  # "merge", "split", "confirm", "reject"
    dcl_global_id: str
    source_records: List[str]  # record descriptions
    performed_by: str
    performed_at: str
    reason: Optional[str] = None
    previous_state: Optional[Dict[str, Any]] = None


def _load_scenario_data() -> Dict[str, Any]:
    """Load entity test scenarios from JSON file."""
    if not SCENARIO_FILE.exists():
        logger.warning(f"Scenario file not found: {SCENARIO_FILE}")
        return {}
    with open(SCENARIO_FILE, "r") as f:
        return json.load(f)


class EntityResolutionStore:
    """In-memory store for entity resolution data."""

    def __init__(self):
        self._source_records: List[SourceRecord] = []
        self._canonical_entities: Dict[str, CanonicalEntity] = {}
        self._match_candidates: Dict[str, MatchCandidate] = {}
        self._merge_history: List[MergeHistoryEntry] = []
        self._seed_from_scenarios()

    def _seed_from_scenarios(self):
        """Load source records from entity_test_scenarios.json."""
        data = _load_scenario_data()
        frag = data.get("entity_fragmentation", {})

        # Load company records
        for company in frag.get("companies", []):
            for rec_data in company.get("records", []):
                rec = SourceRecord(
                    source_system=rec_data["source_system"],
                    record_id=rec_data["record_id"],
                    entity_type="company",
                    name=rec_data["name"],
                    field_values=rec_data.get("field_values", {}),
                )
                self._source_records.append(rec)

        # Load non-match records (they exist in the pool for browse/search)
        for pair in frag.get("non_matches", []):
            for rec_data in pair.get("records", []):
                # Check if this record_id is already loaded (might overlap with company records)
                already = any(
                    r.record_id == rec_data["record_id"] and r.source_system == rec_data["source_system"]
                    for r in self._source_records
                )
                if not already:
                    rec = SourceRecord(
                        source_system=rec_data["source_system"],
                        record_id=rec_data["record_id"],
                        entity_type="company",
                        name=rec_data["name"],
                        field_values=rec_data.get("field_values", {}),
                    )
                    self._source_records.append(rec)

        logger.info(f"Loaded {len(self._source_records)} source records from scenarios")

    def add_source_record(self, record: SourceRecord):
        """Add a source record to the store."""
        self._source_records.append(record)

    def run_entity_resolution(self) -> List[MatchCandidate]:
        """
        Run entity resolution across all source records.

        Pass 1: Deterministic matching on shared keys
        Pass 2: Fuzzy matching on name similarity
        """
        # Clear existing unconfirmed candidates
        confirmed = {k: v for k, v in self._match_candidates.items() if v.status == "confirmed"}
        rejected = {k: v for k, v in self._match_candidates.items() if v.status == "rejected"}
        self._match_candidates = {**confirmed, **rejected}

        records = [r for r in self._source_records if r.entity_type in ALLOWED_ENTITY_TYPES]
        new_candidates = []

        # Pass 1: Deterministic matching
        for i, rec_a in enumerate(records):
            for rec_b in records[i + 1:]:
                if rec_a.source_system == rec_b.source_system:
                    continue

                already_matched = self._already_matched(rec_a, rec_b)
                if already_matched:
                    continue

                shared = self._find_shared_keys(rec_a, rec_b)
                if shared:
                    candidate = self._create_deterministic_match(rec_a, rec_b, shared)
                    new_candidates.append(candidate)

        # Pass 2: Fuzzy matching
        for i, rec_a in enumerate(records):
            for rec_b in records[i + 1:]:
                if rec_a.source_system == rec_b.source_system:
                    continue

                already_matched = self._already_matched(rec_a, rec_b)
                if already_matched:
                    continue

                similarity = self._name_similarity(rec_a.name, rec_b.name)
                industry_match = self._industry_match(rec_a, rec_b)
                industry_conflict = self._industry_conflict(rec_a, rec_b)

                # If industries explicitly conflict, skip regardless of name similarity
                if industry_conflict:
                    continue

                if similarity > 0.6 and industry_match:
                    candidate = self._create_fuzzy_match(rec_a, rec_b, similarity)
                    new_candidates.append(candidate)
                elif similarity > 0.8 and not industry_conflict:
                    candidate = self._create_fuzzy_match(rec_a, rec_b, similarity * 0.9)
                    new_candidates.append(candidate)

        for c in new_candidates:
            self._match_candidates[c.id] = c

        # Auto-confirm deterministic matches and create canonical entities
        for c in new_candidates:
            if c.match_type == "deterministic" and c.confidence >= 0.95:
                self._auto_confirm_match(c)

        return list(self._match_candidates.values())

    def _already_matched(self, rec_a: SourceRecord, rec_b: SourceRecord) -> bool:
        """Check if two records are already matched."""
        for candidate in self._match_candidates.values():
            a_matches = (
                (candidate.record_a.source_system == rec_a.source_system and candidate.record_a.record_id == rec_a.record_id)
                or (candidate.record_a.source_system == rec_b.source_system and candidate.record_a.record_id == rec_b.record_id)
            )
            b_matches = (
                (candidate.record_b.source_system == rec_a.source_system and candidate.record_b.record_id == rec_a.record_id)
                or (candidate.record_b.source_system == rec_b.source_system and candidate.record_b.record_id == rec_b.record_id)
            )
            if a_matches and b_matches:
                return True
        return False

    def _find_shared_keys(self, rec_a: SourceRecord, rec_b: SourceRecord) -> List[str]:
        """Find shared key fields between two records."""
        shared = []
        key_fields = ["email", "domain", "tax_id", "duns_number"]
        for key in key_fields:
            val_a = rec_a.field_values.get(key)
            val_b = rec_b.field_values.get(key)
            if val_a and val_b and str(val_a).lower() == str(val_b).lower():
                shared.append(key)
        return shared

    def _name_similarity(self, name_a: str, name_b: str) -> float:
        """Calculate name similarity using normalized comparison."""
        a = self._normalize_company_name(name_a)
        b = self._normalize_company_name(name_b)

        if a == b:
            return 1.0

        # Check if one is a prefix of the other
        if a.startswith(b) or b.startswith(a):
            return 0.85

        # Simple token overlap
        tokens_a = set(a.split())
        tokens_b = set(b.split())
        if not tokens_a or not tokens_b:
            return 0.0

        intersection = tokens_a & tokens_b
        union = tokens_a | tokens_b
        jaccard = len(intersection) / len(union)

        return jaccard

    def _normalize_company_name(self, name: str) -> str:
        """Normalize company name for comparison."""
        name = name.lower().strip()
        suffixes = [" llc", " inc", " inc.", " corp", " corp.", " corporation", " ltd", " ltd.", " co", " co."]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)].strip()
        name = name.replace(".", "").replace(",", "")
        return name

    def _industry_match(self, rec_a: SourceRecord, rec_b: SourceRecord) -> bool:
        """Check if two records are in the same/similar industry."""
        ind_a = rec_a.field_values.get("industry", "").lower()
        ind_b = rec_b.field_values.get("industry", "").lower()
        if not ind_a or not ind_b:
            return True  # Unknown industries don't disqualify
        # Synonym groups
        tech_terms = {"software", "technology", "tech", "saas", "it"}
        if ind_a in tech_terms and ind_b in tech_terms:
            return True
        conglomerate_terms = {"conglomerate", "diversified", "holding company"}
        if ind_a in conglomerate_terms and ind_b in conglomerate_terms:
            return True
        return ind_a == ind_b

    def _industry_conflict(self, rec_a: SourceRecord, rec_b: SourceRecord) -> bool:
        """Check if two records have explicitly conflicting industries."""
        ind_a = rec_a.field_values.get("industry", "").lower()
        ind_b = rec_b.field_values.get("industry", "").lower()
        if not ind_a or not ind_b:
            return False  # Can't conflict if unknown
        # Synonym groups
        tech_terms = {"software", "technology", "tech", "saas", "it"}
        if ind_a in tech_terms and ind_b in tech_terms:
            return False
        conglomerate_terms = {"conglomerate", "diversified", "holding company"}
        if ind_a in conglomerate_terms and ind_b in conglomerate_terms:
            return False
        manufacturing_terms = {"manufacturing", "industrial"}
        if ind_a in manufacturing_terms and ind_b in manufacturing_terms:
            return False
        return ind_a != ind_b

    def _create_deterministic_match(
        self, rec_a: SourceRecord, rec_b: SourceRecord, shared_keys: List[str]
    ) -> MatchCandidate:
        """Create a deterministic match candidate."""
        return MatchCandidate(
            id=str(uuid.uuid4()),
            record_a=rec_a,
            record_b=rec_b,
            match_type="deterministic",
            confidence=0.98,
            shared_keys=shared_keys,
            status="pending",
        )

    def _create_fuzzy_match(
        self, rec_a: SourceRecord, rec_b: SourceRecord, similarity: float
    ) -> MatchCandidate:
        """Create a fuzzy match candidate."""
        confidence = min(similarity * 0.9, 0.89)  # Fuzzy never exceeds 0.89
        return MatchCandidate(
            id=str(uuid.uuid4()),
            record_a=rec_a,
            record_b=rec_b,
            match_type="fuzzy",
            confidence=round(confidence, 2),
            shared_keys=[],
            status="pending",
        )

    def _auto_confirm_match(self, candidate: MatchCandidate):
        """Auto-confirm a high-confidence deterministic match."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Find or create canonical entity
        global_id = self._find_existing_entity(candidate.record_a, candidate.record_b)
        if not global_id:
            global_id = str(uuid.uuid4())
            entity = CanonicalEntity(
                dcl_global_id=global_id,
                entity_type="company",
                canonical_name=candidate.record_a.name,
                source_records=[candidate.record_a, candidate.record_b],
                created_at=now,
                updated_at=now,
            )
            entity.golden_record = self._build_golden_record(entity)
            self._canonical_entities[global_id] = entity
        else:
            entity = self._canonical_entities[global_id]
            # One-record-per-system constraint: don't add a second record from the same system
            existing_systems = {sr.source_system: sr.record_id for sr in entity.source_records}
            for rec in [candidate.record_a, candidate.record_b]:
                is_in_entity = any(
                    sr.source_system == rec.source_system and sr.record_id == rec.record_id
                    for sr in entity.source_records
                )
                if not is_in_entity:
                    if rec.source_system in existing_systems:
                        # Different record from same system - skip this match entirely
                        return
                    entity.source_records.append(rec)
            entity.updated_at = now
            entity.golden_record = self._build_golden_record(entity)

        candidate.status = "confirmed"
        candidate.dcl_global_id = global_id
        candidate.resolved_by = "system_auto"
        candidate.resolved_at = now

        self._merge_history.append(
            MergeHistoryEntry(
                id=str(uuid.uuid4()),
                action="merge",
                dcl_global_id=global_id,
                source_records=[
                    f"{candidate.record_a.source_system}:{candidate.record_a.record_id}",
                    f"{candidate.record_b.source_system}:{candidate.record_b.record_id}",
                ],
                performed_by="system_auto",
                performed_at=now,
                reason=f"Deterministic match on {', '.join(candidate.shared_keys)}",
            )
        )

    def _find_existing_entity(self, *records: SourceRecord) -> Optional[str]:
        """Find existing canonical entity containing any of these records."""
        for global_id, entity in self._canonical_entities.items():
            for rec in records:
                for sr in entity.source_records:
                    if sr.source_system == rec.source_system and sr.record_id == rec.record_id:
                        return global_id
        return None

    def _build_golden_record(self, entity: CanonicalEntity) -> Dict[str, Any]:
        """Build a golden record from source records using trust-based selection with freshness tiebreaker."""
        # SOR priority: netsuite_erp = sap_erp (ERP) > salesforce_crm > hubspot_crm > others
        SOR_PRIORITY = {
            "netsuite_erp": 4,
            "sap_erp": 4,
            "salesforce_crm": 3,
            "hubspot_crm": 2,
        }

        golden: Dict[str, Any] = {"canonical_name": entity.canonical_name}
        field_sources: Dict[str, Dict[str, Any]] = {}

        for rec in entity.source_records:
            priority = SOR_PRIORITY.get(rec.source_system, 1)
            last_updated = rec.field_values.get("last_updated", "1970-01-01T00:00:00Z")

            for field, value in rec.field_values.items():
                if field == "last_updated":
                    continue

                if field not in field_sources:
                    field_sources[field] = {
                        "value": value,
                        "source_system": rec.source_system,
                        "priority": priority,
                        "last_updated": last_updated,
                        "reason": f"Highest SOR priority ({rec.source_system})",
                    }
                else:
                    existing = field_sources[field]
                    # Prefer higher priority; tiebreak by freshness
                    if priority > existing["priority"]:
                        field_sources[field] = {
                            "value": value,
                            "source_system": rec.source_system,
                            "priority": priority,
                            "last_updated": last_updated,
                            "reason": f"Highest SOR priority ({rec.source_system})",
                        }
                    elif priority == existing["priority"] and last_updated > existing["last_updated"]:
                        field_sources[field] = {
                            "value": value,
                            "source_system": rec.source_system,
                            "priority": priority,
                            "last_updated": last_updated,
                            "reason": f"Most recent data ({rec.source_system}, updated {last_updated[:10]})",
                        }

        for field, info in field_sources.items():
            golden[field] = {
                "value": info["value"],
                "source_system": info["source_system"],
                "selection_reason": info["reason"],
            }

        return golden

    def confirm_match(self, candidate_id: str, approved: bool, resolved_by: str = "admin") -> Optional[MatchCandidate]:
        """Confirm or reject a match candidate."""
        candidate = self._match_candidates.get(candidate_id)
        if not candidate:
            return None

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        candidate.resolved_by = resolved_by
        candidate.resolved_at = now

        if approved:
            candidate.status = "confirmed"
            # Create or update canonical entity
            global_id = self._find_existing_entity(candidate.record_a, candidate.record_b)
            if not global_id:
                global_id = str(uuid.uuid4())
                entity = CanonicalEntity(
                    dcl_global_id=global_id,
                    entity_type="company",
                    canonical_name=candidate.record_a.name,
                    source_records=[candidate.record_a, candidate.record_b],
                    created_at=now,
                    updated_at=now,
                )
                entity.golden_record = self._build_golden_record(entity)
                self._canonical_entities[global_id] = entity
            else:
                entity = self._canonical_entities[global_id]
                for rec in [candidate.record_a, candidate.record_b]:
                    if not any(
                        sr.source_system == rec.source_system and sr.record_id == rec.record_id
                        for sr in entity.source_records
                    ):
                        entity.source_records.append(rec)
                entity.updated_at = now
                entity.golden_record = self._build_golden_record(entity)

            candidate.dcl_global_id = global_id

            self._merge_history.append(
                MergeHistoryEntry(
                    id=str(uuid.uuid4()),
                    action="confirm",
                    dcl_global_id=global_id,
                    source_records=[
                        f"{candidate.record_a.source_system}:{candidate.record_a.record_id}",
                        f"{candidate.record_b.source_system}:{candidate.record_b.record_id}",
                    ],
                    performed_by=resolved_by,
                    performed_at=now,
                    reason="Human confirmed match",
                )
            )
        else:
            candidate.status = "rejected"
            self._merge_history.append(
                MergeHistoryEntry(
                    id=str(uuid.uuid4()),
                    action="reject",
                    dcl_global_id="N/A",
                    source_records=[
                        f"{candidate.record_a.source_system}:{candidate.record_a.record_id}",
                        f"{candidate.record_b.source_system}:{candidate.record_b.record_id}",
                    ],
                    performed_by=resolved_by,
                    performed_at=now,
                    reason="Human rejected match",
                )
            )

        return candidate

    def undo_merge(self, dcl_global_id: str, performed_by: str = "admin") -> bool:
        """Undo a confirmed merge - split entity back into separate records."""
        entity = self._canonical_entities.get(dcl_global_id)
        if not entity:
            return False

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Save previous state for audit
        previous_state = {
            "canonical_name": entity.canonical_name,
            "source_records": [
                f"{sr.source_system}:{sr.record_id}" for sr in entity.source_records
            ],
            "golden_record": entity.golden_record,
        }

        # Revert all match candidates pointing to this entity
        for candidate in self._match_candidates.values():
            if candidate.dcl_global_id == dcl_global_id and candidate.status == "confirmed":
                candidate.status = "pending"
                candidate.dcl_global_id = None
                candidate.resolved_by = None
                candidate.resolved_at = None

        # Remove canonical entity
        del self._canonical_entities[dcl_global_id]

        self._merge_history.append(
            MergeHistoryEntry(
                id=str(uuid.uuid4()),
                action="split",
                dcl_global_id=dcl_global_id,
                source_records=previous_state["source_records"],
                performed_by=performed_by,
                performed_at=now,
                reason="Undo merge",
                previous_state=previous_state,
            )
        )

        return True

    def browse_entities(self, search_term: str) -> List[Dict[str, Any]]:
        """Browse entities matching a search term across all systems."""
        term_lower = search_term.lower()
        results = []

        for rec in self._source_records:
            if term_lower in rec.name.lower():
                # Find match status
                match_status = "unmatched"
                confidence = 0.0
                dcl_global_id = None

                for candidate in self._match_candidates.values():
                    if (
                        (candidate.record_a.source_system == rec.source_system and candidate.record_a.record_id == rec.record_id)
                        or (candidate.record_b.source_system == rec.source_system and candidate.record_b.record_id == rec.record_id)
                    ):
                        match_status = candidate.status
                        confidence = candidate.confidence
                        dcl_global_id = candidate.dcl_global_id
                        break

                results.append({
                    "source_system": rec.source_system,
                    "record_id": rec.record_id,
                    "name": rec.name,
                    "entity_type": rec.entity_type,
                    "field_values": rec.field_values,
                    "match_status": match_status,
                    "confidence": confidence,
                    "dcl_global_id": dcl_global_id,
                })

        return results

    def get_canonical_entity(self, dcl_global_id: str) -> Optional[CanonicalEntity]:
        """Get a canonical entity by ID."""
        return self._canonical_entities.get(dcl_global_id)

    def get_all_canonical_entities(self) -> List[CanonicalEntity]:
        """Get all canonical entities."""
        return list(self._canonical_entities.values())

    def get_match_candidates(self) -> List[MatchCandidate]:
        """Get all match candidates."""
        return list(self._match_candidates.values())

    def get_match_candidate(self, candidate_id: str) -> Optional[MatchCandidate]:
        """Get a match candidate by ID."""
        return self._match_candidates.get(candidate_id)

    def get_merge_history(self) -> List[MergeHistoryEntry]:
        """Get the full merge history."""
        return self._merge_history

    def is_entity_type_allowed(self, entity_type: str) -> bool:
        """Check if an entity type is allowed for resolution."""
        return entity_type.lower() in ALLOWED_ENTITY_TYPES


# Singleton
_store: Optional[EntityResolutionStore] = None


def get_entity_store() -> EntityResolutionStore:
    """Get or create the singleton entity resolution store."""
    global _store
    if _store is None:
        _store = EntityResolutionStore()
    return _store
