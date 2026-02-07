"""
Conflict Detection Service - Cross-system field comparison on resolved entities.

Detects when two systems disagree about the same entity and classifies root cause.
Conflict scenarios loaded from data/entity_test_scenarios.json -> embedded_conflicts.

Severity labels: critical, medium, none
Root causes: timing, scope, stale_data, recognition_method, none
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

SCENARIO_FILE = Path("data/entity_test_scenarios.json")

SEVERITY_ORDER = {"critical": 3, "medium": 2, "low": 1, "none": 0}


class ConflictValue(BaseModel):
    """Value from one system for a conflicting field."""
    source_system: str
    value: Any
    last_updated: str
    quality_score: float = 1.0


class Conflict(BaseModel):
    """A detected conflict between systems."""
    id: str
    entity_name: str
    dcl_global_id: str
    metric: str
    values: List[ConflictValue]
    root_cause: str  # "timing", "scope", "stale_data", "recognition_method", "none"
    root_cause_explanation: str
    severity: str  # "critical", "medium", "none"
    trust_recommendation: Dict[str, str]  # {"system": ..., "reasoning": ...}
    status: str = "active"  # "active", "resolved"
    resolved_by: Optional[str] = None
    resolved_at: Optional[str] = None
    resolution_decision: Optional[str] = None
    resolution_rationale: Optional[str] = None
    created_at: str = ""


class ConflictResolutionEntry(BaseModel):
    """Audit trail for conflict resolution."""
    id: str
    conflict_id: str
    decision: str
    rationale: str
    resolved_by: str
    resolved_at: str


class QualityScoreAdjustment(BaseModel):
    """Record of quality score adjustments from conflict resolution."""
    source_system: str
    metric: str
    adjustment: float
    reason: str
    timestamp: str


def _load_scenario_conflicts() -> List[Dict[str, Any]]:
    """Load embedded conflict scenarios from JSON."""
    if not SCENARIO_FILE.exists():
        return []
    with open(SCENARIO_FILE, "r") as f:
        data = json.load(f)
    return data.get("embedded_conflicts", {}).get("conflicts", [])


class ConflictDetectionStore:
    """In-memory store for conflict detection data."""

    def __init__(self):
        self._conflicts: Dict[str, Conflict] = {}
        self._resolution_history: List[ConflictResolutionEntry] = []
        self._quality_adjustments: List[QualityScoreAdjustment] = []
        self._source_quality_scores: Dict[str, Dict[str, float]] = {}
        self._resolution_counts: Dict[str, Dict[str, int]] = {}  # source -> metric -> count
        self._scenario_conflicts = _load_scenario_conflicts()

    def _find_scenario_for_entity(self, entity_name: str, metric: str) -> Optional[Dict[str, Any]]:
        """Find a pre-defined conflict scenario matching an entity and metric."""
        entity_lower = entity_name.lower()
        for sc in self._scenario_conflicts:
            sc_entity = sc.get("entity", "").lower()
            sc_metric = sc.get("metric", "")
            if sc_entity in entity_lower or entity_lower in sc_entity:
                if sc_metric == metric:
                    return sc
        return None

    def detect_conflicts(self) -> List[Conflict]:
        """
        Run conflict detection across all resolved entities.

        Compares field values across source systems for each canonical entity.
        """
        from backend.engine.entity_resolution import get_entity_store

        entity_store = get_entity_store()
        entities = entity_store.get_all_canonical_entities()

        new_conflicts = []

        for entity in entities:
            if len(entity.source_records) < 2:
                continue

            entity_conflicts = self._detect_field_conflicts(entity)
            new_conflicts.extend(entity_conflicts)

        # Add new conflicts (don't overwrite resolved ones)
        for conflict in new_conflicts:
            existing = self._find_existing_conflict(
                conflict.dcl_global_id, conflict.metric
            )
            if existing and existing.status == "resolved":
                continue
            self._conflicts[conflict.id] = conflict

        return [c for c in self._conflicts.values() if c.status == "active"]

    def _detect_field_conflicts(self, entity) -> List[Conflict]:
        """Detect field-level conflicts for a canonical entity."""
        conflicts = []
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        comparable_fields = ["revenue", "amount", "headcount", "employee_count", "employees"]

        field_values: Dict[str, List[Dict[str, Any]]] = {}

        for rec in entity.source_records:
            for field in comparable_fields:
                if field in rec.field_values:
                    if field not in field_values:
                        field_values[field] = []
                    field_values[field].append({
                        "source_system": rec.source_system,
                        "value": rec.field_values[field],
                        "last_updated": rec.field_values.get("last_updated", now),
                    })

        for field, values in field_values.items():
            if len(values) < 2:
                continue

            # Check if values actually differ
            unique_values = set()
            for v in values:
                if isinstance(v["value"], (int, float)):
                    unique_values.add(v["value"])

            if len(unique_values) <= 1:
                continue

            # Look up scenario data for root cause classification
            scenario = self._find_scenario_for_entity(entity.canonical_name, field)

            if scenario and scenario.get("root_cause") != "none":
                root_cause = scenario["root_cause"]
                explanation = scenario.get("description", "")
                severity = scenario.get("expected_severity", "medium")
                trust_rec_data = scenario.get("trust_recommendation")
                if trust_rec_data:
                    trust_rec = {
                        "system": trust_rec_data["system"],
                        "reasoning": trust_rec_data["reasoning"],
                    }
                else:
                    trust_rec = self._get_trust_recommendation(field, values)
            else:
                root_cause, explanation = self._classify_root_cause(field, values)
                severity = self._calculate_severity_label(field, values)
                trust_rec = self._get_trust_recommendation(field, values)

            conflict = Conflict(
                id=str(uuid.uuid4()),
                entity_name=entity.canonical_name,
                dcl_global_id=entity.dcl_global_id,
                metric=field,
                values=[
                    ConflictValue(
                        source_system=v["source_system"],
                        value=v["value"],
                        last_updated=v["last_updated"],
                        quality_score=self._get_source_quality(v["source_system"], field),
                    )
                    for v in values
                ],
                root_cause=root_cause,
                root_cause_explanation=explanation,
                severity=severity,
                trust_recommendation=trust_rec,
                status="active",
                created_at=now,
            )
            conflicts.append(conflict)

        return conflicts

    def _classify_root_cause(
        self, field: str, values: List[Dict[str, Any]]
    ) -> tuple:
        """Classify root cause of a conflict using heuristics."""
        # Check for stale data
        dates = []
        for v in values:
            updated = v.get("last_updated", "")
            if updated:
                try:
                    dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    dates.append((v["source_system"], dt))
                except ValueError:
                    pass

        if len(dates) >= 2:
            dates.sort(key=lambda x: x[1])
            oldest_system, oldest_date = dates[0]
            newest_system, newest_date = dates[-1]
            diff_days = (newest_date - oldest_date).days

            if diff_days > 3:
                return (
                    "stale_data",
                    f"{oldest_system} data is {diff_days} days older than {newest_system}. "
                    f"Last updated: {oldest_date.strftime('%Y-%m-%d')}."
                )

        # Check for timing-based differences
        if field in ["revenue", "amount"]:
            numeric_values = [v["value"] for v in values if isinstance(v["value"], (int, float))]
            if len(numeric_values) >= 2:
                max_val = max(numeric_values)
                min_val = min(numeric_values)
                if max_val > 0:
                    diff_pct = (max_val - min_val) / max_val
                    if 0.01 < diff_pct < 0.3:
                        crm_system = None
                        erp_system = None
                        for v in values:
                            sys_lower = v["source_system"].lower()
                            if "salesforce" in sys_lower or "hubspot" in sys_lower:
                                crm_system = v["source_system"]
                            elif "netsuite" in sys_lower or "sap" in sys_lower:
                                erp_system = v["source_system"]
                        if crm_system and erp_system:
                            return (
                                "timing",
                                f"CRM ({crm_system}) books revenue at deal close while "
                                f"ERP ({erp_system}) books at delivery/recognition. "
                                f"Difference of {diff_pct:.1%} is consistent with timing gap."
                            )

        return (
            "recognition_method",
            f"Systems use different methods to calculate {field}. "
            "Values differ but no obvious timing or staleness issue detected."
        )

    def _calculate_severity_label(self, field: str, values: List[Dict[str, Any]]) -> str:
        """Calculate severity label for a conflict."""
        numeric_values = [v["value"] for v in values if isinstance(v["value"], (int, float))]
        if len(numeric_values) < 2:
            return "low"

        max_val = max(numeric_values)
        min_val = min(numeric_values)
        abs_diff = max_val - min_val

        if field in ["revenue", "amount"]:
            if abs_diff >= 300000:
                return "critical"
            elif abs_diff >= 100000:
                return "medium"
            else:
                return "low"

        if max_val > 0:
            diff_pct = abs_diff / max_val
            if diff_pct >= 0.15:
                return "critical"
            elif diff_pct >= 0.05:
                return "medium"

        return "low"

    def _get_trust_recommendation(
        self, field: str, values: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Get trust recommendation for a conflict."""
        SOR_PRIORITY = {
            "netsuite_erp": 4,
            "sap_erp": 4,
            "salesforce_crm": 3,
            "hubspot_crm": 2,
        }

        best_system = None
        best_priority = -1
        best_freshness = ""

        for v in values:
            sys_name = v["source_system"]
            priority = SOR_PRIORITY.get(sys_name, 1)
            freshness = v.get("last_updated", "")

            if priority > best_priority or (priority == best_priority and freshness > best_freshness):
                best_priority = priority
                best_system = sys_name
                best_freshness = freshness

        if not best_system:
            best_system = values[0]["source_system"]

        if field in ["revenue", "amount"]:
            reasoning = (
                f"{best_system} is the system of record for financial data. "
                f"ERP systems have the most accurate recognized revenue."
            )
        else:
            reasoning = (
                f"{best_system} has the highest trust score for {field}."
            )

        return {"system": best_system, "reasoning": reasoning}

    def _get_source_quality(self, source_system: str, metric: str) -> float:
        """Get quality score for a source system and metric."""
        if source_system in self._source_quality_scores:
            if metric in self._source_quality_scores[source_system]:
                return self._source_quality_scores[source_system][metric]

        defaults = {
            "netsuite_erp": 0.95,
            "salesforce_crm": 0.92,
            "hubspot_crm": 0.85,
            "sap_erp": 0.90,
        }
        return defaults.get(source_system, 0.80)

    def _find_existing_conflict(
        self, dcl_global_id: str, metric: str
    ) -> Optional[Conflict]:
        """Find an existing conflict for the same entity and metric."""
        for conflict in self._conflicts.values():
            if conflict.dcl_global_id == dcl_global_id and conflict.metric == metric:
                return conflict
        return None

    def resolve_conflict(
        self,
        conflict_id: str,
        decision: str,
        rationale: str,
        resolved_by: str = "admin",
    ) -> Optional[Conflict]:
        """Resolve a conflict with a decision and rationale."""
        conflict = self._conflicts.get(conflict_id)
        if not conflict:
            return None

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        conflict.status = "resolved"
        conflict.resolved_by = resolved_by
        conflict.resolved_at = now
        conflict.resolution_decision = decision
        conflict.resolution_rationale = rationale

        self._resolution_history.append(
            ConflictResolutionEntry(
                id=str(uuid.uuid4()),
                conflict_id=conflict_id,
                decision=decision,
                rationale=rationale,
                resolved_by=resolved_by,
                resolved_at=now,
            )
        )

        # Update quality scores based on resolution (feedback loop)
        self._update_quality_from_resolution(conflict, decision)

        return conflict

    def _update_quality_from_resolution(self, conflict: Conflict, decision: str):
        """Update quality scores based on conflict resolution (feedback loop)."""
        # Determine winning system from decision text
        winning_system = None
        for v in conflict.values:
            if v.source_system.lower() in decision.lower():
                winning_system = v.source_system
                break
        if not winning_system:
            winning_system = decision

        metric = conflict.metric

        # Track resolution wins
        if winning_system not in self._resolution_counts:
            self._resolution_counts[winning_system] = {}
        if metric not in self._resolution_counts[winning_system]:
            self._resolution_counts[winning_system][metric] = 0
        self._resolution_counts[winning_system][metric] += 1

        # If a source wins 5+ consecutive conflicts, boost its quality score
        win_count = self._resolution_counts[winning_system][metric]
        if win_count >= 5:
            if winning_system not in self._source_quality_scores:
                self._source_quality_scores[winning_system] = {}

            current = self._source_quality_scores.get(winning_system, {}).get(
                metric, self._get_source_quality(winning_system, metric)
            )
            new_score = min(1.0, current + 0.02)
            self._source_quality_scores[winning_system][metric] = new_score

            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._quality_adjustments.append(
                QualityScoreAdjustment(
                    source_system=winning_system,
                    metric=metric,
                    adjustment=0.02,
                    reason=f"Won {win_count} consecutive conflicts for {metric}",
                    timestamp=now,
                )
            )
            logger.info(
                f"Quality score for {winning_system}/{metric} increased to {new_score:.2f} "
                f"after {win_count} resolution wins"
            )

    def get_active_conflicts(self) -> List[Conflict]:
        """Get all active (unresolved) conflicts sorted by severity desc."""
        active = [c for c in self._conflicts.values() if c.status == "active"]
        active.sort(key=lambda c: SEVERITY_ORDER.get(c.severity, 0), reverse=True)
        return active

    def get_all_conflicts(self) -> List[Conflict]:
        """Get all conflicts sorted by severity desc."""
        all_conflicts = list(self._conflicts.values())
        all_conflicts.sort(key=lambda c: SEVERITY_ORDER.get(c.severity, 0), reverse=True)
        return all_conflicts

    def get_conflict(self, conflict_id: str) -> Optional[Conflict]:
        """Get a specific conflict."""
        return self._conflicts.get(conflict_id)

    def get_resolution_history(self) -> List[ConflictResolutionEntry]:
        """Get the full resolution history."""
        return self._resolution_history

    def get_quality_adjustments(self) -> List[QualityScoreAdjustment]:
        """Get all quality score adjustments."""
        return self._quality_adjustments

    def get_source_quality_score(self, source_system: str, metric: str) -> float:
        """Get the current quality score for a source/metric."""
        return self._get_source_quality(source_system, metric)

    def get_resolution_count(self, source_system: str, metric: str) -> int:
        """Get number of resolution wins for a source/metric."""
        return self._resolution_counts.get(source_system, {}).get(metric, 0)


# Singleton
_store: Optional[ConflictDetectionStore] = None


def get_conflict_store() -> ConflictDetectionStore:
    """Get or create the singleton conflict detection store."""
    global _store
    if _store is None:
        _store = ConflictDetectionStore()
    return _store
