"""
DCL Semantic Export API - Exposes semantic layer for NLQ consumption.

This module provides a single endpoint that NLQ can poll to get the full
semantic catalog including metrics, entities (dimensions), and bindings.

NLQ uses this data to:
- Resolve aliases ("AR" → "ar")
- Know which dimensions are valid for each metric
- Fail fast with helpful messages when metrics don't exist
"""

import logging
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from enum import Enum

logger = logging.getLogger(__name__)


class Pack(str, Enum):
    CFO = "cfo"
    CRO = "cro"
    COO = "coo"
    CTO = "cto"
    CHRO = "chro"


class TimeGrain(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class VersionHistoryEntry(BaseModel):
    """Version history entry for a metric definition."""
    version: int
    changed_by: str
    change_description: str
    changed_at: str
    previous_value: Optional[str] = None
    new_value: Optional[str] = None


class MetricDefinition(BaseModel):
    """Published metric definition for NLQ consumption."""
    id: str
    name: str
    description: str
    aliases: List[str] = Field(default_factory=list)
    pack: Pack
    allowed_dims: List[str] = Field(default_factory=list)
    allowed_grains: List[TimeGrain] = Field(default_factory=list)
    measure_op: Optional[str] = None
    default_grain: Optional[TimeGrain] = None
    best_direction: str = "high"
    rankable_dimensions: List[str] = Field(default_factory=list)
    version_history: Optional[List[VersionHistoryEntry]] = None


class EntityDefinition(BaseModel):
    """Entity (dimension) definition for NLQ consumption."""
    id: str
    name: str
    description: str
    aliases: List[str] = Field(default_factory=list)
    pack: Optional[Pack] = None
    allowed_values: List[str] = Field(default_factory=list)


class BindingSummary(BaseModel):
    """Source system binding summary."""
    source_system: str
    canonical_event: str
    quality_score: float = Field(ge=0.0, le=1.0)
    freshness_score: float = Field(ge=0.0, le=1.0)
    dims_coverage: Dict[str, bool] = Field(default_factory=dict)


class ModeInfo(BaseModel):
    """Current DCL mode information."""
    data_mode: str  # "Demo" or "Farm"
    run_mode: str   # "Dev" or "Prod"
    last_updated: Optional[str] = None


class SemanticExport(BaseModel):
    """Full semantic export payload for NLQ."""
    version: str = "1.0.0"
    tenant_id: str = "default"
    mode: ModeInfo
    metrics: List[MetricDefinition] = Field(default_factory=list)
    entities: List[EntityDefinition] = Field(default_factory=list)
    persona_concepts: Dict[str, List[str]] = Field(default_factory=dict)
    bindings: List[BindingSummary] = Field(default_factory=list)
    metric_entity_matrix: Dict[str, List[str]] = Field(default_factory=dict)


CONFIG_DIR = Path(__file__).parent.parent / "config" / "definitions"


def _load_metrics() -> List[MetricDefinition]:
    config_path = CONFIG_DIR / "metrics.yaml"
    if not config_path.exists():
        logger.warning(f"Metrics config not found: {config_path}")
        return []
    with open(config_path) as f:
        data = yaml.safe_load(f)
    metrics = []
    for m in data.get("metrics", []):
        metrics.append(MetricDefinition(
            id=m["id"],
            name=m["name"],
            description=m["description"],
            aliases=m.get("aliases", []),
            pack=Pack(m["pack"]),
            allowed_dims=m.get("allowed_dims", []),
            allowed_grains=[TimeGrain(g) for g in m.get("allowed_grains", [])],
            measure_op=m.get("measure_op"),
            default_grain=TimeGrain(m["default_grain"]) if m.get("default_grain") else None,
            best_direction=m.get("best_direction", "high"),
            rankable_dimensions=m.get("rankable_dimensions", []),
        ))
    return metrics


def _load_entities() -> List[EntityDefinition]:
    config_path = CONFIG_DIR / "entities.yaml"
    if not config_path.exists():
        logger.warning(f"Entities config not found: {config_path}")
        return []
    with open(config_path) as f:
        data = yaml.safe_load(f)
    entities = []
    for e in data.get("entities", []):
        entities.append(EntityDefinition(
            id=e["id"],
            name=e["name"],
            description=e["description"],
            aliases=e.get("aliases", []),
            pack=Pack(e["pack"]) if e.get("pack") else None,
            allowed_values=e.get("allowed_values", []),
        ))
    return entities


def _load_bindings() -> List[BindingSummary]:
    config_path = CONFIG_DIR / "bindings.yaml"
    if not config_path.exists():
        logger.warning(f"Bindings config not found: {config_path}")
        return []
    with open(config_path) as f:
        data = yaml.safe_load(f)
    bindings = []
    for b in data.get("bindings", []):
        bindings.append(BindingSummary(
            source_system=b["source_system"],
            canonical_event=b["canonical_event"],
            quality_score=b["quality_score"],
            freshness_score=b["freshness_score"],
            dims_coverage=b.get("dims_coverage", {}),
        ))
    return bindings


def _load_persona_concepts() -> Dict[str, List[str]]:
    config_path = CONFIG_DIR / "persona_concepts.yaml"
    if not config_path.exists():
        logger.warning(f"Persona concepts config not found: {config_path}")
        return {}
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return data.get("persona_concepts", {})


PUBLISHED_METRICS: List[MetricDefinition] = _load_metrics()
PUBLISHED_ENTITIES: List[EntityDefinition] = _load_entities()
DEMO_BINDINGS: List[BindingSummary] = _load_bindings()
DEFAULT_PERSONA_CONCEPTS: Dict[str, List[str]] = _load_persona_concepts()
FARM_BINDINGS: List[BindingSummary] = []


def build_metric_entity_matrix() -> Dict[str, List[str]]:
    """Build matrix of metric → valid dimensions."""
    return {m.id: m.allowed_dims for m in PUBLISHED_METRICS}


def get_bindings_for_mode(data_mode: str) -> List[BindingSummary]:
    """Get bindings appropriate for the current mode."""
    if data_mode == "Demo":
        return DEMO_BINDINGS
    else:
        return FARM_BINDINGS


def resolve_metric(query: str) -> Optional[MetricDefinition]:
    """Resolve a query string to a canonical metric."""
    query_lower = query.lower().strip()
    
    for metric in PUBLISHED_METRICS:
        if query_lower == metric.id:
            return metric
        if query_lower in [a.lower() for a in metric.aliases]:
            return metric
    
    return None


def resolve_entity(query: str) -> Optional[EntityDefinition]:
    """Resolve a query string to a canonical entity."""
    query_lower = query.lower().strip()
    
    for entity in PUBLISHED_ENTITIES:
        if query_lower == entity.id:
            return entity
        if query_lower in [a.lower() for a in entity.aliases]:
            return entity
    
    return None


def get_semantic_export(tenant_id: str = "default") -> SemanticExport:
    """Build the full semantic export payload reflecting current DCL mode."""
    from backend.core.mode_state import get_current_mode

    current_mode = get_current_mode()

    mode_info = ModeInfo(
        data_mode=current_mode.data_mode,
        run_mode=current_mode.run_mode,
        last_updated=current_mode.last_updated
    )

    bindings = get_bindings_for_mode(current_mode.data_mode)

    enriched_metrics = _enrich_metrics_with_version_history(PUBLISHED_METRICS)

    return SemanticExport(
        version="1.0.0",
        tenant_id=tenant_id,
        mode=mode_info,
        metrics=enriched_metrics,
        entities=PUBLISHED_ENTITIES,
        persona_concepts=DEFAULT_PERSONA_CONCEPTS,
        bindings=bindings,
        metric_entity_matrix=build_metric_entity_matrix()
    )


def _enrich_metrics_with_version_history(
    metrics: List[MetricDefinition],
) -> List[MetricDefinition]:
    """Add version history to each metric from the temporal versioning store."""
    try:
        from backend.engine.temporal_versioning import get_temporal_store
        store = get_temporal_store()

        enriched = []
        for metric in metrics:
            history = store.get_history(metric.id)
            if history:
                enriched_metric = metric.model_copy()
                enriched_metric.version_history = [
                    VersionHistoryEntry(
                        version=h.version,
                        changed_by=h.changed_by,
                        change_description=h.change_description,
                        changed_at=h.changed_at,
                        previous_value=h.previous_value,
                        new_value=h.new_value,
                    )
                    for h in history
                ]
                enriched.append(enriched_metric)
            else:
                enriched.append(metric)
        return enriched
    except Exception:
        return metrics
