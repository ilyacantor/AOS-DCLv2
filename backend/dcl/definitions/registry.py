"""
DCL Definition Registry - Centralized definition management with metadata.

Provides:
- ranked_list: Whether definition supports ranked/ordered results
- supports_limit: Whether limit can be applied
- default_limit: Default limit when none specified
- Definition matching and lookup
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum


class DefinitionKind(str, Enum):
    RANKED_LIST = "ranked_list"  # Supports ordering and limiting (e.g., top customers)
    AGGREGATE = "aggregate"      # Single-row aggregations (e.g., total ARR)
    TIMESERIES = "timeseries"    # Time-based data (e.g., MoM trends)
    DIMENSION = "dimension"      # Dimension/entity lookup


@dataclass
class DefinitionMetadata:
    """Extended metadata for a definition."""
    definition_id: str
    kind: DefinitionKind = DefinitionKind.RANKED_LIST
    supports_limit: bool = True
    default_limit: Optional[int] = None  # None means no default limit
    ranked_list: bool = True  # True if results are meaningfully ordered
    primary_metric: Optional[str] = None  # e.g., "revenue", "cost"
    entity_type: Optional[str] = None  # e.g., "customer", "vendor"
    supports_delta: bool = False  # Supports MoM/QoQ/YoY
    supports_trend: bool = False  # Supports time-series trending


# Definition metadata registry
# This supplements the BLL definitions with DCL-specific metadata
_DEFINITION_METADATA: Dict[str, DefinitionMetadata] = {
    "crm.top_customers": DefinitionMetadata(
        definition_id="crm.top_customers",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=10,  # Default to top 10 if not specified
        ranked_list=True,
        primary_metric="revenue",
        entity_type="customer",
    ),
    "crm.pipeline": DefinitionMetadata(
        definition_id="crm.pipeline",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="amount",
        entity_type="deal",
    ),
    "finops.arr": DefinitionMetadata(
        definition_id="finops.arr",
        kind=DefinitionKind.AGGREGATE,  # ARR is an aggregate metric, not a ranked list
        supports_limit=False,  # Don't apply limit for aggregate queries
        default_limit=None,  # No default limit - show aggregate total
        ranked_list=False,  # Not a ranked list
        primary_metric="revenue",
        entity_type="deal",
    ),
    "finops.saas_spend": DefinitionMetadata(
        definition_id="finops.saas_spend",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=10,
        ranked_list=True,
        primary_metric="cost",
        entity_type="vendor",
    ),
    "finops.top_vendor_deltas_mom": DefinitionMetadata(
        definition_id="finops.top_vendor_deltas_mom",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=10,
        ranked_list=True,
        primary_metric="cost",
        entity_type="vendor",
        supports_delta=True,
    ),
    "finops.unallocated_spend": DefinitionMetadata(
        definition_id="finops.unallocated_spend",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="cost",
        entity_type="resource",
    ),
    "finops.burn_rate": DefinitionMetadata(
        definition_id="finops.burn_rate",
        kind=DefinitionKind.AGGREGATE,
        supports_limit=False,
        ranked_list=False,
        primary_metric="cost",
    ),
    "aod.findings_by_severity": DefinitionMetadata(
        definition_id="aod.findings_by_severity",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="count",
        entity_type="finding",
    ),
    "aod.identity_gap_financially_anchored": DefinitionMetadata(
        definition_id="aod.identity_gap_financially_anchored",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="cost",
        entity_type="resource",
    ),
    "aod.zombies_overview": DefinitionMetadata(
        definition_id="aod.zombies_overview",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="cost",
        entity_type="resource",
    ),
    "infra.slo_attainment": DefinitionMetadata(
        definition_id="infra.slo_attainment",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="attainment",
        entity_type="service",
    ),
    "infra.deploy_frequency": DefinitionMetadata(
        definition_id="infra.deploy_frequency",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="count",
        entity_type="service",
    ),
    "infra.lead_time": DefinitionMetadata(
        definition_id="infra.lead_time",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="time",
        entity_type="service",
    ),
    "infra.change_failure_rate": DefinitionMetadata(
        definition_id="infra.change_failure_rate",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="rate",
        entity_type="service",
    ),
    "infra.mttr": DefinitionMetadata(
        definition_id="infra.mttr",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="time",
        entity_type="incident",
    ),
    "infra.incidents": DefinitionMetadata(
        definition_id="infra.incidents",
        kind=DefinitionKind.RANKED_LIST,
        supports_limit=True,
        default_limit=20,
        ranked_list=True,
        primary_metric="count",
        entity_type="incident",
    ),
}


class DefinitionRegistry:
    """Registry for definition metadata."""

    @staticmethod
    def get_metadata(definition_id: str) -> Optional[DefinitionMetadata]:
        """Get metadata for a definition."""
        return _DEFINITION_METADATA.get(definition_id)

    @staticmethod
    def is_ranked_list(definition_id: str) -> bool:
        """Check if definition is a ranked list type."""
        meta = _DEFINITION_METADATA.get(definition_id)
        return meta.ranked_list if meta else True  # Default to True

    @staticmethod
    def get_default_limit(definition_id: str) -> Optional[int]:
        """Get default limit for a definition."""
        meta = _DEFINITION_METADATA.get(definition_id)
        return meta.default_limit if meta else None

    @staticmethod
    def supports_limit(definition_id: str) -> bool:
        """Check if definition supports limiting."""
        meta = _DEFINITION_METADATA.get(definition_id)
        return meta.supports_limit if meta else True

    @staticmethod
    def list_definitions() -> List[str]:
        """List all known definition IDs."""
        return list(_DEFINITION_METADATA.keys())
