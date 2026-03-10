"""
Engagement configuration loader.

Provides entity-agnostic access to engagement parameters. Every engine reads
entity names, file paths, and column keys from this config rather than
hardcoding "meridian" / "cascadia".

Default engagement: data/engagements/demo-001.json
Override via ENGAGEMENT_CONFIG env var or by calling load_engagement() with a path.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_DEFAULT_ENGAGEMENT = _DATA_DIR / "engagements" / "demo-001.json"


@dataclass(frozen=True)
class EntityConfig:
    """Configuration for one entity in an engagement."""
    id: str
    name: str
    short_name: str
    service_catalog: str = ""
    fact_base: str = ""


@dataclass(frozen=True)
class ColumnKeys:
    """Maps generic names (entity_a, entity_b) to actual column keys in data files."""
    entity_a: str
    entity_b: str
    adjustments: str = "adjustments"
    combined: str = "combined"


@dataclass(frozen=True)
class OverlapKeys:
    """Maps generic overlap field names to actual keys in entity_overlap.json."""
    entity_a_customers: str
    entity_b_customers: str
    entity_a_headcount: str
    entity_b_headcount: str
    entity_a_spend: str
    entity_b_spend: str
    entity_a_name: str
    entity_b_name: str
    entity_a_revenue: str
    entity_b_revenue: str
    overlap_pct_a: str
    overlap_pct_b: str


@dataclass(frozen=True)
class EngagementConfig:
    """Full engagement configuration."""
    engagement_id: str
    deal_name: str
    entity_a: EntityConfig
    entity_b: EntityConfig
    combining_data: str
    overlap_data: str
    customer_profiles: str
    adjustments: str
    column_keys: ColumnKeys
    overlap_keys: OverlapKeys
    synergy_targets: dict = field(default_factory=dict)

    def entity_by_id(self, entity_id: str) -> Optional[EntityConfig]:
        """Look up an entity by its id."""
        if entity_id == self.entity_a.id:
            return self.entity_a
        if entity_id == self.entity_b.id:
            return self.entity_b
        return None

    def entity_label(self, entity_id: str) -> str:
        """Return human-readable name for an entity id, or the id itself."""
        e = self.entity_by_id(entity_id)
        return e.name if e else entity_id

    def resolve_path(self, relative_path: str) -> Path:
        """Resolve a data path relative to the repo root."""
        return _DATA_DIR.parent / relative_path


# ── Singleton ──────────────────────────────────────────────────────────

_active: Optional[EngagementConfig] = None


def load_engagement(path: Optional[str] = None) -> EngagementConfig:
    """Load an engagement config from JSON.

    Args:
        path: Path to engagement JSON file. If None, uses ENGAGEMENT_CONFIG
              env var or falls back to data/engagements/demo-001.json.

    Returns:
        Frozen EngagementConfig dataclass.
    """
    global _active

    if path is None:
        path = os.environ.get("ENGAGEMENT_CONFIG")
    if path is None:
        config_path = _DEFAULT_ENGAGEMENT
    else:
        config_path = Path(path)
        if not config_path.is_absolute():
            config_path = _DATA_DIR.parent / config_path

    if not config_path.exists():
        raise FileNotFoundError(
            f"Engagement config not found at {config_path}. "
            f"Create one based on data/engagements/demo-001.json."
        )

    with open(config_path) as f:
        raw = json.load(f)

    col_keys = raw.get("combining_column_keys", {})
    ovl_keys = raw.get("overlap_keys", {})

    config = EngagementConfig(
        engagement_id=raw["engagement_id"],
        deal_name=raw.get("deal_name", "Untitled Engagement"),
        entity_a=EntityConfig(
            id=raw["entity_a"]["id"],
            name=raw["entity_a"]["name"],
            short_name=raw["entity_a"].get("short_name", raw["entity_a"]["id"][:1].upper()),
            service_catalog=raw["entity_a"].get("service_catalog", ""),
            fact_base=raw["entity_a"].get("fact_base", ""),
        ),
        entity_b=EntityConfig(
            id=raw["entity_b"]["id"],
            name=raw["entity_b"]["name"],
            short_name=raw["entity_b"].get("short_name", raw["entity_b"]["id"][:1].upper()),
            service_catalog=raw["entity_b"].get("service_catalog", ""),
            fact_base=raw["entity_b"].get("fact_base", ""),
        ),
        combining_data=raw.get("combining_data", "data/combining_statements.json"),
        overlap_data=raw.get("overlap_data", "data/entity_overlap.json"),
        customer_profiles=raw.get("customer_profiles", "data/customer_profiles.json"),
        adjustments=raw.get("adjustments", "data/ebitda_adjustments.json"),
        column_keys=ColumnKeys(
            entity_a=col_keys.get("entity_a", raw["entity_a"]["id"]),
            entity_b=col_keys.get("entity_b", raw["entity_b"]["id"]),
            adjustments=col_keys.get("adjustments", "adjustments"),
            combined=col_keys.get("combined", "combined"),
        ),
        overlap_keys=OverlapKeys(
            entity_a_customers=ovl_keys.get("entity_a_customers", f"{raw['entity_a']['id']}_customers"),
            entity_b_customers=ovl_keys.get("entity_b_customers", f"{raw['entity_b']['id']}_customers"),
            entity_a_headcount=ovl_keys.get("entity_a_headcount", f"{raw['entity_a']['id']}_headcount"),
            entity_b_headcount=ovl_keys.get("entity_b_headcount", f"{raw['entity_b']['id']}_headcount"),
            entity_a_spend=ovl_keys.get("entity_a_spend", f"{raw['entity_a']['id']}_spend_M"),
            entity_b_spend=ovl_keys.get("entity_b_spend", f"{raw['entity_b']['id']}_spend_M"),
            entity_a_name=ovl_keys.get("entity_a_name", f"{raw['entity_a']['id']}_name"),
            entity_b_name=ovl_keys.get("entity_b_name", f"{raw['entity_b']['id']}_name"),
            entity_a_revenue=ovl_keys.get("entity_a_revenue", f"{raw['entity_a']['id']}_revenue_M"),
            entity_b_revenue=ovl_keys.get("entity_b_revenue", f"{raw['entity_b']['id']}_revenue_M"),
            overlap_pct_a=ovl_keys.get("overlap_pct_a", f"overlap_pct_of_{raw['entity_a']['id']}"),
            overlap_pct_b=ovl_keys.get("overlap_pct_b", f"overlap_pct_of_{raw['entity_b']['id']}"),
        ),
        synergy_targets=raw.get("synergy_targets", {}),
    )

    _active = config
    logger.info(
        "[engagement_config] Loaded engagement '%s': %s (%s) + %s (%s)",
        config.engagement_id,
        config.entity_a.name, config.entity_a.id,
        config.entity_b.name, config.entity_b.id,
    )
    return config


def get_engagement() -> EngagementConfig:
    """Return the active engagement config, loading defaults if needed."""
    global _active
    if _active is None:
        _active = load_engagement()
    return _active


def reset_engagement() -> None:
    """Clear the cached engagement config (for testing)."""
    global _active
    _active = None
