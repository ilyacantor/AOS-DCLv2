"""
Engagement configuration loader.

Provides the active engagement config to all engines. Every engine reads
entity IDs, display names, and deal parameters from this config — never
from hardcoded strings.

The active engagement is loaded from data/engagements/demo-001.json.
Switching to a new entity pair = new engagement JSON file.  Zero code changes.
"""

import json
from dataclasses import dataclass
from pathlib import Path

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_ENGAGEMENTS_DIR = _DATA_DIR / "engagements"

# Active engagement file — change this to switch entity pairs
_ACTIVE_ENGAGEMENT_FILE = "demo-001.json"


@dataclass(frozen=True)
class EngagementEntity:
    """One side of the M&A engagement."""
    id: str              # e.g. "meridian"
    display_name: str    # e.g. "Meridian Partners"
    role: str            # "acquirer" or "target"
    business_model: str  # "consultancy", "bpm", "saas"
    source_systems: dict  # {"crm": "salesforce_crm", ...}


@dataclass(frozen=True)
class EngagementConfig:
    """Full engagement configuration."""
    engagement_id: str
    deal_name: str
    entity_a: EngagementEntity
    entity_b: EngagementEntity
    deal_parameters: dict
    synergy_targets: dict

    def entity_ids(self) -> tuple[str, str]:
        """Return (entity_a_id, entity_b_id)."""
        return self.entity_a.id, self.entity_b.id

    def entity_by_id(self, entity_id: str) -> EngagementEntity:
        """Look up an entity by ID.  Raises ValueError if not found."""
        if entity_id == self.entity_a.id:
            return self.entity_a
        if entity_id == self.entity_b.id:
            return self.entity_b
        raise ValueError(
            f"Entity '{entity_id}' not in engagement {self.engagement_id}. "
            f"Known entities: {self.entity_a.id}, {self.entity_b.id}"
        )

    def a_to_b_label(self) -> str:
        """Direction label: 'entity_a → entity_b'."""
        return f"{self.entity_a.display_name} → {self.entity_b.display_name}"

    def b_to_a_label(self) -> str:
        """Direction label: 'entity_b → entity_a'."""
        return f"{self.entity_b.display_name} → {self.entity_a.display_name}"


# Module-level singleton — loaded once, reused.
_cached_config: EngagementConfig | None = None


def _parse_entity(data: dict) -> EngagementEntity:
    """Parse an entity block from the engagement JSON."""
    return EngagementEntity(
        id=data["id"],
        display_name=data["display_name"],
        role=data["role"],
        business_model=data.get("business_model", "unknown"),
        source_systems=data.get("source_systems", {}),
    )


def get_active_engagement() -> EngagementConfig:
    """Load and return the active engagement config.

    Cached after first load.  Call invalidate_engagement() to force reload.

    Raises FileNotFoundError if the engagement file is missing.
    Raises KeyError/ValueError if the file is malformed.
    """
    global _cached_config
    if _cached_config is not None:
        return _cached_config

    path = _ENGAGEMENTS_DIR / _ACTIVE_ENGAGEMENT_FILE
    if not path.exists():
        raise FileNotFoundError(
            f"Active engagement config not found at {path}. "
            f"Create data/engagements/{_ACTIVE_ENGAGEMENT_FILE} with entity_a and entity_b definitions."
        )

    with open(path) as f:
        raw = json.load(f)

    config = EngagementConfig(
        engagement_id=raw["engagement_id"],
        deal_name=raw["deal_name"],
        entity_a=_parse_entity(raw["entity_a"]),
        entity_b=_parse_entity(raw["entity_b"]),
        deal_parameters=raw.get("deal_parameters", {}),
        synergy_targets=raw.get("synergy_targets", {}),
    )

    _cached_config = config
    logger.info(
        "[engagement] Loaded engagement %s: %s (%s) vs %s (%s)",
        config.engagement_id,
        config.entity_a.display_name,
        config.entity_a.id,
        config.entity_b.display_name,
        config.entity_b.id,
    )
    return config


def invalidate_engagement() -> None:
    """Force reload of engagement config on next access."""
    global _cached_config
    _cached_config = None
    logger.info("[engagement] Engagement config cache invalidated")
