"""
Source Normalizer Service

Normalizes raw source system identifiers to canonical sources using:
1. Exact alias matching
2. Pattern/prefix matching  
3. Fuzzy matching as fallback
4. Discovery mode for unrecognized sources
"""

import os
import re
import time
import httpx
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from difflib import SequenceMatcher

from backend.aam.ingress import normalize_source_id
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class DiscoveryStatus(str, Enum):
    CANONICAL = "canonical"
    PENDING_TRIAGE = "pending_triage"
    CUSTOM = "custom"
    REJECTED = "rejected"


class ResolutionType(str, Enum):
    EXACT = "exact"
    ALIAS = "alias"
    PATTERN = "pattern"
    FUZZY = "fuzzy"
    DISCOVERED = "discovered"
    REJECTED = "rejected"


@dataclass
class CanonicalSource:
    source_id: str
    name: str
    description: str
    source_type: str
    category: str
    vendor: str
    connection_type: str
    entities: List[str]
    trust_score: int
    data_quality_score: int
    is_primary: bool
    metadata: Dict[str, Any] = field(default_factory=dict)
    discovery_status: DiscoveryStatus = DiscoveryStatus.CANONICAL
    aliases: List[str] = field(default_factory=list)


@dataclass
class NormalizationResult:
    canonical_id: str
    raw_id: str
    canonical_source: CanonicalSource
    resolution_type: ResolutionType
    confidence: float
    match_details: Optional[str] = None


class SourceNormalizer:
    # --- Config loaded from config/source_aliases.yaml at init ---
    # Hardcoded fallbacks only used if YAML is missing or malformed.
    _YAML_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "source_aliases.yaml"

    def __init__(self):
        self._registry_cache: Dict[str, CanonicalSource] = {}
        self._discovered_sources: Dict[str, CanonicalSource] = {}
        self._registry_loaded = False

        try:
            with open(self._YAML_CONFIG_PATH, "r") as f:
                cfg = yaml.safe_load(f) or {}
            self.ALIAS_MAP: Dict[str, str] = cfg.get("alias_map", {})
            raw_patterns = cfg.get("pattern_rules", [])
            self.PATTERN_RULES: List[Tuple[str, str]] = [
                (r["pattern"], r["canonical_id"]) for r in raw_patterns
            ]
            self.CATEGORY_PATTERNS: Dict[str, List[str]] = cfg.get("category_patterns", {})
        except FileNotFoundError:
            logger.info(
                f"Source aliases config not found at {self._YAML_CONFIG_PATH} — using minimal defaults"
            )
            self.ALIAS_MAP = {"salesforce": "salesforce_crm", "netsuite": "netsuite_erp"}
            self.PATTERN_RULES = []
            self.CATEGORY_PATTERNS = {}
        except Exception as e:
            raise RuntimeError(
                f"Source aliases config at {self._YAML_CONFIG_PATH} exists but failed to load: {e}"
            ) from e

    def load_registry_from_pipe_store(self, narration=None, run_id: Optional[str] = None) -> int:
        """Populate registry cache from DCL's pipe_store (AOD → AAM → DCL chain).

        This is the authoritative registry source per RACI v6.
        """
        from backend.api.pipe_store import get_pipe_store
        pipe_store = get_pipe_store()
        definitions = pipe_store.get_all_definitions()

        if not definitions:
            return 0

        # Deduplicate by vendor — multiple pipes may share a source
        seen_sources: Dict[str, bool] = {}
        for defn in definitions:
            source_id = normalize_source_id(defn.vendor) if defn.vendor else ""
            if not source_id or source_id in seen_sources:
                continue
            seen_sources[source_id] = True

            canonical = CanonicalSource(
                source_id=source_id,
                name=defn.source_name or defn.vendor,
                description=f"Registered via AAM /export-pipes",
                source_type=defn.category.upper() if defn.category else "UNKNOWN",
                category=defn.category or "unknown",
                vendor=defn.vendor or "Unknown",
                connection_type="api",
                entities=[],
                trust_score=defn.trust_score,
                data_quality_score=defn.data_quality_score,
                is_primary=False,
                metadata={"pipe_store": True, "pipe_id": defn.pipe_id},
                discovery_status=DiscoveryStatus.CANONICAL,
            )
            self._registry_cache[canonical.source_id] = canonical

        self._registry_loaded = True

        if narration and run_id:
            narration.add_message(
                run_id, "SourceNormalizer",
                f"Loaded {len(self._registry_cache)} canonical sources from pipe_store"
            )

        return len(self._registry_cache)

    def load_registry(self, narration=None, run_id: Optional[str] = None) -> int:
        """Load registry from pipe_store. Hard fail if pipe_store is empty."""
        count = self.load_registry_from_pipe_store(narration, run_id)
        if count > 0:
            return count

        # pipe_store is empty — raise instead of falling back
        raise RuntimeError(
            "SourceNormalizer registry not loaded — pipe_store is empty. "
            "Call load_registry_from_pipe_store() after AAM /export-pipes completes."
        )

    def normalize(self, raw_source: str, narration=None, run_id: Optional[str] = None) -> NormalizationResult:
        if not self._registry_loaded:
            self.load_registry(narration, run_id)

        raw_lower = raw_source.lower().strip()

        result = self._try_exact_match(raw_lower, raw_source)
        if result:
            return result

        result = self._try_alias_match(raw_lower, raw_source)
        if result:
            return result

        result = self._try_pattern_match(raw_lower, raw_source)
        if result:
            return result

        result = self._try_fuzzy_match(raw_lower, raw_source)
        if result:
            return result

        return self._reject_unknown_source(raw_source, narration, run_id)

    def _try_exact_match(self, raw_lower: str, raw_source: str) -> Optional[NormalizationResult]:
        for canonical_id, canonical in self._registry_cache.items():
            if raw_lower == canonical_id.lower():
                return NormalizationResult(
                    canonical_id=canonical_id,
                    raw_id=raw_source,
                    canonical_source=canonical,
                    resolution_type=ResolutionType.EXACT,
                    confidence=1.0,
                    match_details=f"Exact match to {canonical_id}"
                )
        return None

    def _try_alias_match(self, raw_lower: str, raw_source: str) -> Optional[NormalizationResult]:
        if raw_lower in self.ALIAS_MAP:
            canonical_id = self.ALIAS_MAP[raw_lower]
            canonical = self._registry_cache.get(canonical_id)

            if canonical:
                return NormalizationResult(
                    canonical_id=canonical_id,
                    raw_id=raw_source,
                    canonical_source=canonical,
                    resolution_type=ResolutionType.ALIAS,
                    confidence=0.95,
                    match_details=f"Alias '{raw_lower}' maps to {canonical_id}"
                )
            else:
                canonical = self._create_fallback_canonical(canonical_id, raw_source)
                return NormalizationResult(
                    canonical_id=canonical_id,
                    raw_id=raw_source,
                    canonical_source=canonical,
                    resolution_type=ResolutionType.ALIAS,
                    confidence=0.90,
                    match_details=f"Alias match (registry entry not found)"
                )
        return None

    def _try_pattern_match(self, raw_lower: str, raw_source: str) -> Optional[NormalizationResult]:
        for pattern, canonical_id in self.PATTERN_RULES:
            if re.match(pattern, raw_lower, re.IGNORECASE):
                canonical = self._registry_cache.get(canonical_id)

                if canonical:
                    return NormalizationResult(
                        canonical_id=canonical_id,
                        raw_id=raw_source,
                        canonical_source=canonical,
                        resolution_type=ResolutionType.PATTERN,
                        confidence=0.85,
                        match_details=f"Pattern '{pattern}' matched to {canonical_id}"
                    )
                else:
                    canonical = self._create_fallback_canonical(canonical_id, raw_source)
                    return NormalizationResult(
                        canonical_id=canonical_id,
                        raw_id=raw_source,
                        canonical_source=canonical,
                        resolution_type=ResolutionType.PATTERN,
                        confidence=0.80,
                        match_details=f"Pattern match (registry entry not found)"
                    )
        return None

    def _try_fuzzy_match(self, raw_lower: str, raw_source: str) -> Optional[NormalizationResult]:
        best_match = None
        best_score = 0.0
        threshold = 0.7

        for canonical_id, canonical in self._registry_cache.items():
            candidates = [
                canonical_id.lower(),
                canonical.name.lower(),
                canonical.vendor.lower(),
            ]

            for candidate in candidates:
                score = SequenceMatcher(None, raw_lower, candidate).ratio()
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = (canonical_id, canonical)

        if best_match:
            canonical_id, canonical = best_match
            return NormalizationResult(
                canonical_id=canonical_id,
                raw_id=raw_source,
                canonical_source=canonical,
                resolution_type=ResolutionType.FUZZY,
                confidence=best_score * 0.9,
                match_details=f"Fuzzy match to {canonical_id} (score: {best_score:.2f})"
            )

        return None

    def _reject_unknown_source(
        self, raw_source: str, narration=None, run_id: Optional[str] = None
    ) -> NormalizationResult:
        """Reject an unrecognized source with an actionable error message.

        Per RACI v6: AOD owns SOR identification. DCL does not infer or
        auto-discover sources — it rejects them with guidance.
        """
        from backend.utils.log_utils import get_logger as _gl
        _gl(__name__).warning(
            f"[SourceNormalizer] REJECTED unknown source '{raw_source}'. "
            f"Register via AOD discovery, then run AAM /export-pipes."
        )

        if narration and run_id:
            narration.add_message(
                run_id, "SourceNormalizer",
                f"REJECTED unknown source: '{raw_source}' — not registered. "
                f"Register via AOD discovery, then run AAM /export-pipes to propagate to DCL."
            )

        rejected_canonical = CanonicalSource(
            source_id=f"rejected_{raw_source.lower().strip()}",
            name=raw_source,
            description=f"Rejected: not registered in AOD",
            source_type="REJECTED",
            category="unknown",
            vendor="Unknown",
            connection_type="unknown",
            entities=[],
            trust_score=0,
            data_quality_score=0,
            is_primary=False,
            metadata={"raw_identifier": raw_source, "rejected": True},
            discovery_status=DiscoveryStatus.REJECTED,
        )

        return NormalizationResult(
            canonical_id=rejected_canonical.source_id,
            raw_id=raw_source,
            canonical_source=rejected_canonical,
            resolution_type=ResolutionType.REJECTED,
            confidence=0.0,
            match_details=(
                f"Source '{raw_source}' is not registered. Register via AOD "
                f"discovery, then run AAM /export-pipes to propagate to DCL."
            ),
        )

    # DEPRECATED: _create_discovered_source() — retained for reference only.
    # AOD owns SOR identification per RACI v6. Remove by 2026-04-26.
    def _create_discovered_source(
        self, raw_source: str, narration=None, run_id: Optional[str] = None
    ) -> NormalizationResult:
        raw_lower = raw_source.lower().strip()
        safe_id = re.sub(r"[^a-z0-9_]", "_", raw_lower)
        discovered_id = f"discovered_{safe_id}"

        if discovered_id in self._discovered_sources:
            canonical = self._discovered_sources[discovered_id]
        else:
            category = self._infer_category(raw_lower)

            canonical = CanonicalSource(
                source_id=discovered_id,
                name=raw_source.replace("_", " ").title(),
                description=f"Auto-discovered source from raw identifier: {raw_source}",
                source_type="DISCOVERED",
                category=category,
                vendor="Unknown",
                connection_type="unknown",
                entities=[],
                trust_score=30,
                data_quality_score=30,
                is_primary=False,
                metadata={"raw_identifier": raw_source, "auto_discovered": True},
                discovery_status=DiscoveryStatus.PENDING_TRIAGE,
            )

            self._discovered_sources[discovered_id] = canonical

            if narration and run_id:
                narration.add_message(
                    run_id, "SourceNormalizer",
                    f"Discovered new source: '{raw_source}' -> {discovered_id} (pending triage)"
                )

        return NormalizationResult(
            canonical_id=discovered_id,
            raw_id=raw_source,
            canonical_source=canonical,
            resolution_type=ResolutionType.DISCOVERED,
            confidence=0.5,
            match_details=f"New source discovered, pending triage"
        )

    def _infer_category(self, raw_lower: str) -> str:
        for category, patterns in self.CATEGORY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, raw_lower, re.IGNORECASE):
                    return category
        return "unknown"

    def _create_fallback_canonical(self, canonical_id: str, raw_source: str) -> CanonicalSource:
        from backend.utils.log_utils import get_logger as _gl
        from backend.core.constants import TRUST_SCORE_FALLBACK
        _gl(__name__).warning(
            f"[SourceNormalizer] Creating fallback canonical for '{canonical_id}' "
            f"(raw='{raw_source}') — registry entry missing"
        )
        parts = canonical_id.split("_")
        vendor = parts[0].title() if parts else "Unknown"
        category = parts[-1] if len(parts) > 1 else "unknown"

        return CanonicalSource(
            source_id=canonical_id,
            name=canonical_id.replace("_", " ").title(),
            description=f"Fallback entry for {canonical_id}",
            source_type="FALLBACK",
            category=category,
            vendor=vendor,
            connection_type="api",
            entities=[],
            trust_score=TRUST_SCORE_FALLBACK,
            data_quality_score=TRUST_SCORE_FALLBACK,
            is_primary=False,
            metadata={"fallback": True, "raw_identifier": raw_source},
            discovery_status=DiscoveryStatus.CANONICAL,
        )

    def get_all_sources(self) -> Dict[str, CanonicalSource]:
        return {**self._registry_cache, **self._discovered_sources}

    def get_discovered_sources(self) -> Dict[str, CanonicalSource]:
        return self._discovered_sources.copy()

    def get_registry_sources(self) -> Dict[str, CanonicalSource]:
        return self._registry_cache.copy()

    def get_stats(self) -> Dict[str, int]:
        return {
            "registry_sources": len(self._registry_cache),
            "discovered_sources": len(self._discovered_sources),
            "total_sources": len(self._registry_cache) + len(self._discovered_sources),
        }


_normalizer_instance: Optional[SourceNormalizer] = None


def get_normalizer() -> SourceNormalizer:
    global _normalizer_instance
    if _normalizer_instance is None:
        _normalizer_instance = SourceNormalizer()
    return _normalizer_instance
