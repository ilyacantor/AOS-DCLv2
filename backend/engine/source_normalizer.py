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
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from difflib import SequenceMatcher


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
    ALIAS_MAP = {
        "salesforce": "salesforce_crm",
        "sfdc": "salesforce_crm",
        "sf": "salesforce_crm",
        "sf_crm": "salesforce_crm",
        "dynamics": "dynamics_crm",
        "dynamics365": "dynamics_crm",
        "d365": "dynamics_crm",
        "msdyn": "dynamics_crm",
        "hubspot": "hubspot_crm",
        "hs": "hubspot_crm",
        "hs_crm": "hubspot_crm",
        "netsuite": "netsuite_erp",
        "ns": "netsuite_erp",
        "oracle_netsuite": "netsuite_erp",
        "sap": "sap_erp",
        "sap_s4": "sap_erp",
        "s4hana": "sap_erp",
        "oracle": "oracle_erp",
        "oracle_cloud": "oracle_erp",
        "oracle_fusion": "oracle_erp",
        "stripe": "stripe_billing",
        "chargebee": "chargebee_billing",
        "cb": "chargebee_billing",
        "zuora": "zuora_billing",
        "recurly": "recurly_billing",
        "paddle": "paddle_billing",
        "workday": "workday_hcm",
        "wd": "workday_hcm",
        "zendesk": "zendesk_support",
        "zd": "zendesk_support",
        "jira": "jira_engineering",
        "atlassian_jira": "jira_engineering",
        "datadog": "datadog_monitoring",
        "dd": "datadog_monitoring",
        "aws_cost": "aws_cost_explorer",
        "aws_cost_explorer": "aws_cost_explorer",
        "aws_cur": "aws_cost_explorer",
        "paypal": "paypal_payments",
        "braintree": "braintree_payments",
        "square": "square_payments",
        "adyen": "adyen_payments",
        "quickbooks": "quickbooks_accounting",
        "qb": "quickbooks_accounting",
        "intuit": "quickbooks_accounting",
        "xero": "xero_accounting",
        "freshbooks": "freshbooks_accounting",
        "fb_accounting": "freshbooks_accounting",
        "postgres": "internal_crm_postgres",
        "postgresql": "internal_crm_postgres",
        "mongodb": "mongodb_customer_db",
        "mongo": "mongodb_customer_db",
        "supabase": "supabase_app_db",
        "mysql": "mysql_orders_db",
        "snowflake": "dw_dim_customer",
        "bigquery": "bigquery_unified_customers",
        "bq": "bigquery_unified_customers",
        "redshift": "redshift_fact_orders",
        "databricks": "databricks_gold_accounts",
    }

    PATTERN_RULES = [
        (r"^sf[-_]?", "salesforce_crm"),
        (r"^sfdc[-_]?", "salesforce_crm"),
        (r"^salesforce[-_]?", "salesforce_crm"),
        (r"^dyn[-_]?", "dynamics_crm"),
        (r"^d365[-_]?", "dynamics_crm"),
        (r"^dynamics[-_]?", "dynamics_crm"),
        (r"^hs[-_]?", "hubspot_crm"),
        (r"^hubspot[-_]?", "hubspot_crm"),
        (r"^ns[-_]?", "netsuite_erp"),
        (r"^netsuite[-_]?", "netsuite_erp"),
        (r"^sap[-_]?", "sap_erp"),
        (r"^oracle[-_]?", "oracle_erp"),
        (r"^stripe[-_]?", "stripe_billing"),
        (r"^cb[-_]?", "chargebee_billing"),
        (r"^chargebee[-_]?", "chargebee_billing"),
        (r"^wd[-_]?", "workday_hcm"),
        (r"^workday[-_]?", "workday_hcm"),
        (r"^zendesk[-_]?", "zendesk_support"),
        (r"^zd[-_]?", "zendesk_support"),
        (r"^jira[-_]?", "jira_engineering"),
        (r"^datadog[-_]?", "datadog_monitoring"),
        (r"^dd[-_]?", "datadog_monitoring"),
        (r"^aws[-_]?cost[-_]?", "aws_cost_explorer"),
        (r"^qb[-_]?", "quickbooks_accounting"),
        (r"^quickbooks[-_]?", "quickbooks_accounting"),
        (r"^xero[-_]?", "xero_accounting"),
        (r"^pg[-_]?", "internal_crm_postgres"),
        (r"^postgres[-_]?", "internal_crm_postgres"),
        (r"^mongo[-_]?", "mongodb_customer_db"),
        (r"^snow[-_]?", "dw_dim_customer"),
        (r"^snowflake[-_]?", "dw_dim_customer"),
    ]

    CATEGORY_PATTERNS = {
        "crm": [r"crm", r"customer", r"sales", r"lead", r"contact"],
        "erp": [r"erp", r"enterprise", r"resource", r"planning"],
        "billing": [r"bill", r"subscription", r"recurring"],
        "payment": [r"pay", r"transaction", r"checkout", r"payments"],
        "payments": [r"pay", r"transaction", r"checkout", r"payment"],
        "accounting": [r"account", r"ledger", r"finance", r"book"],
        "warehouse": [r"warehouse", r"dw", r"analytics", r"bi"],
        "database": [r"db", r"database", r"sql"],
        "custom_db": [r"postgres", r"mysql", r"mongo", r"supabase"],
        "hr": [r"hcm", r"worker", r"employee", r"position", r"hr", r"workday"],
        "support": [r"support", r"ticket", r"helpdesk", r"zendesk"],
        "engineering": [r"jira", r"sprint", r"issue", r"agile"],
        "monitoring": [r"datadog", r"monitor", r"incident", r"slo", r"observability"],
        "cloud": [r"aws", r"cloud", r"cost_explorer", r"cur"],
    }

    # Circuit breaker: skip Farm API calls for this many seconds after a failure
    _CB_COOLDOWN = 120  # 2 minutes
    _cb_last_failure: float = 0.0  # class-level, shared across instances

    def __init__(self):
        self._registry_cache: Dict[str, CanonicalSource] = {}
        self._discovered_sources: Dict[str, CanonicalSource] = {}
        self._registry_loaded = False

    def load_registry(self, narration=None, run_id: Optional[str] = None) -> int:
        # Circuit breaker: if Farm API failed recently, skip the network call
        now = time.time()
        if SourceNormalizer._cb_last_failure > 0 and (now - SourceNormalizer._cb_last_failure) < SourceNormalizer._CB_COOLDOWN:
            if narration and run_id:
                narration.add_message(
                    run_id, "SourceNormalizer",
                    "Skipping registry load (Farm API circuit breaker open). Using built-in aliases."
                )
            self._registry_loaded = True  # mark loaded so normalize() doesn't retry
            return 0

        farm_url = os.getenv("FARM_API_URL", "https://autonomos.farm")
        registry_url = f"{farm_url}/api/sources/registry"

        try:
            with httpx.Client(timeout=5.0) as client:
                response = client.get(registry_url)
                response.raise_for_status()
                data = response.json()

                sources = data.get("sources", data) if isinstance(data, dict) else data

                for source_data in sources:
                    canonical = CanonicalSource(
                        source_id=source_data["sourceId"],
                        name=source_data["name"],
                        description=source_data.get("description", ""),
                        source_type=source_data.get("sourceType", "UNKNOWN"),
                        category=source_data.get("category", "unknown"),
                        vendor=source_data.get("vendor", "Unknown"),
                        connection_type=source_data.get("connectionType", "api"),
                        entities=source_data.get("entities", []),
                        trust_score=source_data.get("trustScore", 50),
                        data_quality_score=source_data.get("dataQualityScore", 50),
                        is_primary=source_data.get("isPrimary", False),
                        metadata=source_data.get("metadata", {}),
                        discovery_status=DiscoveryStatus.CANONICAL,
                    )
                    self._registry_cache[canonical.source_id] = canonical

                self._registry_loaded = True
                SourceNormalizer._cb_last_failure = 0.0  # reset circuit breaker on success

                if narration and run_id:
                    narration.add_message(
                        run_id, "SourceNormalizer",
                        f"Loaded {len(self._registry_cache)} canonical sources from registry"
                    )

                return len(self._registry_cache)

        except Exception as e:
            SourceNormalizer._cb_last_failure = now  # trip the circuit breaker
            self._registry_loaded = True  # don't retry on every normalize() call
            if narration and run_id:
                narration.add_message(
                    run_id, "SourceNormalizer",
                    f"Registry unavailable ({type(e).__name__}). Using built-in aliases."
                )
            return 0

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

        return self._create_discovered_source(raw_source, narration, run_id)

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
            trust_score=60,
            data_quality_score=60,
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
