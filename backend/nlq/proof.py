"""
Enhanced Proof Resolution Service for NLQ Semantic Layer.

Provides:
- Resolution of proof hooks to clickable source system URLs
- Proof verification (checking links are valid)
- Proof chain construction from definition to source records
- Multi-source proof aggregation
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
import json
import re
from urllib.parse import urlencode, quote

from backend.utils.log_utils import get_logger
from backend.nlq.persistence import NLQPersistence
from backend.nlq.models import ProofHook, DefinitionVersion
from backend.nlq.lineage import LineageService

logger = get_logger(__name__)


class ProofType(Enum):
    """Types of proof pointers."""
    QUERY_HASH = "query_hash"
    SOURCE_POINTER = "source_pointer"
    EVENT_TRACE = "event_trace"
    SAVED_SEARCH = "saved_search"
    REPORT = "report"
    DASHBOARD = "dashboard"
    RECORD = "record"
    AUDIT_LOG = "audit_log"


class ProofStatus(Enum):
    """Status of a proof link."""
    VALID = "valid"
    INVALID = "invalid"
    UNKNOWN = "unknown"
    EXPIRED = "expired"


@dataclass
class ResolvedProof:
    """A resolved proof pointer with URL."""
    proof_type: ProofType
    system: str
    reference: str
    url: Optional[str]
    status: ProofStatus
    availability_score: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.proof_type.value,
            "system": self.system,
            "ref": self.reference,
            "url": self.url,
            "status": self.status.value,
            "availability_score": self.availability_score,
            "metadata": self.metadata,
        }


@dataclass
class ProofChain:
    """A chain of proofs from definition to source records."""
    definition_id: str
    version: str
    query_hash: str
    source_proofs: List[ResolvedProof] = field(default_factory=list)
    event_traces: List[Dict[str, Any]] = field(default_factory=list)
    total_availability: float = 0.0
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "definition_id": self.definition_id,
            "version": self.version,
            "query_hash": self.query_hash,
            "source_proofs": [p.to_dict() for p in self.source_proofs],
            "event_traces": self.event_traces,
            "total_availability": self.total_availability,
            "created_at": self.created_at,
        }


class SourceSystemConfig:
    """Configuration for source system URL generation."""

    # Base URL patterns by system
    BASE_URLS = {
        "NetSuite": "https://{account_id}.app.netsuite.com",
        "Salesforce": "https://{org_id}.lightning.force.com",
        "Chargebee": "https://{site}.chargebee.com",
        "Jira": "https://{domain}.atlassian.net",
        "GitHub": "https://github.com",
        "PagerDuty": "https://{subdomain}.pagerduty.com",
        "AWS": "https://{region}.console.aws.amazon.com",
        "Zendesk": "https://{subdomain}.zendesk.com",
        "Snyk": "https://app.snyk.io",
        "Expensify": "https://www.expensify.com",
        "Snowflake": "https://app.snowflake.com/{account}",
    }

    # URL templates by system and proof type
    URL_TEMPLATES = {
        "NetSuite": {
            "saved_search": "/app/common/search/searchresults.nl?searchid={search_id}",
            "record": "/app/common/entity/entity.nl?id={record_id}",
            "report": "/app/reporting/reportrunner.nl?reportid={report_id}",
            "transaction": "/app/accounting/transactions/transaction.nl?id={transaction_id}",
        },
        "Salesforce": {
            "report": "/lightning/r/Report/{report_id}/view",
            "record": "/lightning/r/{object_type}/{record_id}/view",
            "dashboard": "/lightning/r/Dashboard/{dashboard_id}/view",
            "list_view": "/lightning/o/{object_type}/list?filterName={filter_name}",
        },
        "Chargebee": {
            "subscription": "/subscriptions/{subscription_id}",
            "customer": "/customers/{customer_id}",
            "invoice": "/invoices/{invoice_id}",
        },
        "Jira": {
            "issue": "/browse/{issue_key}",
            "board": "/jira/software/c/projects/{project_key}/boards/{board_id}",
            "filter": "/issues/?filter={filter_id}",
        },
        "GitHub": {
            "repo": "/{owner}/{repo}",
            "commit": "/{owner}/{repo}/commit/{sha}",
            "deployment": "/{owner}/{repo}/deployments/{deployment_id}",
            "actions_run": "/{owner}/{repo}/actions/runs/{run_id}",
        },
        "PagerDuty": {
            "incident": "/incidents/{incident_id}",
            "service": "/services/{service_id}",
        },
        "AWS": {
            "cost_explorer": "/cost-management/home#/cost-explorer",
            "resource": "/{service}/home?region={region}#/{resource_type}/{resource_id}",
        },
        "Zendesk": {
            "ticket": "/agent/tickets/{ticket_id}",
            "view": "/agent/filters/{view_id}",
        },
        "Snyk": {
            "project": "/org/{org}/project/{project_id}",
            "issue": "/org/{org}/project/{project_id}#issue-{issue_id}",
        },
        "Snowflake": {
            "query": "/#/query?query_id={query_id}",
            "worksheet": "/#/worksheet/{worksheet_id}",
            "table": "/#/data/databases/{database}/schemas/{schema}/table/{table}",
        },
    }


class ProofResolver:
    """
    Enhanced service for resolving proof hooks to clickable URLs.

    Features:
    - Multi-system URL generation
    - Proof chain construction
    - Availability scoring
    - Verification (mock for now)
    """

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize proof resolver.

        Args:
            persistence: NLQPersistence instance
        """
        self.persistence = persistence or NLQPersistence()
        self.lineage = LineageService(self.persistence)

        # System configurations (would come from tenant config in production)
        self.system_configs: Dict[str, Dict[str, str]] = {
            "NetSuite": {"account_id": "123456"},
            "Salesforce": {"org_id": "na1"},
            "Chargebee": {"site": "acme"},
            "Jira": {"domain": "acme"},
            "GitHub": {},
            "PagerDuty": {"subdomain": "acme"},
            "AWS": {"region": "us-east-1"},
            "Zendesk": {"subdomain": "acme"},
            "Snyk": {},
            "Snowflake": {"account": "acme"},
        }

    def resolve_proof_hook(
        self,
        hook: ProofHook,
        context: Dict[str, Any]
    ) -> ResolvedProof:
        """
        Resolve a proof hook to a clickable URL.

        Args:
            hook: The proof hook to resolve
            context: Context values for template substitution

        Returns:
            ResolvedProof with URL and metadata
        """
        template = hook.pointer_template_json
        system = template.get("system", "Unknown")
        proof_type_str = template.get("type", "record")
        ref_template = template.get("ref_template", "")

        # Resolve reference
        reference = self._substitute_template(ref_template, context)

        # Generate URL
        url = self._generate_url(system, proof_type_str, context)

        # Determine proof type
        try:
            proof_type = ProofType(proof_type_str)
        except ValueError:
            proof_type = ProofType.SOURCE_POINTER

        return ResolvedProof(
            proof_type=proof_type,
            system=system,
            reference=reference,
            url=url,
            status=ProofStatus.UNKNOWN,  # Would verify in production
            availability_score=hook.availability_score,
            metadata={
                "hook_id": hook.id,
                "definition_id": hook.definition_id,
            },
        )

    def resolve_definition_proofs(
        self,
        definition_id: str,
        version: str = "v1",
        context: Optional[Dict[str, Any]] = None,
        tenant_id: str = "default"
    ) -> List[ResolvedProof]:
        """
        Resolve all proof hooks for a definition.

        Args:
            definition_id: Definition ID
            version: Version
            context: Context for template substitution
            tenant_id: Tenant ID

        Returns:
            List of resolved proofs
        """
        context = context or {}
        hooks = self.persistence.get_proof_hooks_for_definition(definition_id, tenant_id)

        proofs = []
        for hook in hooks:
            resolved = self.resolve_proof_hook(hook, context)
            proofs.append(resolved)

        return proofs

    def build_proof_chain(
        self,
        definition_id: str,
        version: str = "v1",
        sql: Optional[str] = None,
        params: Optional[List[Any]] = None,
        tenant_id: str = "default"
    ) -> ProofChain:
        """
        Build a complete proof chain from definition to sources.

        Args:
            definition_id: Definition ID
            version: Version
            sql: Compiled SQL query
            params: Query parameters
            tenant_id: Tenant ID

        Returns:
            ProofChain with all proofs
        """
        # Generate query hash
        query_hash = ""
        if sql:
            content = json.dumps({"sql": sql, "params": [str(p) for p in (params or [])]}, sort_keys=True)
            query_hash = f"sha256:{hashlib.sha256(content.encode()).hexdigest()[:16]}"

        # Get definition lineage
        lineage = self.lineage.get_definition_lineage(definition_id, tenant_id)

        # Resolve proof hooks
        source_proofs = self.resolve_definition_proofs(
            definition_id, version, {"definition_id": definition_id}, tenant_id
        )

        # Build event traces
        event_traces = []
        def_version = self.persistence.get_definition_version(definition_id, version, tenant_id)
        if def_version and def_version.spec:
            for event_id in def_version.spec.required_events:
                bindings = self.persistence.get_bindings_for_event(event_id, tenant_id)
                for binding in bindings:
                    event_traces.append({
                        "event": event_id,
                        "source_system": binding.source_system,
                        "binding_id": binding.id,
                        "quality_score": binding.quality_score,
                        "freshness_score": binding.freshness_score,
                    })

        # Calculate total availability
        total_availability = 0.0
        if source_proofs:
            total_availability = sum(p.availability_score for p in source_proofs) / len(source_proofs)

        return ProofChain(
            definition_id=definition_id,
            version=version,
            query_hash=query_hash,
            source_proofs=source_proofs,
            event_traces=event_traces,
            total_availability=total_availability,
        )

    def generate_query_proof(
        self,
        sql: str,
        params: List[Any],
        definition_id: Optional[str] = None
    ) -> ResolvedProof:
        """
        Generate a query hash proof.

        Args:
            sql: SQL query
            params: Query parameters
            definition_id: Optional definition ID

        Returns:
            ResolvedProof with query hash
        """
        content = json.dumps({"sql": sql, "params": [str(p) for p in params]}, sort_keys=True)
        query_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

        return ResolvedProof(
            proof_type=ProofType.QUERY_HASH,
            system="internal",
            reference=f"sha256:{query_hash}",
            url=None,
            status=ProofStatus.VALID,
            availability_score=1.0,
            metadata={
                "definition_id": definition_id,
                "sql_length": len(sql),
                "param_count": len(params),
            },
        )

    def generate_source_proof(
        self,
        system: str,
        proof_type: str,
        identifiers: Dict[str, str]
    ) -> ResolvedProof:
        """
        Generate a source system proof.

        Args:
            system: Source system name
            proof_type: Type of proof (record, report, etc.)
            identifiers: Identifiers for URL generation

        Returns:
            ResolvedProof with source URL
        """
        url = self._generate_url(system, proof_type, identifiers)

        try:
            ptype = ProofType(proof_type)
        except ValueError:
            ptype = ProofType.SOURCE_POINTER

        return ResolvedProof(
            proof_type=ptype,
            system=system,
            reference=json.dumps(identifiers),
            url=url,
            status=ProofStatus.UNKNOWN,
            availability_score=0.9,
            metadata=identifiers,
        )

    def _generate_url(
        self,
        system: str,
        proof_type: str,
        identifiers: Dict[str, Any]
    ) -> Optional[str]:
        """Generate URL for a source system proof."""
        if system not in SourceSystemConfig.BASE_URLS:
            return None

        base_url_template = SourceSystemConfig.BASE_URLS[system]
        system_config = self.system_configs.get(system, {})

        # Substitute system config into base URL
        base_url = self._substitute_template(base_url_template, system_config)

        # Get path template
        system_templates = SourceSystemConfig.URL_TEMPLATES.get(system, {})
        path_template = system_templates.get(proof_type, "")

        if not path_template:
            return base_url

        # Substitute identifiers into path
        path = self._substitute_template(path_template, identifiers)

        return f"{base_url}{path}"

    def _substitute_template(self, template: str, values: Dict[str, Any]) -> str:
        """Substitute values into a template string."""
        result = template
        for key, value in values.items():
            result = result.replace(f"{{{key}}}", str(value))
        return result

    def verify_proof(self, proof: ResolvedProof) -> ProofStatus:
        """
        Verify a proof link is valid.

        In production, this would make HTTP HEAD requests to verify URLs.
        For now, returns UNKNOWN.

        Args:
            proof: Proof to verify

        Returns:
            ProofStatus
        """
        # Mock verification
        if proof.url is None:
            return ProofStatus.INVALID

        # Would verify URL in production
        return ProofStatus.UNKNOWN

    def aggregate_proofs(
        self,
        definition_ids: List[str],
        tenant_id: str = "default"
    ) -> Dict[str, Any]:
        """
        Aggregate proofs across multiple definitions.

        Args:
            definition_ids: List of definition IDs
            tenant_id: Tenant ID

        Returns:
            Aggregated proof summary
        """
        all_systems: Dict[str, int] = {}
        all_proof_types: Dict[str, int] = {}
        total_availability = 0.0
        proof_count = 0

        for def_id in definition_ids:
            proofs = self.resolve_definition_proofs(def_id, "v1", {}, tenant_id)
            for proof in proofs:
                all_systems[proof.system] = all_systems.get(proof.system, 0) + 1
                all_proof_types[proof.proof_type.value] = all_proof_types.get(proof.proof_type.value, 0) + 1
                total_availability += proof.availability_score
                proof_count += 1

        return {
            "definition_count": len(definition_ids),
            "total_proofs": proof_count,
            "systems": all_systems,
            "proof_types": all_proof_types,
            "average_availability": total_availability / proof_count if proof_count > 0 else 0.0,
        }

    def get_proof_coverage(self, tenant_id: str = "default") -> Dict[str, Any]:
        """
        Get proof coverage statistics for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            Coverage statistics
        """
        definitions = self.persistence.get_definitions(tenant_id)

        with_proofs = 0
        without_proofs = 0
        by_system: Dict[str, int] = {}

        for defn in definitions:
            hooks = self.persistence.get_proof_hooks_for_definition(defn.id, tenant_id)
            if hooks:
                with_proofs += 1
                for hook in hooks:
                    system = hook.pointer_template_json.get("system", "unknown")
                    by_system[system] = by_system.get(system, 0) + 1
            else:
                without_proofs += 1

        total = with_proofs + without_proofs
        coverage_pct = (with_proofs / total * 100) if total > 0 else 0

        return {
            "total_definitions": total,
            "with_proofs": with_proofs,
            "without_proofs": without_proofs,
            "coverage_percentage": coverage_pct,
            "proofs_by_system": by_system,
        }
