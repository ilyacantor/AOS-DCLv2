"""
Ontology concept definitions.

Loads from config/ontology_concepts.yaml (source of truth).
Falls back to a minimal hardcoded list only if the YAML is missing.
"""
import os
from pathlib import Path
from typing import List, Dict, Optional

from backend.domain import OntologyConcept
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_YAML_PATH = Path(__file__).parent.parent.parent / "config" / "ontology_concepts.yaml"

# Fallback floor used only when the YAML file is missing. The authoritative
# domain list is the top-level `domains:` block in
# `config/ontology_concepts.yaml`, loaded by `_load_valid_domains_from_yaml()`
# at import time. Treat _FALLBACK_DOMAINS as a last-resort safety net, not as
# the source of truth.
_FALLBACK_DOMAINS: frozenset[str] = frozenset({
    "finance", "sales", "hr", "customer_success",
    "product_eng", "it_infra", "operations", "marketing", "compliance",
    "cofa", "cloud_spend",
})


def _load_valid_domains_from_yaml() -> frozenset[str]:
    """Read the top-level `domains:` block from the ontology YAML.

    The YAML is the single source of truth for domains. If the file is missing
    or the `domains:` block is empty/absent, fall back to `_FALLBACK_DOMAINS`
    and log a warning — failing here would break every downstream import.
    """
    if not _YAML_PATH.exists():
        logger.warning(
            f"[Ontology] YAML not found at {_YAML_PATH}; using fallback domain set"
        )
        return _FALLBACK_DOMAINS
    try:
        import yaml as _yaml
        with open(_YAML_PATH) as _f:
            _data = _yaml.safe_load(_f) or {}
        declared = _data.get("domains")
        if not declared or not isinstance(declared, list):
            logger.warning(
                f"[Ontology] `domains:` block missing or empty in "
                f"{_YAML_PATH.name}; using fallback domain set"
            )
            return _FALLBACK_DOMAINS
        return frozenset(str(d) for d in declared if d)
    except Exception as _exc:
        logger.error(
            f"[Ontology] Failed to read `domains:` from {_YAML_PATH.name}: "
            f"{_exc}; using fallback domain set",
            exc_info=True,
        )
        return _FALLBACK_DOMAINS


VALID_DOMAINS: frozenset[str] = _load_valid_domains_from_yaml()

# Minimal fallback — only used if YAML file is missing
_FALLBACK_ONTOLOGY: List[OntologyConcept] = [
    OntologyConcept(id="account", name="Account", description="Business account or customer entity",
                    example_fields=["account_id", "account_name", "company", "customer_id"], expected_type="string"),
    OntologyConcept(id="revenue", name="Revenue", description="Revenue or monetary amount",
                    example_fields=["amount", "revenue", "total", "value", "price"], expected_type="float"),
    OntologyConcept(id="date", name="Date/Timestamp", description="Date or timestamp field",
                    example_fields=["date", "timestamp", "created_at", "updated_at"], expected_type="datetime"),
]

_cached_ontology: Optional[List[OntologyConcept]] = None

# ── Demo ontology: exact copy of deployed AOS-DCLv2 CORE_ONTOLOGY (10 concepts) ──
_DEMO_ONTOLOGY: List[OntologyConcept] = [
    OntologyConcept(
        id="account", name="Account",
        description="Business account or customer entity",
        example_fields=[
            "account_id", "accountId", "account_name", "accountName",
            "company", "customer_id", "customerId", "customer_name", "customerName",
            "client_id", "clientId", "vendor_id", "vendorId",
        ],
        expected_type="string",
    ),
    OntologyConcept(
        id="opportunity", name="Opportunity",
        description="Sales opportunity or deal",
        example_fields=[
            "opportunity_id", "opportunityId", "deal_id", "dealId",
            "opportunity_name", "opportunityName", "pipeline", "stage",
        ],
        expected_type="string",
    ),
    OntologyConcept(
        id="revenue", name="Revenue",
        description="Revenue or monetary amount",
        example_fields=[
            "amount", "revenue", "total", "value", "price",
            "totalAmount", "total_amount", "lineTotal", "line_total",
            "invoiceAmount", "invoice_amount", "grossAmount", "netAmount",
            "unitPrice", "unit_price", "subtotal",
        ],
        expected_type="float",
    ),
    OntologyConcept(
        id="cost", name="Cost",
        description="Cost or expense amount",
        example_fields=[
            "cost", "spend", "expense", "fee", "charge",
            "taxAmount", "tax_amount", "discount", "shipping",
        ],
        expected_type="float",
    ),
    OntologyConcept(
        id="invoice", name="Invoice",
        description="Invoice or billing document",
        example_fields=[
            "invoice_id", "invoiceId", "invoice_number", "invoiceNumber",
            "invoice_no", "invoiceNo", "bill_id", "billId", "receipt_id",
        ],
        expected_type="string",
    ),
    OntologyConcept(
        id="currency", name="Currency",
        description="Currency code or monetary unit",
        example_fields=[
            "currency", "currency_code", "currencyCode", "currency_id",
        ],
        expected_type="string",
    ),
    OntologyConcept(
        id="aws_resource", name="AWS Resource",
        description="AWS cloud resource",
        example_fields=[
            "resource_id", "resourceId", "instance_id", "instanceId",
            "resource_type", "resourceType", "arn",
        ],
        expected_type="string",
    ),
    OntologyConcept(
        id="health", name="Health Score",
        description="Account health or status metric",
        example_fields=[
            "health_score", "healthScore", "status", "health", "state",
        ],
        expected_type="float",
    ),
    OntologyConcept(
        id="usage", name="Usage Metrics",
        description="Usage or consumption metrics",
        example_fields=[
            "usage", "consumption", "utilization", "quantity",
        ],
        expected_type="float",
    ),
    OntologyConcept(
        id="date", name="Date/Timestamp",
        description="Date or timestamp field",
        example_fields=[
            "date", "timestamp", "created_at", "createdAt", "updated_at", "updatedAt",
            "due_date", "dueDate", "paid_date", "paidDate", "invoice_date", "invoiceDate",
            "start_date", "startDate", "end_date", "endDate",
        ],
        expected_type="datetime",
    ),
]


def get_demo_ontology() -> List[OntologyConcept]:
    """Return the 10-concept ontology matching the deployed AOS-DCLv2."""
    return list(_DEMO_ONTOLOGY)


def _load_from_yaml() -> List[OntologyConcept]:
    """Parse config/ontology_concepts.yaml into OntologyConcept objects."""
    import yaml

    with open(_YAML_PATH) as f:
        data = yaml.safe_load(f)

    concepts: List[OntologyConcept] = []
    seen_ids: set = set()
    seen_concept_ids: set = set()

    for entry in data.get("concepts", []):
        cid = entry.get("id")
        if not cid:
            raise ValueError("Concept entry missing required field: id")
        if cid in seen_ids:
            raise ValueError(f"Duplicate concept id: {cid}")
        seen_ids.add(cid)

        concept_id = entry.get("concept_id", "")
        if concept_id:
            if concept_id in seen_concept_ids:
                raise ValueError(f"Duplicate concept_id: {concept_id}")
            seen_concept_ids.add(concept_id)

        domain = entry.get("domain", "")
        if domain and domain not in VALID_DOMAINS:
            raise ValueError(f"Invalid domain '{domain}' for concept '{cid}'. Valid: {VALID_DOMAINS}")

        name = entry.get("name")
        if not name:
            raise ValueError(f"Concept '{cid}' missing required field: name")

        description = entry.get("description", "")
        if not description:
            raise ValueError(f"Concept '{cid}' missing required field: description")

        concept = OntologyConcept(
            id=cid,
            concept_id=concept_id,
            name=name,
            description=description,
            domain=domain,
            cluster=entry.get("cluster", ""),
            example_fields=entry.get("example_fields", []),
            aliases=entry.get("aliases", []),
            expected_type=entry.get("expected_type", "string"),
            typical_source_systems=entry.get("typical_source_systems", []),
            persona_relevance=entry.get("persona_relevance", {}),
        )
        concepts.append(concept)

    if not concepts:
        raise ValueError(f"No concepts found in {_YAML_PATH}")

    return concepts


def get_ontology() -> List[OntologyConcept]:
    """Return ontology concepts, loading from YAML on first call."""
    global _cached_ontology
    if _cached_ontology is not None:
        return _cached_ontology

    if _YAML_PATH.exists():
        try:
            _cached_ontology = _load_from_yaml()
            logger.info(f"[Ontology] Loaded {len(_cached_ontology)} concepts from {_YAML_PATH.name}")
            return _cached_ontology
        except Exception as e:
            logger.error(f"[Ontology] Failed to load YAML ({e}), using fallback", exc_info=True)

    logger.warning("[Ontology] YAML not found, using minimal fallback list")
    _cached_ontology = list(_FALLBACK_ONTOLOGY)
    return _cached_ontology


def get_ontology_by_id(ontology_id: str) -> OntologyConcept:
    for concept in get_ontology():
        if concept.id == ontology_id:
            return concept
    raise ValueError(f"Ontology concept not found: {ontology_id}")


def reload_ontology() -> List[OntologyConcept]:
    """Force re-read from YAML. Useful after config changes."""
    global _cached_ontology
    _cached_ontology = None
    return get_ontology()
