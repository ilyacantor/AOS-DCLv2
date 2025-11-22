from typing import List, Dict
from backend.domain import OntologyConcept


CORE_ONTOLOGY: List[OntologyConcept] = [
    OntologyConcept(
        id="account",
        name="Account",
        description="Business account or customer entity",
        example_fields=["account_id", "account_name", "company", "customer_id"],
        expected_type="string"
    ),
    OntologyConcept(
        id="opportunity",
        name="Opportunity",
        description="Sales opportunity or deal",
        example_fields=["opportunity_id", "deal_id", "opportunity_name"],
        expected_type="string"
    ),
    OntologyConcept(
        id="revenue",
        name="Revenue",
        description="Revenue or monetary amount",
        example_fields=["amount", "revenue", "total", "value"],
        expected_type="float"
    ),
    OntologyConcept(
        id="cost",
        name="Cost",
        description="Cost or expense amount",
        example_fields=["cost", "spend", "expense", "price"],
        expected_type="float"
    ),
    OntologyConcept(
        id="aws_resource",
        name="AWS Resource",
        description="AWS cloud resource",
        example_fields=["resource_id", "instance_id", "resource_type"],
        expected_type="string"
    ),
    OntologyConcept(
        id="health",
        name="Health Score",
        description="Account health or status metric",
        example_fields=["health_score", "status", "health"],
        expected_type="float"
    ),
    OntologyConcept(
        id="usage",
        name="Usage Metrics",
        description="Usage or consumption metrics",
        example_fields=["usage", "consumption", "utilization"],
        expected_type="float"
    ),
    OntologyConcept(
        id="date",
        name="Date/Timestamp",
        description="Date or timestamp field",
        example_fields=["date", "timestamp", "created_at", "updated_at"],
        expected_type="datetime"
    )
]


def get_ontology() -> List[OntologyConcept]:
    return CORE_ONTOLOGY


def get_ontology_by_id(ontology_id: str) -> OntologyConcept:
    for concept in CORE_ONTOLOGY:
        if concept.id == ontology_id:
            return concept
    raise ValueError(f"Ontology concept not found: {ontology_id}")
