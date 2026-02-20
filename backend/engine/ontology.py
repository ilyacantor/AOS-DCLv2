from typing import List, Dict
from backend.domain import OntologyConcept


CORE_ONTOLOGY: List[OntologyConcept] = [
    OntologyConcept(
        id="account",
        name="Account",
        description="Business account or customer entity",
        example_fields=[
            "account_id", "accountId", "account_name", "accountName",
            "company", "customer_id", "customerId", "customer_name", "customerName",
            "client_id", "clientId", "vendor_id", "vendorId"
        ],
        expected_type="string"
    ),
    OntologyConcept(
        id="opportunity",
        name="Opportunity",
        description="Sales opportunity or deal",
        example_fields=[
            "opportunity_id", "opportunityId", "deal_id", "dealId",
            "opportunity_name", "opportunityName", "pipeline", "stage"
        ],
        expected_type="string"
    ),
    OntologyConcept(
        id="revenue",
        name="Revenue",
        description="Revenue or monetary amount",
        example_fields=[
            "amount", "revenue", "total", "value", "price",
            "totalAmount", "total_amount", "lineTotal", "line_total",
            "invoiceAmount", "invoice_amount", "grossAmount", "netAmount",
            "unitPrice", "unit_price", "subtotal"
        ],
        expected_type="float"
    ),
    OntologyConcept(
        id="cost",
        name="Cost",
        description="Cost or expense amount",
        example_fields=[
            "cost", "spend", "expense", "fee", "charge",
            "taxAmount", "tax_amount", "discount", "shipping"
        ],
        expected_type="float"
    ),
    OntologyConcept(
        id="invoice",
        name="Invoice",
        description="Invoice or billing document",
        example_fields=[
            "invoice_id", "invoiceId", "invoice_number", "invoiceNumber",
            "invoice_no", "invoiceNo", "bill_id", "billId", "receipt_id"
        ],
        expected_type="string"
    ),
    OntologyConcept(
        id="currency",
        name="Currency",
        description="Currency code or monetary unit",
        example_fields=[
            "currency", "currency_code", "currencyCode", "currency_id"
        ],
        expected_type="string"
    ),
    OntologyConcept(
        id="aws_resource",
        name="AWS Resource",
        description="AWS cloud resource",
        example_fields=[
            "resource_id", "resourceId", "instance_id", "instanceId",
            "resource_type", "resourceType", "arn"
        ],
        expected_type="string"
    ),
    OntologyConcept(
        id="health",
        name="Health Score",
        description="Account health or status metric",
        example_fields=[
            "health_score", "healthScore", "status", "health", "state"
        ],
        expected_type="float"
    ),
    OntologyConcept(
        id="usage",
        name="Usage Metrics",
        description="Usage or consumption metrics",
        example_fields=[
            "usage", "consumption", "utilization", "quantity"
        ],
        expected_type="float"
    ),
    OntologyConcept(
        id="date",
        name="Date/Timestamp",
        description="Date or timestamp field",
        example_fields=[
            "date", "timestamp", "created_at", "createdAt", "updated_at", "updatedAt",
            "due_date", "dueDate", "paid_date", "paidDate", "invoice_date", "invoiceDate",
            "start_date", "startDate", "end_date", "endDate"
        ],
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
