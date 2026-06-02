"""Context-aware field classification — orders' amount_usd lands in revenue,
not cloud_spend (#49).

Operator-visible outcome under test: when the Confluent orders.v1 feed
(order_id, customer_name, amount_usd, currency, status, period) lands through
the records-path with NO declared domain, DCL's Live Semantic Mapper classifies
amount_usd to the **revenue** concept and NEVER to cloud_spend. amount_usd
EXACT-matches cloud_spend's example_fields, so the raw name default IS
cloud_spend — this fix overrides it with the pipe's own context: currency ->
finance and customer_name -> sales make finance native to the pipe and
cloud_spend foreign, so the ambiguous amount_usd binds to revenue (finance).

It generalizes: a differently-named finance feed (Stripe charges) reusing the
same ambiguous amount_usd is routed to revenue by ITS OWN context. And
cloud_spend itself is unchanged: a domain="cloud_spend" pipe still aggregates to
cloud_spend.* via the aggregator path, which never touches per-field mapping.

Live-service integration tests: TestClient drives the real FastAPI app against
aos-dev — the same path AAM's transport hits over HTTP.
"""

import sys
import uuid
from pathlib import Path

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv

load_dotenv(_repo / ".env.development")

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.api.routes.ingest_triples import delete_tenant_triples

client = TestClient(app, raise_server_exceptions=False)


def _ingest(tenant_id: str, entity_id: str, pipe: dict):
    run_id = str(uuid.uuid4())
    body = {
        "tenant_id": tenant_id,
        "run_id": run_id,
        "entity_id": entity_id,
        "run_mode": "Dev",
        "pipes": [pipe],
    }
    resp = client.post("/api/dcl/ingest-records", json=body)
    assert resp.status_code == 201, f"ingest failed {resp.status_code}: {resp.text}"
    return run_id, resp.json()


def _browse(tenant_id: str, run_id: str):
    resp = client.get(
        f"/api/dcl/triples/browse?tenant_id={tenant_id}&run_id={run_id}&limit=200"
    )
    assert resp.status_code == 200, f"browse failed {resp.status_code}: {resp.text}"
    d = resp.json()
    return d if isinstance(d, list) else (d.get("triples") or d.get("rows") or d.get("data") or [])


# Real Confluent orders.v1 event shape (Farm sim: farm/src/fabric_sims/confluent/sim.py).
ORDERS = [
    {"order_id": "ord-10000", "customer_name": "Acme Robotics", "amount_usd": 1234.5,
     "currency": "USD", "status": "placed", "period": "2026-Q2"},
    {"order_id": "ord-10001", "customer_name": "Globex Logistics", "amount_usd": 880.0,
     "currency": "USD", "status": "fulfilled", "period": "2026-Q2"},
]


def test_orders_amount_usd_classifies_to_revenue_not_cloud_spend():
    """amount_usd in an orders pipe -> revenue (positive) and never cloud_spend (paired negative)."""
    tenant = str(uuid.uuid5(uuid.NAMESPACE_DNS, "orders-revenue-49-pos"))
    delete_tenant_triples(tenant)
    pipe = {
        "pipe_id": str(uuid.uuid4()), "source_system": "confluent",
        "fabric_plane": "event_bus", "fabric_product": "confluent",
        "domain": None, "identity_key": None, "record_key_field": "order_id",
        "records": ORDERS,
    }
    try:
        run_id, resp = _ingest(tenant, "OrdersRevenueProof", pipe)
        summary = resp["concept_summary"]
        # Positive: both orders' amount_usd became revenue.
        assert summary.get("revenue") == 2, f"expected revenue=2, got concept_summary={summary}"
        # Paired negative: the bug cannot return — nothing classified as cloud_spend.
        assert "cloud_spend" not in summary, f"amount mis-bound to cloud_spend: {summary}"
        # Persisted + user-visible: the amount_usd row carries concept=revenue.
        rows = _browse(tenant, run_id)
        amt_rows = [r for r in rows if r.get("property") == "amount_usd"]
        assert amt_rows, "amount_usd produced no triple"
        assert all(r.get("concept") == "revenue" for r in amt_rows), \
            f"amount_usd concept(s) = {[r.get('concept') for r in amt_rows]} (want all 'revenue')"
        assert all(r.get("concept") != "cloud_spend" for r in rows), \
            "a cloud_spend triple surfaced from the orders feed"
    finally:
        delete_tenant_triples(tenant)


def test_new_finance_feed_routes_amount_by_context_not_name():
    """Generalization: a different source + field names reusing amount_usd routes by context.

    A Stripe charges feed (charge_id, account_name, amount_usd, currency,
    billing_period) shares none of the orders field names except the ambiguous
    amount_usd, yet still binds amount_usd to revenue — currency anchors finance
    for THIS pipe too. A new domain reusing a known field name routes by context,
    not the cloud_spend name default.
    """
    tenant = str(uuid.uuid5(uuid.NAMESPACE_DNS, "orders-revenue-49-gen"))
    delete_tenant_triples(tenant)
    charges = [
        {"charge_id": "ch_1", "account_name": "Initech", "amount_usd": 500.0,
         "currency": "USD", "billing_period": "2026-Q2"},
        {"charge_id": "ch_2", "account_name": "Hooli", "amount_usd": 750.0,
         "currency": "USD", "billing_period": "2026-Q2"},
    ]
    pipe = {
        "pipe_id": str(uuid.uuid4()), "source_system": "stripe",
        "fabric_plane": "api_gateway", "fabric_product": "stripe",
        "domain": None, "identity_key": None, "record_key_field": "charge_id",
        "records": charges,
    }
    try:
        _run_id, resp = _ingest(tenant, "ChargesRevenueProof", pipe)
        summary = resp["concept_summary"]
        assert summary.get("revenue") == 2, f"new feed amount_usd not revenue: {summary}"
        assert "cloud_spend" not in summary, f"new feed mis-bound to cloud_spend: {summary}"
    finally:
        delete_tenant_triples(tenant)


def test_cloud_spend_pipe_classifies_unchanged():
    """cloud_spend is untouched: a domain=cloud_spend pipe still aggregates to
    cloud_spend.* via the aggregator (never per-field mapping), and cloud-spend
    cost never leaks into revenue."""
    tenant = str(uuid.uuid5(uuid.NAMESPACE_DNS, "orders-revenue-49-cs"))
    delete_tenant_triples(tenant)
    resources = [
        {"resource_id": "i-1", "service": "EC2", "region": "us-east-1",
         "cost_usd": 120.0, "account_id": "acct-1", "period": "2026-Q2"},
        {"resource_id": "i-2", "service": "S3", "region": "us-east-1",
         "cost_usd": 30.0, "account_id": "acct-1", "period": "2026-Q2"},
    ]
    pipe = {
        "pipe_id": str(uuid.uuid4()), "source_system": "aws_cost_explorer",
        "fabric_plane": "warehouse", "fabric_product": "aws_cost_explorer",
        "domain": "cloud_spend", "identity_key": None, "record_key_field": "resource_id",
        "records": resources,
    }
    try:
        run_id, _resp = _ingest(tenant, "CloudSpendProof", pipe)
        rows = _browse(tenant, run_id)
        concepts = {r.get("concept") for r in rows}
        assert any(str(c).startswith("cloud_spend.summary") for c in concepts), \
            f"cloud_spend aggregation changed; concepts={sorted(str(c) for c in concepts)}"
        assert "revenue" not in concepts, \
            f"cloud-spend cost leaked into revenue: {sorted(str(c) for c in concepts)}"
    finally:
        delete_tenant_triples(tenant)
