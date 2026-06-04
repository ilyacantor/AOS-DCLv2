"""Acceptance suite for the real fabric connect server side (AAM Blueprint v3.1
§3.6 decision (c)): POST /api/dcl/ingest-records maps + resolves + converts raw
enterprise records into triples inbound, reproducing AAM's retiring SE-path
identity resolver inside DCL.

Operator-visible outcome under test: when AAM transports a NetSuite customer
record ("Acme Corp Inc.", customer #12345) and then a Sage Intacct customer
record ("Acme Corp", ACME-Corp), DCL resolves the second to the SAME canonical
identity as the first at similarity 0.9455 (auto-applied, >= 0.90), every triple
from both records carries that canonical_id with full provenance, the match
surfaces in the resolver HITL queue as auto_applied, and re-ingesting is
idempotent.

These are live-service integration tests: TestClient drives the real FastAPI app
against the aos-dev database (the same path AAM's transport will hit over HTTP).
"""

import sys
import uuid
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.api.routes.ingest_triples import get_run_triples, delete_tenant_triples
from backend.core.db import get_connection
from backend.resolver.record_resolver import similarity_score

client = TestClient(app, raise_server_exceptions=False)

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "fabric-connect-ingest-test"))
ENTITY = "AcmeCo-TEST"

NETSUITE_PIPE = "11111111-1111-1111-1111-111111111111"
SAGE_PIPE = "22222222-2222-2222-2222-222222222222"
METRICS_PIPE = "33333333-3333-3333-3333-333333333333"


def _new_run_id():
    return str(uuid.uuid4())


def _customer_pipe(pipe_id, source_system, records):
    return {
        "pipe_id": pipe_id,
        "source_system": source_system,
        "fabric_plane": "ipaas",
        "fabric_product": source_system,
        "domain": "customer",
        "identity_key": "company_name",
        "record_key_field": "customer_id",
        "records": records,
    }


def _acme_pipes():
    """NetSuite seeds "Acme Corp Inc." (customer #12345); Sage sends "Acme Corp"
    (ACME-Corp) — the AR/AP cross-source identity case."""
    netsuite = _customer_pipe(NETSUITE_PIPE, "NetSuite", [
        {"customer_id": "12345", "company_name": "Acme Corp Inc.",
         "address": "1 Industrial Way", "currency": "USD"},
    ])
    sage = _customer_pipe(SAGE_PIPE, "Sage Intacct", [
        {"customer_id": "ACME-Corp", "company_name": "Acme Corp",
         "address": "1 Industrial Way", "currency": "USD"},
    ])
    return [netsuite, sage]


def _post_records(pipes, *, run_id=None, replace=True, entity_id=ENTITY):
    run_id = run_id or _new_run_id()
    body = {
        "tenant_id": TEST_TENANT_ID,
        "dcl_ingest_id": run_id,
        "entity_id": entity_id,
        "run_mode": "Dev",
        "pipes": pipes,
    }
    resp = client.post(f"/api/dcl/ingest-records?replace={str(replace).lower()}", json=body)
    return run_id, resp


def _triples_for_run(run_id):
    # Read through the whitelisted store helper (keeps the triple-table query out
    # of the test layer; same data, all resolution + provenance columns).
    return get_run_triples(TEST_TENANT_ID, run_id)


def _hitl_rows(status=None):
    sql = "SELECT status, domain, left_value, right_value, confidence, proposed_canonical_id, extra_json FROM resolver_hitl_queue WHERE tenant_id=%s"
    params = [TEST_TENANT_ID]
    if status:
        sql += " AND status=%s"
        params.append(status)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def _cleanup():
    delete_tenant_triples(TEST_TENANT_ID)  # store helper (whitelisted boundary)
    with get_connection() as conn:
        with conn.cursor() as cur:
            for tbl in ("tenant_runs", "canonical_registry", "resolver_hitl_queue"):
                cur.execute(f"DELETE FROM {tbl} WHERE tenant_id = %s", (TEST_TENANT_ID,))
            cur.execute(
                "DELETE FROM resolver_hitl_audit WHERE hitl_queue_id NOT IN "
                "(SELECT hitl_queue_id FROM resolver_hitl_queue)"
            )
            conn.commit()
    # Drop the converter's in-process snapshot cache so a deleted registry is
    # not served stale across tests.
    from backend.db.canonical_registry import _SNAPSHOTS
    _SNAPSHOTS.clear()


@pytest.fixture(autouse=True)
def cleanup_around_each():
    _cleanup()
    yield
    _cleanup()


# ---------------------------------------------------------------------------
# Headline: AR/AP Acme reproduced in DCL
# ---------------------------------------------------------------------------

def test_acme_arap_identity_resolved_in_dcl():
    # Sanity: the ported similarity algorithm is the AAM one, so the demo pair
    # lands in the auto-apply band at ~0.94.
    score = similarity_score("Acme Corp Inc.", "Acme Corp")
    assert score == 0.9455, f"expected 0.9455 (auto-apply), got {score}"

    run_id, resp = _post_records(_acme_pipes())
    assert resp.status_code == 201, resp.text
    body = resp.json()

    # NetSuite "Acme Corp Inc." minted a canonical (discovery); Sage "Acme Corp"
    # fuzzy-matched it (auto-applied) — one of each.
    assert body["resolution_summary"].get("discovery") == 1, body["resolution_summary"]
    assert body["resolution_summary"].get("fuzzy") == 1, body["resolution_summary"]

    triples = _triples_for_run(run_id)
    customer_triples = [t for t in triples if t["concept"] == "customer"]
    assert customer_triples, "no customer triples written"

    # Both source records resolved to the SAME canonical identity.
    canon_ids = {str(t["canonical_id"]) for t in customer_triples if t["canonical_id"]}
    assert len(canon_ids) == 1, f"expected one shared canonical, got {canon_ids}"
    canonical = canon_ids.pop()

    # The Sage ("Acme Corp") record's triples are the fuzzy-resolved ones.
    sage_triples = [t for t in customer_triples if str(t["pipe_id"]) == SAGE_PIPE]
    assert sage_triples, "no Sage customer triples"
    for t in sage_triples:
        assert str(t["canonical_id"]) == canonical
        assert t["resolution_method"] == "fuzzy", t
        assert float(t["resolution_confidence"]) >= 0.90, t

    # The NetSuite seed record bound to the same canonical, deterministic side.
    ns_triples = [t for t in customer_triples if str(t["pipe_id"]) == NETSUITE_PIPE]
    for t in ns_triples:
        assert str(t["canonical_id"]) == canonical
        assert t["resolution_method"] == "deterministic", t

    # The match is auditable in the resolver HITL queue as auto_applied at 0.9455.
    auto = _hitl_rows(status="auto_applied")
    assert len(auto) == 1, f"expected one auto_applied row, got {auto}"
    row = auto[0]
    assert row["domain"] == "customer"
    assert float(row["confidence"]) == 0.9455
    assert str(row["proposed_canonical_id"]) == canonical
    assert (row["extra_json"] or {}).get("match_rule") == "fuzzy"


def test_acme_idempotent_on_replace():
    run_id, resp = _post_records(_acme_pipes())
    assert resp.status_code == 201, resp.text
    first = _triples_for_run(run_id)

    # Re-ingest the SAME records under the SAME run id with replace=true.
    _, resp2 = _post_records(_acme_pipes(), run_id=run_id, replace=True)
    assert resp2.status_code == 201, resp2.text
    second = _triples_for_run(run_id)

    assert len(second) == len(first), f"triple count drifted: {len(first)} -> {len(second)}"

    # The two source representations of Acme UNIFY to ONE canonical (that is the
    # point of identity resolution) — and replay mints nothing new.
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM canonical_registry WHERE tenant_id=%s AND domain='customer'",
                (TEST_TENANT_ID,),
            )
            canon_count = cur.fetchone()[0]
    assert canon_count == 1, f"expected 1 merged customer canonical after replay, got {canon_count}"

    # HITL auto_applied row deduped to one (replay is a no-op, not a duplicate).
    assert len(_hitl_rows(status="auto_applied")) == 1


# ---------------------------------------------------------------------------
# Cloud-spend path (deliverable 1 only): map + convert, no identity resolution
# ---------------------------------------------------------------------------

def test_domainless_pipe_maps_without_resolution():
    pipe = {
        "pipe_id": METRICS_PIPE,
        "source_system": "AWS Cost Explorer",
        "fabric_plane": "warehouse",
        # no domain / identity_key -> Live Semantic Mapper classifies per field,
        # resolver is skipped.
        "records": [
            {"invoice_number": "INV-9001", "amount": 4200, "currency": "USD"},
        ],
    }
    run_id, resp = _post_records([pipe])
    assert resp.status_code == 201, resp.text
    body = resp.json()

    # No identity was resolved (domainless pipe).
    assert body["resolution_summary"] == {}, body["resolution_summary"]

    triples = _triples_for_run(run_id)
    assert triples, "domainless pipe produced no triples"
    # The mapper classified at least the invoice field to a persona concept; no
    # triple carries a canonical (resolution was skipped).
    assert all(t["canonical_id"] is None for t in triples), triples
    assert all(t["resolution_method"] is None for t in triples), triples


def test_nonpersona_field_dropped_loudly_not_silently():
    fields = ["invoice_number", "currency", "amount"]
    pipe = {
        "pipe_id": METRICS_PIPE,
        "source_system": "AWS Cost Explorer",
        "fabric_plane": "warehouse",
        "records": [{f: ("INV-9002" if f == "invoice_number" else
                         "USD" if f == "currency" else 12) for f in fields}],
    }
    run_id, resp = _post_records([pipe])
    assert resp.status_code == 201, resp.text
    warnings = resp.json()["warnings"]

    # `currency` classifies to a non-persona concept -> dropped with a LOUD warning.
    assert any(w["type"] == "non_persona_concept" and w["field"] == "currency"
               for w in warnings), warnings

    # No silent drop: every input field is accounted for as either a written
    # triple (property == field) or a warning.
    triple_fields = {t["property"] for t in _triples_for_run(run_id)
                     if str(t["pipe_id"]) == METRICS_PIPE}
    warning_fields = {w.get("field") for w in warnings}
    assert set(fields) <= (triple_fields | warning_fields), \
        f"field(s) silently lost: {set(fields) - (triple_fields | warning_fields)}"


# ---------------------------------------------------------------------------
# Provenance completeness
# ---------------------------------------------------------------------------

def test_every_triple_carries_full_provenance():
    run_id, resp = _post_records(_acme_pipes())
    assert resp.status_code == 201, resp.text
    triples = _triples_for_run(run_id)
    assert triples
    for t in triples:
        assert t["source_system"], t
        assert t["source_field"], t
        assert t["pipe_id"], t
        assert t["fabric_plane"] == "ipaas", t
        assert t["confidence_score"] is not None, t
        assert t["confidence_tier"] in {"exact", "high", "medium", "low"}, t


# ---------------------------------------------------------------------------
# HITL pending -> operator approve promotes the bound triples to manual
# ---------------------------------------------------------------------------

def test_hitl_pending_approve_promotes_triples_to_manual():
    # Find a value pair that lands in the pending band [0.65, 0.90).
    seed_val = "Northwind Traders"
    probe_val = "Northwind Trading Co"
    score = similarity_score(probe_val, seed_val)
    assert 0.65 <= score < 0.90, f"probe pair not in pending band: {score}"

    seed_pipe = _customer_pipe(NETSUITE_PIPE, "NetSuite",
                               [{"customer_id": "S1", "company_name": seed_val}])
    probe_pipe = _customer_pipe(SAGE_PIPE, "Sage Intacct",
                                [{"customer_id": "P1", "company_name": probe_val}])
    run_id, resp = _post_records([seed_pipe, probe_pipe])
    assert resp.status_code == 201, resp.text
    assert resp.json()["resolution_summary"].get("hitl_pending") == 1, resp.json()

    pending = _hitl_rows(status="pending")
    assert len(pending) == 1, pending
    hitl_id = None
    # fetch the id via the operator list endpoint (the surface AAM's UI uses)
    listed = client.get(f"/api/dcl/resolver/hitl?tenant_id={TEST_TENANT_ID}&status=pending").json()
    assert listed["count"] == 1, listed
    hitl_id = listed["items"][0]["hitl_queue_id"]

    # Pre-approval: the probe's per-record triples are fuzzy-bound. (The records-path
    # also emits a non-resolution customer.total summary aggregate per pipe; scope to
    # the per-record party concept so the resolution assertion checks what it means to.)
    probe_triples = [t for t in _triples_for_run(run_id)
                     if str(t["pipe_id"]) == SAGE_PIPE and t["concept"] == "customer"]
    assert probe_triples and all(t["resolution_method"] == "fuzzy" for t in probe_triples)

    dec = client.post(f"/api/dcl/resolver/hitl/{hitl_id}/decide",
                      json={"decision": "approved", "decided_by": "tester"})
    assert dec.status_code == 200, dec.text
    assert dec.json()["triples_promoted"] >= 1, dec.json()

    # Post-approval: the same per-record triples are now manual @ 0.99 (hitl_confirmed).
    # (Scope to the per-record party concept — the customer.total summary aggregate is not
    # resolution-bound and carries no resolution_method/confidence.)
    after = [t for t in _triples_for_run(run_id)
             if str(t["pipe_id"]) == SAGE_PIPE and t["concept"] == "customer"]
    assert all(t["resolution_method"] == "manual" for t in after), after
    assert all(float(t["resolution_confidence"]) == 0.99 for t in after), after


# ---------------------------------------------------------------------------
# Negative tests (paired with the visible failure surfaces)
# ---------------------------------------------------------------------------

def test_missing_entity_id_is_422():
    body = {"tenant_id": TEST_TENANT_ID, "dcl_ingest_id": _new_run_id(),
            "entity_id": "", "pipes": _acme_pipes()}
    resp = client.post("/api/dcl/ingest-records", json=body)
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["error"] == "ENTITY_ID_REQUIRED"


def test_non_uuid_pipe_id_is_400():
    pipe = _customer_pipe("not-a-uuid", "NetSuite",
                          [{"customer_id": "1", "company_name": "X Co"}])
    _, resp = _post_records([pipe])
    assert resp.status_code == 400, resp.text
    assert "pipe_id" in resp.json()["detail"]["message"]


def test_identity_key_without_domain_is_422():
    pipe = {"pipe_id": METRICS_PIPE, "source_system": "NetSuite",
            "fabric_plane": "ipaas", "identity_key": "company_name",
            "records": [{"company_name": "X Co"}]}
    _, resp = _post_records([pipe])
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["error"] == "RESOLVER_CONTRACT"
