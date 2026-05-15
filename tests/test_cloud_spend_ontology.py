"""
Cloud Spend (FinOps) ontology coverage tests — WP2 of Plan A.

Covers:
  - The `cloud_spend` domain is declared in the YAML and is part of
    `VALID_DOMAINS`.
  - All 7 cloud_spend concepts (1 umbrella + 6 satellites) load through
    `_load_from_yaml()` and are registered in `ConceptRegistry`.
  - An ingest call carrying a representative cloud-resource record writes
    triples to `semantic_triples` with concepts in the `cloud_spend.*`
    namespace, no entries land in any unmapped/review queue, and the
    `SELECT DISTINCT concept WHERE concept LIKE 'cloud_spend.%'` query
    returns the expected concept set.
  - An ingest of a triple whose root concept is unregistered is rejected at
    validation (INVALID_CONCEPT).

Pattern matches `tests/test_prod_mode_ingest.py` and `tests/test_s1_dcl.py`:
  - uuid5-derived TEST_TENANT_ID to avoid collisions with seeded data
  - TestClient against backend.api.main:app
  - DB cleanup fixture around each test
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
from backend.core.db import get_connection
from backend.engine.ontology import VALID_DOMAINS, reload_ontology
from backend.registry.concept_registry import ConceptRegistry


client = TestClient(app, raise_server_exceptions=False)

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "cloud-spend-ontology-test"))

# The 7 concepts declared in the cloud_spend domain. Updated whenever the
# YAML's cloud_spend block changes.
EXPECTED_CLOUD_SPEND_CONCEPT_IDS = {
    "cloud_spend",          # CSP-001 — umbrella root for cloud_spend.* concepts
    "cloud_resource",       # CSP-002
    "cloud_account",        # CSP-003
    "cloud_service",        # CSP-004
    "cloud_spend_period",   # CSP-005
    "cloud_utilization",    # CSP-006
    "cloud_owner",          # CSP-007
}


# ---------------------------------------------------------------------------
# DB cleanup
# ---------------------------------------------------------------------------

def _cleanup():
    """Remove anything this test inserted under TEST_TENANT_ID."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM semantic_triples WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            cur.execute(
                "DELETE FROM tenant_runs WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            conn.commit()


@pytest.fixture(autouse=True)
def cleanup_around_each():
    _cleanup()
    yield
    _cleanup()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cloud_spend_triple(concept: str, value, **overrides) -> dict:
    """Factory for cloud_spend ingest triples.

    Mirrors the standard ingest payload shape used by the rest of the suite
    (test_s1_dcl, test_prod_mode_ingest).
    """
    base = {
        "entity_id": "test_entity_cloud_spend",
        "concept": concept,
        "property": "value",
        "value": value,
        "period": "2026-04",
        "currency": "USD",
        "source_system": "aws_cur",
        "source_table": "billing_export",
        "source_field": concept.split(".")[-1],
        "pipe_id": "00000000-0000-0000-0000-000000000099",
        "fabric_plane": "warehouse",
        "confidence_score": 0.95,
        "confidence_tier": "high",
    }
    base.update(overrides)
    return base


def _post_triples(triples: list[dict], run_id: str | None = None) -> tuple:
    rid = run_id or str(uuid.uuid4())
    resp = client.post(
        "/api/dcl/ingest-triples",
        json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": rid,
            "triples": triples,
        },
    )
    return resp, rid


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_cloud_spend_domain_is_declared():
    """The cloud_spend domain is in VALID_DOMAINS, derived from the YAML
    `domains:` block. Asserting via VALID_DOMAINS, not via a hardcoded list,
    is the contract under WP2 (single source of truth in the YAML).
    """
    assert "cloud_spend" in VALID_DOMAINS, (
        f"cloud_spend domain missing from VALID_DOMAINS — "
        f"check the `domains:` block in config/ontology_concepts.yaml. "
        f"VALID_DOMAINS = {sorted(VALID_DOMAINS)}"
    )
    # Sanity: pre-existing domains are still present (regression guard).
    for expected in ("finance", "it_infra", "cofa"):
        assert expected in VALID_DOMAINS, (
            f"Pre-existing domain '{expected}' missing — "
            f"YAML `domains:` block lost an entry."
        )


def test_all_seven_cloud_spend_concepts_load_and_are_mappable():
    """The 7 cloud_spend concepts load through _load_from_yaml() (the same
    path the engine uses at startup) and are present in the ConceptRegistry
    used by the ingest validator.
    """
    ont = reload_ontology()
    cloud_spend_concepts = {c.id for c in ont if c.domain == "cloud_spend"}
    assert cloud_spend_concepts == EXPECTED_CLOUD_SPEND_CONCEPT_IDS, (
        f"cloud_spend domain concept set differs from expected.\n"
        f"  expected: {sorted(EXPECTED_CLOUD_SPEND_CONCEPT_IDS)}\n"
        f"  got:      {sorted(cloud_spend_concepts)}\n"
        f"  missing:  {sorted(EXPECTED_CLOUD_SPEND_CONCEPT_IDS - cloud_spend_concepts)}\n"
        f"  extra:    {sorted(cloud_spend_concepts - EXPECTED_CLOUD_SPEND_CONCEPT_IDS)}"
    )

    registry = ConceptRegistry()
    for cid in EXPECTED_CLOUD_SPEND_CONCEPT_IDS:
        assert registry.is_valid_concept(cid), (
            f"Concept '{cid}' not registered in ConceptRegistry"
        )
        # Each concept root accepts a `<root>.<sub>` extension via
        # prefix-based validation, the same shape Farm and AAM produce.
        assert registry.is_valid_concept(f"{cid}.sub"), (
            f"Prefix validation failed for '{cid}.sub'"
        )


def test_cloud_resource_ingest_writes_triples_and_no_review_queue():
    """An ingest call carrying a representative cloud resource record (with
    cost_usd, cpu_utilization_pct, owner_cost_center, account_id, service,
    period) lands triples in semantic_triples under cloud_spend.* concepts,
    and produces no entry in any unmapped/review queue.

    Operator-visible outcome (verbatim from WP2 prompt):
    "Querying SELECT DISTINCT concept FROM semantic_triples
     WHERE concept LIKE 'cloud_spend.%' AND tenant_id = '<test-tenant>'
     returns the expected concept set."
    """
    # One EC2-shaped resource record decomposed into individual triples
    # under the cloud_spend.* namespace. Each triple uses the cloud_spend
    # root concept so the operator query LIKE 'cloud_spend.%' will return
    # the full set.
    triples = [
        _make_cloud_spend_triple(
            concept="cloud_spend.cloud_resource.cost_usd",
            value=1234.56,
            property="cost_usd",
            source_field="cost_usd",
        ),
        _make_cloud_spend_triple(
            concept="cloud_spend.cloud_utilization.cpu_pct",
            value=42.7,
            property="cpu_pct",
            source_field="cpu_utilization_pct",
        ),
        _make_cloud_spend_triple(
            concept="cloud_spend.cloud_owner.cost_center",
            value="cc-eng-platform",
            property="cost_center",
            source_field="owner_cost_center",
            expected_type_value=None,
        ),
        _make_cloud_spend_triple(
            concept="cloud_spend.cloud_account.account_id",
            value="123456789012",
            property="account_id",
            source_field="account_id",
        ),
        _make_cloud_spend_triple(
            concept="cloud_spend.cloud_service.service_code",
            value="ec2",
            property="service_code",
            source_field="service",
        ),
        _make_cloud_spend_triple(
            concept="cloud_spend.cloud_spend_period.period_end",
            value="2026-04-30",
            property="period_end",
            source_field="period",
        ),
    ]
    # The factory accidentally added an unsupported key; strip it before posting.
    for t in triples:
        t.pop("expected_type_value", None)

    resp, run_id = _post_triples(triples)
    assert resp.status_code == 201, (
        f"Expected 201 from ingest; got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body["triple_count"] == len(triples), (
        f"Expected {len(triples)} triples ingested; got {body['triple_count']}"
    )
    # No "unmapped"/"review"/"rejected" entries should appear in the response.
    # The ingest API returns a flat success response; the absence of any
    # non-empty review/queue keys is the contract.
    for forbidden in ("unmapped", "review_queue", "rejected", "human_review"):
        val = body.get(forbidden)
        assert not val, (
            f"Ingest response should not include any '{forbidden}' entries "
            f"for fully-mapped cloud_spend triples. Got: {val!r}"
        )

    # Verify the operator-visible outcome directly against the DB.
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT concept
                FROM semantic_triples
                WHERE concept LIKE 'cloud_spend.%%'
                  AND tenant_id = %s
                """,
                (TEST_TENANT_ID,),
            )
            stored_concepts = {row[0] for row in cur.fetchall()}

    expected_stored = {t["concept"] for t in triples}
    assert stored_concepts == expected_stored, (
        f"Operator-visible outcome failed.\n"
        f"  Query:     SELECT DISTINCT concept FROM semantic_triples "
        f"WHERE concept LIKE 'cloud_spend.%' AND tenant_id = "
        f"'{TEST_TENANT_ID}'\n"
        f"  Expected:  {sorted(expected_stored)}\n"
        f"  Got:       {sorted(stored_concepts)}\n"
        f"  Missing:   {sorted(expected_stored - stored_concepts)}\n"
        f"  Extra:     {sorted(stored_concepts - expected_stored)}"
    )


def test_unregistered_concept_root_is_rejected():
    """A triple whose root concept is not in the registry is rejected at
    validation. This is the negative test paired with the cloud_spend
    domain registration — confirms the validator still blocks unmapped
    concepts after the new domain is added.
    """
    triples = [
        _make_cloud_spend_triple(
            concept="zzz_not_a_real_root.unknown_concept",
            value=1.0,
            property="value",
            source_field="value",
        )
    ]
    resp, _ = _post_triples(triples)
    # Validation must reject — either at the concept-registry layer (400
    # INVALID_CONCEPT) or at the persona-domain layer (422 UNMAPPED_DOMAIN).
    # Either failure mode is acceptable; what matters is that the triple
    # does NOT land in semantic_triples.
    assert resp.status_code in (400, 422), (
        f"Expected 400/422 for unregistered root concept; got "
        f"{resp.status_code}: {resp.text}"
    )

    # Confirm no triple landed in storage.
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM semantic_triples
                WHERE tenant_id = %s
                  AND concept LIKE 'zzz_not_a_real_root%%'
                """,
                (TEST_TENANT_ID,),
            )
            count = cur.fetchone()[0]
    assert count == 0, (
        f"Unregistered concept should not write any triples; "
        f"found {count} rows."
    )
