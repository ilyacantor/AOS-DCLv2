# Operator-visible outcome: for a tenant with chain_steps=2 require_distinct,
# proposer P creating a proposal → P's approve is DENIED (409, traced);
# A1 approves step 1 (proposal stays pending, no canonical_artifact_id, trace exists);
# A2 (≠A1, ≠P) approves step 2 → proposal canonical, trace exists;
# A1 trying step 2 is DENIED (traced); a tenant with no policy single-approves as Gate 3A.
"""Gate 3C D2 — approval chain enforcement acceptance tests.

Live-service integration tests against aos-dev. All tenant IDs are per-run-unique
uuid4 values so the durable store is re-runnable without cleanup races (B14).
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

client = TestClient(app, raise_server_exceptions=False)


# ── per-run unique tenants (B14-safe) ─────────────────────────────────────────
TAG = uuid.uuid4().hex[:6]
CHAIN_TENANT = str(uuid.uuid4())   # 2-step distinct policy
NOPP_TENANT  = str(uuid.uuid4())   # require_distinct only (chain_steps=1)
BACK_TENANT  = str(uuid.uuid4())   # no policy → Gate 3A back-compat

PROPOSER  = f"proposer-{TAG}"
APPROVER1 = f"approver1-{TAG}"
APPROVER2 = f"approver2-{TAG}"


# ── helpers ───────────────────────────────────────────────────────────────────

def _authority_map_proposal(tenant_id: str, concept_prefix: str, proposer: str | None = None):
    body = {
        "tenant_id": tenant_id,
        "proposals": [{
            "proposal_type": "authority_map",
            "payload": {
                "concept_prefix": concept_prefix,
                "ranked_sources": ["ERP", "CRM"],
            },
            "confidence": 0.9,
            "provenance": {"basis": "confirmed", "confirmed_by": proposer or "system"},
            "proposer": proposer,
        }],
    }
    r = client.post("/api/dcl/proposals", json=body)
    assert r.status_code == 201, f"intake failed {r.status_code}: {r.text}"
    proposals = r.json()["proposals"]
    assert proposals[0]["status"] == "accepted", proposals
    return proposals[0]["proposal_id"]


def _decide(tenant_id: str, proposal_id: str, decision: str, decided_by: str, note: str = None):
    body = {
        "tenant_id": tenant_id,
        "decision": decision,
        "decided_by": decided_by,
    }
    if note:
        body["note"] = note
    return client.post(f"/api/dcl/proposals/{proposal_id}/decide", json=body)


def _get_proposal(tenant_id: str, proposal_id: str):
    r = client.get(f"/api/dcl/proposals", params={"tenant_id": tenant_id})
    assert r.status_code == 200
    for p in r.json()["proposals"]:
        if p["proposal_id"] == proposal_id:
            return p
    return None


def _get_traces(tenant_id: str, proposal_id: str):
    r = client.get("/api/dcl/traces", params={"tenant_id": tenant_id})
    if r.status_code == 200:
        return [t for t in r.json().get("traces", [])
                if t.get("refs", {}).get("proposal_id") == proposal_id]
    return []


def _set_policy(tenant_id: str, require_distinct: bool, chain_steps: int):
    r = client.put("/api/dcl/approval-policy", json={
        "tenant_id": tenant_id,
        "require_distinct_proposer_approver": require_distinct,
        "chain_steps": chain_steps,
    })
    assert r.status_code == 200, f"policy set failed {r.status_code}: {r.text}"


def _cleanup_tenant(tenant_id: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM change_proposal_decisions WHERE tenant_id = %s::uuid",
                (tenant_id,),
            )
            cur.execute(
                "DELETE FROM change_proposals WHERE tenant_id = %s::uuid",
                (tenant_id,),
            )
            cur.execute(
                "DELETE FROM tenant_authority_map WHERE tenant_id = %s",
                (tenant_id,),
            )
            cur.execute(
                "DELETE FROM tenant_approval_policy WHERE tenant_id = %s::uuid",
                (tenant_id,),
            )
        conn.commit()


@pytest.fixture(autouse=True)
def _clean():
    for t in [CHAIN_TENANT, NOPP_TENANT, BACK_TENANT]:
        _cleanup_tenant(t)
    yield
    for t in [CHAIN_TENANT, NOPP_TENANT, BACK_TENANT]:
        _cleanup_tenant(t)


# =============================================================================
# 1. Policy round-trip
# =============================================================================

def test_policy_default_returns_defaults():
    """GET on a tenant with no policy row returns defaults (chain_steps=1, no distinct)."""
    tenant = str(uuid.uuid4())
    r = client.get("/api/dcl/approval-policy", params={"tenant_id": tenant})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["chain_steps"] == 1
    assert d["require_distinct_proposer_approver"] is False
    assert d["policy_source"] == "default"


def test_policy_put_and_get():
    """PUT then GET round-trips chain_steps and require_distinct correctly."""
    _set_policy(CHAIN_TENANT, require_distinct=True, chain_steps=2)
    r = client.get("/api/dcl/approval-policy", params={"tenant_id": CHAIN_TENANT})
    assert r.status_code == 200
    d = r.json()
    assert d["chain_steps"] == 2
    assert d["require_distinct_proposer_approver"] is True
    assert d["policy_source"] == "configured"


def test_policy_put_zero_steps_rejected():
    """PUT with chain_steps=0 is rejected 422."""
    r = client.put("/api/dcl/approval-policy", json={
        "tenant_id": CHAIN_TENANT,
        "require_distinct_proposer_approver": False,
        "chain_steps": 0,
    })
    assert r.status_code == 422


# =============================================================================
# 2. Proposer≠approver enforcement (require_distinct, chain_steps=1)
# =============================================================================

def test_proposer_cannot_approve_own_proposal():
    """Proposer trying to approve their own proposal is denied 409 + traced."""
    _set_policy(NOPP_TENANT, require_distinct=True, chain_steps=1)
    pid = _authority_map_proposal(NOPP_TENANT, "cloud_spend", proposer=PROPOSER)

    # Same identity as proposer → denied
    r = _decide(NOPP_TENANT, pid, "approve", PROPOSER)
    assert r.status_code == 409, f"expected 409, got {r.status_code}: {r.text}"
    detail = r.json().get("detail", "")
    assert "cannot approve their own proposal" in detail, detail
    assert PROPOSER in detail, detail

    # Proposal must still be pending
    proposal = _get_proposal(NOPP_TENANT, pid)
    assert proposal is not None
    assert proposal["status"] == "pending", proposal["status"]

    # Denial trace must exist
    traces = _get_traces(NOPP_TENANT, pid)
    denied_traces = [t for t in traces if t.get("decision_type") == "denied"]
    assert len(denied_traces) >= 1, f"no denied trace found; traces={traces}"


def test_distinct_approver_succeeds():
    """A distinct approver (≠ proposer) succeeds when require_distinct=true, chain_steps=1."""
    _set_policy(NOPP_TENANT, require_distinct=True, chain_steps=1)
    pid = _authority_map_proposal(NOPP_TENANT, "revenue", proposer=PROPOSER)

    r = _decide(NOPP_TENANT, pid, "approve", APPROVER1)
    assert r.status_code == 200, f"unexpected {r.status_code}: {r.text}"
    d = r.json()
    assert d["decision"] == "approve"
    assert d["canonical_artifact_id"] is not None
    assert d["is_final"] is True

    proposal = _get_proposal(NOPP_TENANT, pid)
    assert proposal["status"] == "approved"


# =============================================================================
# 3. Two-step chain end-to-end
# =============================================================================

def test_two_step_chain_end_to_end():
    """Full two-step chain: P creates, A1 approves step1 (not canonical), A2 approves step2 (canonical)."""
    _set_policy(CHAIN_TENANT, require_distinct=True, chain_steps=2)
    pid = _authority_map_proposal(CHAIN_TENANT, "workforce", proposer=PROPOSER)

    # Step 1: A1 approves (proposer P ≠ A1, so allowed)
    r = _decide(CHAIN_TENANT, pid, "approve", APPROVER1, note="step 1 approval")
    assert r.status_code == 200, f"step1 failed {r.status_code}: {r.text}"
    d = r.json()
    assert d["decision"] == "approve"
    assert d["step_number"] == 1
    assert d["chain_steps"] == 2
    assert d["is_final"] is False
    assert d["canonical_artifact_id"] is None, "canonical must NOT be applied at step 1"

    # Proposal must still be pending
    proposal = _get_proposal(CHAIN_TENANT, pid)
    assert proposal is not None
    assert proposal["status"] == "pending", f"after step1 status must be pending, got {proposal['status']}"
    assert proposal.get("canonical_artifact_id") is None

    # Step 1 trace must exist
    traces_after_step1 = _get_traces(CHAIN_TENANT, pid)
    step1_traces = [t for t in traces_after_step1 if t.get("decision_type") == "approve"]
    assert len(step1_traces) == 1, f"expected 1 approve trace after step1, got {step1_traces}"

    # Step 2: A2 approves (distinct from A1 and from PROPOSER) → canonical
    r = _decide(CHAIN_TENANT, pid, "approve", APPROVER2, note="step 2 approval")
    assert r.status_code == 200, f"step2 failed {r.status_code}: {r.text}"
    d = r.json()
    assert d["decision"] == "approve"
    assert d["step_number"] == 2
    assert d["chain_steps"] == 2
    assert d["is_final"] is True
    assert d["canonical_artifact_id"] is not None, "canonical must be applied at step 2"

    # Proposal must now be approved with canonical_artifact_id
    proposal = _get_proposal(CHAIN_TENANT, pid)
    assert proposal["status"] == "approved", proposal["status"]
    assert proposal["canonical_artifact_id"] is not None

    # Both step traces must exist
    all_traces = _get_traces(CHAIN_TENANT, pid)
    approve_traces = [t for t in all_traces if t.get("decision_type") == "approve"]
    assert len(approve_traces) == 2, f"expected 2 approve traces, got {approve_traces}"


def test_step2_approver_must_differ_from_step1():
    """A1 who approved step1 cannot also approve step2 (distinct across steps)."""
    _set_policy(CHAIN_TENANT, require_distinct=True, chain_steps=2)
    pid = _authority_map_proposal(CHAIN_TENANT, "gl", proposer=PROPOSER)

    # Step 1 by A1
    r = _decide(CHAIN_TENANT, pid, "approve", APPROVER1)
    assert r.status_code == 200, r.text

    # A1 tries step 2 → denied
    r = _decide(CHAIN_TENANT, pid, "approve", APPROVER1)
    assert r.status_code == 409, f"expected 409, got {r.status_code}: {r.text}"
    detail = r.json().get("detail", "")
    assert "already approved a prior step" in detail, detail

    # Proposal must still be pending
    proposal = _get_proposal(CHAIN_TENANT, pid)
    assert proposal["status"] == "pending"

    # A denial trace must exist
    all_traces = _get_traces(CHAIN_TENANT, pid)
    denied_traces = [t for t in all_traces if t.get("decision_type") == "denied"]
    assert len(denied_traces) >= 1, f"no denied trace after step2 attempt by A1"


def test_proposer_cannot_approve_any_step_in_chain():
    """Proposer P cannot approve step1 OR step2 when require_distinct=true."""
    _set_policy(CHAIN_TENANT, require_distinct=True, chain_steps=2)
    pid = _authority_map_proposal(CHAIN_TENANT, "revenue.arr", proposer=PROPOSER)

    # P tries step 1 → denied
    r = _decide(CHAIN_TENANT, pid, "approve", PROPOSER)
    assert r.status_code == 409, f"expected 409 at step1, got {r.status_code}"

    # Proposal still pending; A1 takes step 1
    r = _decide(CHAIN_TENANT, pid, "approve", APPROVER1)
    assert r.status_code == 200, r.text

    # P tries step 2 → denied (P in prior_approvers via proposer≠approver check)
    r = _decide(CHAIN_TENANT, pid, "approve", PROPOSER)
    assert r.status_code == 409, f"expected 409 at step2, got {r.status_code}"

    # A2 takes step 2 → succeeds
    r = _decide(CHAIN_TENANT, pid, "approve", APPROVER2)
    assert r.status_code == 200, r.text
    proposal = _get_proposal(CHAIN_TENANT, pid)
    assert proposal["status"] == "approved"


# =============================================================================
# 4. Back-compat: no policy → Gate 3A single-approve behavior unchanged
# =============================================================================

def test_no_policy_single_approve_canonicalizes():
    """A tenant with no policy row single-approves exactly as Gate 3A."""
    pid = _authority_map_proposal(BACK_TENANT, "cloud_spend.cost")

    r = _decide(BACK_TENANT, pid, "approve", "any_approver")
    assert r.status_code == 200, f"back-compat approve failed {r.status_code}: {r.text}"
    d = r.json()
    assert d["decision"] == "approve"
    assert d["canonical_artifact_id"] is not None
    assert d["is_final"] is True
    assert d["step_number"] == 1
    assert d["chain_steps"] == 1

    proposal = _get_proposal(BACK_TENANT, pid)
    assert proposal["status"] == "approved"
    assert proposal["canonical_artifact_id"] is not None


def test_no_policy_reject_leaves_zero_residue():
    """A tenant with no policy: reject leaves zero canonical residue."""
    pid = _authority_map_proposal(BACK_TENANT, "workforce.headcount")

    r = _decide(BACK_TENANT, pid, "reject", "any_approver", note="wrong data")
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["decision"] == "reject"
    assert d["canonical_artifact_id"] is None

    proposal = _get_proposal(BACK_TENANT, pid)
    assert proposal["status"] == "rejected"
    assert proposal["canonical_artifact_id"] is None


# =============================================================================
# 5. Reject mid-chain leaves zero residue
# =============================================================================

def test_reject_mid_chain_zero_residue():
    """Reject at step 1 of a 2-step chain leaves proposal rejected, no canonical."""
    _set_policy(CHAIN_TENANT, require_distinct=True, chain_steps=2)
    pid = _authority_map_proposal(CHAIN_TENANT, "gl.balance", proposer=PROPOSER)

    r = _decide(CHAIN_TENANT, pid, "reject", APPROVER1, note="rejected at step 1")
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["decision"] == "reject"
    assert d["canonical_artifact_id"] is None
    assert d["is_final"] is True  # reject is always final

    proposal = _get_proposal(CHAIN_TENANT, pid)
    assert proposal["status"] == "rejected"
    assert proposal["canonical_artifact_id"] is None

    # No tenant_authority_map entry must exist for this prefix
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM tenant_authority_map WHERE tenant_id = %s AND concept_prefix = %s",
                (CHAIN_TENANT, "gl.balance"),
            )
            row = cur.fetchone()
    assert row is None, "canonical residue found in tenant_authority_map after mid-chain reject"


# =============================================================================
# 6. Proposer=null skips distinct check even when policy requires it
# =============================================================================

def test_null_proposer_skips_distinct_check():
    """When proposer is null, proposer≠approver check is skipped (drift proposals, etc.)."""
    _set_policy(NOPP_TENANT, require_distinct=True, chain_steps=1)
    # No proposer set (automated monitor path)
    pid = _authority_map_proposal(NOPP_TENANT, "cloud_spend.region", proposer=None)

    # The "proposer" identity used by the monitor is None; any approver identity works
    r = _decide(NOPP_TENANT, pid, "approve", "monitor-approver")
    assert r.status_code == 200, f"null-proposer skip failed {r.status_code}: {r.text}"
    d = r.json()
    assert d["canonical_artifact_id"] is not None
