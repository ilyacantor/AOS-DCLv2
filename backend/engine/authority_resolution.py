"""Authority resolution (Stage 5, ContextOS §8) — turn a value-conflict into ONE
decisive value plus disclosure of who disagreed and by how much.

This is the Gate-1 surface: conflicting sources resolve to a single decisive
value WITH disclosure, via the per-tenant authority map. The recommendation
(precedent > authority > escalate) is computed upstream in
backend/engine/conflict_detection._recommend and stored on the register row's
`recommended`. This module reads that recommendation and the conflict's claims
and emits the operator-facing resolution shape — it does not re-derive the
winner.

Two authority decisions seeded for the ContextOS demo tenant
(51aee6ec-15c3-4fb0-833a-a19bb4511296), documented here because they are the
arbitration rules the demo turns on:

  cloud_spend -> [aws_billing, netsuite_gl_allocation]
      The provider invoice (aws_billing) is the ACTUAL CHARGE. A GL allocation
      (netsuite_gl_allocation) is an internal cost-spread estimate. The invoice
      is authoritative over the allocation.

  headcount / workforce -> [workday_hr, netsuite_finance_rollup]
      The HRIS (workday_hr) is the SYSTEM OF RECORD for who is employed. A
      finance payroll rollup (netsuite_finance_rollup) can lag, double-count
      contractors, or bucket differently. HRIS is authoritative.

No silent fallback (A1): when the recommendation is escalate (no precedent, no
authority match), resolution returns status="escalated" with decisive_value
None and EVERY claim disclosed. It never picks a winner to look resolved.
"""

from __future__ import annotations

from typing import Any, Optional

# Recommendation bases that name a decisive winner. 'none' (escalate) does not.
_DECISIVE_BASES = {"authority", "precedent"}


def _claim_value(claim: dict) -> Any:
    """A value-conflict claim carries a scalar `value`. Per-record-summary
    claims (structural, ledger detail) carry `row_count` instead and have no
    single value — those are not value-resolvable and surface as escalated."""
    return claim.get("value")


def _disclose(claim: dict) -> dict:
    """One disclosure row: the source and what it claimed."""
    return {"source_system": claim.get("source_system"), "value": _claim_value(claim)}


def resolve_conflict(conflict: dict) -> dict:
    """Resolve one register conflict to a decisive value + disclosure.

    Resolved (authority/precedent names a winner whose claim has a scalar
    value):
        {decisive_value, decisive_source, basis, root_cause,
         disclosed: [{source_system, value} for the LOSING claims],
         gap_abs: max-min over numeric claims (or None), status: "resolved"}

    Escalated (no decisive basis, or the named winner has no scalar value —
    e.g. a structural / per-record group): NO silent pick.
        {decisive_value: None, decisive_source, basis, root_cause,
         disclosed: [ALL claims], gap_abs, status: "escalated"}
    """
    claims: list[dict] = conflict.get("claims") or []
    recommended: dict = conflict.get("recommended") or {}
    basis = recommended.get("basis")
    winner_source = recommended.get("winner_source")
    root_cause = conflict.get("root_cause_explanation")

    # gap_abs over numeric scalar claims only (disclosure of the spread).
    numeric_vals = [
        float(_claim_value(c))
        for c in claims
        if isinstance(_claim_value(c), (int, float))
        and not isinstance(_claim_value(c), bool)
    ]
    gap_abs = round(max(numeric_vals) - min(numeric_vals), 10) if len(numeric_vals) >= 2 else None

    decisive_basis_named = basis in _DECISIVE_BASES and winner_source is not None
    winner_claim: Optional[dict] = None
    if decisive_basis_named:
        for c in claims:
            if c.get("source_system") == winner_source:
                winner_claim = c
                break

    # Resolve only when a winner is named AND that winner carries a scalar
    # value (a value conflict). A named winner with no scalar value (structural
    # per-record group) cannot produce a decisive number — escalate, loud.
    winner_value = _claim_value(winner_claim) if winner_claim is not None else None
    resolvable = winner_claim is not None and winner_value is not None

    if not resolvable:
        # Escalated — disclose EVERY claim, pick nothing (A1).
        return {
            "decisive_value": None,
            "decisive_source": winner_source if decisive_basis_named else None,
            "basis": basis,
            "root_cause": root_cause,
            "disclosed": [_disclose(c) for c in claims],
            "gap_abs": gap_abs,
            "status": "escalated",
        }

    disclosed = [_disclose(c) for c in claims if c.get("source_system") != winner_source]
    return {
        "decisive_value": winner_value,
        "decisive_source": winner_source,
        "basis": basis,
        "root_cause": root_cause,
        "disclosed": disclosed,
        "gap_abs": gap_abs,
        "status": "resolved",
    }
