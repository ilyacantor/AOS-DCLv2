"""ProposalStore — data access for the change proposal HITL queue (Gate 3A).

Tables: change_proposals, change_proposal_decisions, tenant_contour, tenant_concept_aliases.
Cross-table writes to: tenant_authority_map, conflict_register (apply-on-approve).

Approval applies the canonical artifact in the SAME TRANSACTION as the status flip.
Rejection records the decision and leaves zero canonical residue.

I1: no field named run_id in any response shape from this store.
I2: tenant_id is required on every call; missing => ValueError (surfaces as 422).
"""

import json
import uuid
from typing import Any, Optional

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_VALID_PROPOSAL_TYPES = frozenset({
    "authority_map", "conflict_candidate", "vocabulary_alias",
    "org_hierarchy", "management_overlay", "priority_query",
    "structural_drift",   # Gate 3B D1 — scheduled structural drift detector
    "value_drift",        # Gate 3B D2 — scheduled value conflict sweep
})

_PROPOSAL_COLS = (
    "proposal_id, tenant_id, entity_id, proposal_type, natural_key, "
    "payload, confidence, provenance, status, created_at, decided_at, "
    "decided_by, decision_note, canonical_artifact_id"
)


def _natural_key(proposal_type: str, payload: dict) -> str:
    """Derive the natural key for duplicate detection per proposal_type."""
    if proposal_type == "authority_map":
        return str(payload.get("concept_prefix", "")).strip().lower()
    if proposal_type == "conflict_candidate":
        concept = payload.get("concept", "")
        prop = payload.get("property", "")
        period = payload.get("period", "")
        return f"{concept}|{prop}|{period}".lower()
    if proposal_type == "vocabulary_alias":
        return str(payload.get("alias", "")).strip().lower()
    if proposal_type == "org_hierarchy":
        return str(payload.get("dimension", "")).strip().lower()
    if proposal_type == "management_overlay":
        return str(payload.get("board_segment", "")).strip().lower()
    if proposal_type == "priority_query":
        return str(payload.get("query_label", "")).strip().lower()
    if proposal_type == "structural_drift":
        # Identifies the drift detection between a specific base→compare run pair
        # for one entity. Same entity + same run pair = same pending drift = dedup.
        entity = str(payload.get("entity_id", "")).strip().lower()
        base = str(payload.get("dcl_ingest_id_base", "")).strip().lower()
        compare = str(payload.get("dcl_ingest_id_compare", "")).strip().lower()
        return f"{entity}|{base}|{compare}"
    if proposal_type == "value_drift":
        # Identifies a specific value conflict by entity·concept·property·period.
        # Stable across re-detections (same coord = same pending conflict = dedup).
        entity = str(payload.get("entity_id", "")).strip().lower()
        concept = str(payload.get("concept", "")).strip().lower()
        prop = str(payload.get("property", "")).strip().lower()
        period = str(payload.get("period") or "").strip().lower()
        return f"{entity}|{concept}|{prop}|{period}"
    raise ValueError(f"Unknown proposal_type for natural key: {proposal_type!r}")


def _row_to_proposal(row: tuple, cols: list[str]) -> dict:
    d = dict(zip(cols, row))
    d["proposal_id"] = str(d["proposal_id"])
    d["tenant_id"] = str(d["tenant_id"])
    for ts in ("created_at", "decided_at"):
        if d.get(ts) is not None:
            d[ts] = d[ts].isoformat()
    return d


class ProposalStore:

    # ── proposal intake ──────────────────────────────────────────────────────

    def check_duplicates(
        self, tenant_id: str, items: list[tuple[str, str]],
        statuses: tuple[str, ...] = ("pending",),
    ) -> dict[tuple[str, str], Optional[str]]:
        """For each (proposal_type, natural_key) pair return an existing
        proposal_id (in one of `statuses`), else None. One query, no per-row
        round trips.

        Default ('pending',) = "is this already queued". Callers that must not
        re-raise a RESOLVED item pass ('pending','approved') — e.g. the
        value_drift sweep: once a conflict's natural_key is approved
        (dispositioned/canonicalized), the SAME key must not re-drift on the
        next sweep (a resolved conflict re-proposing itself is noise; a new
        period yields a new natural_key and still proposes). Rejected is
        deliberately excluded — an operator decline does not bar re-detection."""
        if not items:
            return {}
        # Most-recent match wins per key (ORDER BY created_at) so an approved
        # row is reported even if an older rejected one shares the key.
        sql = """
            SELECT DISTINCT ON (proposal_type, natural_key)
                   proposal_type, natural_key, proposal_id
            FROM change_proposals
            WHERE tenant_id = %s
              AND status = ANY(%s)
              AND (proposal_type, natural_key) IN %s
            ORDER BY proposal_type, natural_key, created_at DESC
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (tenant_id, list(statuses),
                     tuple((pt, nk) for pt, nk in items)),
                )
                rows = cur.fetchall()

        result: dict[tuple[str, str], Optional[str]] = {k: None for k in items}
        for ptype, nkey, pid in rows:
            result[(ptype, nkey)] = str(pid)
        return result

    def insert_proposals(self, rows: list[dict]) -> list[dict]:
        """Insert validated proposals. Returns inserted rows in order.
        Caller has already done duplicate check; no ON CONFLICT here (per A1).
        Each row may carry an optional 'proposer' field (Gate 3C D2) — the
        identity that created the proposal. NULL for automated monitors."""
        if not rows:
            return []
        inserted = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO change_proposals
                            (tenant_id, entity_id, proposal_type, natural_key,
                             payload, confidence, provenance, status, proposer)
                        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, 'pending', %s)
                        RETURNING proposal_id, created_at
                        """,
                        (r["tenant_id"], r.get("entity_id"), r["proposal_type"],
                         r["natural_key"], json.dumps(r["payload"]),
                         r["confidence"], json.dumps(r["provenance"]),
                         r.get("proposer")),
                    )
                    pid, created_at = cur.fetchone()
                    inserted.append({
                        **r,
                        "proposal_id": str(pid),
                        "status": "pending",
                        "created_at": created_at.isoformat(),
                    })
                conn.commit()
        return inserted

    # ── proposal reads ────────────────────────────────────────────────────────

    def list_proposals(
        self, tenant_id: str, *,
        entity_id: Optional[str] = None,
        status: Optional[str] = None,
        proposal_type: Optional[str] = None,
        limit: int = 100, offset: int = 0,
    ) -> tuple[list[dict], int]:
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        # entity_id is an OPTIONAL filter. The operator UI panel is
        # entity-scoped (it passes the selected snapshot's entity_id), and
        # multiple entities share one tenant — without this filter an
        # "entity-scoped" panel shows the whole tenant's proposals
        # (cross-entity contamination; caught by the Gate 3B D3 e2e where a
        # value_drift entity's panel surfaced a sibling entity's
        # structural_drift). When omitted, the call stays tenant-wide
        # (back-compat for tenant-grain callers).
        if entity_id:
            # Entity-scoped view = THIS entity's proposals PLUS tenant-grain
            # proposals (entity_id IS NULL — authority_map / vocabulary_alias /
            # org_hierarchy / management_overlay / priority_query apply to the
            # whole tenant, not one entity, so they must show under any entity
            # selection). Entity-specific proposals (drift, conflict candidates)
            # of OTHER entities stay hidden — the contamination fix holds.
            clauses.append("(entity_id = %s OR entity_id IS NULL)")
            params.append(entity_id)
        if status:
            clauses.append("status = %s")
            params.append(status)
        if proposal_type:
            clauses.append("proposal_type = %s")
            params.append(proposal_type)
        where = " AND ".join(clauses)
        safe_limit = max(1, min(int(limit), 500))
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM change_proposals WHERE {where}", params
                )
                total = cur.fetchone()[0]
                cur.execute(
                    f"SELECT {_PROPOSAL_COLS} FROM change_proposals "
                    f"WHERE {where} ORDER BY created_at DESC, proposal_id DESC "
                    f"LIMIT %s OFFSET %s",
                    params + [safe_limit, max(0, int(offset))],
                )
                cols = [d[0] for d in cur.description]
                rows = [_row_to_proposal(r, cols) for r in cur.fetchall()]
        return rows, total

    def get_proposal(self, tenant_id: str, proposal_id: str) -> Optional[dict]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {_PROPOSAL_COLS} FROM change_proposals "
                    f"WHERE tenant_id = %s AND proposal_id = %s",
                    (tenant_id, proposal_id),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in cur.description]
                return _row_to_proposal(row, cols)

    # ── decide (approve / reject) ─────────────────────────────────────────────

    def get_approval_policy(self, tenant_id: str) -> dict:
        """Return the approval policy for a tenant, or defaults if none configured."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT require_distinct_proposer_approver, chain_steps "
                    "FROM tenant_approval_policy WHERE tenant_id = %s::uuid",
                    (tenant_id,),
                )
                row = cur.fetchone()
        if row is None:
            return {
                "tenant_id": tenant_id,
                "require_distinct_proposer_approver": False,
                "chain_steps": 1,
                "policy_source": "default",
            }
        require_distinct, chain_steps = row
        return {
            "tenant_id": tenant_id,
            "require_distinct_proposer_approver": bool(require_distinct),
            "chain_steps": chain_steps,
            "policy_source": "configured",
        }

    def set_approval_policy(
        self,
        tenant_id: str,
        require_distinct_proposer_approver: bool,
        chain_steps: int,
    ) -> dict:
        """Upsert the approval policy for a tenant."""
        if chain_steps < 1:
            raise ValueError(
                f"chain_steps must be >= 1; got {chain_steps}"
            )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tenant_approval_policy
                        (tenant_id, require_distinct_proposer_approver, chain_steps, updated_at)
                    VALUES (%s::uuid, %s, %s, now())
                    ON CONFLICT (tenant_id) DO UPDATE SET
                        require_distinct_proposer_approver = EXCLUDED.require_distinct_proposer_approver,
                        chain_steps = EXCLUDED.chain_steps,
                        updated_at  = now()
                    RETURNING updated_at
                    """,
                    (tenant_id, require_distinct_proposer_approver, chain_steps),
                )
                (updated_at,) = cur.fetchone()
            conn.commit()
        return {
            "tenant_id": tenant_id,
            "require_distinct_proposer_approver": require_distinct_proposer_approver,
            "chain_steps": chain_steps,
            "updated_at": updated_at.isoformat(),
        }

    def decide_proposal(
        self, *,
        proposal_id: str,
        tenant_id: str,
        decision: str,
        decided_by: str,
        decision_note: Optional[str],
    ) -> dict:
        """Flip status + apply canonical (if approve) + write decision trace.
        ALL in one transaction. Returns the decision record.

        Gate 3C D2 — chain enforcement:
        - Reads tenant_approval_policy for require_distinct_proposer_approver + chain_steps.
        - If require_distinct and proposer is known and decided_by == proposer → denied.
        - For chain_steps > 1: intermediate steps increment steps_approved, keep
          status pending, write a trace with step_number; only the final step
          canonicalizes. Distinct-approver check applies across ALL prior step approvers.
        - Denial writes a trace (decision='denied') and commits before raising ValueError
          so the denial is visible via GET /api/dcl/traces. Proposal stays pending.

        Back-compat (no policy or defaults): behaves identically to Gate 3A.
        """
        denial_error: Optional[str] = None
        result: Optional[dict] = None

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Lock the proposal row; verify it's pending.
                cur.execute(
                    "SELECT status, proposal_type, payload, entity_id, "
                    "       confidence, provenance, proposer, steps_approved "
                    "FROM change_proposals "
                    "WHERE proposal_id = %s AND tenant_id = %s FOR UPDATE",
                    (proposal_id, tenant_id),
                )
                row = cur.fetchone()
                if row is None:
                    raise LookupError(
                        f"Proposal {proposal_id} not found for tenant {tenant_id} — "
                        f"check GET /api/dcl/proposals for this tenant."
                    )
                (current_status, ptype, payload, entity_id,
                 confidence, provenance, proposer, steps_approved) = row

                if current_status != "pending":
                    raise ValueError(
                        f"Proposal {proposal_id} is already {current_status!r} — "
                        f"a proposal can be decided only once."
                    )

                # Load approval policy (defaults if absent).
                cur.execute(
                    "SELECT require_distinct_proposer_approver, chain_steps "
                    "FROM tenant_approval_policy WHERE tenant_id = %s::uuid",
                    (tenant_id,),
                )
                policy_row = cur.fetchone()
                require_distinct = bool(policy_row[0]) if policy_row else False
                chain_steps = int(policy_row[1]) if policy_row else 1

                step_number = steps_approved + 1  # the step being attempted

                # ── Proposer≠approver check ───────────────────────────────────
                if decision == "approve" and require_distinct and proposer:
                    if decided_by == proposer:
                        denial_reason = (
                            f"Proposer {proposer!r} cannot approve their own proposal "
                            f"(policy: require_distinct_proposer_approver=true) — "
                            f"submit the decision under a distinct approver identity."
                        )
                        cur.execute(
                            """
                            INSERT INTO change_proposal_decisions
                                (tenant_id, entity_id, proposal_id, proposal_type,
                                 decision, decided_by, decision_note, payload,
                                 canonical_artifact_id, step_number)
                            VALUES (%s, %s, %s, %s, 'denied', %s, %s, %s::jsonb, NULL, %s)
                            """,
                            (tenant_id, entity_id, proposal_id, ptype,
                             decided_by, denial_reason,
                             json.dumps(payload) if payload else None,
                             step_number),
                        )
                        conn.commit()
                        denial_error = denial_reason
                        # Fall through to raise after the with-block

                # ── Distinct-approver across prior steps (multi-step chains) ──
                if denial_error is None and decision == "approve" and chain_steps > 1:
                    cur.execute(
                        "SELECT decided_by FROM change_proposal_decisions "
                        "WHERE proposal_id = %s AND decision = 'approve'",
                        (proposal_id,),
                    )
                    prior_approvers = {r[0] for r in cur.fetchall()}
                    if decided_by in prior_approvers:
                        denial_reason = (
                            f"Approver {decided_by!r} has already approved a prior step "
                            f"for proposal {proposal_id} — each chain step requires a "
                            f"distinct approver identity."
                        )
                        cur.execute(
                            """
                            INSERT INTO change_proposal_decisions
                                (tenant_id, entity_id, proposal_id, proposal_type,
                                 decision, decided_by, decision_note, payload,
                                 canonical_artifact_id, step_number)
                            VALUES (%s, %s, %s, %s, 'denied', %s, %s, %s::jsonb, NULL, %s)
                            """,
                            (tenant_id, entity_id, proposal_id, ptype,
                             decided_by, denial_reason,
                             json.dumps(payload) if payload else None,
                             step_number),
                        )
                        conn.commit()
                        denial_error = denial_reason

                if denial_error is None:
                    # ── Normal flow: approve (step or final) or reject ────────
                    is_final_step = (decision == "reject") or (step_number >= chain_steps)
                    canonical_artifact_id: Optional[str] = None

                    if decision == "approve" and is_final_step:
                        canonical_artifact_id = _apply_canonical(
                            cur, tenant_id=tenant_id, entity_id=entity_id,
                            proposal_id=proposal_id, proposal_type=ptype, payload=payload,
                        )
                        cur.execute(
                            """
                            UPDATE change_proposals
                            SET status = 'approved', decided_at = now(), decided_by = %s,
                                decision_note = %s, canonical_artifact_id = %s,
                                steps_approved = %s
                            WHERE proposal_id = %s
                            """,
                            (decided_by, decision_note, canonical_artifact_id,
                             step_number, proposal_id),
                        )
                    elif decision == "approve" and not is_final_step:
                        # Intermediate step: advance counter, stay pending.
                        cur.execute(
                            "UPDATE change_proposals SET steps_approved = %s "
                            "WHERE proposal_id = %s",
                            (step_number, proposal_id),
                        )
                    else:
                        # Reject at any step: flip to rejected, zero canonical residue.
                        cur.execute(
                            """
                            UPDATE change_proposals
                            SET status = 'rejected', decided_at = now(), decided_by = %s,
                                decision_note = %s
                            WHERE proposal_id = %s
                            """,
                            (decided_by, decision_note, proposal_id),
                        )

                    cur.execute(
                        """
                        INSERT INTO change_proposal_decisions
                            (tenant_id, entity_id, proposal_id, proposal_type,
                             decision, decided_by, decision_note, payload,
                             canonical_artifact_id, step_number)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                        RETURNING id, decided_at
                        """,
                        (tenant_id, entity_id, proposal_id, ptype, decision,
                         decided_by, decision_note,
                         json.dumps(payload) if payload else None,
                         canonical_artifact_id, step_number),
                    )
                    decision_id, decided_at = cur.fetchone()
                    conn.commit()

                    logger.info(
                        "[proposal-decide] proposal=%s type=%s decision=%s step=%d/%d "
                        "canonical=%s by=%s",
                        proposal_id, ptype, decision, step_number, chain_steps,
                        canonical_artifact_id, decided_by,
                    )
                    result = {
                        "decision_id": str(decision_id),
                        "proposal_id": proposal_id,
                        "proposal_type": ptype,
                        "decision": decision,
                        "step_number": step_number,
                        "chain_steps": chain_steps,
                        "decided_by": decided_by,
                        "decision_note": decision_note,
                        "decided_at": decided_at.isoformat(),
                        "canonical_artifact_id": canonical_artifact_id,
                        "is_final": is_final_step,
                    }

        if denial_error:
            raise ValueError(denial_error)
        return result  # type: ignore[return-value]

    # ── contour reads ─────────────────────────────────────────────────────────

    def get_tenant_contour(self, tenant_id: str) -> Optional[dict]:
        """Return the raw tenant_contour row or None if no approved contour exists."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT hierarchy, management_overlay, priority_queries, "
                    "proposal_ids, updated_at "
                    "FROM tenant_contour WHERE tenant_id = %s",
                    (tenant_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        hierarchy, management_overlay, priority_queries, pids, updated_at = row
        return {
            "hierarchy": hierarchy or {},
            "management_overlay": management_overlay or [],
            "priority_queries": priority_queries or [],
            "proposal_ids": list(pids or []),
            "updated_at": updated_at.isoformat(),
        }

    def load_approved_contour_for_rebuild(self) -> Optional[dict]:
        """Return the contour dict (hierarchy + management_overlay + sor_authority)
        for the first approved contour in the store, suitable for passing to
        SemanticGraph.load_from_contour_map(contour_data=...).

        Returns None if no approved contour exists (falls back to sample YAML).
        sor_authority is projected from tenant_authority_map — not from tenant_contour —
        to prevent split brain (single source of truth for source authority).
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tenant_id, hierarchy, management_overlay "
                    "FROM tenant_contour ORDER BY updated_at DESC LIMIT 1"
                )
                row = cur.fetchone()
                if row is None:
                    return None
                tenant_id, hierarchy, management_overlay = row

                cur.execute(
                    "SELECT concept_prefix, ranked_sources FROM tenant_authority_map "
                    "WHERE tenant_id = %s OR tenant_id = '*' "
                    "ORDER BY CASE WHEN tenant_id = '*' THEN 0 ELSE 1 END",
                    (str(tenant_id),),
                )
                authority_rows = cur.fetchall()

        sor_authority: dict = {}
        for prefix, sources in authority_rows:
            if sources:
                sor_authority[prefix] = {"system": sources[0], "confidence": 0.9}

        return {
            "hierarchy": hierarchy or {},
            "management_overlay": management_overlay or [],
            "sor_authority": sor_authority,
        }

    # ── vocabulary alias lookup ───────────────────────────────────────────────

    def resolve_concept_alias(
        self, tenant_id: str, alias: str,
    ) -> Optional[dict]:
        """Return the concept_id for a tenant-scoped vocabulary alias, or None."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT concept_id, alias, proposal_id, created_at "
                    "FROM tenant_concept_aliases "
                    "WHERE tenant_id = %s AND lower(alias) = lower(%s)",
                    (tenant_id, alias),
                )
                row = cur.fetchone()
        if row is None:
            return None
        concept_id, matched_alias, proposal_id, created_at = row
        return {
            "concept_id": concept_id,
            "alias": matched_alias,
            "proposal_id": str(proposal_id),
            "created_at": created_at.isoformat(),
        }


# =============================================================================
# Apply-on-approve dispatch (runs inside the decide_proposal transaction)
# =============================================================================

def _apply_canonical(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, proposal_type: str, payload: dict,
) -> str:
    """Write the canonical artifact for an approved proposal.
    Runs inside an open transaction — no commit here.
    Returns canonical_artifact_id (a string reference suitable for the proposals row)."""
    dispatch = {
        "authority_map":      _apply_authority_map,
        "conflict_candidate": _apply_conflict_candidate,
        "vocabulary_alias":   _apply_vocabulary_alias,
        "org_hierarchy":      _apply_org_hierarchy,
        "management_overlay": _apply_management_overlay,
        "priority_query":     _apply_priority_query,
        "structural_drift":   _apply_structural_drift,   # Gate 3B D1: acknowledgment
        "value_drift":        _apply_value_drift,        # Gate 3B D2: disposition via authority map
    }
    fn = dispatch.get(proposal_type)
    if fn is None:
        raise ValueError(f"No apply handler for proposal_type={proposal_type!r}")
    return fn(cur, tenant_id=tenant_id, entity_id=entity_id,
               proposal_id=proposal_id, payload=payload)


def _apply_authority_map(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, payload: dict,
) -> str:
    concept_prefix = payload["concept_prefix"].strip()
    ranked_sources = payload["ranked_sources"]
    cur.execute(
        """
        INSERT INTO tenant_authority_map (tenant_id, concept_prefix, ranked_sources)
        VALUES (%s, %s, %s)
        ON CONFLICT (tenant_id, concept_prefix)
        DO UPDATE SET ranked_sources = EXCLUDED.ranked_sources, updated_at = now()
        """,
        (str(tenant_id), concept_prefix, ranked_sources),
    )
    return f"authority_map:{tenant_id}:{concept_prefix}"


def _apply_conflict_candidate(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, payload: dict,
) -> str:
    eid = entity_id or payload.get("entity_id")
    if not eid:
        raise ValueError(
            "conflict_candidate approval requires entity_id (from proposal entity_id "
            "or payload.entity_id) — cannot register a conflict without an entity."
        )
    source_class = payload.get("source_class", "stakeholder_system")
    cur.execute(
        """
        INSERT INTO conflict_register
            (tenant_id, entity_id, conflict_type, conflict_class, concept,
             property, period, dcl_ingest_id, claims, source_class,
             root_cause_explanation, root_cause_source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::uuid, %s::jsonb, %s, %s, %s)
        ON CONFLICT (tenant_id, entity_id, concept, property, COALESCE(period, ''), dcl_ingest_id)
        DO UPDATE SET
            claims = EXCLUDED.claims,
            source_class = EXCLUDED.source_class,
            root_cause_explanation = EXCLUDED.root_cause_explanation,
            updated_at = now()
        RETURNING id
        """,
        (
            str(tenant_id), eid,
            payload.get("conflict_type", "value"),
            payload.get("conflict_class", "stakeholder_reported"),
            payload["concept"],
            payload.get("property", "amount"),
            payload.get("period"),
            proposal_id,
            json.dumps(payload.get("claims", [])),
            source_class,
            "Stakeholder-identified conflict via proposals",
            "onboarding",
        ),
    )
    row = cur.fetchone()
    conflict_id = str(row[0])
    return f"conflict:{conflict_id}"


def _apply_vocabulary_alias(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, payload: dict,
) -> str:
    concept_id = payload["concept_id"].strip()
    alias = payload["alias"].strip()
    cur.execute(
        """
        INSERT INTO tenant_concept_aliases
            (tenant_id, concept_id, alias, proposal_id)
        VALUES (%s, %s, %s, %s::uuid)
        ON CONFLICT (tenant_id, alias)
        DO UPDATE SET concept_id = EXCLUDED.concept_id,
                      proposal_id = EXCLUDED.proposal_id
        RETURNING id
        """,
        (str(tenant_id), concept_id, alias.lower(), proposal_id),
    )
    row = cur.fetchone()
    return f"concept_alias:{row[0]}"


def _apply_org_hierarchy(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, payload: dict,
) -> str:
    dimension = payload["dimension"].strip()
    roots = payload["roots"]
    cur.execute(
        """
        INSERT INTO tenant_contour (tenant_id, hierarchy, proposal_ids)
        VALUES (%s, %s::jsonb, ARRAY[%s])
        ON CONFLICT (tenant_id) DO UPDATE SET
            hierarchy = tenant_contour.hierarchy || %s::jsonb,
            proposal_ids = array_append(
                array_remove(tenant_contour.proposal_ids, %s), %s
            ),
            updated_at = now()
        """,
        (
            str(tenant_id),
            json.dumps({dimension: roots}),
            proposal_id,
            json.dumps({dimension: roots}),
            proposal_id, proposal_id,
        ),
    )
    return f"contour:hierarchy:{dimension}:{tenant_id}"


def _apply_management_overlay(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, payload: dict,
) -> str:
    board_segment = payload["board_segment"].strip()
    maps_to = payload["maps_to"]
    new_entry = json.dumps({"board_segment": board_segment, "maps_to": maps_to})
    cur.execute(
        """
        INSERT INTO tenant_contour (tenant_id, management_overlay, proposal_ids)
        VALUES (%s, %s::jsonb, ARRAY[%s])
        ON CONFLICT (tenant_id) DO UPDATE SET
            management_overlay = (
                SELECT jsonb_agg(elem)
                FROM (
                    SELECT elem FROM jsonb_array_elements(tenant_contour.management_overlay) elem
                    WHERE elem->>'board_segment' != %s
                    UNION ALL
                    SELECT %s::jsonb
                ) combined
            ),
            proposal_ids = array_append(
                array_remove(tenant_contour.proposal_ids, %s), %s
            ),
            updated_at = now()
        """,
        (
            str(tenant_id),
            json.dumps([{"board_segment": board_segment, "maps_to": maps_to}]),
            proposal_id,
            board_segment,
            new_entry,
            proposal_id, proposal_id,
        ),
    )
    return f"contour:overlay:{board_segment}:{tenant_id}"


def _apply_priority_query(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, payload: dict,
) -> str:
    query_label = payload.get("query_label", "").strip()
    query_entry = json.dumps(payload)
    cur.execute(
        """
        INSERT INTO tenant_contour (tenant_id, priority_queries, proposal_ids)
        VALUES (%s, %s::jsonb, ARRAY[%s])
        ON CONFLICT (tenant_id) DO UPDATE SET
            priority_queries = (
                SELECT jsonb_agg(elem)
                FROM (
                    SELECT elem FROM jsonb_array_elements(tenant_contour.priority_queries) elem
                    WHERE elem->>'query_label' != %s
                    UNION ALL
                    SELECT %s::jsonb
                ) combined
            ),
            proposal_ids = array_append(
                array_remove(tenant_contour.proposal_ids, %s), %s
            ),
            updated_at = now()
        """,
        (
            str(tenant_id),
            json.dumps([payload]),
            proposal_id,
            query_label,
            query_entry,
            proposal_id, proposal_id,
        ),
    )
    return f"contour:priority_query:{query_label}:{tenant_id}"


def _apply_structural_drift(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, payload: dict,
) -> str:
    """Structural drift APPROVE = operator acknowledgment of schema evolution.

    The graph already reflects reality — new/removed fields exist or not in
    the current run's triples.  No store write is needed beyond what
    decide_proposal already writes (status flip + decision trace).  The
    canonical_artifact_id names the acknowledged delta so GET /api/dcl/traces
    can surface the full loop.

    A6/A2: no speculative structural-baseline table built here — if
    acknowledgment needs durable state to prevent re-drift, that is a separate
    design decision (surface to governor rather than build silently).
    """
    eid = entity_id or payload.get("entity_id", "unknown")
    compare_run = payload.get("dcl_ingest_id_compare", "unknown")
    return f"structural_drift:ack:{eid}:{compare_run}"


def _apply_value_drift(
    cur, *, tenant_id: str, entity_id: Optional[str],
    proposal_id: str, payload: dict,
) -> str:
    """Value drift APPROVE = resolve the underlying conflict via the authority map.

    Routes through ConflictStore.record_disposition_in_txn on the SHARED cursor
    so the disposition, triple supersession, and proposal status flip are all
    one atomic transaction.  Same supersession SQL as the HITL disposition route
    (conflicts.py POST /{id}/disposition) — no duplication of that logic.

    Authority-map winner determination: longest-prefix match on concept prefix,
    first source in the ranked list that appears in the conflict's claims wins.
    If no authority map entry covers the concept, the conflict is escalated
    (operator review required) rather than silently ignoring the gap.
    """
    from backend.db.conflict_store import ConflictStore

    conflict_id = payload.get("conflict_id")
    if not conflict_id:
        raise ValueError(
            f"value_drift proposal {proposal_id} payload missing conflict_id — "
            "cannot disposition the conflict without it."
        )
    eid = entity_id or payload.get("entity_id")
    if not eid:
        raise ValueError(
            f"value_drift proposal {proposal_id} requires entity_id — "
            "cannot write disposition without entity."
        )

    claims = payload.get("claims", [])
    sources = [c["source_system"] for c in claims]
    concept = payload.get("concept", "")
    conflict_class = payload.get("conflict_class", "")

    conflict_store = ConflictStore()
    amap = conflict_store.load_authority_map(tenant_id)

    best_prefix_len = -1
    ranked: Optional[list] = None
    for prefix, sources_ranked in amap.items():
        if concept.startswith(prefix) and len(prefix) > best_prefix_len:
            best_prefix_len = len(prefix)
            ranked = sources_ranked

    winner_source: Optional[str] = None
    if ranked:
        for src in ranked:
            if src in sources:
                winner_source = src
                break

    if winner_source:
        action = "accept_a" if (sources and sources[0] == winner_source) else "accept_b"
        loser_sources = [s for s in sources if s != winner_source]
        superseded_triple_ids = [
            c["triple_id"] for c in claims
            if c["source_system"] != winner_source and c.get("triple_id")
        ]
        new_status = "dispositioned"
        rationale = (
            f"Value drift proposal approved — authority-map winner: {winner_source!r}. "
            f"Sources in conflict: {sources}."
        )
    else:
        action = "escalate"
        loser_sources = []
        superseded_triple_ids = []
        new_status = "escalated"
        rationale = (
            f"Value drift proposal approved — no authority map entry for concept "
            f"{concept!r}; escalated for operator review. Sources: {sources}."
        )

    disp = conflict_store.record_disposition_in_txn(
        cur,
        conflict_id=conflict_id,
        tenant_id=tenant_id,
        entity_id=eid,
        conflict_class=conflict_class,
        action=action,
        winner_source=winner_source,
        loser_sources=loser_sources,
        superseded_triple_ids=superseded_triple_ids,
        decided_by=f"value_drift_approve:proposal:{proposal_id}",
        rationale=rationale,
        context={"proposal_id": proposal_id, "claims": claims,
                 "trend": payload.get("trend")},
        new_status=new_status,
    )
    return f"conflict_disposition:{disp['disposition_id']}"
