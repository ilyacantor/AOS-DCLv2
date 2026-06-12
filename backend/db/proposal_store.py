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
    ) -> dict[tuple[str, str], Optional[str]]:
        """For each (proposal_type, natural_key) pair return the pending proposal_id
        if one already exists, else None. One query, no per-row round trips."""
        if not items:
            return {}
        values_clause = ",".join(
            f"('{tenant_id}'::uuid, {i}, %s, %s)"
            for i, _ in enumerate(items)
        )
        params: list[Any] = []
        for ptype, nkey in items:
            params.extend([ptype, nkey])

        sql = """
            SELECT proposal_type, natural_key, proposal_id
            FROM change_proposals
            WHERE tenant_id = %s
              AND status = 'pending'
              AND (proposal_type, natural_key) IN %s
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, tuple((pt, nk) for pt, nk in items)))
                rows = cur.fetchall()

        result: dict[tuple[str, str], Optional[str]] = {k: None for k in items}
        for ptype, nkey, pid in rows:
            result[(ptype, nkey)] = str(pid)
        return result

    def insert_proposals(self, rows: list[dict]) -> list[dict]:
        """Insert validated proposals. Returns inserted rows in order.
        Caller has already done duplicate check; no ON CONFLICT here (per A1)."""
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
                             payload, confidence, provenance, status)
                        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, 'pending')
                        RETURNING proposal_id, created_at
                        """,
                        (r["tenant_id"], r.get("entity_id"), r["proposal_type"],
                         r["natural_key"], json.dumps(r["payload"]),
                         r["confidence"], json.dumps(r["provenance"])),
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
        status: Optional[str] = None,
        proposal_type: Optional[str] = None,
        limit: int = 100, offset: int = 0,
    ) -> tuple[list[dict], int]:
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
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

        approval applies the canonical artifact; rejection writes zero canonical
        residue — the only DB writes are the status flip and the decision trace row.
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Lock the proposal row; verify it's pending.
                cur.execute(
                    "SELECT status, proposal_type, payload, entity_id, confidence, provenance "
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
                current_status, ptype, payload, entity_id, confidence, provenance = row
                if current_status != "pending":
                    raise ValueError(
                        f"Proposal {proposal_id} is already {current_status!r} — "
                        f"a proposal can be decided only once."
                    )

                canonical_artifact_id: Optional[str] = None

                if decision == "approve":
                    canonical_artifact_id = _apply_canonical(
                        cur, tenant_id=tenant_id, entity_id=entity_id,
                        proposal_id=proposal_id, proposal_type=ptype, payload=payload,
                    )

                _STATUS = {"approve": "approved", "reject": "rejected"}
                cur.execute(
                    """
                    UPDATE change_proposals
                    SET status = %s, decided_at = now(), decided_by = %s,
                        decision_note = %s, canonical_artifact_id = %s
                    WHERE proposal_id = %s
                    """,
                    (_STATUS[decision], decided_by, decision_note,
                     canonical_artifact_id, proposal_id),
                )

                cur.execute(
                    """
                    INSERT INTO change_proposal_decisions
                        (tenant_id, entity_id, proposal_id, proposal_type,
                         decision, decided_by, decision_note, payload, canonical_artifact_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    RETURNING id, decided_at
                    """,
                    (tenant_id, entity_id, proposal_id, ptype, decision,
                     decided_by, decision_note,
                     json.dumps(payload) if payload else None,
                     canonical_artifact_id),
                )
                decision_id, decided_at = cur.fetchone()

            conn.commit()

        logger.info(
            "[proposal-decide] proposal=%s type=%s decision=%s canonical=%s by=%s",
            proposal_id, ptype, decision, canonical_artifact_id, decided_by,
        )
        return {
            "decision_id": str(decision_id),
            "proposal_id": proposal_id,
            "proposal_type": ptype,
            "decision": decision,
            "decided_by": decided_by,
            "decision_note": decision_note,
            "decided_at": decided_at.isoformat(),
            "canonical_artifact_id": canonical_artifact_id,
        }

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
