"""RuleStore — standing_rules + standing_rule_provenance (Gate 2A,
ContextOS §9, migration 021).

Rules are promoted from repeated decisions. Promotion is PROPOSAL-ONLY
without approval, EVER: rows are born status='proposed' and the status flips
exactly once (proposed→approved | proposed→rejected), enforced here the same
way resolver_hitl_store.decide refuses to overwrite a finalized decision.
Gate 2A binds NO engine behavior to these rows — this is a provenance-
carrying registry, not a policy engine.

Sync psycopg2, parameterized queries. Identity (tenant_id) required on every
call (I2). Typed errors so routes map loudly: NoQualifyingPatternError → 422,
RuleAlreadyDecidedError → 409, LookupError → 404.
"""

import json
from typing import Any, Optional

from backend.core.db import get_connection
from backend.db.trace_store import TraceStore, _require_tenant
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class NoQualifyingPatternError(ValueError):
    """No recurring disposition pattern meets the recurrence threshold —
    there is nothing to promote. Routes surface as 422."""


class RuleAlreadyDecidedError(ValueError):
    """The rule's status already flipped once — re-decide refused.
    Routes surface as 409."""


_RULE_FIELDS = [
    "rule_id", "tenant_id", "entity_id", "rule_scope", "conflict_class",
    "rule_body", "status", "proposed_by", "proposed_at", "proposal_rationale",
    "decided_by", "decided_at", "decision_rationale", "created_at",
]

_RULE_COLS_BARE = ", ".join(_RULE_FIELDS)
_RULE_COLS = ", ".join(f"r.{f}" for f in _RULE_FIELDS)


def _row_to_rule(row: tuple) -> dict:
    d = dict(zip(_RULE_FIELDS, row))
    d["rule_id"] = str(d["rule_id"])
    d["tenant_id"] = str(d["tenant_id"])
    for k in ("proposed_at", "decided_at", "created_at"):
        if d.get(k) is not None:
            d[k] = d[k].isoformat()
    return d


class RuleStore:

    def propose_rule(
        self,
        tenant_id: str,
        conflict_class: str,
        proposed_by: str,
        *,
        entity_id: Optional[str] = None,
        min_recurrence: int = 2,
    ) -> dict:
        """Promote the tenant's top recurring disposition pattern for one
        conflict class into a PROPOSED standing rule, carrying the exact
        disposition trace_ids that justified it.

        Inspects TraceStore.recurring_disposition_patterns (non-escalate,
        grouped by action+winner_source). If the top pattern's count >=
        min_recurrence, inserts the standing_rules row (status='proposed')
        and one standing_rule_provenance row per justifying disposition —
        ONE transaction, so a rule can never exist without its provenance.
        Raises NoQualifyingPatternError when nothing qualifies.
        """
        tenant = _require_tenant(tenant_id, "propose_rule")
        if not conflict_class or not str(conflict_class).strip():
            raise ValueError("propose_rule requires conflict_class.")
        if not proposed_by or not str(proposed_by).strip():
            raise ValueError(
                "propose_rule requires proposed_by — an unattributed proposal "
                "is not a proposal (decision-trace rule, §9)."
            )
        if int(min_recurrence) < 1:
            raise ValueError(f"min_recurrence must be >= 1; got {min_recurrence}")

        patterns = TraceStore().recurring_disposition_patterns(tenant, conflict_class)
        top = patterns[0] if patterns else None
        if top is None or top["count"] < int(min_recurrence):
            have = top["count"] if top else 0
            raise NoQualifyingPatternError(
                f"No recurring disposition pattern qualifies for tenant {tenant}, "
                f"conflict_class {conflict_class!r}: top pattern count {have} < "
                f"min_recurrence {min_recurrence}. A standing rule needs repeated "
                f"same-class precedent before it can even be proposed."
            )

        rule_body = {"action": top["action"], "winner_source": top["winner_source"]}
        rationale = (
            f"Auto-proposed from {top['count']} recurring "
            f"'{top['action']}' dispositions (winner_source="
            f"{top['winner_source']!r}) on conflict class {conflict_class!r}; "
            f"latest precedent decided at {top['latest_decided_at']}."
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO standing_rules
                        (tenant_id, entity_id, rule_scope, conflict_class,
                         rule_body, status, proposed_by, proposal_rationale)
                    VALUES (%s, %s, 'conflict_class', %s, %s::jsonb, 'proposed', %s, %s)
                    RETURNING {_RULE_COLS_BARE}
                    """,
                    (tenant, entity_id, conflict_class, json.dumps(rule_body),
                     str(proposed_by).strip(), rationale),
                )
                rule = _row_to_rule(cur.fetchone())
                for trace_id in top["trace_ids"]:
                    cur.execute(
                        """
                        INSERT INTO standing_rule_provenance
                            (rule_id, trace_type, trace_id, tenant_id)
                        VALUES (%s, 'conflict_disposition', %s, %s)
                        """,
                        (rule["rule_id"], trace_id, tenant),
                    )
                conn.commit()
        # Sorted by trace_id — same deterministic order the read paths return.
        prov_ids = sorted(top["trace_ids"])
        rule["provenance_trace_ids"] = prov_ids
        rule["provenance"] = [
            {"trace_type": "conflict_disposition", "trace_id": t} for t in prov_ids
        ]
        logger.info(
            "[standing-rule] proposed rule=%s class=%s pattern=%s/%s precedents=%d by=%s",
            rule["rule_id"], conflict_class, top["action"], top["winner_source"],
            top["count"], proposed_by,
        )
        return rule

    def decide_rule(
        self,
        rule_id: str,
        tenant_id: str,
        decision: str,
        decided_by: str,
        decision_rationale: str,
    ) -> dict:
        """Flip a proposed rule to approved/rejected — exactly once.

        Same contract as resolver_hitl_store.decide: a finalized decision is
        never overwritten (RuleAlreadyDecidedError), the row must exist for
        this tenant (LookupError), and the decision must be attributed and
        explained (decided_by + decision_rationale required).
        """
        tenant = _require_tenant(tenant_id, "decide_rule")
        if decision not in ("approved", "rejected"):
            raise ValueError(
                f"decide_rule: decision must be 'approved' or 'rejected'; got {decision!r}"
            )
        if not decided_by or not str(decided_by).strip():
            raise ValueError("decide_rule: decided_by required (audit trail).")
        if not decision_rationale or not str(decision_rationale).strip():
            raise ValueError(
                "decide_rule: decision_rationale required — an unexplained "
                "rule decision is not a decision (§9)."
            )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM standing_rules "
                    "WHERE tenant_id = %s AND rule_id = %s FOR UPDATE",
                    (tenant, rule_id),
                )
                row = cur.fetchone()
                if row is None:
                    raise LookupError(
                        f"standing rule {rule_id} not found for tenant {tenant}."
                    )
                if row[0] != "proposed":
                    raise RuleAlreadyDecidedError(
                        f"standing rule {rule_id} is already {row[0]} — the status "
                        f"flips exactly once (proposed→approved|rejected); refusing "
                        f"to overwrite a finalized decision."
                    )
                cur.execute(
                    f"""
                    UPDATE standing_rules
                    SET status = %s, decided_by = %s, decided_at = now(),
                        decision_rationale = %s
                    WHERE tenant_id = %s AND rule_id = %s
                    RETURNING {_RULE_COLS_BARE}
                    """,
                    (decision, str(decided_by).strip(),
                     str(decision_rationale).strip(), tenant, rule_id),
                )
                rule = _row_to_rule(cur.fetchone())
                conn.commit()
        rule.update(self._provenance_for(tenant, rule["rule_id"]))
        logger.info(
            "[standing-rule] decided rule=%s decision=%s by=%s",
            rule_id, decision, decided_by,
        )
        return rule

    def list_rules(
        self,
        tenant_id: str,
        *,
        status: Optional[str] = None,
        conflict_class: Optional[str] = None,
    ) -> list[dict]:
        """Rules for a tenant (newest proposal first), each carrying its
        provenance trace ids."""
        tenant = _require_tenant(tenant_id, "list_rules")
        clauses = ["r.tenant_id = %s"]
        params: list[Any] = [tenant]
        if status is not None:
            clauses.append("r.status = %s")
            params.append(status)
        if conflict_class is not None:
            clauses.append("r.conflict_class = %s")
            params.append(conflict_class)
        sql = f"""
            SELECT {_RULE_COLS},
                   COALESCE(
                       json_agg(json_build_object(
                           'trace_type', p.trace_type,
                           'trace_id', p.trace_id::text
                       ) ORDER BY p.trace_id) FILTER (WHERE p.trace_id IS NOT NULL),
                       '[]'::json
                   ) AS provenance
            FROM standing_rules r
            LEFT JOIN standing_rule_provenance p ON p.rule_id = r.rule_id
            WHERE {' AND '.join(clauses)}
            GROUP BY {_RULE_COLS}
            ORDER BY r.proposed_at DESC, r.rule_id DESC
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        out = []
        for r in rows:
            rule = _row_to_rule(r[: len(_RULE_FIELDS)])
            prov = r[len(_RULE_FIELDS)]
            rule["provenance"] = prov
            rule["provenance_trace_ids"] = [p["trace_id"] for p in prov]
            out.append(rule)
        return out

    def get_rule(self, rule_id: str, tenant_id: str) -> Optional[dict]:
        """One rule + its provenance, tenant-scoped. None when absent."""
        tenant = _require_tenant(tenant_id, "get_rule")
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {_RULE_COLS} FROM standing_rules r "
                    f"WHERE r.tenant_id = %s AND r.rule_id = %s",
                    (tenant, rule_id),
                )
                row = cur.fetchone()
                if row is None:
                    return None
        rule = _row_to_rule(row)
        rule.update(self._provenance_for(tenant, rule["rule_id"]))
        return rule

    def _provenance_for(self, tenant_id: str, rule_id: str) -> dict:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT trace_type, trace_id::text FROM standing_rule_provenance "
                    "WHERE tenant_id = %s AND rule_id = %s ORDER BY trace_id",
                    (tenant_id, rule_id),
                )
                rows = cur.fetchall()
        return {
            "provenance": [{"trace_type": r[0], "trace_id": r[1]} for r in rows],
            "provenance_trace_ids": [r[1] for r in rows],
        }
