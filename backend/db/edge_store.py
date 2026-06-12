"""EdgeStore — data access for the entity_edges table (ContextOS Gate 1B, §7).

Sync psycopg2, parameterized queries, no business logic beyond the edge-type
constraint rules (cardinality + allowed node-type pairs), which are enforced
HERE — at the persistence boundary — so no write path can bypass them.

Bi-temporal (Temporal Columns v1, SCHEMA_CONTRACT): every edge carries
valid_from/valid_to (world time) and ingested_at/superseded_at (knowledge
time). Lifecycle writes CLOSE an edge's knowledge window (SET superseded_at)
— they never delete. is_active is STORED GENERATED (superseded_at IS NULL).
The one DELETE is the same-run redelivery scrub inside replace mode, mirror
of TripleStore.replace_tenant_triples.

Constraint violations are EXCLUDED from the graph and FLAGGED into the
conflict register's structural class (migration 018) in the same
transaction — never silently dropped (Blueprint §7). Identity (tenant_id
UUID + entity_id) is required on every call: missing ⇒ EdgeIdentityError,
which routes surface as 422 (I2, no fallback).
"""

import json
import uuid as _uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from backend.core.db import get_connection
from backend.core.constants import INGEST_STATEMENT_TIMEOUT_MS
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Wildcard tenant for built-in edge types (same convention as tenant_authority_map).
_BUILTIN_TENANT = "*"

_EDGE_COLS = [
    "tenant_id", "entity_id",
    "src_type", "src_key", "edge_type", "dst_type", "dst_key",
    "properties",
    "source_system", "source_table", "source_field",
    "pipe_id", "run_id", "source_run_tag",
    "confidence_score", "confidence_tier",
    "fabric_plane", "fabric_product",
    "derivation", "valid_from",
]


class EdgeIdentityError(ValueError):
    """Missing/invalid identity pair at the edge persistence boundary (I2)."""


class EdgeContractError(ValueError):
    """Edge payload violates the column contract (not a constraint-rule case)."""


@dataclass
class EdgeWriteResult:
    written: int = 0
    superseded: int = 0
    scrubbed: int = 0
    violations: list[dict] = field(default_factory=list)  # registered, excluded


def _require_tenant(tenant_id: str) -> None:
    if not tenant_id or not str(tenant_id).strip():
        raise EdgeIdentityError("tenant_id is required at the edge persistence boundary (I2)")
    try:
        _uuid.UUID(str(tenant_id))
    except (ValueError, AttributeError, TypeError):
        raise EdgeIdentityError(f"tenant_id is not a valid UUID: {tenant_id!r}")


def _require_identity(tenant_id: str, entity_id: str) -> None:
    _require_tenant(tenant_id)
    if not entity_id or not str(entity_id).strip():
        raise EdgeIdentityError("entity_id is required at the edge persistence boundary (I2)")


def _coord(e: dict) -> tuple:
    """An edge's live-identity coordinates — re-asserting the same coordinates
    supersedes the prior row (correction semantics, same as facts)."""
    return (e["src_type"], e["src_key"], e["edge_type"], e["dst_type"], e["dst_key"])


def _validate_edge_payload(e: dict, idx: int) -> None:
    for f in ("src_type", "src_key", "edge_type", "dst_type", "dst_key",
              "source_system", "dcl_ingest_id", "confidence_score",
              "confidence_tier", "derivation"):
        v = e.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            raise EdgeContractError(
                f"edges[{idx}]: {f!r} is required on every edge (provenance contract)"
            )
    if e["derivation"] not in ("derived", "declared"):
        raise EdgeContractError(
            f"edges[{idx}]: derivation must be 'derived' or 'declared', got {e['derivation']!r}"
        )


def load_edge_types(tenant_id: str) -> dict[str, dict]:
    """Edge-type registry for a tenant: built-ins ('*') overlaid by tenant rows."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tenant_id, edge_type, description, cardinality, allowed_pairs "
                "FROM edge_types WHERE tenant_id IN (%s, %s)",
                [_BUILTIN_TENANT, str(tenant_id)],
            )
            rows = cur.fetchall()
    out: dict[str, dict] = {}
    # built-ins first, tenant rows overlay
    for owner in (_BUILTIN_TENANT, str(tenant_id)):
        for (own, et, desc, card, pairs) in rows:
            if own == owner:
                out[et] = {
                    "edge_type": et, "description": desc, "cardinality": card,
                    "allowed_pairs": pairs, "defined_by": own,
                }
    return out


def put_edge_type(
    tenant_id: str, edge_type: str, description: str,
    cardinality: str = "many_to_many",
    allowed_pairs: Optional[list[list[str]]] = None,
) -> dict:
    """Define (or update) a tenant edge type. Built-ins ('*') are not writable here."""
    if not tenant_id or tenant_id == _BUILTIN_TENANT:
        raise EdgeContractError("tenant edge types require a concrete tenant_id (not '*')")
    if cardinality not in ("one_to_one", "one_to_many", "many_to_one", "many_to_many"):
        raise EdgeContractError(f"unknown cardinality {cardinality!r}")
    if not edge_type or not edge_type.strip():
        raise EdgeContractError("edge_type is required")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO edge_types (tenant_id, edge_type, description, cardinality, allowed_pairs) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (tenant_id, edge_type) DO UPDATE SET "
                "description = EXCLUDED.description, cardinality = EXCLUDED.cardinality, "
                "allowed_pairs = EXCLUDED.allowed_pairs, updated_at = now()",
                [str(tenant_id), edge_type.strip(),
                 description, cardinality,
                 json.dumps(allowed_pairs) if allowed_pairs is not None else None],
            )
            conn.commit()
    return {"tenant_id": str(tenant_id), "edge_type": edge_type.strip(),
            "cardinality": cardinality, "allowed_pairs": allowed_pairs}


class EdgeStore:

    # ------------------------------------------------------------------ writes

    def assert_edges(
        self,
        tenant_id: str,
        entity_id: str,
        edges: list[dict],
        *,
        replace: bool = False,
    ) -> EdgeWriteResult:
        """Persist a batch of edges with constraint enforcement.

        Lifecycle (mirror of the facts store):
          default      — re-asserted coordinates supersede their prior live row
                         (idempotent correction); unrelated topology persists.
          replace=True — every live edge for (tenant, entity) is superseded
                         first (clean re-run), then the batch inserts; rows
                         already carrying THIS batch's run_id are scrubbed
                         (same-run redelivery, identical to the facts path).

        Constraint rules (edge_types registry) are evaluated against the
        post-supersession live state plus the in-batch accepted set.
        Violating edges are EXCLUDED from insert and FLAGGED into
        conflict_register (conflict_type='structural') in the SAME
        transaction — the register write and the graph write commit or roll
        back together (a violation can never be silently lost).
        """
        _require_identity(tenant_id, entity_id)
        if not edges:
            return EdgeWriteResult()
        for i, e in enumerate(edges):
            _validate_edge_payload(e, i)

        # Payload contract is the NAMESPACED id (I1); only the storage column
        # is named run_id (schema contract).
        run_ids = {str(e["dcl_ingest_id"]) for e in edges}
        if len(run_ids) != 1:
            raise EdgeContractError(
                f"assert_edges requires exactly one dcl_ingest_id across the batch; got {sorted(run_ids)}"
            )
        run_id = run_ids.pop()

        registry = load_edge_types(tenant_id)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")

                # Same-run redelivery scrub (idempotent replay of THIS ingest event).
                cur.execute(
                    "DELETE FROM entity_edges WHERE tenant_id = %s AND entity_id = %s AND run_id = %s",
                    [str(tenant_id), entity_id, run_id],
                )
                scrubbed = cur.rowcount

                superseded = 0
                if replace:
                    cur.execute(
                        "UPDATE entity_edges SET superseded_at = now(), updated_at = now() "
                        "WHERE tenant_id = %s AND entity_id = %s AND is_active = true",
                        [str(tenant_id), entity_id],
                    )
                    superseded = cur.rowcount

                # Live state AFTER supersession — the baseline constraints check
                # against. source_system + id ride along so a violation's register
                # claim can name the conflicting live edge's source and drill to it.
                cur.execute(
                    "SELECT src_type, src_key, edge_type, dst_type, dst_key, "
                    "       source_system, id FROM entity_edges "
                    "WHERE tenant_id = %s AND entity_id = %s AND is_active = true",
                    [str(tenant_id), entity_id],
                )
                live = [
                    {"src_type": r[0], "src_key": r[1], "edge_type": r[2],
                     "dst_type": r[3], "dst_key": r[4],
                     "source_system": r[5], "edge_id": str(r[6])}
                    for r in cur.fetchall()
                ]

                accepted: list[dict] = []
                violations: list[dict] = []
                # Coordinates being re-asserted in this batch supersede their live
                # predecessor — remove them from the constraint baseline up front.
                batch_coords = {_coord(e) for e in edges}
                base_live = [e for e in live if _coord(e) not in batch_coords]

                def _live_view() -> list[dict]:
                    return base_live + accepted

                for idx, e in enumerate(edges):
                    rule = registry.get(e["edge_type"])
                    v = self._check_edge(e, rule, _live_view())
                    if v is not None:
                        v["edge_index"] = idx
                        violations.append(v)
                        continue
                    accepted.append(
                        {k: e[k] for k in ("src_type", "src_key", "edge_type", "dst_type", "dst_key")}
                        # source_system/edge_id make an in-batch winner claimable
                        # in the register exactly like a DB-live one; edge_id is
                        # filled from RETURNING at insert below.
                        | {"source_system": e["source_system"], "edge_id": None, "_full": e}
                    )

                # Supersede the live predecessors of accepted re-asserted coordinates.
                for e in accepted:
                    cur.execute(
                        "UPDATE entity_edges SET superseded_at = now(), updated_at = now() "
                        "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                        "AND src_type = %s AND src_key = %s AND edge_type = %s "
                        "AND dst_type = %s AND dst_key = %s",
                        [str(tenant_id), entity_id,
                         e["src_type"], e["src_key"], e["edge_type"], e["dst_type"], e["dst_key"]],
                    )
                    superseded += cur.rowcount

                # Insert accepted edges. valid_from is optional on the payload
                # (declared edges may assert a past world-time); omitted → the
                # column default now() applies.
                for e in accepted:
                    full = e["_full"]
                    cols = list(_EDGE_COLS)
                    vals = [str(tenant_id), entity_id,
                            full["src_type"], full["src_key"], full["edge_type"],
                            full["dst_type"], full["dst_key"],
                            json.dumps(full["properties"]) if full.get("properties") is not None else None,
                            full["source_system"], full.get("source_table"), full.get("source_field"),
                            full.get("pipe_id"), run_id, full.get("source_run_tag"),
                            full["confidence_score"], full["confidence_tier"],
                            full.get("fabric_plane"), full.get("fabric_product"),
                            full["derivation"], full.get("valid_from")]
                    if full.get("valid_from") is None:
                        cols.pop()   # drop valid_from → DB default now()
                        vals.pop()
                    cur.execute(
                        f"INSERT INTO entity_edges ({', '.join(cols)}) VALUES "
                        f"({', '.join(['%s'] * len(cols))}) RETURNING id",
                        vals,
                    )
                    e["edge_id"] = str(cur.fetchone()[0])

                # Register violations — structural class, same transaction as the
                # graph write (commit or roll back together; a violation can never
                # be silently lost). ONE register with Gate 1A: rows live in
                # conflict_register under 1A's contract — claims is an ARRAY of
                # per-source claim dicts each carrying source_system (the conflict
                # drill UI maps claims; the disposition route reads
                # c["source_system"] and tolerates absent triple_id). The inline
                # INSERT mirrors ConflictStore.upsert_conflicts' coords/ON CONFLICT
                # semantics; it stays inline (not the store call) so the register
                # row and the edge writes share this transaction.
                for v in violations:
                    e = edges[v["edge_index"]]
                    claims: list[dict] = [{
                        "source_system": e["source_system"],
                        "pipe_id": str(e["pipe_id"]) if e.get("pipe_id") else None,
                        "derivation": e["derivation"],
                        "asserted_edge": {k: e.get(k) for k in (
                            "src_type", "src_key", "edge_type", "dst_type", "dst_key",
                            "properties")},
                        "rule": v["rule"],
                        "detail": v["detail"],
                        "standing": "rejected",
                    }]
                    cw = v.get("conflicting_with")
                    if cw:
                        claims.append({
                            "source_system": cw["source_system"],
                            "edge_id": cw["edge_id"],
                            "asserted_edge": {k: cw[k] for k in (
                                "src_type", "src_key", "edge_type", "dst_type", "dst_key")},
                            "standing": "live",
                        })
                    cur.execute(
                        "INSERT INTO conflict_register "
                        "(tenant_id, entity_id, conflict_type, conflict_class, concept, property, "
                        " period, dcl_ingest_id, status, claims) "
                        "VALUES (%s, %s, 'structural', %s, %s, %s, NULL, %s, 'open', %s) "
                        "ON CONFLICT (tenant_id, entity_id, concept, property, COALESCE(period, ''), dcl_ingest_id) "
                        "DO UPDATE SET claims = EXCLUDED.claims, "
                        "conflict_type = EXCLUDED.conflict_type, "
                        "conflict_class = EXCLUDED.conflict_class, updated_at = now()",
                        [str(tenant_id), entity_id, v["conflict_class"],
                         f"edge.{e['edge_type']}",
                         f"{e['src_type']}:{e['src_key']}->{e['dst_type']}:{e['dst_key']}",
                         run_id, json.dumps(claims)],
                    )

                conn.commit()

        if violations:
            logger.warning(
                "[assert_edges] %d/%d edge(s) violated constraint rules for "
                "tenant=%s entity=%s run=%s — registered as structural conflicts, excluded from graph",
                len(violations), len(edges), tenant_id, entity_id, run_id,
            )
        logger.info(
            "[assert_edges] tenant=%s entity=%s run=%s: written=%d superseded=%d "
            "scrubbed=%d violations=%d replace=%s",
            tenant_id, entity_id, run_id, len(accepted), superseded, scrubbed,
            len(violations), replace,
        )
        return EdgeWriteResult(
            written=len(accepted), superseded=superseded, scrubbed=scrubbed,
            violations=[{k: v[k] for k in ("conflict_class", "rule", "detail", "edge_index")}
                        | {"edge": {kk: edges[v["edge_index"]].get(kk) for kk in (
                            "src_type", "src_key", "edge_type", "dst_type", "dst_key")}}
                        for v in violations],
        )

    @staticmethod
    def _check_edge(e: dict, rule: Optional[dict], live: list[dict]) -> Optional[dict]:
        """Evaluate one edge against its type's constraint rules and the live view.

        Returns a violation dict (conflict_class, rule, detail, conflicting_with)
        or None if the edge is admissible.
        """
        if rule is None:
            return {
                "conflict_class": "edge_type_unregistered",
                "rule": "edge_type must be a built-in or tenant-defined type",
                "detail": f"edge_type {e['edge_type']!r} is not registered for this tenant",
                "conflicting_with": None,
            }

        pairs = rule.get("allowed_pairs")
        if pairs:
            if [e["src_type"], e["dst_type"]] not in [list(p) for p in pairs]:
                return {
                    "conflict_class": "edge_pair_disallowed",
                    "rule": f"allowed_pairs={pairs}",
                    "detail": (
                        f"({e['src_type']} -> {e['dst_type']}) is not an allowed node-type "
                        f"pair for edge_type {e['edge_type']!r}"
                    ),
                    "conflicting_with": None,
                }

        card = rule.get("cardinality", "many_to_many")
        same_type = [x for x in live if x["edge_type"] == e["edge_type"]]
        if card in ("many_to_one", "one_to_one"):
            clash = [x for x in same_type
                     if x["src_type"] == e["src_type"] and x["src_key"] == e["src_key"]
                     and not (x["dst_type"] == e["dst_type"] and x["dst_key"] == e["dst_key"])]
            if clash:
                return {
                    "conflict_class": "edge_cardinality",
                    "rule": f"cardinality={card}: src holds at most one live {e['edge_type']} edge",
                    "detail": (
                        f"{e['src_type']}:{e['src_key']} already has a live {e['edge_type']} "
                        f"edge to {clash[0]['dst_type']}:{clash[0]['dst_key']}; "
                        f"second target {e['dst_type']}:{e['dst_key']} violates {card}"
                    ),
                    "conflicting_with": clash[0],
                }
        if card in ("one_to_many", "one_to_one"):
            clash = [x for x in same_type
                     if x["dst_type"] == e["dst_type"] and x["dst_key"] == e["dst_key"]
                     and not (x["src_type"] == e["src_type"] and x["src_key"] == e["src_key"])]
            if clash:
                return {
                    "conflict_class": "edge_cardinality",
                    "rule": f"cardinality={card}: dst is pointed at by at most one live {e['edge_type']} edge",
                    "detail": (
                        f"{e['dst_type']}:{e['dst_key']} is already the target of a live "
                        f"{e['edge_type']} edge from {clash[0]['src_type']}:{clash[0]['src_key']}; "
                        f"second source {e['src_type']}:{e['src_key']} violates {card}"
                    ),
                    "conflicting_with": clash[0],
                }
        return None

    # ------------------------------------------------------------------- reads

    _READ_COLS = (
        "id, src_type, src_key, edge_type, dst_type, dst_key, properties, "
        "source_system, source_field, pipe_id, run_id, confidence_score, "
        "confidence_tier, derivation, valid_from, valid_to, ingested_at, "
        "superseded_at, is_active"
    )

    @staticmethod
    def _row_to_edge(r: tuple) -> dict:
        return {
            "id": str(r[0]),
            "src_type": r[1], "src_key": r[2], "edge_type": r[3],
            "dst_type": r[4], "dst_key": r[5],
            "properties": r[6],
            "source_system": r[7], "source_field": r[8],
            "pipe_id": str(r[9]) if r[9] else None,
            "dcl_ingest_id": str(r[10]),  # namespaced (I1) — run_id never leaves the store
            "confidence_score": float(r[11]), "confidence_tier": r[12],
            "derivation": r[13],
            "valid_from": r[14].isoformat() if r[14] else None,
            "valid_to": r[15].isoformat() if r[15] else None,
            "ingested_at": r[16].isoformat() if r[16] else None,
            "superseded_at": r[17].isoformat() if r[17] else None,
            "is_active": r[18],
        }

    @staticmethod
    def _temporal_clause(as_of: Optional[str], params: list) -> str:
        """Knowledge-time predicate: live now, or live as of T (same predicate
        as the facts store: ingested_at <= T AND (superseded_at IS NULL OR > T))."""
        if as_of is None:
            return " AND is_active = true"
        params.extend([as_of, as_of])
        return " AND ingested_at <= %s AND (superseded_at IS NULL OR superseded_at > %s)"

    def get_neighbors(
        self,
        tenant_id: str,
        entity_id: str,
        node_type: str,
        node_key: str,
        *,
        edge_type: Optional[str] = None,
        direction: str = "both",      # out | in | both
        as_of: Optional[str] = None,
        limit: int = 500,
    ) -> list[dict]:
        """Edges touching one node, with type/direction filters and as-of support."""
        _require_identity(tenant_id, entity_id)
        if direction not in ("out", "in", "both"):
            raise EdgeContractError(f"direction must be out|in|both, got {direction!r}")

        params: list[Any] = [str(tenant_id), entity_id]
        if direction == "out":
            node_clause = "src_type = %s AND src_key = %s"
            params += [node_type, node_key]
        elif direction == "in":
            node_clause = "dst_type = %s AND dst_key = %s"
            params += [node_type, node_key]
        else:
            node_clause = "((src_type = %s AND src_key = %s) OR (dst_type = %s AND dst_key = %s))"
            params += [node_type, node_key, node_type, node_key]

        type_clause = ""
        if edge_type:
            type_clause = " AND edge_type = %s"
            params.append(edge_type)

        temporal = self._temporal_clause(as_of, params)
        params.append(int(limit))

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {self._READ_COLS} FROM entity_edges "
                    f"WHERE tenant_id = %s AND entity_id = %s AND {node_clause}"
                    f"{type_clause}{temporal} "
                    f"ORDER BY edge_type, dst_type, dst_key LIMIT %s",
                    params,
                )
                return [self._row_to_edge(r) for r in cur.fetchall()]

    def list_entities(self, tenant_id: str) -> list[str]:
        """Distinct entity_ids holding at least one live edge for the tenant —
        the tenant-wide enumeration the Gate 2C exports walk (and their
        'does this tenant have a graph at all' existence predicate)."""
        _require_tenant(tenant_id)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT entity_id FROM entity_edges "
                    "WHERE tenant_id = %s AND is_active = true ORDER BY entity_id",
                    [str(tenant_id)],
                )
                return [r[0] for r in cur.fetchall()]

    def get_subgraph(
        self,
        tenant_id: str,
        entity_id: str,
        *,
        edge_types: Optional[list[str]] = None,
        as_of: Optional[str] = None,
        limit: int = 2000,
    ) -> dict:
        """The enterprise's edge set (live or as-of) + derived node list —
        the hero read. Nodes are derived from edge endpoints; node values are
        joined from semantic_triples by the route layer."""
        _require_identity(tenant_id, entity_id)
        params: list[Any] = [str(tenant_id), entity_id]
        type_clause = ""
        if edge_types:
            type_clause = f" AND edge_type IN ({', '.join(['%s'] * len(edge_types))})"
            params.extend(edge_types)
        temporal = self._temporal_clause(as_of, params)
        params.append(int(limit))

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {self._READ_COLS} FROM entity_edges "
                    f"WHERE tenant_id = %s AND entity_id = %s{type_clause}{temporal} "
                    f"ORDER BY edge_type, src_type, src_key, dst_type, dst_key LIMIT %s",
                    params,
                )
                edges = [self._row_to_edge(r) for r in cur.fetchall()]

        nodes: dict[tuple, dict] = {}
        for e in edges:
            for side in (("src_type", "src_key"), ("dst_type", "dst_key")):
                k = (e[side[0]], e[side[1]])
                nodes.setdefault(k, {"node_type": k[0], "node_key": k[1]})
        return {"edges": edges, "nodes": list(nodes.values())}


_edge_store: Optional[EdgeStore] = None


def get_edge_store() -> EdgeStore:
    global _edge_store
    if _edge_store is None:
        _edge_store = EdgeStore()
    return _edge_store
