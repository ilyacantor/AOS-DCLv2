"""Canonical entity registry — per-(tenant_id, domain) source of truth for the
SE-path record-identity resolver.

Brought into DCL from AAM (app/db/canonical_registry.py) per AAM Blueprint v3.1
§3.6 decision (c): mapping + resolution of business records moves to DCL's
ingest endpoint so AAM's copy can be retired. The pure-Python pieces
(_normalize, compute_block_keys, CanonicalEntry, PatternRule, _SnapshotCache)
are kept identical to AAM's so the resolver behaves byte-for-byte the same; the
storage layer is rewritten onto DCL's pooled psycopg2 connection
(backend.core.db.get_connection) — AAM used an autocommit supabase_client, DCL's
pool is NOT autocommit so every write commits explicitly.

Public interface (preserved from AAM):
  add_canonical(tenant_id, domain, value, canonical_id?, aliases?) -> CanonicalEntry
  find_exact(tenant_id, domain, value)   -> Optional[CanonicalEntry]
  find_alias(tenant_id, domain, value)   -> Optional[CanonicalEntry]
  find_pattern(tenant_id, domain, value) -> Optional[CanonicalEntry]
  iter_canonicals(tenant_id, domain)     -> Iterable[CanonicalEntry]
  add_alias(tenant_id, domain, alias, canonical_id) -> None
  add_pattern_rule(tenant_id, domain, pattern, canonical_id, canonical_value) -> None

Concurrency: discovery uses INSERT ... ON CONFLICT DO NOTHING RETURNING so two
concurrent workers minting the same normalized value converge on one
canonical_id. A bounded TTL snapshot cache amortizes the per-(tenant, domain)
read across a single ingest batch; the table is the source of truth.
"""
from __future__ import annotations

import json
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Iterable, Optional

import psycopg2.extras

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_NORM_SEP = re.compile(r"[\s\-_./,;:]+")


def _normalize(s: str) -> str:
    """Lowercase + collapse separator runs to single space. Strip ends.

    Identical to backend.resolver.record_resolver._normalize. Duplicated here so
    this module does not import from backend.resolver (avoids an import cycle).
    """
    return _NORM_SEP.sub(" ", str(s)).lower().strip()


def compute_block_keys(value: str, aliases: Optional[Iterable[str]] = None) -> frozenset[str]:
    """Cheap blocking keys for tier-4 candidate prefiltering.

    Tier-4 fuzzy comparison would otherwise scan the whole registry per record
    (O(N^2) intra-batch). Block keys prefilter candidates to those plausibly
    similar BEFORE running similarity_score: every significant token (len >= 2)
    of the value/aliases plus every 2-char prefix of those tokens. Two
    candidates share a key iff a token starts with the same two chars or they
    share a whole token. False positives are fine (similarity_score handles
    them); recall loss is the failure mode this guards against — which is why
    2-char prefixes are included (preserves abbreviation matches like
    "FinTeam-NA" <-> "Finance North America").
    """
    keys: set[str] = set()
    strings = [value] + list(aliases or [])
    for s in strings:
        toks = [t for t in _NORM_SEP.split(str(s).lower()) if t and len(t) >= 2]
        for tok in toks:
            keys.add(tok)
            keys.add(tok[:2])
    return frozenset(keys)


@dataclass
class CanonicalEntry:
    """One row in the per-domain canonical registry.

    block_keys: precomputed cheap-prefix block keys for tier-4 fuzzy candidate
    prefiltering (see compute_block_keys). Populated at construction time.
    """
    canonical_id: str
    value: str
    domain: str
    aliases: list[str] = field(default_factory=list)
    block_keys: frozenset[str] = field(default_factory=frozenset)


@dataclass
class PatternRule:
    """One pattern -> canonical_id binding. Regex pre-compiled."""
    domain: str
    pattern: re.Pattern
    canonical_id: str
    canonical_value: str


# ---------------------------------------------------------------------------
# Snapshot cache — bounded, TTL-evicted. Never the source of truth.
# ---------------------------------------------------------------------------
_TTL_SECONDS = 60.0
_MAX_KEYS = 64


class _SnapshotCache:
    """Thread-safe TTL+LRU cache of per-(tenant_id, domain) snapshots."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[tuple[str, str], tuple[list[CanonicalEntry], float, float]] = {}

    def get(self, key: tuple[str, str]) -> Optional[list[CanonicalEntry]]:
        with self._lock:
            tup = self._data.get(key)
            if tup is None:
                return None
            snapshot, expires_at, _ = tup
            now = time.monotonic()
            if now >= expires_at:
                self._data.pop(key, None)
                return None
            self._data[key] = (snapshot, expires_at, now)
            return snapshot

    def put(self, key: tuple[str, str], snapshot: list[CanonicalEntry]) -> None:
        with self._lock:
            now = time.monotonic()
            self._data[key] = (snapshot, now + _TTL_SECONDS, now)
            if len(self._data) > _MAX_KEYS:
                oldest_key = min(self._data, key=lambda k: self._data[k][2])
                self._data.pop(oldest_key, None)

    def invalidate(self, key: tuple[str, str]) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_SNAPSHOTS = _SnapshotCache()


def _query(sql: str, params: tuple, *, fetch: bool = True) -> list[dict]:
    """Run a parameterized query on a pooled connection with an explicit commit.

    Returns rows as dicts (RealDictCursor). DCL's pool is not autocommit, so
    writes must commit here — unlike AAM's autocommit supabase_client.
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()] if fetch and cur.description else []
        conn.commit()
    return rows


class CanonicalRegistry:
    """Canonical registry persisted in DCL Postgres (table canonical_registry).

    Pattern rules remain in-process (no runtime mutation today). Process-local
    state is bounded to _SnapshotCache (TTL=60s, <=64 keys).
    """

    def __init__(self) -> None:
        self._pattern_rules: dict[tuple[str, str], list[PatternRule]] = {}

    # ---- mutations -------------------------------------------------------

    def add_canonical(
        self,
        *,
        tenant_id: str,
        domain: str,
        value: str,
        canonical_id: Optional[str] = None,
        aliases: Optional[list[str]] = None,
    ) -> CanonicalEntry:
        """Insert or return existing canonical. Idempotent on (tenant, domain, norm)."""
        if not tenant_id or not domain or not value:
            raise ValueError(
                f"add_canonical: tenant_id, domain, value required "
                f"(got tenant_id={tenant_id!r} domain={domain!r} value={value!r})"
            )
        norm = _normalize(value)
        if not norm:
            raise ValueError(f"add_canonical: value normalizes to empty string ({value!r})")
        cid = canonical_id or str(uuid.uuid4())
        alias_list = list(aliases or [])

        rows = _query(
            "INSERT INTO canonical_registry "
            "(canonical_id, tenant_id, domain, normalized_value, original_value, aliases_jsonb) "
            "VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT (tenant_id, domain, normalized_value) DO NOTHING "
            "RETURNING canonical_id, original_value, aliases_jsonb",
            (cid, tenant_id, domain, norm, str(value), json.dumps(alias_list)),
        )
        if rows:
            _SNAPSHOTS.invalidate((tenant_id, domain))
            return self._row_to_entry(rows[0], domain)

        # Conflict — another writer beat us to this normalized value. Fetch it.
        existing = _query(
            "SELECT canonical_id, original_value, aliases_jsonb FROM canonical_registry "
            "WHERE tenant_id=%s AND domain=%s AND normalized_value=%s",
            (tenant_id, domain, norm),
        )
        if not existing:
            raise RuntimeError(
                f"add_canonical: ON CONFLICT returned nothing AND no row found for "
                f"(tenant_id={tenant_id!r}, domain={domain!r}, value={value!r}) — DB inconsistency"
            )
        return self._row_to_entry(existing[0], domain)

    def add_alias(self, *, tenant_id: str, domain: str, alias: str, canonical_id: str) -> None:
        if not alias or not canonical_id:
            raise ValueError("add_alias: alias and canonical_id required")
        _query(
            "UPDATE canonical_registry "
            "SET aliases_jsonb = CASE "
            "      WHEN aliases_jsonb @> to_jsonb(%s::text) THEN aliases_jsonb "
            "      ELSE aliases_jsonb || to_jsonb(%s::text) END, "
            "    updated_at = now() "
            "WHERE canonical_id=%s AND tenant_id=%s AND domain=%s",
            (alias, alias, canonical_id, tenant_id, domain),
            fetch=False,
        )
        _SNAPSHOTS.invalidate((tenant_id, domain))

    def add_pattern_rule(
        self, *, domain: str, pattern: str, canonical_id: str,
        canonical_value: str, tenant_id: str,
    ) -> None:
        """Pattern rules remain in-process (no runtime mutation today)."""
        rule = PatternRule(
            domain=domain,
            pattern=re.compile(pattern, re.IGNORECASE),
            canonical_id=canonical_id,
            canonical_value=canonical_value,
        )
        self._pattern_rules.setdefault((tenant_id, domain), []).append(rule)

    # ---- queries ---------------------------------------------------------

    @staticmethod
    def _row_to_entry(row: dict, domain: str) -> CanonicalEntry:
        row_aliases = list(row.get("aliases_jsonb") or [])
        return CanonicalEntry(
            canonical_id=str(row["canonical_id"]),
            value=row["original_value"],
            domain=domain,
            aliases=row_aliases,
            block_keys=compute_block_keys(row["original_value"], row_aliases),
        )

    def _snapshot(self, *, tenant_id: str, domain: str) -> list[CanonicalEntry]:
        """Load or return cached list of all canonicals for (tenant, domain)."""
        key = (tenant_id, domain)
        cached = _SNAPSHOTS.get(key)
        if cached is not None:
            return cached
        rows = _query(
            "SELECT canonical_id, original_value, aliases_jsonb FROM canonical_registry "
            "WHERE tenant_id=%s AND domain=%s",
            (tenant_id, domain),
        )
        snapshot = [self._row_to_entry(r, domain) for r in rows]
        _SNAPSHOTS.put(key, snapshot)
        return snapshot

    def find_exact(self, *, tenant_id: str, domain: str, value: str) -> Optional[CanonicalEntry]:
        norm = _normalize(value)
        if not norm:
            return None
        cached = _SNAPSHOTS.get((tenant_id, domain))
        if cached is not None:
            for e in cached:
                if _normalize(e.value) == norm:
                    return e
            return None
        rows = _query(
            "SELECT canonical_id, original_value, aliases_jsonb FROM canonical_registry "
            "WHERE tenant_id=%s AND domain=%s AND normalized_value=%s",
            (tenant_id, domain, norm),
        )
        return self._row_to_entry(rows[0], domain) if rows else None

    def find_alias(self, *, tenant_id: str, domain: str, value: str) -> Optional[CanonicalEntry]:
        norm = _normalize(value)
        if not norm:
            return None
        for entry in self._snapshot(tenant_id=tenant_id, domain=domain):
            for alias in entry.aliases:
                if _normalize(alias) == norm:
                    return entry
        return None

    def find_pattern(self, *, tenant_id: str, domain: str, value: str) -> Optional[CanonicalEntry]:
        for rule in self._pattern_rules.get((tenant_id, domain), []):
            if rule.pattern.search(value):
                for entry in self._snapshot(tenant_id=tenant_id, domain=domain):
                    if entry.canonical_id == rule.canonical_id:
                        return entry
                # Canonical doesn't exist yet — mint with the rule's value.
                return self.add_canonical(
                    tenant_id=tenant_id, domain=domain,
                    canonical_id=rule.canonical_id, value=rule.canonical_value,
                )
        return None

    def iter_canonicals(self, *, tenant_id: str, domain: str) -> Iterable[CanonicalEntry]:
        return iter(self._snapshot(tenant_id=tenant_id, domain=domain))

    # ---- test helpers ----------------------------------------------------

    def reset_for_tenant(self, *, tenant_id: str) -> int:
        """Delete all canonical entries for a tenant. Test-only."""
        if not tenant_id:
            raise ValueError("reset_for_tenant: tenant_id required")
        rows = _query(
            "DELETE FROM canonical_registry WHERE tenant_id=%s RETURNING canonical_id",
            (tenant_id,),
        )
        _SNAPSHOTS.clear()
        return len(rows)
