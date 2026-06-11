"""Concept hierarchy — parent links over the concept library (Gate 1B, §7).

The library was flat: 161 root concepts grouped into domains by the ontology
YAML, with dotted children implied by name. This module makes the hierarchy
explicit and queryable WITHOUT duplicating the ontology:

  level 0  domain            ('hr', 'finance', …)         — from the YAML
  level 1  root concept      ('workforce', 'revenue', …)  — parent = its domain
  level 2+ dotted concepts   ('workforce.headcount.total')— parent = dotted prefix

The ontology YAML stays the single source of the DEFAULT tree (derived here at
read time, never copied into the DB). The concept_hierarchy table (migration
019) holds TENANT-DEFINED links only — a tenant row for a concept overrides
its default parent; rows may also attach tenant-custom concepts into the tree.

`expand_for_read` is how the hierarchy participates in reads: a parent concept
expands to itself + all descendants, expressed as (exact roots, LIKE
prefixes) the triple query layer can push into SQL.
"""

from functools import lru_cache
from typing import Optional

from backend.core.db import get_connection
from backend.registry.concept_registry import ConceptRegistry
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_BUILTIN_TENANT = "*"


@lru_cache(maxsize=1)
def _registry() -> ConceptRegistry:
    return ConceptRegistry()


def _tenant_links(tenant_id: str) -> dict[str, str]:
    """Explicit parent links: '*' defaults overlaid by tenant rows."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tenant_id, concept, parent_concept FROM concept_hierarchy "
                "WHERE tenant_id IN (%s, %s)",
                [_BUILTIN_TENANT, str(tenant_id)],
            )
            rows = cur.fetchall()
    links: dict[str, str] = {}
    for owner in (_BUILTIN_TENANT, str(tenant_id)):
        for (own, concept, parent) in rows:
            if own == owner:
                links[concept] = parent
    return links


def put_link(tenant_id: str, concept: str, parent_concept: str) -> dict:
    """Define (or move) a tenant parent link. Cycles are rejected loudly."""
    if not tenant_id or not str(tenant_id).strip():
        raise ValueError("tenant_id is required")
    concept = (concept or "").strip()
    parent_concept = (parent_concept or "").strip()
    if not concept or not parent_concept:
        raise ValueError("concept and parent_concept are required")
    if concept == parent_concept:
        raise ValueError("a concept cannot be its own parent")
    # cycle check against the would-be tree
    links = _tenant_links(tenant_id)
    links[concept] = parent_concept
    seen = {concept}
    cur = parent_concept
    while cur is not None:
        if cur in seen:
            raise ValueError(
                f"link {concept!r} -> {parent_concept!r} would create a cycle through {cur!r}"
            )
        seen.add(cur)
        cur = links.get(cur) or _default_parent(cur)
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "INSERT INTO concept_hierarchy (tenant_id, concept, parent_concept) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (tenant_id, concept) DO UPDATE SET parent_concept = EXCLUDED.parent_concept",
                [str(tenant_id), concept, parent_concept],
            )
            conn.commit()
    return {"tenant_id": str(tenant_id), "concept": concept, "parent_concept": parent_concept}


def _default_parent(concept: str) -> Optional[str]:
    """The ontology-derived default parent (no DB)."""
    if "." in concept:
        return concept.rsplit(".", 1)[0]
    reg = _registry()
    entry = reg.get_concept(concept)
    if entry:
        return entry.get("domain")
    return None  # a domain (or unknown) has no default parent


def parent_of(tenant_id: str, concept: str) -> Optional[str]:
    """Effective parent: tenant/explicit link wins, else the ontology default."""
    links = _tenant_links(tenant_id)
    if concept in links:
        return links[concept]
    return _default_parent(concept)


def children_of(tenant_id: str, concept: str) -> list[str]:
    """Direct children in the effective tree (domains -> roots; explicit links)."""
    reg = _registry()
    links = _tenant_links(tenant_id)
    overridden = set(links.keys())
    children = {c for c, p in links.items() if p == concept}
    # ontology defaults: roots whose domain is `concept`, unless overridden
    for root in reg.list_concepts():
        if root in overridden:
            continue
        entry = reg.get_concept(root)
        if entry and entry.get("domain") == concept:
            children.add(root)
    return sorted(children)


def list_domains() -> list[str]:
    reg = _registry()
    return sorted({
        (reg.get_concept(r) or {}).get("domain")
        for r in reg.list_concepts()
        if (reg.get_concept(r) or {}).get("domain")
    })


def expand_for_read(tenant_id: str, concept: str) -> dict:
    """Expand a concept to itself + all descendants for read participation.

    Returns {"exact": [concepts matched exactly], "prefixes": [dotted prefixes
    matched as `concept LIKE prefix || '.%'`]}. A root concept expands to
    itself + its dotted subtree; a domain expands to every root beneath it
    (and their subtrees); a dotted concept to itself + its subtree.
    """
    reg = _registry()
    exact: set[str] = set()
    prefixes: set[str] = set()

    def add_subtree(c: str) -> None:
        exact.add(c)
        prefixes.add(c)
        # explicit child links attached anywhere under c
        for child, parent in _tenant_links(tenant_id).items():
            if parent == c:
                add_subtree(child)

    if reg.get_concept(concept) or "." in concept:
        add_subtree(concept)
    else:
        # treat as a domain (or an explicit-link interior node)
        kids = children_of(tenant_id, concept)
        if not kids:
            # unknown node: expand to itself only — the read finds nothing,
            # loudly empty rather than silently broadened
            add_subtree(concept)
        else:
            exact.add(concept)
            for k in kids:
                add_subtree(k)
    return {"exact": sorted(exact), "prefixes": sorted(prefixes)}


def hierarchy_view(tenant_id: str, concept: Optional[str] = None) -> dict:
    """Read surface: one node (parent + children) or the full domain->root map."""
    if concept:
        return {
            "concept": concept,
            "parent": parent_of(tenant_id, concept),
            "children": children_of(tenant_id, concept),
        }
    return {
        "domains": {d: children_of(tenant_id, d) for d in list_domains()},
        "tenant_links": _tenant_links(tenant_id),
    }
