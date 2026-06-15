"""Stage-3 edge derivation — the stitched graph (ContextOS).

Stage 1 ingests source records into triples. Stage 2 (semantic_triples_current)
is the canonical current-state surface. Stage 3 — HERE — DERIVES typed edges
ACROSS resolved entities and persists them in entity_edges via EdgeStore.

THE PRODUCT PRINCIPLE. This module is a GENERAL engine. Its rules name NOTHING
entity-specific — no department, group, band, driver, or reason string appears in
the rule logic. Every specific (which department, which group concentrates, which
band is load-bearing, which internal key resolves to which external key) is read
from the entity's DATA. The same code that yields ContextOSDemo's hero graph
yields a different graph on a different entity, because the data differs, not the
rules. (Verified by grep: no entity literal lives in this file.)

The governing gate: every derived edge must carry information NO single source
record holds. The hero is a cross-source comp-gap synthesized by JOINING an
internal comp-band median (one source) against an external market median (another
source) across a resolution the ENTITY DECLARES IN ITS DATA. Neither source record
holds the gap — it exists only in the join.

The three general rules (each takes no entity-name argument):

  Rule 1 — GAP (BELOW_MARKET, derived). For any (base, band) carrying an internal
      comp-band median that resolves — via a resolution mapping DECLARED IN THE
      DATA (comp_resolution.* triples: internal_key -> external_key) — to an
      external market median, emit a gap edge. pct = (market-internal)/market*100;
      properties carry both medians + their source_systems, the band, and the two
      consumed concepts. The resolution is read, never hardcoded. Department-level
      and (base, band)-level forms both derive; the band form feeds rule 3.

  Rule 2 — CONCENTRATION (in-engine, NOT a stored edge). Over the workforce-
      departure-by-group-band feed, compute for each (group, band) its share of
      that band's departures. This is internal evidence for rule 3; a stored
      departure-share ranking would mirror a single-source group-by and earn no
      edge (the §3 honesty gate), so it is never persisted.

  Rule 3 — DRIVEN_BY (the earned synthesis edge). For any group whose departures
      CONCENTRATE (rule 2) in a band that itself carries a driver edge (a band gap
      from rule 1), emit a DRIVEN_BY edge group -> driver, joined VIA THE BAND.
      Properties record the join: the concentration (share + count), the band, the
      driver's key facts (gap_pct, medians), and the consumed concepts + driver
      edge. Asserts what no single record holds — a group's concentrated departures
      tied to a cross-source driver, joined on the band. No group/band/driver NAME
      appears; it fires wherever data aligns a concentration with a driver on the
      same band.

Plus the walkable structural membership (org -> department -> group), derived
generally from a group-membership record field whose value declares each group's
parent department (read from data), and the declared internal->external resolution
(RESOLVES_TO) the gap rule stands on.

Identity (tenant_id + entity_id) and one dcl_ingest_id ride every edge (I1/I2).
Fail loud (A1): an internal median with no resolvable external median is a
derivation gap — it RAISES, never silently skips.
"""

from __future__ import annotations

from typing import Any, Optional

from backend.core.db import get_connection
from backend.db.edge_store import EdgeStore, put_edge_type
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Comp/market medians are stable across months in the source (one band per
# department, atemporal); we read them at the headline period so the derivation
# is deterministic and produces one edge per (base[, band]).
_HEADLINE_PERIOD = "2026-03"

# ── Concept names DCL forms from the feeds (the classifier's vocabulary) ──────
# These are DCL-owned canonical concept names — generic, no entity specifics.
# Internal comp by department[/band], external market by job_family[/band], the
# declared internal->external resolution, the group-membership structure, and the
# workforce-departure feed cut by group and band. The KEYS inside these concepts
# (which department, which group, which band) are the entity's data.
_COMP_CONCEPT = "comp_band.median.by_department"                     # property "<base>"
_MARKET_CONCEPT = "market_benchmark.median.by_job_family"            # property "<external>"
_COMP_BAND_CONCEPT = "comp_band.median.by_department_band"           # property "<base>:<band>"
_MARKET_BAND_CONCEPT = "market_benchmark.median.by_job_family_band"  # property "<external>:<band>"

# The DECLARED internal->external resolution, carried AS DATA (one triple per
# internal key, value = the external key). The gap rule reads it to resolve the
# cross-source pair — there is NO hardcoded mapping in this module. Nested under
# the registered comp_band root (comp-domain metadata, no new ontology root).
_RESOLUTION_CONCEPT = "comp_band.resolution.department_to_job_family"  # property "<internal>", value "<external>"

# Structural team membership: property=team, value "<parent_base>|<roster>" —
# the parent the team belongs to (read from data) and the team's bench size.
# 'team' is a structural org-tree type (like 'department'/'org_unit'), not an
# entity specific — which team, which parent are the data's.
_GROUP_MEMBER_CONCEPT = "workforce.team.member_of"
_GROUP_MEMBER_SEP = "|"

# Workforce departures cut by team and by team+band (the concentration feed).
_DEPARTURES_BY_GROUP_BAND_CONCEPT = "workforce.departures.by_team_band"  # property "<team>:<band>"

# Exit-theme driver: per-reason departure counts by department. The reason set is
# DISCOVERED from the data (every concept matching this template's shape), never
# enumerated here. The template is a name-free format string ({reason} is a
# placeholder); it is also used by the provenance reveal to reconstruct the
# consumed concepts from an edge's stamped breakdown keys.
_EXIT_CONCEPT_PREFIX = "workforce.exit_theme."
_EXIT_CONCEPT_SUFFIX = ".by_department"
_EXIT_CONCEPT_TMPL = _EXIT_CONCEPT_PREFIX + "{reason}" + _EXIT_CONCEPT_SUFFIX


class EdgeDerivationError(RuntimeError):
    """A derivation could not complete with the integrity the gate requires
    (e.g. an internal median with no resolvable external median). Surfaces loud
    (A1) — never a silent skip."""


def _read_current_triples(tenant_id: str, entity_id: str, concept: str, period: str) -> dict[str, dict]:
    """All active (Stage-2) triples for one concept at one period, keyed by
    property. source_system rides along so the derivation can assert provenance
    on the edge it builds."""
    out: dict[str, dict] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT property, value, source_system FROM semantic_triples_current "
                "WHERE tenant_id = %s AND entity_id = %s AND concept = %s AND period = %s",
                [tenant_id, entity_id, concept, period],
            )
            for prop, value, source_system in cur.fetchall():
                out[prop] = {"value": value, "source_system": source_system}
    return out


def _discover_exit_concepts(tenant_id: str, entity_id: str, period: str) -> dict[str, str]:
    """Discover the exit-theme reason concepts present in the data — reason ->
    full concept. The reason set is the entity's (whatever reasons its feed
    carries); this rule enumerates none. Matches concepts of the shape
    'workforce.exit_theme.<reason>.by_department' at the period."""
    out: dict[str, str] = {}
    like = _EXIT_CONCEPT_PREFIX + "%" + _EXIT_CONCEPT_SUFFIX
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT concept FROM semantic_triples_current "
                "WHERE tenant_id = %s AND entity_id = %s AND period = %s "
                "AND concept LIKE %s",
                [tenant_id, entity_id, period, like],
            )
            for (concept,) in cur.fetchall():
                if not concept.startswith(_EXIT_CONCEPT_PREFIX) or not concept.endswith(_EXIT_CONCEPT_SUFFIX):
                    continue
                reason = concept[len(_EXIT_CONCEPT_PREFIX):-len(_EXIT_CONCEPT_SUFFIX)]
                # A nested-deeper concept (extra dots) is not a flat reason — skip
                # so the reason is exactly the single segment between prefix/suffix.
                if reason and "." not in reason:
                    out[reason] = concept
    return out


def _as_int(value: Any) -> int:
    """Coerce a store value (jsonb -> float for whole-dollar/whole-count facts)
    to int. Raises if the value is not a finite whole number — a non-integer
    median or count is a data-integrity problem the gate must not paper over."""
    f = float(value)
    if f != int(f):
        raise EdgeDerivationError(
            f"expected a whole-number median/count, got {value!r} — refusing to "
            f"round-trip a fractional value through an int edge property"
        )
    return int(f)


def _split_band(prop: str) -> Optional[tuple[str, str]]:
    """How a band-keyed property splits into (base, band). Properties are
    '<base>:<band>' (':' compound key — the flat source shape, no dots). Split on
    the LAST ':' so a base that itself contains no ':' is recovered exactly."""
    if ":" not in prop:
        return None
    base, band = prop.rsplit(":", 1)
    if not base or not band:
        return None
    return base, band


def _read_resolution(tenant_id: str, entity_id: str) -> dict[str, str]:
    """The DECLARED internal->external resolution, read from the data (the
    comp_resolution.* triples). property = internal key, value = external key
    (a string). This is the mapping the gap rule resolves the cross-source pair
    through — it is the ENTITY'S, carried as data, never a hardcoded dict.

    Read across all periods then deduped (the mapping is atemporal — an annual
    declaration the survey feed repeats every month); a key declaring two
    different external targets is a data conflict and RAISES (A1)."""
    resolution: dict[str, str] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT property, value FROM semantic_triples_current "
                "WHERE tenant_id = %s AND entity_id = %s AND concept = %s",
                [tenant_id, entity_id, _RESOLUTION_CONCEPT],
            )
            for prop, value in cur.fetchall():
                external = str(value)
                if prop in resolution and resolution[prop] != external:
                    raise EdgeDerivationError(
                        f"the declared resolution is inconsistent: internal key {prop!r} "
                        f"maps to both {resolution[prop]!r} and {external!r} in "
                        f"{_RESOLUTION_CONCEPT}; a key must resolve to one external "
                        f"target — fix the source mapping, do not pick one silently"
                    )
                resolution[prop] = external
    return resolution


def _edge(
    dcl_ingest_id: str, *, src_type: str, src_key: str, edge_type: str,
    dst_type: str, dst_key: str, properties: dict,
    derivation: str, confidence: float, tier: str = "exact",
) -> dict:
    """Build an EdgeStore.assert_edges payload for a dcl_derived edge. Centralises
    the bookkeeping fields shared by every derived/declared ContextOS edge so the
    derivations carry only the fields that distinguish them."""
    return {
        "src_type": src_type, "src_key": src_key,
        "edge_type": edge_type,
        "dst_type": dst_type, "dst_key": dst_key,
        "properties": properties,
        "source_system": "dcl_derived",
        "source_table": None, "source_field": None,
        "pipe_id": None, "source_run_tag": None,
        "dcl_ingest_id": dcl_ingest_id,
        "confidence_score": confidence, "confidence_tier": tier,
        "fabric_plane": None, "fabric_product": None,
        "derivation": derivation,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Rule 1 — GAP (BELOW_MARKET): cross-source comp-gap via the data-declared
# resolution. Department-level AND (base, band)-level. The band form returns an
# index keyed by (base, band) that rule 3 joins on.
# ─────────────────────────────────────────────────────────────────────────────


def _derive_comp_gap(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    comp: dict[str, dict], market: dict[str, dict], resolution: dict[str, str],
) -> list[dict]:
    """Department-level gap. For each internal base with a comp median AND a
    market median for its resolved external key, emit a BELOW_MARKET edge whose
    properties carry the SYNTHESIZED gap (gap_usd / gap_pct / below_market) —
    values no single source record holds. The resolution is read from data.

    Fail loud (A1): an internal base with a comp median but no resolution entry,
    or no market median for its resolved key, RAISES — the gap cannot be honestly
    synthesized without both sides, and a silent skip would hide a broken
    resolution."""
    edges: list[dict] = []
    for base in sorted(comp):
        internal = _as_int(comp[base]["value"])
        internal_source = comp[base]["source_system"]

        external = resolution.get(base)
        if external is None:
            raise EdgeDerivationError(
                f"internal key {base!r} has a {_COMP_CONCEPT} median ({internal}) but no "
                f"entry in the declared resolution {_RESOLUTION_CONCEPT} "
                f"(declared keys: {sorted(resolution)}); cannot resolve an external "
                f"median to synthesize its comp-gap — fix the declared mapping, do not skip"
            )
        if external not in market:
            raise EdgeDerivationError(
                f"internal key {base!r} resolves to external key {external!r} but no "
                f"{_MARKET_CONCEPT} median exists for it at period {_HEADLINE_PERIOD} "
                f"(market keys present: {sorted(market)}); cannot synthesize the "
                f"cross-source comp-gap — missing market side, failing loud"
            )

        market_median = _as_int(market[external]["value"])
        market_source = market[external]["source_system"]
        gap_usd = market_median - internal
        gap_pct = round((market_median - internal) / market_median * 100, 2)
        below_market = market_median > internal

        edges.append(_edge(
            dcl_ingest_id,
            src_type="department", src_key=base,
            edge_type="BELOW_MARKET",
            dst_type="job_family", dst_key=external,
            properties={
                "internal_median": internal,
                "market_median": market_median,
                "gap_usd": gap_usd,
                "gap_pct": gap_pct,
                "below_market": below_market,
                "internal_source": internal_source,
                "market_source": market_source,
                "consumed": [_COMP_CONCEPT, _MARKET_CONCEPT, _RESOLUTION_CONCEPT],
            },
            derivation="derived", confidence=0.95,
        ))
    return edges


def _derive_comp_gap_band(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    comp_band: dict[str, dict], market_band: dict[str, dict], resolution: dict[str, str],
) -> tuple[list[dict], dict[tuple[str, str], dict]]:
    """The (base, band)-level cross-source comp-gap — the refinement that lets a
    band-concentrated departure group tie to a band-specific gap. For each
    (base, band) with an internal median AND a market median for its resolved
    (external, band), emit a BELOW_MARKET edge
    department_band:<base>:<band> -> job_family_band:<external>:<band> carrying the
    synthesized gap. The resolution is read from data.

    Returns (edges, gap_index) where gap_index maps (base, band) -> that edge's
    gap facts — rule 3 looks up a concentration's (group's-parent, band) here to
    JOIN on the band. Fail loud (A1): a band with an internal median but no
    resolvable market median RAISES."""
    edges: list[dict] = []
    gap_index: dict[tuple[str, str], dict] = {}
    for prop in sorted(comp_band):
        split = _split_band(prop)
        if split is None:
            raise EdgeDerivationError(
                f"{_COMP_BAND_CONCEPT} property {prop!r} is not a '<base>:<band>' "
                f"compound key — cannot resolve a base+band to join a market "
                f"median; fix the classifier, do not skip"
            )
        base, band = split
        internal = _as_int(comp_band[prop]["value"])
        internal_source = comp_band[prop]["source_system"]

        external = resolution.get(base)
        if external is None:
            raise EdgeDerivationError(
                f"internal key {base!r} (band {band!r}) has a {_COMP_BAND_CONCEPT} median "
                f"({internal}) but no entry in the declared resolution "
                f"{_RESOLUTION_CONCEPT} (declared keys: {sorted(resolution)}); cannot "
                f"resolve a band market median — fix the declared mapping, do not skip"
            )
        market_prop = f"{external}:{band}"
        if market_prop not in market_band:
            raise EdgeDerivationError(
                f"department_band {base}:{band} resolves to {market_prop!r} but no "
                f"{_MARKET_BAND_CONCEPT} median exists for it at period {_HEADLINE_PERIOD} "
                f"(market band keys present: {sorted(market_band)}); cannot synthesize "
                f"the band-level cross-source comp-gap — missing market side, failing loud"
            )
        market_median = _as_int(market_band[market_prop]["value"])
        market_source = market_band[market_prop]["source_system"]
        gap_usd = market_median - internal
        gap_pct = round((market_median - internal) / market_median * 100, 2)
        below_market = market_median > internal

        edges.append(_edge(
            dcl_ingest_id,
            src_type="department_band", src_key=f"{base}:{band}",
            edge_type="BELOW_MARKET",
            dst_type="job_family_band", dst_key=market_prop,
            properties={
                "band": band,
                "internal_median": internal,
                "market_median": market_median,
                "gap_usd": gap_usd,
                "gap_pct": gap_pct,
                "below_market": below_market,
                "internal_source": internal_source,
                "market_source": market_source,
                "consumed": [_COMP_BAND_CONCEPT, _MARKET_BAND_CONCEPT, _RESOLUTION_CONCEPT],
            },
            derivation="derived", confidence=0.95,
        ))
        gap_index[(base, band)] = {
            "base": base, "band": band,
            "src_type": "department_band", "src_key": f"{base}:{band}",
            "dst_type": "job_family_band", "dst_key": market_prop,
            "internal_median": internal, "market_median": market_median,
            "gap_usd": gap_usd, "gap_pct": gap_pct, "below_market": below_market,
        }
    return edges, gap_index


# ─────────────────────────────────────────────────────────────────────────────
# Exit driver (DRIVEN_BY exit_theme): the dominant exit reason per department.
# Reasons are DISCOVERED from data, not enumerated. (A second DRIVEN_BY shape
# from a different source — the band-driver synthesis — lives in rule 3.)
# ─────────────────────────────────────────────────────────────────────────────


def _derive_exit_driver(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    exit_by_reason: dict[str, dict[str, dict]],
) -> list[dict]:
    """For each department, rank its exit-theme counts and emit ONE DRIVEN_BY edge
    to the dominant reason. The ranking — which reason dominates — is the
    synthesized information; no single count triple asserts it. The reason set is
    whatever the data carries (discovered); ties break by reason order for
    reproducibility (B14), a deterministic pick, not a value judgement."""
    reasons = sorted(exit_by_reason)            # discovered reasons, stable order
    departments: set[str] = set()
    for reason in reasons:
        departments.update(exit_by_reason.get(reason, {}).keys())

    edges: list[dict] = []
    for dept in sorted(departments):
        breakdown = {
            reason: _as_int(exit_by_reason.get(reason, {}).get(dept, {}).get("value", 0))
            for reason in reasons
        }
        total = sum(breakdown.values())
        if total == 0:
            logger.info(
                "[edge_derivation] exit_driver: dept=%s has zero total exits across "
                "all reasons — no dominant-reason edge emitted (no driver exists)",
                dept,
            )
            continue
        dominant = max(reasons, key=lambda r: (breakdown[r], -reasons.index(r)))
        count = breakdown[dominant]
        edges.append(_edge(
            dcl_ingest_id,
            src_type="department", src_key=dept,
            edge_type="DRIVEN_BY",
            dst_type="exit_theme", dst_key=dominant,
            properties={
                "count": count,
                "total": total,
                "share": round(count / total, 3),
                "rank": 1,
                "breakdown": breakdown,
            },
            derivation="derived", confidence=0.95,
        ))
    return edges


def _derive_resolution_edges(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    comp: dict[str, dict], resolution: dict[str, str],
) -> list[dict]:
    """The declared internal->external identity mapping that ENABLES the
    cross-source comp-gap. Emitted for every internal base that holds a comp
    median (the bases the graph actually resolves), reading its external target
    from the data-declared resolution. derivation='declared' — the mapping is
    asserted by the entity, not computed."""
    edges: list[dict] = []
    for base in sorted(comp):
        external = resolution.get(base)
        if external is None:
            raise EdgeDerivationError(
                f"internal key {base!r} holds a {_COMP_CONCEPT} median but has no "
                f"entry in the declared resolution {_RESOLUTION_CONCEPT} "
                f"(declared keys: {sorted(resolution)}) — cannot assert RESOLVES_TO"
            )
        edges.append(_edge(
            dcl_ingest_id,
            src_type="department", src_key=base,
            edge_type="RESOLVES_TO",
            dst_type="job_family", dst_key=external,
            properties={"mapping": "department->job_family", "consumed": [_RESOLUTION_CONCEPT]},
            derivation="declared", confidence=1.0,
        ))
    return edges


# ─────────────────────────────────────────────────────────────────────────────
# Structural membership (HAS_DEPARTMENT / HAS_TEAM): the walkable org tree.
# Each team's parent is read from the membership value, never hardcoded.
# ─────────────────────────────────────────────────────────────────────────────


def _parse_team_member(value: Any) -> tuple[str, int]:
    """A team-membership value declares '<parent_base>|<roster>'. The parent is
    DATA (which department the team belongs to); the roster the bench size. Both
    are required — a malformed value is a data-integrity gap (A1), not a default."""
    text = str(value)
    if _GROUP_MEMBER_SEP not in text:
        raise EdgeDerivationError(
            f"{_GROUP_MEMBER_CONCEPT} value {value!r} is not a "
            f"'<parent>{_GROUP_MEMBER_SEP}<roster>' compound — cannot read the team's "
            f"parent department; fix the classifier/feed, do not assume a parent"
        )
    parent, roster_raw = text.split(_GROUP_MEMBER_SEP, 1)
    parent = parent.strip()
    if not parent:
        raise EdgeDerivationError(
            f"{_GROUP_MEMBER_CONCEPT} value {value!r} declares an empty parent — a "
            f"team must belong to a named parent department (A1)"
        )
    roster = _as_int(roster_raw)
    return parent, roster


def _derive_membership(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    comp: dict[str, dict], team_members: dict[str, dict],
) -> list[dict]:
    """STRUCTURAL membership edges that make the org -> department -> team walk
    traversable. Declared structure (the org tree asserts them), not synthesis:

      org_unit:<entity> HAS_DEPARTMENT department:<base>   (every resolved base)
      department:<parent> HAS_TEAM team:<team>             (parent read from data)

    The team's parent and bench size are read from the membership value
    ('<parent>|<roster>'); the bench size is carried on the edge so the team node
    is self-describing. Declared — structure is asserted by the source."""
    edges: list[dict] = []
    for base in sorted(comp):
        edges.append(_edge(
            dcl_ingest_id,
            src_type="org_unit", src_key=entity_id,
            edge_type="HAS_DEPARTMENT",
            dst_type="department", dst_key=base,
            properties={"membership": "org_unit->department"},
            derivation="declared", confidence=1.0,
        ))
    for team in sorted(team_members):
        parent, roster = _parse_team_member(team_members[team]["value"])
        edges.append(_edge(
            dcl_ingest_id,
            src_type="department", src_key=parent,
            edge_type="HAS_TEAM",
            dst_type="team", dst_key=team,
            properties={
                "membership": "department->team",
                "parent_department": parent,
                "roster": roster,
                "consumed": [_GROUP_MEMBER_CONCEPT],
            },
            derivation="declared", confidence=1.0,
        ))
    return edges


# ─────────────────────────────────────────────────────────────────────────────
# Rule 2 — CONCENTRATION (in-engine, NOT stored) feeding Rule 3 — DRIVEN_BY.
# ─────────────────────────────────────────────────────────────────────────────


def _compute_band_concentration(
    departures_by_team_band: dict[str, dict],
) -> dict[str, dict[str, dict]]:
    """RULE 2 (in-engine). For each band, compute each team's share of that
    band's departures: band -> {team -> {"count": n, "band_total": T, "share": s}}.
    A within-band group-by — the evidence the synthesis rule (rule 3) joins to a
    band driver. NOT persisted: a stored departure-share ranking would mirror a
    single-source roster group-by and earn no edge (the §3 honesty gate)."""
    # team, band -> count
    counts: dict[tuple[str, str], int] = {}
    for prop, rec in departures_by_team_band.items():
        split = _split_band(prop)
        if split is None:
            raise EdgeDerivationError(
                f"{_DEPARTURES_BY_GROUP_BAND_CONCEPT} property {prop!r} is not a "
                f"'<team>:<band>' compound key — cannot read a team's band "
                f"departures; fix the classifier, do not skip"
            )
        team, band = split
        counts[(team, band)] = _as_int(rec["value"])

    band_totals: dict[str, int] = {}
    for (team, band), n in counts.items():
        band_totals[band] = band_totals.get(band, 0) + n

    out: dict[str, dict[str, dict]] = {}
    for (team, band), n in counts.items():
        total = band_totals[band]
        share = round(n / total, 3) if total else 0.0
        out.setdefault(band, {})[team] = {"count": n, "band_total": total, "share": share}
    return out


def _derive_band_driver_synthesis(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    concentration: dict[str, dict[str, dict]],
    gap_index: dict[tuple[str, str], dict],
    team_members: dict[str, dict],
) -> list[dict]:
    """RULE 3 — THE EARNED SYNTHESIS EDGE. For any team whose departures
    CONCENTRATE in a band that itself carries a driver (a band gap edge from rule
    1), emit ONE derived edge

        team:<team> DRIVEN_BY job_family_band:<external>:<band>

    that JOINS the team's band-concentrated departures with the band's cross-
    source comp-gap, VIA THE BAND. The band is the load-bearing key: it ties a
    team-level departure count to a band-level pay gap. No single record asserts
    this tie — one record holds the team's band departures, two OTHER records (two
    source systems) hold the band gap; the edge is the only place they meet.

    'Concentrate' = the team holds the plurality (largest share) of that band's
    departures AND more than one departure (a single departure is not a
    concentration). The team's parent department must carry a band gap in the
    gap_index for that same band (the driver) — the join key is (parent, band).

    Honesty gate (§3): this is NOT a departure-share ranking. It stores no rank and
    no share-of-total flag; it asserts the CROSS-FACT tie (a team's band-
    concentrated departures stand against that band's cross-source pay gap).
    Without the band there is no edge. Emitted only where a concentration and a
    driver align on the same band — wherever the data aligns them, for any
    entity."""
    edges: list[dict] = []
    # Map team -> parent (read from the membership values, the parent is data).
    team_parent: dict[str, str] = {}
    for team, rec in team_members.items():
        parent, _roster = _parse_team_member(rec["value"])
        team_parent[team] = parent

    for band in sorted(concentration):
        teams = concentration[band]
        # The plurality team in this band (largest share; ties broken by name for
        # determinism). A band with no departures contributes no synthesis.
        if not teams:
            continue
        top_team = max(sorted(teams), key=lambda g: teams[g]["share"])
        info = teams[top_team]
        if info["count"] <= 1:
            # Not a concentration — a lone departure is not a pattern. The absence
            # is the truth (logged), not a gap.
            logger.info(
                "[edge_derivation] band synthesis: band=%s top team=%s has only %d "
                "departure(s) — no team->driver tie (no concentration)",
                band, top_team, info["count"],
            )
            continue

        parent = team_parent.get(top_team)
        if parent is None:
            raise EdgeDerivationError(
                f"team {top_team!r} concentrates departures in band {band!r} but has no "
                f"{_GROUP_MEMBER_CONCEPT} entry to read its parent department from — "
                f"cannot resolve the band driver to join; fix the membership feed (A1)"
            )
        gap = gap_index.get((parent, band))
        if gap is None:
            # The team concentrates in a band with NO driver for its department —
            # there is nothing to tie to. Not a gap (the data is complete: this
            # band carries no cross-source driver for this department); logged.
            logger.info(
                "[edge_derivation] band synthesis: team=%s concentrates in band=%s "
                "but its parent dept=%s carries no band gap for that band — no tie "
                "(no driver on the join band)",
                top_team, band, parent,
            )
            continue

        props = {
            "team": top_team,
            "band": band,
            "concentration_departures": info["count"],
            "concentration_share": info["share"],
            "band_total_departures": info["band_total"],
            "joined_via": "band",
            "joined_gap_pct": gap["gap_pct"],
            "internal_median": gap["internal_median"],
            "market_median": gap["market_median"],
            "below_market": gap["below_market"],
            # What the tie consumed: the team-band departures AND the two source
            # concepts behind the band gap + the declared resolution (the gap is
            # the join of those three — the synthesis stands on all of them).
            "consumed": [
                _DEPARTURES_BY_GROUP_BAND_CONCEPT,
                _COMP_BAND_CONCEPT,
                _MARKET_BAND_CONCEPT,
                _RESOLUTION_CONCEPT,
            ],
            # The band BELOW_MARKET edge this tie stands on (drillable coordinate).
            "joined_driver_edge": {
                "src_type": gap["src_type"], "src_key": gap["src_key"],
                "edge_type": "BELOW_MARKET",
                "dst_type": gap["dst_type"], "dst_key": gap["dst_key"],
            },
        }
        edges.append(_edge(
            dcl_ingest_id,
            src_type="team", src_key=top_team,
            edge_type="DRIVEN_BY",
            dst_type=gap["dst_type"], dst_key=gap["dst_key"],
            properties=props, derivation="derived", confidence=0.95,
        ))
    return edges


def _register_edge_types(tenant_id: str) -> None:
    """Register the derived/structural edge types with correct allowed_pairs and
    cardinality BEFORE asserting, so the demo edges are ACCEPTED — not flagged to
    the conflict register's structural class."""
    put_edge_type(
        tenant_id, "BELOW_MARKET",
        "Cross-source comp-gap: an internal comp-band median sits below the "
        "external market median for the resolved external key (the resolution is "
        "declared in the entity's data). Derived at department level AND at "
        "(department, band) level (the band gap a band-concentrated group ties to). "
        "Properties carry the synthesized gap.",
        cardinality="many_to_one",
        allowed_pairs=[["department", "job_family"], ["department_band", "job_family_band"]],
    )
    put_edge_type(
        tenant_id, "DRIVEN_BY",
        "Two derived shapes share this type: (a) a department's attrition driven "
        "primarily by one exit theme (the dominant reason, ranked from the "
        "discovered exit-theme counts); (b) a team whose band-concentrated "
        "departures are tied to that band's cross-source comp-gap, joined via the "
        "band — the earned synthesis no single record holds.",
        cardinality="many_to_one",
        allowed_pairs=[["department", "exit_theme"], ["team", "job_family_band"]],
    )
    put_edge_type(
        tenant_id, "RESOLVES_TO",
        "Declared internal->external identity mapping (department -> job_family), "
        "read from the entity's data; enables the cross-source comp-gap join.",
        cardinality="one_to_one",
        allowed_pairs=[["department", "job_family"]],
    )
    put_edge_type(
        tenant_id, "HAS_DEPARTMENT",
        "Declared org structure: the enterprise org_unit contains this "
        "department. Enables the org -> department walk.",
        cardinality="one_to_many",
        allowed_pairs=[["org_unit", "department"]],
    )
    put_edge_type(
        tenant_id, "HAS_TEAM",
        "Declared org structure: a department contains this team (the team's "
        "parent is read from the membership feed). Enables the department -> team "
        "walk.",
        cardinality="one_to_many",
        allowed_pairs=[["department", "team"]],
    )


def derive_edges(tenant_id: str, entity_id: str, dcl_ingest_id: str) -> dict:
    """Derive the Stage-3 stitched graph for one entity and persist it.

    Reads Stage-2 current-state triples, derives the gap edges (rule 1), the exit
    driver, the declared resolution, the structural membership, and the band-
    driver synthesis (rules 2+3), registers the edge types, and writes the batch
    via EdgeStore.assert_edges(replace=True). Returns the counts and the edges.

    Identity (tenant_id + entity_id) is required (I2); one dcl_ingest_id stamps
    the whole batch (I1). Raises EdgeDerivationError (A1) on any unresolvable
    median or malformed structural value — never a silent skip.
    """
    if not tenant_id or not str(tenant_id).strip():
        raise EdgeDerivationError("tenant_id is required for edge derivation (I2)")
    if not entity_id or not str(entity_id).strip():
        raise EdgeDerivationError("entity_id is required for edge derivation (I2)")
    if not dcl_ingest_id or not str(dcl_ingest_id).strip():
        raise EdgeDerivationError("dcl_ingest_id is required to stamp the edge batch (I1)")

    comp = _read_current_triples(tenant_id, entity_id, _COMP_CONCEPT, _HEADLINE_PERIOD)
    market = _read_current_triples(tenant_id, entity_id, _MARKET_CONCEPT, _HEADLINE_PERIOD)
    resolution = _read_resolution(tenant_id, entity_id)
    exit_concepts = _discover_exit_concepts(tenant_id, entity_id, _HEADLINE_PERIOD)
    exit_by_reason: dict[str, dict[str, dict]] = {
        reason: _read_current_triples(tenant_id, entity_id, concept, _HEADLINE_PERIOD)
        for reason, concept in exit_concepts.items()
    }
    # Band/team inputs — band-level comp/market, the team membership, and the
    # team-band departure feed (rule 2's input).
    comp_band = _read_current_triples(tenant_id, entity_id, _COMP_BAND_CONCEPT, _HEADLINE_PERIOD)
    market_band = _read_current_triples(tenant_id, entity_id, _MARKET_BAND_CONCEPT, _HEADLINE_PERIOD)
    team_members = _read_current_triples(tenant_id, entity_id, _GROUP_MEMBER_CONCEPT, _HEADLINE_PERIOD)
    departures_by_team_band = _read_current_triples(
        tenant_id, entity_id, _DEPARTURES_BY_GROUP_BAND_CONCEPT, _HEADLINE_PERIOD
    )

    if not comp:
        raise EdgeDerivationError(
            f"no {_COMP_CONCEPT} triples found for tenant={tenant_id} entity={entity_id} "
            f"at period {_HEADLINE_PERIOD} — Stage-2 current state holds no comp band to "
            f"derive a graph from; refusing to write an empty graph silently"
        )
    if not resolution:
        raise EdgeDerivationError(
            f"no {_RESOLUTION_CONCEPT} triples found for tenant={tenant_id} "
            f"entity={entity_id} — the internal->external resolution the comp-gap "
            f"rule reads is not declared in the data; refusing to fall back to any "
            f"hardcoded mapping (the rules name nothing). Ingest the declared "
            f"resolution feed, do not skip"
        )

    # Rule 1 (department + band) — both forms read the declared resolution.
    comp_gap_edges = _derive_comp_gap(tenant_id, entity_id, dcl_ingest_id, comp, market, resolution)
    exit_driver_edges = _derive_exit_driver(tenant_id, entity_id, dcl_ingest_id, exit_by_reason)
    resolution_edges = _derive_resolution_edges(tenant_id, entity_id, dcl_ingest_id, comp, resolution)

    comp_gap_band_edges: list[dict] = []
    membership_edges: list[dict] = []
    synthesis_edges: list[dict] = []
    gap_index: dict[tuple[str, str], dict] = {}
    if comp_band:
        comp_gap_band_edges, gap_index = _derive_comp_gap_band(
            tenant_id, entity_id, dcl_ingest_id, comp_band, market_band, resolution
        )
    if team_members:
        membership_edges = _derive_membership(
            tenant_id, entity_id, dcl_ingest_id, comp, team_members
        )
    if departures_by_team_band:
        # Rule 2 (in-engine, not stored) feeds Rule 3 (the earned synthesis edge).
        concentration = _compute_band_concentration(departures_by_team_band)
        synthesis_edges = _derive_band_driver_synthesis(
            tenant_id, entity_id, dcl_ingest_id,
            concentration, gap_index, team_members,
        )

    all_edges = (
        comp_gap_edges + exit_driver_edges + resolution_edges
        + comp_gap_band_edges + membership_edges + synthesis_edges
    )

    _register_edge_types(tenant_id)
    store = EdgeStore()
    result = store.assert_edges(tenant_id, entity_id, all_edges, replace=True)

    if result.violations:
        raise EdgeDerivationError(
            f"edge derivation produced {len(result.violations)} constraint "
            f"violation(s) despite registered edge types — structural conflict, "
            f"not silently dropped: {result.violations}"
        )

    counts = {
        "comp_gap": len(comp_gap_edges),
        "exit_driver": len(exit_driver_edges),
        "resolution": len(resolution_edges),
        "comp_gap_band": len(comp_gap_band_edges),
        "membership": len(membership_edges),
        "synthesis": len(synthesis_edges),
        "total": len(all_edges),
        "written": result.written,
        "superseded": result.superseded,
    }
    logger.info(
        "[edge_derivation] tenant=%s entity=%s ingest=%s: derived %s",
        tenant_id, entity_id, dcl_ingest_id, counts,
    )
    return {
        "tenant_id": tenant_id,
        "entity_id": entity_id,
        "dcl_ingest_id": dcl_ingest_id,
        "counts": counts,
        "edges": all_edges,
    }
