"""
Cross-sell propensity engine.

Scores non-overlapping customers of each entity against the other entity's
service catalog.  Two directions:
  A→B: entity_a services → entity_b's clients
  B→A: entity_b services → entity_a's clients

Scoring dimensions (0-100 total):
  - Industry match (0-25): customer industry vs product industry fit
  - Size match (0-20): company size vs product sweet spot
  - Behavioral signals (0-30): cross-sell-specific signals from customer profiles
  - Engagement fit (0-15): spending patterns suggesting need
  - Relationship strength (0-10): depth of existing relationship

Threshold: ≥40 = candidate.  >80 = high confidence.

Depends on:
  - data/customer_profiles.json (from Farm via generate_combining_data.py)
  - data/entity_overlap.json (for overlap exclusion)
  - engagement config for entity definitions and service catalog paths
"""

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.engine.engagement_config import get_engagement
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"


# =============================================================================
# Service catalogs
# =============================================================================

@dataclass
class ServiceOffering:
    name: str
    typical_buyer: str
    acv_low: float        # $
    acv_high: float       # $
    sweet_spot_industries: list[str]
    adjacent_industries: list[str]
    sweet_spot_employees_min: int
    sweet_spot_employees_max: int
    trigger_keywords: list[str]


def _load_service_catalog(catalog_path: str) -> list[ServiceOffering]:
    """Load service offerings from a service catalog JSON file.

    Args:
        catalog_path: Path to the service catalog JSON file (relative to repo root
                      or absolute).

    Returns:
        List of ServiceOffering dataclass instances.

    Raises:
        FileNotFoundError: If the catalog file does not exist.
    """
    path = Path(catalog_path)
    if not path.is_absolute():
        path = _DATA_DIR.parent / path
    if not path.exists():
        raise FileNotFoundError(
            f"Service catalog not found at {path}. "
            f"Ensure the engagement config points to a valid service catalog file."
        )
    with open(path) as f:
        raw = json.load(f)

    services: list[ServiceOffering] = []
    for s in raw.get("services", []):
        services.append(ServiceOffering(
            name=s["name"],
            typical_buyer=s["typical_buyer"],
            acv_low=s["acv_low"],
            acv_high=s["acv_high"],
            sweet_spot_industries=s.get("sweet_spot_industries", []),
            adjacent_industries=s.get("adjacent_industries", []),
            sweet_spot_employees_min=s.get("sweet_spot_employees_min", 0),
            sweet_spot_employees_max=s.get("sweet_spot_employees_max", 999_999),
            trigger_keywords=s.get("trigger_keywords", []),
        ))
    return services


# =============================================================================
# Scoring functions
# =============================================================================

def _industry_score(customer_industry: str, service: ServiceOffering) -> int:
    """Score 0-25 based on industry match."""
    if customer_industry in service.sweet_spot_industries:
        return 25
    if customer_industry in service.adjacent_industries:
        return 12
    return 0


def _size_score(employees: int, service: ServiceOffering) -> int:
    """Score 0-20 based on company size fit."""
    lo = service.sweet_spot_employees_min
    hi = service.sweet_spot_employees_max
    if lo <= employees <= hi:
        return 20
    # How far outside the range?
    if employees < lo:
        ratio = employees / lo
    else:
        ratio = hi / employees
    if ratio > 0.5:
        return 10
    if ratio > 0.2:
        return 4
    return 0


def _behavioral_score_a_to_b(customer: dict) -> int:
    """Score 0-30 for A→B direction (entity_a services → entity_b clients).

    Uses threshold-based scoring — only signals above baseline contribute.
    Most entity_b clients have moderate complexity; only the extreme ones are
    strong entity_a service candidates.
    """
    score = 0.0
    # process_complexity: only high values (>5) contribute meaningfully
    pc = customer.get("process_complexity", 0)
    score += max(0, (pc - 4) * 1.67)                                 # 0-10

    # regulatory_burden: only >2 contributes
    rb = customer.get("regulatory_burden", 0)
    score += max(0, (rb - 2) * 1.67)                                 # 0-5

    # recent_ma: only meaningful M&A activity
    ma = customer.get("recent_ma", 0)
    score += max(0, (ma - 2) * 1.67)                                 # 0-5

    # growth_rate: only high growth
    gr = customer.get("growth_rate", 0)
    score += max(0, (gr - 2.5) * 2.0)                                # 0-5

    # escalation_history: only notable history
    eh = customer.get("escalation_history", 0)
    score += max(0, (eh - 2) * 1.67)                                 # 0-5

    return min(int(round(score)), 30)


def _behavioral_score_b_to_a(customer: dict) -> int:
    """Score 0-30 for B→A direction (entity_b services → entity_a clients).

    Highly selective — most entity_a clients do NOT need entity_b services.
    Only customers with extreme process pain, high outsourcing readiness,
    AND recent transformation completion score well.
    """
    score = 0.0

    # manual_process_count: only high counts (>22) start scoring meaningfully
    mpc = customer.get("manual_process_count", 0)
    score += max(0, (mpc - 18) / 3.2)                                # 0-10

    # outsourcing_readiness: only >2.5 matters (most customers <2)
    readiness = customer.get("outsourcing_readiness", 0)
    score += max(0, (readiness - 2.0) * 2.0)                         # 0-6 → capped at 5

    # transformation_maturity: only mature orgs (>3.0) that know their gaps
    maturity = customer.get("transformation_maturity", 0)
    score += max(0, (maturity - 2.8) * 2.27)                         # 0-5

    # engagement_recency: only very recent (>3.0) suggests active need
    recency = customer.get("engagement_recency", 0)
    score += max(0, (recency - 2.8) * 2.27)                          # 0-5

    # expressed_interest: strongest signal — only if > 2.5
    interest = customer.get("expressed_interest", 0)
    score += max(0, (interest - 2.0) * 2.0)                          # 0-6 → capped at 5

    return min(int(round(score)), 30)


def _engagement_fit_a_to_b(customer: dict, service: ServiceOffering) -> int:
    """Score 0-15: engagement fit for A→B (entity_a services → entity_b client)."""
    score = 0.0
    complexity = customer.get("process_complexity", 0)
    reg = customer.get("regulatory_burden", 0)
    engagement_val = customer.get("engagement_value_M", 0)

    # High engagement + complexity = advisory need (strict thresholds)
    if engagement_val > 8 and complexity > 7:
        score += 8
    elif engagement_val > 4 and complexity > 5:
        score += 4
    elif engagement_val > 2 and complexity > 4:
        score += 2

    # Multi-service buying = cross-buy propensity
    services = customer.get("active_services", [])
    if len(services) >= 4:
        score += 4
    elif len(services) >= 3:
        score += 2

    # Regulated industry + high burden = compliance advisory need
    if reg > 4:
        score += 3
    elif reg > 3:
        score += 1

    return min(int(round(score)), 15)


def _engagement_fit_b_to_a(customer: dict, service: ServiceOffering) -> int:
    """Score 0-15: engagement fit for B→A (entity_b services → entity_a client).

    Very selective — requires strong convergence of signals.
    """
    score = 0.0
    mpc = customer.get("manual_process_count", 0)
    readiness = customer.get("outsourcing_readiness", 0)
    maturity = customer.get("transformation_maturity", 0)
    engagement_val = customer.get("engagement_value_M", 0)

    # Requires BOTH high maturity AND high manual process count
    if maturity > 3.8 and mpc > 28:
        score += 8
    elif maturity > 3.2 and mpc > 22:
        score += 5
    elif maturity > 2.8 and mpc > 18:
        score += 2

    # Very high manual process count = strong outsourcing signal
    if mpc > 32:
        score += 4
    elif mpc > 26:
        score += 2

    # Outsourcing readiness: only if high
    if readiness > 3.8:
        score += 3
    elif readiness > 3.0:
        score += 1

    return min(int(round(score)), 15)


def _relationship_score(customer: dict) -> int:
    """Score 0-10 based on relationship depth."""
    score = 0.0
    years = customer.get("years_as_client", 0)
    engagement_val = customer.get("engagement_value_M", 0)
    services = customer.get("active_services", [])

    # Years (longer = warmer intro)
    if years >= 5:
        score += 4
    elif years >= 3:
        score += 3
    elif years >= 1:
        score += 2

    # Contract size (larger = more trust)
    if engagement_val > 10:
        score += 3
    elif engagement_val > 2:
        score += 2
    elif engagement_val > 0.5:
        score += 1

    # Multi-product
    if len(services) >= 3:
        score += 3
    elif len(services) >= 2:
        score += 2
    elif len(services) >= 1:
        score += 1

    return min(int(round(score)), 10)


def _estimate_acv(
    service: ServiceOffering,
    customer: dict,
    behavioral_score: int,
    total_score: int,
) -> float:
    """Estimate ACV based on service range, customer size, and behavioral intensity.

    Enterprise customers with high propensity get premium ACV (top of range).
    Score intensity (60-100 mapped to 0-1) drives the position within the range.
    """
    lo = service.acv_low
    hi = service.acv_high

    # Size factor: larger companies → higher ACV (log scale)
    employees = customer.get("employees", 1000)
    size_factor = min(math.log10(max(employees, 100)) / math.log10(100_000), 1.0)

    # Score intensity: map 60-100 to 0-1 (since threshold is 60)
    score_factor = min(max((total_score - 60) / 40.0, 0.0), 1.0)

    # Behavioral intensity
    intensity = behavioral_score / 30.0

    # Segment multiplier: Enterprise clients pay more
    segment = customer.get("segment", "Mid-Market")
    if segment == "Enterprise":
        seg_mult = 2.3
    elif segment == "Mid-Market":
        seg_mult = 1.15
    else:
        seg_mult = 0.65

    # Weighted blend
    blend = 0.3 * size_factor + 0.3 * score_factor + 0.2 * intensity + 0.2 * (seg_mult / 1.8)
    acv = lo + (hi - lo) * blend * seg_mult

    # Cap at 2x the high end for very large Enterprise deals
    acv = min(acv, hi * 2.0)

    return round(acv, -3)  # round to nearest $1K


def _buyer_persona(service: ServiceOffering) -> str:
    """Extract primary buyer persona from service."""
    return service.typical_buyer.split("/")[0]


# =============================================================================
# Comparable customer finder
# =============================================================================

def _find_comparable(
    customer: dict,
    existing_customers: list[dict],
    service_name: str,
    direction: str,
) -> list[str]:
    """Find 1-2 existing customers that look like this candidate.

    Finds clients in similar industry/size buying similar services from
    the entity that owns the recommended service.
    """
    comparables = []
    industry = customer.get("industry", "")
    employees = customer.get("employees", 0)

    for ec in existing_customers:
        if ec.get("industry") != industry:
            continue
        ec_emp = ec.get("employees", 0)
        if ec_emp == 0:
            continue
        size_ratio = min(employees, ec_emp) / max(employees, ec_emp) if max(employees, ec_emp) > 0 else 0
        if size_ratio < 0.3:
            continue
        ec_services = ec.get("active_services", [])
        if not ec_services:
            continue
        eng_val = ec.get("engagement_value_M", 0)
        entity_id = ec.get("entity_id", "")
        comparables.append(
            f"{ec['customer_name']} ({entity_id} client, ${eng_val:.1f}M engagement)"
        )
        if len(comparables) >= 2:
            break

    return comparables


# =============================================================================
# Rationale generator
# =============================================================================

def _generate_rationale_a_to_b(customer: dict, service: ServiceOffering, scores: dict) -> str:
    """Generate human-readable rationale for A→B recommendation."""
    parts = []
    eng_val = customer.get("engagement_value_M", 0)
    services = customer.get("active_services", [])
    complexity = customer.get("process_complexity", 0)
    reg = customer.get("regulatory_burden", 0)

    parts.append(f"Client with ${eng_val:.1f}M engagement across {len(services)} services.")
    if complexity > 7:
        parts.append(f"High process complexity ({complexity:.1f}/10) indicates advisory need.")
    if reg > 3:
        parts.append(f"Elevated regulatory burden ({reg:.1f}/5) aligns with {service.name}.")
    if customer.get("recent_ma", 0) > 3:
        parts.append("Recent M&A activity creates integration advisory opportunity.")
    if customer.get("growth_rate", 0) > 3.5:
        parts.append("Strong growth trajectory suggests strategic advisory need.")

    return " ".join(parts)


def _generate_rationale_b_to_a(customer: dict, service: ServiceOffering, scores: dict) -> str:
    """Generate human-readable rationale for B→A recommendation."""
    parts = []
    eng_val = customer.get("engagement_value_M", 0)
    mpc = customer.get("manual_process_count", 0)
    readiness = customer.get("outsourcing_readiness", 0)
    maturity = customer.get("transformation_maturity", 0)
    last_end = customer.get("last_project_end")

    parts.append(f"Client with ${eng_val:.1f}M engagement and {customer.get('completed_projects', 0)} completed projects.")
    if mpc > 20:
        parts.append(f"{mpc} manual processes identified — strong {service.name} candidate.")
    elif mpc > 10:
        parts.append(f"{mpc} manual processes suggest {service.name} opportunity.")
    if readiness > 3:
        parts.append(f"High outsourcing readiness ({readiness:.1f}/5).")
    if maturity > 3 and last_end:
        parts.append(f"Transformation maturity {maturity:.1f}/5; last project ended {last_end} — natural handoff.")
    if customer.get("expressed_interest", 0) > 3:
        parts.append("Has expressed interest in outsourcing/managed services.")

    return " ".join(parts)


# =============================================================================
# Main engine
# =============================================================================

@dataclass
class CrossSellCandidate:
    """A single cross-sell recommendation."""
    customer_id: str
    customer_name: str
    entity_id: str             # which entity the customer belongs to
    recommended_service: str   # service from the OTHER entity
    propensity_score: int      # 0-100
    estimated_acv: float       # $
    industry_match: int
    size_match: int
    behavioral_score: int
    engagement_fit: int
    relationship_strength: int
    rationale: str
    comparable_customers: list[str]
    buyer_persona: str
    customer_engagement_M: float
    years_as_client: int
    industry: str
    segment: str


@dataclass
class CrossSellPipeline:
    """Full cross-sell analysis output."""
    a_to_b: list[CrossSellCandidate]  # entity_a services → entity_b's clients
    b_to_a: list[CrossSellCandidate]  # entity_b services → entity_a's clients

    def to_dict(self) -> dict:
        def _candidate_dict(c: CrossSellCandidate) -> dict:
            return {
                "customer_id": c.customer_id,
                "customer_name": c.customer_name,
                "entity_id": c.entity_id,
                "recommended_service": c.recommended_service,
                "propensity_score": c.propensity_score,
                "estimated_acv": c.estimated_acv,
                "industry_match": c.industry_match,
                "size_match": c.size_match,
                "behavioral_score": c.behavioral_score,
                "engagement_fit": c.engagement_fit,
                "relationship_strength": c.relationship_strength,
                "rationale": c.rationale,
                "comparable_customers": c.comparable_customers,
                "buyer_persona": c.buyer_persona,
                "customer_engagement_M": c.customer_engagement_M,
                "years_as_client": c.years_as_client,
                "industry": c.industry,
                "segment": c.segment,
            }

        a_to_b_list = sorted(
            [_candidate_dict(c) for c in self.a_to_b],
            key=lambda x: x["propensity_score"], reverse=True,
        )
        b_to_a_list = sorted(
            [_candidate_dict(c) for c in self.b_to_a],
            key=lambda x: x["propensity_score"], reverse=True,
        )

        a_to_b_total_acv = sum(c.estimated_acv for c in self.a_to_b)
        a_to_b_high = [c for c in self.a_to_b if c.propensity_score > 80]
        b_to_a_total_acv = sum(c.estimated_acv for c in self.b_to_a)
        b_to_a_high = [c for c in self.b_to_a if c.propensity_score > 80]

        result = {
            # Entity-agnostic keys
            "a_to_b": a_to_b_list,
            "b_to_a": b_to_a_list,
            # Backward-compatible keys (same data)
            "m_to_c": a_to_b_list,
            "c_to_m": b_to_a_list,
            "summary": {
                "a_to_b_candidates": len(self.a_to_b),
                "a_to_b_total_acv": round(a_to_b_total_acv),
                "a_to_b_high_conf_count": len(a_to_b_high),
                "a_to_b_high_conf_acv": round(sum(c.estimated_acv for c in a_to_b_high)),
                "b_to_a_candidates": len(self.b_to_a),
                "b_to_a_total_acv": round(b_to_a_total_acv),
                "b_to_a_high_conf_count": len(b_to_a_high),
                "b_to_a_high_conf_acv": round(sum(c.estimated_acv for c in b_to_a_high)),
                "total_candidates": len(self.a_to_b) + len(self.b_to_a),
                "total_pipeline_acv": round(a_to_b_total_acv + b_to_a_total_acv),
                "total_high_conf_acv": round(
                    sum(c.estimated_acv for c in a_to_b_high) +
                    sum(c.estimated_acv for c in b_to_a_high)
                ),
                # Backward-compatible summary keys
                "m_to_c_candidates": len(self.a_to_b),
                "m_to_c_total_acv": round(a_to_b_total_acv),
                "m_to_c_high_conf_count": len(a_to_b_high),
                "m_to_c_high_conf_acv": round(sum(c.estimated_acv for c in a_to_b_high)),
                "c_to_m_candidates": len(self.b_to_a),
                "c_to_m_total_acv": round(b_to_a_total_acv),
                "c_to_m_high_conf_count": len(b_to_a_high),
                "c_to_m_high_conf_acv": round(sum(c.estimated_acv for c in b_to_a_high)),
            },
        }
        return result


def run_cross_sell_engine() -> CrossSellPipeline:
    """Run the cross-sell propensity engine using pre-generated customer profiles.

    Loads the engagement config to determine entity identities and service catalog
    paths, then loads customer_profiles.json and entity_overlap.json, scores every
    non-overlapping customer against the other entity's service catalog,
    and returns the pipeline of candidates scoring >=60.

    Raises FileNotFoundError if data files are missing.
    """
    engagement = get_engagement()
    entity_a = engagement.entity_a
    entity_b = engagement.entity_b

    # Load service catalogs from engagement config paths
    entity_a_services = _load_service_catalog(entity_a.service_catalog)
    entity_b_services = _load_service_catalog(entity_b.service_catalog)

    # Load customer profiles
    profiles_path = _DATA_DIR / "customer_profiles.json"
    if not profiles_path.exists():
        raise FileNotFoundError(
            f"Customer profiles not found at {profiles_path}. "
            f"Run scripts/generate_combining_data.py first."
        )
    with open(profiles_path) as f:
        profiles = json.load(f)

    # Load overlap data (to verify exclusion)
    overlap_path = _DATA_DIR / "entity_overlap.json"
    if not overlap_path.exists():
        raise FileNotFoundError(
            f"Entity overlap data not found at {overlap_path}. "
            f"Run scripts/generate_combining_data.py first."
        )
    with open(overlap_path) as f:
        overlap = json.load(f)

    overlap_canonical_names = {
        m["canonical_name"].lower()
        for m in overlap.get("customer_overlap", {}).get("matches", [])
    }

    # Use overlap_keys from engagement config for customer list field names
    entity_a_customers_key = engagement.overlap_keys.entity_a_customers
    entity_b_customers_key = engagement.overlap_keys.entity_b_customers

    entity_a_customers = profiles.get(entity_a_customers_key, [])
    entity_b_customers = profiles.get(entity_b_customers_key, [])

    # Non-overlapping customers only
    a_non_overlap = [c for c in entity_a_customers if not c.get("is_overlap", False)]
    b_non_overlap = [c for c in entity_b_customers if not c.get("is_overlap", False)]

    logger.info(
        "[cross_sell] Scoring %d %s non-overlap customers against %d %s services",
        len(a_non_overlap), entity_a.name, len(entity_b_services), entity_b.name,
    )
    logger.info(
        "[cross_sell] Scoring %d %s non-overlap customers against %d %s services",
        len(b_non_overlap), entity_b.name, len(entity_a_services), entity_a.name,
    )

    # -- A→B: Score entity_b clients for entity_a services --
    a_to_b_candidates: list[CrossSellCandidate] = []
    for customer in b_non_overlap:
        best_score = 0
        best_candidate: Optional[CrossSellCandidate] = None
        for service in entity_a_services:
            ind = _industry_score(customer.get("industry", ""), service)
            siz = _size_score(customer.get("employees", 0), service)
            beh = _behavioral_score_a_to_b(customer)
            eng = _engagement_fit_a_to_b(customer, service)
            rel = _relationship_score(customer)
            total = ind + siz + beh + eng + rel

            if total >= 60 and total > best_score:
                acv = _estimate_acv(service, customer, beh, total)
                scores = {"industry": ind, "size": siz, "behavioral": beh, "engagement": eng, "relationship": rel}
                rationale = _generate_rationale_a_to_b(customer, service, scores)
                comparables = _find_comparable(customer, entity_a_customers, service.name, "a_to_b")
                best_score = total
                best_candidate = CrossSellCandidate(
                    customer_id=customer["customer_id"],
                    customer_name=customer["customer_name"],
                    entity_id=entity_b.id,
                    recommended_service=service.name,
                    propensity_score=total,
                    estimated_acv=acv,
                    industry_match=ind,
                    size_match=siz,
                    behavioral_score=beh,
                    engagement_fit=eng,
                    relationship_strength=rel,
                    rationale=rationale,
                    comparable_customers=comparables,
                    buyer_persona=_buyer_persona(service),
                    customer_engagement_M=customer.get("engagement_value_M", 0),
                    years_as_client=customer.get("years_as_client", 0),
                    industry=customer.get("industry", ""),
                    segment=customer.get("segment", ""),
                )

        if best_candidate:
            a_to_b_candidates.append(best_candidate)

    # -- B→A: Score entity_a clients for entity_b services --
    b_to_a_candidates: list[CrossSellCandidate] = []
    for customer in a_non_overlap:
        best_score = 0
        best_candidate: Optional[CrossSellCandidate] = None
        for service in entity_b_services:
            ind = _industry_score(customer.get("industry", ""), service)
            siz = _size_score(customer.get("employees", 0), service)
            beh = _behavioral_score_b_to_a(customer)
            eng = _engagement_fit_b_to_a(customer, service)
            rel = _relationship_score(customer)
            total = ind + siz + beh + eng + rel

            if total >= 60 and total > best_score:
                acv = _estimate_acv(service, customer, beh, total)
                scores = {"industry": ind, "size": siz, "behavioral": beh, "engagement": eng, "relationship": rel}
                rationale = _generate_rationale_b_to_a(customer, service, scores)
                comparables = _find_comparable(customer, entity_b_customers, service.name, "b_to_a")
                best_score = total
                best_candidate = CrossSellCandidate(
                    customer_id=customer["customer_id"],
                    customer_name=customer["customer_name"],
                    entity_id=entity_a.id,
                    recommended_service=service.name,
                    propensity_score=total,
                    estimated_acv=acv,
                    industry_match=ind,
                    size_match=siz,
                    behavioral_score=beh,
                    engagement_fit=eng,
                    relationship_strength=rel,
                    rationale=rationale,
                    comparable_customers=comparables,
                    buyer_persona=_buyer_persona(service),
                    customer_engagement_M=customer.get("engagement_value_M", 0),
                    years_as_client=customer.get("years_as_client", 0),
                    industry=customer.get("industry", ""),
                    segment=customer.get("segment", ""),
                )

        if best_candidate:
            b_to_a_candidates.append(best_candidate)

    # Verify no overlap customers snuck in
    for c in a_to_b_candidates + b_to_a_candidates:
        cname = c.customer_name.lower()
        if cname in overlap_canonical_names:
            raise RuntimeError(
                f"Cross-sell engine bug: overlap customer '{c.customer_name}' "
                f"appeared as a cross-sell candidate. This should not happen."
            )

    pipeline = CrossSellPipeline(a_to_b=a_to_b_candidates, b_to_a=b_to_a_candidates)
    summary = pipeline.to_dict()["summary"]
    logger.info(
        "[cross_sell] Pipeline complete: %s→%s=%d candidates ($%dM), %s→%s=%d candidates ($%dM), "
        "Total pipeline=$%dM, High-conf=$%dM",
        entity_a.name, entity_b.name,
        summary["a_to_b_candidates"],
        summary["a_to_b_total_acv"] // 1_000_000,
        entity_b.name, entity_a.name,
        summary["b_to_a_candidates"],
        summary["b_to_a_total_acv"] // 1_000_000,
        summary["total_pipeline_acv"] // 1_000_000,
        summary["total_high_conf_acv"] // 1_000_000,
    )

    return pipeline
