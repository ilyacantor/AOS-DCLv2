"""Unit regression for the records-path operational classifier (DCL-owned metric catalog).

Labelled regression (not the B17 gate). Proves the deterministic source-metric -> canonical
(concept, property) mapping: feed period-keyed operational-KPI records, assert the canonical
concepts NLQ resolves come out with the right values, properties, periods, provenance; a drifted
field surfaces loudly.
"""
from backend.resolver.operational_records_aggregator import (
    aggregate_operational_records,
    OPERATIONAL_FIELD_CONCEPTS,
)


def _pipe():
    return {"pipe_id": "ops-1", "source_system": "snowflake",
            "fabric_plane": "bi", "fabric_product": "snowflake", "domain": "operations"}


def test_maps_operational_fields_to_canonical_concepts():
    records = [{
        "period": "2025-Q4",
        "sales_pipeline_total": 1406.12, "win_rate": 0.404, "headcount_total": 367,
        "headcount_engineering": 117, "sprint_velocity": 104.2, "p1_incidents": 3,
        "uptime_overall": 0.9967, "nrr": 1.14, "csat": 4.26,
    }]
    warnings: list = []
    out = aggregate_operational_records(
        entity_id="OpTest-AAAA", pipe=_pipe(), records=records, warnings=warnings,
    )
    by_concept = {p.concept: p for p in out}
    expected = {
        "sales.pipeline.total": (1406.12, "amount"), "sales.win_rate": (0.404, "rate"),
        "workforce.headcount.total": (367, "count"), "workforce.headcount.engineering": (117, "count"),
        "sprint_velocity.team": (104.2, "points"), "incident_count.p1": (3, "count"),
        "uptime_pct.overall": (0.9967, "rate"), "customer.nrr": (1.14, "rate"),
        "support.csat": (4.26, "score"),
    }
    for concept, (value, prop) in expected.items():
        assert concept in by_concept, f"missing canonical concept {concept}"
        p = by_concept[concept]
        assert p.value == value, f"{concept} value {p.value} != {value}"
        assert p.property == prop, f"{concept} property {p.property} != {prop}"
        assert p.period == "2025-Q4"
        assert p.fabric_plane == "bi"
    assert warnings == []


def test_unmapped_operational_field_warns_not_silent_drop():
    records = [{"period": "2025-Q4", "headcount_total": 367, "mystery_kpi": 9}]
    warnings: list = []
    out = aggregate_operational_records(
        entity_id="OpTest-AAAA", pipe=_pipe(), records=records, warnings=warnings,
    )
    assert "workforce.headcount.total" in {p.concept for p in out}
    assert len(warnings) == 1 and warnings[0]["field"] == "mystery_kpi"
    assert warnings[0]["type"] == "unmapped_operational_field"


def test_catalog_spans_the_cro_coo_cto_dimensions():
    roots = {c.split(".")[0] for c, _p, _u in OPERATIONAL_FIELD_CONCEPTS.values()}
    for must in ("sales", "customer", "workforce", "sprint_velocity", "engineering",
                 "incident_count", "uptime_pct", "support"):
        assert must in roots, f"operational catalog missing the {must} dimension"
