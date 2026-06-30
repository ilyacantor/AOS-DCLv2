"""Unit regression for the cloud_spend team-efficiency aggregation (DCL ingest).

Labelled regression (not the B17 gate). Proves, in isolation, the records-path
cloud_spend math the estate demo depends on:
  - a COST pipe → cloud_spend.by_team.<team> (cost), drift-tolerant across cost
    sources, surfacing an unmappable cost LOUDLY (never silently zero'd);
  - a billing-only read is genuinely SHALLOW (total + by_service, no team/output);
  - an OUTPUT pipe → cloud_spend.output_by_team.<team> only (no fabricated cost);
  - the cross-pipe join cloud_spend.efficiency_by_team.<team> = round(cost/output, 2),
    emitted ONLY for teams present in BOTH signals — exact, no fabricated rows.

Mirrors how DCL's record_converter.convert_pipes drives it: aggregate each
cloud_spend pipe, then compute_team_efficiency over the combined payloads.
"""
import pytest

from backend.resolver.cloud_spend_aggregator import (
    aggregate_cloud_spend,
    compute_team_efficiency,
)

_ENTITY = "CloudFleet-USE1-7f3a"

_COST_PIPE = {
    "pipe_id": "e5af2b6d-8939-527d-83f1-88466f6643d0",
    "source_system": "aws_cost_explorer", "fabric_plane": "warehouse",
    "fabric_product": "aws_cost_explorer", "domain": "cloud_spend",
    "record_key_field": "resource_id",
}
_OUTPUT_PIPE = {
    "pipe_id": "5fa72b25-2c11-5ffb-a9e1-aef18a2369a5",
    "source_system": "deploy_analytics", "fabric_plane": "warehouse",
    "fabric_product": "deploy_analytics", "domain": "cloud_spend",
    "record_key_field": "team",
}


def _cost_rec(rid, team, cost, *, field="monthly_cost_usd", svc="AmazonEC2"):
    return {"resource_id": rid, "service_code": svc, "owner_cost_center": team,
            field: cost, "utilization": {"cpu_pct": 55.0}}


def _out_rec(team, deploys):
    return {"team": team, "deploys_per_month": deploys, "period": "2026-04"}


def _by(payloads, concept):
    return {p.property: p for p in payloads if p.concept == concept}


# --- COST pipe: per-team cost, exact -----------------------------------------

def test_cost_pipe_emits_by_team_exact():
    recs = [
        _cost_rec("i-1", "Eng-NA", 600.0), _cost_rec("i-2", "Eng-NA", 400.0),
        _cost_rec("i-3", "Plat-NorthAm", 900.0),
    ]
    warnings: list = []
    out = aggregate_cloud_spend(entity_id=_ENTITY, pipe=_COST_PIPE, records=recs, warnings=warnings)
    by_team = _by(out, "cloud_spend.by_team")
    assert by_team["eng_na"].value == 1000.0
    assert by_team["plat_northam"].value == 900.0
    assert by_team["eng_na"].confidence_tier == "exact"
    summary = _by(out, "cloud_spend.summary")
    assert summary["total_cost"].value == 1900.0
    assert warnings == []


# --- 5a Schema drift: a second cost source's column name is TOLERATED ---------

def test_cost_field_drift_is_tolerated_not_dropped():
    # A drifted source uses `cost_usd`; it must be counted, not silently zero'd.
    recs = [_cost_rec("b-1", "Eng-NA", 250.0, field="cost_usd"),
            _cost_rec("b-2", "Eng-NA", 250.0, field="cost_usd")]
    warnings: list = []
    out = aggregate_cloud_spend(entity_id=_ENTITY, pipe=_COST_PIPE, records=recs, warnings=warnings)
    assert _by(out, "cloud_spend.summary")["total_cost"].value == 500.0
    assert _by(out, "cloud_spend.by_team")["eng_na"].value == 500.0
    assert warnings == []


def test_unmappable_cost_surfaces_loudly_not_silently_zeroed():
    # One record carries NO recognized cost field → loud warning, excluded from
    # the total; the priced record still counts. Never a silent 0 (A1).
    recs = [_cost_rec("i-1", "Eng-NA", 700.0),
            {"resource_id": "i-bad", "service_code": "AmazonS3", "owner_cost_center": "Eng-NA"}]
    warnings: list = []
    out = aggregate_cloud_spend(entity_id=_ENTITY, pipe=_COST_PIPE, records=recs, warnings=warnings)
    assert _by(out, "cloud_spend.summary")["total_cost"].value == 700.0
    assert len(warnings) == 1
    w = warnings[0]
    assert w["type"] == "unmappable_cost_field"
    assert w["record_key"] == "i-bad"
    assert "monthly_cost_usd" in w["tried"]


# --- Shallow billing-only read: total + top service, NO team/output ----------

def test_billing_only_read_is_genuinely_shallow():
    # A billing export: cost (drifted column) + service only, no owner_cost_center,
    # no utilization. Yields total + by_service, but NO team/util/output attribution.
    recs = [{"service_code": "AmazonEC2", "cost_usd": 1200.0},
            {"service_code": "AmazonS3", "cost_usd": 300.0}]
    warnings: list = []
    out = aggregate_cloud_spend(entity_id=_ENTITY, pipe=_COST_PIPE, records=recs, warnings=warnings)
    assert _by(out, "cloud_spend.summary")["total_cost"].value == 1500.0
    assert "AmazonEC2" in _by(out, "cloud_spend.by_service")
    assert _by(out, "cloud_spend.by_team") == {}            # no team attribution
    assert _by(out, "cloud_spend.utilization") == {}        # no utilization claim
    assert _by(out, "cloud_spend.output_by_team") == {}     # no output
    eff = compute_team_efficiency(_ENTITY, out)
    assert eff == []                                        # nothing to join
    assert warnings == []


# --- OUTPUT pipe: output_by_team only, no fabricated cost ---------------------

def test_output_pipe_emits_output_by_team_only():
    recs = [_out_rec("Eng-NA", 50), _out_rec("Plat-NorthAm", 25), _out_rec("Sales North America", 10)]
    warnings: list = []
    out = aggregate_cloud_spend(entity_id=_ENTITY, pipe=_OUTPUT_PIPE, records=recs, warnings=warnings)
    obt = _by(out, "cloud_spend.output_by_team")
    assert obt["eng_na"].value == 50.0
    assert obt["plat_northam"].value == 25.0
    assert obt["eng_na"].unit == "deploys"
    # An output feed asserts NOTHING about spend — no fabricated cost summary.
    assert _by(out, "cloud_spend.summary") == {}
    assert _by(out, "cloud_spend.by_team") == {}


def test_pipe_with_both_cost_and_output_is_ambiguous_and_raises():
    bad = [{"resource_id": "x", "monthly_cost_usd": 10.0, "deploys_per_month": 3, "team": "Eng-NA"}]
    with pytest.raises(ValueError, match="ambiguous"):
        aggregate_cloud_spend(entity_id=_ENTITY, pipe=_COST_PIPE, records=bad, warnings=[])


# --- Cross-pipe efficiency = cost ÷ output, exact, only where BOTH present ----

def test_efficiency_joins_cost_and_output_per_team_exact():
    cost = aggregate_cloud_spend(
        entity_id=_ENTITY, pipe=_COST_PIPE, warnings=[],
        records=[_cost_rec("i-1", "Eng-NA", 600.0), _cost_rec("i-2", "Eng-NA", 400.0),
                 _cost_rec("i-3", "Plat-NorthAm", 900.0),
                 _cost_rec("i-4", "DevOps EMEA", 300.0)],  # cost, but NO output
    )
    output = aggregate_cloud_spend(
        entity_id=_ENTITY, pipe=_OUTPUT_PIPE, warnings=[],
        records=[_out_rec("Eng-NA", 50), _out_rec("Plat-NorthAm", 25),
                 _out_rec("Sales North America", 10)],     # output, but NO cost
    )
    combined = cost + output
    eff = {p.property: p for p in compute_team_efficiency(_ENTITY, combined)}

    # Eng-NA: 1000 / 50 = 20.0 ; Plat-NorthAm: 900 / 25 = 36.0 (exact)
    assert eff["eng_na"].value == 20.0
    assert eff["plat_northam"].value == 36.0
    assert eff["eng_na"].confidence_tier == "exact"
    assert eff["eng_na"].unit == "usd_per_deploys_per_month"
    # Teams with only ONE signal get NO efficiency row (no fabricated join).
    assert "devops_emea" not in eff          # cost only
    assert "sales_north_america" not in eff  # output only


def test_efficiency_empty_without_output_signal():
    cost = aggregate_cloud_spend(
        entity_id=_ENTITY, pipe=_COST_PIPE, warnings=[],
        records=[_cost_rec("i-1", "Eng-NA", 600.0)],
    )
    assert compute_team_efficiency(_ENTITY, cost) == []
