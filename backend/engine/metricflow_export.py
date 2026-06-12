"""MetricFlow-spec export of the DCL metric catalog (ContextOS Gate 2C).

Emits ONE multi-document YAML file in the exact minimal shape the
dbt-semantic-interfaces 0.10.5 parser + SemanticManifestValidator accept —
the shape was established by RUNNING that parser, not assumed:

  - a metrics-only file is NOT valid for the spec parser: it requires exactly
    one `project_configuration` document, and an empty
    `time_spine_table_configurations` list fails validation, so the file
    carries one fixed day-grain time-spine scaffold;
  - every simple metric must reference a measure that lives in a
    `semantic_model`, so the file carries one model (`dcl_metric_catalog`,
    node_relation = DCL's semantic_triples store) holding one measure per
    catalog metric;
  - a model with measures must declare a primary entity (`entity_id` — DCL's
    identity column) and the measures' agg_time_dimension (`period` — DCL's
    period column), whose granularity is the FINEST grain any catalog metric
    allows, so every metric's own grain validates against it;
  - `count` aggregations require an `expr` (validator rule) — count measures
    carry `expr: "1"` (row count).

Catalog derivation (campaign-pinned):
  - metric names  = DCL catalog ids, one `metric` document each, type simple
    (direct metrics only);
  - label         = catalog `name`, description = catalog `description`;
  - measure       = `{id}_measure`, agg mapped from the catalog `measure_op`
    (_MEASURE_AGG — an unknown op fails loudly, never defaults), with the
    original op + unit preserved verbatim in measure description and
    config.meta (`dcl_measure_op`, `dcl_unit`);
  - time          = metric.time_granularity from `default_grain`; the shared
    `period` dimension's granularity from the catalog-wide finest grain in
    `allowed_grains`/`default_grain`.

Output is deterministic: metrics and measures sorted by id, keys sorted by
the YAML dumper. No tenant identifier and no run identifier appears anywhere
in the body (I1/I2). No import-time DB or catalog access — the route passes
the already-loaded catalog in.
"""

from typing import Iterable, Sequence

import yaml

# DCL measure_op -> dbt-semantic-interfaces AggregationType value.
_MEASURE_AGG = {
    "sum": "sum",
    "avg": "average",
    "count": "count",
    "point_in_time_sum": "sum",       # point-in-time semantics preserved in meta
    "ratio": "average",               # ratios re-aggregate as averages
    "avg_days_between": "average",
}

_GRAIN_ORDER = ("day", "week", "month", "quarter", "year")

_SEMANTIC_MODEL_NAME = "dcl_metric_catalog"


def _grain_value(grain) -> str:
    """TimeGrain enum or plain string -> its string value."""
    return getattr(grain, "value", grain)


def _finest_grain(metrics: Iterable) -> str:
    """The finest time grain any catalog metric declares (floor: day)."""
    seen: set[str] = set()
    for m in metrics:
        seen.update(_grain_value(g) for g in (m.allowed_grains or []))
        if m.default_grain:
            seen.add(_grain_value(m.default_grain))
    if not seen:
        return "day"
    return min(seen, key=_GRAIN_ORDER.index)


def _measure_for(metric) -> dict:
    op = metric.measure_op
    agg = _MEASURE_AGG.get(op)
    if agg is None:
        raise ValueError(
            f"Metric {metric.id!r}: measure_op {op!r} has no MetricFlow aggregation "
            f"mapping ({sorted(_MEASURE_AGG)}) — refusing to emit an invalid catalog."
        )
    meta = {"dcl_measure_op": op}
    if metric.unit:
        meta["dcl_unit"] = metric.unit
    measure = {
        "name": f"{metric.id}_measure",
        "agg": agg,
        "description": (
            f"Measure for DCL metric '{metric.id}' "
            f"(measure_op={op}, unit={metric.unit or 'n/a'})"
        ),
        "config": {"meta": meta},
    }
    if agg == "count":
        measure["expr"] = "1"  # the spec validator requires an expr for count aggs
    return measure


def _metric_doc(metric) -> dict:
    doc = {
        "name": metric.id,
        "description": metric.description,
        "label": metric.name,
        "type": "simple",
        "type_params": {"measure": f"{metric.id}_measure"},
    }
    if metric.default_grain:
        doc["time_granularity"] = _grain_value(metric.default_grain)
    return {"metric": doc}


def build_metricflow_yaml(metrics: Sequence) -> str:
    """The full catalog as one MetricFlow-spec multi-document YAML string.

    `metrics` is the loaded DCL catalog (backend.api.semantic_export
    MetricDefinition objects — fields consumed: id, name, description,
    measure_op, unit, allowed_grains, default_grain).
    """
    if not metrics:
        raise ValueError(
            "DCL metric catalog is empty — refusing to emit a metrics export "
            "with no metrics (config/definitions/metrics.yaml failed to load?)."
        )
    ordered = sorted(metrics, key=lambda m: m.id)

    semantic_model = {
        "semantic_model": {
            "name": _SEMANTIC_MODEL_NAME,
            "description": (
                "DCL metric catalog projected over the semantic triple store. "
                "One measure per DCL metric; period is the shared aggregation "
                "time dimension."
            ),
            "node_relation": {"alias": "semantic_triples", "schema_name": "dcl"},
            "defaults": {"agg_time_dimension": "period"},
            "entities": [{"name": "entity_id", "type": "primary"}],
            "dimensions": [
                {
                    "name": "period",
                    "type": "time",
                    "type_params": {"time_granularity": _finest_grain(ordered)},
                }
            ],
            "measures": [_measure_for(m) for m in ordered],
        }
    }
    project_configuration = {
        "project_configuration": {
            # Required scaffolding: the spec parser demands exactly one
            # project_configuration with a non-empty time spine.
            "time_spine_table_configurations": [
                {"location": "dcl.time_spine", "column_name": "ds", "grain": "day"}
            ]
        }
    }
    documents = [semantic_model] + [_metric_doc(m) for m in ordered] + [project_configuration]
    return yaml.safe_dump_all(
        documents, sort_keys=True, default_flow_style=False, allow_unicode=True
    )
