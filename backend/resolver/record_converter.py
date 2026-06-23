"""Inbound record -> triple converter (the "real fabric connect" mapping step).

Given raw enterprise records + per-pipe metadata from AAM's transport, this runs
DCL's own mapping + resolution + conversion — the work that used to live in AAM
(AAM Blueprint v3.1 §3.6 decision (c)):

  1. SOURCE NORMALIZATION — canonicalize the pipe's source_system via DCL's
     normalize_source_id (the single source-id normalizer the source_normalizer
     itself uses).
  2. MAPPING — classify the pipe's fields with DCL's Live Semantic Mapper
     (HeuristicMapper, called directly — no DB persistence side effect).
       * domain-tagged pipe (AAM SE path: customer / vendor / invoice):
         concept = the declared domain (a persona-routed business concept),
         property = the canonical predicate from semantic_mapper.property_aliases
         (config/concept_property_aliases.yaml) — e.g. company_name -> name,
         status -> payment_status — defaulting to the source field name when no
         alias is registered. This matches AAM's triple vocabulary (the field->
         property remap ported from AAM's mappings.py, #59 Option A) and keeps
         utility fields like `currency` as PROPERTIES of the business concept
         rather than standalone non-persona concepts.
       * domainless pipe (e.g. cloud-spend metrics): concept = the Live Mapper's
         per-field classification.
     A field whose concept is not routed to any persona is DROPPED with a loud
     warning (never silently) — the ingest-triples 422 guard stays intact for
     Farm; the records path degrades loudly instead of failing the whole batch.
  3. RESOLUTION — when the pipe declares domain + identity_key, resolve that
     field's value through the SE-path RecordResolver (4-tier fuzzy + HITL). The
     verdict (canonical_id / resolution_method / resolution_confidence) is
     attached to EVERY triple built from that record.
  4. CONVERSION — emit TriplePayload objects with full provenance for the shared
     ingest persistence path.

The converter writes no triples; it returns payloads + warnings + a resolution
summary for the endpoint to persist and report.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from backend.api.routes.ingest_triples import TriplePayload
from backend.aam.ingress import normalize_source_id
from backend.db.canonical_registry import CanonicalRegistry
from backend.domain.models import FieldSchema, Mapping, SourceSystem, TableSchema
from backend.engine.ontology import get_ontology
from backend.engine.persona_view import get_persona_domain_mapping
from backend.resolver.record_resolver import RecordResolver
from backend.semantic_mapper.heuristic_mapper import HeuristicMapper
from backend.semantic_mapper.property_aliases import canonical_property
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Resolver's richer method vocabulary -> the triple store's resolution_method
# CHECK (deterministic / fuzzy / manual / NULL). Same translation AAM applied at
# its write boundary, kept identical so retiring AAM's copy changes nothing.
_RESOLUTION_METHOD_TO_PG = {
    "exact": "deterministic",
    "alias": "deterministic",
    "pattern": "deterministic",
    "discovery": "deterministic",
    "fuzzy": "fuzzy",
    "hitl_pending": "fuzzy",
    "hitl_confirmed": "manual",
    "rejected": None,
}

# Confidence the operator-declared domain mapping carries when the Live Mapper
# has no opinion on the field — mirrors AAM's explicit FieldMapping confidence.
_DECLARED_CONFIDENCE = 0.95


def _tier(score: float) -> str:
    """Confidence float -> tier. Ported from AAM's triples._tier."""
    if score >= 0.95:
        return "exact"
    if score >= 0.75:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def _json_safe(value: Any) -> Any:
    """Coerce a raw record value to a JSON-encodable primitive.

    The triple value column is JSONB and the store json.dumps() it with no custom
    encoder — a datetime/Decimal/etc. would raise mid-write. Native JSON types
    pass through; everything else is stringified.
    """
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)


def _infer_field(name: str, sample: Any) -> FieldSchema:
    """Build a FieldSchema for the Live Mapper from a field name + sample value."""
    is_number = isinstance(sample, (int, float)) and not isinstance(sample, bool)
    ftype = "number" if is_number else "string"
    lname = name.lower()
    hint: Optional[str] = None
    if lname == "id" or lname.endswith("_id"):
        hint = "id"
    elif is_number and any(k in lname for k in ("amount", "cost", "price", "revenue", "spend", "total")):
        hint = "amount"
    return FieldSchema(name=name, type=ftype, semantic_hint=hint)


@dataclass
class ConversionResult:
    payloads: list[TriplePayload] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)
    # method -> count, for the operator-visible response summary
    resolution_summary: dict = field(default_factory=dict)
    hitl_queue_ids: list[str] = field(default_factory=list)
    # field->concept classifications learned this conversion, for the live graph
    # (persisted to field_concept_mappings by the endpoint — see
    # derive_field_mappings).
    mappings: list[Mapping] = field(default_factory=list)


def derive_field_mappings(payloads: list[TriplePayload]) -> list[Mapping]:
    """Field->concept mappings the converted triples imply, for the semantic graph.

    Every converted triple is evidence that a source field was classified as a
    concept. rebuild_graph()/build_graph_snapshot() read field_concept_mappings to
    build the CLASSIFIED_AS edges /resolve traverses; without this the records path
    leaves the graph blind to its own ingest (the deprecated pipe path was the only
    writer — deferred #76).

    Derived from the PAYLOADS, not the raw HeuristicMapper output: domain-tagged
    pipes force concept=domain (and the aggregators emit their own concepts), so the
    mapping must carry exactly the concept the triple carries — otherwise the graph
    would know a field under one concept while the facts live under another and
    resolve would find a source with no data. Deduped to the highest confidence per
    (source_system, source_table, source_field, concept); method is "heuristic"
    (DCL's own rule-based classification — the records path builds no AAM edge index,
    so never aam_edge, and is not rag/llm).
    """
    best: dict[tuple, float] = {}
    skipped = 0
    for p in payloads:
        if not p.source_field or not p.source_table:
            skipped += 1
            continue
        key = (p.source_system, p.source_table, p.source_field, p.concept)
        conf = float(p.confidence_score)
        if key not in best or conf > best[key]:
            best[key] = conf
    if skipped:
        logger.warning(
            "[records-mappings] %d payload(s) lacked source_field/source_table — "
            "no field->concept mapping derived for them (triples still written)",
            skipped,
        )
    return [
        Mapping(
            id=f"{ss}_{st}_{sf}_{concept}",
            source_field=sf, source_table=st, source_system=ss,
            ontology_concept=concept, confidence=conf,
            method="heuristic", status="ok",
        )
        for (ss, st, sf, concept), conf in best.items()
    ]


class RecordConverter:
    """Maps + resolves + converts records to TriplePayloads.

    Holds a process-lifetime CanonicalRegistry (its TTL snapshot cache must stay
    warm across a batch) and the ontology concept list for the Live Mapper.
    """

    def __init__(self, registry: Optional[CanonicalRegistry] = None) -> None:
        self._registry = registry or CanonicalRegistry()
        self._resolver = RecordResolver(self._registry)
        self._ontology_dicts = [
            {
                "id": c.id, "concept_id": c.concept_id, "name": c.name,
                "description": c.description, "domain": c.domain, "cluster": c.cluster,
                "example_fields": c.example_fields, "aliases": c.aliases,
                "expected_type": c.expected_type,
                "typical_source_systems": c.typical_source_systems,
                "persona_relevance": c.persona_relevance,
            }
            for c in get_ontology()
        ]
        self._persona_prefixes = frozenset(
            d for domains in get_persona_domain_mapping().values() for d in domains
        )

    def convert_pipes(self, *, tenant_id: str, entity_id: str, pipes: list[dict]) -> ConversionResult:
        result = ConversionResult()
        for pipe in pipes:
            self._convert_one_pipe(tenant_id, entity_id, pipe, result)
        # The field->concept classifications this batch implies — persisted by the
        # endpoint so the graph can resolve them (deferred #76).
        result.mappings = derive_field_mappings(result.payloads)
        return result

    def _classify_fields(self, *, source_id: str, pipe_id: str, table_name: str,
                         records: list[dict]) -> dict[str, Any]:
        """Run the Live Semantic Mapper over the pipe's field union.

        Returns {field_name: Mapping}. Pure (no DB persistence).
        """
        sample_by_field: dict[str, Any] = {}
        for rec in records:
            for k, v in rec.items():
                if k not in sample_by_field and v is not None:
                    sample_by_field[k] = v
        # Include keys that were always null too, so they are classified/warned.
        for rec in records:
            for k in rec.keys():
                sample_by_field.setdefault(k, None)
        fields = [_infer_field(name, sample) for name, sample in sample_by_field.items()]
        source = SourceSystem(
            id=source_id, name=source_id, type="ingest",
            tables=[TableSchema(id=pipe_id, system_id=source_id, name=table_name, fields=fields)],
        )
        mappings = HeuristicMapper(self._ontology_dicts).create_mappings([source])
        return {m.source_field: m for m in mappings}

    def _convert_one_pipe(self, tenant_id: str, entity_id: str, pipe: dict,
                          result: ConversionResult) -> None:
        pipe_id = pipe["pipe_id"]
        raw_source = pipe["source_system"]
        source_system = normalize_source_id(raw_source)
        fabric_plane = pipe.get("fabric_plane")
        fabric_product = pipe.get("fabric_product")
        domain = (pipe.get("domain") or "").strip() or None
        # Cloud-spend pipes carry a metric fleet, not party records. NLQ's
        # cloud-spend metrics are direct lookups of pre-aggregated concepts
        # (cloud_spend.summary.total_cost, cloud_spend.by_service.<svc>, ...)
        # that per-field mapping cannot produce. Compute the fleet aggregates
        # instead — cloud-spend aggregation lives in DCL ingest.
        if domain == "cloud_spend":
            from backend.resolver.cloud_spend_aggregator import aggregate_cloud_spend
            result.payloads.extend(aggregate_cloud_spend(
                entity_id=entity_id, pipe=pipe, records=pipe.get("records") or [],
            ))
            return
        # Financial-statement pipe: period-keyed P&L/BS/CF bundles. DCL owns the
        # account->canonical-concept map (the SE financial cutover — concept formation
        # moves Farm->DCL via ingest-records). Like cloud_spend, this REPLACES the
        # per-record path: the records are metric bundles, not party records.
        if domain == "financials":
            from backend.resolver.financial_records_aggregator import aggregate_financial_records
            result.payloads.extend(aggregate_financial_records(
                entity_id=entity_id, pipe=pipe, records=pipe.get("records") or [],
                warnings=result.warnings,
            ))
            return
        # Operational-metrics pipe: period-keyed sales/workforce/eng/uptime/support KPIs.
        # DCL owns the metric catalog (operational counterpart to the CoA map). Like
        # financials, REPLACES per-record — these are metric bundles, not party records.
        if domain == "operations":
            from backend.resolver.operational_records_aggregator import aggregate_operational_records
            result.payloads.extend(aggregate_operational_records(
                entity_id=entity_id, pipe=pipe, records=pipe.get("records") or [],
                warnings=result.warnings,
            ))
            return
        # Raw-ledger pipe: per-record detail (gl/coa/journal_entry/invoice/AP/AR/ebitda_adjustment
        # + observability/ops). DCL composes concept = root.key from the record's structural fields
        # (no fixed catalog — thousands of distinct ledger keys). Like financials/operations, this
        # REPLACES per-record + bypasses the persona guard (full-depth detail, not a persona tile).
        if domain == "ledger":
            from backend.resolver.ledger_records_aggregator import aggregate_ledger_records
            result.payloads.extend(aggregate_ledger_records(
                entity_id=entity_id, pipe=pipe, records=pipe.get("records") or [],
                warnings=result.warnings,
            ))
            return
        identity_key = (pipe.get("identity_key") or "").strip() or None
        record_key_field = pipe.get("record_key_field") or "id"
        records = pipe.get("records") or []
        table_name = domain or str(pipe_id)

        field_map = self._classify_fields(
            source_id=source_system, pipe_id=str(pipe_id),
            table_name=table_name, records=records,
        )

        for rec_idx, record in enumerate(records):
            # --- Resolution (only when the pipe declares a party identity) ---
            resolution = None
            if domain and identity_key:
                res = self._resolver.resolve(
                    record, domain=domain, pipe_id=str(pipe_id),
                    tenant_id=tenant_id, entity_id=entity_id,
                    value_field=identity_key, record_key_field=record_key_field,
                )
                pg_method = _RESOLUTION_METHOD_TO_PG.get(res.resolution_method, None)
                resolution = {
                    "canonical_id": res.canonical_id if pg_method else None,
                    "resolution_method": pg_method,
                    "resolution_confidence": res.resolution_confidence if pg_method else None,
                }
                result.resolution_summary[res.resolution_method] = (
                    result.resolution_summary.get(res.resolution_method, 0) + 1
                )
                if res.hitl_queue_id:
                    result.hitl_queue_ids.append(res.hitl_queue_id)
                if res.resolution_method == "rejected":
                    result.warnings.append({
                        "type": "identity_rejected", "pipe_id": str(pipe_id),
                        "record_key": str(record.get(record_key_field) or rec_idx),
                        "value": str(record.get(identity_key)),
                        "detail": "no canonical above fuzzy_threshold; triple written with null resolution",
                    })

            # --- period: stamp a `period`-named field's value on every triple ---
            record_period = None
            for k, v in record.items():
                if k.lower() == "period" and v is not None and str(v).strip():
                    record_period = str(v)
                    break

            # --- Map + emit one triple per field ---
            for fname, value in record.items():
                mapped = field_map.get(fname)
                if domain:
                    concept = domain
                    confidence = (mapped.confidence if (mapped and mapped.ontology_concept == domain)
                                  else _DECLARED_CONFIDENCE)
                else:
                    if not mapped:
                        result.warnings.append({
                            "type": "unmapped_field", "pipe_id": str(pipe_id),
                            "field": fname,
                            "detail": "Live Semantic Mapper produced no concept; field not converted",
                        })
                        continue
                    concept = mapped.ontology_concept
                    confidence = mapped.confidence

                # Persona-prefix guard: a concept not routed to any persona cannot
                # be placed on the graph. Drop loudly (the triples 422 guard stays
                # for Farm; here we degrade with a warning rather than 422 the batch).
                if concept.split(".", 1)[0] not in self._persona_prefixes:
                    result.warnings.append({
                        "type": "non_persona_concept", "pipe_id": str(pipe_id),
                        "field": fname, "concept": concept,
                        "detail": "concept not routed to any persona; field not converted",
                    })
                    continue

                payload = TriplePayload(
                    entity_id=entity_id,
                    concept=concept,
                    property=canonical_property(concept, fname),
                    value=_json_safe(value),
                    period=record_period,
                    currency="USD",
                    unit=None,
                    source_system=source_system,
                    source_table=f"fabric_via:{raw_source}",
                    source_field=fname,
                    pipe_id=str(pipe_id),
                    confidence_score=round(float(confidence), 4),
                    confidence_tier=_tier(float(confidence)),
                    canonical_id=resolution["canonical_id"] if resolution else None,
                    resolution_method=resolution["resolution_method"] if resolution else None,
                    resolution_confidence=resolution["resolution_confidence"] if resolution else None,
                    fabric_plane=fabric_plane,
                    fabric_product=fabric_product,
                )
                result.payloads.append(payload)

        # --- Plane summary aggregates (NLQ-resolvable metrics) ---
        # The per-record triples above feed the agent (DCL-MCP per-record reads);
        # NLQ answers from PRE-AGGREGATED concepts, so emit each plane's summary
        # metric too (revenue.total / customer.total / service.support_tickets).
        # Scoped to this pipe's fabric_plane so the order-event plane's incidental
        # customer_name fields don't inflate the customer count (four-fabric
        # context). Additive — the per-record triples remain for the agent.
        from backend.resolver.records_summary_aggregator import aggregate_records_summary
        result.payloads.extend(aggregate_records_summary(
            entity_id=entity_id, pipe=pipe, records=records,
        ))


# Process-lifetime singleton — keeps the registry snapshot cache warm across
# requests (per-request instances would defeat tier-4 block-key prefiltering).
_converter: Optional[RecordConverter] = None


def get_converter() -> RecordConverter:
    global _converter
    if _converter is None:
        _converter = RecordConverter()
    return _converter
