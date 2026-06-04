"""Plane summary aggregation — party/event records -> NLQ-resolvable metrics.

The four-fabric planes other than cloud-spend (warehouse) carry party/event
records that DCL classifies PER RECORD (the agent reads each via DCL-MCP). NLQ,
though, answers from PRE-AGGREGATED concepts (like cloud_spend.summary.total_cost),
so the per-record triples alone leave NLQ with "I recognize <metric> but I don't
have data". This module emits the per-plane summary each NLQ metric resolves
(nlq config/metric_concept_map.yaml):

  ipaas        -> customer.total / count           (NLQ metric customer_count)
  event_bus    -> revenue.total / amount           (NLQ metric revenue)
  api_gateway  -> service.support_tickets / count   (NLQ metric support_tickets)

It runs ALONGSIDE the per-record path (ADDITIVE — the per-record triples remain
for the agent), unlike the cloud-spend aggregator which REPLACES per-record
(cloud-spend pipes are a metric fleet, not party records).

Scoped to the pipe's fabric_plane: the event-bus order records also carry a
customer_name field (classified per record as customer), but the CUSTOMER COUNT
must come from the customer-sync plane (ipaas), not from order-customers — the
four-fabric thesis is that the plane (context) determines the summary domain.
Routing by domain would be wrong here: record_converter forces concept=domain on
domain-tagged pipes, so ipaas/event_bus must stay domainless to keep their
per-field split (amount_usd->revenue, customer_name->customer); fabric_plane is
the stable per-pipe context that survives that.

Aggregates are atemporal (period=None) so NLQ's period fallback resolves them
regardless of the requested quarter; every triple carries the pipe's transport
provenance; confidence 0.95/exact (deterministic sums/counts, not heuristics).
"""
from __future__ import annotations

from typing import Any, Optional

from backend.api.routes.ingest_triples import TriplePayload

_CONF = 0.95
_TIER = "exact"
# The order-event monetary field the per-record path classifies as revenue (#49).
_REVENUE_AMOUNT_FIELD = "amount_usd"
_SUMMARY_PLANES = ("ipaas", "event_bus", "api_gateway")


def _num(v: Any) -> Optional[float]:
    # JSON numbers only; non-numeric (None, str, dict) -> None so the caller
    # skips it. No exception-swallowing default.
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def aggregate_records_summary(
    *, entity_id: str, pipe: dict, records: list[dict],
) -> list[TriplePayload]:
    """Per-plane summary TriplePayloads NLQ's metrics resolve against.

    Routed by fabric_plane (always set by the AAM transport). Returns [] for
    planes with no summary metric (cloud-spend is handled by its own aggregator
    and never reaches here; unknown planes get no summary).
    """
    plane = pipe.get("fabric_plane")
    if not records or plane not in _SUMMARY_PLANES:
        return []

    source_system = pipe.get("source_system")
    fabric_product = pipe.get("fabric_product")
    pipe_id = str(pipe.get("pipe_id"))

    def _t(concept: str, prop: str, value: Any, *, unit: str,
           currency: Optional[str] = None) -> TriplePayload:
        return TriplePayload(
            entity_id=entity_id, concept=concept, property=prop, value=value,
            period=None, currency=currency, unit=unit,
            source_system=source_system, source_table=f"fabric_summary:{plane}",
            source_field=prop, pipe_id=pipe_id,
            confidence_score=_CONF, confidence_tier=_TIER,
            fabric_plane=plane, fabric_product=fabric_product,
        )

    if plane == "event_bus":
        # Revenue = sum of the order-event monetary amounts (NLQ `revenue` ->
        # revenue.total / amount). amount_usd is the order value the per-record
        # path classifies as revenue, NOT cloud_spend (#49).
        amounts = [a for a in (_num(r.get(_REVENUE_AMOUNT_FIELD)) for r in records) if a is not None]
        if not amounts:
            return []
        return [_t("revenue.total", "amount", round(sum(amounts), 2), unit="usd", currency="USD")]

    if plane == "ipaas":
        # Customer count = number of customer-sync rows (NLQ `customer_count` ->
        # customer.total / count). One row per synced customer.
        return [_t("customer.total", "count", len(records), unit="count")]

    # plane == "api_gateway": support ticket count (NLQ `support_tickets` ->
    # service.support_tickets / count). One record per ticket.
    return [_t("service.support_tickets", "count", len(records), unit="count")]
