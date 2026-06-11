"""Financial records -> canonical concept triples (the records-path SE financial cutover).

The four operational fabrics (ipaas/warehouse/event_bus/api_gateway) carry genuinely
raw party/event records that DCL classifies + aggregates (records_summary_aggregator,
cloud_spend_aggregator). The SE financial depth is different in KIND: there are no raw
GL transactions under the P&L — the source computes the statements (FinancialModel) and
emits each period as a financial-statement RECORD. This module is DCL's classification of
those records: it owns the account->canonical-concept map (the chart-of-accounts contract)
that used to live in Farm's FinancialStatementTripleGenerator, so concept-formation happens
in DCL via ingest-records — not pre-formed in Farm and pushed via ingest-triples (the crutch).

Input pipe shape (domain == "financials"):
    {
      "pipe_id": ..., "source_system": "<erp vendor>", "fabric_plane": "erp",
      "fabric_product": "<erp>", "domain": "financials",
      "records": [ {"period": "2024-Q1", "revenue": 102.3, "cogs": 74.26,
                    "net_income": 12.1, "cash": 318.0, ...}, ... ]   # one per period
    }

Each record is one period's statement bundle (P&L + Balance Sheet + Cash Flow line items
+ totals). DCL maps each known field to its canonical concept (revenue -> revenue.total,
cash -> asset.current.cash, ...) and emits one period-stamped triple per field. Unknown
fields are skipped with a loud warning rather than silently dropped (A1) — surfaced via
the returned warnings list, never swallowed.

Property is "amount" for every financial line item (matches nlq/config/metric_concept_map.yaml
and FluxEdge's ingest-triples baseline, so the dashboards resolve them with zero NLQ change).
Confidence 0.95/exact: this is a deterministic account->concept lookup, not a heuristic guess.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.api.routes.ingest_triples import TriplePayload

_CONF = 0.95
_TIER = "exact"

# Chart-of-accounts contract: source financial field -> (canonical concept, property).
# Concepts are exactly what nlq/config/metric_concept_map.yaml resolves (so every CFO
# dashboard tile + the derived metrics that compose from these components light up) and
# what FluxEdge's SE-path triples carry (so a records-path entity reaches SE parity).
# Every value is a USD amount; property "amount" throughout (financial-statement grain).
FINANCIAL_FIELD_CONCEPTS: dict[str, str] = {
    # ── P&L ──────────────────────────────────────────────────────────
    "revenue": "revenue.total",
    "cogs": "cogs.total",
    "opex_total": "opex.total",
    "opex_sales_marketing": "opex.sales_marketing",
    "opex_research_development": "opex.research_development",
    "opex_general_admin": "opex.general_admin",
    "ebitda": "pnl.ebitda",
    "operating_profit": "pnl.operating_profit",
    "tax_expense": "pnl.tax",
    "net_income": "pnl.net_income",
    "depreciation_amortization": "pnl.depreciation_amortization",
    # ── Balance Sheet — assets ───────────────────────────────────────
    "cash": "asset.current.cash",
    "accounts_receivable": "asset.current.accounts_receivable",
    "unbilled_revenue": "asset.current.unbilled_revenue",
    "prepaid": "asset.current.prepaid",
    "property_plant_equipment": "asset.noncurrent.property_plant_equipment",
    "intangibles": "asset.noncurrent.intangibles",
    "goodwill": "asset.noncurrent.goodwill",
    "asset_total": "asset.total",
    # ── Balance Sheet — liabilities ──────────────────────────────────
    "accounts_payable": "liability.current.accounts_payable",
    "accrued_expenses": "liability.current.accrued_expenses",
    "deferred_revenue": "liability.current.deferred_revenue",
    "long_term_debt": "liability.noncurrent.long_term_debt",
    "current_liabilities": "liability.current",
    "liability_total": "liability.total",
    # ── Balance Sheet — equity ───────────────────────────────────────
    "retained_earnings": "equity.retained_earnings",
    "common_stock": "equity.common_stock",
    "equity_total": "equity.total",
    # ── Cash Flow ────────────────────────────────────────────────────
    "cfo": "cash_flow.operating.total",
    "capex": "cash_flow.investing.capex",
    "depreciation_add_back": "cash_flow.operating.depreciation_add_back",
    "change_in_ar": "cash_flow.operating.change_in_ar",
    "change_in_ap": "cash_flow.operating.change_in_ap",
    "change_in_deferred_rev": "cash_flow.operating.change_in_deferred_rev",
    # CF statement totals (#54 gate-4: records-path full cash-flow statement —
    # investing/financing/net_change so CF identity + cash continuity hold)
    "investing_total": "cash_flow.investing.total",
    "financing_total": "cash_flow.financing.total",
    "financing_debt_repayment": "cash_flow.financing.debt_repayment",
    "financing_dividends": "cash_flow.financing.dividends",
    "cf_net_change": "cash_flow.net_change",
    # ── ARR ──────────────────────────────────────────────────────────
    "arr_beginning": "arr.beginning",
    "arr_ending": "arr.ending",
}

# Reserved record keys that are not financial line items (carry structure, not a fact).
_PERIOD_KEY = "period"
_BY_CUSTOMER_KEY = "revenue_by_customer"  # nested {customer_name: amount} -> revenue.by_customer
_STRUCTURAL_KEYS = frozenset({_PERIOD_KEY, _BY_CUSTOMER_KEY, "id"})


def _num(v: Any) -> Optional[float]:
    """JSON number -> float; anything else (None, str, bool, dict) -> None (skip, no default)."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def aggregate_financial_records(
    *, entity_id: str, pipe: dict, records: list[dict], warnings: list[dict],
) -> list[TriplePayload]:
    """Classify period-keyed financial-statement records into canonical concept triples.

    Returns the payloads; appends any unmapped-field warnings to `warnings` (the caller's
    ConversionResult.warnings) so a drifted source field surfaces loudly instead of vanishing.
    """
    source_system = pipe.get("source_system")
    fabric_plane = pipe.get("fabric_plane")
    fabric_product = pipe.get("fabric_product")
    pipe_id = str(pipe.get("pipe_id"))
    raw_source = pipe.get("source_system")

    def _t(concept: str, prop: str, value: float, period: Optional[str],
           source_field: str) -> TriplePayload:
        return TriplePayload(
            entity_id=entity_id, concept=concept, property=prop, value=value,
            period=period, currency="USD", unit="dollars",
            source_system=source_system, source_table=f"fabric_via:{raw_source}",
            source_field=source_field, pipe_id=pipe_id,
            confidence_score=_CONF, confidence_tier=_TIER,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
        )

    payloads: list[TriplePayload] = []
    for rec_idx, record in enumerate(records):
        period = record.get(_PERIOD_KEY)
        period = str(period) if period is not None and str(period).strip() else None

        for fname, raw_value in record.items():
            if fname in _STRUCTURAL_KEYS:
                continue
            concept = FINANCIAL_FIELD_CONCEPTS.get(fname)
            if concept is None:
                warnings.append({
                    "type": "unmapped_financial_field", "pipe_id": pipe_id,
                    "field": fname, "record_index": rec_idx,
                    "detail": (
                        f"financial field '{fname}' has no chart-of-accounts concept "
                        f"mapping in FINANCIAL_FIELD_CONCEPTS; not converted"
                    ),
                })
                continue
            value = _num(raw_value)
            if value is None:
                continue  # null/non-numeric line item for this period — nothing to assert
            payloads.append(_t(concept, "amount", value, period, fname))

        # revenue.by_customer: nested {customer_name: amount} -> one triple per customer,
        # property = the customer name (matches FluxEdge's revenue.by_customer shape).
        by_customer = record.get(_BY_CUSTOMER_KEY)
        if isinstance(by_customer, dict):
            for customer_name, amount in by_customer.items():
                value = _num(amount)
                if value is None:
                    continue
                payloads.append(_t("revenue.by_customer", str(customer_name), value,
                                   period, f"{_BY_CUSTOMER_KEY}.{customer_name}"))

    return payloads
