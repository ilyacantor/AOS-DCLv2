"""
Explainer for NLQ Circles.

Generates deterministic explanations for hypotheses without executing queries.
Uses stored metadata and stub fixtures for MVP.
"""

import hashlib
from typing import Optional, Dict, Any
from backend.utils.log_utils import get_logger
from backend.nlq.models import (
    ExplainRequest,
    ExplainResponse,
    Fact,
    BridgeComponent,
    GoDeeper,
    ProofPointer,
    NextAction,
)
from backend.nlq.persistence import NLQPersistence

logger = get_logger(__name__)


# =============================================================================
# Explanation Templates (Stub Data for MVP)
# =============================================================================

HYPOTHESIS_EXPLANATIONS = {
    "h_volume": {
        "headline_template": "{metric} is down primarily due to lower recognized volume, not rate.",
        "facts": [
            {"fact": "Recognized {metric} events down 46% QoQ", "confidence": 0.82},
            {"fact": "Top 3 customers explain 62% of the decline", "confidence": 0.74},
            {"fact": "No significant change in average deal size", "confidence": 0.68},
        ],
        "bridge": [
            {"component": "Volume", "share": 0.62},
            {"component": "Rate", "share": 0.14},
            {"component": "Mix", "share": 0.11},
            {"component": "Timing", "share": 0.13},
        ],
        "drilldowns": ["by_customer", "by_service_line", "by_region"],
    },
    "h_timing": {
        "headline_template": "{metric} timing shifted across the quarter boundary.",
        "facts": [
            {"fact": "Recognition timing shifted by avg 12 days vs prior quarter", "confidence": 0.76},
            {"fact": "Invoice-to-recognition lag increased 23%", "confidence": 0.71},
            {"fact": "End-of-quarter spike was 35% lower than typical", "confidence": 0.65},
        ],
        "bridge": [
            {"component": "Timing", "share": 0.58},
            {"component": "Volume", "share": 0.25},
            {"component": "Rate", "share": 0.10},
            {"component": "Mix", "share": 0.07},
        ],
        "drilldowns": ["by_week", "by_service_line", "by_contract_type"],
    },
    "h_reclass": {
        "headline_template": "{metric} classification changed, affecting reported totals.",
        "facts": [
            {"fact": "Service line mapping changed for 15% of transactions", "confidence": 0.68},
            {"fact": "Reclassification from Services to Support category detected", "confidence": 0.62},
            {"fact": "Change effective mid-quarter", "confidence": 0.55},
        ],
        "bridge": [
            {"component": "Reclass", "share": 0.72},
            {"component": "Volume", "share": 0.18},
            {"component": "Rate", "share": 0.06},
            {"component": "Timing", "share": 0.04},
        ],
        "drilldowns": ["by_service_line", "by_mapping_change", "by_effective_date"],
    },
}


class HypothesisExplainer:
    """
    Generates deterministic explanations for hypotheses.

    For MVP, uses stub data and templates. Does not execute real queries.
    """

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize the explainer.

        Args:
            persistence: NLQPersistence instance. Creates default if not provided.
        """
        self.persistence = persistence or NLQPersistence()

    def _generate_query_hash(self, hypothesis_id: str, plan_id: str) -> str:
        """Generate a deterministic query hash for proof."""
        content = f"{hypothesis_id}:{plan_id}"
        return f"sha256:{hashlib.sha256(content.encode()).hexdigest()[:16]}"

    def _get_proof_pointers(
        self, definition_id: Optional[str], hypothesis_id: str, plan_id: str, tenant_id: str
    ) -> list:
        """
        Get proof pointers for the explanation.

        Returns source system pointers from proof hooks.
        """
        pointers = [
            ProofPointer(
                type="query_hash",
                value=self._generate_query_hash(hypothesis_id, plan_id),
            )
        ]

        if definition_id:
            hooks = self.persistence.get_proof_hooks_for_definition(definition_id, tenant_id)
            for hook in hooks:
                template = hook.pointer_template_json
                pointers.append(
                    ProofPointer(
                        type="source_pointer",
                        system=template.get("system", "Unknown"),
                        ref=template.get("ref_template", "").replace("{search_id}", "123"),
                    )
                )

        return pointers

    def _format_metric_name(self, question: str) -> str:
        """Extract metric name from question for formatting."""
        question_lower = question.lower()

        if "services revenue" in question_lower:
            return "Services revenue"
        elif "subscription" in question_lower:
            return "Subscription revenue"
        elif "total revenue" in question_lower:
            return "Total revenue"
        elif "arr" in question_lower:
            return "ARR"
        else:
            return "Revenue"

    def explain(self, request: ExplainRequest) -> ExplainResponse:
        """
        Generate an explanation for a hypothesis.

        Args:
            request: ExplainRequest with question, hypothesis_id, plan_id

        Returns:
            ExplainResponse with headline, facts, proof, and next actions
        """
        hypothesis_id = request.hypothesis_id
        plan_id = request.plan_id
        tenant_id = request.tenant_id

        # Get template for this hypothesis
        template = HYPOTHESIS_EXPLANATIONS.get(hypothesis_id)
        if not template:
            logger.warning(f"No template for hypothesis: {hypothesis_id}")
            return ExplainResponse(
                headline="Unable to generate explanation for this hypothesis.",
                why=[Fact(fact="Hypothesis template not found", confidence=0.0)],
                proof=[],
                next=[NextAction(action="retry", label="Try a different hypothesis")],
            )

        # Format metric name
        metric_name = self._format_metric_name(request.question)

        # Build headline
        headline = template["headline_template"].format(metric=metric_name)

        # Build facts with metric name substitution
        facts = []
        for f in template["facts"]:
            fact_text = f["fact"].format(metric=metric_name.lower())
            facts.append(Fact(fact=fact_text, confidence=f["confidence"]))

        # Build bridge
        bridge_components = [
            BridgeComponent(component=b["component"], share=b["share"])
            for b in template["bridge"]
        ]

        go_deeper = GoDeeper(
            bridge=bridge_components,
            drilldowns=template["drilldowns"],
        )

        # Get proof pointers
        # Try to resolve definition from plan_id
        definition_id = None
        if "services" in plan_id.lower():
            definition_id = "services_revenue"
        elif "total" in plan_id.lower():
            definition_id = "total_revenue"

        proof = self._get_proof_pointers(definition_id, hypothesis_id, plan_id, tenant_id)

        # Build next actions
        next_actions = [
            NextAction(action="open_sources", label="Show sources"),
            NextAction(action="deeper", label="Go deeper"),
        ]

        return ExplainResponse(
            headline=headline,
            why=facts,
            go_deeper=go_deeper,
            proof=proof,
            next=next_actions,
        )
