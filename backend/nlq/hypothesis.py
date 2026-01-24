"""
Dynamic Hypothesis Generator for NLQ Answerability Circles.

Generates hypotheses based on:
- Definition structure (events, dims, time semantics)
- Question type (change, trend, comparison)
- Available metadata
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from backend.utils.log_utils import get_logger
from backend.nlq.models import DefinitionVersion, DefinitionVersionSpec
from backend.nlq.persistence import NLQPersistence

logger = get_logger(__name__)


@dataclass
class HypothesisTemplate:
    """
    Template for a hypothesis type.

    Each hypothesis has required events, dimensions, and a plan_id.
    """
    id: str
    label_template: str  # Use {metric} as placeholder
    required_events: List[str]
    required_dims: List[str]
    plan_id: str
    base_probability: float = 0.5
    category: str = "change"  # change, trend, composition, anomaly
    description: str = ""


# =============================================================================
# Base Hypothesis Templates
# =============================================================================

# These are base templates that get customized per definition
BASE_HYPOTHESES = {
    "change": [
        {
            "id": "h_volume",
            "label_template": "Volume change (event count/sum changed)",
            "category": "change",
            "base_probability": 0.6,
            "description": "The underlying volume of events changed",
        },
        {
            "id": "h_rate",
            "label_template": "Rate change (per-unit value changed)",
            "category": "change",
            "base_probability": 0.5,
            "description": "The rate or per-unit amount changed",
        },
        {
            "id": "h_mix",
            "label_template": "Mix shift (composition across {dim} changed)",
            "category": "change",
            "base_probability": 0.45,
            "description": "The mix across a dimension shifted",
        },
        {
            "id": "h_timing",
            "label_template": "Timing slip (recognition shifted across period boundary)",
            "category": "change",
            "base_probability": 0.4,
            "description": "Event timing shifted across reporting periods",
        },
        {
            "id": "h_reclass",
            "label_template": "Reclassification ({metric} tagged differently)",
            "category": "change",
            "base_probability": 0.35,
            "description": "Classification or tagging changed",
        },
    ],
    "trend": [
        {
            "id": "h_seasonal",
            "label_template": "Seasonal pattern ({metric} follows expected seasonality)",
            "category": "trend",
            "base_probability": 0.5,
            "description": "Normal seasonal variation",
        },
        {
            "id": "h_growth",
            "label_template": "Growth trajectory ({metric} on expected growth path)",
            "category": "trend",
            "base_probability": 0.45,
            "description": "Following expected growth trajectory",
        },
    ],
    "anomaly": [
        {
            "id": "h_outlier",
            "label_template": "Outlier event (unusual {metric} transaction)",
            "category": "anomaly",
            "base_probability": 0.4,
            "description": "One-time unusual event",
        },
        {
            "id": "h_data_issue",
            "label_template": "Data quality issue (missing or incorrect data)",
            "category": "anomaly",
            "base_probability": 0.3,
            "description": "Data quality problem",
        },
    ],
}


class HypothesisGenerator:
    """
    Generates hypotheses dynamically based on definition structure.
    """

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        self.persistence = persistence or NLQPersistence()

    def generate(
        self,
        definition_id: str,
        question_type: str = "change",
        metric_name: str = "metric",
        version: str = "v1",
        tenant_id: str = "default",
    ) -> List[HypothesisTemplate]:
        """
        Generate hypotheses for a definition.

        Args:
            definition_id: The definition to generate hypotheses for
            question_type: Type of question (change, trend, anomaly)
            metric_name: Human-readable metric name
            version: Definition version
            tenant_id: Tenant ID

        Returns:
            List of HypothesisTemplate objects tailored to the definition
        """
        # Get the definition version
        def_version = self.persistence.get_definition_version(
            definition_id, version, tenant_id
        )

        if not def_version:
            # Fall back to generic hypotheses
            return self._get_generic_hypotheses(question_type, metric_name)

        spec = def_version.spec

        # Generate hypotheses based on definition structure
        hypotheses = []

        # Always include volume hypothesis if there's an aggregation
        if spec.measure:
            hypotheses.append(self._create_volume_hypothesis(spec, metric_name))

        # Add rate hypothesis if measure is a sum (implies volume * rate)
        if spec.measure.get("op") in ["sum", "avg"]:
            hypotheses.append(self._create_rate_hypothesis(spec, metric_name))

        # Add mix hypothesis for each dimension
        for dim in spec.allowed_dims[:2]:  # Top 2 dims only
            hypotheses.append(self._create_mix_hypothesis(spec, metric_name, dim))

        # Add timing hypothesis if there's a time field
        if spec.time_field:
            hypotheses.append(self._create_timing_hypothesis(spec, metric_name))

        # Add reclass hypothesis if there are filter-based dimensions
        if spec.filters:
            hypotheses.append(self._create_reclass_hypothesis(spec, metric_name))

        # Add question-type specific hypotheses
        if question_type == "trend":
            hypotheses.extend(self._get_trend_hypotheses(spec, metric_name))
        elif question_type == "anomaly":
            hypotheses.extend(self._get_anomaly_hypotheses(spec, metric_name))

        return hypotheses

    def _create_volume_hypothesis(
        self, spec: DefinitionVersionSpec, metric_name: str
    ) -> HypothesisTemplate:
        """Create volume-based hypothesis."""
        return HypothesisTemplate(
            id="h_volume",
            label_template=f"Volume drop (fewer recognized {metric_name} events)",
            required_events=spec.required_events,
            required_dims=spec.allowed_dims[:2] if spec.allowed_dims else [],
            plan_id=f"plan_{metric_name.replace(' ', '_')}_volume_bridge",
            base_probability=0.6,
            category="change",
            description="The count or sum of underlying events changed",
        )

    def _create_rate_hypothesis(
        self, spec: DefinitionVersionSpec, metric_name: str
    ) -> HypothesisTemplate:
        """Create rate-based hypothesis."""
        return HypothesisTemplate(
            id="h_rate",
            label_template=f"Rate change ({metric_name} per-unit value changed)",
            required_events=spec.required_events,
            required_dims=["customer"] if "customer" in spec.allowed_dims else spec.allowed_dims[:1],
            plan_id=f"plan_{metric_name.replace(' ', '_')}_rate_analysis",
            base_probability=0.5,
            category="change",
            description="The average rate or per-unit amount changed",
        )

    def _create_mix_hypothesis(
        self, spec: DefinitionVersionSpec, metric_name: str, dim: str
    ) -> HypothesisTemplate:
        """Create mix-shift hypothesis for a dimension."""
        return HypothesisTemplate(
            id=f"h_mix_{dim}",
            label_template=f"Mix shift ({metric_name} composition across {dim} changed)",
            required_events=spec.required_events,
            required_dims=[dim],
            plan_id=f"plan_{metric_name.replace(' ', '_')}_mix_{dim}",
            base_probability=0.45,
            category="change",
            description=f"The distribution across {dim} shifted",
        )

    def _create_timing_hypothesis(
        self, spec: DefinitionVersionSpec, metric_name: str
    ) -> HypothesisTemplate:
        """Create timing-based hypothesis."""
        # Need invoice_posted if it exists in bindings
        required_events = list(spec.required_events)
        if "invoice_posted" not in required_events:
            # Check if invoice_posted event exists
            if self.persistence.event_exists("invoice_posted"):
                required_events.append("invoice_posted")

        return HypothesisTemplate(
            id="h_timing",
            label_template=f"Timing slip ({metric_name} recognition shifted across period boundary)",
            required_events=required_events,
            required_dims=["service_line"] if "service_line" in spec.allowed_dims else [],
            plan_id=f"plan_{metric_name.replace(' ', '_')}_timing_check",
            base_probability=0.4,
            category="change",
            description="Event timing shifted between periods",
        )

    def _create_reclass_hypothesis(
        self, spec: DefinitionVersionSpec, metric_name: str
    ) -> HypothesisTemplate:
        """Create reclassification hypothesis."""
        return HypothesisTemplate(
            id="h_reclass",
            label_template=f"Reclass / mapping drift ({metric_name} tagged differently)",
            required_events=["mapping_changed"] if self.persistence.event_exists("mapping_changed") else [],
            required_dims=list(spec.filters.keys())[:1] if spec.filters else [],
            plan_id=f"plan_{metric_name.replace(' ', '_')}_mapping_drift",
            base_probability=0.35,
            category="change",
            description="Classification or tagging definitions changed",
        )

    def _get_trend_hypotheses(
        self, spec: DefinitionVersionSpec, metric_name: str
    ) -> List[HypothesisTemplate]:
        """Get trend-related hypotheses."""
        return [
            HypothesisTemplate(
                id="h_seasonal",
                label_template=f"Seasonal pattern ({metric_name} follows expected seasonality)",
                required_events=spec.required_events,
                required_dims=[],
                plan_id=f"plan_{metric_name.replace(' ', '_')}_seasonal",
                base_probability=0.5,
                category="trend",
                description="Normal seasonal variation in the metric",
            ),
        ]

    def _get_anomaly_hypotheses(
        self, spec: DefinitionVersionSpec, metric_name: str
    ) -> List[HypothesisTemplate]:
        """Get anomaly-related hypotheses."""
        return [
            HypothesisTemplate(
                id="h_outlier",
                label_template=f"Outlier event (unusual {metric_name} transaction)",
                required_events=spec.required_events,
                required_dims=["customer"] if "customer" in spec.allowed_dims else [],
                plan_id=f"plan_{metric_name.replace(' ', '_')}_outlier",
                base_probability=0.4,
                category="anomaly",
                description="One-time unusual event affecting the metric",
            ),
        ]

    def _get_generic_hypotheses(
        self, question_type: str, metric_name: str
    ) -> List[HypothesisTemplate]:
        """Get generic hypotheses when no definition is found."""
        base_templates = BASE_HYPOTHESES.get(question_type, BASE_HYPOTHESES["change"])

        hypotheses = []
        for template in base_templates[:3]:  # Max 3
            hypotheses.append(HypothesisTemplate(
                id=template["id"],
                label_template=template["label_template"].format(
                    metric=metric_name, dim="segment"
                ),
                required_events=["revenue_recognized"],  # Default
                required_dims=["customer", "service_line"],
                plan_id=f"plan_generic_{template['id']}",
                base_probability=template["base_probability"],
                category=template["category"],
                description=template["description"],
            ))

        return hypotheses


# =============================================================================
# Legacy Hypothesis Templates (for backwards compatibility)
# =============================================================================

# Standard hypotheses for "metric down/up" questions
METRIC_CHANGE_HYPOTHESES = [
    HypothesisTemplate(
        id="h_volume",
        label_template="Volume drop (fewer recognized {metric} events)",
        required_events=["revenue_recognized"],
        required_dims=["customer", "service_line"],
        plan_id="plan_{metric}_bridge",
        base_probability=0.6,
    ),
    HypothesisTemplate(
        id="h_timing",
        label_template="Timing slip (recognition shifted across quarter boundary)",
        required_events=["revenue_recognized", "invoice_posted"],
        required_dims=["service_line"],
        plan_id="plan_timing_slip_check",
        base_probability=0.5,
    ),
    HypothesisTemplate(
        id="h_reclass",
        label_template="Reclass / mapping drift ({metric} tagged differently)",
        required_events=["mapping_changed"],
        required_dims=["service_line"],
        plan_id="plan_mapping_drift",
        base_probability=0.4,
    ),
]
