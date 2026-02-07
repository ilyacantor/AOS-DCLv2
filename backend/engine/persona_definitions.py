"""
Persona-Contextual Definitions - Same question, different correct answer depending on who asks.

"How many customers?" returns 2,400 for the CFO and 8,100 for the CRO.

Definitions are stored in a dedicated table (separate from ontology_concepts).
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class PersonaDefinition(BaseModel):
    """A persona-specific definition for a metric."""
    id: str
    metric_id: str
    persona: str  # "CFO", "CRO", "COO", "CTO", "CHRO"
    definition: str
    calculation_method: str
    value_override: Optional[float] = None
    value_multiplier: Optional[float] = None


class PersonaDefinitionStore:
    """
    In-memory store for persona-contextual definitions.

    Each metric can have different definitions per persona.
    """

    def __init__(self):
        self._definitions: Dict[str, List[PersonaDefinition]] = {}
        self._seed_definitions()

    def _seed_definitions(self):
        """Seed persona-specific definitions."""
        definitions = [
            # Customers: CFO sees billing entities, CRO sees active opportunities
            PersonaDefinition(
                id="pcd-customers-cfo",
                metric_id="customers",
                persona="CFO",
                definition="Active billing entities with at least one paid invoice in the last 12 months",
                calculation_method="COUNT(DISTINCT billing_entity_id) WHERE last_invoice_date > NOW() - INTERVAL '12 months'",
                value_override=2400,
            ),
            PersonaDefinition(
                id="pcd-customers-cro",
                metric_id="customers",
                persona="CRO",
                definition="Active accounts with at least one open opportunity or active subscription",
                calculation_method="COUNT(DISTINCT account_id) WHERE has_active_opportunity = true OR has_active_subscription = true",
                value_override=8100,
            ),
            PersonaDefinition(
                id="pcd-customers-coo",
                metric_id="customers",
                persona="COO",
                definition="Accounts with active support contracts or SLA agreements",
                calculation_method="COUNT(DISTINCT account_id) WHERE has_support_contract = true OR has_sla = true",
                value_override=3200,
            ),
            PersonaDefinition(
                id="pcd-customers-cto",
                metric_id="customers",
                persona="CTO",
                definition="Accounts with active API integrations or platform connections",
                calculation_method="COUNT(DISTINCT account_id) WHERE active_integrations > 0",
                value_override=3800,
            ),
            # Revenue: Different recognition methods per persona
            PersonaDefinition(
                id="pcd-revenue-cfo",
                metric_id="revenue",
                persona="CFO",
                definition="GAAP recognized revenue, net of returns and allowances",
                calculation_method="SUM(recognized_amount) - SUM(returns) - SUM(allowances)",
                value_override=200000000,
            ),
            PersonaDefinition(
                id="pcd-revenue-cro",
                metric_id="revenue",
                persona="CRO",
                definition="Booked revenue from closed-won deals including services",
                calculation_method="SUM(amount) WHERE stage = 'Closed-Won'",
                value_override=228000000,
            ),
            # Pipeline: Different scope per persona
            PersonaDefinition(
                id="pcd-pipeline-cro",
                metric_id="pipeline",
                persona="CRO",
                definition="Total value of all open opportunities including new and expansion",
                calculation_method="SUM(amount) WHERE stage NOT IN ('Closed-Won', 'Closed-Lost')",
                value_multiplier=1.0,
            ),
            PersonaDefinition(
                id="pcd-pipeline-cfo",
                metric_id="pipeline",
                persona="CFO",
                definition="Weighted pipeline value using stage-probability forecasting",
                calculation_method="SUM(amount * stage_probability) WHERE stage NOT IN ('Closed-Won', 'Closed-Lost')",
                value_multiplier=0.65,
            ),
        ]

        for d in definitions:
            if d.metric_id not in self._definitions:
                self._definitions[d.metric_id] = []
            self._definitions[d.metric_id].append(d)

    def get_definition(self, metric_id: str, persona: str) -> Optional[PersonaDefinition]:
        """Get the persona-specific definition for a metric."""
        defs = self._definitions.get(metric_id, [])
        for d in defs:
            if d.persona.upper() == persona.upper():
                return d
        return None

    def get_all_definitions(self, metric_id: str) -> List[PersonaDefinition]:
        """Get all persona definitions for a metric."""
        return self._definitions.get(metric_id, [])

    def has_persona_definitions(self, metric_id: str) -> bool:
        """Check if a metric has any persona-specific definitions."""
        return metric_id in self._definitions and len(self._definitions[metric_id]) > 0

    def apply_persona_context(
        self,
        metric_id: str,
        persona: Optional[str],
        base_value: float,
    ) -> tuple:
        """
        Apply persona context to a base value.

        Returns (adjusted_value, definition_used) tuple.
        If no persona or no definition exists, returns (base_value, None).
        """
        if not persona:
            return base_value, None

        definition = self.get_definition(metric_id, persona)
        if not definition:
            return base_value, None

        if definition.value_override is not None:
            return definition.value_override, definition

        if definition.value_multiplier is not None:
            return base_value * definition.value_multiplier, definition

        return base_value, definition


# Singleton
_store: Optional[PersonaDefinitionStore] = None


def get_persona_definition_store() -> PersonaDefinitionStore:
    """Get or create the singleton persona definition store."""
    global _store
    if _store is None:
        _store = PersonaDefinitionStore()
    return _store
