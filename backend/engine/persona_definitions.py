"""
Persona-Contextual Definitions - Same question, different correct answer depending on who asks.

"How many customers?" returns 2,400 for the CFO and 8,100 for the CRO.

Definitions are stored in a dedicated table (separate from ontology_concepts).
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.utils.log_utils import get_logger

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

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
        self._load_definitions()

    def _load_definitions(self):
        """Load persona-specific definitions from JSON data file."""
        json_path = _DATA_DIR / "persona_definitions.json"
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for entry in data["definitions"]:
            d = PersonaDefinition(**entry)
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
