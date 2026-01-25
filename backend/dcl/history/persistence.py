"""
DCL History Persistence - Stores and retrieves query history.

Provides:
- In-memory storage for quick development
- JSON-based persistence for durability
- Query replay from stored results
"""
import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any


@dataclass
class HistoryEntry:
    """A single history entry."""
    id: str
    timestamp: str
    question: str
    dataset_id: str
    definition_id: str
    extracted_params: Dict[str, Any]
    response: Dict[str, Any]
    latency_ms: int
    status: str  # "success" | "error"
    tenant_id: str = "default"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HistoryEntry":
        return cls(**data)


class HistoryStore:
    """
    In-memory history store with optional JSON persistence.

    For production, this should be replaced with a database-backed store.
    """

    def __init__(self, persist_path: Optional[str] = None, max_entries: int = 1000):
        self._entries: Dict[str, HistoryEntry] = {}
        self._order: List[str] = []  # Maintain insertion order
        self._max_entries = max_entries
        self._persist_path = Path(persist_path) if persist_path else None

        # Load persisted data if available
        if self._persist_path and self._persist_path.exists():
            self._load()

    def add(
        self,
        question: str,
        dataset_id: str,
        definition_id: str,
        extracted_params: Dict[str, Any],
        response: Dict[str, Any],
        latency_ms: int,
        status: str = "success",
        tenant_id: str = "default",
    ) -> HistoryEntry:
        """Add a new history entry."""
        entry_id = str(uuid.uuid4())
        entry = HistoryEntry(
            id=entry_id,
            timestamp=datetime.utcnow().isoformat() + "Z",
            question=question,
            dataset_id=dataset_id,
            definition_id=definition_id,
            extracted_params=extracted_params,
            response=response,
            latency_ms=latency_ms,
            status=status,
            tenant_id=tenant_id,
        )

        self._entries[entry_id] = entry
        self._order.append(entry_id)

        # Trim old entries if over limit
        while len(self._order) > self._max_entries:
            old_id = self._order.pop(0)
            del self._entries[old_id]

        # Persist if configured
        if self._persist_path:
            self._save()

        return entry

    def get(self, entry_id: str) -> Optional[HistoryEntry]:
        """Get a specific history entry by ID."""
        return self._entries.get(entry_id)

    def list(
        self,
        tenant_id: str = "default",
        limit: int = 50,
        offset: int = 0,
    ) -> List[HistoryEntry]:
        """List history entries, newest first."""
        # Filter by tenant
        filtered = [
            self._entries[eid]
            for eid in reversed(self._order)
            if self._entries[eid].tenant_id == tenant_id
        ]

        # Apply pagination
        return filtered[offset:offset + limit]

    def clear(self, tenant_id: Optional[str] = None):
        """Clear history entries."""
        if tenant_id:
            # Clear only for specific tenant
            to_remove = [
                eid for eid in self._order
                if self._entries[eid].tenant_id == tenant_id
            ]
            for eid in to_remove:
                del self._entries[eid]
                self._order.remove(eid)
        else:
            # Clear all
            self._entries.clear()
            self._order.clear()

        if self._persist_path:
            self._save()

    def _save(self):
        """Save to JSON file."""
        if not self._persist_path:
            return

        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "entries": [self._entries[eid].to_dict() for eid in self._order],
        }
        with open(self._persist_path, "w") as f:
            json.dump(data, f, indent=2)

    def _load(self):
        """Load from JSON file."""
        if not self._persist_path or not self._persist_path.exists():
            return

        try:
            with open(self._persist_path, "r") as f:
                data = json.load(f)

            for entry_data in data.get("entries", []):
                entry = HistoryEntry.from_dict(entry_data)
                self._entries[entry.id] = entry
                self._order.append(entry.id)
        except Exception as e:
            print(f"[HistoryStore] Failed to load persisted data: {e}")


# Global history store instance
_history_store: Optional[HistoryStore] = None


def get_history_store() -> HistoryStore:
    """Get the global history store instance."""
    global _history_store
    if _history_store is None:
        # Use a persistent path in the project directory
        persist_path = Path(__file__).parent.parent.parent.parent / "data" / "history.json"
        _history_store = HistoryStore(persist_path=str(persist_path))
    return _history_store
