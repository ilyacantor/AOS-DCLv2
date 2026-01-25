"""DCL History Module - Query history persistence and replay."""
from .persistence import HistoryStore, HistoryEntry

__all__ = ["HistoryStore", "HistoryEntry"]
