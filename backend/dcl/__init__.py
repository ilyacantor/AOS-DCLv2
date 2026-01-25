"""
DCL (Data Connectivity Layer) - Unified runtime for NLQ and structured execution.

This module consolidates NLQ, BLL, and execution into a single runtime:
- dcl.nlq: Parameter extraction and definition matching
- dcl.executor: Query execution with proper aggregations
- dcl.presenter: Mechanical data summary generation
- dcl.definitions: Definition registry and metadata
- dcl.history: Query history persistence and replay
"""
from .executor.executor import execute_query, ExecuteRequest, ExecuteResponse
from .presenter.presenter import generate_data_summary
from .history.persistence import HistoryStore
from .definitions.registry import DefinitionRegistry

__all__ = [
    "execute_query",
    "ExecuteRequest",
    "ExecuteResponse",
    "generate_data_summary",
    "HistoryStore",
    "DefinitionRegistry",
]
