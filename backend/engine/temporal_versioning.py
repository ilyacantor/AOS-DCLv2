"""
Temporal Versioning - Append-only changelog for ontology concepts and metric definitions.

Tracks when definitions change so year-over-year comparisons remain valid.
NLQ's query resolver checks query time range against changelog to issue warnings.
"""

import copy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class VersionEntry(BaseModel):
    """Single version history entry for a metric definition."""
    version: int
    changed_by: str
    change_description: str
    changed_at: str
    previous_value: Optional[str] = None
    new_value: Optional[str] = None


class TemporalWarning(BaseModel):
    """Warning issued when a query spans a definition change."""
    metric: str
    change_date: str
    old_definition: str
    new_definition: str
    message: str


class TemporalVersioningStore:
    """
    In-memory append-only store for metric version histories.

    Entries can only be appended, never modified or deleted.
    """

    def __init__(self):
        # metric_id -> list of VersionEntry
        self._histories: Dict[str, List[VersionEntry]] = {}
        self._seed_initial_versions()

    def _seed_initial_versions(self):
        """Seed initial version entries for all published metrics."""
        from backend.api.semantic_export import PUBLISHED_METRICS

        for metric in PUBLISHED_METRICS:
            self._histories[metric.id] = [
                VersionEntry(
                    version=1,
                    changed_by="system",
                    change_description="Initial definition",
                    changed_at="2024-01-01T00:00:00Z",
                    previous_value=None,
                    new_value=metric.description,
                )
            ]

        # Seed definition changes from entity_test_scenarios.json -> temporal_versioning
        # Revenue redefined on 2025-03-01
        self._histories["revenue"].append(
            VersionEntry(
                version=2,
                changed_by="finance_team",
                change_description="Changed from bookings at close to GAAP recognized at delivery",
                changed_at="2025-03-01T00:00:00Z",
                previous_value="Total bookings revenue at deal close",
                new_value="GAAP recognized revenue at delivery",
            )
        )

        # Customers redefined on 2025-06-15
        if "customers" in self._histories:
            self._histories["customers"].append(
                VersionEntry(
                    version=2,
                    changed_by="ops_team",
                    change_description="Changed from closed-won accounts to active subscription or services",
                    changed_at="2025-06-15T00:00:00Z",
                    previous_value="Count of accounts with at least one closed-won deal",
                    new_value="Count of accounts with active subscription or professional services engagement",
                )
            )

    def get_history(self, metric_id: str) -> Optional[List[VersionEntry]]:
        """Get version history for a metric. Returns None if metric not found."""
        return self._histories.get(metric_id)

    def add_version(
        self,
        metric_id: str,
        changed_by: str,
        change_description: str,
        previous_value: str,
        new_value: str,
    ) -> VersionEntry:
        """Append a new version entry. Creates history list if needed."""
        history = self._histories.get(metric_id, [])
        next_version = len(history) + 1

        entry = VersionEntry(
            version=next_version,
            changed_by=changed_by,
            change_description=change_description,
            changed_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            previous_value=previous_value,
            new_value=new_value,
        )

        if metric_id not in self._histories:
            self._histories[metric_id] = []
        self._histories[metric_id].append(entry)

        logger.info(f"Added version {next_version} for metric '{metric_id}'")
        return entry

    def get_entry_count(self, metric_id: str) -> int:
        """Get number of version entries for a metric."""
        return len(self._histories.get(metric_id, []))

    def check_temporal_warning(
        self, metric_id: str, time_range: Optional[Dict[str, str]] = None
    ) -> Optional[TemporalWarning]:
        """
        Check if a query's time range crosses a definition change boundary.

        Returns a TemporalWarning if it does, None otherwise.
        """
        if not time_range:
            return None

        history = self._histories.get(metric_id)
        if not history or len(history) < 2:
            return None

        start_str = time_range.get("start", "")
        end_str = time_range.get("end", "")

        if not start_str or not end_str:
            return None

        # Normalize period strings (e.g., "2024-Q4" -> "2024-10-01")
        start_date = self._parse_period(start_str)
        end_date = self._parse_period(end_str)

        if not start_date or not end_date:
            return None

        # Check each version change to see if it falls within the query range
        for entry in history[1:]:  # Skip first (initial) entry
            change_date = self._parse_iso_date(entry.changed_at)
            if change_date and start_date <= change_date <= end_date:
                return TemporalWarning(
                    metric=metric_id,
                    change_date=entry.changed_at,
                    old_definition=entry.previous_value or "N/A",
                    new_definition=entry.new_value or "N/A",
                    message=(
                        f"The definition of '{metric_id}' changed on "
                        f"{entry.changed_at}. Your query spans this change. "
                        f"Old: {entry.previous_value}. New: {entry.new_value}."
                    ),
                )

        return None

    @staticmethod
    def _parse_period(period: str) -> Optional[datetime]:
        """Parse a period string like '2024-Q4' or '2025-Q2' into a datetime."""
        try:
            if "-Q" in period:
                year_str, q_str = period.split("-Q")
                year = int(year_str)
                quarter = int(q_str)
                month = (quarter - 1) * 3 + 1
                return datetime(year, month, 1, tzinfo=timezone.utc)
            # Try ISO date
            return datetime.fromisoformat(period.replace("Z", "+00:00"))
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _parse_iso_date(iso_str: str) -> Optional[datetime]:
        """Parse an ISO datetime string."""
        try:
            return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        except ValueError:
            return None


# Singleton instance
_store: Optional[TemporalVersioningStore] = None


def get_temporal_store() -> TemporalVersioningStore:
    """Get or create the singleton temporal versioning store."""
    global _store
    if _store is None:
        _store = TemporalVersioningStore()
    return _store
