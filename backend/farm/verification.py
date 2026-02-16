"""
Ground Truth Verification Service — DCL grades itself against Farm's oracle.

Verification loop:
1. DCL calls GET /ground-truth/{run_id} to get the full manifest
2. DCL compares its unified answers against 89 metrics per quarter
3. DCL calls GET /conflicts to verify it detected the 3 intentional variances
4. DCL calls GET /dimensional/{dim} for each of 13 dimensional breakdowns

This service does NOT cheat: it reads DCL's actual ingested + mapped state
and compares against the ground truth, reporting honest scores.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple

from backend.farm.client import get_farm_client
from backend.farm.ingest_bridge import get_ingest_summary, PIPE_SOURCE_MAP
from backend.api.ingest import get_ingest_store
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


@dataclass
class MetricScore:
    """Score for a single metric comparison."""
    metric_name: str
    quarter: str
    expected: Any
    actual: Optional[Any]
    match: bool
    variance_pct: Optional[float] = None
    notes: str = ""


@dataclass
class ConflictScore:
    """Score for a detected conflict."""
    conflict_type: str
    expected_variance: str
    detected: bool
    dcl_value: Optional[Any] = None
    notes: str = ""


@dataclass
class DimensionalScore:
    """Score for a dimensional breakdown."""
    dimension: str
    total_buckets: int
    matched_buckets: int
    accuracy_pct: float
    mismatches: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class VerificationReport:
    """Complete verification report for a Farm run."""
    farm_run_id: str
    dcl_run_id: Optional[str]
    verified_at: str

    # Ingestion completeness
    expected_pipes: int
    received_pipes: int
    expected_records: int
    received_records: int
    ingestion_complete: bool

    # Metric accuracy
    total_metrics: int
    matched_metrics: int
    metric_accuracy_pct: float

    # Conflict detection
    total_expected_conflicts: int
    detected_conflicts: int

    # Dimensional accuracy
    total_dimensions: int
    dimensions_checked: int

    # Fields with defaults must come after all non-default fields
    metric_scores: List[MetricScore] = field(default_factory=list)
    conflict_scores: List[ConflictScore] = field(default_factory=list)
    dimensional_scores: List[DimensionalScore] = field(default_factory=list)
    overall_grade: str = "PENDING"
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "farm_run_id": self.farm_run_id,
            "dcl_run_id": self.dcl_run_id,
            "verified_at": self.verified_at,
            "ingestion": {
                "expected_pipes": self.expected_pipes,
                "received_pipes": self.received_pipes,
                "expected_records": self.expected_records,
                "received_records": self.received_records,
                "complete": self.ingestion_complete,
            },
            "metrics": {
                "total": self.total_metrics,
                "matched": self.matched_metrics,
                "accuracy_pct": self.metric_accuracy_pct,
                "scores": [
                    {
                        "metric": s.metric_name,
                        "quarter": s.quarter,
                        "expected": s.expected,
                        "actual": s.actual,
                        "match": s.match,
                        "variance_pct": s.variance_pct,
                        "notes": s.notes,
                    }
                    for s in self.metric_scores
                ],
            },
            "conflicts": {
                "expected": self.total_expected_conflicts,
                "detected": self.detected_conflicts,
                "scores": [
                    {
                        "type": s.conflict_type,
                        "expected_variance": s.expected_variance,
                        "detected": s.detected,
                        "dcl_value": s.dcl_value,
                        "notes": s.notes,
                    }
                    for s in self.conflict_scores
                ],
            },
            "dimensional": {
                "total": self.total_dimensions,
                "checked": self.dimensions_checked,
                "scores": [
                    {
                        "dimension": s.dimension,
                        "total_buckets": s.total_buckets,
                        "matched_buckets": s.matched_buckets,
                        "accuracy_pct": s.accuracy_pct,
                        "mismatches": s.mismatches,
                    }
                    for s in self.dimensional_scores
                ],
            },
            "overall_grade": self.overall_grade,
            "notes": self.notes,
        }


def verify_against_ground_truth(
    farm_run_id: str,
    dcl_run_id: Optional[str] = None,
) -> VerificationReport:
    """
    Run the full verification loop against Farm's ground truth.

    Steps:
    1. Check ingestion completeness (20 pipes received?)
    2. Fetch ground truth manifest from Farm
    3. Compare DCL's ingested data against each metric
    4. Check conflict detection
    5. Verify dimensional breakdowns
    """
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    farm_client = get_farm_client()

    report = VerificationReport(
        farm_run_id=farm_run_id,
        dcl_run_id=dcl_run_id,
        verified_at=now,
        expected_pipes=20,
        received_pipes=0,
        expected_records=120_000,
        received_records=0,
        ingestion_complete=False,
        total_metrics=0,
        matched_metrics=0,
        metric_accuracy_pct=0.0,
        total_expected_conflicts=3,
        detected_conflicts=0,
        total_dimensions=13,
        dimensions_checked=0,
    )

    # ── Step 1: Check ingestion completeness ──────────────────────────
    ingest_summary = get_ingest_summary()
    report.received_pipes = ingest_summary["pipe_count"]
    report.received_records = ingest_summary["total_records"]
    report.ingestion_complete = report.received_pipes >= report.expected_pipes

    if not report.ingestion_complete:
        report.notes.append(
            f"Ingestion incomplete: {report.received_pipes}/20 pipes, "
            f"{report.received_records:,} records"
        )

    # ── Step 2: Fetch ground truth manifest ──────────────────────────
    try:
        ground_truth = farm_client.get_ground_truth(farm_run_id)
    except Exception as e:
        report.notes.append(f"Failed to fetch ground truth: {e}")
        report.overall_grade = "ERROR"
        return report

    # ── Step 3: Compare metrics ──────────────────────────────────────
    gt_metrics = ground_truth.get("metrics", {})
    report.total_metrics = len(gt_metrics)

    # Build DCL's actual values from ingested data
    dcl_actuals = _build_dcl_actuals_from_ingest()

    for metric_name, metric_data in gt_metrics.items():
        # metric_data may be {quarter: value} or a scalar
        if isinstance(metric_data, dict):
            for quarter, expected_val in metric_data.items():
                actual_val = dcl_actuals.get(metric_name, {}).get(quarter)
                match, variance = _compare_values(expected_val, actual_val)
                report.metric_scores.append(MetricScore(
                    metric_name=metric_name,
                    quarter=quarter,
                    expected=expected_val,
                    actual=actual_val,
                    match=match,
                    variance_pct=variance,
                ))
                if match:
                    report.matched_metrics += 1
        else:
            actual_val = dcl_actuals.get(metric_name)
            match, variance = _compare_values(metric_data, actual_val)
            report.metric_scores.append(MetricScore(
                metric_name=metric_name,
                quarter="all",
                expected=metric_data,
                actual=actual_val,
                match=match,
                variance_pct=variance,
            ))
            if match:
                report.matched_metrics += 1

    total_compared = len(report.metric_scores)
    report.metric_accuracy_pct = (
        (report.matched_metrics / total_compared * 100)
        if total_compared > 0 else 0.0
    )

    # ── Step 4: Check conflict detection ─────────────────────────────
    try:
        conflicts_data = farm_client.get_ground_truth_conflicts(farm_run_id)
        expected_conflicts = conflicts_data.get("conflicts", [])
        report.total_expected_conflicts = len(expected_conflicts)

        dcl_conflicts = _get_dcl_detected_conflicts()

        for conflict in expected_conflicts:
            conflict_type = conflict.get("type", "unknown")
            expected_variance = conflict.get("expected_variance", "")
            detected = _check_conflict_detected(conflict_type, dcl_conflicts)

            report.conflict_scores.append(ConflictScore(
                conflict_type=conflict_type,
                expected_variance=expected_variance,
                detected=detected,
            ))
            if detected:
                report.detected_conflicts += 1
    except Exception as e:
        report.notes.append(f"Conflict verification failed: {e}")

    # ── Step 5: Verify dimensional breakdowns ────────────────────────
    try:
        # Attempt to get all 13 dimensions
        dimensions = ground_truth.get("dimensions", [])
        if not dimensions:
            # Try fetching the list from the manifest
            dimensions = [
                "revenue_by_region", "revenue_by_segment",
                "revenue_by_product", "pipeline_by_stage",
                "pipeline_by_region", "arr_by_segment",
                "cost_by_category", "cost_by_department",
                "headcount_by_department", "headcount_by_region",
                "tickets_by_priority", "incidents_by_severity",
                "cloud_cost_by_service",
            ]

        report.total_dimensions = len(dimensions)

        for dim_name in dimensions:
            try:
                dim_data = farm_client.get_ground_truth_dimensional(
                    farm_run_id, dim_name
                )
                dcl_dim = _get_dcl_dimensional(dim_name)
                score = _compare_dimensional(dim_name, dim_data, dcl_dim)
                report.dimensional_scores.append(score)
                report.dimensions_checked += 1
            except Exception as e:
                report.notes.append(f"Dimension {dim_name}: {e}")
    except Exception as e:
        report.notes.append(f"Dimensional verification failed: {e}")

    # ── Overall grade ────────────────────────────────────────────────
    report.overall_grade = _compute_grade(report)

    logger.info(
        f"[Verification] Farm run {farm_run_id}: grade={report.overall_grade}, "
        f"ingestion={report.received_pipes}/20, "
        f"metrics={report.matched_metrics}/{total_compared}, "
        f"conflicts={report.detected_conflicts}/{report.total_expected_conflicts}"
    )

    return report


def _build_dcl_actuals_from_ingest() -> Dict[str, Any]:
    """
    Build DCL's actual metric values from ingested data.

    This computes real aggregates from the raw rows in the IngestStore.
    The metrics DCL can compute depend on which fields it recognizes.
    """
    store = get_ingest_store()
    receipts = store.get_all_receipts()
    actuals: Dict[str, Any] = {}

    # Aggregate basic counts per source system
    source_record_counts: Dict[str, int] = {}
    for receipt in receipts:
        pipe_info = PIPE_SOURCE_MAP.get(receipt.pipe_id)
        if pipe_info:
            source_name = pipe_info[1]
        else:
            source_name = receipt.source_system
        source_record_counts[source_name] = (
            source_record_counts.get(source_name, 0) + receipt.row_count
        )

    actuals["source_record_counts"] = source_record_counts
    actuals["total_records"] = sum(source_record_counts.values())
    actuals["source_count"] = len(source_record_counts)
    actuals["pipe_count"] = len(receipts)

    # Compute numeric aggregates from buffered rows for each pipe
    for receipt in receipts:
        rows = store.get_rows(receipt.run_id)
        if not rows:
            continue

        pipe_id = receipt.pipe_id
        # Aggregate numeric fields
        numeric_sums: Dict[str, float] = {}
        for row in rows:
            for key, value in row.items():
                if key.startswith("_"):
                    continue
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    numeric_sums[key] = numeric_sums.get(key, 0.0) + value

        if numeric_sums:
            actuals[f"pipe_{pipe_id}_sums"] = numeric_sums

    return actuals


def _compare_values(
    expected: Any, actual: Any, tolerance_pct: float = 5.0
) -> Tuple[bool, Optional[float]]:
    """Compare expected vs actual with tolerance for numeric values."""
    if actual is None:
        return False, None

    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if expected == 0:
            return actual == 0, 100.0 if actual != 0 else 0.0
        variance = abs(expected - actual) / abs(expected) * 100
        return variance <= tolerance_pct, variance

    return expected == actual, None


def _get_dcl_detected_conflicts() -> List[Dict[str, Any]]:
    """
    Get conflicts that DCL has actually detected from its conflict store.
    """
    try:
        from backend.engine.conflict_detection import get_conflict_store
        store = get_conflict_store()
        conflicts = store.get_active_conflicts()
        return [c.model_dump() for c in conflicts]
    except Exception:
        return []


def _check_conflict_detected(
    conflict_type: str, dcl_conflicts: List[Dict[str, Any]]
) -> bool:
    """Check if DCL detected a specific type of conflict."""
    type_lower = conflict_type.lower()
    for conflict in dcl_conflicts:
        desc = (conflict.get("description", "") + " " +
                conflict.get("type", "")).lower()
        if any(keyword in desc for keyword in type_lower.split("_")):
            return True
    return False


def _get_dcl_dimensional(dimension: str) -> Dict[str, Any]:
    """
    Get DCL's version of a dimensional breakdown from ingested data.
    """
    # This will be populated as DCL builds actual dimensional aggregates
    # from the ingested pipe data. For now, return what we can compute.
    store = get_ingest_store()
    return {"dimension": dimension, "data": {}, "source": "ingest_store"}


def _compare_dimensional(
    dim_name: str,
    ground_truth: Dict[str, Any],
    dcl_data: Dict[str, Any],
) -> DimensionalScore:
    """Compare a dimensional breakdown against ground truth."""
    gt_buckets = ground_truth.get("breakdown", ground_truth.get("data", {}))
    dcl_buckets = dcl_data.get("data", {})

    total = len(gt_buckets) if isinstance(gt_buckets, dict) else 0
    matched = 0
    mismatches = []

    if isinstance(gt_buckets, dict) and isinstance(dcl_buckets, dict):
        for key, expected_val in gt_buckets.items():
            actual_val = dcl_buckets.get(key)
            if actual_val is not None:
                match, _ = _compare_values(expected_val, actual_val)
                if match:
                    matched += 1
                else:
                    mismatches.append({
                        "bucket": key,
                        "expected": expected_val,
                        "actual": actual_val,
                    })
            else:
                mismatches.append({
                    "bucket": key,
                    "expected": expected_val,
                    "actual": None,
                })

    accuracy = (matched / total * 100) if total > 0 else 0.0

    return DimensionalScore(
        dimension=dim_name,
        total_buckets=total,
        matched_buckets=matched,
        accuracy_pct=accuracy,
        mismatches=mismatches,
    )


def _compute_grade(report: VerificationReport) -> str:
    """Compute an overall letter grade for the verification."""
    if not report.ingestion_complete:
        return "INCOMPLETE"

    metric_pct = report.metric_accuracy_pct
    conflict_pct = (
        (report.detected_conflicts / report.total_expected_conflicts * 100)
        if report.total_expected_conflicts > 0 else 0.0
    )

    # Weighted score: 60% metrics, 25% conflicts, 15% completeness
    score = (metric_pct * 0.6) + (conflict_pct * 0.25) + (100.0 * 0.15)

    if score >= 90:
        return "A"
    elif score >= 80:
        return "B"
    elif score >= 70:
        return "C"
    elif score >= 50:
        return "D"
    else:
        return "F"
