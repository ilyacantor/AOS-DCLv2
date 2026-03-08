"""
Metric Materializer — transforms raw ingest rows into canonical metric data points.

Uses the ontology (concept → example_fields) to recognize source-system field names,
then applies extraction rules (from metric_extractions.yaml) to aggregate raw
transactional rows into publishable metric values.

Flow:
  1. Build a reverse index: lowercase field name → ontology concept ID
  2. For each extraction rule, scan rows to find matching value fields
  3. Group by (metric, period, dimensions) and aggregate per measure_op
  4. Return canonical data points ready for the query engine

RACI: This is DCL's job — semantic catalog, ontology, schema-on-write validation.
"""

import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from backend.engine.ontology import get_ontology
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_EXTRACTIONS_PATH = (
    Path(__file__).parent.parent / "config" / "definitions" / "metric_extractions.yaml"
)

# ---------------------------------------------------------------------------
# Source-system field name normalization
# ---------------------------------------------------------------------------

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_SF_CUSTOM_SUFFIX = re.compile(r"__c$", re.IGNORECASE)


def _normalize_key(name: str) -> str:
    """Normalize a source-system field name to snake_case.

    Handles common source-system naming conventions:
      HireDate      → hire_date     (PascalCase → snake_case)
      Region__c     → region        (Salesforce custom field suffix)
      StageName     → stage_name    (PascalCase → snake_case)
      AccountId     → account_id    (PascalCase → snake_case)
      Worker_Status → worker_status (already correct)
      IsWon         → is_won        (camelCase bool prefix)
    """
    # Strip Salesforce __c custom field suffix
    cleaned = _SF_CUSTOM_SUFFIX.sub("", name)
    # PascalCase/camelCase → snake_case
    snake = _CAMEL_BOUNDARY.sub("_", cleaned)
    return snake.lower()


# ---------------------------------------------------------------------------
# Field → Concept reverse index
# ---------------------------------------------------------------------------


def _build_field_concept_index() -> Dict[str, str]:
    """Build a reverse index mapping lowercase field names to concept IDs.

    Uses example_fields and aliases from the ontology. If a field name
    appears in multiple concepts, the first concept wins (order is stable
    from the YAML).
    """
    index: Dict[str, str] = {}
    for concept in get_ontology():
        cid = concept.id
        for field in concept.example_fields:
            key = field.lower()
            if key not in index:
                index[key] = cid
        for alias in concept.aliases:
            key = alias.lower()
            if key not in index:
                index[key] = cid
    return index


# ---------------------------------------------------------------------------
# Extraction rule loader
# ---------------------------------------------------------------------------


def _load_extraction_rules() -> List[Dict[str, Any]]:
    """Load metric extraction rules from YAML."""
    if not _EXTRACTIONS_PATH.exists():
        logger.error(f"[Materializer] Extraction rules not found: {_EXTRACTIONS_PATH}")
        return []
    with open(_EXTRACTIONS_PATH) as f:
        data = yaml.safe_load(f)
    rules = data.get("extractions", [])
    logger.info(f"[Materializer] Loaded {len(rules)} extraction rules")
    return rules


# ---------------------------------------------------------------------------
# Period derivation
# ---------------------------------------------------------------------------

_DATE_PATTERNS = [
    (re.compile(r"^\d{4}-Q[1-4]$"), None),                      # Already quarterly
    (re.compile(r"^\d{4}-[01]\d$"), "month"),                    # YYYY-MM
    (re.compile(r"^\d{4}-[01]\d-[0-3]\d$"), "date"),             # YYYY-MM-DD
    (re.compile(r"^\d{4}-[01]\d-[0-3]\dT"), "datetime"),         # ISO datetime
    (re.compile(r"^Q[1-4]\s*\d{4}$"), "quarter_alt"),            # Q1 2024
    (re.compile(r"^\d{4}$"), "year"),                             # YYYY
]


def _derive_period(raw_value: Any, grain: str = "quarter") -> Optional[str]:
    """Convert a raw date/period value into a canonical period string.

    Returns a quarter string like '2024-Q3' by default.
    """
    if raw_value is None:
        return None

    s = str(raw_value).strip()
    if not s:
        return None

    # Already a quarter
    if re.match(r"^\d{4}-Q[1-4]$", s):
        return s

    # Quarter alt format: "Q3 2024"
    m = re.match(r"^Q([1-4])\s*(\d{4})$", s)
    if m:
        return f"{m.group(2)}-Q{m.group(1)}"

    # Year only → Q4
    if re.match(r"^\d{4}$", s):
        return f"{s}-Q4"

    # YYYY-MM or YYYY-MM-DD or ISO datetime
    m = re.match(r"^(\d{4})-(\d{2})", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        quarter = (month - 1) // 3 + 1
        if grain == "month":
            return f"{year}-{month:02d}"
        return f"{year}-Q{quarter}"

    return None


# ---------------------------------------------------------------------------
# MetricMaterializer
# ---------------------------------------------------------------------------


class MetricMaterializer:
    """Transforms raw ingest buffer rows into canonical metric data points."""

    def __init__(self) -> None:
        self._rules = _load_extraction_rules()
        self._field_index = _build_field_concept_index()
        self._concept_by_id = {c.id: c for c in get_ontology()}

    def _resolve_field(
        self, row: Dict[str, Any], concept_id: str,
        field_hint: Optional[str] = None, partial: bool = False,
        *,
        _keys_lower: Optional[Dict[str, str]] = None,
        _keys_normalized: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """Find the actual field name in a row that maps to the given concept.

        Args:
            row: The data row to search.
            concept_id: Ontology concept to look for.
            field_hint: Prefer a field literally matching this name.
            partial: If True, allow substring matching as a fallback.
                     Use for value/period fields, NOT for dimensions or counts.
            _keys_lower: Pre-computed {lowercase_key: original_key} index.
            _keys_normalized: Pre-computed {normalized_key: original_key} index.

        Priority:
          1. field_hint matches a key in the row (case-insensitive, then normalized)
          2. Exact match: concept's example_fields against row keys (lowercase, then normalized)
          3. Reverse index: row key maps to concept_id (lowercase, then normalized)
          4. (partial only) Substring match with length constraints
        """
        if _keys_lower is None:
            _keys_lower = {k.lower(): k for k in row.keys() if not k.startswith("_")}
        if _keys_normalized is None:
            _keys_normalized = {_normalize_key(k): k for k in row.keys() if not k.startswith("_")}
        row_keys_lower = _keys_lower
        row_keys_normalized = _keys_normalized

        # Priority 1: field hint (exact, then normalized)
        if field_hint:
            hint_lower = field_hint.lower()
            if hint_lower in row_keys_lower:
                return row_keys_lower[hint_lower]
            if hint_lower in row_keys_normalized:
                return row_keys_normalized[hint_lower]

        # Priority 2: concept's example_fields (exact, then normalized)
        concept = self._concept_by_id.get(concept_id)
        if concept:
            for ef in concept.example_fields:
                ef_lower = ef.lower()
                if ef_lower in row_keys_lower:
                    return row_keys_lower[ef_lower]
            # Try again with normalized row keys
            for ef in concept.example_fields:
                ef_lower = ef.lower()
                if ef_lower in row_keys_normalized:
                    return row_keys_normalized[ef_lower]

        # Priority 3: reverse index (exact, then normalized)
        for key_lower, original_key in row_keys_lower.items():
            if self._field_index.get(key_lower) == concept_id:
                return original_key
        for key_norm, original_key in row_keys_normalized.items():
            if key_norm not in row_keys_lower and self._field_index.get(key_norm) == concept_id:
                return original_key

        # Priority 4: partial/substring match (only if requested)
        # Matches cases like CloseDate→date, StageName→stage_name
        if partial and concept:
            best_match = None
            best_len = 0
            for ef in concept.example_fields:
                ef_lower = ef.lower()
                if len(ef_lower) < 4:
                    continue
                for key_lower, original_key in row_keys_lower.items():
                    if len(key_lower) < 4:
                        continue
                    if ef_lower in key_lower or key_lower in ef_lower:
                        # Require meaningful overlap: shorter must be >= 40% of longer
                        shorter = min(len(ef_lower), len(key_lower))
                        longer = max(len(ef_lower), len(key_lower))
                        if shorter / longer >= 0.35:
                            if len(ef_lower) > best_len:
                                best_match = original_key
                                best_len = len(ef_lower)
            if best_match:
                return best_match

        return None

    def _extract_numeric(self, row: Dict[str, Any], field_name: str) -> Optional[float]:
        """Safely extract a numeric value from a row field."""
        val = row.get(field_name)
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            # Strip currency symbols, commas
            cleaned = val.replace(",", "").replace("$", "").replace("€", "").strip()
            try:
                return float(cleaned)
            except (ValueError, TypeError):
                return None
        return None

    def _check_filters(
        self, row: Dict[str, Any], filters: List[Dict[str, Any]],
        *,
        _keys_lower: Optional[Dict[str, str]] = None,
        _keys_normalized: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Evaluate concept-based filter conditions. Returns True if row passes.

        optional semantics:
          - If the filter field is NOT found in the row → skip (don't reject)
          - If the filter field IS found but value doesn't match → reject
        This lets sources without the filter field pass, while sources
        WITH the field get properly filtered.
        """
        for filt in filters:
            concept_id = filt.get("concept", "")
            is_optional = filt.get("optional", False)

            # Find the field in the row for this concept
            hint = filt.get("field_hint")
            field_name = self._resolve_field(
                row, concept_id, hint,
                _keys_lower=_keys_lower, _keys_normalized=_keys_normalized,
            )

            if field_name is None:
                # Field not present in this row
                if is_optional:
                    continue  # Skip filter — source doesn't have this field
                return False  # Required filter missing → reject

            raw_val = row.get(field_name)
            if raw_val is None:
                if is_optional:
                    continue
                return False

            # Normalize to string for comparison
            val_str = str(raw_val).strip()
            val_lower = val_str.lower()

            # Include filter: field present, value must be in allowed list
            values = filt.get("values")
            if values is not None:
                normalized_values = [str(v).lower().strip() for v in values]
                if val_lower not in normalized_values:
                    return False  # Field present but wrong value → always reject

            # Exclude filter: field present, value must NOT be in excluded list
            exclude_values = filt.get("exclude_values")
            if exclude_values is not None:
                normalized_excludes = [str(v).lower().strip() for v in exclude_values]
                if val_lower in normalized_excludes:
                    return False  # Field present with excluded value → reject

        return True

    def _resolve_dimensions(
        self, row: Dict[str, Any], dimension_map: Dict[str, str],
        *,
        _keys_lower: Optional[Dict[str, str]] = None,
        _keys_normalized: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """Extract canonical dimension values from a row using concept mapping."""
        dims: Dict[str, str] = {}
        for concept_id, dim_name in dimension_map.items():
            field_name = self._resolve_field(
                row, concept_id,
                _keys_lower=_keys_lower, _keys_normalized=_keys_normalized,
            )
            if field_name:
                val = row.get(field_name)
                if val is not None:
                    dims[dim_name] = str(val)
        return dims

    def materialize(
        self,
        pipe_id: str,
        source_system: str,
        rows: List[Dict[str, Any]],
        dispatch_id: str = "",
    ) -> List[Dict[str, Any]]:
        """Transform raw rows into canonical metric data points.

        Returns a list of dicts:
            {metric, value, period, dimensions, source_system, pipe_id, materialized_at}
        """
        if not rows or not self._rules:
            return []

        now = datetime.now(timezone.utc).isoformat()
        all_points: List[Dict[str, Any]] = []

        # Pre-compute key indexes from the union of all row keys.
        # Rows in the same pipe may have different schemas (e.g. total rows
        # without a region field vs regional rows with one), so we must
        # include keys from every row to avoid silently dropping dimensions.
        all_keys: Dict[str, str] = {}
        for row in rows:
            for k in row.keys():
                if not k.startswith("_") and k not in all_keys:
                    all_keys[k] = k
        keys_lower = {k.lower(): k for k in all_keys}
        keys_normalized = {_normalize_key(k): k for k in all_keys}

        for rule in self._rules:
            metric_id = rule["metric"]
            value_concept = rule.get("value_concept", "")
            measure_op = rule.get("measure_op", "sum")
            period_concept = rule.get("period_concept", "date")
            count_concept = rule.get("count_concept")
            value_hint = rule.get("value_field_hint")
            strict_hint = rule.get("strict_hint", False)
            filters = rule.get("filters", [])
            dim_map = rule.get("dimension_map", {})
            unit_scale = rule.get("unit_scale", 1.0)
            ratio_hint = rule.get("ratio_hint")

            # Determine the default grain from the metric
            grain = "quarter"

            # Accumulator: (period, frozenset(dims)) → list of values
            groups: Dict[Tuple, List[float]] = defaultdict(list)
            count_groups: Dict[Tuple, int] = defaultdict(int)

            for row in rows:
                # Step 1: Check filters
                if filters and not self._check_filters(
                    row, filters,
                    _keys_lower=keys_lower, _keys_normalized=keys_normalized,
                ):
                    continue

                # Step 2: Find the period (use partial matching for date fields)
                period_field = self._resolve_field(
                    row, period_concept, partial=True,
                    _keys_lower=keys_lower, _keys_normalized=keys_normalized,
                )
                period = None
                if period_field:
                    period = _derive_period(row.get(period_field), grain)
                if not period:
                    period = "current"

                # Step 3: Resolve dimensions (exact matching only — no false positives)
                dims = self._resolve_dimensions(
                    row, dim_map,
                    _keys_lower=keys_lower, _keys_normalized=keys_normalized,
                )
                group_key = (period, tuple(sorted(dims.items())))

                # Step 4: Extract value or count
                if measure_op == "count" and count_concept:
                    # For count metrics, check if this row has fields for the concept
                    # Use exact matching only to avoid false positives
                    count_field = self._resolve_field(
                        row, count_concept,
                        _keys_lower=keys_lower, _keys_normalized=keys_normalized,
                    )
                    if count_field is not None:
                        count_groups[group_key] += 1
                    continue

                if measure_op == "ratio" and ratio_hint:
                    # For ratio metrics, look for a pre-computed ratio field
                    ratio_field = self._resolve_field(
                        row, value_concept, ratio_hint, partial=True,
                        _keys_lower=keys_lower, _keys_normalized=keys_normalized,
                    )
                    if ratio_field:
                        val = self._extract_numeric(row, ratio_field)
                        if val is not None:
                            groups[group_key].append(val)
                    continue

                # Standard value extraction (use partial for value fields)
                if strict_hint and value_hint:
                    # strict_hint: ONLY match the exact field name, no concept fallback.
                    # Used for P&L metrics that must only come from rows with the
                    # exact field (e.g. net_income, ebitda, cogs) — not from
                    # transaction-level rows that happen to have revenue/cost concepts.
                    hint_lower = value_hint.lower()
                    if hint_lower in keys_lower:
                        value_field = keys_lower[hint_lower]
                    elif hint_lower in keys_normalized:
                        value_field = keys_normalized[hint_lower]
                    else:
                        continue
                else:
                    value_field = self._resolve_field(
                        row, value_concept, value_hint, partial=True,
                        _keys_lower=keys_lower, _keys_normalized=keys_normalized,
                    )
                if value_field is None:
                    continue

                val = self._extract_numeric(row, value_field)
                if val is None:
                    continue

                groups[group_key].append(val)

            # Step 5: Aggregate
            result_groups = groups if measure_op != "count" else {}

            if measure_op == "count" and count_concept:
                for group_key, count in count_groups.items():
                    period, dim_tuple = group_key
                    all_points.append({
                        "metric": metric_id,
                        "value": float(count),
                        "period": period,
                        "dimensions": dict(dim_tuple),
                        "source_system": source_system,
                        "pipe_id": pipe_id,
                        "dispatch_id": dispatch_id,
                        "materialized_at": now,
                    })
                continue

            for group_key, values in result_groups.items():
                if not values:
                    continue

                period, dim_tuple = group_key

                if measure_op in ("sum", "point_in_time_sum"):
                    agg_val = sum(values)
                elif measure_op == "avg":
                    agg_val = sum(values) / len(values)
                elif measure_op == "ratio":
                    agg_val = sum(values) / len(values)
                elif measure_op == "avg_days_between":
                    agg_val = sum(values) / len(values)
                else:
                    agg_val = sum(values)

                agg_val *= unit_scale

                all_points.append({
                    "metric": metric_id,
                    "value": round(agg_val, 6),
                    "period": period,
                    "dimensions": dict(dim_tuple),
                    "source_system": source_system,
                    "pipe_id": pipe_id,
                    "dispatch_id": dispatch_id,
                    "materialized_at": now,
                })

        if all_points:
            metrics_found = set(p["metric"] for p in all_points)
            logger.info(
                f"[Materializer] {pipe_id}: {len(all_points)} data points "
                f"from {len(rows)} rows — metrics: {sorted(metrics_found)}"
            )

        return all_points


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_materializer: Optional[MetricMaterializer] = None


def get_materializer() -> MetricMaterializer:
    """Return the singleton MetricMaterializer instance."""
    global _materializer
    if _materializer is None:
        _materializer = MetricMaterializer()
    return _materializer


def reload_materializer() -> MetricMaterializer:
    """Force re-create the materializer (e.g. after rule changes)."""
    global _materializer
    _materializer = MetricMaterializer()
    return _materializer
