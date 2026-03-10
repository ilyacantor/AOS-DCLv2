"""
Hierarchical Dimension Store — Supabase-backed dimension value hierarchy.

Provides tree-structured dimension hierarchies with parent-child relationships,
depth levels, materialized paths, and roll-up/drill-down support.

Required dimensions (COFA foundation):
  - geo: Geographic hierarchy (Region → Country → Sub-region)
  - segment: Business unit / practice / service line
  - cost_center: Organizational cost center hierarchy
  - project: Project/engagement identifiers (flat for now)
  - period: Time hierarchy (Year → Quarter → Month)
  - account_type: GL account type (Asset, Liability, Equity, Revenue, Expense)

Revenue drill-through uses relationship tables (not this module):
  Region → Rep → Customer → Project
  See: drill_through.py

Storage: Supabase PostgreSQL via DCL's shared connection pool.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Schema
# ═══════════════════════════════════════════════════════════════════════════════

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dimension_values (
    id SERIAL PRIMARY KEY,
    dimension_id VARCHAR(50) NOT NULL,
    value VARCHAR(200) NOT NULL,
    value_code VARCHAR(50),
    parent_id VARCHAR(200),
    depth INTEGER NOT NULL DEFAULT 0,
    path VARCHAR(500) NOT NULL,
    entity_id VARCHAR(100),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(dimension_id, value)
);

CREATE INDEX IF NOT EXISTS idx_dim_values_dimension_value
    ON dimension_values(dimension_id, value);
CREATE INDEX IF NOT EXISTS idx_dim_values_dimension_parent
    ON dimension_values(dimension_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_dim_values_dimension_path
    ON dimension_values(dimension_id, path);
CREATE INDEX IF NOT EXISTS idx_dim_values_depth
    ON dimension_values(dimension_id, depth);

-- Revenue drill-through relationship tables
CREATE TABLE IF NOT EXISTS rep_assignments (
    id SERIAL PRIMARY KEY,
    rep_id VARCHAR(50) NOT NULL UNIQUE,
    rep_name VARCHAR(200) NOT NULL,
    region VARCHAR(100) NOT NULL,
    entity_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rep_assignments_region
    ON rep_assignments(region);

CREATE TABLE IF NOT EXISTS customer_rep_map (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    rep_id VARCHAR(50) NOT NULL REFERENCES rep_assignments(rep_id),
    entity_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(customer_id)
);
CREATE INDEX IF NOT EXISTS idx_customer_rep_map_rep
    ON customer_rep_map(rep_id);

CREATE TABLE IF NOT EXISTS project_customer_map (
    id SERIAL PRIMARY KEY,
    project_id VARCHAR(100) NOT NULL,
    project_name VARCHAR(200) NOT NULL,
    customer_id VARCHAR(100) NOT NULL REFERENCES customer_rep_map(customer_id),
    entity_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(project_id)
);
CREATE INDEX IF NOT EXISTS idx_project_customer_map_customer
    ON project_customer_map(customer_id);
"""


def ensure_schema() -> bool:
    """Create dimension_values and relationship tables if they don't exist.

    Returns True on success, False on failure (DB unavailable).
    """
    with get_connection() as conn:
        if conn is None:
            logger.error(
                "Cannot create dimension hierarchy schema — database unavailable. "
                "Set DATABASE_URL to a valid Supabase PostgreSQL connection string."
            )
            return False
        try:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA_SQL)
            conn.commit()
            logger.info("[dimension_hierarchy] Schema ensured (dimension_values + relationship tables)")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"[dimension_hierarchy] Schema creation failed: {e}", exc_info=True)
            return False


# ═══════════════════════════════════════════════════════════════════════════════
# DimensionValue data class
# ═══════════════════════════════════════════════════════════════════════════════

class DimensionValue:
    """A single value in a dimension hierarchy."""

    __slots__ = (
        "dimension_id", "value", "value_code", "parent_id",
        "depth", "path", "entity_id", "metadata",
    )

    def __init__(
        self,
        dimension_id: str,
        value: str,
        value_code: Optional[str] = None,
        parent_id: Optional[str] = None,
        depth: int = 0,
        path: str = "",
        entity_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.dimension_id = dimension_id
        self.value = value
        self.value_code = value_code
        self.parent_id = parent_id
        self.depth = depth
        self.path = path or value
        self.entity_id = entity_id
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension_id": self.dimension_id,
            "value": self.value,
            "value_code": self.value_code,
            "parent_id": self.parent_id,
            "depth": self.depth,
            "path": self.path,
            "entity_id": self.entity_id,
            "metadata": self.metadata,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# DimensionHierarchyStore
# ═══════════════════════════════════════════════════════════════════════════════

class DimensionHierarchyStore:
    """Read/write operations on the dimension_values table."""

    def insert_value(self, dv: DimensionValue) -> bool:
        """Insert a dimension value. Returns True on success."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "Cannot insert dimension value — database unavailable. "
                    "Ensure DATABASE_URL is set."
                )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dimension_values
                            (dimension_id, value, value_code, parent_id, depth, path, entity_id, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (dimension_id, value) DO UPDATE SET
                            value_code = EXCLUDED.value_code,
                            parent_id = EXCLUDED.parent_id,
                            depth = EXCLUDED.depth,
                            path = EXCLUDED.path,
                            entity_id = EXCLUDED.entity_id,
                            metadata = EXCLUDED.metadata,
                            updated_at = NOW()
                        """,
                        (
                            dv.dimension_id, dv.value, dv.value_code,
                            dv.parent_id, dv.depth, dv.path,
                            dv.entity_id, json.dumps(dv.metadata),
                        ),
                    )
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"[dimension_hierarchy] Insert failed for {dv.dimension_id}/{dv.value}: {e}",
                    exc_info=True,
                )
                raise

    def insert_batch(self, values: List[DimensionValue]) -> int:
        """Insert multiple dimension values in a single multi-row statement.

        Returns count of inserted rows.
        """
        if not values:
            return 0
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "Cannot insert dimension values — database unavailable. "
                    "Ensure DATABASE_URL is set."
                )
            try:
                with conn.cursor() as cur:
                    args = [
                        (
                            dv.dimension_id, dv.value, dv.value_code,
                            dv.parent_id, dv.depth, dv.path,
                            dv.entity_id, json.dumps(dv.metadata),
                        )
                        for dv in values
                    ]
                    cur.executemany(
                        """
                        INSERT INTO dimension_values
                            (dimension_id, value, value_code, parent_id, depth, path, entity_id, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (dimension_id, value) DO UPDATE SET
                            value_code = EXCLUDED.value_code,
                            parent_id = EXCLUDED.parent_id,
                            depth = EXCLUDED.depth,
                            path = EXCLUDED.path,
                            entity_id = EXCLUDED.entity_id,
                            metadata = EXCLUDED.metadata,
                            updated_at = NOW()
                        """,
                        args,
                    )
                conn.commit()
                return len(values)
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"[dimension_hierarchy] Batch insert failed: {e}",
                    exc_info=True,
                )
                raise

    # ─── Read operations ─────────────────────────────────────────────────

    def get_value(self, dimension_id: str, value: str) -> Optional[DimensionValue]:
        """Get a single dimension value."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable for dimension query")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dimension_id, value, value_code, parent_id,
                           depth, path, entity_id, metadata
                    FROM dimension_values
                    WHERE dimension_id = %s AND value = %s
                    """,
                    (dimension_id, value),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return self._row_to_dv(row)

    def get_children(self, dimension_id: str, parent_value: str) -> List[DimensionValue]:
        """Get immediate children of a value in a dimension."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable for dimension query")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dimension_id, value, value_code, parent_id,
                           depth, path, entity_id, metadata
                    FROM dimension_values
                    WHERE dimension_id = %s AND parent_id = %s
                    ORDER BY value
                    """,
                    (dimension_id, parent_value),
                )
                return [self._row_to_dv(row) for row in cur.fetchall()]

    def get_parent(self, dimension_id: str, value: str) -> Optional[DimensionValue]:
        """Get the parent of a value in a dimension."""
        dv = self.get_value(dimension_id, value)
        if dv is None or dv.parent_id is None:
            return None
        return self.get_value(dimension_id, dv.parent_id)

    def get_ancestors(self, dimension_id: str, value: str) -> List[DimensionValue]:
        """Get all ancestors from value up to root (root first, value last)."""
        dv = self.get_value(dimension_id, value)
        if dv is None:
            return []
        # Use materialized path for efficient lookup
        parts = dv.path.split("/")
        if len(parts) <= 1:
            return [dv]
        # Single query for all path components instead of N+1
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable for dimension ancestor query")
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(parts))
                cur.execute(
                    f"""
                    SELECT dimension_id, value, value_code, parent_id,
                           depth, path, entity_id, metadata
                    FROM dimension_values
                    WHERE dimension_id = %s AND value IN ({placeholders})
                    ORDER BY depth, value
                    """,
                    (dimension_id, *parts),
                )
                rows = cur.fetchall()
        # Re-order to match path order (root first, value last)
        by_value = {self._row_to_dv(row).value: self._row_to_dv(row) for row in rows}
        return [by_value[part] for part in parts if part in by_value]

    def get_descendants(self, dimension_id: str, value: str) -> List[DimensionValue]:
        """Get all descendants of a value using path prefix matching."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable for dimension query")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dimension_id, value, value_code, parent_id,
                           depth, path, entity_id, metadata
                    FROM dimension_values
                    WHERE dimension_id = %s AND path LIKE %s AND value != %s
                    ORDER BY depth, value
                    """,
                    (dimension_id, f"{value}/%", value),
                )
                return [self._row_to_dv(row) for row in cur.fetchall()]

    def get_siblings(self, dimension_id: str, value: str) -> List[DimensionValue]:
        """Get siblings (same parent, excluding self)."""
        dv = self.get_value(dimension_id, value)
        if dv is None:
            return []
        if dv.parent_id is None:
            # Root level — siblings are other root nodes
            with get_connection() as conn:
                if conn is None:
                    raise RuntimeError("Database unavailable")
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT dimension_id, value, value_code, parent_id,
                               depth, path, entity_id, metadata
                        FROM dimension_values
                        WHERE dimension_id = %s AND parent_id IS NULL AND value != %s
                        ORDER BY value
                        """,
                        (dimension_id, value),
                    )
                    return [self._row_to_dv(row) for row in cur.fetchall()]
        children = self.get_children(dimension_id, dv.parent_id)
        return [c for c in children if c.value != value]

    def get_roots(self, dimension_id: str) -> List[DimensionValue]:
        """Get root values (depth=0) for a dimension."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dimension_id, value, value_code, parent_id,
                           depth, path, entity_id, metadata
                    FROM dimension_values
                    WHERE dimension_id = %s AND parent_id IS NULL
                    ORDER BY value
                    """,
                    (dimension_id,),
                )
                return [self._row_to_dv(row) for row in cur.fetchall()]

    def get_at_depth(self, dimension_id: str, depth: int) -> List[DimensionValue]:
        """Get all values at a specific depth for a dimension."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dimension_id, value, value_code, parent_id,
                           depth, path, entity_id, metadata
                    FROM dimension_values
                    WHERE dimension_id = %s AND depth = %s
                    ORDER BY value
                    """,
                    (dimension_id, depth),
                )
                return [self._row_to_dv(row) for row in cur.fetchall()]

    def get_max_depth(self, dimension_id: str) -> int:
        """Get the maximum depth for a dimension."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(MAX(depth), 0) FROM dimension_values WHERE dimension_id = %s",
                    (dimension_id,),
                )
                row = cur.fetchone()
                return row[0] if row else 0

    def get_all_values(self, dimension_id: str) -> List[DimensionValue]:
        """Get all values for a dimension, ordered by depth then value."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dimension_id, value, value_code, parent_id,
                           depth, path, entity_id, metadata
                    FROM dimension_values
                    WHERE dimension_id = %s
                    ORDER BY depth, value
                    """,
                    (dimension_id,),
                )
                return [self._row_to_dv(row) for row in cur.fetchall()]

    def get_dimension_ids(self) -> List[str]:
        """Get all distinct dimension IDs."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT dimension_id FROM dimension_values ORDER BY dimension_id"
                )
                return [row[0] for row in cur.fetchall()]

    # ─── Roll-up support ─────────────────────────────────────────────────

    def get_rollup_values(
        self, dimension_id: str, target_value: str
    ) -> List[str]:
        """Get all leaf values that roll up to target_value.

        If target_value is a leaf, returns [target_value].
        If target_value has children, returns all descendant leaf values.
        Used for aggregation: "revenue for US" = sum of revenue for all
        sub-regions under US.
        """
        dv = self.get_value(dimension_id, target_value)
        if dv is None:
            return []
        descendants = self.get_descendants(dimension_id, target_value)
        if not descendants:
            return [target_value]  # Leaf node
        # Return only leaves (values with no children)
        all_parents = {d.parent_id for d in descendants if d.parent_id}
        all_parents.add(target_value)
        leaves = [d.value for d in descendants if d.value not in all_parents]
        if not leaves:
            # All descendants are parents — shouldn't happen in well-formed hierarchy
            return [d.value for d in descendants]
        return leaves

    # ─── Internal ────────────────────────────────────────────────────────

    @staticmethod
    def _row_to_dv(row: tuple) -> DimensionValue:
        """Convert a database row to DimensionValue."""
        metadata = row[7]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        return DimensionValue(
            dimension_id=row[0],
            value=row[1],
            value_code=row[2],
            parent_id=row[3],
            depth=row[4],
            path=row[5],
            entity_id=row[6],
            metadata=metadata or {},
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Initial hierarchy population
# ═══════════════════════════════════════════════════════════════════════════════

def _build_geo_hierarchy() -> List[DimensionValue]:
    """Build the geo dimension hierarchy: Root → Region → Country → Sub-region."""
    values = []

    def _add(value, code, parent, depth, path, display=None):
        meta = {}
        if display:
            meta["display_name"] = display
        values.append(DimensionValue(
            dimension_id="geo",
            value=value,
            value_code=code,
            parent_id=parent,
            depth=depth,
            path=path,
            metadata=meta,
        ))

    # Depth 0: Root regions (mapped to Farm's AMER/EMEA/APAC)
    _add("NA", "NA", None, 0, "NA", "North America")
    _add("EMEA", "EMEA", None, 0, "EMEA", "Europe, Middle East & Africa")
    _add("APAC", "APAC", None, 0, "APAC", "Asia Pacific")

    # Depth 1: Countries under NA
    _add("US", "US", "NA", 1, "NA/US", "United States")
    _add("Canada", "CA", "NA", 1, "NA/Canada", "Canada")

    # Depth 2: Sub-regions under US
    _add("US-West", "10", "US", 2, "NA/US/US-West", "US West Coast")
    _add("US-East", "20", "US", 2, "NA/US/US-East", "US East Coast")
    _add("US-Central", "30", "US", 2, "NA/US/US-Central", "US Central")

    # Depth 1: Countries under EMEA
    _add("UK", "UK", "EMEA", 1, "EMEA/UK", "United Kingdom")
    _add("Germany", "DE", "EMEA", 1, "EMEA/Germany", "Germany")
    _add("France", "FR", "EMEA", 1, "EMEA/France", "France")
    _add("Poland", "PL", "EMEA", 1, "EMEA/Poland", "Poland")

    # Depth 1: Countries under APAC
    _add("India", "IN", "APAC", 1, "APAC/India", "India")
    _add("Japan", "JP", "APAC", 1, "APAC/Japan", "Japan")
    _add("Australia", "AU", "APAC", 1, "APAC/Australia", "Australia")
    _add("Philippines", "PH", "APAC", 1, "APAC/Philippines", "Philippines")

    return values


def _build_segment_hierarchy() -> List[DimensionValue]:
    """Build the segment dimension hierarchy (flat for now, matches Farm)."""
    values = []
    for code, value, display in [
        ("100", "Enterprise", "Enterprise Segment"),
        ("200", "Mid-Market", "Mid-Market Segment"),
        ("300", "SMB", "Small & Medium Business"),
    ]:
        values.append(DimensionValue(
            dimension_id="segment",
            value=value,
            value_code=code,
            parent_id=None,
            depth=0,
            path=value,
            metadata={"display_name": display},
        ))
    return values


def _build_cost_center_hierarchy() -> List[DimensionValue]:
    """Build cost center hierarchy based on department structure from Farm."""
    values = []

    def _add(value, code, parent, depth, path):
        values.append(DimensionValue(
            dimension_id="cost_center",
            value=value, value_code=code, parent_id=parent,
            depth=depth, path=path,
        ))

    # Depth 0: Top-level cost centers
    _add("Revenue", "CC-REV", None, 0, "Revenue")
    _add("COGS", "CC-COGS", None, 0, "COGS")
    _add("OpEx", "CC-OPEX", None, 0, "OpEx")

    # Depth 1: Under Revenue
    _add("Sales", "CC-SALES", "Revenue", 1, "Revenue/Sales")
    _add("Customer Success", "CC-CS", "Revenue", 1, "Revenue/Customer Success")

    # Depth 1: Under COGS
    _add("Hosting", "CC-HOST", "COGS", 1, "COGS/Hosting")
    _add("Support Staff", "CC-SUPP", "COGS", 1, "COGS/Support Staff")
    _add("Professional Services", "CC-PS", "COGS", 1, "COGS/Professional Services")

    # Depth 1: Under OpEx
    _add("Sales & Marketing", "CC-SM", "OpEx", 1, "OpEx/Sales & Marketing")
    _add("Research & Development", "CC-RD", "OpEx", 1, "OpEx/Research & Development")
    _add("General & Administrative", "CC-GA", "OpEx", 1, "OpEx/General & Administrative")

    # Depth 2: Under Engineering (R&D)
    _add("Engineering", "CC-ENG", "Research & Development", 2,
         "OpEx/Research & Development/Engineering")
    _add("Product", "CC-PROD", "Research & Development", 2,
         "OpEx/Research & Development/Product")

    return values


def _build_period_hierarchy() -> List[DimensionValue]:
    """Build time hierarchy: Year → Quarter → Month."""
    values = []
    for year in range(2024, 2027):
        y_str = str(year)
        values.append(DimensionValue(
            dimension_id="period", value=y_str, value_code=y_str,
            parent_id=None, depth=0, path=y_str,
        ))
        for q in range(1, 5):
            q_str = f"{year}-Q{q}"
            values.append(DimensionValue(
                dimension_id="period", value=q_str, value_code=q_str,
                parent_id=y_str, depth=1, path=f"{y_str}/{q_str}",
            ))
            for m_offset in range(3):
                month = (q - 1) * 3 + m_offset + 1
                m_str = f"{year}-{month:02d}"
                values.append(DimensionValue(
                    dimension_id="period", value=m_str, value_code=m_str,
                    parent_id=q_str, depth=2, path=f"{y_str}/{q_str}/{m_str}",
                ))
    return values


def _build_account_type_hierarchy() -> List[DimensionValue]:
    """Build GL account type hierarchy."""
    values = []
    for code, value in [
        ("1000", "Asset"),
        ("2000", "Liability"),
        ("3000", "Equity"),
        ("4000", "Revenue"),
        ("5000", "Expense"),
    ]:
        values.append(DimensionValue(
            dimension_id="account_type", value=value, value_code=code,
            parent_id=None, depth=0, path=value,
        ))
    # Sub-types under Expense
    for code, value in [
        ("5100", "COGS"), ("5200", "Sales & Marketing"),
        ("5300", "R&D"), ("5400", "G&A"), ("5500", "D&A"),
    ]:
        values.append(DimensionValue(
            dimension_id="account_type", value=value, value_code=code,
            parent_id="Expense", depth=1, path=f"Expense/{value}",
        ))
    return values


def _build_project_values() -> List[DimensionValue]:
    """Build flat project dimension (populated from Farm data later)."""
    # Placeholder — Farm will populate real project IDs
    return []


def populate_initial_hierarchies() -> Dict[str, int]:
    """Populate all 6 required dimensions. Returns counts per dimension.

    Idempotent — uses ON CONFLICT DO UPDATE.
    """
    store = DimensionHierarchyStore()
    counts = {}

    builders = {
        "geo": _build_geo_hierarchy,
        "segment": _build_segment_hierarchy,
        "cost_center": _build_cost_center_hierarchy,
        "period": _build_period_hierarchy,
        "account_type": _build_account_type_hierarchy,
        "project": _build_project_values,
    }

    for dim_id, builder in builders.items():
        values = builder()
        if values:
            count = store.insert_batch(values)
            counts[dim_id] = count
            logger.info(f"[dimension_hierarchy] Populated {dim_id}: {count} values")
        else:
            counts[dim_id] = 0
            logger.info(f"[dimension_hierarchy] {dim_id}: no initial values (populated later)")

    return counts


# ═══════════════════════════════════════════════════════════════════════════════
# Drill-through relationship management
# ═══════════════════════════════════════════════════════════════════════════════

class DrillThroughStore:
    """Manages revenue drill-through relationships: Region → Rep → Customer → Project."""

    def insert_rep(self, rep_id: str, rep_name: str, region: str,
                   entity_id: Optional[str] = None) -> bool:
        """Insert or update a rep assignment."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO rep_assignments (rep_id, rep_name, region, entity_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (rep_id) DO UPDATE SET
                            rep_name = EXCLUDED.rep_name,
                            region = EXCLUDED.region,
                            entity_id = EXCLUDED.entity_id
                        """,
                        (rep_id, rep_name, region, entity_id),
                    )
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                raise

    def insert_customer(self, customer_id: str, customer_name: str,
                        rep_id: str, entity_id: Optional[str] = None) -> bool:
        """Insert or update a customer-rep mapping."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO customer_rep_map (customer_id, customer_name, rep_id, entity_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (customer_id) DO UPDATE SET
                            customer_name = EXCLUDED.customer_name,
                            rep_id = EXCLUDED.rep_id,
                            entity_id = EXCLUDED.entity_id
                        """,
                        (customer_id, customer_name, rep_id, entity_id),
                    )
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                raise

    def insert_project(self, project_id: str, project_name: str,
                       customer_id: str, entity_id: Optional[str] = None) -> bool:
        """Insert or update a project-customer mapping."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO project_customer_map (project_id, project_name, customer_id, entity_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (project_id) DO UPDATE SET
                            project_name = EXCLUDED.project_name,
                            customer_id = EXCLUDED.customer_id,
                            entity_id = EXCLUDED.entity_id
                        """,
                        (project_id, project_name, customer_id, entity_id),
                    )
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                raise

    def get_reps_by_region(self, region: str) -> List[Dict[str, Any]]:
        """Get all reps assigned to a region."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT rep_id, rep_name, region FROM rep_assignments WHERE region = %s ORDER BY rep_name",
                    (region,),
                )
                return [{"rep_id": r[0], "rep_name": r[1], "region": r[2]} for r in cur.fetchall()]

    def get_customers_by_rep(self, rep_id: str) -> List[Dict[str, Any]]:
        """Get all customers assigned to a rep."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT customer_id, customer_name, rep_id FROM customer_rep_map WHERE rep_id = %s ORDER BY customer_name",
                    (rep_id,),
                )
                return [{"customer_id": r[0], "customer_name": r[1], "rep_id": r[2]} for r in cur.fetchall()]

    def get_projects_by_customer(self, customer_id: str) -> List[Dict[str, Any]]:
        """Get all projects for a customer."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT project_id, project_name, customer_id FROM project_customer_map WHERE customer_id = %s ORDER BY project_name",
                    (customer_id,),
                )
                return [{"project_id": r[0], "project_name": r[1], "customer_id": r[2]} for r in cur.fetchall()]

    def get_all_regions(self) -> List[str]:
        """Get distinct regions with reps."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT region FROM rep_assignments ORDER BY region")
                return [r[0] for r in cur.fetchall()]

    def check_integrity(self) -> Dict[str, int]:
        """Check referential integrity of the drill-through chain.

        Returns counts of orphans at each level (0 = clean).
        """
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError("Database unavailable")
            with conn.cursor() as cur:
                # Orphan reps: reps with no region in dimension_values
                cur.execute("""
                    SELECT COUNT(*) FROM rep_assignments r
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dimension_values d
                        WHERE d.dimension_id = 'geo' AND d.value = r.region
                    )
                """)
                orphan_reps = cur.fetchone()[0]

                # Orphan customers: customers with no valid rep
                cur.execute("""
                    SELECT COUNT(*) FROM customer_rep_map c
                    WHERE NOT EXISTS (
                        SELECT 1 FROM rep_assignments r WHERE r.rep_id = c.rep_id
                    )
                """)
                orphan_customers = cur.fetchone()[0]

                # Orphan projects: projects with no valid customer
                cur.execute("""
                    SELECT COUNT(*) FROM project_customer_map p
                    WHERE NOT EXISTS (
                        SELECT 1 FROM customer_rep_map c WHERE c.customer_id = p.customer_id
                    )
                """)
                orphan_projects = cur.fetchone()[0]

                return {
                    "orphan_reps": orphan_reps,
                    "orphan_customers": orphan_customers,
                    "orphan_projects": orphan_projects,
                }


# ═══════════════════════════════════════════════════════════════════════════════
# Module-level singletons
# ═══════════════════════════════════════════════════════════════════════════════

_hierarchy_store: Optional[DimensionHierarchyStore] = None
_drill_through_store: Optional[DrillThroughStore] = None


def get_hierarchy_store() -> DimensionHierarchyStore:
    """Get or create the singleton hierarchy store."""
    global _hierarchy_store
    if _hierarchy_store is None:
        _hierarchy_store = DimensionHierarchyStore()
    return _hierarchy_store


def get_drill_through_store() -> DrillThroughStore:
    """Get or create the singleton drill-through store."""
    global _drill_through_store
    if _drill_through_store is None:
        _drill_through_store = DrillThroughStore()
    return _drill_through_store
