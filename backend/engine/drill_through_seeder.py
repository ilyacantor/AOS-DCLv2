"""
Drill-through data seeder — populates rep/customer/project relationship tables.

Generates data that aligns with Farm's ground_truth.py `_generate_rep_level_data()`:
- 36 reps across 3 regions (AMER/EMEA/APAC), split by Farm's region_amer/emea/apac ratios
- ~2-4 customers per rep (deterministic, seeded)
- ~1-2 projects per customer (deterministic, seeded)

The rep names and region assignments must be STABLE across runs (use deterministic
generation, not random). This ensures the NLQ harness tests can assert on specific
rep/customer/project values.
"""

from typing import Any, Dict, List

from backend.engine.dimension_hierarchy import (
    DimensionHierarchyStore,
    DimensionValue,
    DrillThroughStore,
    get_drill_through_store,
    get_hierarchy_store,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Rep data — mirrors Farm's ground_truth.py _build_reps() exactly
# ═══════════════════════════════════════════════════════════════════════════════

_REP_FIRST = [
    "James", "Maria", "David", "Sarah", "Carlos", "Priya", "Ahmed", "Yuki",
    "Michael", "Lisa", "Wei", "Fatima", "Thomas", "Elena", "Raj", "Anna",
    "Robert", "Sofia", "John", "Mei", "Alex", "Nina", "Daniel", "Aisha",
    "Kevin", "Rachel", "Chris", "Laura", "Patrick", "Hannah", "Marcus",
    "Chloe", "Brian", "Zara", "Tyler", "Eva",
]
_REP_LAST = [
    "Smith", "Garcia", "Patel", "Kim", "Johnson", "Chen", "Singh", "Tanaka",
    "Williams", "Lopez", "Brown", "Nakamura", "Davis", "Fernandez", "Gupta",
    "Sato", "Anderson", "Martinez", "Lee", "Wang", "Taylor", "Hernandez",
    "Kumar", "Suzuki", "Wilson", "Gomez", "Shah", "Watanabe", "Moore",
    "Rodriguez", "Ali", "Park", "Clark", "Torres", "Murphy", "Costa",
]

# Region distribution: 18 AMER, 11 EMEA, 7 APAC (matches Farm exactly)
_REP_REGIONS_FARM = (["AMER"] * 18) + (["EMEA"] * 11) + (["APAC"] * 7)

# Map Farm's regions to DCL's geo dimension values.
# Farm uses "AMER"; DCL geo hierarchy uses "NA" for the same region.
_FARM_TO_DCL_REGION = {
    "AMER": "NA",
    "EMEA": "EMEA",
    "APAC": "APAC",
}


def _build_reps() -> List[Dict[str, str]]:
    """Build stable list of 36 sales reps — identical to Farm's _build_reps()."""
    reps = []
    for i in range(36):
        farm_region = _REP_REGIONS_FARM[i]
        reps.append({
            "rep_id": f"REP-{i + 1:03d}",
            "rep_name": f"{_REP_FIRST[i]} {_REP_LAST[i]}",
            "farm_region": farm_region,
            "dcl_region": _FARM_TO_DCL_REGION[farm_region],
        })
    return reps


# ═══════════════════════════════════════════════════════════════════════════════
# Customer generation — deterministic
# ═══════════════════════════════════════════════════════════════════════════════

_ADJECTIVES = [
    "Apex", "Summit", "Pacific", "Atlantic", "Northern", "Global", "Prime",
    "Stellar", "Nova", "Vertex", "Pinnacle", "Atlas", "Horizon", "Zenith",
    "Meridian", "Vanguard", "Crest", "Quantum", "Nexus", "Catalyst",
    "Frontier", "Compass", "Beacon", "Forge", "Keystone", "Bedrock",
    "Ironclad", "Cobalt", "Emerald", "Onyx", "Sierra", "Alpine",
    "Cedar", "Falcon", "Eagle", "Osprey", "Titan", "Polaris", "Astral",
    "Lunar", "Solar", "Arctic", "Coral", "Sapphire", "Amber", "Jade",
    "Obsidian", "Granite", "Marble", "Quartz",
]

_NOUNS = [
    "Systems", "Solutions", "Analytics", "Technologies", "Dynamics",
    "Innovations", "Ventures", "Networks", "Partners", "Industries",
    "Logistics", "Capital", "Enterprises", "Services", "Digital",
    "Group", "Labs", "Consulting", "Platforms", "Strategies",
]

_SUFFIXES = [
    "Inc", "Corp", "LLC", "Ltd", "Group", "Co",
]


def _customer_name(index: int) -> str:
    """Generate a deterministic customer name from its index."""
    adj = _ADJECTIVES[index % len(_ADJECTIVES)]
    noun = _NOUNS[index % len(_NOUNS)]
    suffix = _SUFFIXES[index % len(_SUFFIXES)]
    # Use a second rotation to avoid identical adj+noun pairs for early indices
    adj2_offset = (index * 7 + 3) % len(_ADJECTIVES)
    if adj == _ADJECTIVES[adj2_offset]:
        adj2_offset = (adj2_offset + 1) % len(_ADJECTIVES)
    adj = _ADJECTIVES[adj2_offset]
    return f"{adj} {noun} {suffix}"


def _customers_per_rep(rep_index: int) -> int:
    """Deterministic count of customers per rep: 2, 3, or 4."""
    # Pattern: cycles through 3, 2, 4, 3, 2, 3, 4, 2 ...
    pattern = [3, 2, 4, 3, 2, 3, 4, 2]
    return pattern[rep_index % len(pattern)]


def _build_customers(reps: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Build deterministic customer list, each assigned to a rep."""
    customers = []
    cust_idx = 0
    for rep_i, rep in enumerate(reps):
        n = _customers_per_rep(rep_i)
        for _ in range(n):
            customers.append({
                "customer_id": f"CUST-{cust_idx + 1:03d}",
                "customer_name": _customer_name(cust_idx),
                "rep_id": rep["rep_id"],
            })
            cust_idx += 1
    return customers


# ═══════════════════════════════════════════════════════════════════════════════
# Project generation — deterministic
# ═══════════════════════════════════════════════════════════════════════════════

_PROJECT_PREFIXES = [
    "Migration", "Rollout", "Integration", "Deployment", "Optimization",
    "Modernization", "Expansion", "Consolidation", "Transformation",
    "Implementation", "Assessment", "Overhaul", "Upgrade", "Launch",
    "Pilot", "Phase", "Initiative", "Buildout", "Retrofit", "Redesign",
]

_PROJECT_DOMAINS = [
    "Platform", "Infrastructure", "Cloud", "Analytics", "Security",
    "Data", "Network", "Compliance", "Ops", "Core",
    "Portal", "Pipeline", "Workspace", "Marketplace", "Dashboard",
]


def _project_name(index: int) -> str:
    """Generate a deterministic, unique project name from its index."""
    prefix = _PROJECT_PREFIXES[index % len(_PROJECT_PREFIXES)]
    domain = _PROJECT_DOMAINS[(index // len(_PROJECT_PREFIXES)) % len(_PROJECT_DOMAINS)]
    seq = index // (len(_PROJECT_PREFIXES) * len(_PROJECT_DOMAINS)) + 1
    suffix = f" Ph{seq}" if seq > 1 else ""
    return f"{prefix} — {domain}{suffix}"


def _projects_per_customer(cust_index: int) -> int:
    """Deterministic count of projects per customer: 1 or 2."""
    # ~60% get 1 project, ~40% get 2 projects
    pattern = [1, 2, 1, 1, 2, 1, 2, 1, 1, 2]
    return pattern[cust_index % len(pattern)]


def _build_projects(customers: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Build deterministic project list, each assigned to a customer."""
    projects = []
    proj_idx = 0
    for cust_i, cust in enumerate(customers):
        n = _projects_per_customer(cust_i)
        for _ in range(n):
            projects.append({
                "project_id": f"PROJ-{proj_idx + 1:03d}",
                "project_name": _project_name(proj_idx),
                "customer_id": cust["customer_id"],
            })
            proj_idx += 1
    return projects


# ═══════════════════════════════════════════════════════════════════════════════
# Main seeder function
# ═══════════════════════════════════════════════════════════════════════════════

def seed_drill_through() -> Dict[str, int]:
    """
    Seed drill-through relationship tables with deterministic data
    matching Farm's rep-level ground truth.

    Populates:
      - rep_assignments (36 reps)
      - customer_rep_map (~100 customers)
      - project_customer_map (~150 projects)
      - dimension_values for the 'project' dimension

    Returns a dict of counts: {"reps": N, "customers": N, "projects": N}.
    Idempotent — uses ON CONFLICT DO UPDATE via store methods.

    Raises RuntimeError if the database is unavailable.
    """
    dt_store: DrillThroughStore = get_drill_through_store()
    dim_store: DimensionHierarchyStore = get_hierarchy_store()

    # ── Reps ──────────────────────────────────────────────────────────────
    reps = _build_reps()
    logger.info(
        "[drill_through_seeder] Seeding %d reps (AMER→NA: %d, EMEA: %d, APAC: %d)",
        len(reps),
        sum(1 for r in reps if r["dcl_region"] == "NA"),
        sum(1 for r in reps if r["dcl_region"] == "EMEA"),
        sum(1 for r in reps if r["dcl_region"] == "APAC"),
    )
    for rep in reps:
        dt_store.insert_rep(
            rep_id=rep["rep_id"],
            rep_name=rep["rep_name"],
            region=rep["dcl_region"],
        )

    # ── Customers ─────────────────────────────────────────────────────────
    customers = _build_customers(reps)
    logger.info(
        "[drill_through_seeder] Seeding %d customers across %d reps",
        len(customers), len(reps),
    )
    for cust in customers:
        dt_store.insert_customer(
            customer_id=cust["customer_id"],
            customer_name=cust["customer_name"],
            rep_id=cust["rep_id"],
        )

    # ── Projects ──────────────────────────────────────────────────────────
    projects = _build_projects(customers)
    logger.info(
        "[drill_through_seeder] Seeding %d projects across %d customers",
        len(projects), len(customers),
    )
    for proj in projects:
        dt_store.insert_project(
            project_id=proj["project_id"],
            project_name=proj["project_name"],
            customer_id=proj["customer_id"],
        )

    # ── Populate 'project' dimension in dimension_values ──────────────────
    # Build lookup dicts to avoid O(n^2) scans
    cust_by_id = {c["customer_id"]: c for c in customers}
    rep_by_id = {r["rep_id"]: r for r in reps}

    project_dim_values = []
    for proj in projects:
        cust_id = proj["customer_id"]
        cust_record = cust_by_id[cust_id]
        rep_record = rep_by_id[cust_record["rep_id"]]
        path = f"{rep_record['dcl_region']}/{rep_record['rep_id']}/{cust_id}/{proj['project_id']}"

        project_dim_values.append(DimensionValue(
            dimension_id="project",
            value=proj["project_id"],
            value_code=proj["project_id"],
            parent_id=cust_id,
            depth=0,  # flat dimension — all at depth 0
            path=path,
            metadata={
                "project_name": proj["project_name"],
                "customer_id": cust_id,
                "customer_name": cust_record["customer_name"],
                "rep_id": rep_record["rep_id"],
                "region": rep_record["dcl_region"],
            },
        ))

    inserted = dim_store.insert_batch(project_dim_values)
    logger.info(
        "[drill_through_seeder] Populated 'project' dimension with %d values",
        inserted,
    )

    counts = {
        "reps": len(reps),
        "customers": len(customers),
        "projects": len(projects),
    }
    logger.info("[drill_through_seeder] Seeding complete: %s", counts)
    return counts
