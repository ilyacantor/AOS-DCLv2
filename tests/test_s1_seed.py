"""
S1-SEED verification harness — 16 tests validating the full Meridian + Cascadia
triple dataset in DCL's PG database.

All tests query PG directly (not via the API). Financial identities are
mathematical invariants, not hardcoded values.
"""

import os
import random
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import psycopg2
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _load_db_url() -> str:
    """Load DATABASE_URL from .env file."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        raise RuntimeError(f"No .env file at {env_path}")
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise RuntimeError("DATABASE_URL not found in .env")


@pytest.fixture(scope="module")
def conn():
    """Shared PG connection for the test module."""
    c = psycopg2.connect(_load_db_url())
    yield c
    c.close()


@pytest.fixture(scope="module")
def seed_run_id():
    """The run_id used by the seed script (deterministic UUID5)."""
    import uuid
    # Must match the seed_database.py logic
    farm_run_id = _discover_farm_run_id()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"farm-seed:{farm_run_id}"))


def _discover_farm_run_id() -> str:
    """Find the Farm run_id from the seed metadata or PG."""
    # Check for seed metadata file in Farm output
    farm_output = Path("/home/ilyac/code/farm/output/triples")
    if farm_output.exists():
        for meta_file in sorted(farm_output.glob("*_seed_meta.json"), reverse=True):
            import json
            with open(meta_file) as f:
                meta = json.load(f)
            return meta["farm_run_id"]
    raise RuntimeError(
        "Cannot discover Farm run_id — no seed metadata found in "
        "/home/ilyac/code/farm/output/triples/"
    )


@pytest.fixture(scope="module")
def seed_tenant_id():
    """The tenant_id used by the seed script."""
    import uuid
    farm_tenant_id = "dev-00000000-0000-0000-0000-000000000000"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"farm-tenant:{farm_tenant_id}"))


QUARTERS = [
    "2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4",
    "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4",
    "2026-Q1", "2026-Q2", "2026-Q3", "2026-Q4",
]


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _fetch_value(cur, run_id: str, entity_id: str, concept: str, prop: str, period: str) -> float | None:
    """Fetch a single triple value as float."""
    cur.execute(
        """
        SELECT value FROM semantic_triples
        WHERE run_id = %s AND entity_id = %s AND concept = %s
          AND property = %s AND period = %s AND is_active = true
        """,
        (run_id, entity_id, concept, prop, period),
    )
    row = cur.fetchone()
    if row is None:
        return None
    v = row[0]
    if isinstance(v, (int, float, Decimal)):
        return float(v)
    # JSONB stores as native type; psycopg2 may return float or Decimal
    return float(v)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSeedData:
    """16-test harness for S1-SEED verification."""

    # 1. Triples exist
    def test_01_triples_exist(self, conn, seed_run_id):
        """Total triple count > 0."""
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM semantic_triples WHERE run_id = %s AND is_active = true",
            (seed_run_id,),
        )
        count = cur.fetchone()[0]
        assert count > 0, f"Expected triples for run_id={seed_run_id}, found 0"
        print(f"  Total triples: {count}")

    # 2. Both entities present
    def test_02_both_entities_present(self, conn, seed_run_id):
        """Triples exist for both meridian and cascadia."""
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT entity_id FROM semantic_triples WHERE run_id = %s AND is_active = true",
            (seed_run_id,),
        )
        entities = {r[0] for r in cur.fetchall()}
        assert any("meridian" in e.lower() for e in entities), (
            f"No entity_id containing 'meridian'. Found: {entities}"
        )
        assert any("cascadia" in e.lower() for e in entities), (
            f"No entity_id containing 'cascadia'. Found: {entities}"
        )

    # 3. Revenue scale — Meridian (~$5B annual, within 10%)
    def test_03_revenue_scale_meridian(self, conn, seed_run_id):
        """Sum revenue.total for meridian for any 4 consecutive quarters ~ $5B."""
        cur = conn.cursor()
        quarterly = []
        for q in QUARTERS:
            v = _fetch_value(cur, seed_run_id, "meridian", "revenue.total", "amount", q)
            assert v is not None, f"Missing revenue.total for meridian {q}"
            quarterly.append(v)

        # Check that at least one window of 4 consecutive quarters is within 10% of $5B
        target = 5000.0  # $5B in dollars_millions
        tolerance = target * 0.10
        windows = [sum(quarterly[i:i + 4]) for i in range(len(quarterly) - 3)]
        matching = [w for w in windows if abs(w - target) <= tolerance]
        assert matching, (
            f"No 4-quarter window within 10% of ${target:.0f}. "
            f"Windows: {[f'{w:.1f}' for w in windows]}"
        )
        print(f"  Meridian closest annual revenue: {min(matching, key=lambda w: abs(w - target)):.1f}")

    # 4. Revenue scale — Cascadia (~$1B annual, within 10%)
    def test_04_revenue_scale_cascadia(self, conn, seed_run_id):
        """Sum revenue.total for cascadia for any 4 consecutive quarters ~ $1B."""
        cur = conn.cursor()
        quarterly = []
        for q in QUARTERS:
            v = _fetch_value(cur, seed_run_id, "cascadia", "revenue.total", "amount", q)
            assert v is not None, f"Missing revenue.total for cascadia {q}"
            quarterly.append(v)

        target = 1000.0
        tolerance = target * 0.10
        windows = [sum(quarterly[i:i + 4]) for i in range(len(quarterly) - 3)]
        matching = [w for w in windows if abs(w - target) <= tolerance]
        assert matching, (
            f"No 4-quarter window within 10% of ${target:.0f}. "
            f"Windows: {[f'{w:.1f}' for w in windows]}"
        )
        print(f"  Cascadia closest annual revenue: {min(matching, key=lambda w: abs(w - target)):.1f}")

    # 5. P&L identity: revenue.total - cogs.total - opex.total == pnl.ebitda
    def test_05_pl_identity(self, conn, seed_run_id):
        """For each entity and quarter: revenue - COGS - opex == EBITDA."""
        cur = conn.cursor()
        failures = []
        for entity in ("meridian", "cascadia"):
            for q in QUARTERS:
                rev = _fetch_value(cur, seed_run_id, entity, "revenue.total", "amount", q)
                cogs = _fetch_value(cur, seed_run_id, entity, "cogs.total", "amount", q)
                opex = _fetch_value(cur, seed_run_id, entity, "opex.total", "amount", q)
                ebitda = _fetch_value(cur, seed_run_id, entity, "pnl.ebitda", "amount", q)

                if any(v is None for v in (rev, cogs, opex, ebitda)):
                    failures.append(f"{entity} {q}: missing value(s) — rev={rev}, cogs={cogs}, opex={opex}, ebitda={ebitda}")
                    continue

                computed = rev - cogs - opex
                if abs(computed - ebitda) > 0.01:
                    failures.append(
                        f"{entity} {q}: rev({rev}) - cogs({cogs}) - opex({opex}) = {computed} != ebitda({ebitda})"
                    )

        assert not failures, "P&L identity failures:\n" + "\n".join(failures)

    # 6. BS identity: asset.total == liability.total + equity.total (tolerance $0)
    def test_06_bs_identity(self, conn, seed_run_id):
        """For each entity and quarter: assets == liabilities + equity."""
        cur = conn.cursor()
        failures = []
        for entity in ("meridian", "cascadia"):
            for q in QUARTERS:
                assets = _fetch_value(cur, seed_run_id, entity, "asset.total", "amount", q)
                liab = _fetch_value(cur, seed_run_id, entity, "liability.total", "amount", q)
                equity = _fetch_value(cur, seed_run_id, entity, "equity.total", "amount", q)

                if any(v is None for v in (assets, liab, equity)):
                    failures.append(f"{entity} {q}: missing value(s) — assets={assets}, liab={liab}, equity={equity}")
                    continue

                rhs = liab + equity
                if abs(assets - rhs) > 0.01:
                    failures.append(
                        f"{entity} {q}: assets({assets}) != liab({liab}) + equity({equity}) = {rhs}"
                    )

        assert not failures, "BS identity failures:\n" + "\n".join(failures)

    # 7. CF identity: operating + investing + financing == net_change (tolerance $0)
    def test_07_cf_identity(self, conn, seed_run_id):
        """For each entity and quarter: CF operating + investing + financing == net_change."""
        cur = conn.cursor()
        failures = []
        for entity in ("meridian", "cascadia"):
            for q in QUARTERS:
                op = _fetch_value(cur, seed_run_id, entity, "cash_flow.operating.total", "amount", q)
                inv = _fetch_value(cur, seed_run_id, entity, "cash_flow.investing.total", "amount", q)
                fin = _fetch_value(cur, seed_run_id, entity, "cash_flow.financing.total", "amount", q)
                net = _fetch_value(cur, seed_run_id, entity, "cash_flow.net_change", "amount", q)

                if any(v is None for v in (op, inv, fin, net)):
                    failures.append(f"{entity} {q}: missing value(s) — op={op}, inv={inv}, fin={fin}, net={net}")
                    continue

                computed = op + inv + fin
                if abs(computed - net) > 0.01:
                    failures.append(
                        f"{entity} {q}: op({op}) + inv({inv}) + fin({fin}) = {computed} != net({net})"
                    )

        assert not failures, "CF identity failures:\n" + "\n".join(failures)

    # 8. Cash continuity: cash[Q(n)] + net_change[Q(n+1)] == cash[Q(n+1)]
    def test_08_cash_continuity(self, conn, seed_run_id):
        """For each entity: cash in Q(n) + net_change in Q(n+1) == cash in Q(n+1)."""
        cur = conn.cursor()
        failures = []
        for entity in ("meridian", "cascadia"):
            cash_vals = {}
            net_vals = {}
            for q in QUARTERS:
                cash_vals[q] = _fetch_value(cur, seed_run_id, entity, "asset.current.cash", "amount", q)
                net_vals[q] = _fetch_value(cur, seed_run_id, entity, "cash_flow.net_change", "amount", q)

            for i in range(len(QUARTERS) - 1):
                q_n = QUARTERS[i]
                q_n1 = QUARTERS[i + 1]
                cash_n = cash_vals[q_n]
                cash_n1 = cash_vals[q_n1]
                net_n1 = net_vals[q_n1]

                if any(v is None for v in (cash_n, cash_n1, net_n1)):
                    failures.append(f"{entity} {q_n}→{q_n1}: missing value(s)")
                    continue

                expected = cash_n + net_n1
                if abs(expected - cash_n1) > 0.01:
                    failures.append(
                        f"{entity} {q_n}→{q_n1}: cash({cash_n}) + net_change({net_n1}) = {expected} != cash({cash_n1})"
                    )

        assert not failures, "Cash continuity failures:\n" + "\n".join(failures)

    # 9. COFA adjustments: 6 COFA conflict triples, each with conflict_id and adjustment_amount
    def test_09_cofa_adjustments(self, conn, seed_run_id):
        """All 6 COFA conflict concepts present with conflict_id and adjustment_amount."""
        cur = conn.cursor()

        # Get distinct COFA concepts
        cur.execute(
            "SELECT DISTINCT concept FROM semantic_triples WHERE run_id = %s AND concept LIKE 'cofa.%%' AND is_active = true",
            (seed_run_id,),
        )
        cofa_concepts = {r[0] for r in cur.fetchall()}
        assert len(cofa_concepts) >= 6, (
            f"Expected at least 6 COFA concepts, found {len(cofa_concepts)}: {cofa_concepts}"
        )

        # Check each has conflict_id and adjustment_amount properties
        for concept in cofa_concepts:
            cur.execute(
                "SELECT DISTINCT property FROM semantic_triples WHERE run_id = %s AND concept = %s AND is_active = true",
                (seed_run_id, concept),
            )
            props = {r[0] for r in cur.fetchall()}
            assert "conflict_id" in props, (
                f"COFA concept '{concept}' missing 'conflict_id' property. Has: {props}"
            )
            assert "adjustment_amount" in props, (
                f"COFA concept '{concept}' missing 'adjustment_amount' property. Has: {props}"
            )

    # 10. Customer overlap
    def test_10_customer_overlap(self, conn, seed_run_id):
        """At least one customer concept has triples under both entity_ids."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT concept FROM semantic_triples
            WHERE run_id = %s AND concept LIKE 'customer.%%' AND is_active = true
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            LIMIT 1
            """,
            (seed_run_id,),
        )
        row = cur.fetchone()
        assert row is not None, "No customer concept found with triples under both entity_ids"
        print(f"  Customer overlap example: {row[0]}")

    # 11. Vendor overlap
    def test_11_vendor_overlap(self, conn, seed_run_id):
        """At least one vendor concept has triples under both entity_ids."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT concept FROM semantic_triples
            WHERE run_id = %s AND concept LIKE 'vendor.%%' AND is_active = true
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            LIMIT 1
            """,
            (seed_run_id,),
        )
        row = cur.fetchone()
        assert row is not None, "No vendor concept found with triples under both entity_ids"
        print(f"  Vendor overlap example: {row[0]}")

    # 12. People overlap
    def test_12_people_overlap(self, conn, seed_run_id):
        """At least one employee concept has triples under both entity_ids."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT concept FROM semantic_triples
            WHERE run_id = %s AND concept LIKE 'employee.%%' AND is_active = true
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            LIMIT 1
            """,
            (seed_run_id,),
        )
        row = cur.fetchone()
        assert row is not None, "No employee concept found with triples under both entity_ids"
        print(f"  Employee overlap example: {row[0]}")

    # 13. Provenance complete
    def test_13_provenance_complete(self, conn, seed_run_id):
        """Random sample of 50 triples — all have non-null provenance fields."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT source_system, confidence_score, confidence_tier, run_id
            FROM semantic_triples
            WHERE run_id = %s AND is_active = true
            ORDER BY random()
            LIMIT 50
            """,
            (seed_run_id,),
        )
        rows = cur.fetchall()
        assert len(rows) == 50, f"Expected 50 sample triples, got {len(rows)}"

        valid_tiers = {"exact", "high", "medium", "low"}
        failures = []
        for i, (source, conf, tier, rid) in enumerate(rows):
            if source is None or not source.strip():
                failures.append(f"Sample #{i}: source_system is null/empty")
            if conf is None or not (0.0 <= float(conf) <= 1.0):
                failures.append(f"Sample #{i}: confidence_score={conf} not in [0,1]")
            if tier not in valid_tiers:
                failures.append(f"Sample #{i}: confidence_tier='{tier}' not in {valid_tiers}")
            if str(rid) != seed_run_id:
                failures.append(f"Sample #{i}: run_id={rid} != expected {seed_run_id}")

        assert not failures, "Provenance failures:\n" + "\n".join(failures)

    # 14. All triples active
    def test_14_all_triples_active(self, conn, seed_run_id):
        """Zero triples with is_active=false for the seed run_id."""
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM semantic_triples WHERE run_id = %s AND is_active = false",
            (seed_run_id,),
        )
        inactive = cur.fetchone()[0]
        assert inactive == 0, f"Found {inactive} inactive triples for seed run_id"

    # 15. Period coverage: both entities have triples for all 12 quarters
    def test_15_period_coverage(self, conn, seed_run_id):
        """Both entities have triples for all 12 quarters (2024-Q1 through 2026-Q4)."""
        cur = conn.cursor()
        expected_quarters = set(QUARTERS)
        failures = []

        for entity in ("meridian", "cascadia"):
            cur.execute(
                """
                SELECT DISTINCT period FROM semantic_triples
                WHERE run_id = %s AND entity_id = %s AND period IS NOT NULL AND is_active = true
                """,
                (seed_run_id, entity),
            )
            actual = {r[0] for r in cur.fetchall()}
            missing = expected_quarters - actual
            if missing:
                failures.append(f"{entity}: missing quarters {sorted(missing)}")

        assert not failures, "Period coverage failures:\n" + "\n".join(failures)

    # 16. Single run_id
    def test_16_single_run_id(self, conn, seed_run_id):
        """All seed triples share the same run_id."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT run_id FROM semantic_triples
            WHERE run_id = %s AND is_active = true
            """,
            (seed_run_id,),
        )
        run_ids = [str(r[0]) for r in cur.fetchall()]
        assert len(run_ids) == 1, f"Expected 1 run_id, found {len(run_ids)}: {run_ids}"
        assert run_ids[0] == seed_run_id, f"run_id mismatch: {run_ids[0]} != {seed_run_id}"
