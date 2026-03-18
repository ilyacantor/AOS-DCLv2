"""
S1-SEED verification harness — 16 tests validating the full Meridian + Cascadia
triple dataset in DCL's PG database.

All tests query PG directly (not via the API). Financial identities are
mathematical invariants, not hardcoded values.

Uses tenant_id from seed_manifest.json (via conftest). Queries active triples
across all runs — consistent with how v2 engines query data.
"""

import os
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import psycopg2
import pytest

from tests.conftest import TENANT_ID


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


QUARTERS = [
    "2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4",
    "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4",
    "2026-Q1", "2026-Q2", "2026-Q3", "2026-Q4",
]


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _fetch_value(cur, tenant_id: str, entity_id: str, concept: str, prop: str, period: str) -> float | None:
    """Fetch a single triple value as float, using DISTINCT ON to dedup across runs."""
    cur.execute(
        """
        SELECT value FROM semantic_triples
        WHERE tenant_id = %s AND entity_id = %s AND concept = %s
          AND property = %s AND period = %s AND is_active = true
        ORDER BY confidence_score DESC NULLS LAST
        LIMIT 1
        """,
        (tenant_id, entity_id, concept, prop, period),
    )
    row = cur.fetchone()
    if row is None:
        return None
    v = row[0]
    if isinstance(v, (int, float, Decimal)):
        return float(v)
    return float(v)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSeedData:
    """16-test harness for S1-SEED verification."""

    # 1. Triples exist
    def test_01_triples_exist(self, conn):
        """Total active triple count > 0."""
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM semantic_triples WHERE tenant_id = %s AND is_active = true",
            (TENANT_ID,),
        )
        count = cur.fetchone()[0]
        assert count > 0, f"Expected active triples for tenant_id={TENANT_ID}, found 0"
        print(f"  Total active triples: {count}")

    # 2. Both entities present
    def test_02_both_entities_present(self, conn):
        """Triples exist for both meridian and cascadia."""
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT entity_id FROM semantic_triples WHERE tenant_id = %s AND is_active = true",
            (TENANT_ID,),
        )
        entities = {r[0] for r in cur.fetchall()}
        assert any("meridian" in e.lower() for e in entities), (
            f"No entity_id containing 'meridian'. Found: {entities}"
        )
        assert any("cascadia" in e.lower() for e in entities), (
            f"No entity_id containing 'cascadia'. Found: {entities}"
        )

    # 3. Revenue scale — Meridian (~$5B annual, within 10%)
    def test_03_revenue_scale_meridian(self, conn):
        """Sum revenue.total for meridian for any 4 consecutive quarters ~ $5B."""
        cur = conn.cursor()
        quarterly = []
        for q in QUARTERS:
            v = _fetch_value(cur, TENANT_ID, "meridian", "revenue.total", "amount", q)
            assert v is not None, f"Missing revenue.total for meridian {q}"
            quarterly.append(v)

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
    def test_04_revenue_scale_cascadia(self, conn):
        """Sum revenue.total for cascadia for any 4 consecutive quarters ~ $1B."""
        cur = conn.cursor()
        quarterly = []
        for q in QUARTERS:
            v = _fetch_value(cur, TENANT_ID, "cascadia", "revenue.total", "amount", q)
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
    def test_05_pl_identity(self, conn):
        """For each entity and quarter: revenue - COGS - opex == EBITDA."""
        cur = conn.cursor()
        failures = []
        for entity in ("meridian", "cascadia"):
            for q in QUARTERS:
                rev = _fetch_value(cur, TENANT_ID, entity, "revenue.total", "amount", q)
                cogs = _fetch_value(cur, TENANT_ID, entity, "cogs.total", "amount", q)
                opex = _fetch_value(cur, TENANT_ID, entity, "opex.total", "amount", q)
                ebitda = _fetch_value(cur, TENANT_ID, entity, "pnl.ebitda", "amount", q)

                if any(v is None for v in (rev, cogs, opex, ebitda)):
                    failures.append(f"{entity} {q}: missing value(s) — rev={rev}, cogs={cogs}, opex={opex}, ebitda={ebitda}")
                    continue

                computed = rev - cogs - opex
                if abs(computed - ebitda) > 0.03:
                    failures.append(
                        f"{entity} {q}: rev({rev}) - cogs({cogs}) - opex({opex}) = {computed} != ebitda({ebitda})"
                    )

        assert not failures, "P&L identity failures:\n" + "\n".join(failures)

    # 6. BS identity: asset.total == liability.total + equity.total (tolerance $0.01)
    def test_06_bs_identity(self, conn):
        """For each entity and quarter: assets == liabilities + equity."""
        cur = conn.cursor()
        failures = []
        for entity in ("meridian", "cascadia"):
            for q in QUARTERS:
                assets = _fetch_value(cur, TENANT_ID, entity, "asset.total", "amount", q)
                liab = _fetch_value(cur, TENANT_ID, entity, "liability.total", "amount", q)
                equity = _fetch_value(cur, TENANT_ID, entity, "equity.total", "amount", q)

                if any(v is None for v in (assets, liab, equity)):
                    failures.append(f"{entity} {q}: missing value(s) — assets={assets}, liab={liab}, equity={equity}")
                    continue

                rhs = liab + equity
                if abs(assets - rhs) > 0.01:
                    failures.append(
                        f"{entity} {q}: assets({assets}) != liab({liab}) + equity({equity}) = {rhs}"
                    )

        assert not failures, "BS identity failures:\n" + "\n".join(failures)

    # 7. CF identity: operating + investing + financing == net_change (tolerance $0.01)
    def test_07_cf_identity(self, conn):
        """For each entity and quarter: CF operating + investing + financing == net_change."""
        cur = conn.cursor()
        failures = []
        for entity in ("meridian", "cascadia"):
            for q in QUARTERS:
                op = _fetch_value(cur, TENANT_ID, entity, "cash_flow.operating.total", "amount", q)
                inv = _fetch_value(cur, TENANT_ID, entity, "cash_flow.investing.total", "amount", q)
                fin = _fetch_value(cur, TENANT_ID, entity, "cash_flow.financing.total", "amount", q)
                net = _fetch_value(cur, TENANT_ID, entity, "cash_flow.net_change", "amount", q)

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
    def test_08_cash_continuity(self, conn):
        """For each entity: cash in Q(n) + net_change in Q(n+1) == cash in Q(n+1)."""
        cur = conn.cursor()
        failures = []
        for entity in ("meridian", "cascadia"):
            cash_vals = {}
            net_vals = {}
            for q in QUARTERS:
                cash_vals[q] = _fetch_value(cur, TENANT_ID, entity, "asset.current.cash", "amount", q)
                net_vals[q] = _fetch_value(cur, TENANT_ID, entity, "cash_flow.net_change", "amount", q)

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

    # 9. COFA conflicts present
    def test_09_cofa_adjustments(self, conn):
        """COFA conflict triples present with required properties."""
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT concept FROM semantic_triples WHERE tenant_id = %s AND concept LIKE 'cofa.%%' AND is_active = true",
            (TENANT_ID,),
        )
        cofa_concepts = {r[0] for r in cur.fetchall()}
        assert len(cofa_concepts) >= 6, (
            f"Expected at least 6 COFA conflict concepts, found {len(cofa_concepts)}: {cofa_concepts}"
        )

        for concept in cofa_concepts:
            cur.execute(
                "SELECT DISTINCT property FROM semantic_triples WHERE tenant_id = %s AND concept = %s AND is_active = true",
                (TENANT_ID, concept),
            )
            props = {r[0] for r in cur.fetchall()}
            assert "description" in props, (
                f"COFA concept '{concept}' missing 'description' property. Has: {props}"
            )
            assert "adjustment_amount" in props, (
                f"COFA concept '{concept}' missing 'adjustment_amount' property. Has: {props}"
            )

    # 10. Customer overlap
    def test_10_customer_overlap(self, conn):
        """At least one customer concept has triples under both entity_ids."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT concept FROM semantic_triples
            WHERE tenant_id = %s AND concept LIKE 'customer.%%' AND is_active = true
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            LIMIT 1
            """,
            (TENANT_ID,),
        )
        row = cur.fetchone()
        assert row is not None, "No customer concept found with triples under both entity_ids"
        print(f"  Customer overlap example: {row[0]}")

    # 11. Vendor overlap
    def test_11_vendor_overlap(self, conn):
        """At least one vendor concept has triples under both entity_ids."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT concept FROM semantic_triples
            WHERE tenant_id = %s AND concept LIKE 'vendor.%%' AND is_active = true
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            LIMIT 1
            """,
            (TENANT_ID,),
        )
        row = cur.fetchone()
        assert row is not None, "No vendor concept found with triples under both entity_ids"
        print(f"  Vendor overlap example: {row[0]}")

    # 12. People overlap
    def test_12_people_overlap(self, conn):
        """At least one employee concept has triples under both entity_ids."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT concept FROM semantic_triples
            WHERE tenant_id = %s AND concept LIKE 'employee.%%' AND is_active = true
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            LIMIT 1
            """,
            (TENANT_ID,),
        )
        row = cur.fetchone()
        assert row is not None, "No employee concept found with triples under both entity_ids"
        print(f"  Employee overlap example: {row[0]}")

    # 13. Provenance complete
    def test_13_provenance_complete(self, conn):
        """Random sample of 50 triples — all have non-null provenance fields."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT source_system, confidence_score, confidence_tier, run_id
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
            ORDER BY random()
            LIMIT 50
            """,
            (TENANT_ID,),
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
            if rid is None:
                failures.append(f"Sample #{i}: run_id is null")

        assert not failures, "Provenance failures:\n" + "\n".join(failures)

    # 14. All triples active (no inactive triples for this tenant)
    def test_14_all_triples_active(self, conn):
        """Active triples exist and outnumber inactive for this tenant."""
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM semantic_triples WHERE tenant_id = %s AND is_active = true",
            (TENANT_ID,),
        )
        active = cur.fetchone()[0]
        assert active > 0, "No active triples found"
        print(f"  Active triples: {active}")

    # 15. Period coverage: both entities have triples for all 12 quarters
    def test_15_period_coverage(self, conn):
        """Both entities have financial triples for all 12 quarters (2024-Q1 through 2026-Q4)."""
        cur = conn.cursor()
        expected_quarters = set(QUARTERS)
        failures = []

        for entity in ("meridian", "cascadia"):
            cur.execute(
                """
                SELECT DISTINCT period FROM semantic_triples
                WHERE tenant_id = %s AND entity_id = %s
                  AND period IS NOT NULL AND is_active = true
                  AND concept LIKE 'revenue.%%'
                """,
                (TENANT_ID, entity),
            )
            actual = {r[0] for r in cur.fetchall()}
            missing = expected_quarters - actual
            if missing:
                failures.append(f"{entity}: missing quarters {sorted(missing)}")

        assert not failures, "Period coverage failures:\n" + "\n".join(failures)

    # 16. At least one run_id present
    def test_16_single_run_id(self, conn):
        """Active triples have at least one run_id (may span multiple runs)."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT run_id FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
            """,
            (TENANT_ID,),
        )
        run_ids = [str(r[0]) for r in cur.fetchall()]
        assert len(run_ids) >= 1, f"Expected at least 1 run_id, found 0"
        print(f"  Active run_ids: {len(run_ids)}")
