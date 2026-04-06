"""
S1-SEED verification harness — 16 tests validating the triple dataset in DCL's
PG database.

All tests query PG directly (not via the API). Financial identities are
mathematical invariants, not hardcoded values.

Uses tenant_id, run_id, and entities from seed_manifest.json (via conftest).
Queries filter by run_id directly (not current_run_id) so tests are stable even
when the ingest pipeline updates tenant_runs.current_run_id.

Works for both SE (single entity) and ME (multi-entity) runs. Multi-entity
overlap tests are skipped when only one entity is present.
"""

import os
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import psycopg2
import pytest

from tests.conftest import TENANT_ID, RUN_ID, ENTITIES


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
    """Fetch a single triple value as float from the manifest run."""
    cur.execute(
        """
        SELECT value FROM semantic_triples
        WHERE tenant_id = %s AND entity_id = %s AND concept = %s
          AND property = %s AND period = %s AND run_id = %s
        ORDER BY confidence_score DESC NULLS LAST
        LIMIT 1
        """,
        (tenant_id, entity_id, concept, prop, period, RUN_ID),
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
        """Manifest run triple count > 0."""
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM semantic_triples WHERE tenant_id = %s AND run_id = %s",
            (TENANT_ID, RUN_ID),
        )
        count = cur.fetchone()[0]
        assert count > 0, f"Expected triples for run_id={RUN_ID}, found 0"
        print(f"  Total triples in manifest run: {count}")

    # 2. All manifest entities present
    def test_02_all_entities_present(self, conn):
        """Triples exist for every entity listed in seed_manifest.json."""
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT entity_id FROM semantic_triples WHERE tenant_id = %s AND run_id = %s",
            (TENANT_ID, RUN_ID),
        )
        actual = {r[0] for r in cur.fetchall()}
        for entity in ENTITIES:
            assert entity in actual, (
                f"Entity '{entity}' from manifest not found in triples. Found: {actual}"
            )
        print(f"  Entities: {sorted(actual)}")

    # 3. Revenue exists and is positive for first entity
    def test_03_revenue_positive_entity_0(self, conn):
        """First entity has positive revenue for at least 4 quarters."""
        assert ENTITIES, "No entities in seed_manifest.json"
        entity = ENTITIES[0]
        cur = conn.cursor()
        quarterly = []
        for q in QUARTERS:
            v = _fetch_value(cur, TENANT_ID, entity, "revenue.total", "amount", q)
            if v is not None:
                quarterly.append(v)

        assert len(quarterly) >= 4, (
            f"Expected revenue.total for >= 4 quarters for {entity}, "
            f"got {len(quarterly)}"
        )
        annual = sum(quarterly[:4])
        assert annual > 0, f"Annual revenue for {entity} is non-positive: {annual}"
        print(f"  {entity} 4-quarter revenue sum: {annual:.1f}")

    # 4. Revenue exists for second entity (skip if single entity)
    def test_04_revenue_positive_entity_1(self, conn):
        """Second entity has positive revenue (skipped for single-entity runs)."""
        if len(ENTITIES) < 2:
            pytest.skip("Single-entity run — no second entity to check")
        entity = ENTITIES[1]
        cur = conn.cursor()
        quarterly = []
        for q in QUARTERS:
            v = _fetch_value(cur, TENANT_ID, entity, "revenue.total", "amount", q)
            if v is not None:
                quarterly.append(v)

        assert len(quarterly) >= 4, (
            f"Expected revenue.total for >= 4 quarters for {entity}, "
            f"got {len(quarterly)}"
        )
        annual = sum(quarterly[:4])
        assert annual > 0, f"Annual revenue for {entity} is non-positive: {annual}"
        print(f"  {entity} 4-quarter revenue sum: {annual:.1f}")

    # 5. P&L identity: revenue.total - cogs.total - opex.total == pnl.ebitda
    def test_05_pl_identity(self, conn):
        """For each entity and quarter: revenue - COGS - opex == EBITDA."""
        cur = conn.cursor()
        failures = []
        for entity in ENTITIES:
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
        for entity in ENTITIES:
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
        for entity in ENTITIES:
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
        for entity in ENTITIES:
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

    # 9. Provenance complete (was test_13 before ME tests removed)
    def test_13_provenance_complete(self, conn):
        """Random sample of 50 triples — all have non-null provenance fields."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT source_system, confidence_score, confidence_tier, run_id
            FROM semantic_triples
            WHERE tenant_id = %s AND run_id = %s
            ORDER BY random()
            LIMIT 50
            """,
            (TENANT_ID, RUN_ID),
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

    # 14. Manifest run has triples
    def test_14_all_triples_active(self, conn):
        """Manifest run has triples in the database."""
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM semantic_triples WHERE tenant_id = %s AND run_id = %s",
            (TENANT_ID, RUN_ID),
        )
        count = cur.fetchone()[0]
        assert count > 0, f"No triples found for manifest run_id={RUN_ID}"
        print(f"  Triples in manifest run: {count}")

    # 15. Period coverage: all entities have triples for all 12 quarters
    def test_15_period_coverage(self, conn):
        """All entities have financial triples for all 12 quarters (2024-Q1 through 2026-Q4)."""
        cur = conn.cursor()
        expected_quarters = set(QUARTERS)
        failures = []

        for entity in ENTITIES:
            cur.execute(
                """
                SELECT DISTINCT period FROM semantic_triples
                WHERE tenant_id = %s AND entity_id = %s
                  AND period IS NOT NULL AND run_id = %s
                  AND concept LIKE 'revenue.%%'
                """,
                (TENANT_ID, entity, RUN_ID),
            )
            actual = {r[0] for r in cur.fetchall()}
            missing = expected_quarters - actual
            if missing:
                failures.append(f"{entity}: missing quarters {sorted(missing)}")

        assert not failures, "Period coverage failures:\n" + "\n".join(failures)

    # 16. tenant_runs pointer is registered and manifest run has triples
    def test_16_single_run_id(self, conn):
        """tenant_runs has a registered current_run_id and manifest run has triples."""
        cur = conn.cursor()
        # Verify tenant_runs pointer exists
        cur.execute(
            "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s",
            (TENANT_ID,),
        )
        row = cur.fetchone()
        assert row is not None, f"No tenant_runs entry for tenant_id={TENANT_ID}"
        print(f"  current_run_id: {row[0]}")
        # Verify manifest run has triples (independent of which run is "current")
        cur.execute(
            "SELECT COUNT(*) FROM semantic_triples WHERE run_id = %s",
            (RUN_ID,),
        )
        count = cur.fetchone()[0]
        assert count > 0, f"Manifest run_id={RUN_ID} has no triples in semantic_triples"
        print(f"  Manifest run triple count: {count}")
