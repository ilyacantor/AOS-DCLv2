"""Apply migration 016 — drop is_active column from semantic_triples.

Single transaction: drops 3 predicated indexes, drops the column, rebuilds
the indexes without the predicate, ANALYZEs. Runs in under a minute on the
current dataset (~220k rows).

Invariants verified post-commit:
    - is_active column is gone from semantic_triples
    - idx_triples_active, idx_triples_concept_domain, idx_triples_canonical_entity exist
"""

import os
import sys
import time

import psycopg2
from dotenv import load_dotenv

_repo = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, _repo)
load_dotenv(os.path.join(_repo, ".env"))

_SQL_PATH = os.path.join(_repo, "migrations", "016_drop_is_active.sql")


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone() is not None


def _index_exists(cur, name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM pg_indexes WHERE indexname = %s",
        (name,),
    )
    return cur.fetchone() is not None


def main() -> None:
    url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, application_name="apply_mig016")
    conn.autocommit = False
    start = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '600000'")

        pre_has_col = _column_exists(cur, "semantic_triples", "is_active")
        print(f"[016] pre: semantic_triples.is_active exists={pre_has_col}")

        with open(_SQL_PATH) as fh:
            sql = fh.read()
        cur.execute(sql)

        post_has_col = _column_exists(cur, "semantic_triples", "is_active")
        if post_has_col:
            raise RuntimeError("mig016 failed: is_active column still present")

        for idx in (
            "idx_triples_active",
            "idx_triples_concept_domain",
            "idx_triples_canonical_entity",
        ):
            if not _index_exists(cur, idx):
                raise RuntimeError(f"mig016 failed: index {idx} missing after rebuild")

        conn.commit()
        print(
            f"[016] OK in {time.monotonic() - start:.1f}s — "
            f"is_active dropped, 3 indexes rebuilt without predicate"
        )
    except Exception:
        conn.rollback()
        print(f"[016] ROLLED BACK after {time.monotonic() - start:.1f}s")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
