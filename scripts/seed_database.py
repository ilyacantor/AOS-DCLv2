"""
Seed DCL database from Farm-generated JSONL triples.

Reads a Farm JSONL triple file and inserts directly into DCL's
semantic_triples table using psycopg2 execute_values for performance.

Usage:
    python scripts/seed_database.py <jsonl_path> [--database-url ...]
"""

import argparse
import json
import os
import sys
import uuid
from collections import Counter
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


COLUMNS = [
    "tenant_id", "entity_id", "concept", "property", "value",
    "period", "currency", "unit",
    "source_system", "source_table", "source_field",
    "pipe_id", "run_id",
    "confidence_score", "confidence_tier",
    "canonical_id", "resolution_method", "resolution_confidence",
]


def load_triples(jsonl_path: str) -> list[dict]:
    """Read all triples from a Farm JSONL file."""
    path = Path(jsonl_path)
    if not path.exists():
        print(f"ERROR: JSONL file not found at {path}", file=sys.stderr)
        sys.exit(1)

    triples = []
    with open(path) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                triples.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"ERROR: Invalid JSON on line {i}: {e}", file=sys.stderr)
                sys.exit(1)

    if not triples:
        print("ERROR: JSONL file is empty — no triples to ingest.", file=sys.stderr)
        sys.exit(1)

    return triples


def validate_concept(concept: str, valid_roots: set[str]) -> bool:
    """Prefix-based validation matching DCL's ConceptRegistry."""
    if not concept:
        return False
    root = concept.split(".")[0]
    return root in valid_roots


def get_valid_concept_roots(conn) -> set[str]:
    """Load concept roots from the same ontology YAML that DCL uses."""
    import yaml
    yaml_path = Path(__file__).resolve().parent.parent / "config" / "ontology_concepts.yaml"
    if not yaml_path.exists():
        print(f"ERROR: Ontology file not found at {yaml_path}", file=sys.stderr)
        sys.exit(1)
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    return {c["id"] for c in data.get("concepts", []) if c.get("id")}


def main():
    parser = argparse.ArgumentParser(description="Seed DCL database from Farm JSONL triples")
    parser.add_argument("jsonl_path", help="Path to Farm-generated JSONL triple file")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL connection string (or set DATABASE_URL env var)",
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per INSERT batch")
    parser.add_argument("--replace", action="store_true", help="Replace existing run if present")
    args = parser.parse_args()

    # Load .env if DATABASE_URL not set
    if not args.database_url:
        env_path = Path(__file__).resolve().parent.parent / ".env"
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("DATABASE_URL="):
                        args.database_url = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    if not args.database_url:
        print("ERROR: No DATABASE_URL provided or found in .env", file=sys.stderr)
        sys.exit(1)

    print(f"Loading triples from {args.jsonl_path}...")
    triples = load_triples(args.jsonl_path)
    print(f"Loaded {len(triples)} triples.")

    # Extract Farm's run_id and tenant_id from the first triple
    farm_run_id = triples[0].get("run_id", "unknown")
    farm_tenant_id = triples[0].get("tenant_id", "unknown")

    # Generate deterministic UUIDs from Farm's identifiers so re-runs produce the same IDs
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"farm-seed:{farm_run_id}"))
    tenant_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"farm-tenant:{farm_tenant_id}"))

    print(f"Farm run_id: {farm_run_id} → DCL run_id: {run_id}")
    print(f"Farm tenant_id: {farm_tenant_id} → DCL tenant_id: {tenant_id}")

    # Connect to PG
    print(f"Connecting to database...")
    conn = psycopg2.connect(args.database_url)
    conn.autocommit = False

    try:
        # Validate concepts against ontology
        valid_roots = get_valid_concept_roots(conn)
        print(f"Loaded {len(valid_roots)} concept roots from ontology.")

        invalid = []
        for i, t in enumerate(triples):
            if not validate_concept(t["concept"], valid_roots):
                invalid.append((i, t["concept"]))
        if invalid:
            print(f"ERROR: {len(invalid)} triples have invalid concepts:", file=sys.stderr)
            for idx, concept in invalid[:10]:
                print(f"  Triple #{idx}: concept '{concept}' — root '{concept.split('.')[0]}' not in ontology", file=sys.stderr)
            if len(invalid) > 10:
                print(f"  ... and {len(invalid) - 10} more", file=sys.stderr)
            sys.exit(1)

        # Check for existing run
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM semantic_triples WHERE run_id = %s", (run_id,))
            existing = cur.fetchone()[0]

        if existing > 0 and not args.replace:
            print(
                f"ERROR: run_id {run_id} already has {existing} triples. "
                "Use --replace to deactivate and re-seed.",
                file=sys.stderr,
            )
            sys.exit(1)

        if existing > 0 and args.replace:
            print(f"Deactivating {existing} existing triples for run_id {run_id}...")
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM semantic_triples WHERE run_id = %s",
                    (run_id,),
                )
            conn.commit()

        # Prepare rows
        rows = []
        concept_counter = Counter()
        for t in triples:
            val = json.dumps(t["value"])
            root = t["concept"].split(".")[0]
            concept_counter[root] += 1
            rows.append((
                tenant_id,
                t["entity_id"],
                t["concept"],
                t["property"],
                val,
                t.get("period"),
                t.get("currency", "USD"),
                t.get("unit"),
                t["source_system"],
                t.get("source_table"),
                t.get("source_field"),
                t.get("pipe_id"),
                run_id,
                t["confidence_score"],
                t["confidence_tier"],
                t.get("canonical_id"),
                t.get("resolution_method"),
                t.get("resolution_confidence"),
            ))

        # Batch insert using execute_values for performance
        col_names = ", ".join(COLUMNS)
        sql = f"INSERT INTO semantic_triples ({col_names}) VALUES %s"

        total_inserted = 0
        total_batches = (len(rows) + args.batch_size - 1) // args.batch_size

        with conn.cursor() as cur:
            for batch_num in range(total_batches):
                start = batch_num * args.batch_size
                end = start + args.batch_size
                batch = rows[start:end]
                execute_values(cur, sql, batch, page_size=args.batch_size)
                total_inserted += len(batch)
                print(f"  Batch {batch_num + 1}/{total_batches}: {len(batch)} rows — cumulative: {total_inserted}")

        conn.commit()

        # Verify
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM semantic_triples WHERE run_id = %s", (run_id,))
            verified = cur.fetchone()[0]

        if verified != total_inserted:
            print(
                f"ERROR: Verification failed — inserted {total_inserted} but found {verified} active triples.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Print summary
        concept_summary = dict(sorted(concept_counter.items()))
        print(f"\n{'='*60}")
        print(f"SEED COMPLETE")
        print(f"{'='*60}")
        print(f"  run_id:     {run_id}")
        print(f"  tenant_id:  {tenant_id}")
        print(f"  total:      {verified} triples (verified)")
        print(f"  concepts:")
        for concept, count in concept_summary.items():
            print(f"    {concept}: {count}")

        # Write metadata for downstream use
        meta = {
            "farm_run_id": farm_run_id,
            "dcl_run_id": run_id,
            "dcl_tenant_id": tenant_id,
            "total_triples": verified,
            "concept_summary": concept_summary,
        }
        meta_path = Path(args.jsonl_path).parent / f"{farm_run_id}_seed_meta.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        print(f"\n  Metadata written to: {meta_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
