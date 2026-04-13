"""
Seed DCL database from Farm-generated JSONL triples.

Reads a Farm JSONL triple file and writes it through TripleStore.swap_and_delete
per entity. That path maintains the three-store invariant the store rebuild
established: semantic_triples (write-ahead log), current_triples (flat live
mirror), and tenant_runs (pointer table) all move in lockstep inside a single
transaction. Direct INSERTs against semantic_triples would leave current_triples
and tenant_runs out of sync and break the rebuild architecture.

--replace turns on swap_and_delete(replace=True) so a re-seed of the same
run_id fully replaces the prior rows for each entity.

Usage:
    python scripts/seed_database.py <jsonl_path> [--database-url ...] [--replace]
"""

import argparse
import json
import os
import sys
import uuid
from collections import Counter, defaultdict
from pathlib import Path

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))


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


def get_valid_concept_roots() -> set[str]:
    """Load concept roots from the same ontology YAML that DCL uses."""
    import yaml
    yaml_path = _repo / "config" / "ontology_concepts.yaml"
    if not yaml_path.exists():
        print(f"ERROR: Ontology file not found at {yaml_path}", file=sys.stderr)
        sys.exit(1)
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    return {c["id"] for c in data.get("concepts", []) if c.get("id")}


def _to_triple_dict(t: dict, tenant_id: str, run_id: str) -> dict:
    """Convert a Farm JSONL row into the dict shape TripleStore expects."""
    return {
        "tenant_id": tenant_id,
        "entity_id": t["entity_id"],
        "concept": t["concept"],
        "property": t["property"],
        "value": t["value"],
        "period": t.get("period"),
        "currency": t.get("currency", "USD"),
        "unit": t.get("unit"),
        "source_system": t["source_system"],
        "source_table": t.get("source_table"),
        "source_field": t.get("source_field"),
        "pipe_id": t.get("pipe_id"),
        "run_id": run_id,
        "source_run_tag": t.get("source_run_tag"),
        "confidence_score": t["confidence_score"],
        "confidence_tier": t["confidence_tier"],
        "canonical_id": t.get("canonical_id"),
        "resolution_method": t.get("resolution_method"),
        "resolution_confidence": t.get("resolution_confidence"),
        "fabric_plane": t.get("fabric_plane"),
        "fabric_product": t.get("fabric_product"),
    }


def main():
    parser = argparse.ArgumentParser(description="Seed DCL database from Farm JSONL triples")
    parser.add_argument("jsonl_path", help="Path to Farm-generated JSONL triple file")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL connection string (or set DATABASE_URL env var)",
    )
    parser.add_argument("--replace", action="store_true", help="Replace existing run if present")
    args = parser.parse_args()

    env_path = _repo / ".env"
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)

    if not args.database_url:
        args.database_url = os.environ.get("DATABASE_URL")
    if not args.database_url:
        print("ERROR: No DATABASE_URL provided or found in .env", file=sys.stderr)
        sys.exit(1)
    os.environ.setdefault("DATABASE_URL", args.database_url)

    # Import after sys.path + .env so backend.core.db picks up the correct URL.
    from backend.db.triple_store import TripleStore  # noqa: E402

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

    # Validate concepts against ontology before touching the DB
    valid_roots = get_valid_concept_roots()
    print(f"Loaded {len(valid_roots)} concept roots from ontology.")

    invalid = []
    for i, t in enumerate(triples):
        if not validate_concept(t["concept"], valid_roots):
            invalid.append((i, t["concept"]))
    if invalid:
        print(f"ERROR: {len(invalid)} triples have invalid concepts:", file=sys.stderr)
        for idx, concept in invalid[:10]:
            print(
                f"  Triple #{idx}: concept '{concept}' — root "
                f"'{concept.split('.')[0]}' not in ontology",
                file=sys.stderr,
            )
        if len(invalid) > 10:
            print(f"  ... and {len(invalid) - 10} more", file=sys.stderr)
        sys.exit(1)

    # Group triples by entity so each (tenant, entity) run goes through
    # swap_and_delete as one atomic unit — this keeps current_triples and
    # tenant_runs in lockstep with semantic_triples.
    by_entity: dict[str, list[dict]] = defaultdict(list)
    concept_counter: Counter = Counter()
    for t in triples:
        by_entity[t["entity_id"]].append(_to_triple_dict(t, tenant_id, run_id))
        concept_counter[t["concept"].split(".")[0]] += 1

    print(f"Writing {len(triples)} triples across {len(by_entity)} entities via swap_and_delete...")
    store = TripleStore()
    total_written = 0
    for entity_id, rows in by_entity.items():
        _, _, new_count = store.swap_and_delete(
            tenant_id=tenant_id,
            entity_id=entity_id,
            new_run_id=run_id,
            snapshot_name=f"farm-seed:{farm_run_id}",
            new_rows=rows,
            replace=args.replace,
        )
        total_written += new_count
        print(f"  {entity_id}: {new_count} rows")

    if total_written != len(triples):
        print(
            f"ERROR: Verification failed — expected {len(triples)} rows "
            f"after swap_and_delete but got {total_written}.",
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
    print(f"  entities:   {len(by_entity)}")
    print(f"  total:      {total_written} triples (verified via swap_and_delete)")
    print(f"  concepts:")
    for concept, count in concept_summary.items():
        print(f"    {concept}: {count}")

    meta = {
        "farm_run_id": farm_run_id,
        "dcl_run_id": run_id,
        "dcl_tenant_id": tenant_id,
        "total_triples": total_written,
        "concept_summary": concept_summary,
    }
    meta_path = Path(args.jsonl_path).parent / f"{farm_run_id}_seed_meta.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"\n  Metadata written to: {meta_path}")


if __name__ == "__main__":
    main()
