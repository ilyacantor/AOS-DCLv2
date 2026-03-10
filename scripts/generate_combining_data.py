"""
Generate pre-computed combining statement and entity overlap data from Farm.

Writes:
  data/combining_statements.json   — combining income statement data by quarter
  data/entity_overlap.json         — customer/vendor/people overlap data

Usage:
  python scripts/generate_combining_data.py
  python scripts/generate_combining_data.py --entity-a meridian --entity-b cascadia
"""

import argparse
import json
import sys
from pathlib import Path

# Farm lives in a sibling repo — add it to the path.
FARM_ROOT = "/home/ilyac/code/farm"
sys.path.insert(0, FARM_ROOT)

from src.generators.combining_statements import CombiningStatementEngine
from src.generators.financial_model import FinancialModel, Assumptions
from src.generators.entity_overlap import EntityOverlapGenerator
from src.generators.customer_profiles import CustomerProfileGenerator

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data"


def generate_combining_statements(entity_a: str, entity_b: str) -> None:
    """Generate combining income statement data from Farm's financial models."""
    config_a = f"{FARM_ROOT}/farm_config_{entity_a}.yaml"
    config_b = f"{FARM_ROOT}/farm_config_{entity_b}.yaml"
    print(f"Loading Farm configs: {config_a}, {config_b}")

    a_m = Assumptions.from_yaml(config_a)
    qs_m = FinancialModel(assumptions=a_m).generate()
    a_c = Assumptions.from_yaml(config_b)
    qs_c = FinancialModel(assumptions=a_c).generate()

    engine = CombiningStatementEngine(qs_m, qs_c)
    result = engine.generate()

    data = {}
    for stmt in result.income_statements:
        data[stmt.period] = {
            "period": stmt.period,
            "statement_type": "income_statement",
            "line_items": [
                {
                    "line_item": li.line_item,
                    "meridian": li.meridian,
                    "cascadia": li.cascadia,
                    "adjustments": li.adjustments,
                    "combined": li.combined,
                    "adjustment_details": [
                        {
                            "conflict_id": adj.conflict_id,
                            "description": adj.description,
                            "metric": adj.metric,
                            "meridian_treatment": adj.meridian_treatment,
                            "cascadia_treatment": adj.cascadia_treatment,
                            "adjustment_amount": adj.adjustment_amount,
                            "adjustment_rationale": adj.adjustment_rationale,
                        }
                        for adj in li.adjustment_details
                    ],
                }
                for li in stmt.line_items
            ],
        }

    # Add COFA adjustments from conflict register (unique conflicts only).
    # Include zero-net reclassifications (COFA-002, COFA-003) — they are real
    # accounting treatment differences even though net P&L impact is zero.
    seen: set = set()
    cofa = []
    for c in result.conflict_register:
        if c.conflict_id not in seen:
            seen.add(c.conflict_id)
            cofa.append(
                {
                    "conflict_id": c.conflict_id,
                    "description": c.description,
                    "metric": c.metric,
                    "meridian_treatment": c.meridian_treatment,
                    "cascadia_treatment": c.cascadia_treatment,
                    "adjustment_amount": c.adjustment_amount,
                    "adjustment_rationale": c.adjustment_rationale,
                }
            )
    data["_cofa_adjustments"] = cofa
    data["_periods"] = [s.period for s in result.income_statements]

    out_path = OUTPUT_DIR / "combining_statements.json"
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Written {len(result.income_statements)} quarters of combining data → {out_path}")


def generate_entity_overlap() -> None:
    """Generate entity overlap data from Farm's overlap generator."""
    overlap = EntityOverlapGenerator(seed=42).generate()
    overlap_dict = overlap.to_ground_truth_dict()

    out_path = OUTPUT_DIR / "entity_overlap.json"
    with open(out_path, "w") as f:
        json.dump(overlap_dict, f, indent=2)
    print(f"Written entity overlap data → {out_path}")


def generate_customer_profiles() -> None:
    """Generate enriched customer profiles with behavioral signals from Farm."""
    gen = CustomerProfileGenerator(seed=42)
    profile_dict = gen.to_dict()

    out_path = OUTPUT_DIR / "customer_profiles.json"
    with open(out_path, "w") as f:
        json.dump(profile_dict, f, indent=2)
    print(
        f"Written {profile_dict['summary']['meridian_count']} Meridian + "
        f"{profile_dict['summary']['cascadia_count']} Cascadia customer profiles → {out_path}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate combining data from Farm.")
    parser.add_argument("--entity-a", default="meridian", help="Entity A identifier (default: meridian)")
    parser.add_argument("--entity-b", default="cascadia", help="Entity B identifier (default: cascadia)")
    args = parser.parse_args()

    print(f"Generating combining data for entities: {args.entity_a} (A) and {args.entity_b} (B)")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generate_combining_statements(args.entity_a, args.entity_b)
    generate_entity_overlap()
    generate_customer_profiles()
