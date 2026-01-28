#!/usr/bin/env python3
"""
Generate test invoice data for NLQ revenue questions.

Ground truth values per calendar year (FIXED):
- 2024: $47,898,547.96 (1,984 transactions)
- 2025: $48,701,027.04 (1,978 transactions)

Relative terms resolve dynamically:
- "last_year" → previous calendar year (e.g., 2025 if today is 2026)
- "this_year" → current calendar year (e.g., 2026 if today is 2026)

For 2025 breakdown (to support quarter/month queries when 2025 is last_year):
- Q4 2025: $12,372,935.60 (496) - for "last_quarter" when in Q1 2026
- Dec 2025: $3,745,344.33 (167) - for "last_month" when in Jan 2026

For 2026 (to support this_year queries when today is in 2026):
- Q1: $11,942,547.22 (484)
- Q2: $11,862,726.26 (482)
- Q3: $12,372,935.60 (496)
- Q4: $12,522,817.96 (516)
- Total 2026: $48,701,027.04 (1,978) - same as 2025 for simplicity
- Jan 2026: $4,666,373.33 (183) - for "this_month" in Jan
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

CUSTOMERS = [
    ("CUST-001", "Acme Corp"),
    ("CUST-002", "TechCo Industries"),
    ("CUST-003", "GlobalTech Partners"),
    ("CUST-004", "Innovate Solutions"),
    ("CUST-005", "DataStream Inc"),
    ("CUST-006", "CloudFirst Systems"),
    ("CUST-007", "Enterprise 360"),
    ("CUST-008", "NextGen Software"),
    ("CUST-009", "Digital Dynamics"),
    ("CUST-010", "Smart Systems Ltd"),
    ("CUST-011", "Summit Enterprises"),
    ("CUST-012", "Pinnacle Group"),
]


def generate_invoices_for_period(count: int, total: float, start_date: str, end_date: str, seed_offset: int = 0) -> list:
    """Generate invoices that sum to target total for a date range."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    days = (end - start).days + 1

    random.seed(42 + hash(start_date) + seed_offset)
    weights = [random.random() for _ in range(count)]
    total_weight = sum(weights)
    amounts = [round(total * w / total_weight, 2) for w in weights]
    amounts[-1] = round(total - sum(amounts[:-1]), 2)

    invoices = []
    for i, amount in enumerate(amounts):
        date = start + timedelta(days=random.randint(0, days - 1))
        customer = random.choice(CUSTOMERS)
        invoices.append({
            "customer_id": customer[0],
            "customer_name": customer[1],
            "amount": amount,
            "invoice_date": date.strftime("%Y-%m-%d"),
        })

    return invoices


def main():
    random.seed(42)
    all_invoices = []

    # === 2024 data: $47,898,547.96 (1,984 transactions) ===
    all_invoices.extend(generate_invoices_for_period(
        count=1984,
        total=47898547.96,
        start_date="2024-01-01",
        end_date="2024-12-31",
        seed_offset=1000
    ))

    # === 2025 data: $48,701,027.04 (1,978 transactions) ===
    # Structure: Q1-Q3 + Q4 (with Dec split for last_month)
    # Q4 2025 = $12,372,935.60 (496)
    # Dec 2025 = $3,745,344.33 (167)
    # Q1-Q3 2025 = $48,701,027.04 - $12,372,935.60 = $36,328,091.44 (1482)

    all_invoices.extend(generate_invoices_for_period(
        count=1482,
        total=36328091.44,
        start_date="2025-01-01",
        end_date="2025-09-30",
        seed_offset=2000
    ))

    # Q4 2025 Oct-Nov = $12,372,935.60 - $3,745,344.33 = $8,627,591.27 (329)
    all_invoices.extend(generate_invoices_for_period(
        count=329,
        total=8627591.27,
        start_date="2025-10-01",
        end_date="2025-11-30",
        seed_offset=3000
    ))

    # Dec 2025 (last_month when in Jan 2026) = $3,745,344.33 (167)
    all_invoices.extend(generate_invoices_for_period(
        count=167,
        total=3745344.33,
        start_date="2025-12-01",
        end_date="2025-12-31",
        seed_offset=4000
    ))

    # === 2026 data: $48,701,027.04 (1,978 transactions) ===
    # Structure: Q1 (with Jan split) + Q2 + Q3 + Q4
    # Jan 2026 (this_month) = $4,666,373.33 (183)
    # Q1 2026 = $11,942,547.22 (484), so Feb-Mar = $7,276,173.89 (301)

    all_invoices.extend(generate_invoices_for_period(
        count=183,
        total=4666373.33,
        start_date="2026-01-01",
        end_date="2026-01-31",
        seed_offset=5000
    ))

    # Feb-Mar 2026
    all_invoices.extend(generate_invoices_for_period(
        count=301,
        total=7276173.89,
        start_date="2026-02-01",
        end_date="2026-03-31",
        seed_offset=5500
    ))

    # Q2 2026 = $11,862,726.26 (482)
    all_invoices.extend(generate_invoices_for_period(
        count=482,
        total=11862726.26,
        start_date="2026-04-01",
        end_date="2026-06-30",
        seed_offset=6000
    ))

    # Q3 2026 = $12,372,935.60 (496)
    all_invoices.extend(generate_invoices_for_period(
        count=496,
        total=12372935.60,
        start_date="2026-07-01",
        end_date="2026-09-30",
        seed_offset=7000
    ))

    # Q4 2026 = $12,522,817.96 (516)
    all_invoices.extend(generate_invoices_for_period(
        count=516,
        total=12522817.96,
        start_date="2026-10-01",
        end_date="2026-12-31",
        seed_offset=8000
    ))

    output_path = Path("dcl/demo/datasets/nlq_test/invoices.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Sort by date
    all_invoices.sort(key=lambda x: x["invoice_date"])

    # Assign invoice IDs
    for i, inv in enumerate(all_invoices, 1):
        inv["invoice_id"] = f"INV-{i:05d}"

    # Write CSV
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["invoice_id", "customer_id", "customer_name", "amount", "invoice_date"])
        writer.writeheader()
        writer.writerows(all_invoices)

    print(f"Generated {len(all_invoices)} invoices to {output_path}")

    # Verify totals
    def sum_period(start: str, end: str) -> tuple:
        filtered = [inv for inv in all_invoices if start <= inv["invoice_date"] <= end]
        return sum(inv["amount"] for inv in filtered), len(filtered)

    print("\n=== Data Verification ===")

    t, c = sum_period("2024-01-01", "2024-12-31")
    print(f"2024: ${t:,.2f} ({c} txns) [expected: $47,898,547.96 (1984)]")

    t, c = sum_period("2025-01-01", "2025-12-31")
    print(f"2025: ${t:,.2f} ({c} txns) [expected: $48,701,027.04 (1978)]")

    t, c = sum_period("2025-10-01", "2025-12-31")
    print(f"Q4 2025: ${t:,.2f} ({c} txns) [expected: $12,372,935.60 (496)]")

    t, c = sum_period("2025-12-01", "2025-12-31")
    print(f"Dec 2025: ${t:,.2f} ({c} txns) [expected: $3,745,344.33 (167)]")

    t, c = sum_period("2026-01-01", "2026-12-31")
    print(f"2026: ${t:,.2f} ({c} txns) [expected: $48,701,027.04 (1978)]")

    t, c = sum_period("2026-01-01", "2026-01-31")
    print(f"Jan 2026: ${t:,.2f} ({c} txns) [expected: $4,666,373.33 (183)]")

    t, c = sum_period("2026-01-01", "2026-03-31")
    print(f"Q1 2026: ${t:,.2f} ({c} txns) [expected: $11,942,547.22 (484)]")


if __name__ == "__main__":
    main()
