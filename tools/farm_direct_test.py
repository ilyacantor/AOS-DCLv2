#!/usr/bin/env python3
"""
Direct Farm API Test - Tests Farm endpoints without going through DCL.

Usage:
    python tools/farm_direct_test.py [--scenario SCENARIO_ID]

This script calls Farm endpoints directly to verify they work.
"""
import os
import sys
import argparse

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.farm.client import FarmClient


def test_farm_endpoints(scenario_id: str):
    """Test Farm endpoints directly."""
    print("=" * 60)
    print("DIRECT FARM API TEST")
    print("=" * 60)
    print(f"Scenario ID: {scenario_id}")
    print()

    client = FarmClient()
    results = []

    # Test 1: Health check
    print("[1/4] Testing Farm health...")
    try:
        health = client.health_check()
        if health.get("status") == "healthy":
            print(f"  ✓ Farm is healthy at {health.get('farm_url')}")
            results.append(("health", True, None))
        else:
            print(f"  ✗ Farm unhealthy: {health.get('error')}")
            results.append(("health", False, health.get("error")))
    except Exception as e:
        print(f"  ✗ Health check failed: {e}")
        results.append(("health", False, str(e)))

    # Test 2: Total Revenue (with time_window)
    print("\n[2/4] Testing total-revenue endpoint...")
    try:
        revenue = client.get_total_revenue(scenario_id, time_window="last_year")
        total = revenue.get("total_revenue", 0)
        period = revenue.get("period", "N/A")
        txn_count = revenue.get("transaction_count", 0)
        applied = revenue.get("time_window_applied")

        print(f"  ✓ Total Revenue: ${total:,.2f}")
        print(f"    Period: {period}")
        print(f"    Transactions: {txn_count}")
        print(f"    Time window applied: {applied}")
        results.append(("total_revenue", True, None))
    except Exception as e:
        print(f"  ✗ Total revenue failed: {e}")
        results.append(("total_revenue", False, str(e)))

    # Test 3: Total Revenue (no time_window - all time)
    print("\n[3/4] Testing total-revenue endpoint (all time)...")
    try:
        revenue = client.get_total_revenue(scenario_id)
        total = revenue.get("total_revenue", 0)
        period = revenue.get("period", "N/A")
        txn_count = revenue.get("transaction_count", 0)

        print(f"  ✓ Total Revenue (All Time): ${total:,.2f}")
        print(f"    Period: {period}")
        print(f"    Transactions: {txn_count}")
        results.append(("total_revenue_all", True, None))
    except Exception as e:
        print(f"  ✗ Total revenue (all time) failed: {e}")
        results.append(("total_revenue_all", False, str(e)))

    # Test 4: Top Customers
    print("\n[4/4] Testing top-customers endpoint...")
    try:
        customers = client.get_top_customers(scenario_id, limit=5, time_window="last_year")
        cust_list = customers.get("customers", [])

        print(f"  ✓ Got {len(cust_list)} top customers:")
        for i, c in enumerate(cust_list[:5], 1):
            name = c.get("name", "Unknown")
            rev = c.get("revenue", 0)
            pct = c.get("percent_of_total", 0)
            print(f"    {i}. {name}: ${rev:,.2f} ({pct:.1f}%)")
        results.append(("top_customers", True, None))
    except Exception as e:
        print(f"  ✗ Top customers failed: {e}")
        results.append(("top_customers", False, str(e)))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, success, _ in results if success)
    failed = sum(1 for _, success, _ in results if not success)

    print(f"Passed: {passed}/{len(results)}")
    print(f"Failed: {failed}/{len(results)}")

    if failed > 0:
        print("\nFailed tests:")
        for name, success, error in results:
            if not success:
                print(f"  - {name}: {error}")

    return failed == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Farm endpoints directly")
    parser.add_argument(
        "--scenario", "-s",
        default=os.environ.get("FARM_SCENARIO_ID", "dfa0ae0d57c9"),
        help="Farm scenario ID (default: FARM_SCENARIO_ID env var or dfa0ae0d57c9)"
    )
    args = parser.parse_args()

    success = test_farm_endpoints(args.scenario)
    sys.exit(0 if success else 1)
