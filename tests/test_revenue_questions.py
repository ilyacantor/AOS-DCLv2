#!/usr/bin/env python3
"""
Test harness for NLQ revenue questions with time windows.
Validates that NLQ returns correct answers matching ground truth.

Ground truth values are FIXED per calendar year:
- 2024: $47,898,547.96 (1,984 txns)
- 2025: $48,701,027.04 (1,978 txns)
- 2026: $48,701,027.04 (1,978 txns)

Relative terms resolve dynamically based on current date.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from backend.nlq.param_extractor import extract_params
from backend.bll.executor import execute_definition, _parse_time_window
from backend.bll.models import ExecuteRequest

# Fixed ground truth values per calendar year
YEAR_DATA = {
    2024: (47898547.96, 1984),
    2025: (48701027.04, 1978),
    2026: (48701027.04, 1978),
}

# Quarter data for 2025 (for last_quarter when in Q1 2026)
Q4_2025 = (12372935.60, 496)
DEC_2025 = (3745344.33, 167)

# Quarter data for 2026
Q1_2026 = (11942547.22, 484)
Q2_2026 = (11862726.26, 482)
Q3_2026 = (12372935.60, 496)
Q4_2026 = (12522817.96, 516)

# Month data for 2026
JAN_2026 = (4666373.33, 183)


def get_expected_values():
    """
    Generate expected values based on current date.
    Relative terms (last_year, this_year, etc.) resolve dynamically.
    """
    today = datetime.now()
    current_year = today.year
    current_month = today.month
    current_quarter = (current_month - 1) // 3 + 1

    # Get year data (use 2026 data for any year >= 2026)
    def get_year_data(year):
        return YEAR_DATA.get(year, YEAR_DATA[2026])

    # Total = all years combined (for this test: 2024+2025+2026)
    total = sum(v[0] for v in YEAR_DATA.values()), sum(v[1] for v in YEAR_DATA.values())

    # last_year = previous calendar year
    last_year_data = get_year_data(current_year - 1)

    # this_year = current calendar year (full year)
    this_year_data = get_year_data(current_year)

    # YTD = current year up to today (partial period - actual data in range)
    # YTD returns actual data from Jan 1 to today, not full month
    if current_year == 2026 and current_month == 1:
        # YTD in Jan 2026 = Jan 1-27 (today), which is a subset of full January
        # The actual data depends on what's in the date range
        # From the data generation: 183 invoices in Jan, ~27/31 days = ~158 invoices
        ytd_data = (3914790.09, 158)  # Actual value for Jan 1-27
    else:
        ytd_data = this_year_data  # Approximate

    # Q1-Q4 refer to current year
    quarter_data = {
        1: Q1_2026 if current_year == 2026 else get_year_data(current_year),
        2: Q2_2026 if current_year == 2026 else get_year_data(current_year),
        3: Q3_2026 if current_year == 2026 else get_year_data(current_year),
        4: Q4_2026 if current_year == 2026 else get_year_data(current_year),
    }

    # last_quarter = previous quarter
    if current_quarter == 1:
        # In Q1, last quarter is Q4 of previous year
        last_quarter_data = Q4_2025 if current_year == 2026 else get_year_data(current_year - 1)
    else:
        last_quarter_data = quarter_data[current_quarter - 1]

    # this_quarter = current quarter (full quarter)
    this_quarter_data = quarter_data[current_quarter]

    # last_month = previous month
    if current_month == 1:
        # In January, last month is December of previous year
        last_month_data = DEC_2025 if current_year == 2026 else get_year_data(current_year - 1)
    else:
        # Approximate with quarter data
        last_month_data = this_year_data

    # this_month = current month (full month)
    if current_year == 2026 and current_month == 1:
        this_month_data = JAN_2026
    else:
        this_month_data = this_year_data  # Approximate

    return {
        "total": total,
        "last_year": last_year_data,
        "this_year": this_year_data,
        "2024": YEAR_DATA[2024],
        "2025": YEAR_DATA[2025],
        "ytd": ytd_data,
        "q1": quarter_data[1],
        "q2": quarter_data[2],
        "q3": quarter_data[3],
        "q4": quarter_data[4],
        "last_quarter": last_quarter_data,
        "this_quarter": this_quarter_data,
        "last_month": last_month_data,
        "this_month": this_month_data,
    }


# Test cases: (question, time_window_key)
TEST_CASES = [
    ("What is our total revenue?", "total"),
    ("What was our revenue last year?", "last_year"),
    ("What is our revenue this year?", "this_year"),
    ("How much revenue did we make in 2024?", "2024"),
    ("What's our 2025 revenue?", "2025"),
    ("What's our year-to-date revenue?", "ytd"),
    ("What was Q1 revenue?", "q1"),
    ("What was Q2 revenue?", "q2"),
    ("What was Q3 revenue?", "q3"),
    ("What was Q4 revenue?", "q4"),
    ("What was revenue last quarter?", "last_quarter"),
    ("What's our revenue this quarter?", "this_quarter"),
    ("What was revenue last month?", "last_month"),
    ("What's our revenue this month?", "this_month"),
]


def test_param_extraction():
    """Test that time windows are correctly extracted from questions."""
    print("\n=== Testing param_extractor.extract_time_window ===\n")

    expected_time_windows = {
        "total": None,
        "last_year": "last_year",
        "this_year": "this_year",
        "2024": "2024",
        "2025": "2025",
        "ytd": "ytd",
        "q1": "q1",
        "q2": "q2",
        "q3": "q3",
        "q4": "q4",
        "last_quarter": "last_quarter",
        "this_quarter": "this_quarter",
        "last_month": "last_month",
        "this_month": "this_month",
    }

    passed = 0
    failed = 0

    for question, key in TEST_CASES:
        exec_args = extract_params(question)
        extracted_tw = exec_args.time_window
        expected_tw = expected_time_windows[key]

        if extracted_tw == expected_tw:
            print(f"✓ '{question[:40]}...' → {extracted_tw}")
            passed += 1
        else:
            print(f"✗ '{question[:40]}...'")
            print(f"  Expected: {expected_tw}")
            print(f"  Got:      {extracted_tw}")
            failed += 1

    print(f"\nParam Extraction: {passed}/{len(TEST_CASES)} passed ({passed/len(TEST_CASES)*100:.1f}%)")
    return failed == 0


def test_time_window_parsing():
    """Test that time window strings are parsed to correct date ranges."""
    print("\n=== Testing _parse_time_window ===\n")

    time_windows = [None, "last_year", "this_year", "2024", "2025", "ytd",
                    "q1", "q2", "q3", "q4", "last_quarter", "this_quarter",
                    "last_month", "this_month"]

    for tw in time_windows:
        start, end = _parse_time_window(tw)
        print(f"  {tw or 'None':15} → {start} to {end}")


def test_full_execution():
    """Test full execution with NLQ-extracted time windows against BLL."""
    print("\n=== Testing Full Execution ===\n")

    expected_values = get_expected_values()
    passed = 0
    failed = 0

    for question, key in TEST_CASES:
        expected_total, expected_count = expected_values[key]

        # Extract params from question
        exec_args = extract_params(question)

        # Execute definition
        request = ExecuteRequest(
            dataset_id="nlq_test",
            definition_id="finops.total_revenue",
            limit=10000,
            time_window_str=exec_args.time_window,
        )

        try:
            result = execute_definition(request)

            if result.summary:
                agg = result.summary.aggregations
                total = agg.get("population_total", 0)
                count = agg.get("transaction_count", 0)
                period = agg.get("period", "All Time")
                tw_applied = agg.get("time_window_applied", False)

                # Check if answer matches (within $1 tolerance for rounding)
                total_match = abs(total - expected_total) < 1.0
                count_match = count == expected_count

                if total_match and count_match:
                    print(f"✓ '{question[:40]}...'")
                    print(f"  → ${total:,.2f} ({count:,} txns) | Period: {period}")
                    passed += 1
                else:
                    print(f"✗ '{question[:40]}...'")
                    print(f"  Expected: ${expected_total:,.2f} ({expected_count:,} txns)")
                    print(f"  Got:      ${total:,.2f} ({count:,} txns)")
                    print(f"  TW Requested: {exec_args.time_window} | TW Applied: {tw_applied}")
                    failed += 1
            else:
                print(f"✗ '{question[:40]}...' - No summary returned")
                failed += 1

        except Exception as e:
            print(f"✗ '{question[:40]}...' - Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\nFull Execution: {passed}/{len(TEST_CASES)} passed ({passed/len(TEST_CASES)*100:.1f}%)")
    return failed == 0


if __name__ == "__main__":
    print("=" * 70)
    print("NLQ Revenue Questions Test Suite")
    print(f"Today: {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 70)

    # Test param extraction
    extraction_ok = test_param_extraction()

    # Test time window parsing
    test_time_window_parsing()

    # Test full execution
    execution_ok = test_full_execution()

    print("\n" + "=" * 70)
    if extraction_ok and execution_ok:
        print("ALL TESTS PASSED ✓")
        sys.exit(0)
    else:
        print("SOME TESTS FAILED ✗")
        sys.exit(1)
