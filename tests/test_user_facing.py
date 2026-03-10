"""
User-Facing Test Harness — tests what the user sees through NLQ.

Every test hits NLQ endpoints (the user-facing layer), not DCL directly.
If a test passes but the UI is broken, the test is wrong.
"""

import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

import httpx
import yaml

NLQ_URL = os.environ.get("NLQ_BASE_URL", "http://localhost:8005")
DCL_URL = os.environ.get("DCL_BASE_URL", "http://localhost:8004")

_client = httpx.Client(timeout=30.0)


# ─── Result tracking ──────────────────────────────────────────────────
@dataclass
class UFResult:
    test_id: str
    name: str
    passed: bool
    message: str = ""
    user_sees: str = ""  # What the user would see in the UI


results: List[UFResult] = []


def _record(test_id: str, name: str, passed: bool, message: str = "", user_sees: str = ""):
    r = UFResult(test_id=test_id, name=name, passed=passed, message=message, user_sees=user_sees)
    results.append(r)
    status = "[PASS]" if passed else "[FAIL]"
    line = f"  {status} {test_id}: {name}"
    if not passed and message:
        line += f"\n         {message}"
    if not passed and user_sees:
        line += f"\n         USER SEES: {user_sees}"
    print(line)
    return r


# ─── Assertion helpers ────────────────────────────────────────────────

def _resolve_field(data: dict, field_path: str) -> Any:
    """Resolve dotted field path in nested dict. Supports array indexing (data.0.value)."""
    parts = field_path.split(".")
    obj = data
    for part in parts:
        if obj is None:
            return None
        if isinstance(obj, list):
            try:
                obj = obj[int(part)]
            except (ValueError, IndexError):
                return None
        elif isinstance(obj, dict):
            obj = obj.get(part)
        else:
            return None
    return obj


def _check(actual: Any, operator: str, expected: Any) -> (bool, str):
    """Check a single assertion. Returns (passed, failure_message)."""
    if operator == "equals":
        ok = actual == expected
        return ok, f"expected {expected!r}, got {actual!r}"
    elif operator == "not_equals":
        ok = actual != expected
        return ok, f"expected NOT {expected!r}, got {actual!r}"
    elif operator == "not_null":
        ok = actual is not None
        return ok, f"expected not null, got {actual!r}"
    elif operator == "greater_than":
        try:
            ok = float(actual) > float(expected)
        except (TypeError, ValueError):
            ok = False
        return ok, f"expected > {expected}, got {actual!r}"
    elif operator == "not_contains":
        if actual is None:
            actual = ""
        ok = str(expected) not in str(actual)
        return ok, f"expected NOT to contain {expected!r}, but found it in: {str(actual)[:200]}"
    elif operator == "contains_any":
        if actual is None:
            actual = ""
        actual_str = str(actual)
        ok = any(sub in actual_str for sub in expected)
        return ok, f"expected to contain one of {expected!r}, got: {actual_str[:200]}"
    elif operator == "min_length":
        if actual is None:
            actual = ""
        ok = len(str(actual)) >= int(expected)
        return ok, f"expected length >= {expected}, got {len(str(actual))}"
    elif operator == "not_in":
        ok = actual not in expected
        return ok, f"expected NOT in {expected!r}, got {actual!r}"
    elif operator == "in":
        ok = actual in expected
        return ok, f"expected in {expected!r}, got {actual!r}"
    else:
        return False, f"unknown operator: {operator}"


# ─── Test execution ───────────────────────────────────────────────────

def _get_service_url(service: str) -> str:
    if service == "nlq":
        return NLQ_URL
    elif service == "dcl":
        return DCL_URL
    else:
        raise ValueError(f"Unknown service: {service}")


def _run_test(tc: dict) -> UFResult:
    """Run a single test case from YAML definition."""
    test_id = tc["id"]
    name = tc["name"]
    service = tc.get("service", "nlq")
    method = tc.get("method", "POST").upper()
    path = tc["path"]
    body = tc.get("body", {})
    assertions = tc.get("assertions", [])

    base_url = _get_service_url(service)
    url = f"{base_url}{path}"

    try:
        if method == "POST":
            resp = _client.post(url, json=body)
        elif method == "GET":
            resp = _client.get(url)
        else:
            return _record(test_id, name, False, f"Unsupported method: {method}")
    except Exception as e:
        return _record(test_id, name, False, f"HTTP error: {e}",
                       user_sees=f"Service {service} unreachable")

    # Parse response
    try:
        data = resp.json()
    except Exception:
        return _record(test_id, name, False,
                       f"Non-JSON response (status={resp.status_code}): {resp.text[:200]}",
                       user_sees=f"Service returned status {resp.status_code}")

    # Add status_code to data for assertion checking
    data["status_code"] = resp.status_code

    # Build user-facing description of what happened
    question = body.get("question", "")
    value = data.get("value")
    answer = str(data.get("answer", ""))[:150]
    data_source = data.get("data_source")
    user_sees_desc = ""
    if question:
        user_sees_desc = f"User asked '{question}'. "
    if value is not None:
        user_sees_desc += f"Got value={value}. "
    if answer:
        user_sees_desc += f"Answer: {answer}"
    if data_source:
        user_sees_desc += f" [source={data_source}]"

    # Check assertions
    for assertion in assertions:
        field_path = assertion.get("field", "")
        operator = assertion.get("operator", "")
        expected = assertion.get("expected")

        actual = _resolve_field(data, field_path)
        passed, msg = _check(actual, operator, expected)

        if not passed:
            return _record(test_id, name, False,
                           f"Assertion failed: {field_path} {operator} {expected!r} — {msg}",
                           user_sees=user_sees_desc)

    return _record(test_id, name, True)


def _run_comparison_test(tc: dict) -> UFResult:
    """Run a comparison test (cross-check between two queries)."""
    test_id = tc["id"]
    name = tc["name"]
    query_a = tc["query_a"]
    query_b = tc["query_b"]
    assertions = tc.get("assertions", [])

    # Execute query A
    url_a = f"{_get_service_url(query_a['service'])}{query_a['path']}"
    try:
        resp_a = _client.post(url_a, json=query_a.get("body", {}))
        data_a = resp_a.json()
    except Exception as e:
        return _record(test_id, name, False, f"Query A failed: {e}")

    # Execute query B
    url_b = f"{_get_service_url(query_b['service'])}{query_b['path']}"
    try:
        resp_b = _client.post(url_b, json=query_b.get("body", {}))
        data_b = resp_b.json()
    except Exception as e:
        return _record(test_id, name, False, f"Query B failed: {e}")

    # Extract values
    val_a = _resolve_field(data_a, query_a["extract"])
    val_b = _resolve_field(data_b, query_b["extract"])

    for assertion in assertions:
        op = assertion.get("operator", "")
        if op == "values_equal":
            tolerance = assertion.get("tolerance", 0.01)
            if val_a is None or val_b is None:
                return _record(test_id, name, False,
                               f"Cannot compare: A={val_a!r}, B={val_b!r}",
                               user_sees=f"NLQ returned {val_a}, DCL returned {val_b}")
            try:
                diff = abs(float(val_a) - float(val_b))
                threshold = abs(float(val_b)) * tolerance if float(val_b) != 0 else tolerance
                if diff > threshold:
                    return _record(test_id, name, False,
                                   f"Values differ: A={val_a}, B={val_b} (diff={diff:.4f}, tolerance={tolerance})",
                                   user_sees=f"NLQ shows {val_a}, but DCL has {val_b}")
            except (TypeError, ValueError) as e:
                return _record(test_id, name, False, f"Cannot compare values: {e}")

    return _record(test_id, name, True)


# ─── Test suites ──────────────────────────────────────────────────────

def _load_tests() -> List[dict]:
    """Load test definitions from YAML."""
    test_file = os.path.join(os.path.dirname(__file__), "test_user_facing.yaml")
    with open(test_file) as f:
        return yaml.safe_load(f)


def run_suite(level: str, tests: List[dict]):
    """Run all tests for a given level."""
    suite_tests = [tc for tc in tests if tc.get("level") == level]
    if not suite_tests:
        return
    print(f"\n=== SUITE: {level.upper()} ===")
    for tc in suite_tests:
        if tc.get("type") == "comparison":
            _run_comparison_test(tc)
        else:
            _run_test(tc)


# ─── main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n=== USER-FACING TEST HARNESS ===")
    print(f"NLQ: {NLQ_URL}")
    print(f"DCL: {DCL_URL}")

    # Check services are up
    try:
        r = _client.get(f"{NLQ_URL}/api/v1/health")
        if r.status_code != 200:
            print(f"FATAL: NLQ not healthy (status={r.status_code})")
            sys.exit(1)
        print(f"NLQ: healthy")
    except Exception as e:
        print(f"FATAL: NLQ unreachable at {NLQ_URL}: {e}")
        sys.exit(1)

    try:
        r = _client.get(f"{DCL_URL}/health")
        if r.status_code != 200:
            print(f"FATAL: DCL not healthy (status={r.status_code})")
            sys.exit(1)
        dcl_health = r.json()
        print(f"DCL: healthy (mode={dcl_health.get('data_mode', 'unknown')})")
    except Exception as e:
        print(f"FATAL: DCL unreachable at {DCL_URL}: {e}")
        sys.exit(1)

    tests = _load_tests()

    run_suite("chat", tests)
    run_suite("dashboard", tests)
    run_suite("report", tests)
    run_suite("cross_check", tests)

    # Summary
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    print(f"\n=== RESULTS ===")
    print(f"Total: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed:
        print(f"\n=== FAILURES ===")
        for r in results:
            if not r.passed:
                print(f"  {r.test_id}: {r.name}")
                if r.message:
                    print(f"    {r.message}")
                if r.user_sees:
                    print(f"    USER SEES: {r.user_sees}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
