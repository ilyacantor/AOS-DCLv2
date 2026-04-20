#!/usr/bin/env python3
"""
Mai Status Endpoint — DCL Test Suite

Tests GET /mai/status per session1_module_status.md spec.
All rules from CLAUDE.md apply.
"""

import os
import sys
import time
from typing import List

import requests

BASE_URL = os.environ.get("DCL_BASE_URL", "http://localhost:8004")
TIMEOUT = 30.0


class TestResult:
    def __init__(self, test_id: str, description: str):
        self.test_id = test_id
        self.description = description
        self.passed = False
        self.message = ""

    def pass_(self, msg: str = ""):
        self.passed = True
        self.message = msg
        print(f"  [PASS] {self.test_id}: {self.description}")

    def fail(self, expected: str, got: str):
        self.passed = False
        self.message = f"expected {expected}, got {got}"
        print(f"  [FAIL] {self.test_id}: {self.description} -- {self.message}")


results: List[TestResult] = []


def test(test_id: str, description: str) -> TestResult:
    r = TestResult(test_id, description)
    results.append(r)
    return r


def api_get(path: str, **kwargs) -> requests.Response:
    return requests.get(f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)


def wait_for_backend(max_wait: int = 15) -> bool:
    for i in range(max_wait):
        try:
            r = requests.get(f"{BASE_URL}/health", timeout=5)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


# ─── Tests ─────────────────────────────────────────────────────────────────


def test_mai_health_gate():
    """DCL must be healthy before testing mai status."""
    t = test("MAI-000", "DCL backend is reachable")
    try:
        r = api_get("/health")
        if r.status_code == 200:
            t.pass_()
        else:
            t.fail("HTTP 200", f"HTTP {r.status_code}")
    except Exception as e:
        t.fail("reachable", str(e))


def test_mai_status_200():
    """GET /mai/status returns HTTP 200."""
    t = test("MAI-001", "GET /mai/status returns 200")
    r = api_get("/mai/status", params={"tenant_id": "default"})
    if r.status_code == 200:
        t.pass_()
    else:
        t.fail("HTTP 200", f"HTTP {r.status_code}: {r.text[:200]}")


def test_mai_status_valid_json():
    """Response is valid JSON."""
    t = test("MAI-002", "Response is valid JSON")
    r = api_get("/mai/status", params={"tenant_id": "default"})
    try:
        r.json()
        t.pass_()
    except Exception as e:
        t.fail("valid JSON", str(e))


def test_mai_status_module_field():
    """module field is 'dcl'."""
    t = test("MAI-003", "module field == 'dcl'")
    data = api_get("/mai/status", params={"tenant_id": "default"}).json()
    if data.get("module") == "dcl":
        t.pass_()
    else:
        t.fail("'dcl'", repr(data.get("module")))


def test_mai_status_tenant_id():
    """tenant_id matches request."""
    t = test("MAI-004", "tenant_id matches request parameter")
    tid = "meridian"
    data = api_get("/mai/status", params={"tenant_id": tid}).json()
    if data.get("tenant_id") == tid:
        t.pass_()
    else:
        t.fail(repr(tid), repr(data.get("tenant_id")))


def test_mai_status_healthy_boolean():
    """healthy field is a boolean."""
    t = test("MAI-005", "healthy field is boolean")
    data = api_get("/mai/status", params={"tenant_id": "default"}).json()
    if isinstance(data.get("healthy"), bool):
        t.pass_()
    else:
        t.fail("bool", repr(type(data.get("healthy"))))


def test_mai_status_schema_fields():
    """Response contains all required fields from the spec."""
    t = test("MAI-006", "All required schema fields present")
    data = api_get("/mai/status", params={"tenant_id": "default"}).json()
    required = [
        "module", "tenant_id", "concepts", "dimensions", "pairings",
        "entities", "extraction_rules",
        "last_update_at", "healthy",
    ]
    missing = [f for f in required if f not in data]
    if not missing:
        t.pass_()
    else:
        t.fail("all fields present", f"missing: {missing}")


def test_mai_status_concepts_structure():
    """concepts has count field with int > 0."""
    t = test("MAI-007", "concepts.count is int > 0")
    data = api_get("/mai/status", params={"tenant_id": "default"}).json()
    concepts = data.get("concepts", {})
    count = concepts.get("count")
    if isinstance(count, int) and count > 0:
        t.pass_(f"count={count}")
    else:
        t.fail("int > 0", repr(count))


def test_mai_status_dimensions_structure():
    """dimensions has count field with int > 0."""
    t = test("MAI-008", "dimensions.count is int > 0")
    data = api_get("/mai/status", params={"tenant_id": "default"}).json()
    dims = data.get("dimensions", {})
    count = dims.get("count")
    if isinstance(count, int) and count > 0:
        t.pass_(f"count={count}")
    else:
        t.fail("int > 0", repr(count))


def test_mai_status_pairings_structure():
    """pairings has count field with int > 0."""
    t = test("MAI-009", "pairings.count is int > 0")
    data = api_get("/mai/status", params={"tenant_id": "default"}).json()
    pairings = data.get("pairings", {})
    count = pairings.get("count")
    if isinstance(count, int) and count > 0:
        t.pass_(f"count={count}")
    else:
        t.fail("int > 0", repr(count))


def test_mai_status_entities_structure():
    """entities has count and list fields."""
    t = test("MAI-010", "entities has count (int > 0) and list (non-empty)")
    data = api_get("/mai/status", params={"tenant_id": "default"}).json()
    entities = data.get("entities", {})
    count = entities.get("count")
    elist = entities.get("list")
    if isinstance(count, int) and count > 0 and isinstance(elist, list) and len(elist) > 0:
        t.pass_(f"count={count}, list has {len(elist)} items")
    else:
        t.fail("count > 0 and non-empty list", f"count={count}, list={elist}")


def test_mai_status_extraction_rules_structure():
    """extraction_rules has count, active, errored fields."""
    t = test("MAI-011", "extraction_rules has count/active/errored")
    data = api_get("/mai/status", params={"tenant_id": "default"}).json()
    er = data.get("extraction_rules", {})
    required_keys = ["count", "active", "errored"]
    missing = [k for k in required_keys if k not in er]
    if missing:
        t.fail(f"keys {required_keys}", f"missing: {missing}")
        return
    if isinstance(er["count"], int) and er["count"] > 0:
        t.pass_(f"count={er['count']}, active={er['active']}, errored={er['errored']}")
    else:
        t.fail("count > 0", f"count={er['count']}")


def test_mai_status_response_time():
    """Response time < 500ms."""
    t = test("MAI-013", "Response time < 500ms")
    start = time.monotonic()
    r = api_get("/mai/status", params={"tenant_id": "default"})
    elapsed_ms = (time.monotonic() - start) * 1000
    if r.status_code == 200 and elapsed_ms < 500:
        t.pass_(f"{elapsed_ms:.0f}ms")
    else:
        t.fail("<500ms", f"{elapsed_ms:.0f}ms (HTTP {r.status_code})")


def test_mai_status_default_tenant():
    """Omitting tenant_id defaults to 'default'."""
    t = test("MAI-014", "Default tenant_id is 'default'")
    data = api_get("/mai/status").json()
    if data.get("tenant_id") == "default":
        t.pass_()
    else:
        t.fail("'default'", repr(data.get("tenant_id")))


# ─── Runner ─────────────────────────────────────────────────────────────────


def run_all():
    print("\n" + "=" * 60)
    print("  DCL Mai Status Endpoint — Test Suite")
    print("=" * 60 + "\n")

    if not wait_for_backend():
        print(f"[FATAL] DCL backend not reachable at {BASE_URL}")
        sys.exit(1)

    test_mai_health_gate()
    test_mai_status_200()
    test_mai_status_valid_json()
    test_mai_status_module_field()
    test_mai_status_tenant_id()
    test_mai_status_healthy_boolean()
    test_mai_status_schema_fields()
    test_mai_status_concepts_structure()
    test_mai_status_dimensions_structure()
    test_mai_status_pairings_structure()
    test_mai_status_entities_structure()
    test_mai_status_extraction_rules_structure()
    test_mai_status_response_time()
    test_mai_status_default_tenant()

    passed = sum(1 for r in results if r.passed)
    total = len(results)

    print(f"\n{'=' * 60}")
    print(f"  Results: {passed}/{total} passed")
    print(f"{'=' * 60}\n")

    if passed < total:
        failed = [r for r in results if not r.passed]
        for r in failed:
            print(f"  FAILED: {r.test_id} — {r.description}: {r.message}")
        sys.exit(1)


if __name__ == "__main__":
    run_all()
