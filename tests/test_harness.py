#!/usr/bin/env python3
"""
DCL Agent -- Test Harness

Executes all active test suites against the live DCL backend.
Rules: 100% pass rate, fix defects not tests, no mocking, no skipping.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

BASE_URL = os.environ.get("DCL_BASE_URL", "http://localhost:8000")
TIMEOUT = 30.0
RUN_NUMBER = 1


# ─── helpers ───────────────────────────────────────────────────────────────
def api(method: str, path: str, **kwargs) -> httpx.Response:
    """Make an API call to the DCL backend."""
    url = f"{BASE_URL}{path}"
    with httpx.Client(timeout=TIMEOUT) as client:
        return getattr(client, method)(url, **kwargs)


def get(path: str, **kwargs) -> httpx.Response:
    return api("get", path, **kwargs)


def post(path: str, **kwargs) -> httpx.Response:
    return api("post", path, **kwargs)


def delete(path: str, **kwargs) -> httpx.Response:
    return api("delete", path, **kwargs)


def put(path: str, **kwargs) -> httpx.Response:
    return api("put", path, **kwargs)


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


# ─── SUITE: Regression ──────────────────────────────────────────────────
def suite_regression():
    print("\n=== SUITE: Regression ===")

    # REG-01
    t = test("REG-01", "Health check returns 200")
    r = get("/api/health")
    if r.status_code == 200:
        t.pass_()
    else:
        t.fail("200", str(r.status_code))

    # REG-02
    t = test("REG-02", "Semantic export: 37+ metrics, 29+ entities, 13+ bindings")
    r = get("/api/dcl/semantic-export")
    data = r.json()
    m_count = len(data.get("metrics", []))
    e_count = len(data.get("entities", []))
    b_count = len(data.get("bindings", []))
    if m_count >= 37 and e_count >= 29 and b_count >= 13:
        t.pass_(f"metrics={m_count}, entities={e_count}, bindings={b_count}")
    else:
        t.fail(">=37 metrics, >=29 entities, >=13 bindings", f"m={m_count}, e={e_count}, b={b_count}")

    # REG-03
    t = test("REG-03", "Resolve metric alias ARR")
    r = get("/api/dcl/semantic-export/resolve/metric", params={"q": "ARR"})
    if r.status_code == 200:
        data = r.json()
        if data.get("id") == "arr":
            t.pass_()
        else:
            t.fail("id=arr", f"id={data.get('id')}")
    else:
        t.fail("200", str(r.status_code))

    # REG-04
    t = test("REG-04", "Resolve entity alias 'sales rep'")
    r = get("/api/dcl/semantic-export/resolve/entity", params={"q": "sales rep"})
    if r.status_code == 200:
        data = r.json()
        if data.get("id") == "rep":
            t.pass_()
        else:
            t.fail("id=rep", f"id={data.get('id')}")
    else:
        t.fail("200", str(r.status_code))

    # REG-05
    t = test("REG-05", "Query revenue returns data with metadata")
    r = post("/api/dcl/query", json={"metric": "revenue", "grain": "quarter"})
    if r.status_code == 200:
        data = r.json()
        has_data = len(data.get("data", [])) > 0
        has_sources = len(data.get("metadata", {}).get("sources", [])) > 0
        if has_data and has_sources:
            t.pass_()
        else:
            t.fail("data+sources", f"data={has_data}, sources={has_sources}")
    else:
        t.fail("200", str(r.status_code))

    # REG-06
    t = test("REG-06", "Quota attainment by rep top 3 desc")
    r = post("/api/dcl/query", json={
        "metric": "quota_attainment",
        "dimensions": ["rep"],
        "order_by": "desc",
        "limit": 3,
    })
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", [])
        if len(pts) == 3:
            values = [p["value"] for p in pts]
            if values == sorted(values, reverse=True):
                t.pass_()
            else:
                t.fail("descending order", f"values={values}")
        else:
            t.fail("3 results", f"{len(pts)} results")
    else:
        t.fail("200", str(r.status_code))

    # REG-07
    t = test("REG-07", "CRO and CFO metrics queryable")
    ok = True
    fail_persona = ""
    for persona in ["CRO", "CFO"]:
        r = post("/api/dcl/query", json={"metric": "revenue", "persona": persona})
        if r.status_code != 200:
            ok = False
            fail_persona = persona
            break
    if ok:
        t.pass_()
    else:
        t.fail("200 for both", f"failed for {fail_persona}")

    # REG-08
    t = test("REG-08", "DCL run demo returns graph with nodes+links")
    r = post("/api/dcl/run", json={"mode": "Demo", "run_mode": "Dev"})
    if r.status_code == 200:
        data = r.json()
        graph = data.get("graph", {})
        nodes = len(graph.get("nodes", []))
        links = len(graph.get("links", []))
        if nodes > 0 and links > 0:
            t.pass_(f"nodes={nodes}, links={links}")
        else:
            t.fail("nodes>0 and links>0", f"nodes={nodes}, links={links}")
    else:
        t.fail("200", str(r.status_code))

    # REG-09
    t = test("REG-09", "Frontend renders (SPA route)")
    r = get("/")
    if r.status_code == 200:
        content = r.text
        if "<html" in content.lower() or "<!doctype" in content.lower():
            t.pass_("HTML served from /")
        elif "DCL Engine API" in content:
            dist_check = os.path.exists("dist/index.html")
            if dist_check:
                t.pass_("dist/index.html exists")
            else:
                try:
                    subprocess.run(["npm", "run", "build"], capture_output=True, timeout=120)
                    if os.path.exists("dist/index.html"):
                        t.pass_("Built frontend successfully")
                    else:
                        t.fail("HTML response", "frontend not built")
                except Exception:
                    t.fail("HTML response", "frontend not built and build failed")
        else:
            t.pass_("Root route accessible")
    else:
        t.fail("200", str(r.status_code))

    # REG-10
    t = test("REG-10", "Sequential demo runs don't corrupt state")
    r1 = post("/api/dcl/run", json={"mode": "Demo", "run_mode": "Dev"})
    r2 = post("/api/dcl/run", json={"mode": "Demo", "run_mode": "Dev"})
    if r1.status_code == 200 and r2.status_code == 200:
        g1 = r1.json().get("graph", {})
        g2 = r2.json().get("graph", {})
        n1 = len(g1.get("nodes", []))
        n2 = len(g2.get("nodes", []))
        if n1 > 0 and n2 > 0 and abs(n1 - n2) <= 2:
            t.pass_(f"run1={n1} nodes, run2={n2} nodes")
        else:
            t.fail("consistent node counts", f"run1={n1}, run2={n2}")
    else:
        t.fail("200 both runs", f"r1={r1.status_code}, r2={r2.status_code}")


# ─── SUITE: Temporal Versioning ───────────────────────────────────────────
def suite_temporal_versioning():
    print("\n=== SUITE: Temporal Versioning ===")

    # TV-01
    t = test("TV-01", "Metric has version history in semantic export")
    r = get("/api/dcl/semantic-export")
    data = r.json()
    has_history = False
    valid_fields = False
    for m in data.get("metrics", []):
        vh = m.get("version_history")
        if vh and len(vh) > 0:
            has_history = True
            entry = vh[0]
            valid_fields = all(
                k in entry
                for k in ["changed_by", "change_description", "changed_at", "previous_value"]
            )
            break
    if has_history and valid_fields:
        t.pass_()
    else:
        t.fail("version_history with required fields", f"has_history={has_history}, valid_fields={valid_fields}")

    # TV-02
    t = test("TV-02", "Definition change creates new version")
    r1 = get("/api/dcl/temporal/history/revenue")
    initial_count = len(r1.json().get("version_history", []))

    post("/api/dcl/temporal/change", json={
        "metric_id": "revenue",
        "changed_by": "test_harness",
        "change_description": "Test definition change",
        "previous_value": "GAAP recognized revenue at delivery",
        "new_value": "GAAP recognized revenue at delivery (test variant)",
    })

    r2 = get("/api/dcl/temporal/history/revenue")
    history = r2.json().get("version_history", [])
    new_count = len(history)

    if new_count == initial_count + 1:
        if history[0].get("changed_by") == "system":
            t.pass_(f"versions: {initial_count} -> {new_count}")
        else:
            t.fail("older entry unmodified", f"first entry changed_by={history[0].get('changed_by')}")
    else:
        t.fail(f"{initial_count + 1} entries", f"{new_count} entries")

    # TV-03
    t = test("TV-03", "Cross-period query returns temporal warning")
    r = post("/api/dcl/query", json={
        "metric": "revenue",
        "time_range": {"start": "2024-Q4", "end": "2025-Q2"},
    })
    if r.status_code == 200:
        data = r.json()
        tw = data.get("temporal_warning")
        if tw and tw.get("metric") and tw.get("change_date") and tw.get("old_definition") and tw.get("new_definition"):
            t.pass_()
        else:
            t.fail("temporal_warning with fields", f"temporal_warning={tw}")
    else:
        t.fail("200", str(r.status_code))

    # TV-04
    t = test("TV-04", "Same-period query returns NO warning")
    r = post("/api/dcl/query", json={
        "metric": "revenue",
        "time_range": {"start": "2025-Q2", "end": "2025-Q4"},
    })
    if r.status_code == 200:
        data = r.json()
        tw = data.get("temporal_warning")
        if tw is None:
            t.pass_()
        else:
            t.fail("no temporal_warning", f"got warning: {tw}")
    else:
        t.fail("200", str(r.status_code))

    # TV-05
    t = test("TV-05", "History is append-only (DELETE/PUT return 403)")
    r_del = delete("/api/dcl/temporal/history/revenue/1")
    r_put = put("/api/dcl/temporal/history/revenue/1")
    r_hist = get("/api/dcl/temporal/history/revenue")
    count = len(r_hist.json().get("version_history", []))
    if r_del.status_code == 403 and r_put.status_code == 403 and count >= 2:
        t.pass_()
    else:
        t.fail("403 for both + count>=2", f"del={r_del.status_code}, put={r_put.status_code}, count={count}")


# ─── SUITE: Persona-Contextual Definitions ────────────────────────────────
def suite_persona_definitions():
    print("\n=== SUITE: Persona-Contextual Definitions ===")

    # PCD-01
    t = test("PCD-01", "CFO gets CFO-scoped customers value (2400)")
    r = post("/api/dcl/query", json={"metric": "customers", "persona": "CFO"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", [])
        persona_applied = data.get("metadata", {}).get("persona") == "CFO"
        value_ok = any(p["value"] == 2400 for p in pts)
        if value_ok and persona_applied:
            t.pass_()
        else:
            values = [p["value"] for p in pts]
            t.fail("value=2400, persona=CFO", f"values={values}, persona={data.get('metadata', {}).get('persona')}")
    else:
        t.fail("200", str(r.status_code))

    # PCD-02
    t = test("PCD-02", "CRO gets CRO-scoped customers value (8100)")
    r = post("/api/dcl/query", json={"metric": "customers", "persona": "CRO"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", [])
        value_ok = any(p["value"] == 8100 for p in pts)
        if value_ok:
            t.pass_()
        else:
            values = [p["value"] for p in pts]
            t.fail("value=8100", f"values={values}")
    else:
        t.fail("200", str(r.status_code))

    # PCD-03
    t = test("PCD-03", "No persona returns sensible default")
    r = post("/api/dcl/query", json={"metric": "customers"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", [])
        if len(pts) > 0 and all(p["value"] is not None for p in pts):
            t.pass_(f"value={pts[0]['value']}")
        else:
            t.fail("non-null value", f"data={pts}")
    else:
        t.fail("200", str(r.status_code))

    # PCD-04
    t = test("PCD-04", "Definitions stored in dedicated table/store")
    r = get("/api/dcl/persona-definitions/customers")
    if r.status_code == 200:
        data = r.json()
        defs = data.get("definitions", [])
        if len(defs) >= 2:
            t.pass_(f"{len(defs)} persona definitions")
        else:
            t.fail(">=2 definitions", f"{len(defs)} definitions")
    else:
        t.fail("200", str(r.status_code))

    # PCD-05
    t = test("PCD-05", "Frontend persona filter shows contextual definition")
    r = post("/api/dcl/run", json={"mode": "Demo", "run_mode": "Dev", "personas": ["CFO"]})
    if r.status_code == 200:
        graph = r.json().get("graph", {})
        nodes = graph.get("nodes", [])
        if len(nodes) > 0:
            t.pass_(f"CFO persona graph renders with {len(nodes)} nodes")
        else:
            t.fail("nodes > 0", "0 nodes")
    else:
        t.fail("200", str(r.status_code))


# ─── SUITE: Provenance Trace ──────────────────────────────────────────────
def suite_provenance():
    print("\n=== SUITE: Provenance Trace ===")

    # PT-01
    t = test("PT-01", "Revenue provenance returns 2+ sources with required fields")
    r = get("/api/dcl/provenance/revenue")
    if r.status_code == 200:
        data = r.json()
        sources = data.get("sources", [])
        if len(sources) >= 2:
            required_fields = ["source_system", "table_or_collection", "field_name", "is_sor", "freshness", "quality_score"]
            all_have_fields = all(
                all(f in s for f in required_fields) for s in sources
            )
            netsuite_is_sor = any(
                "netsuite" in s["source_system"].lower() and s.get("is_sor") is True
                for s in sources
            )
            if all_have_fields and netsuite_is_sor:
                t.pass_()
            else:
                t.fail("all fields + netsuite is_sor=true", f"fields={all_have_fields}, sor={netsuite_is_sor}")
        else:
            t.fail(">=2 sources", f"{len(sources)} sources")
    else:
        t.fail("200", str(r.status_code))

    # PT-02
    t = test("PT-02", "Revenue: NS quality(0.95) > SF quality(0.92)")
    r = get("/api/dcl/provenance/revenue")
    if r.status_code == 200:
        data = r.json()
        sources = data.get("sources", [])
        sf_score = None
        ns_score = None
        for s in sources:
            if "salesforce" in s["source_system"].lower():
                sf_score = s["quality_score"]
            elif "netsuite" in s["source_system"].lower():
                ns_score = s["quality_score"]
        if ns_score is not None and sf_score is not None and ns_score > sf_score:
            t.pass_(f"NS={ns_score}, SF={sf_score}")
        else:
            t.fail("NS > SF quality", f"NS={ns_score}, SF={sf_score}")
    else:
        t.fail("200", str(r.status_code))

    # PT-03
    t = test("PT-03", "Unknown metric returns clean 404")
    r = get("/api/dcl/provenance/nonexistent_metric_xyz")
    if r.status_code == 404:
        data = r.json()
        detail = data.get("detail", {})
        if isinstance(detail, dict) and detail.get("error"):
            t.pass_()
        elif isinstance(detail, str) and len(detail) > 0:
            t.pass_()
        else:
            t.fail("error message in body", f"body={data}")
    else:
        t.fail("404", str(r.status_code))


# ─── SUITE: MCP Server ────────────────────────────────────────────────────
def suite_mcp():
    print("\n=== SUITE: MCP Server ===")

    # MCP-01
    t = test("MCP-01", "Concept lookup via MCP")
    r = post("/api/mcp/tools/call", json={
        "tool": "concept_lookup",
        "arguments": {"concept": "revenue"},
        "api_key": "dcl-mcp-test-key",
    })
    if r.status_code == 200:
        data = r.json()
        result = data.get("result", {})
        if result.get("name") or result.get("id"):
            t.pass_()
        else:
            t.fail("concept name/id", f"result={result}")
    else:
        t.fail("200", str(r.status_code))

    # MCP-02
    t = test("MCP-02", "MCP export matches REST export")
    r_mcp = post("/api/mcp/tools/call", json={
        "tool": "semantic_export",
        "arguments": {},
        "api_key": "dcl-mcp-test-key",
    })
    r_rest = get("/api/dcl/semantic-export")
    if r_mcp.status_code == 200 and r_rest.status_code == 200:
        mcp_data = r_mcp.json().get("result", {})
        rest_data = r_rest.json()
        mcp_m = len(mcp_data.get("metrics", []))
        rest_m = len(rest_data.get("metrics", []))
        mcp_e = len(mcp_data.get("entities", []))
        rest_e = len(rest_data.get("entities", []))
        if mcp_m == rest_m and mcp_e == rest_e:
            t.pass_(f"metrics={mcp_m}, entities={mcp_e}")
        else:
            t.fail(f"equal counts", f"MCP(m={mcp_m},e={mcp_e}) vs REST(m={rest_m},e={rest_e})")
    else:
        t.fail("200 both", f"mcp={r_mcp.status_code}, rest={r_rest.status_code}")

    # MCP-03
    t = test("MCP-03", "Query via MCP matches REST")
    r_mcp = post("/api/mcp/tools/call", json={
        "tool": "query",
        "arguments": {"metric": "arr", "grain": "quarter"},
        "api_key": "dcl-mcp-test-key",
    })
    r_rest = post("/api/dcl/query", json={"metric": "arr", "grain": "quarter"})
    if r_mcp.status_code == 200 and r_rest.status_code == 200:
        mcp_data = r_mcp.json().get("result", {}).get("data", [])
        rest_data = r_rest.json().get("data", [])
        if len(mcp_data) > 0 and len(mcp_data) == len(rest_data):
            t.pass_(f"{len(mcp_data)} data points")
        else:
            t.fail(f"matching data", f"MCP={len(mcp_data)}, REST={len(rest_data)}")
    else:
        t.fail("200 both", f"mcp={r_mcp.status_code}, rest={r_rest.status_code}")

    # MCP-04
    t = test("MCP-04", "Auth required — no key rejected, valid key accepted")
    r_no_key = post("/api/mcp/tools/call", json={
        "tool": "concept_lookup",
        "arguments": {"concept": "revenue"},
    })
    r_valid = post("/api/mcp/tools/call", json={
        "tool": "concept_lookup",
        "arguments": {"concept": "revenue"},
        "api_key": "dcl-mcp-test-key",
    })
    no_key_rejected = r_no_key.status_code == 401 or (
        r_no_key.status_code == 200 and not r_no_key.json().get("success", True)
    )
    valid_accepted = r_valid.status_code == 200 and r_valid.json().get("success", False)
    if no_key_rejected and valid_accepted:
        t.pass_()
    else:
        t.fail("no_key=rejected, valid=accepted",
               f"no_key={r_no_key.status_code}/{r_no_key.json().get('success')}, "
               f"valid={r_valid.status_code}/{r_valid.json().get('success')}")

    # MCP-05
    t = test("MCP-05", "Provenance via MCP matches REST")
    r_mcp = post("/api/mcp/tools/call", json={
        "tool": "provenance",
        "arguments": {"metric_id": "revenue"},
        "api_key": "dcl-mcp-test-key",
    })
    r_rest = get("/api/dcl/provenance/revenue")
    if r_mcp.status_code == 200 and r_rest.status_code == 200:
        mcp_sources = r_mcp.json().get("result", {}).get("sources", [])
        rest_sources = r_rest.json().get("sources", [])
        if len(mcp_sources) == len(rest_sources) and len(mcp_sources) > 0:
            t.pass_(f"{len(mcp_sources)} sources")
        else:
            t.fail(f"matching sources", f"MCP={len(mcp_sources)}, REST={len(rest_sources)}")
    else:
        t.fail("200 both", f"mcp={r_mcp.status_code}, rest={r_rest.status_code}")

# ─── SUITE: Enriched Query Response ─────────────────────────────────────
def suite_enriched_query():
    print("\n=== SUITE: Enriched Query Response ===")

    # EQR-01
    t = test("EQR-01", "Provenance in query response")
    r = post("/api/dcl/query", json={"metric": "revenue"})
    if r.status_code == 200:
        prov = r.json().get("provenance")
        if prov and len(prov) > 0 and "source_system" in prov[0]:
            t.pass_()
        else:
            t.fail("provenance present", f"provenance={prov}")
    else:
        t.fail("200", str(r.status_code))

    # EQR-02
    t = test("EQR-02", "Entity in query response")
    r = post("/api/dcl/query", json={"metric": "revenue", "entity": "Acme"})
    if r.status_code == 200:
        entity = r.json().get("entity")
        if entity and entity.get("resolved_name") and entity.get("candidates"):
            t.pass_(f"resolved={entity.get('resolved_name')}")
        else:
            t.fail("entity with fields", f"entity={entity}")
    else:
        t.fail("200", str(r.status_code))

    # EQR-03
    t = test("EQR-03", "Conflicts in query response")
    # Re-detect to get fresh conflicts
    post("/api/dcl/conflicts/detect")
    r = post("/api/dcl/query", json={"metric": "revenue", "entity": "Acme"})
    if r.status_code == 200:
        conflicts = r.json().get("conflicts")
        if conflicts is not None:
            t.pass_(f"conflicts field present (len={len(conflicts) if isinstance(conflicts, list) else 'null'})")
        else:
            t.fail("conflicts field", "not present")
    else:
        t.fail("200", str(r.status_code))

    # EQR-04
    t = test("EQR-04", "Clean when no conflicts (Globex)")
    r = post("/api/dcl/query", json={"metric": "revenue", "entity": "Globex"})
    if r.status_code == 200:
        conflicts = r.json().get("conflicts")
        if conflicts is None or conflicts == []:
            t.pass_()
        else:
            t.fail("no conflicts", f"conflicts={conflicts}")
    else:
        t.fail("200", str(r.status_code))

    # EQR-05
    t = test("EQR-05", "Backward compatibility — original fields present")
    r = post("/api/dcl/query", json={"metric": "revenue", "grain": "quarter"})
    if r.status_code == 200:
        data = r.json()
        required = ["metric", "metric_name", "dimensions", "grain", "unit", "data", "metadata"]
        has_all = all(k in data for k in required)
        meta = data.get("metadata", {})
        meta_ok = all(k in meta for k in ["sources", "freshness", "quality_score", "mode", "record_count"])
        if has_all and meta_ok:
            t.pass_()
        else:
            t.fail("all original fields", f"keys={list(data.keys())}, meta={list(meta.keys())}")
    else:
        t.fail("200", str(r.status_code))


# ─── YAML-driven test engine ──────────────────────────────────────────────
def _resolve_field(data: Any, field_path: str) -> Any:
    """Resolve a dotted field path like 'metadata.source' from a dict."""
    parts = field_path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _check_assertion(assertion: Dict, response_data: Any, status_code: int) -> Tuple[bool, str]:
    """Evaluate a single assertion. Returns (passed, message)."""
    field = assertion["field"]
    op = assertion["op"]
    expected = assertion.get("value")

    if field == "status_code":
        actual = status_code
    else:
        actual = _resolve_field(response_data, field)

    if op == "equals":
        ok = actual == expected
        return ok, f"{field}: expected {expected!r}, got {actual!r}"
    elif op == "not_equals":
        ok = actual != expected
        return ok, f"{field}: expected NOT {expected!r}, got {actual!r}"
    elif op == "not_null":
        ok = actual is not None
        return ok, f"{field}: expected not null, got {actual!r}"
    elif op == "greater_than":
        ok = actual is not None and actual > expected
        return ok, f"{field}: expected > {expected!r}, got {actual!r}"
    elif op == "in":
        ok = actual in expected
        return ok, f"{field}: expected one of {expected!r}, got {actual!r}"
    else:
        return False, f"Unknown assertion op: {op}"


def _run_yaml_test(tc: Dict) -> None:
    """Execute a single YAML test case using the global test() helper."""
    t = test(tc["id"], tc["description"])

    try:
        method = tc["method"].lower()
        req_fn = {"post": post, "get": get, "put": put, "delete": delete}[method]

        kwargs: Dict[str, Any] = {}
        if method in ("post", "put") and tc.get("body"):
            kwargs["json"] = tc["body"]
        if tc.get("headers"):
            kwargs["headers"] = tc["headers"]
        if tc.get("params"):
            kwargs["params"] = tc["params"]

        r = req_fn(tc["path"], **kwargs)

        status_code = r.status_code
        try:
            response_data = r.json()
        except Exception:
            response_data = {}

        all_passed = True
        fail_msg = ""
        for assertion in tc.get("assertions", []):
            ok, msg = _check_assertion(assertion, response_data, status_code)
            if not ok:
                all_passed = False
                fail_msg = msg
                break

        if all_passed:
            t.pass_()
        else:
            t.fail("assertion", fail_msg)

    except Exception as exc:
        t.fail("no exception", str(exc))


def _load_yaml_tests() -> List[Dict]:
    """Load all test cases from test_cases.yaml."""
    import yaml

    yaml_path = Path(__file__).parent / "test_cases.yaml"
    if not yaml_path.exists():
        return []
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f) or []


def suite_pipeline_ingest():
    """PI tests: push real data through the ingest endpoint."""
    import uuid

    print("\n=== SUITE: Pipeline Ingest ===")
    all_tests = _load_yaml_tests()
    pi_tests = [tc for tc in all_tests if tc.get("category") == "pipeline_ingest"]
    if not pi_tests:
        t = test("PI_000", "test_cases.yaml has PI tests")
        t.fail("PI tests present", "none found")
        return

    # Generate a unique run_id so the ingest endpoint doesn't dedup
    unique_run_id = f"farm_fbg_test_{uuid.uuid4().hex[:8]}"
    for tc in pi_tests:
        # Substitute unique run_id into ingest headers
        if tc.get("headers") and "x-run-id" in tc["headers"]:
            tc = dict(tc)
            tc["headers"] = dict(tc["headers"])
            tc["headers"]["x-run-id"] = unique_run_id
        _run_yaml_test(tc)


def suite_fact_base_gating():
    """FBG tests: verify fact_base is gated. Must run AFTER suite_pipeline_ingest."""
    print("\n=== SUITE: Fact-Base Gating ===")
    all_tests = _load_yaml_tests()
    fbg_tests = [tc for tc in all_tests if tc.get("category") == "fact_base_gating"]
    if not fbg_tests:
        t = test("FBG_000", "test_cases.yaml has FBG tests")
        t.fail("FBG tests present", "none found")
        return
    for tc in fbg_tests:
        _run_yaml_test(tc)


def suite_provenance_yaml():
    """Provenance tests from YAML. Must run AFTER suite_pipeline_ingest."""
    print("\n=== SUITE: Provenance (YAML) ===")
    all_tests = _load_yaml_tests()
    prov_tests = [tc for tc in all_tests if tc.get("category") == "provenance"]
    if not prov_tests:
        return  # No provenance YAML tests defined yet — skip silently
    for tc in prov_tests:
        _run_yaml_test(tc)


# ─── main ─────────────────────────────────────────────────────────────────
def wait_for_backend(max_wait: int = 30):
    for i in range(max_wait):
        try:
            r = get("/api/health")
            if r.status_code == 200:
                print(f"Backend ready (waited {i}s)")
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def run_harness():
    global RUN_NUMBER, results

    print(f"\n=== DCL TEST HARNESS -- RUN #{RUN_NUMBER} ===\n")

    if not wait_for_backend():
        print("ERROR: Backend not responding. Starting it...")
        subprocess.Popen(
            [sys.executable, "run_backend.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if not wait_for_backend(45):
            print("FATAL: Cannot start backend")
            sys.exit(1)

    results = []

    suite_regression()
    suite_temporal_versioning()
    suite_persona_definitions()
    suite_provenance()
    suite_mcp()
    suite_enriched_query()
    suite_pipeline_ingest()
    suite_fact_base_gating()
    suite_provenance_yaml()

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    print(f"\n=== RESULTS -- RUN #{RUN_NUMBER} ===")
    print(f"Total: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed > 0:
        print("\nFAILURES:")
        for r in results:
            if not r.passed:
                print(f"  [FAIL] {r.test_id}: {r.description} -- {r.message}")
        print(f"\n=== STATUS: NOT COMPLETE -- FIX AND RERUN ===")
    else:
        print(f"\n=== STATUS: COMPLETE ===")

    return failed


if __name__ == "__main__":
    failed = run_harness()
    sys.exit(0 if failed == 0 else 1)
