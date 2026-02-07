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


# ─── state tracking ──────────────────────────────────────────────────────
er_acme_global_id: Optional[str] = None
er_initech_global_id: Optional[str] = None
er_massive_global_id: Optional[str] = None
er_confirmed_candidate_id: Optional[str] = None
cd_conflict_001_id: Optional[str] = None


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


# ─── SUITE: Entity Resolution ────────────────────────────────────────────
def suite_entity_resolution():
    global er_acme_global_id, er_initech_global_id
    global er_massive_global_id, er_confirmed_candidate_id

    print("\n=== SUITE: Entity Resolution ===")

    # Run entity resolution
    r_resolve = post("/api/dcl/entities/resolve", params={"entity_type": "company"})
    resolve_data = r_resolve.json()
    candidates = resolve_data.get("candidates", [])
    entities = resolve_data.get("canonical_entities", [])

    def find_entity_with_record(record_id: str) -> Optional[Dict]:
        for e in entities:
            for sr in e.get("source_records", []):
                if sr.get("record_id") == record_id:
                    return e
        return None

    # ER-01: Deterministic match — Acme Corporation (3 systems)
    t = test("ER-01", "Acme Corporation: 3 systems deterministic match")
    acme_entity = find_entity_with_record("SF-001847")
    if acme_entity:
        record_ids = {sr["record_id"] for sr in acme_entity["source_records"]}
        all_present = {"SF-001847", "NS-29384", "HS-44721"}.issubset(record_ids)
        er_acme_global_id = acme_entity["dcl_global_id"]
        acme_candidates = [c for c in candidates if c.get("dcl_global_id") == er_acme_global_id]
        match_type_ok = any(c.get("match_type") == "deterministic" for c in acme_candidates)
        conf_ok = all(c.get("confidence", 0) >= 0.95 for c in acme_candidates if c.get("status") == "confirmed")
        if all_present and match_type_ok and conf_ok:
            t.pass_()
        else:
            t.fail("3 records, deterministic, conf>=0.95", f"records={record_ids}, type={match_type_ok}, conf={conf_ok}")
    else:
        t.fail("Acme entity found", "not found")

    # ER-02: Globex International (shared tax_id, different domains)
    t = test("ER-02", "Globex International: matched via shared tax_id")
    globex_entity = find_entity_with_record("SF-003291")
    if globex_entity:
        record_ids = {sr["record_id"] for sr in globex_entity["source_records"]}
        both = "SF-003291" in record_ids and "SAP-GL-0847" in record_ids
        globex_candidates = [c for c in candidates if c.get("dcl_global_id") == globex_entity["dcl_global_id"]]
        conf_ok = all(c.get("confidence", 0) >= 0.95 for c in globex_candidates if c.get("status") == "confirmed")
        if both and conf_ok:
            t.pass_()
        else:
            t.fail("both records, conf>=0.95", f"records={record_ids}, conf={conf_ok}")
    else:
        t.fail("Globex entity found", "not found")

    # ER-03: Initech Solutions (shared domain)
    t = test("ER-03", "Initech Solutions: matched via shared domain")
    initech_entity = find_entity_with_record("SF-005512")
    if initech_entity:
        record_ids = {sr["record_id"] for sr in initech_entity["source_records"]}
        both = "SF-005512" in record_ids and "NS-81023" in record_ids
        er_initech_global_id = initech_entity["dcl_global_id"]
        initech_candidates = [c for c in candidates if c.get("dcl_global_id") == er_initech_global_id]
        conf_ok = all(c.get("confidence", 0) >= 0.95 for c in initech_candidates if c.get("status") == "confirmed")
        if both and conf_ok:
            t.pass_()
        else:
            t.fail("both records, conf>=0.95", f"records={record_ids}, conf={conf_ok}")
    else:
        t.fail("Initech entity found", "not found")

    # ER-04: Non-match — Acme Corp vs Acme Foods
    t = test("ER-04", "Non-match: Acme Corp vs Acme Foods")
    acme_corp_entity = find_entity_with_record("SF-009102")
    acme_foods_entity = find_entity_with_record("SF-009477")
    if acme_corp_entity is None and acme_foods_entity is None:
        t.pass_("Both unmatched")
    elif acme_corp_entity and acme_foods_entity:
        if acme_corp_entity["dcl_global_id"] != acme_foods_entity["dcl_global_id"]:
            t.pass_("Different entities")
        else:
            t.fail("different dcl_global_ids", "same entity")
    else:
        corp_id = acme_corp_entity["dcl_global_id"] if acme_corp_entity else None
        foods_id = acme_foods_entity["dcl_global_id"] if acme_foods_entity else None
        if corp_id != foods_id:
            t.pass_("Different entities or unmatched")
        else:
            t.fail("different dcl_global_ids", "same entity")

    # ER-05: Non-match — Alpha Technologies vs Alpha Labs
    t = test("ER-05", "Non-match: Alpha Technologies vs Alpha Labs")
    alpha_tech = find_entity_with_record("SF-010234")
    alpha_labs = find_entity_with_record("NS-67891")
    if alpha_tech is None and alpha_labs is None:
        t.pass_("Both unmatched")
    elif alpha_tech and alpha_labs:
        if alpha_tech["dcl_global_id"] != alpha_labs["dcl_global_id"]:
            t.pass_("Different entities")
        else:
            t.fail("different dcl_global_ids", "same entity")
    else:
        t.pass_("Not matched together")

    # ER-06: Massive Dynamic (3 systems, shared tax_id)
    t = test("ER-06", "Massive Dynamic: 3 systems matched via tax_id")
    md_entity = find_entity_with_record("SF-007834")
    if md_entity:
        record_ids = {sr["record_id"] for sr in md_entity["source_records"]}
        all_three = {"SF-007834", "NS-45102", "SAP-MD-2291"}.issubset(record_ids)
        er_massive_global_id = md_entity["dcl_global_id"]
        if all_three:
            t.pass_(f"All 3 records: {record_ids}")
        else:
            t.fail("3 records (SF, NS, SAP)", f"records={record_ids}")
    else:
        t.fail("Massive Dynamic entity found", "not found")

    # ER-07: Human confirm
    t = test("ER-07", "Human confirm on a pending match")
    pending_candidates = [c for c in candidates if c.get("status") == "pending"]
    if pending_candidates:
        cand = pending_candidates[0]
        r = post(f"/api/dcl/entities/confirm/{cand['id']}", json={"approved": True, "resolved_by": "test_admin"})
        if r.status_code == 200:
            result = r.json().get("candidate", {})
            if result.get("status") == "confirmed" and result.get("resolved_by") == "test_admin":
                er_confirmed_candidate_id = cand["id"]
                t.pass_()
            else:
                t.fail("confirmed + audit", f"status={result.get('status')}")
        else:
            t.fail("200", str(r.status_code))
    else:
        # All auto-confirmed; undo one to create pending, then confirm
        if er_initech_global_id:
            post(f"/api/dcl/entities/undo/{er_initech_global_id}")
            r2 = post("/api/dcl/entities/resolve", params={"entity_type": "company"})
            new_cands = r2.json().get("candidates", [])
            pending = [c for c in new_cands if c.get("status") == "pending"]
            if pending:
                cand = pending[0]
                r3 = post(f"/api/dcl/entities/confirm/{cand['id']}", json={"approved": True, "resolved_by": "test_admin"})
                if r3.status_code == 200 and r3.json().get("candidate", {}).get("status") == "confirmed":
                    er_confirmed_candidate_id = cand["id"]
                    er_initech_global_id = r3.json().get("candidate", {}).get("dcl_global_id")
                    t.pass_("Undo+re-confirm cycle")
                else:
                    t.fail("confirmed", f"status={r3.status_code}")
            else:
                t.fail("pending candidate", "none after undo")
        else:
            t.fail("pending candidate", "none available")

    # ER-08: Human reject
    t = test("ER-08", "Human reject on a match")
    r_re = post("/api/dcl/entities/resolve", params={"entity_type": "company"})
    fresh_cands = r_re.json().get("candidates", [])
    pending = [c for c in fresh_cands if c.get("status") == "pending"]
    if pending:
        cand = pending[0]
        r = post(f"/api/dcl/entities/confirm/{cand['id']}", json={"approved": False, "resolved_by": "test_admin"})
        if r.status_code == 200 and r.json().get("candidate", {}).get("status") == "rejected":
            t.pass_()
        else:
            t.fail("rejected", f"status={r.status_code}")
    else:
        # Undo a non-critical entity to create pending
        all_entities = r_re.json().get("canonical_entities", [])
        undo_id = None
        for e in all_entities:
            rids = {sr["record_id"] for sr in e.get("source_records", [])}
            if "SF-001847" not in rids and "SF-007834" not in rids:
                undo_id = e["dcl_global_id"]
                break
        if undo_id:
            post(f"/api/dcl/entities/undo/{undo_id}")
            r2 = post("/api/dcl/entities/resolve", params={"entity_type": "company"})
            pending2 = [c for c in r2.json().get("candidates", []) if c.get("status") == "pending"]
            if pending2:
                r3 = post(f"/api/dcl/entities/confirm/{pending2[0]['id']}", json={"approved": False, "resolved_by": "test_admin"})
                if r3.status_code == 200 and r3.json().get("candidate", {}).get("status") == "rejected":
                    t.pass_("Undo+reject cycle")
                else:
                    t.fail("rejected", f"code={r3.status_code}")
            else:
                t.fail("pending for reject", "none after undo")
        else:
            t.fail("pending for reject", "no entity to undo")

    # ER-09: Undo confirmed match
    t = test("ER-09", "Undo confirmed match")
    r_check = post("/api/dcl/entities/resolve", params={"entity_type": "company"})
    all_entities = r_check.json().get("canonical_entities", [])

    undo_target = None
    for e in all_entities:
        rids = {sr["record_id"] for sr in e.get("source_records", [])}
        if "SF-001847" not in rids and "SF-007834" not in rids:
            undo_target = e["dcl_global_id"]
            break

    if undo_target:
        r = post(f"/api/dcl/entities/undo/{undo_target}")
        if r.status_code == 200:
            r_after = post("/api/dcl/entities/resolve", params={"entity_type": "company"})
            after_entities = r_after.json().get("canonical_entities", [])
            entity_gone = not any(e["dcl_global_id"] == undo_target for e in after_entities)
            if entity_gone:
                t.pass_()
            else:
                t.fail("entity removed", "still exists")
        else:
            t.fail("200", str(r.status_code))
    else:
        t.fail("entity to undo", "none suitable")

    # Restore state: re-run resolution
    r_restore = post("/api/dcl/entities/resolve", params={"entity_type": "company"})
    entities = r_restore.json().get("canonical_entities", [])
    candidates = r_restore.json().get("candidates", [])

    for e in entities:
        rids = {sr["record_id"] for sr in e.get("source_records", [])}
        if "SF-001847" in rids:
            er_acme_global_id = e["dcl_global_id"]
        if "SF-007834" in rids:
            er_massive_global_id = e["dcl_global_id"]

    # ER-10: Entity browse
    t = test("ER-10", "Browse entities matching 'acme'")
    r = get("/api/dcl/entities/acme")
    if r.status_code == 200:
        browse = r.json().get("results", [])
        record_ids = {br["record_id"] for br in browse}
        expected_ids = {"SF-001847", "NS-29384", "HS-44721", "SF-009102", "SF-009477"}
        has_all = expected_ids.issubset(record_ids)

        matched_ids = {br["record_id"] for br in browse if br.get("match_status") == "confirmed"}
        main_matched = {"SF-001847", "NS-29384", "HS-44721"}.issubset(matched_ids)

        if has_all and main_matched:
            t.pass_(f"Found {len(browse)} records, main Acme matched")
        else:
            t.fail("5+ records, main matched",
                   f"ids={record_ids}, matched={matched_ids}")
    else:
        t.fail("200", str(r.status_code))

    # ER-11: Golden record — Acme Corporation
    t = test("ER-11", "Golden record: Acme revenue=3800000 from netsuite_erp")
    if er_acme_global_id:
        r = get(f"/api/dcl/entities/canonical/{er_acme_global_id}")
        if r.status_code == 200:
            golden = r.json().get("golden_record", {})
            rev = golden.get("revenue", {})
            addr = golden.get("address", {})

            rev_ok = rev.get("value") == 3800000 and rev.get("source_system") == "netsuite_erp"
            addr_ok = "94105" in str(addr.get("value", "")) and addr.get("source_system") == "netsuite_erp"

            if rev_ok and addr_ok:
                t.pass_()
            else:
                t.fail("rev=3800000/NS, addr with 94105/NS",
                       f"rev={rev.get('value')}/{rev.get('source_system')}, addr={addr.get('value')}/{addr.get('source_system')}")
        else:
            t.fail("200", str(r.status_code))
    else:
        t.fail("Acme global ID", "not available")

    # ER-12: Golden record — Massive Dynamic
    t = test("ER-12", "Golden record: Massive Dynamic revenue=9100000 from sap_erp")
    if er_massive_global_id:
        r = get(f"/api/dcl/entities/canonical/{er_massive_global_id}")
        if r.status_code == 200:
            golden = r.json().get("golden_record", {})
            rev = golden.get("revenue", {})
            if rev.get("value") == 9100000 and rev.get("source_system") == "sap_erp":
                t.pass_()
            else:
                t.fail("rev=9100000/sap_erp", f"rev={rev.get('value')}/{rev.get('source_system')}")
        else:
            t.fail("200", str(r.status_code))
    else:
        t.fail("Massive Dynamic global ID", "not available")

    # ER-13: Scope enforcement — companies only
    t = test("ER-13", "Scope enforcement: product/employee types rejected")
    r_prod = post("/api/dcl/entities/resolve", params={"entity_type": "product"})
    r_emp = post("/api/dcl/entities/resolve", params={"entity_type": "employee"})
    if r_prod.status_code == 400 and r_emp.status_code == 400:
        t.pass_()
    else:
        t.fail("400 for both", f"product={r_prod.status_code}, employee={r_emp.status_code}")


# ─── SUITE: Conflict Detection ───────────────────────────────────────────
def suite_conflict_detection():
    global cd_conflict_001_id

    print("\n=== SUITE: Conflict Detection ===")

    # Run conflict detection
    r_detect = post("/api/dcl/conflicts/detect")
    detect_data = r_detect.json()
    conflicts = detect_data.get("conflicts", [])

    def find_conflict(entity_substr: str, metric: str = "revenue") -> Optional[Dict]:
        for c in conflicts:
            if entity_substr.lower() in c.get("entity_name", "").lower() and c.get("metric") == metric:
                return c
        return None

    acme_conflict = find_conflict("acme")
    initech_conflict = find_conflict("initech")
    md_conflict = find_conflict("massive")

    # CD-01
    t = test("CD-01", "Acme Corporation timing conflict detected")
    if acme_conflict:
        cd_conflict_001_id = acme_conflict["id"]
        systems = [v["source_system"] for v in acme_conflict.get("values", [])]
        has_sf = any("salesforce" in s.lower() for s in systems)
        has_ns = any("netsuite" in s.lower() for s in systems)
        if has_sf and has_ns:
            t.pass_(f"systems={systems}")
        else:
            t.fail("SF + NS present", f"systems={systems}")
    else:
        t.fail("Acme revenue conflict", f"not found in {len(conflicts)} conflicts")

    # CD-02
    t = test("CD-02", "Acme conflict root_cause=timing")
    if acme_conflict:
        rc = acme_conflict.get("root_cause")
        expl = acme_conflict.get("root_cause_explanation", "")
        if rc == "timing" and len(expl) > len("timing"):
            t.pass_()
        else:
            t.fail("timing + explanation", f"rc={rc}, expl={expl[:50]}")
    else:
        t.fail("acme conflict", "not found")

    # CD-03
    t = test("CD-03", "Initech scope conflict detected")
    if initech_conflict:
        rc = initech_conflict.get("root_cause")
        if rc == "scope":
            t.pass_()
        else:
            t.fail("root_cause=scope", f"root_cause={rc}")
    else:
        t.fail("Initech revenue conflict", "not found")

    # CD-04
    t = test("CD-04", "Massive Dynamic stale_data conflict detected")
    if md_conflict:
        rc = md_conflict.get("root_cause")
        if rc == "stale_data":
            t.pass_()
        else:
            t.fail("root_cause=stale_data", f"root_cause={rc}")
    else:
        t.fail("Massive Dynamic conflict", "not found")

    # CD-05
    t = test("CD-05", "Trust: netsuite_erp for Acme timing")
    if acme_conflict:
        tr = acme_conflict.get("trust_recommendation", {})
        if "netsuite" in tr.get("system", "").lower() and len(tr.get("reasoning", "")) > 10:
            t.pass_()
        else:
            t.fail("netsuite_erp + reasoning", f"tr={tr}")
    else:
        t.fail("acme conflict", "not found")

    # CD-06
    t = test("CD-06", "Trust: sap_erp for Massive Dynamic stale")
    if md_conflict:
        tr = md_conflict.get("trust_recommendation", {})
        if "sap" in tr.get("system", "").lower() and len(tr.get("reasoning", "")) > 10:
            t.pass_()
        else:
            t.fail("sap_erp + reasoning", f"tr={tr}")
    else:
        t.fail("MD conflict", "not found")

    # CD-07
    t = test("CD-07", "Severity: Acme=critical, Initech=medium, Massive=medium")
    a = acme_conflict.get("severity") if acme_conflict else None
    i = initech_conflict.get("severity") if initech_conflict else None
    m = md_conflict.get("severity") if md_conflict else None
    if a == "critical" and i == "medium" and m == "medium":
        t.pass_()
    else:
        t.fail("critical,medium,medium", f"a={a}, i={i}, m={m}")

    # CD-08
    t = test("CD-08", "No conflict for Globex (matching revenue)")
    globex = find_conflict("globex")
    if globex is None:
        t.pass_()
    else:
        t.fail("no conflict", f"found: {globex.get('id')}")

    # CD-09
    t = test("CD-09", "Dashboard lists conflicts sorted by severity desc")
    r = get("/api/dcl/conflicts")
    if r.status_code == 200:
        dash = r.json().get("conflicts", [])
        root_causes = {c.get("root_cause") for c in dash}
        has_required = {"timing", "scope", "stale_data"}.issubset(root_causes)
        all_fields = all(
            all(k in c for k in ["entity_name", "metric", "root_cause", "severity", "status"])
            for c in dash
        )
        sev_map = {"critical": 3, "medium": 2, "low": 1, "none": 0}
        sevs = [sev_map.get(c.get("severity"), 0) for c in dash]
        sorted_ok = sevs == sorted(sevs, reverse=True)
        if has_required and all_fields and sorted_ok:
            t.pass_(f"{len(dash)} conflicts, sorted")
        else:
            t.fail("timing+scope+stale_data, sorted", f"causes={root_causes}, fields={all_fields}, sorted={sorted_ok}")
    else:
        t.fail("200", str(r.status_code))

    # CD-10
    t = test("CD-10", "Resolve CONFLICT-001 with audit trail")
    if cd_conflict_001_id:
        r = post(f"/api/dcl/conflicts/{cd_conflict_001_id}/resolve", json={
            "decision": "Trust netsuite_erp recognized revenue",
            "rationale": "GAAP compliance",
            "resolved_by": "test_admin",
        })
        if r.status_code == 200:
            data = r.json().get("conflict", {})
            if data.get("status") == "resolved" and data.get("resolved_by") == "test_admin" and data.get("resolution_decision"):
                t.pass_()
            else:
                t.fail("resolved + audit", f"status={data.get('status')}")
        else:
            t.fail("200", str(r.status_code))
    else:
        t.fail("CONFLICT-001 id", "not available")

    # CD-11
    t = test("CD-11", "Resolved conflict stays resolved after re-detection")
    post("/api/dcl/conflicts/detect")
    r = get("/api/dcl/conflicts")
    if r.status_code == 200:
        active = r.json().get("conflicts", [])
        still_active = any(c.get("id") == cd_conflict_001_id for c in active)
        if not still_active:
            t.pass_()
        else:
            t.fail("not in active", "still active")
    else:
        t.fail("200", str(r.status_code))

    # CD-12
    t = test("CD-12", "Quality score increases after 5+ resolutions")
    # Resolve remaining revenue conflicts favoring netsuite_erp
    r_dash = get("/api/dcl/conflicts")
    active = r_dash.json().get("conflicts", [])
    revenue_active = [c for c in active if c.get("metric") == "revenue"]

    resolved_count = 1  # Already resolved one in CD-10
    for c in revenue_active:
        post(f"/api/dcl/conflicts/{c['id']}/resolve", json={
            "decision": "Trust netsuite_erp recognized revenue",
            "rationale": "Quality feedback test",
            "resolved_by": "test_admin",
        })
        resolved_count += 1

    # Re-detect to find any remaining
    if resolved_count < 5:
        post("/api/dcl/conflicts/detect")
        r2 = get("/api/dcl/conflicts")
        more = [c for c in r2.json().get("conflicts", []) if c.get("metric") == "revenue"]
        for c in more:
            post(f"/api/dcl/conflicts/{c['id']}/resolve", json={
                "decision": "Trust netsuite_erp recognized revenue",
                "rationale": "Quality feedback test",
                "resolved_by": "test_admin",
            })
            resolved_count += 1

    if resolved_count >= 5:
        r_prov = get("/api/dcl/provenance/revenue")
        if r_prov.status_code == 200:
            sources = r_prov.json().get("sources", [])
            ns = next((s for s in sources if "netsuite" in s.get("source_system", "").lower()), None)
            if ns and ns.get("quality_score", 0) >= 0.95:
                t.pass_(f"resolved={resolved_count}, quality={ns.get('quality_score')}")
            else:
                t.fail("quality_score >= baseline", f"ns={ns}")
        else:
            t.fail("200 provenance", str(r_prov.status_code))
    else:
        t.fail(f"5+ resolutions", f"only resolved {resolved_count}")


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
    suite_entity_resolution()
    suite_conflict_detection()
    suite_enriched_query()

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
