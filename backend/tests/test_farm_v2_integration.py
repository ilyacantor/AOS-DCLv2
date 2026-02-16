"""
Self-running test harness for Farm v2 → DCL integration.

Tests the full pipeline:
1. Simulate Farm pushing 20 pipe payloads to DCL ingest
2. Verify IngestStore receives all 20 pipes
3. Verify ingest bridge builds 8 source systems from 20 pipes
4. Verify DCL engine builds graph from ingested Farm data
5. Verify ontology mapping works for all 8 source types
6. Verify persona filtering includes new concepts

This test does NOT depend on Farm being reachable — it injects
synthetic pipe data directly into DCL's ingest endpoint contract.
"""

import sys
import os
import json
import time
import uuid
from typing import Dict, List, Any

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backend.api.ingest import get_ingest_store, IngestRequest
from backend.farm.ingest_bridge import (
    build_sources_from_ingest,
    get_ingest_summary,
    PIPE_SOURCE_MAP,
)
from backend.domain import Persona


# =============================================================================
# Synthetic pipe data generators (mimics what Farm v2 pushes)
# =============================================================================

def _make_sf_users(n: int = 42) -> List[Dict[str, Any]]:
    return [
        {
            "user_id": f"USR-{i:04d}",
            "name": f"User {i}",
            "email": f"user{i}@corp.com",
            "role": "AE" if i % 3 == 0 else "SDR",
            "is_active": True,
            "created_at": "2023-01-15T10:00:00Z",
        }
        for i in range(n)
    ]


def _make_sf_accounts(n: int = 50) -> List[Dict[str, Any]]:
    return [
        {
            "account_id": f"ACC-{i:05d}",
            "account_name": f"Acme Corp {i}",
            "industry": ["SaaS", "FinTech", "Healthcare", "Manufacturing"][i % 4],
            "region": ["AMER", "EMEA", "APAC"][i % 3],
            "arr": 50000 + i * 1000,
            "health_score": 70 + (i % 30),
            "owner_id": f"USR-{i % 42:04d}",
            "created_at": "2023-03-01T10:00:00Z",
        }
        for i in range(n)
    ]


def _make_sf_opportunities(n: int = 100) -> List[Dict[str, Any]]:
    return [
        {
            "opportunity_id": f"OPP-{i:05d}",
            "account_id": f"ACC-{i % 50:05d}",
            "opportunity_name": f"Deal {i}",
            "stage": ["Qualification", "Discovery", "Proposal", "Negotiation", "Closed Won"][i % 5],
            "amount": 25000 + i * 500,
            "close_date": f"2024-Q{(i % 4) + 1}",
            "created_at": "2024-01-15T10:00:00Z",
        }
        for i in range(n)
    ]


def _make_ns_invoices(n: int = 80) -> List[Dict[str, Any]]:
    return [
        {
            "invoice_id": f"INV-{i:05d}",
            "customer_id": f"ACC-{i % 50:05d}",
            "amount": 10000 + i * 200,
            "currency": "USD",
            "status": "paid" if i % 3 != 0 else "pending",
            "issue_date": f"2024-0{(i % 9) + 1}-15",
            "due_date": f"2024-0{(i % 9) + 1}-30",
        }
        for i in range(n)
    ]


def _make_ns_gl_entries(n: int = 100) -> List[Dict[str, Any]]:
    return [
        {
            "entry_id": f"GL-{i:06d}",
            "gl_account": f"4{i % 10:03d}",
            "debit": 5000.0 if i % 2 == 0 else 0.0,
            "credit": 0.0 if i % 2 == 0 else 5000.0,
            "description": f"Revenue entry {i}",
            "posting_date": f"2024-0{(i % 9) + 1}-01",
        }
        for i in range(n)
    ]


def _make_ns_rev_schedules(n: int = 40) -> List[Dict[str, Any]]:
    return [
        {
            "schedule_id": f"RS-{i:04d}",
            "invoice_id": f"INV-{i:05d}",
            "amount": 8000 + i * 100,
            "recognition_date": f"2024-0{(i % 9) + 1}-01",
            "status": "recognized",
        }
        for i in range(n)
    ]


def _make_ns_ar(n: int = 30) -> List[Dict[str, Any]]:
    return [
        {
            "ar_id": f"AR-{i:04d}",
            "customer_id": f"ACC-{i % 50:05d}",
            "amount_due": 12000 + i * 300,
            "due_date": f"2024-0{(i % 9) + 1}-30",
            "days_outstanding": 15 + i % 60,
        }
        for i in range(n)
    ]


def _make_ns_ap(n: int = 25) -> List[Dict[str, Any]]:
    return [
        {
            "ap_id": f"AP-{i:04d}",
            "vendor_id": f"VND-{i:03d}",
            "amount": 3000 + i * 100,
            "due_date": f"2024-0{(i % 9) + 1}-28",
            "status": "approved",
        }
        for i in range(n)
    ]


def _make_cb_subscriptions(n: int = 50) -> List[Dict[str, Any]]:
    return [
        {
            "subscription_id": f"SUB-{i:05d}",
            "customer_id": f"ACC-{i % 50:05d}",
            "plan_id": f"plan_{['starter', 'growth', 'enterprise'][i % 3]}",
            "plan_name": ["Starter", "Growth", "Enterprise"][i % 3],
            "mrr": [500, 2000, 10000][i % 3],
            "subscription_status": "active" if i % 5 != 0 else "cancelled",
            "billing_period": "monthly",
            "created_at": "2023-06-01T10:00:00Z",
        }
        for i in range(n)
    ]


def _make_cb_invoices(n: int = 100) -> List[Dict[str, Any]]:
    return [
        {
            "invoice_id": f"CB-INV-{i:06d}",
            "subscription_id": f"SUB-{i % 50:05d}",
            "amount": [500, 2000, 10000][i % 3],
            "currency": "USD",
            "status": "paid",
            "invoice_date": f"2024-0{(i % 9) + 1}-01",
        }
        for i in range(n)
    ]


def _make_wd_workers(n: int = 40) -> List[Dict[str, Any]]:
    return [
        {
            "worker_id": f"WKR-{i:04d}",
            "employee_name": f"Employee {i}",
            "department": ["Engineering", "Sales", "Marketing", "Finance", "Support"][i % 5],
            "position_id": f"POS-{i:04d}",
            "hire_date": "2022-03-15",
            "status": "active",
            "is_contractor": i % 15 == 0,  # ~3 contractors per run
        }
        for i in range(n)
    ]


def _make_wd_positions(n: int = 45) -> List[Dict[str, Any]]:
    return [
        {
            "position_id": f"POS-{i:04d}",
            "title": f"Position {i}",
            "department": ["Engineering", "Sales", "Marketing", "Finance", "Support"][i % 5],
            "level": ["IC1", "IC2", "IC3", "Manager", "Director"][i % 5],
            "is_filled": i % 4 != 0,
        }
        for i in range(n)
    ]


def _make_wd_timeoff(n: int = 60) -> List[Dict[str, Any]]:
    return [
        {
            "request_id": f"TO-{i:05d}",
            "worker_id": f"WKR-{i % 40:04d}",
            "type": ["PTO", "Sick", "Holiday"][i % 3],
            "start_date": f"2024-0{(i % 9) + 1}-10",
            "end_date": f"2024-0{(i % 9) + 1}-12",
            "status": "approved",
        }
        for i in range(n)
    ]


def _make_zendesk_tickets(n: int = 200) -> List[Dict[str, Any]]:
    return [
        {
            "ticket_id": i + 1000,
            "subject": f"Support request {i}",
            "priority": ["low", "normal", "high", "urgent"][i % 4],
            "status": ["new", "open", "pending", "solved"][i % 4],
            "assignee": f"agent_{i % 10}",
            "organization_id": f"ORG-{i % 20:03d}",
            "satisfaction_rating": ["good", "bad", None][i % 3],
            "created_at": f"2024-0{(i % 9) + 1}-{(i % 28) + 1:02d}T10:00:00Z",
        }
        for i in range(n)
    ]


def _make_zendesk_orgs(n: int = 20) -> List[Dict[str, Any]]:
    return [
        {
            "organization_id": f"ORG-{i:03d}",
            "name": f"Customer Org {i}",
            "domain": f"customer{i}.com",
            "tags": ["enterprise"] if i % 3 == 0 else ["startup"],
        }
        for i in range(n)
    ]


def _make_jira_issues(n: int = 80) -> List[Dict[str, Any]]:
    return [
        {
            "issue_key": f"ENG-{i + 100}",
            "issue_id": f"issue_{i}",
            "issue_type": ["Bug", "Story", "Task", "Epic"][i % 4],
            "priority": ["Critical", "High", "Medium", "Low"][i % 4],
            "status": ["To Do", "In Progress", "In Review", "Done"][i % 4],
            "story_points": [1, 2, 3, 5, 8][i % 5],
            "sprint_id": f"SPR-{i % 8:02d}",
            "assignee": f"dev_{i % 10}",
            "created_at": f"2024-0{(i % 9) + 1}-{(i % 28) + 1:02d}",
        }
        for i in range(n)
    ]


def _make_jira_sprints(n: int = 8) -> List[Dict[str, Any]]:
    return [
        {
            "sprint_id": f"SPR-{i:02d}",
            "sprint_name": f"Sprint {i + 1}",
            "state": "closed" if i < 6 else "active",
            "start_date": f"2024-0{i + 1}-01",
            "end_date": f"2024-0{i + 1}-14",
            "velocity": 30 + i * 2,
        }
        for i in range(n)
    ]


def _make_datadog_incidents(n: int = 15) -> List[Dict[str, Any]]:
    return [
        {
            "incident_id": f"INC-{i:04d}",
            "title": f"Incident {i}",
            "severity": ["SEV1", "SEV2", "SEV3", "SEV4"][i % 4],
            "status": "resolved" if i < 10 else "active",
            "service": ["api-gateway", "payment-service", "auth-service"][i % 3],
            "mttr": 30 + i * 10,  # minutes
            "created_at": f"2024-0{(i % 9) + 1}-{(i % 28) + 1:02d}T08:00:00Z",
        }
        for i in range(n)
    ]


def _make_datadog_slos(n: int = 5) -> List[Dict[str, Any]]:
    return [
        {
            "slo_id": f"SLO-{i:03d}",
            "slo_name": f"SLO {['API Availability', 'Latency P99', 'Error Rate', 'Uptime', 'Throughput'][i]}",
            "slo_target": [99.9, 200, 0.1, 99.99, 10000][i],
            "current_value": [99.85, 195, 0.15, 99.95, 11000][i],
            "status": "ok" if i % 2 == 0 else "warning",
        }
        for i in range(n)
    ]


def _make_aws_cost(n: int = 100) -> List[Dict[str, Any]]:
    return [
        {
            "line_item_id": f"AWS-{i:06d}",
            "service": ["EC2", "S3", "RDS", "Lambda", "CloudFront"][i % 5],
            "resource_id": f"arn:aws:{['ec2', 's3', 'rds', 'lambda', 'cloudfront'][i % 5]}:us-east-1:123456:resource-{i}",
            "cost": 10 + i * 0.5,
            "usage_amount": 100 + i * 10,
            "usage_type": "compute" if i % 2 == 0 else "storage",
            "period": f"2024-0{(i % 9) + 1}",
        }
        for i in range(n)
    ]


# Pipe ID → generator function
PIPE_GENERATORS = {
    "sf_users": lambda: _make_sf_users(42),
    "sf_accounts": lambda: _make_sf_accounts(50),
    "sf_opportunities": lambda: _make_sf_opportunities(100),
    "ns-erp-001-invoices": lambda: _make_ns_invoices(80),
    "ns-erp-001-rev-schedules": lambda: _make_ns_rev_schedules(40),
    "ns-erp-001-gl-entries": lambda: _make_ns_gl_entries(100),
    "ns-erp-001-ar": lambda: _make_ns_ar(30),
    "ns-erp-001-ap": lambda: _make_ns_ap(25),
    "cb_main_subscriptions": lambda: _make_cb_subscriptions(50),
    "cb_main_invoices": lambda: _make_cb_invoices(100),
    "wd-workers-001": lambda: _make_wd_workers(40),
    "wd-positions-001": lambda: _make_wd_positions(45),
    "wd-timeoff-001": lambda: _make_wd_timeoff(60),
    "zendesk_tickets": lambda: _make_zendesk_tickets(200),
    "zendesk_organizations": lambda: _make_zendesk_orgs(20),
    "jira_issues": lambda: _make_jira_issues(80),
    "jira_sprints": lambda: _make_jira_sprints(8),
    "datadog_incidents": lambda: _make_datadog_incidents(15),
    "datadog_slos": lambda: _make_datadog_slos(5),
    "aws_cost_line_items": lambda: _make_aws_cost(100),
}


# =============================================================================
# Test harness
# =============================================================================

class FarmV2TestHarness:
    """Self-running test harness for Farm v2 integration."""

    def __init__(self):
        self.results: List[Dict[str, Any]] = []
        self.run_id = str(uuid.uuid4())
        self.passed = 0
        self.failed = 0

    def _assert(self, condition: bool, test_name: str, details: str = ""):
        status = "PASS" if condition else "FAIL"
        self.results.append({
            "test": test_name,
            "status": status,
            "details": details,
        })
        if condition:
            self.passed += 1
        else:
            self.failed += 1
        icon = "+" if condition else "X"
        print(f"  [{icon}] {test_name}" + (f" — {details}" if details else ""))

    def run_all(self) -> Dict[str, Any]:
        """Run all integration tests."""
        print("=" * 70)
        print("Farm v2 → DCL Integration Test Harness")
        print("=" * 70)

        self._test_ingest_all_pipes()
        self._test_ingest_summary()
        self._test_ingest_bridge_sources()
        self._test_dcl_engine_farm_mode()
        self._test_ontology_mapping_coverage()
        self._test_persona_concept_coverage()

        print("\n" + "=" * 70)
        print(f"Results: {self.passed} passed, {self.failed} failed, "
              f"{self.passed + self.failed} total")
        print("=" * 70)

        return {
            "passed": self.passed,
            "failed": self.failed,
            "total": self.passed + self.failed,
            "results": self.results,
        }

    def _test_ingest_all_pipes(self):
        """Test 1: Simulate Farm pushing all 20 pipes to DCL ingest."""
        print("\n[Test 1] Ingest all 20 Farm pipes")

        store = get_ingest_store()
        total_rows = 0

        for pipe_id, generator in PIPE_GENERATORS.items():
            rows = generator()
            pipe_info = PIPE_SOURCE_MAP.get(pipe_id)
            source_system = pipe_info[1] if pipe_info else "unknown"

            req = IngestRequest(
                source_system=source_system,
                tenant_id="farm_v2_test",
                snapshot_name="farm_v2_test_run",
                run_timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                schema_version="2.0",
                row_count=len(rows),
                rows=rows,
            )

            receipt = store.ingest(
                run_id=f"{self.run_id}_{pipe_id}",
                pipe_id=pipe_id,
                schema_hash=f"hash_{pipe_id}",
                request=req,
            )
            total_rows += len(rows)

            self._assert(
                receipt.row_count == len(rows),
                f"Pipe {pipe_id} ingested",
                f"{receipt.row_count} rows"
            )

        self._assert(
            total_rows > 0,
            "Total rows ingested",
            f"{total_rows:,} records across 20 pipes"
        )

    def _test_ingest_summary(self):
        """Test 2: Verify ingest summary shows all 20 pipes from 8 sources."""
        print("\n[Test 2] Ingest summary")

        summary = get_ingest_summary()

        self._assert(
            summary["pipe_count"] >= 20,
            "Pipe count >= 20",
            f"got {summary['pipe_count']}"
        )
        self._assert(
            summary["source_count"] >= 8,
            "Source count >= 8",
            f"got {summary['source_count']}"
        )
        self._assert(
            summary["total_records"] > 500,
            "Total records > 500",
            f"got {summary['total_records']:,}"
        )

        # Verify all 8 source systems are represented
        expected_sources = {
            "Salesforce", "NetSuite", "Chargebee", "Workday",
            "Zendesk", "Jira", "Datadog", "AWS Cost Explorer",
        }
        found_sources = set(summary["sources"].keys())
        for src in expected_sources:
            self._assert(
                src in found_sources,
                f"Source '{src}' present",
                f"found: {src in found_sources}"
            )

    def _test_ingest_bridge_sources(self):
        """Test 3: Verify ingest bridge builds 8 SourceSystem objects."""
        print("\n[Test 3] Ingest bridge → SourceSystem objects")

        sources = build_sources_from_ingest()

        self._assert(
            len(sources) >= 8,
            "8+ source systems built",
            f"got {len(sources)}"
        )

        # Verify each source has tables and fields
        for source in sources:
            self._assert(
                len(source.tables) > 0,
                f"Source '{source.name}' has tables",
                f"{len(source.tables)} tables"
            )

            total_fields = sum(len(t.fields) for t in source.tables)
            self._assert(
                total_fields > 0,
                f"Source '{source.name}' has fields",
                f"{total_fields} fields across {len(source.tables)} tables"
            )

    def _test_dcl_engine_farm_mode(self):
        """Test 4: Verify DCL engine builds graph in Farm mode with ingested data."""
        print("\n[Test 4] DCL engine Farm mode graph build")

        from backend.engine.dcl_engine import DCLEngine

        engine = DCLEngine()
        personas = [Persona.CFO, Persona.CRO, Persona.COO, Persona.CTO]

        snapshot, metrics = engine.build_graph_snapshot(
            mode="Farm",
            run_mode="Dev",
            personas=personas,
            run_id=str(uuid.uuid4()),
            source_limit=1000,
        )

        self._assert(
            len(snapshot.nodes) > 0,
            "Graph has nodes",
            f"{len(snapshot.nodes)} nodes"
        )
        self._assert(
            len(snapshot.links) > 0,
            "Graph has links",
            f"{len(snapshot.links)} links"
        )

        # Verify L0/L1/L2/L3 layers are present
        levels = {n.level for n in snapshot.nodes}
        for level in ["L0", "L1", "L2", "L3"]:
            self._assert(
                level in levels,
                f"Level {level} present",
                f"found: {level in levels}"
            )

        # Verify source count matches
        source_nodes = [n for n in snapshot.nodes if n.kind == "source"]
        self._assert(
            len(source_nodes) >= 8,
            "8+ source nodes in graph",
            f"got {len(source_nodes)}"
        )

        # Verify mappings were created
        self._assert(
            metrics.total_mappings > 0,
            "Mappings created",
            f"{metrics.total_mappings} mappings"
        )

    def _test_ontology_mapping_coverage(self):
        """Test 5: Verify ontology concepts get mappings from Farm v2 sources."""
        print("\n[Test 5] Ontology mapping coverage")

        from backend.engine.ontology import get_ontology
        from backend.semantic_mapper import SemanticMapper

        sources = build_sources_from_ingest()
        mapper = SemanticMapper()
        mappings, stats = mapper.run_mapping(sources, mode="heuristic", clear_existing=False)

        # Collect which concepts got mapped
        mapped_concepts = set()
        for m in mappings:
            mapped_concepts.add(m.ontology_concept)

        # Core concepts that SHOULD map from the 8 source systems
        expected_concepts = [
            "account",      # SF accounts
            "opportunity",  # SF opportunities
            "revenue",      # NS invoices, CB invoices, amounts
            "cost",         # NS AP, AWS cost
            "invoice",      # NS invoices, CB invoices
            "date",         # dates in most systems
        ]

        for concept in expected_concepts:
            self._assert(
                concept in mapped_concepts,
                f"Concept '{concept}' has mappings",
                f"mapped: {concept in mapped_concepts}"
            )

        self._assert(
            len(mapped_concepts) >= 5,
            "5+ concepts mapped",
            f"got {len(mapped_concepts)}: {sorted(mapped_concepts)}"
        )

    def _test_persona_concept_coverage(self):
        """Test 6: Verify persona views include new concepts."""
        print("\n[Test 6] Persona concept coverage")

        from backend.engine.persona_view import PersonaView

        pv = PersonaView()
        personas = [Persona.CFO, Persona.CRO, Persona.COO, Persona.CTO]
        all_concepts = pv.get_all_relevant_concept_ids(personas)

        # New concepts should be in the relevant set
        new_concepts_expected = {
            "subscription", "employee", "ticket",
            "engineering_work", "incident",
        }
        found = new_concepts_expected.intersection(all_concepts)

        self._assert(
            len(found) >= 3,
            "3+ new concepts in persona views",
            f"found: {sorted(found)}"
        )

        # CFO should see subscription
        cfo_concepts = pv.get_relevant_concepts([Persona.CFO])
        cfo_ids = set(cfo_concepts.get("CFO", []))
        self._assert(
            "subscription" in cfo_ids,
            "CFO sees 'subscription'",
        )

        # CTO should see incident and engineering_work
        cto_concepts = pv.get_relevant_concepts([Persona.CTO])
        cto_ids = set(cto_concepts.get("CTO", []))
        self._assert(
            "incident" in cto_ids,
            "CTO sees 'incident'",
        )


def main():
    harness = FarmV2TestHarness()
    results = harness.run_all()

    exit_code = 0 if results["failed"] == 0 else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
