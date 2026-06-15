"""
Semantics vs contextOS tier demo (ContextOS_Blueprint_v1 §13) — DCL's
successor to the Sankey semantics demo. A TIER contrast over ONE governed
enterprise: the Semantics (base) panel and the contextOS (premium) panel
both read the same resolved store; the premium tier adds the capabilities
to CONNECT the facts (graph traversal, arbitration, as-of).

Three strict layers (the demo rule):

  1. Operations  — every step is a real platform operation (API call, MCP
     call, operator action), each runnable standalone by hand. Documented
     in demo/OPERATIONS.md. If a step can't be run manually, it isn't in
     the demo.
  2. Sequence    — demo/sequence.py orders those operations; runs headless
     for dev/debug; the same artifact CI runs. Headless run = regression run.
  3. Wrapper     — src/components/demo/GroundedDemoTab.tsx renders captured
     runs. Presentation only: rendering, narration, pacing. Zero logic that
     changes outcomes.

Containment: this package is operator-tooling. Platform code (backend/*)
must never import demo.* — the panels in particular are operator-gated
tools, not APIs, and must not become an importable data path. Enforced by
tests/test_demo_operations.py::test_backend_never_imports_demo.
"""
