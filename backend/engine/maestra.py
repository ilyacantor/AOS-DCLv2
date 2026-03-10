"""
Maestra — Convergence (M&A Integration) Engagement Lifecycle Engine

A rule-based state machine managing three phases of an M&A integration engagement:
  1. Scoping    — Collecting deal parameters, identifying workstreams, estimating timelines
  2. Execution  — Tracking workstream progress, flagging risks, surfacing synergy opportunities
  3. Ongoing    — Monitoring integration KPIs, managing run-rate tracking, governance

Expanded in Phase 1 Part 2 to read from all engine outputs (EBITDA bridge, cross-sell,
QofE, dashboards, entity overlap) and produce data-backed responses with portal navigation.

No LLM calls.  No external API calls.  Purely deterministic.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.engine.engagement import get_active_engagement
from backend.utils.log_utils import get_logger

logger = get_logger("maestra")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PHASES = ("scoping", "execution", "ongoing")

_DEFAULT_WORKSTREAMS: list[dict[str, Any]] = [
    {
        "name": "IT Systems Migration",
        "owner": "",
        "status": "not_started",
        "progress_pct": 0,
        "milestones": [
            {"name": "System inventory complete", "target_date": "2026-08-15", "status": "pending"},
            {"name": "Migration plan approved", "target_date": "2026-09-30", "status": "pending"},
            {"name": "Data migration complete", "target_date": "2027-03-31", "status": "pending"},
        ],
    },
    {
        "name": "Organizational Design",
        "owner": "",
        "status": "not_started",
        "progress_pct": 0,
        "milestones": [
            {"name": "Org assessment complete", "target_date": "2026-08-01", "status": "pending"},
            {"name": "New org chart approved", "target_date": "2026-09-15", "status": "pending"},
            {"name": "Roles fully mapped", "target_date": "2026-11-30", "status": "pending"},
        ],
    },
    {
        "name": "Client Retention",
        "owner": "",
        "status": "not_started",
        "progress_pct": 0,
        "milestones": [
            {"name": "Top-50 client outreach complete", "target_date": "2026-07-31", "status": "pending"},
            {"name": "Retention risk assessment", "target_date": "2026-08-31", "status": "pending"},
            {"name": "Client migration plans finalized", "target_date": "2026-10-31", "status": "pending"},
        ],
    },
    {
        "name": "Vendor Consolidation",
        "owner": "",
        "status": "not_started",
        "progress_pct": 0,
        "milestones": [
            {"name": "Vendor inventory complete", "target_date": "2026-08-15", "status": "pending"},
            {"name": "Consolidation plan approved", "target_date": "2026-10-15", "status": "pending"},
            {"name": "Vendor rationalization complete", "target_date": "2027-06-30", "status": "pending"},
        ],
    },
    {
        "name": "Culture Integration",
        "owner": "",
        "status": "not_started",
        "progress_pct": 0,
        "milestones": [
            {"name": "Culture assessment survey", "target_date": "2026-07-31", "status": "pending"},
            {"name": "Integration workshops complete", "target_date": "2026-09-30", "status": "pending"},
            {"name": "Unified values rollout", "target_date": "2026-12-31", "status": "pending"},
        ],
    },
    {
        "name": "Financial Integration",
        "owner": "",
        "status": "not_started",
        "progress_pct": 0,
        "milestones": [
            {"name": "Chart of accounts harmonized", "target_date": "2026-08-31", "status": "pending"},
            {"name": "Consolidated reporting live", "target_date": "2026-10-31", "status": "pending"},
            {"name": "Audit-ready close process", "target_date": "2027-03-31", "status": "pending"},
        ],
    },
    {
        "name": "Go-to-Market Unification",
        "owner": "",
        "status": "not_started",
        "progress_pct": 0,
        "milestones": [
            {"name": "Unified brand strategy approved", "target_date": "2026-09-15", "status": "pending"},
            {"name": "Combined sales playbook", "target_date": "2026-11-30", "status": "pending"},
            {"name": "Joint pipeline operational", "target_date": "2027-03-31", "status": "pending"},
        ],
    },
]

def _build_default_risks(eng) -> list[dict[str, Any]]:
    """Build default risk register with entity names from engagement config."""
    target = eng.entity_b.display_name
    acquirer = eng.entity_a.display_name
    return [
        {
            "id": "RSK-001",
            "description": f"Key talent attrition during integration — critical {target} engineers may leave before knowledge transfer completes",
            "severity": "high",
            "mitigation": "Retention bonuses for top-30 critical roles; accelerated knowledge-transfer sprints",
            "status": "open",
        },
        {
            "id": "RSK-002",
            "description": "Client revenue leakage — top-10 overlapping clients may consolidate spend downward or churn during transition",
            "severity": "high",
            "mitigation": "Dedicated account transition teams; executive sponsor pairing for top-10 accounts",
            "status": "open",
        },
        {
            "id": "RSK-003",
            "description": f"IT integration delays — legacy {target} systems run on-prem Oracle stack incompatible with {acquirer} cloud-first architecture",
            "severity": "medium",
            "mitigation": "Parallel-run strategy with 90-day coexistence window; dedicated migration pod",
            "status": "open",
        },
        {
            "id": "RSK-004",
            "description": "Regulatory approval timeline risk — antitrust review in three jurisdictions may delay close beyond Q2 2026",
            "severity": "medium",
            "mitigation": "Proactive engagement with regulators; clean-team data room prepared; contingency timeline modeled",
            "status": "open",
        },
    ]

# ---------------------------------------------------------------------------
# Phase-advance criteria
# ---------------------------------------------------------------------------

_PHASE_ADVANCE_CRITERIA: dict[str, str] = {
    "scoping": (
        "To advance from Scoping to Execution, all workstreams must have assigned owners "
        "and at least one workstream must be in_progress."
    ),
    "execution": (
        "To advance from Execution to Ongoing Management, at least 80% of workstreams "
        "must be complete and all high-severity risks must be mitigated or closed."
    ),
}


def _can_advance(state: dict) -> tuple[bool, str]:
    """Return (allowed, reason) for advancing from the current phase."""
    phase = state["phase"]

    if phase == "scoping":
        all_owners = all(ws["owner"] for ws in state["workstreams"])
        any_in_progress = any(ws["status"] == "in_progress" for ws in state["workstreams"])
        if all_owners and any_in_progress:
            return True, "All workstreams have owners and at least one is in progress."
        return False, _PHASE_ADVANCE_CRITERIA["scoping"]

    if phase == "execution":
        total = len(state["workstreams"])
        complete = sum(1 for ws in state["workstreams"] if ws["status"] == "complete")
        high_risks_open = any(
            r["severity"] == "high" and r["status"] == "open" for r in state["risks"]
        )
        if total > 0 and (complete / total) >= 0.80 and not high_risks_open:
            return True, "Workstream completion threshold met and high-severity risks resolved."
        return False, _PHASE_ADVANCE_CRITERIA["execution"]

    if phase == "ongoing":
        return False, "The engagement is already in the final phase (Ongoing Management)."

    raise ValueError(f"Unknown phase: {phase}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_engagement() -> dict:
    """Create a new integration engagement with default state from engagement config."""

    import copy

    eng = get_active_engagement()
    now = datetime.now(timezone.utc).isoformat()
    engagement_id = str(uuid.uuid4())
    dp = eng.deal_parameters
    st = eng.synergy_targets

    state: dict[str, Any] = {
        "engagement_id": engagement_id,
        "phase": "scoping",
        "created_at": now,
        "deal_name": eng.deal_name,
        "deal_parameters": {
            "acquirer": eng.entity_a.display_name,
            "target": eng.entity_b.display_name,
            "deal_value_M": dp.get("deal_value_M", 0.0),
            "close_date": dp.get("close_date", ""),
            "integration_timeline_months": dp.get("integration_timeline_months", 0),
        },
        "workstreams": copy.deepcopy(_DEFAULT_WORKSTREAMS),
        "risks": _build_default_risks(eng),
        "synergy_tracker": {
            "cost_synergies_target_M": st.get("cost_synergies_target_M", 0.0),
            "cost_synergies_realized_M": 0.0,
            "revenue_synergies_target_M": st.get("revenue_synergies_target_M", 0.0),
            "revenue_synergies_realized_M": 0.0,
        },
        "conversation_history": [],
    }

    logger.info("Created engagement %s — %s", engagement_id, state["deal_name"])
    return state


def process_message(engagement_id: str, message: str, state: dict) -> dict:
    """
    Parse *message* for intent, mutate *state* accordingly, and return a
    structured response envelope.

    Raises ValueError on invalid inputs.
    """
    if not engagement_id or not isinstance(engagement_id, str):
        raise ValueError("engagement_id must be a non-empty string")
    if not message or not isinstance(message, str):
        raise ValueError("message must be a non-empty string")
    if not isinstance(state, dict):
        raise ValueError("state must be a dict")
    if state.get("engagement_id") != engagement_id:
        raise ValueError(
            f"engagement_id mismatch: argument is {engagement_id!r}, "
            f"state contains {state.get('engagement_id')!r}"
        )

    now = datetime.now(timezone.utc).isoformat()
    msg_lower = message.strip().lower()

    # Record the user message
    state["conversation_history"].append(
        {"role": "user", "content": message, "timestamp": now}
    )

    actions_taken: list[str] = []
    suggestions: list[str] = []
    response_text: str
    navigation: dict | None = None

    # Engine-backed intents (check first — more specific)
    ctx = _EngineContext()

    # ----- Intent: overview / engagement status with engine data -----
    if _matches(msg_lower, ["overview", "engagement", "good morning", "current status", "what's the status"]) and not msg_lower.startswith("workstream"):
        response_text, actions_taken, suggestions, navigation = _handle_overview(state, ctx)

    # ----- Intent: EBITDA bridge detail -----
    elif _matches(msg_lower, ["bridge", "ebitda bridge", "ebitda", "adjustments"]) and not _matches(msg_lower, ["qoe", "quality of earnings"]):
        response_text, actions_taken, suggestions, navigation = _handle_bridge_detail(state, ctx)

    # ----- Intent: cross-sell / pipeline -----
    elif _matches(msg_lower, ["cross-sell", "cross sell", "pipeline", "candidates"]) or ("client" in msg_lower and _matches(msg_lower, ["best", "practice", "service"])):
        response_text, actions_taken, suggestions, navigation = _handle_cross_sell_detail(state, ctx, msg_lower)

    # ----- Intent: QofE -----
    elif _matches(msg_lower, ["qoe", "quality of earnings", "sustainability", "earnings quality"]):
        response_text, actions_taken, suggestions, navigation = _handle_qoe_detail(state, ctx)

    # ----- Intent: people / headcount / overlap (with function filtering) -----
    elif _matches(msg_lower, ["people", "headcount", "head count", "talent", "retention"]):
        response_text, actions_taken, suggestions, navigation = _handle_people_detail(state, ctx, msg_lower)

    # ----- Intent: dashboard for a persona -----
    elif _matches(msg_lower, ["dashboard", "cfo", "cro", "coo", "cto", "chro"]):
        response_text, actions_taken, suggestions, navigation = _handle_dashboard_detail(state, ctx, msg_lower)

    # ----- Original lifecycle intents below -----

    # ----- Intent: advance / next phase -----
    elif _matches(msg_lower, ["advance", "next phase"]):
        response_text, actions_taken, suggestions = _handle_advance(state)

    # ----- Intent: status / update (simple — no engine data) -----
    elif _matches(msg_lower, ["status", "update"]):
        response_text, actions_taken, suggestions = _handle_status(state)

    # ----- Intent: complete <workstream> -----
    elif msg_lower.startswith("complete"):
        response_text, actions_taken, suggestions = _handle_complete(msg_lower, state)

    # ----- Intent: progress <workstream> <number> -----
    elif msg_lower.startswith("progress"):
        response_text, actions_taken, suggestions = _handle_progress(msg_lower, state)

    # ----- Intent: workstream <name> -----
    elif msg_lower.startswith("workstream"):
        response_text, actions_taken, suggestions = _handle_workstream(msg_lower, state)

    # ----- Intent: risk -----
    elif _matches(msg_lower, ["risk"]):
        response_text, actions_taken, suggestions = _handle_risk(state)

    # ----- Intent: synergy -----
    elif _matches(msg_lower, ["synergy"]):
        response_text, actions_taken, suggestions = _handle_synergy(state)

    # ----- Intent: milestone -----
    elif _matches(msg_lower, ["milestone"]):
        response_text, actions_taken, suggestions = _handle_milestone(state)

    # ----- Default: phase guidance -----
    else:
        response_text, actions_taken, suggestions = _handle_default(state)

    # Record assistant response
    state["conversation_history"].append(
        {"role": "assistant", "content": response_text, "timestamp": now}
    )

    logger.info(
        "Processed message for engagement %s — intent resolved, %d actions taken",
        engagement_id,
        len(actions_taken),
    )

    result = {
        "response": response_text,
        "state": state,
        "actions_taken": actions_taken,
        "suggestions": suggestions,
    }
    if navigation:
        result["navigation"] = navigation
    return result


def get_engagement_status(state: dict) -> dict:
    """Return a structured summary of the engagement's current position."""
    if not isinstance(state, dict):
        raise ValueError("state must be a dict")
    if "phase" not in state:
        raise ValueError("state is missing required key 'phase'")

    workstreams = state.get("workstreams", [])
    total_ws = len(workstreams)
    overall_progress = (
        round(sum(ws["progress_pct"] for ws in workstreams) / total_ws)
        if total_ws > 0
        else 0
    )

    open_risks = sum(1 for r in state.get("risks", []) if r["status"] == "open")

    synergy = state.get("synergy_tracker", {})
    total_target = synergy.get("cost_synergies_target_M", 0) + synergy.get(
        "revenue_synergies_target_M", 0
    )
    total_realized = synergy.get("cost_synergies_realized_M", 0) + synergy.get(
        "revenue_synergies_realized_M", 0
    )
    synergy_pct = round((total_realized / total_target) * 100, 1) if total_target > 0 else 0.0

    created_at = state.get("created_at", datetime.now(timezone.utc).isoformat())
    try:
        created_dt = datetime.fromisoformat(created_at)
    except (ValueError, TypeError):
        created_dt = datetime.now(timezone.utc)
    days_since = (datetime.now(timezone.utc) - created_dt).days

    # Collect the nearest upcoming milestone per workstream
    next_milestones: list[dict[str, str]] = []
    for ws in workstreams:
        for ms in ws.get("milestones", []):
            if ms["status"] == "pending":
                next_milestones.append(
                    {
                        "workstream": ws["name"],
                        "milestone": ms["name"],
                        "target_date": ms["target_date"],
                    }
                )
                break  # only the first pending milestone per workstream

    # Sort milestones by target_date
    next_milestones.sort(key=lambda m: m["target_date"])

    return {
        "phase": state["phase"],
        "deal_name": state.get("deal_name", ""),
        "overall_progress_pct": overall_progress,
        "workstream_summary": [
            {"name": ws["name"], "status": ws["status"], "progress_pct": ws["progress_pct"]}
            for ws in workstreams
        ],
        "open_risks": open_risks,
        "synergy_realization_pct": synergy_pct,
        "days_since_start": days_since,
        "next_milestones": next_milestones,
    }


# ---------------------------------------------------------------------------
# Intent handlers (private)
# ---------------------------------------------------------------------------


def _matches(text: str, keywords: list[str]) -> bool:
    """Return True if any keyword appears as a whole word in text."""
    for kw in keywords:
        if re.search(rf"\b{re.escape(kw)}\b", text):
            return True
    return False


def _find_workstream(name_fragment: str, state: dict) -> dict | None:
    """Find a workstream whose name contains *name_fragment* (case-insensitive)."""
    fragment = name_fragment.strip().lower()
    for ws in state["workstreams"]:
        if fragment in ws["name"].lower():
            return ws
    return None


def _handle_advance(state: dict) -> tuple[str, list[str], list[str]]:
    eng = get_active_engagement()
    allowed, reason = _can_advance(state)
    if not allowed:
        return (
            f"Cannot advance from {state['phase']} phase yet. {reason}",
            [],
            _phase_suggestions(state["phase"]),
        )

    current_idx = PHASES.index(state["phase"])
    new_phase = PHASES[current_idx + 1]
    old_phase = state["phase"]
    state["phase"] = new_phase

    return (
        f"Phase advanced from {old_phase} to {new_phase}. "
        f"The {eng.deal_name} integration now enters the {new_phase} phase.",
        [f"phase_advanced:{old_phase}->{new_phase}"],
        _phase_suggestions(new_phase),
    )


def _handle_status(state: dict) -> tuple[str, list[str], list[str]]:
    eng = get_active_engagement()
    summary = get_engagement_status(state)
    ws_lines = "\n".join(
        f"  - {ws['name']}: {ws['status']} ({ws['progress_pct']}%)"
        for ws in summary["workstream_summary"]
    )
    text = (
        f"{eng.deal_name} — Status Report\n"
        f"Phase: {summary['phase'].title()}\n"
        f"Overall progress: {summary['overall_progress_pct']}%\n"
        f"Open risks: {summary['open_risks']}\n"
        f"Synergy realization: {summary['synergy_realization_pct']}%\n"
        f"Days since engagement start: {summary['days_since_start']}\n\n"
        f"Workstreams:\n{ws_lines}"
    )
    return text, ["status_retrieved"], _phase_suggestions(state["phase"])


def _handle_complete(msg_lower: str, state: dict) -> tuple[str, list[str], list[str]]:
    # Extract workstream name after "complete"
    ws_name = msg_lower.replace("complete", "", 1).strip()
    if not ws_name:
        raise ValueError(
            "Please specify which workstream to mark complete, e.g. 'complete IT Systems Migration'"
        )
    ws = _find_workstream(ws_name, state)
    if ws is None:
        raise ValueError(
            f"No workstream matching '{ws_name}' found. "
            f"Available workstreams: {', '.join(w['name'] for w in state['workstreams'])}"
        )

    ws["status"] = "complete"
    ws["progress_pct"] = 100
    for ms in ws["milestones"]:
        ms["status"] = "complete"

    return (
        f"Workstream '{ws['name']}' marked complete. All milestones set to complete.",
        [f"workstream_completed:{ws['name']}"],
        _phase_suggestions(state["phase"]),
    )


def _handle_progress(msg_lower: str, state: dict) -> tuple[str, list[str], list[str]]:
    # Expected: "progress <workstream name> <number>"
    remainder = msg_lower.replace("progress", "", 1).strip()

    # Extract trailing number
    match = re.search(r"(\d+)\s*%?\s*$", remainder)
    if not match:
        raise ValueError(
            "Please specify a progress percentage, e.g. 'progress IT Systems Migration 45'"
        )
    pct = int(match.group(1))
    if pct < 0 or pct > 100:
        raise ValueError(f"Progress percentage must be between 0 and 100, got {pct}")

    ws_name = remainder[: match.start()].strip()
    if not ws_name:
        raise ValueError(
            "Please specify which workstream to update, e.g. 'progress IT Systems Migration 45'"
        )

    ws = _find_workstream(ws_name, state)
    if ws is None:
        raise ValueError(
            f"No workstream matching '{ws_name}' found. "
            f"Available workstreams: {', '.join(w['name'] for w in state['workstreams'])}"
        )

    old_pct = ws["progress_pct"]
    ws["progress_pct"] = pct
    if pct > 0 and ws["status"] == "not_started":
        ws["status"] = "in_progress"
    if pct == 100:
        ws["status"] = "complete"
        for ms in ws["milestones"]:
            ms["status"] = "complete"

    return (
        f"Workstream '{ws['name']}' progress updated from {old_pct}% to {pct}%.",
        [f"workstream_progress_updated:{ws['name']}:{old_pct}->{pct}"],
        _phase_suggestions(state["phase"]),
    )


def _handle_workstream(msg_lower: str, state: dict) -> tuple[str, list[str], list[str]]:
    ws_name = msg_lower.replace("workstream", "", 1).strip()
    if not ws_name:
        # Show all workstreams
        lines = "\n".join(
            f"  - {ws['name']}: {ws['status']} ({ws['progress_pct']}%)"
            for ws in state["workstreams"]
        )
        return (
            f"All workstreams for {get_active_engagement().deal_name}:\n{lines}",
            ["workstreams_listed"],
            _phase_suggestions(state["phase"]),
        )

    ws = _find_workstream(ws_name, state)
    if ws is None:
        raise ValueError(
            f"No workstream matching '{ws_name}' found. "
            f"Available workstreams: {', '.join(w['name'] for w in state['workstreams'])}"
        )

    ms_lines = "\n".join(
        f"    - {ms['name']} (target: {ms['target_date']}, status: {ms['status']})"
        for ms in ws["milestones"]
    )
    text = (
        f"Workstream: {ws['name']}\n"
        f"Owner: {ws['owner'] or 'Unassigned'}\n"
        f"Status: {ws['status']}\n"
        f"Progress: {ws['progress_pct']}%\n"
        f"Milestones:\n{ms_lines}"
    )
    return text, [f"workstream_viewed:{ws['name']}"], _phase_suggestions(state["phase"])


def _handle_risk(state: dict) -> tuple[str, list[str], list[str]]:
    eng = get_active_engagement()
    risks = state.get("risks", [])
    if not risks:
        return (
            f"No risks currently tracked for the {eng.deal_name} integration.",
            ["risks_viewed"],
            ["Add a risk by describing potential integration threats."],
        )

    lines = "\n".join(
        f"  [{r['id']}] ({r['severity'].upper()}, {r['status']}) {r['description']}\n"
        f"    Mitigation: {r['mitigation']}"
        for r in risks
    )
    open_count = sum(1 for r in risks if r["status"] == "open")
    text = (
        f"Risk Register — {eng.deal_name}\n"
        f"Total risks: {len(risks)} | Open: {open_count}\n\n{lines}"
    )
    return text, ["risks_viewed"], _phase_suggestions(state["phase"])


def _handle_synergy(state: dict) -> tuple[str, list[str], list[str]]:
    s = state.get("synergy_tracker", {})
    cost_target = s.get("cost_synergies_target_M", 0)
    cost_realized = s.get("cost_synergies_realized_M", 0)
    rev_target = s.get("revenue_synergies_target_M", 0)
    rev_realized = s.get("revenue_synergies_realized_M", 0)
    total_target = cost_target + rev_target
    total_realized = cost_realized + rev_realized
    pct = round((total_realized / total_target) * 100, 1) if total_target > 0 else 0.0

    text = (
        f"Synergy Tracker — {get_active_engagement().deal_name}\n"
        f"Cost synergies:    ${cost_realized:.1f}M realized of ${cost_target:.1f}M target\n"
        f"Revenue synergies: ${rev_realized:.1f}M realized of ${rev_target:.1f}M target\n"
        f"Total realization:  {pct}% (${total_realized:.1f}M of ${total_target:.1f}M)"
    )
    return text, ["synergies_viewed"], _phase_suggestions(state["phase"])


def _handle_milestone(state: dict) -> tuple[str, list[str], list[str]]:
    milestones: list[tuple[str, dict]] = []
    for ws in state["workstreams"]:
        for ms in ws.get("milestones", []):
            if ms["status"] == "pending":
                milestones.append((ws["name"], ms))

    milestones.sort(key=lambda m: m[1]["target_date"])

    if not milestones:
        return (
            "All milestones have been completed across every workstream.",
            ["milestones_viewed"],
            _phase_suggestions(state["phase"]),
        )

    lines = "\n".join(
        f"  - [{ms['target_date']}] {ws_name}: {ms['name']}"
        for ws_name, ms in milestones
    )
    text = f"Upcoming Milestones — {get_active_engagement().deal_name}\n{lines}"
    return text, ["milestones_viewed"], _phase_suggestions(state["phase"])


# ---------------------------------------------------------------------------
# Engine context — loads all engine outputs for data-backed responses
# ---------------------------------------------------------------------------


class _EngineContext:
    """Accessor over the module-level engine cache.

    All expensive computation is done once in _engine_cache and reused across
    requests until source data files change on disk.  This class is a thin
    wrapper — it holds no per-instance state.
    """

    def get_bridge(self) -> dict:
        from backend.engine import _engine_cache
        return _engine_cache.get("bridge")

    def get_cross_sell(self) -> dict:
        from backend.engine import _engine_cache
        return _engine_cache.get("cross_sell")

    def get_qoe(self) -> dict:
        from backend.engine import _engine_cache
        return _engine_cache.get("qoe")

    def get_overlap(self) -> dict:
        from backend.engine import _engine_cache
        return _engine_cache.get("overlap")

    def get_combining(self) -> dict:
        from backend.engine import _engine_cache
        return _engine_cache.get("combining")


def _fmt_m(dollars: float) -> str:
    """Format dollars to $XM or $XB."""
    if abs(dollars) >= 1_000_000_000:
        return f"${dollars / 1_000_000_000:.1f}B"
    return f"${dollars / 1_000_000:.1f}M"


# ---------------------------------------------------------------------------
# Expanded intent handlers (engine-backed)
# ---------------------------------------------------------------------------


def _handle_overview(state: dict, ctx: _EngineContext) -> tuple[str, list[str], list[str], dict | None]:
    """Scene 1: Full engagement overview with exact numbers from all engines."""
    overlap = ctx.get_overlap()
    bridge = ctx.get_bridge()
    cs = ctx.get_cross_sell()
    qoe = ctx.get_qoe()
    combining = ctx.get_combining()

    co = overlap.get("customer_overlap", {})
    vo = overlap.get("vendor_overlap", {})
    po = overlap.get("people_overlap", {})

    # Vendor savings
    vendor_savings = sum(
        m.get("consolidation_detail", {}).get("estimated_savings_M", 0)
        for m in vo.get("matches", [])
        if isinstance(m.get("consolidation_detail"), dict)
    )

    # Latest quarter revenue
    periods = sorted([k for k in combining.keys() if not k.startswith("_")])
    latest = combining[periods[-1]] if periods else {}
    li_map = {li["line_item"]: li for li in latest.get("line_items", [])}
    combined_rev_q = li_map.get("Total Revenue", {}).get("combined", 0)

    # Bridge
    pf = bridge["pro_forma_ebitda"]

    cs_summary = cs.get("summary", {})

    text = (
        f"Good morning. Here's the current status of the {get_active_engagement().deal_name} engagement.\n\n"
        f"All five reconciliation objects are complete:\n"
        f"  ✓ Financial Statements — COFA unified, combining P&L operational\n"
        f"  ✓ Customers — {co.get('total_overlapping', 0)} overlapping accounts identified, "
        f"{cs_summary.get('total_candidates', 0)} cross-sell candidates scored\n"
        f"  ✓ Vendors — {vo.get('total_overlapping', 0)} overlapping vendors, "
        f"${vendor_savings:.0f}M consolidation opportunity\n"
        f"  ✓ People — corporate overlap mapped across {len(po.get('functions', []))} functions\n"
        f"  ✓ IT Landscape — SOR conflicts identified, redundant platforms flagged\n\n"
        f"The combining P&L shows ${combined_rev_q * 4:.2f}B combined annualized revenue "
        f"with {_fmt_m(bridge['reported_ebitda']['combined_reported'])} reported EBITDA.\n"
        f"After adjustments, pro forma adjusted EBITDA is {_fmt_m(pf['year_1']['current'])} (Year 1) "
        f"and {_fmt_m(pf['steady_state']['current'])} (Steady State).\n\n"
        f"Quality of Earnings sustainability score: {qoe['sustainability_score']['overall']:.0f}/100 "
        f"(Grade: {qoe['sustainability_score']['grade']}).\n\n"
        f"Open risks: {sum(1 for r in state.get('risks', []) if r['status'] == 'open')}."
    )

    return text, ["overview_presented"], _phase_suggestions(state["phase"]), None


def _handle_bridge_detail(state: dict, ctx: _EngineContext) -> tuple[str, list[str], list[str], dict | None]:
    """EBITDA bridge detail with exact amounts."""
    bridge = ctx.get_bridge()
    rep = bridge["reported_ebitda"]
    ea = bridge["entity_adjusted_ebitda"]
    pf = bridge["pro_forma_ebitda"]

    adj_lines = []
    for adj in bridge.get("entity_adjustments", []):
        adj_lines.append(f"  + {adj['name']}: {_fmt_m(adj['amount'])} ({adj['confidence']})")
    syn_lines = []
    for syn in bridge.get("combination_synergies", []):
        prefix = "−" if syn.get("category") == "dis_synergy" else "+"
        syn_lines.append(f"  {prefix} {syn['name']}: {_fmt_m(abs(syn['amount']))} ({syn['confidence']})")

    eng = get_active_engagement()
    text = (
        f"EBITDA Bridge — {eng.deal_name}\n\n"
        f"Reported EBITDA (Combined): {_fmt_m(rep['combined_reported'])}\n"
        f"  {eng.entity_a.display_name}: {_fmt_m(rep[eng.entity_a.id])} | {eng.entity_b.display_name}: {_fmt_m(rep[eng.entity_b.id])}\n\n"
        f"Entity-Level Adjustments:\n" + "\n".join(adj_lines) + "\n\n"
        f"Entity-Adjusted EBITDA: {_fmt_m(ea['combined'])}\n\n"
        f"Combination Synergies:\n" + "\n".join(syn_lines) + "\n\n"
        f"Pro Forma Year 1: {_fmt_m(pf['year_1']['current'])} "
        f"(range: {_fmt_m(pf['year_1']['low'])} — {_fmt_m(pf['year_1']['high'])})\n"
        f"Pro Forma Steady State: {_fmt_m(pf['steady_state']['current'])} "
        f"(range: {_fmt_m(pf['steady_state']['low'])} — {_fmt_m(pf['steady_state']['high'])})"
    )
    return text, ["bridge_detail_presented"], _phase_suggestions(state["phase"]), {"tab": "bridge"}


def _handle_cross_sell_detail(state: dict, ctx: _EngineContext, msg_lower: str) -> tuple[str, list[str], list[str], dict | None]:
    """Cross-sell pipeline detail, optionally filtered by practice area."""
    cs = ctx.get_cross_sell()
    summary = cs.get("summary", {})
    all_candidates = cs.get("m_to_c", []) + cs.get("c_to_m", [])

    # Check for practice/service filter
    filter_keywords = {
        "risk": "Risk", "compliance": "Risk", "strategy": "Strategy",
        "operations": "Operations", "tech": "Tech", "digital": "Digital",
        "ai": "Digital", "f&a": "F&A", "outsourc": "F&A", "cx": "CX",
        "data": "Data", "analytics": "Data", "bpo": "Industry",
    }
    service_filter = None
    for kw, svc in filter_keywords.items():
        if kw in msg_lower:
            service_filter = svc
            break

    if service_filter:
        filtered = [c for c in all_candidates if service_filter.lower() in c.get("recommended_service", "").lower()]
        filtered.sort(key=lambda c: c["propensity_score"], reverse=True)
        top = filtered[:5]
        total_acv = sum(c["estimated_acv"] for c in filtered)

        lines = []
        for i, c in enumerate(top, 1):
            lines.append(
                f"  {i}. {c['customer_name']} — propensity {c['propensity_score']}%, "
                f"est. {_fmt_m(c['estimated_acv'])} ACV. "
                f"{c.get('industry', '')}, {_fmt_m(c.get('customer_engagement_M', 0) * 1_000_000)} existing contract."
            )
        text = (
            f"{len(filtered)} candidates for {service_filter} services. Top {len(top)}:\n\n"
            + "\n".join(lines) + "\n\n"
            f"Total {service_filter} pipeline: {_fmt_m(total_acv)} ACV, "
            f"{sum(1 for c in filtered if c['propensity_score'] > 80)} high-confidence."
        )
    else:
        eng = get_active_engagement()
        text = (
            f"Cross-Sell Pipeline — {eng.deal_name}\n\n"
            f"{eng.entity_a.display_name} → {eng.entity_b.display_name} (advisory clients → BPM services):\n"
            f"  {summary.get('m_to_c_candidates', 0)} candidates, {_fmt_m(summary.get('m_to_c_total_acv', 0))} pipeline\n"
            f"  {summary.get('m_to_c_high_conf_count', 0)} high-confidence ({_fmt_m(summary.get('m_to_c_high_conf_acv', 0))})\n\n"
            f"{eng.entity_b.display_name} → {eng.entity_a.display_name} (BPM clients → consulting services):\n"
            f"  {summary.get('c_to_m_candidates', 0)} candidates, {_fmt_m(summary.get('c_to_m_total_acv', 0))} pipeline\n"
            f"  {summary.get('c_to_m_high_conf_count', 0)} high-confidence ({_fmt_m(summary.get('c_to_m_high_conf_acv', 0))})\n\n"
            f"Total pipeline: {summary.get('total_candidates', 0)} candidates, {_fmt_m(summary.get('total_pipeline_acv', 0))}\n"
            f"High-confidence: {_fmt_m(summary.get('total_high_conf_acv', 0))}"
        )
    return text, ["cross_sell_presented"], _phase_suggestions(state["phase"]), {"tab": "crosssell"}


def _handle_qoe_detail(state: dict, ctx: _EngineContext) -> tuple[str, list[str], list[str], dict | None]:
    """Scene 4: QofE quarterly update with exact metrics."""
    qoe = ctx.get_qoe()
    summary = qoe["summary"]
    sus = qoe["sustainability_score"]
    rq = qoe["revenue_quality"]
    wc = qoe["working_capital"]

    # Adjustment status summary
    status_lines = []
    for row in qoe["ebitda_bridge"]:
        if row["status"] == "resolved":
            status_lines.append(f"  → RESOLVED: {row['name']} ({_fmt_m(row['current_amount'])})")
        elif row["status"] == "new":
            status_lines.append(f"  → NEW: {row['name']} ({_fmt_m(row['current_amount'])})")
        elif row["status"] == "changed":
            status_lines.append(
                f"  → CHANGED: {row['name']} ({_fmt_m(row.get('prior_amount', 0) or 0)} → {_fmt_m(row['current_amount'])})"
            )

    adj_status_text = "\n".join(status_lines) if status_lines else "  All adjustments stable — no changes from prior period."

    # DSO
    dso_current = wc["dso_trend"][-1]["value"] if wc.get("dso_trend") else "N/A"

    text = (
        f"Quality of Earnings Report — {qoe['period']}\n\n"
        f"Adjusted EBITDA: {_fmt_m(summary['entity_adjusted_ebitda'])}\n"
        f"Pro Forma Year 1: {_fmt_m(summary['pro_forma_year_1'])}\n"
        f"Pro Forma Steady State: {_fmt_m(summary['pro_forma_steady_state'])}\n\n"
        f"Adjustment changes this period:\n{adj_status_text}\n\n"
        f"Revenue quality: HHI={rq['customer_concentration']['hhi']:.0f}, "
        f"top-10 concentration={rq['customer_concentration']['top_10_pct']:.1f}%, "
        f"recurring revenue={rq['revenue_mix']['recurring_pct']:.1f}%.\n\n"
        f"Cross-sell penetration: {rq['cross_sell_penetration']['total_candidates']} candidates, "
        f"{rq['cross_sell_penetration']['converted_count']} converted.\n\n"
        f"Earnings sustainability score: {sus['overall']:.0f}/100 (Grade: {sus['grade']})\n"
        f"  Components: " + ", ".join(f"{c['name']}={c['score']:.0f}" for c in sus["components"]) + "\n\n"
        f"Working capital: DSO={dso_current} days. "
        f"{'No flags.' if isinstance(dso_current, (int, float)) and dso_current < 60 else 'Elevated — investigate.'}\n\n"
        f"New items detected: {len(qoe['new_items'])}."
    )
    return text, ["qoe_presented"], _phase_suggestions(state["phase"]), {"tab": "qoe"}


def _handle_people_detail(state: dict, ctx: _EngineContext, msg_lower: str) -> tuple[str, list[str], list[str], dict | None]:
    """People overlap detail, optionally filtered by function."""
    overlap = ctx.get_overlap()
    po = overlap.get("people_overlap", {})
    functions = po.get("functions", [])

    # Check for function filter
    func_filter = None
    for fn in functions:
        if fn["function"].lower() in msg_lower:
            func_filter = fn
            break

    eng = get_active_engagement()
    if func_filter:
        fn = func_filter
        role_lines = []
        a_id, b_id = eng.entity_a.id, eng.entity_b.id
        for rd in fn.get("role_detail", []):
            role_lines.append(
                f"  - {rd['title']}: {eng.entity_a.display_name}={rd.get(f'{a_id}_count', 0)}, "
                f"{eng.entity_b.display_name}={rd.get(f'{b_id}_count', 0)}, "
                f"Combined={rd['combined_count']} [{rd.get('consolidation_action', 'N/A').upper()}]"
            )
        text = (
            f"People Overlap — {fn['function']}\n\n"
            f"{eng.entity_a.display_name}: {fn.get(f'{a_id}_headcount', 0)} | "
            f"{eng.entity_b.display_name}: {fn.get(f'{b_id}_headcount', 0)} | "
            f"Combined: {fn['combined_headcount']}\n\n"
            f"Key roles: {', '.join(fn.get('role_overlap_examples', []))}\n\n"
            f"Note: {fn.get('definitional_note', '')}\n\n"
            f"Role detail:\n" + "\n".join(role_lines)
        )
    else:
        a_id, b_id = eng.entity_a.id, eng.entity_b.id
        fn_lines = []
        for fn in functions:
            fn_lines.append(
                f"  - {fn['function']}: {eng.entity_a.display_name}={fn.get(f'{a_id}_headcount', 0)}, "
                f"{eng.entity_b.display_name}={fn.get(f'{b_id}_headcount', 0)}, "
                f"Combined={fn['combined_headcount']}"
            )
        text = (
            f"People Overlap — {eng.deal_name}\n\n"
            f"Total corporate: {eng.entity_a.display_name}={po.get(f'total_{a_id}_corporate', 0)}, "
            f"{eng.entity_b.display_name}={po.get(f'total_{b_id}_corporate', 0)}, "
            f"Combined={po.get('total_combined_corporate', 0)}\n\n"
            f"By function:\n" + "\n".join(fn_lines)
        )
    return text, ["people_detail_presented"], _phase_suggestions(state["phase"]), {"tab": "overlap"}


def _handle_dashboard_detail(state: dict, ctx: _EngineContext, msg_lower: str) -> tuple[str, list[str], list[str], dict | None]:
    """Dashboard summary for a specific persona."""
    from backend.engine.dashboards import compute_dashboard

    persona = "cfo"
    for p in ("cfo", "cro", "coo", "cto", "chro"):
        if p in msg_lower:
            persona = p
            break

    dashboard = compute_dashboard(persona)
    kpi_lines = []
    for k, v in dashboard.get("kpis", {}).items():
        label = k.replace("_", " ").title()
        if isinstance(v, (int, float)) and abs(v) > 100_000:
            kpi_lines.append(f"  {label}: {_fmt_m(v)}")
        else:
            kpi_lines.append(f"  {label}: {v:,.0f}" if isinstance(v, (int, float)) else f"  {label}: {v}")

    text = (
        f"{dashboard.get('title', persona.upper() + ' Dashboard')}\n\n"
        f"Key metrics:\n" + "\n".join(kpi_lines)
    )
    return text, ["dashboard_presented"], _phase_suggestions(state["phase"]), {"tab": "dashboards"}


def _handle_default(state: dict) -> tuple[str, list[str], list[str]]:
    eng = get_active_engagement()
    phase = state["phase"]
    guidance = {
        "scoping": (
            f"You are in the Scoping phase of the {eng.deal_name} integration. "
            "Key activities: finalize deal parameters, assign workstream owners, "
            "validate synergy assumptions, and confirm the integration timeline. "
            "Use 'status' to see where things stand, or 'workstream <name>' to drill into a specific area."
        ),
        "execution": (
            f"You are in the Execution phase of the {eng.deal_name} integration. "
            "Key activities: drive workstream progress, track milestones, manage risks, "
            "and begin realizing synergies. "
            "Use 'progress <workstream> <pct>' to update progress, 'risk' to review the risk register, "
            "or 'synergy' to check synergy realization."
        ),
        "ongoing": (
            f"You are in the Ongoing Management phase of the {eng.deal_name} integration. "
            "Key activities: monitor integration KPIs, track run-rate synergy capture, "
            "manage governance cadence, and resolve residual integration items. "
            "Use 'synergy' to track realization, 'risk' to review open items, "
            "or 'status' for the full dashboard."
        ),
    }
    return guidance[phase], [], _phase_suggestions(phase)


def _phase_suggestions(phase: str) -> list[str]:
    """Return contextual next-step suggestions for the current phase."""
    if phase == "scoping":
        return [
            "Assign owners to all workstreams before advancing to Execution.",
            "Review the risk register and validate initial risk assessments.",
            "Confirm synergy targets with the deal team.",
            "Use 'advance' when all workstreams have owners and at least one is in progress.",
        ]
    if phase == "execution":
        return [
            "Update workstream progress regularly with 'progress <workstream> <pct>'.",
            "Monitor the risk register — mitigate high-severity items before phase advance.",
            "Track synergy realization against the $250M combined target.",
            "Use 'advance' when 80%+ workstreams are complete and high risks are resolved.",
        ]
    if phase == "ongoing":
        return [
            "Monitor run-rate synergy capture on a quarterly basis.",
            "Review residual risks and ensure mitigation plans are active.",
            "Track integration KPIs against baseline.",
            "Prepare governance review materials for the integration steering committee.",
        ]
    return []
