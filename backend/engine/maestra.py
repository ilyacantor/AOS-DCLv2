"""
Maestra — Convergence (M&A Integration) Engagement Lifecycle Engine

A rule-based state machine managing three phases of an M&A integration engagement:
  1. Scoping    — Collecting deal parameters, identifying workstreams, estimating timelines
  2. Execution  — Tracking workstream progress, flagging risks, surfacing synergy opportunities
  3. Ongoing    — Monitoring integration KPIs, managing run-rate tracking, governance

No LLM calls.  No external API calls.  Purely deterministic.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any

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

_DEFAULT_RISKS: list[dict[str, Any]] = [
    {
        "id": "RSK-001",
        "description": "Key talent attrition during integration — critical Cascadia BPM engineers may leave before knowledge transfer completes",
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
        "description": "IT integration delays — legacy Cascadia systems run on-prem Oracle stack incompatible with Meridian cloud-first architecture",
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
    """Create a new Meridian-Cascadia integration engagement with default state."""

    import copy

    now = datetime.now(timezone.utc).isoformat()
    engagement_id = str(uuid.uuid4())

    state: dict[str, Any] = {
        "engagement_id": engagement_id,
        "phase": "scoping",
        "created_at": now,
        "deal_name": "Meridian-Cascadia Integration",
        "deal_parameters": {
            "acquirer": "Meridian Partners",
            "target": "Cascadia BPM",
            "deal_value_M": 3200.0,
            "close_date": "2026-06-30",
            "integration_timeline_months": 18,
        },
        "workstreams": copy.deepcopy(_DEFAULT_WORKSTREAMS),
        "risks": copy.deepcopy(_DEFAULT_RISKS),
        "synergy_tracker": {
            "cost_synergies_target_M": 200.0,
            "cost_synergies_realized_M": 0.0,
            "revenue_synergies_target_M": 50.0,
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

    # ----- Intent: advance / next phase -----
    if _matches(msg_lower, ["advance", "next phase"]):
        response_text, actions_taken, suggestions = _handle_advance(state)

    # ----- Intent: status / update -----
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

    return {
        "response": response_text,
        "state": state,
        "actions_taken": actions_taken,
        "suggestions": suggestions,
    }


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
        f"The Meridian-Cascadia integration now enters the {new_phase} phase.",
        [f"phase_advanced:{old_phase}->{new_phase}"],
        _phase_suggestions(new_phase),
    )


def _handle_status(state: dict) -> tuple[str, list[str], list[str]]:
    summary = get_engagement_status(state)
    ws_lines = "\n".join(
        f"  - {ws['name']}: {ws['status']} ({ws['progress_pct']}%)"
        for ws in summary["workstream_summary"]
    )
    text = (
        f"Meridian-Cascadia Integration — Status Report\n"
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
            f"All workstreams for Meridian-Cascadia Integration:\n{lines}",
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
    risks = state.get("risks", [])
    if not risks:
        return (
            "No risks currently tracked for the Meridian-Cascadia integration.",
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
        f"Risk Register — Meridian-Cascadia Integration\n"
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
        f"Synergy Tracker — Meridian-Cascadia Integration\n"
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
    text = f"Upcoming Milestones — Meridian-Cascadia Integration\n{lines}"
    return text, ["milestones_viewed"], _phase_suggestions(state["phase"])


def _handle_default(state: dict) -> tuple[str, list[str], list[str]]:
    phase = state["phase"]
    guidance = {
        "scoping": (
            "You are in the Scoping phase of the Meridian-Cascadia integration. "
            "Key activities: finalize deal parameters, assign workstream owners, "
            "validate synergy assumptions, and confirm the integration timeline. "
            "Use 'status' to see where things stand, or 'workstream <name>' to drill into a specific area."
        ),
        "execution": (
            "You are in the Execution phase of the Meridian-Cascadia integration. "
            "Key activities: drive workstream progress, track milestones, manage risks, "
            "and begin realizing synergies. "
            "Use 'progress <workstream> <pct>' to update progress, 'risk' to review the risk register, "
            "or 'synergy' to check synergy realization."
        ),
        "ongoing": (
            "You are in the Ongoing Management phase of the Meridian-Cascadia integration. "
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
