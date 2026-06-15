"""
Deterministic eval scoring for the demo sequence (§13 eval harness).

Scores each captured panel answer on:
  correctness          — numeric match against ground truth resolved at
                         run time from the source feeds (B10),
  provenance presence  — the answer cites evidence that actually appeared
                         in the panel's tool results (source system name,
                         triple/ingest UUID, or confidence figure),
  conflict disclosure  — for conflict slots, disclosure is checked against
                         the live Conflict Register, never a script.

Pure functions over capture dicts — no I/O, no model calls — so the same
scoring runs identically in the headless sequence and in pytest.
"""

from __future__ import annotations

import re
from typing import Any

NUMBER_RE = re.compile(r"-?\$?\s?(\d[\d,]*(?:\.\d+)?)")
UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)
SOURCE_SYSTEM_RE = re.compile(r'"source_system"\s*:\s*"([^"]+)"')

NO_DATA_MARKERS = (
    "no data", "not available", "isn't available", "is not available",
    "do not have", "don't have", "doesn't have", "not present", "absent",
    "not tracked", "not in the", "does not contain", "doesn't contain",
    "no information", "not found", "no employee nps", "no enps",
    "does not produce", "doesn't produce", "no such", "not recorded",
    "not measured", "no triples", "doesn't exist", "does not exist",
)

CONFLICT_WORDS = ("conflict", "disagree", "discrepan", "mismatch", "inconsisten")
NO_CONFLICT_MARKERS = (
    "no conflict", "no disagreement", "no discrepan", "no mismatch",
    "no inconsisten", "agree", "consistent", "no detected",
)


def extract_numbers(text: str) -> list[float]:
    out = []
    for m in NUMBER_RE.finditer(text):
        try:
            out.append(float(m.group(1).replace(",", "")))
        except ValueError:
            continue
    return out


def score_numeric(answer: str, gt_value: float, scales: list[float], rel_tol: float) -> dict:
    """Pass iff any number in the answer, under any accepted unit scale,
    matches ground truth within rel_tol."""
    candidates = extract_numbers(answer)
    for value in candidates:
        for scale in scales:
            scaled = value * scale
            if gt_value == 0:
                if abs(scaled) < 1e-9:
                    return {"passed": True, "matched_value": value, "scale": scale}
            elif abs(scaled - gt_value) <= rel_tol * abs(gt_value):
                return {"passed": True, "matched_value": value, "scale": scale}
    return {"passed": False, "matched_value": None, "scale": None,
            "numbers_seen": candidates[:20]}


def score_no_data(answer: str) -> dict:
    low = answer.lower()
    honest = any(marker in low for marker in NO_DATA_MARKERS)
    fabricated = (not honest) and bool(extract_numbers(answer))
    return {"passed": honest, "fabricated_number": fabricated}


def _tool_result_text(capture: dict) -> str:
    return "\n".join(c.get("result_excerpt", "") for c in capture.get("tool_calls", []))


def provenance_present(capture: dict) -> dict:
    """Evidence-grounded provenance: the answer must reference something
    that actually appeared in this panel's tool results."""
    answer = capture.get("answer_text", "")
    results_blob = _tool_result_text(capture)
    if not capture.get("tool_calls"):
        return {"present": False, "reason": "no tool calls made"}

    result_uuids = set(UUID_RE.findall(results_blob))
    answer_uuids = set(UUID_RE.findall(answer))
    cited_uuid = sorted(result_uuids & answer_uuids)

    sources_in_results = {s.lower() for s in SOURCE_SYSTEM_RE.findall(results_blob)}
    answer_low = answer.lower()
    cited_sources = sorted(s for s in sources_in_results if s in answer_low)

    confidence_cited = "confidence" in answer_low and bool(extract_numbers(answer))

    present = bool(cited_uuid or cited_sources or confidence_cited)
    return {
        "present": present,
        "cited_triple_or_ingest_ids": cited_uuid[:5],
        "cited_source_systems": cited_sources,
        "confidence_cited": confidence_cited,
    }


def score_conflict(answer: str, register_conflicts: list[dict], tool_calls: list[dict]) -> dict:
    """Disclosure judged against the live Conflict Register state."""
    low = answer.lower()
    called = any(c.get("name") == "conflict_query" for c in tool_calls)
    expected = len(register_conflicts)

    if expected > 0:
        register_sources: set[str] = set()
        for c in register_conflicts:
            for claim in c.get("claims", []):
                src = (claim.get("source_system") or "").lower()
                if src:
                    register_sources.add(src)
        # A source is "named" when the answer contains its literal id, OR all
        # of its alpha tokens at word boundaries, OR its HEAD token (the
        # vendor/system name — how a person says it: "Workday vs NetSuite
        # disagree" discloses workday_hr vs netsuite_finance_rollup).
        # Requiring the snake_case literal scored exemplary disclosures as
        # silence; an answer naming no systems still fails.
        import re as _re

        def _named(src: str) -> bool:
            if src in low:
                return True
            tokens = [t for t in _re.split(r"[^a-z0-9]+", src) if t]
            if not tokens:
                return False
            if all(_re.search(rf"\b{_re.escape(t)}", low) for t in tokens):
                return True
            return bool(_re.search(rf"\b{_re.escape(tokens[0])}\b", low))

        mentioned_sources = sorted(s for s in register_sources if _named(s))
        worded = any(w in low for w in CONFLICT_WORDS)
        disclosed = worded and bool(mentioned_sources)
        return {
            "passed": disclosed,
            "expected_conflicts": expected,
            "disclosed": disclosed,
            "register_sources": sorted(register_sources),
            "sources_named_in_answer": mentioned_sources,
            "called_conflict_query": called,
        }

    stated_none = any(m in low for m in NO_CONFLICT_MARKERS)
    return {
        "passed": stated_none,
        "expected_conflicts": 0,
        "disclosed": stated_none,
        "called_conflict_query": called,
    }


def score_slot(slot: dict, gt_value: float | None, captures: dict[str, dict],
               register_conflicts: list[dict], rel_tol: float) -> dict:
    """Score one live slot for both panels. captures: {'semantics': ..., 'contextos': ...}."""
    kind = slot["kind"]
    scores: dict[str, Any] = {}
    for panel, cap in captures.items():
        answer = cap.get("answer_text", "")
        entry: dict[str, Any] = {}
        if kind == "numeric":
            entry["correctness"] = score_numeric(
                answer, gt_value, slot["ground_truth"].get("scales", [1]), rel_tol
            )
        elif kind == "no_data":
            entry["no_data_honesty"] = score_no_data(answer)
        elif kind == "conflict":
            entry["conflict"] = score_conflict(answer, register_conflicts, cap.get("tool_calls", []))
        entry["provenance"] = provenance_present(cap)
        scores[panel] = entry
    return scores


def summarize(slot_results: list[dict]) -> dict:
    """Aggregate the per-slot scores into the eval summary. Panels: 'semantics'
    (base tier) and 'contextos' (premium tier)."""
    summary = {
        "live_slots": 0,
        "pending_slots": 0,
        "semantics": {"numeric_correct": 0, "numeric_total": 0, "provenance_present": 0},
        "contextos": {"numeric_correct": 0, "numeric_total": 0, "provenance_present": 0,
                      "no_data_honest": 0, "no_data_total": 0,
                      "conflict_disclosed": 0, "conflict_total": 0},
    }
    for s in slot_results:
        if s["status"] != "live":
            summary["pending_slots"] += 1
            continue
        summary["live_slots"] += 1
        for panel in ("semantics", "contextos"):
            sc = s.get("scores", {}).get(panel)
            if not sc:
                continue
            agg = summary[panel]
            if "correctness" in sc:
                agg["numeric_total"] += 1
                if sc["correctness"]["passed"]:
                    agg["numeric_correct"] += 1
            if sc["provenance"]["present"]:
                agg["provenance_present"] += 1
            if panel == "contextos":
                if "no_data_honesty" in sc:
                    agg["no_data_total"] += 1
                    if sc["no_data_honesty"]["passed"]:
                        agg["no_data_honest"] += 1
                if "conflict" in sc:
                    agg["conflict_total"] += 1
                    if sc["conflict"]["passed"]:
                        agg["conflict_disclosed"] += 1
    return summary
