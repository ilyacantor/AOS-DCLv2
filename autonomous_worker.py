#!/usr/bin/env python3
"""
Autonomous Engineering Worker

A script that runs the engineering loop by calling Claude API directly.
Claude thinks, this script acts.

Usage:
    python autonomous_worker.py run          # Run continuous loop
    python autonomous_worker.py once         # Run one cycle
    python autonomous_worker.py observe      # Run just OBSERVE stage

Environment Variables:
    ANTHROPIC_API_KEY    - Your Claude API key (required)
    SLACK_WEBHOOK_URL    - Slack webhook for notifications (optional)
    REPO_PATH            - Path to your repo (default: current directory)
    CHECK_INTERVAL       - Minutes between cycles (default: 60)

Install:
    pip install anthropic httpx pyyaml slack-sdk
"""

import os
import re
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
import yaml

import anthropic
import httpx

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    repo_path: str = "."
    state_file: str = "STATE.md"
    api_key: str = ""
    slack_webhook: Optional[str] = None
    nlq_endpoint: str = "http://localhost:5000/api/nlq/registry/execute"
    check_interval_minutes: int = 60
    max_cycles_before_hitl: int = 5
    model: str = "claude-sonnet-4-20250514"
    
    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            repo_path=os.getenv("REPO_PATH", "."),
            api_key=os.getenv("ANTHROPIC_API_KEY", ""),
            slack_webhook=os.getenv("SLACK_WEBHOOK_URL"),
            nlq_endpoint=os.getenv("NLQ_ENDPOINT", "http://localhost:5000/api/nlq/registry/execute"),
            check_interval_minutes=int(os.getenv("CHECK_INTERVAL", "60")),
        )


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

@dataclass
class Violation:
    id: str
    invariant: str
    observed: str
    expected: str
    severity: str = "CRITICAL"


@dataclass 
class State:
    stage: str = "OBSERVE"
    violation: Optional[Dict] = None
    diagnosis: Optional[Dict] = None
    plan: Optional[Dict] = None
    act: Optional[Dict] = None
    reflect: Optional[Dict] = None
    history: List[Dict] = None
    
    def __post_init__(self):
        if self.history is None:
            self.history = []


def read_state(config: Config) -> State:
    """Read and parse STATE.md"""
    state_path = Path(config.repo_path) / config.state_file
    
    if not state_path.exists():
        return State()
    
    content = state_path.read_text()
    
    # Extract stage
    stage_match = re.search(r'STAGE:\s*(\w+)', content)
    stage = stage_match.group(1) if stage_match else "OBSERVE"
    
    # Extract YAML blocks
    violation = extract_yaml_block(content, "Active Violation")
    diagnosis = extract_yaml_block(content, "Diagnosis")
    plan = extract_yaml_block(content, "Plan")
    act = extract_yaml_block(content, "Act")
    reflect = extract_yaml_block(content, "Reflect")
    
    return State(
        stage=stage,
        violation=violation,
        diagnosis=diagnosis,
        plan=plan,
        act=act,
        reflect=reflect,
    )


def extract_yaml_block(content: str, section_name: str) -> Optional[Dict]:
    """Extract a YAML block from a markdown section."""
    pattern = rf'## {section_name}.*?```yaml\s*(.*?)```'
    match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
    if match:
        try:
            return yaml.safe_load(match.group(1))
        except:
            return None
    return None


def write_state(config: Config, state: State, updates: Dict[str, Any]):
    """Update STATE.md with new information."""
    timestamp = datetime.now().isoformat()
    
    state_path = Path(config.repo_path) / config.state_file
    
    # Build the new STATE.md content
    content = f"""# Engineering Loop State

**Last Updated:** {timestamp}
**Last Updated By:** Autonomous Worker

---

## Current Stage

```
STAGE: {updates.get('stage', state.stage)}
```

---

## Active Violation

```yaml
{yaml.dump(updates.get('violation', state.violation), default_flow_style=False) if updates.get('violation', state.violation) else 'null'}
```

---

## Diagnosis

```yaml
{yaml.dump(updates.get('diagnosis', state.diagnosis), default_flow_style=False) if updates.get('diagnosis', state.diagnosis) else 'null'}
```

---

## Plan

```yaml
{yaml.dump(updates.get('plan', state.plan), default_flow_style=False) if updates.get('plan', state.plan) else 'null'}
```

---

## Act

```yaml
{yaml.dump(updates.get('act', state.act), default_flow_style=False) if updates.get('act', state.act) else 'null'}
```

---

## Reflect

```yaml
{yaml.dump(updates.get('reflect', state.reflect), default_flow_style=False) if updates.get('reflect', state.reflect) else 'null'}
```

---

## Session History

| Timestamp | Stage | Action | Result |
|-----------|-------|--------|--------|
"""
    
    # Add history
    history = state.history or []
    if 'history_entry' in updates:
        history.append(updates['history_entry'])
    
    for entry in history[-20:]:  # Keep last 20 entries
        content += f"| {entry.get('timestamp', '')} | {entry.get('stage', '')} | {entry.get('action', '')} | {entry.get('result', '')} |\n"
    
    content += """
---

## Next Action

"""
    content += updates.get('next_action', 'Awaiting next cycle.')
    
    state_path.write_text(content)
    return state_path


# =============================================================================
# CANARY QUERIES
# =============================================================================

CANARY_QUERIES = [
    "What was revenue last year?",
    "Top 5 customers by revenue",
    "What is our burn rate?",
    "Show me zombie resources",
]

INVARIANTS = {
    "INV-001": {
        "name": "confidence_range",
        "check": lambda r: 0.0 <= (r.get("confidence_score") or r.get("confidence") or 0) <= 1.0,
        "expected": "[0.0, 1.0]",
        "severity": "CRITICAL",
    },
    "INV-002": {
        "name": "execution_timeout",
        "check": lambda r: (r.get("execution_time_ms") or 0) < 5000,
        "expected": "< 5000ms",
        "severity": "HIGH",
    },
    "INV-003": {
        "name": "row_aggregation_consistency",
        "check": lambda r: not (
            r.get("metadata", {}).get("row_count", 0) == 0 
            and r.get("summary", {}).get("aggregations")
            and any(v for v in r.get("summary", {}).get("aggregations", {}).values() if v not in [None, 0, "", []])
        ),
        "expected": "row_count=0 implies empty aggregations",
        "severity": "CRITICAL",
    },
    "INV-009": {
        "name": "answer_backed_by_data",
        "check": lambda r: not (
            r.get("summary", {}).get("answer") 
            and "$" in str(r.get("summary", {}).get("answer", ""))
            and r.get("metadata", {}).get("row_count", 0) == 0
        ),
        "expected": "dollar amounts require row_count > 0",
        "severity": "CRITICAL",
    },
}


def run_canary_queries(config: Config) -> List[Dict]:
    """Execute canary queries and check invariants."""
    results = []
    
    for query in CANARY_QUERIES:
        try:
            response = httpx.post(
                config.nlq_endpoint,
                json={"question": query, "dataset_id": "demo9"},
                timeout=30.0
            )
            
            if response.status_code == 200:
                data = response.json()
                violations = []
                
                for inv_id, inv in INVARIANTS.items():
                    if not inv["check"](data):
                        violations.append({
                            "invariant_id": inv_id,
                            "name": inv["name"],
                            "expected": inv["expected"],
                            "severity": inv["severity"],
                        })
                
                results.append({
                    "query": query,
                    "status": "success",
                    "confidence": data.get("confidence_score") or data.get("confidence"),
                    "row_count": data.get("metadata", {}).get("row_count"),
                    "definition_id": data.get("definition_id"),
                    "violations": violations,
                    "raw": data,
                })
            else:
                results.append({
                    "query": query,
                    "status": "error",
                    "error": f"HTTP {response.status_code}",
                    "violations": [{"invariant_id": "INV-005", "name": "no_errors", "expected": "status_code 200", "severity": "CRITICAL"}],
                })
                
        except Exception as e:
            results.append({
                "query": query,
                "status": "error", 
                "error": str(e),
                "violations": [{"invariant_id": "INV-005", "name": "no_errors", "expected": "no exceptions", "severity": "CRITICAL"}],
            })
    
    return results


# =============================================================================
# CLAUDE API INTERACTION
# =============================================================================

def call_claude(config: Config, system_prompt: str, user_prompt: str) -> Dict:
    """Call Claude API and parse the response."""
    client = anthropic.Anthropic(api_key=config.api_key)
    
    message = client.messages.create(
        model=config.model,
        max_tokens=4096,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}]
    )
    
    response_text = message.content[0].text
    
    # Try to extract JSON from the response
    json_match = re.search(r'```json\s*(.*?)```', response_text, re.DOTALL)
    if json_match:
        try:
            return {"parsed": json.loads(json_match.group(1)), "raw": response_text}
        except:
            pass
    
    # Try to extract YAML
    yaml_match = re.search(r'```yaml\s*(.*?)```', response_text, re.DOTALL)
    if yaml_match:
        try:
            return {"parsed": yaml.safe_load(yaml_match.group(1)), "raw": response_text}
        except:
            pass
    
    return {"parsed": None, "raw": response_text}


SYSTEM_PROMPT = """You are an engineering analysis assistant. You analyze code and provide structured recommendations.

You MUST respond with a valid JSON or YAML block containing your analysis.

You do NOT execute changes. You only analyze and recommend. The calling system will execute approved changes.

Be specific. Name exact files, functions, and line numbers. Provide evidence for your conclusions."""


def build_diagnose_prompt(violation: Dict, repo_path: str) -> str:
    """Build the prompt for DIAGNOSE stage."""
    
    # Read relevant source files - prioritize executor since that's where summaries are computed
    files_to_read = [
        Path(repo_path) / "backend" / "nlq" / "executor.py",
        Path(repo_path) / "backend" / "bll" / "executor.py",
        Path(repo_path) / "backend" / "nlq" / "intent_matcher.py",
        Path(repo_path) / "backend" / "nlq" / "scorer.py",
    ]
    
    context = ""
    for path in files_to_read:
        if path.exists():
            content = path.read_text()
            # Truncate if too long
            if len(content) > 8000:
                content = content[:8000] + "\n... (truncated)"
            context += f"\n\n### {path}\n```python\n{content}\n```"
    
    # Build violation-specific guidance
    inv_id = violation.get('invariant', '')
    
    if inv_id == "INV-009" or "dollar" in violation.get('expected', '').lower():
        specific_guidance = """
## IMPORTANT CONTEXT

This violation is about the system returning dollar amounts (like "$419.00M") in the answer 
when row_count=0. The problem is NOT in confidence scoring or intent matching.

The problem is in the EXECUTOR - specifically where it computes the summary/answer.
Look for where the "answer" string is built and where aggregations are computed.
The fix should be: if row_count == 0, do not include dollar amounts in the answer.
"""
    elif inv_id == "INV-003":
        specific_guidance = """
## IMPORTANT CONTEXT

This violation is about returning non-empty aggregations when row_count=0.
Look in the EXECUTOR where aggregations are computed. If no rows are returned,
aggregations should be empty or null.
"""
    else:
        specific_guidance = ""
    
    return f"""## DIAGNOSE TASK

An invariant violation was detected:

**Invariant:** {violation.get('invariant')} - {violation.get('name', '')}
**Observed:** {violation.get('observed')}  
**Expected:** {violation.get('expected')}
**Query:** {violation.get('query', 'N/A')}
{specific_guidance}

## Source Code Context
{context}

## Your Task

1. Trace where the answer/summary is generated
2. Identify the exact file, function, and line range where the bug is
3. Explain why the system returns invalid data
4. Estimate fix complexity

## Required Output Format

```json
{{
  "diagnosis_id": "diag_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
  "component": "backend/[path]/[file].py",
  "function": "[function_name]",
  "line_range": "[start]-[end]",
  "root_cause": "[specific explanation]",
  "evidence": ["[evidence 1]", "[evidence 2]"],
  "confidence": 0.0-1.0,
  "complexity": "low|medium|high"
}}
```
"""


def build_plan_prompt(diagnosis: Dict, repo_path: str) -> str:
    """Build the prompt for PLAN stage."""
    
    # Read the specific file mentioned in diagnosis
    component = diagnosis.get("component", "")
    file_path = Path(repo_path) / component
    
    file_content = ""
    if file_path.exists():
        file_content = file_path.read_text()
        if len(file_content) > 15000:
            file_content = file_content[:15000] + "\n... (truncated)"
    
    return f"""## PLAN TASK

Based on this diagnosis:

**Component:** {diagnosis.get('component')}
**Function:** {diagnosis.get('function')}
**Line Range:** {diagnosis.get('line_range')}
**Root Cause:** {diagnosis.get('root_cause')}

## Source Code

```python
{file_content}
```

## Your Task

Design a MINIMAL fix. Do not refactor. Do not improve. Just fix the specific issue.

## Required Output Format

```json
{{
  "plan_id": "plan_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
  "strategy": "[what will be changed and why]",
  "edits": [
    {{
      "file": "[path]",
      "line": [line_number],
      "action": "replace|insert|delete",
      "old_text": "[exact text to find]",
      "new_text": "[replacement text]",
      "reason": "[why this change]"
    }}
  ],
  "success_criteria": ["[criterion 1]", "[criterion 2]"],
  "risk_level": "low|medium|high",
  "rollback": "git checkout [file]"
}}
```
"""


# =============================================================================
# ACTION EXECUTION
# =============================================================================

def execute_edit(repo_path: str, edit: Dict) -> Dict:
    """Execute a single file edit."""
    file_path = Path(repo_path) / edit["file"]
    
    if not file_path.exists():
        return {"success": False, "error": f"File not found: {edit['file']}"}
    
    content = file_path.read_text()
    old_text = edit.get("old_text", "")
    new_text = edit.get("new_text", "")
    
    if edit["action"] == "replace":
        if old_text not in content:
            return {"success": False, "error": f"Old text not found in {edit['file']}"}
        
        # Check it only appears once (to avoid ambiguous replacements)
        if content.count(old_text) > 1:
            return {"success": False, "error": f"Old text appears multiple times in {edit['file']}"}
        
        new_content = content.replace(old_text, new_text)
        file_path.write_text(new_content)
        
        return {"success": True, "file": edit["file"], "action": "replaced"}
    
    elif edit["action"] == "insert":
        # Insert after a specific line
        lines = content.split("\n")
        line_num = edit.get("line", 0)
        if 0 < line_num <= len(lines):
            lines.insert(line_num, new_text)
            file_path.write_text("\n".join(lines))
            return {"success": True, "file": edit["file"], "action": "inserted"}
        return {"success": False, "error": f"Invalid line number: {line_num}"}
    
    elif edit["action"] == "delete":
        if old_text not in content:
            return {"success": False, "error": f"Text to delete not found in {edit['file']}"}
        new_content = content.replace(old_text, "")
        file_path.write_text(new_content)
        return {"success": True, "file": edit["file"], "action": "deleted"}
    
    return {"success": False, "error": f"Unknown action: {edit['action']}"}


def run_git_commit(repo_path: str, message: str, files: List[str]) -> Dict:
    """Commit changes to git."""
    try:
        # Add files
        for f in files:
            subprocess.run(["git", "add", f], cwd=repo_path, check=True)
        
        # Commit
        subprocess.run(["git", "commit", "-m", message], cwd=repo_path, check=True)
        
        # Get commit hash
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], 
            cwd=repo_path, 
            capture_output=True, 
            text=True
        )
        
        return {"success": True, "commit": result.stdout.strip()[:8]}
    except subprocess.CalledProcessError as e:
        return {"success": False, "error": str(e)}


def run_tests(repo_path: str, test_path: str = "tests/nlq/") -> Dict:
    """Run pytest and return results."""
    try:
        result = subprocess.run(
            ["python", "-m", "pytest", test_path, "-v", "--tb=short"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        # Parse results
        passed = len(re.findall(r' PASSED', result.stdout))
        failed = len(re.findall(r' FAILED', result.stdout))
        
        return {
            "success": result.returncode == 0,
            "passed": passed,
            "failed": failed,
            "output": result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "Test timeout"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# =============================================================================
# NOTIFICATIONS
# =============================================================================

def send_slack_notification(config: Config, message: str, level: str = "info"):
    """Send a Slack notification."""
    if not config.slack_webhook:
        print(f"[{level.upper()}] {message}")
        return
    
    emoji = {"info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "🚨"}.get(level, "📢")
    
    try:
        httpx.post(config.slack_webhook, json={
            "text": f"{emoji} *Autonomous Worker*\n{message}"
        })
    except:
        print(f"Failed to send Slack notification: {message}")


# =============================================================================
# MAIN LOOP
# =============================================================================

def run_observe(config: Config, state: State) -> Dict:
    """Execute OBSERVE stage."""
    print("\n" + "="*60)
    print("STAGE: OBSERVE")
    print("="*60)
    
    results = run_canary_queries(config)
    
    # Collect all violations
    all_violations = []
    for r in results:
        for v in r.get("violations", []):
            v["query"] = r["query"]
            v["observed"] = r.get("confidence") or r.get("error")
            all_violations.append(v)
    
    print(f"\nCanary results:")
    for r in results:
        status = "✅" if not r.get("violations") else "❌"
        print(f"  {status} {r['query'][:40]}... - {len(r.get('violations', []))} violations")
    
    if all_violations:
        # Pick the first critical violation
        critical = [v for v in all_violations if v.get("severity") == "CRITICAL"]
        primary = critical[0] if critical else all_violations[0]
        
        return {
            "stage": "DIAGNOSE",
            "violation": {
                "id": f"V-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                "invariant": primary.get("invariant_id", "UNKNOWN"),
                "name": primary.get("name", "unknown"),
                "observed": str(primary.get("observed", "N/A")),
                "expected": primary.get("expected", "N/A"),
                "query": primary.get("query", "N/A"),
                "severity": primary.get("severity", "CRITICAL"),
            },
            "history_entry": {
                "timestamp": datetime.now().isoformat(),
                "stage": "OBSERVE",
                "action": "Ran canary queries",
                "result": f"{len(all_violations)} violations found",
            },
            "next_action": f"Proceeding to DIAGNOSE for {primary['invariant_id']}",
        }
    else:
        return {
            "stage": "IDLE",
            "violation": None,
            "history_entry": {
                "timestamp": datetime.now().isoformat(),
                "stage": "OBSERVE",
                "action": "Ran canary queries",
                "result": "All invariants pass",
            },
            "next_action": "System healthy. No action needed.",
        }


def run_diagnose(config: Config, state: State) -> Dict:
    """Execute DIAGNOSE stage."""
    print("\n" + "="*60)
    print("STAGE: DIAGNOSE")
    print("="*60)
    
    if not state.violation:
        return {"stage": "OBSERVE", "next_action": "No violation to diagnose. Returning to OBSERVE."}
    
    prompt = build_diagnose_prompt(state.violation, config.repo_path)
    response = call_claude(config, SYSTEM_PROMPT, prompt)
    
    diagnosis = response.get("parsed")
    if not diagnosis:
        print("Failed to parse Claude response. Raw output:")
        print(response.get("raw", "")[:1000])
        return {
            "stage": "DIAGNOSE",
            "history_entry": {
                "timestamp": datetime.now().isoformat(),
                "stage": "DIAGNOSE",
                "action": "Called Claude API",
                "result": "Failed to parse response",
            },
            "next_action": "Diagnosis failed. Retry or manual intervention needed.",
        }
    
    print(f"\nDiagnosis:")
    print(f"  Component: {diagnosis.get('component')}")
    print(f"  Function: {diagnosis.get('function')}")
    print(f"  Root cause: {diagnosis.get('root_cause', '')[:100]}...")
    print(f"  Confidence: {diagnosis.get('confidence')}")
    
    return {
        "stage": "PLAN",
        "diagnosis": diagnosis,
        "history_entry": {
            "timestamp": datetime.now().isoformat(),
            "stage": "DIAGNOSE",
            "action": f"Diagnosed {diagnosis.get('component')}",
            "result": f"Confidence: {diagnosis.get('confidence')}",
        },
        "next_action": "Proceeding to PLAN.",
    }


def run_plan(config: Config, state: State) -> Dict:
    """Execute PLAN stage."""
    print("\n" + "="*60)
    print("STAGE: PLAN")
    print("="*60)
    
    if not state.diagnosis:
        return {"stage": "DIAGNOSE", "next_action": "No diagnosis. Returning to DIAGNOSE."}
    
    prompt = build_plan_prompt(state.diagnosis, config.repo_path)
    response = call_claude(config, SYSTEM_PROMPT, prompt)
    
    plan = response.get("parsed")
    if not plan:
        print("Failed to parse Claude response.")
        return {
            "stage": "PLAN",
            "history_entry": {
                "timestamp": datetime.now().isoformat(),
                "stage": "PLAN",
                "action": "Called Claude API",
                "result": "Failed to parse response",
            },
            "next_action": "Planning failed. Retry or manual intervention needed.",
        }
    
    print(f"\nPlan:")
    print(f"  Strategy: {plan.get('strategy', '')[:100]}...")
    print(f"  Edits: {len(plan.get('edits', []))}")
    print(f"  Risk: {plan.get('risk_level')}")
    
    # Check if HITL approval needed
    if plan.get("risk_level") == "high":
        send_slack_notification(config, 
            f"🔔 High-risk plan requires approval:\n{plan.get('strategy', '')[:200]}",
            "warning"
        )
        return {
            "stage": "CHECKPOINT",
            "plan": plan,
            "history_entry": {
                "timestamp": datetime.now().isoformat(),
                "stage": "PLAN",
                "action": "Created high-risk plan",
                "result": "Awaiting HITL approval",
            },
            "next_action": "High-risk plan. Awaiting human approval.",
        }
    
    return {
        "stage": "ACT",
        "plan": plan,
        "history_entry": {
            "timestamp": datetime.now().isoformat(),
            "stage": "PLAN",
            "action": "Created plan",
            "result": f"{len(plan.get('edits', []))} edits, risk={plan.get('risk_level')}",
        },
        "next_action": "Proceeding to ACT.",
    }


def run_act(config: Config, state: State) -> Dict:
    """Execute ACT stage."""
    print("\n" + "="*60)
    print("STAGE: ACT")
    print("="*60)
    
    if not state.plan:
        return {"stage": "PLAN", "next_action": "No plan. Returning to PLAN."}
    
    edits = state.plan.get("edits", [])
    results = []
    modified_files = []
    
    for edit in edits:
        print(f"\n  Applying edit to {edit.get('file')}...")
        result = execute_edit(config.repo_path, edit)
        results.append(result)
        
        if result["success"]:
            modified_files.append(edit["file"])
            print(f"    ✅ {result.get('action')}")
        else:
            print(f"    ❌ {result.get('error')}")
    
    # Check if all edits succeeded
    all_success = all(r["success"] for r in results)
    
    if not all_success:
        # Rollback
        rollback_cmd = state.plan.get("rollback", "")
        if rollback_cmd:
            subprocess.run(rollback_cmd, shell=True, cwd=config.repo_path)
        
        return {
            "stage": "PLAN",
            "act": {"success": False, "results": results},
            "history_entry": {
                "timestamp": datetime.now().isoformat(),
                "stage": "ACT",
                "action": "Attempted edits",
                "result": "Failed - rolled back",
            },
            "next_action": "Edit failed. Returning to PLAN.",
        }
    
    # Commit
    commit_result = run_git_commit(
        config.repo_path,
        f"FIX: {state.diagnosis.get('diagnosis_id', 'unknown')} - {state.plan.get('strategy', '')[:50]}",
        modified_files + [config.state_file]
    )
    
    return {
        "stage": "REFLECT",
        "act": {
            "success": True,
            "results": results,
            "commit": commit_result.get("commit"),
            "files_modified": modified_files,
        },
        "history_entry": {
            "timestamp": datetime.now().isoformat(),
            "stage": "ACT",
            "action": f"Applied {len(edits)} edits",
            "result": f"Commit: {commit_result.get('commit', 'N/A')}",
        },
        "next_action": "Proceeding to REFLECT.",
    }


def run_reflect(config: Config, state: State) -> Dict:
    """Execute REFLECT stage."""
    print("\n" + "="*60)
    print("STAGE: REFLECT")
    print("="*60)
    
    # Re-run canary queries
    print("\nRe-running canary queries...")
    results = run_canary_queries(config)
    
    # Check violations
    all_violations = []
    for r in results:
        for v in r.get("violations", []):
            v["query"] = r["query"]
            all_violations.append(v)
    
    # Run tests
    print("\nRunning tests...")
    test_results = run_tests(config.repo_path)
    print(f"  Tests: {test_results.get('passed', 0)} passed, {test_results.get('failed', 0)} failed")
    
    # Determine outcome
    original_inv = state.violation.get("invariant") if state.violation else None
    original_resolved = not any(v["invariant_id"] == original_inv for v in all_violations)
    
    if original_resolved and not all_violations:
        outcome = "SUCCESS"
    elif original_resolved:
        outcome = "PARTIAL"
    elif len(all_violations) > len(state.history or []):
        outcome = "WORSE"
    else:
        outcome = "FAILURE"
    
    print(f"\nOutcome: {outcome}")
    print(f"  Original violation resolved: {original_resolved}")
    print(f"  Remaining violations: {len(all_violations)}")
    
    return {
        "stage": "CHECKPOINT",
        "reflect": {
            "outcome": outcome,
            "original_resolved": original_resolved,
            "violations_remaining": len(all_violations),
            "test_passed": test_results.get("passed", 0),
            "test_failed": test_results.get("failed", 0),
        },
        "history_entry": {
            "timestamp": datetime.now().isoformat(),
            "stage": "REFLECT",
            "action": "Verified fix",
            "result": outcome,
        },
        "next_action": "Proceeding to CHECKPOINT.",
    }


def run_checkpoint(config: Config, state: State, cycle_count: int) -> Dict:
    """Execute CHECKPOINT stage."""
    print("\n" + "="*60)
    print("STAGE: CHECKPOINT")
    print("="*60)
    
    reflect = state.reflect or {}
    outcome = reflect.get("outcome", "UNKNOWN")
    
    summary = f"""
Cycle {cycle_count} complete.
Outcome: {outcome}
Violation addressed: {state.violation.get('invariant') if state.violation else 'N/A'}
Tests: {reflect.get('test_passed', 0)} passed, {reflect.get('test_failed', 0)} failed
Violations remaining: {reflect.get('violations_remaining', 'unknown')}
"""
    
    print(summary)
    
    # Notify
    level = "success" if outcome == "SUCCESS" else "warning" if outcome == "PARTIAL" else "error"
    send_slack_notification(config, summary, level)
    
    # Decide next action
    if outcome in ["SUCCESS", "PARTIAL"]:
        if reflect.get("violations_remaining", 0) > 0:
            next_stage = "OBSERVE"
            next_action = "Starting new cycle for remaining violations."
        else:
            next_stage = "IDLE"
            next_action = "All violations resolved. System healthy."
    elif outcome == "WORSE":
        next_stage = "CHECKPOINT"
        next_action = "Fix made things worse. HITL intervention required."
        send_slack_notification(config, "🚨 Fix made things worse! Manual intervention required.", "error")
    else:
        next_stage = "DIAGNOSE"
        next_action = "Fix didn't work. Returning to DIAGNOSE with new information."
    
    # Check HITL thresholds
    if cycle_count >= config.max_cycles_before_hitl:
        next_stage = "CHECKPOINT"
        next_action = f"Reached {cycle_count} cycles. HITL checkpoint required."
        send_slack_notification(config, f"🔔 Reached {cycle_count} cycles. Please review.", "warning")
    
    return {
        "stage": next_stage,
        "violation": None if next_stage == "IDLE" else state.violation,
        "diagnosis": None if next_stage in ["IDLE", "OBSERVE"] else state.diagnosis,
        "plan": None,
        "act": None,
        "reflect": None,
        "history_entry": {
            "timestamp": datetime.now().isoformat(),
            "stage": "CHECKPOINT",
            "action": f"Cycle {cycle_count} complete",
            "result": outcome,
        },
        "next_action": next_action,
    }


def run_cycle(config: Config) -> bool:
    """Run one complete cycle. Returns True if should continue."""
    state = read_state(config)
    print(f"\nCurrent stage: {state.stage}")
    
    stage_handlers = {
        "OBSERVE": run_observe,
        "DIAGNOSE": run_diagnose,
        "PLAN": run_plan,
        "ACT": run_act,
        "REFLECT": run_reflect,
    }
    
    if state.stage == "IDLE":
        print("System is IDLE. Running OBSERVE to check health...")
        state.stage = "OBSERVE"
    
    if state.stage == "CHECKPOINT":
        # Need cycle count from history
        cycle_count = len([h for h in (state.history or []) if h.get("stage") == "CHECKPOINT"]) + 1
        updates = run_checkpoint(config, state, cycle_count)
    elif state.stage in stage_handlers:
        updates = stage_handlers[state.stage](config, state)
    else:
        print(f"Unknown stage: {state.stage}")
        return False
    
    # Update state
    write_state(config, state, updates)
    
    # Continue if not waiting for HITL
    return updates.get("stage") not in ["IDLE", "CHECKPOINT"] or \
           "HITL" not in updates.get("next_action", "")


def main():
    """Main entry point."""
    import sys
    
    config = Config.from_env()
    
    if not config.api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set")
        sys.exit(1)
    
    command = sys.argv[1] if len(sys.argv) > 1 else "once"
    
    if command == "run":
        # Continuous loop
        print("Starting autonomous engineering loop...")
        print(f"Check interval: {config.check_interval_minutes} minutes")
        
        while True:
            try:
                cycle_count = 0
                while run_cycle(config):
                    cycle_count += 1
                    if cycle_count > 10:  # Safety limit per run
                        print("Reached 10 stages in one run. Pausing.")
                        break
                
                print(f"\nSleeping for {config.check_interval_minutes} minutes...")
                time.sleep(config.check_interval_minutes * 60)
                
            except KeyboardInterrupt:
                print("\nStopped by user.")
                break
            except Exception as e:
                print(f"\nError: {e}")
                send_slack_notification(config, f"Error: {e}", "error")
                time.sleep(300)  # Wait 5 min on error
    
    elif command == "once":
        # Single cycle
        run_cycle(config)
    
    elif command == "observe":
        # Just run observe
        state = read_state(config)
        updates = run_observe(config, state)
        write_state(config, state, updates)
    
    else:
        print(f"Unknown command: {command}")
        print("Usage: python autonomous_worker.py [run|once|observe]")
        sys.exit(1)


if __name__ == "__main__":
    main()
