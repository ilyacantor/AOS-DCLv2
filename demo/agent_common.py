"""
Shared agent-loop machinery for the two demo panels.

Both panels run the SAME model with the same loop (§13: "same model, same
data access" — the access differs by construction, nothing else is
hobbled). The loop is the manual Messages-API tool-use loop so every tool
call is captured verbatim for the run artifact; nothing here changes what
either panel can do.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_MODEL = os.environ.get("DEMO_MODEL", "claude-opus-4-8")
MAX_TOKENS = 16000
MAX_ITERATIONS = 8
RESULT_EXCERPT_CHARS = 2000


# The demo targets the live DEV DCL backend — run_dcl_dev.sh's ${PORT:-8104} —
# never the prod instance (:8004). One guarded definition, imported by both
# panels and the sequence, so a normal `python -m demo.sequence` run needs no
# --dcl-url override.
DCL_DEV_URL_DEFAULT = "http://localhost:8104"
_PROD_DCL_PORT = "8004"


def dcl_dev_url() -> str:
    """Resolve the DCL backend the demo runs against.

    Defaults to the live dev backend (:8104 — the run_dcl_dev.sh PORT default),
    overridable with DEMO_DCL_URL only for a *non-default dev* port. Refuses the
    prod DCL port (:8004) loudly: the grounded demo runs against the dev stack
    only and must never silently hit prod (A1, dev/prod rule).
    """
    url = os.environ.get("DEMO_DCL_URL", DCL_DEV_URL_DEFAULT).rstrip("/")
    if f":{_PROD_DCL_PORT}" in url:
        raise RuntimeError(
            f"DEMO_DCL_URL={url!r} points at the PROD DCL port :{_PROD_DCL_PORT}. "
            f"The grounded demo runs against the dev stack only (default "
            f"{DCL_DEV_URL_DEFAULT}). Refusing to run against prod."
        )
    return url


def load_demo_env() -> None:
    """Resolve the demo's env exactly like the running dev stack (run_dcl_dev.sh):
    the aos-dev DB from .env.development, plus the single shared secrets that live
    in .env — DCL_MCP_TOKEN_SECRET (so the minted MCP token validates against the
    dev backend) and the account-level ANTHROPIC_API_KEY (one key, not a per-DB
    cred). Either secret may also be set in .env.development or the shell.

    The prod DB creds in .env are NEVER loaded into this process: only those two
    secrets are pulled, BY NAME, via dotenv_values (which parses without mutating
    the environment). An already-set env var always wins, so a shell override
    still works and is never clobbered.
    """
    from dotenv import dotenv_values, load_dotenv

    dev_path = REPO_ROOT / ".env.development"
    if not dev_path.exists():
        raise RuntimeError(f"{dev_path} not found — demo runs against dev only")
    load_dotenv(dev_path, override=False)  # aos-dev DB + ANTHROPIC_API_KEY

    # Two values are single shared secrets that live in .env, not .env.development:
    # DCL_MCP_TOKEN_SECRET (the dev backend sources it the same way —
    # run_dcl_dev.sh:27, "single shared dev token") and the account-level
    # ANTHROPIC_API_KEY (one key, not a per-DB cred). Pull ONLY these by name; do
    # NOT load .env wholesale, so its prod DB creds never enter this process.
    _shared = ("DCL_MCP_TOKEN_SECRET", "ANTHROPIC_API_KEY")
    if any(not os.environ.get(k) for k in _shared):
        prod_path = REPO_ROOT / ".env"
        prod_vals = dotenv_values(prod_path) if prod_path.exists() else {}
        for k in _shared:
            if not os.environ.get(k) and prod_vals.get(k):
                os.environ[k] = prod_vals[k]

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError(
            "ANTHROPIC_API_KEY missing — not in the environment, not in "
            ".env.development, and not present in .env. Both demo panels need it "
            "(account-level key, one shared value); set it in .env or "
            ".env.development, or export it. No fallback (A1)."
        )
    if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
        raise RuntimeError(
            "DCL_MCP_TOKEN_SECRET unresolved — not in the environment, not in "
            ".env.development, and not present in .env. The dev DCL backend mints "
            "and validates MCP tokens with this shared dev secret (run_dcl_dev.sh); "
            "set it in .env as the dev stack expects, or export it. No fallback (A1)."
        )


def truncate(text: str, limit: int = RESULT_EXCERPT_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n…[truncated {len(text) - limit} chars]"


async def run_agent_loop(
    *,
    model: str,
    system: str,
    question: str,
    tool_defs: list[dict],
    execute_tool: Callable[[str, dict], Awaitable[str]],
    max_iterations: int = MAX_ITERATIONS,
) -> dict[str, Any]:
    """Manual agentic loop; returns the capture fragment for one panel run.

    Raises RuntimeError on refusal/max_tokens/iteration overrun — the
    sequence records that as a failed beat; nothing is papered over.
    """
    import anthropic

    messages: list[dict] = [{"role": "user", "content": question}]
    tool_calls: list[dict] = []
    usage = {"input_tokens": 0, "output_tokens": 0}
    started = time.time()

    async with anthropic.AsyncAnthropic() as client:
        return await _loop(client, model=model, system=system, messages=messages,
                           tool_defs=tool_defs, execute_tool=execute_tool,
                           max_iterations=max_iterations, tool_calls=tool_calls,
                           usage=usage, started=started)


async def _loop(client, *, model, system, messages, tool_defs, execute_tool,
                max_iterations, tool_calls, usage, started) -> dict[str, Any]:
    for _ in range(max_iterations):
        response = await client.messages.create(
            model=model,
            max_tokens=MAX_TOKENS,
            system=system,
            thinking={"type": "adaptive"},
            tools=tool_defs,
            messages=messages,
        )
        usage["input_tokens"] += response.usage.input_tokens
        usage["output_tokens"] += response.usage.output_tokens

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                call_started = time.time()
                try:
                    result_text = await execute_tool(block.name, dict(block.input))
                    is_error = False
                except Exception as exc:  # tool failure goes BACK to the model, loudly
                    result_text = f"TOOL ERROR ({type(exc).__name__}): {exc}"
                    is_error = True
                tool_calls.append(
                    {
                        "name": block.name,
                        "arguments": dict(block.input),
                        "result_excerpt": truncate(result_text),
                        "is_error": is_error,
                        "latency_ms": int((time.time() - call_started) * 1000),
                    }
                )
                results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_text,
                        "is_error": is_error,
                    }
                )
            messages.append({"role": "user", "content": results})
            continue

        if response.stop_reason == "end_turn":
            answer = "".join(b.text for b in response.content if b.type == "text")
            return {
                "answer_text": answer,
                "tool_calls": tool_calls,
                "usage": usage,
                "model": model,
                "stop_reason": response.stop_reason,
                "elapsed_s": round(time.time() - started, 2),
            }

        raise RuntimeError(
            f"agent loop stopped with stop_reason={response.stop_reason!r} "
            f"(stop_details={getattr(response, 'stop_details', None)!r}) — "
            "not a usable answer; failing the beat loudly."
        )

    raise RuntimeError(
        f"agent loop exceeded {max_iterations} iterations without end_turn — "
        f"{len(tool_calls)} tool calls made; failing the beat loudly."
    )


def run_sync(coro):
    return asyncio.run(coro)


def emit(result: dict, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(result["answer_text"])
        print(
            f"\n--- {len(result['tool_calls'])} tool calls, "
            f"{result['usage']['input_tokens']}in/{result['usage']['output_tokens']}out tokens, "
            f"{result['elapsed_s']}s ---"
        )
