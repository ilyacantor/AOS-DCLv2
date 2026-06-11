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


def load_demo_env() -> None:
    """Load the dev env file explicitly. Never .env (prod) — dev/prod rule."""
    from dotenv import load_dotenv

    env_path = REPO_ROOT / ".env.development"
    if not env_path.exists():
        raise RuntimeError(f"{env_path} not found — demo runs against dev only")
    load_dotenv(env_path, override=False)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError(
            "ANTHROPIC_API_KEY missing after loading .env.development — "
            "both demo panels need it; aborting (A1)."
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
