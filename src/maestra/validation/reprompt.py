"""
Maestra Layer 0 — Reprompt loop for LLM agent output validation.

When validation fails, constructs an error payload from the ValidationResult,
appends it to the original prompt, and resubmits to the same LLM agent.
Maximum 3 attempts. Logs every attempt.
"""

import hashlib
import logging
from typing import Callable

from .rules import validate
from .schema import FinancialOutput, ValidationResult
from .seed_coa import CoALookup

logger = logging.getLogger(__name__)


def _build_error_payload(result: ValidationResult) -> str:
    """Construct a structured error message from validation failures."""
    lines = [
        "VALIDATION FAILED — the following errors must be corrected before resubmission:",
        "",
    ]
    for i, error in enumerate(result.errors, 1):
        lines.append(f"Error {i} [{error.rule_code}] (severity: {error.severity}):")
        lines.append(f"  Message: {error.message}")
        lines.append(f"  Failing data: {error.failing_data}")
        if error.variance is not None:
            lines.append(f"  Variance: {error.variance}")
        lines.append("")

    lines.append(
        "Correct ALL errors above and regenerate the financial output. "
        "Ensure all monetary values use exact decimal arithmetic. "
        "Do not approximate."
    )
    return "\n".join(lines)


def _prompt_hash(prompt: str) -> str:
    """Compute a short hash of the prompt for logging."""
    return hashlib.sha256(prompt.encode()).hexdigest()[:16]


async def reprompt_loop(
    agent_fn: Callable,
    initial_prompt: str,
    max_attempts: int = 3,
    coa: CoALookup | None = None,
) -> tuple[FinancialOutput | None, list[ValidationResult]]:
    """Run the LLM agent, validate, and reprompt on failure.

    Args:
        agent_fn: Async callable that takes a prompt string and returns
                  a FinancialOutput. Must raise on LLM failure — no silent fallbacks.
        initial_prompt: The original prompt to send to the agent.
        max_attempts: Maximum number of attempts before halting. Default 3.
        coa: CoA lookup table for validation rule V-002.

    Returns:
        Tuple of (validated_output_or_None, list_of_all_validation_attempts).
        If all attempts fail, output is None and the list contains all
        ValidationResults showing accumulated errors.
    """
    all_results: list[ValidationResult] = []
    current_prompt = initial_prompt

    for attempt in range(1, max_attempts + 1):
        prompt_id = _prompt_hash(current_prompt)

        logger.info(
            "Reprompt loop attempt %d/%d — prompt_hash=%s",
            attempt, max_attempts, prompt_id,
        )

        # Call the LLM agent — must not silently fail
        output = await agent_fn(current_prompt)

        # Validate the output
        result = validate(output, coa=coa)
        all_results.append(result)

        logger.info(
            "Attempt %d/%d — valid=%s, error_count=%d, prompt_hash=%s",
            attempt, max_attempts, result.valid, len(result.errors), prompt_id,
        )

        if result.valid:
            logger.info(
                "Validation passed on attempt %d/%d", attempt, max_attempts
            )
            return output, all_results

        # Build error payload and append to prompt for next attempt
        error_payload = _build_error_payload(result)
        current_prompt = f"{current_prompt}\n\n{error_payload}"

        logger.warning(
            "Attempt %d/%d failed with %d errors (halt=%d, warning=%d). %s",
            attempt,
            max_attempts,
            len(result.errors),
            sum(1 for e in result.errors if e.severity == "halt"),
            sum(1 for e in result.errors if e.severity == "warning"),
            "Reprompting..." if attempt < max_attempts else "Max attempts exhausted.",
        )

    logger.error(
        "Reprompt loop exhausted after %d attempts. "
        "Total errors across all attempts: %d",
        max_attempts,
        sum(len(r.errors) for r in all_results),
    )

    return None, all_results
