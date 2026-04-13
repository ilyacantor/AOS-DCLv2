"""Static-grep guards for pipeline identity rules I1/I2/I6.

These tests scan source files instead of running the service. They catch
regressions that the pre-commit hook would also catch, so a local fix that
evades the hook still fails in CI. Keep the rules here aligned with
scripts/precommit.sh and CLAUDE.md Sections I1–I6 and F1.

What they enforce:
    - I1: the literal ``"run_id":`` response field is never emitted from API
      handlers (use namespaced ids like ``dcl_ingest_id``).
    - I6(3): no derivation / string-mangling of tenant_id or entity_id in
      the pipeline path — entity_id is passed through, not computed.
    - mig016: ``is_active`` is gone from DCL code (except the narrow
      whitelist: migrations, mig runners, source-schema files).
    - store-rebuild: ``semantic_triples`` reads outside the ingest /
      rebuild whitelist are banned (use ``current_triples``).
    - ME contamination: ``engagement_state``, ``convergence_triples``,
      ``cofa_mapping`` never appear in DCL code.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# File enumeration
# ---------------------------------------------------------------------------

_CODE_GLOBS = (
    "backend/**/*.py",
    "src/**/*.ts",
    "src/**/*.tsx",
)

_EXCLUDE_PREFIXES = (
    "backend/__pycache__/",
    "backend/tests/",
    "tests/",
    "docs/",
    "attached_assets/",
    "ONGOING_PROMPTS/",
    "node_modules/",
    ".venv/",
    "dist/",
)


def _code_files() -> list[Path]:
    seen: set[Path] = set()
    for pat in _CODE_GLOBS:
        for p in REPO.glob(pat):
            if not p.is_file():
                continue
            rel = p.relative_to(REPO).as_posix()
            if any(rel.startswith(pref) for pref in _EXCLUDE_PREFIXES):
                continue
            seen.add(p)
    return sorted(seen)


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")


# ---------------------------------------------------------------------------
# I1: bare run_id response field
# ---------------------------------------------------------------------------

_BARE_RUN_ID = re.compile(r'(?<![a-z_])"run_id"\s*:')

_I1_WHITELIST = {
    # Ingest endpoint legitimately echoes run_id as an internal field
    # (the response still includes dcl_ingest_id as the namespaced id).
    "backend/api/routes/ingest_triples.py",
    # TripleStore builds SQL dictionaries that happen to use "run_id" as
    # a column key, not a response field — filtered by path, not pattern.
    "backend/db/triple_store.py",
}


def test_i1_no_bare_run_id_in_responses():
    offenders: list[str] = []
    for p in _code_files():
        rel = p.relative_to(REPO).as_posix()
        if rel in _I1_WHITELIST:
            continue
        for lineno, line in enumerate(_read(p).splitlines(), 1):
            if _BARE_RUN_ID.search(line):
                offenders.append(f"{rel}:{lineno}: {line.strip()}")
    assert not offenders, (
        "I1 violation: bare \"run_id\" response field found — use namespaced "
        "identifiers (dcl_ingest_id, farm_manifest_id, cofa_run_id, ...). "
        f"Offenders:\n  " + "\n  ".join(offenders)
    )


# ---------------------------------------------------------------------------
# I6(3): no derivation of tenant_id or entity_id in pipeline path
# ---------------------------------------------------------------------------

_MANGLE_PATTERNS = [
    re.compile(r"tenant_id\[:\d+\]"),
    re.compile(r"entity_id\[:\d+\]"),
    re.compile(r"tenant_id\.split\("),
    re.compile(r"entity_id\.split\("),
    re.compile(r"tenant_id\.replace\("),
    re.compile(r"entity_id\.replace\("),
    re.compile(r"re\.(sub|match|search)\([^)]*tenant_id"),
    re.compile(r"re\.(sub|match|search)\([^)]*entity_id"),
]


_I6_DISPLAY_ONLY_WHITELIST = {
    # Cosmetic label formatter — _display_entity is called only from the
    # run-list rendering path, not from any ingest / query path that carries
    # identity downstream.
    "backend/api/routes/triple_monitor.py",
    "backend/engine/dcl_engine.py",
}


def test_i6_no_id_string_mangling():
    offenders: list[str] = []
    for p in _code_files():
        rel = p.relative_to(REPO).as_posix()
        if rel in _I6_DISPLAY_ONLY_WHITELIST:
            continue
        for lineno, line in enumerate(_read(p).splitlines(), 1):
            for pat in _MANGLE_PATTERNS:
                if pat.search(line):
                    offenders.append(f"{rel}:{lineno}: {line.strip()}")
                    break
    assert not offenders, (
        "I6(3) violation: tenant_id/entity_id are mangled in the pipeline path "
        "— pass identifiers through, do not derive them. Offenders:\n  "
        + "\n  ".join(offenders)
    )


# ---------------------------------------------------------------------------
# mig016: is_active is gone from DCL code
# ---------------------------------------------------------------------------

_IS_ACTIVE_WHITELIST = {
    "backend/tests/test_farm_v2_integration.py",  # Salesforce synthetic user field
    "config/ontology_concepts.yaml",              # HR worker_status schema
}


def test_mig016_no_is_active_in_code():
    offenders: list[str] = []
    for p in _code_files():
        rel = p.relative_to(REPO).as_posix()
        if rel in _IS_ACTIVE_WHITELIST:
            continue
        for lineno, line in enumerate(_read(p).splitlines(), 1):
            if "is_active" in line:
                offenders.append(f"{rel}:{lineno}: {line.strip()}")
    assert not offenders, (
        "mig016 violation: is_active references remain after column drop. "
        f"Offenders:\n  " + "\n  ".join(offenders)
    )


# ---------------------------------------------------------------------------
# Store-rebuild: semantic_triples reads outside whitelist are banned
# ---------------------------------------------------------------------------

_SEMANTIC_WHITELIST = {
    "backend/api/routes/ingest_triples.py",
    "backend/db/triple_store.py",
    "backend/engine/materialized_views.py",
    "backend/engine/query_resolver_v2.py",
    "backend/engine/dcl_engine.py",
    "backend/api/routes/recon_checks.py",
    "backend/api/routes/v2_helpers.py",
    "backend/api/routes/triple_monitor.py",
    "backend/api/main.py",
}


def test_store_rebuild_semantic_triples_whitelist():
    offenders: list[str] = []
    for p in _code_files():
        rel = p.relative_to(REPO).as_posix()
        if rel in _SEMANTIC_WHITELIST:
            continue
        for lineno, line in enumerate(_read(p).splitlines(), 1):
            if "semantic_triples" in line:
                offenders.append(f"{rel}:{lineno}: {line.strip()}")
    assert not offenders, (
        "store-rebuild violation: semantic_triples outside whitelist. "
        "Read from current_triples instead. Offenders:\n  "
        + "\n  ".join(offenders)
    )


# ---------------------------------------------------------------------------
# ME contamination — DCL is SE-only
# ---------------------------------------------------------------------------

_ME_PATTERNS = (
    "convergence_triples",
    "engagement_state",
    "cofa_mapping",
    "convergence_ingest_id",
    "cofa_engine",
)


def test_no_me_contamination_in_dcl():
    offenders: list[str] = []
    for p in _code_files():
        rel = p.relative_to(REPO).as_posix()
        for lineno, line in enumerate(_read(p).splitlines(), 1):
            for pat in _ME_PATTERNS:
                if pat in line:
                    offenders.append(f"{rel}:{lineno}: {line.strip()}")
                    break
    assert not offenders, (
        "ME contamination: DCL is SE-only — ME engines live in Convergence. "
        f"Offenders:\n  " + "\n  ".join(offenders)
    )
