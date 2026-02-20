#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Code Quality Audit v2 — Self-Running Test Loop
# ──────────────────────────────────────────────────────────────────────────────
# Runs all code quality regression tests (v1 + v2) and reports pass/fail.
# Usage: bash scripts/audit_v2_loop.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

cd "$(dirname "$0")/.."

echo "=============================================="
echo "  AOS-DCL Code Quality Audit v2 — Test Suite"
echo "=============================================="
echo ""

# Phase 1: Run existing v1 tests
echo "--- Phase A: v1 Regression Tests ---"
python -m pytest tests/test_code_quality.py -v --tb=short 2>&1
V1_EXIT=$?
echo ""

# Phase 2: Run new v2 audit tests
echo "--- Phase B: v2 Audit Tests ---"
python -m pytest tests/test_audit_v2.py -v --tb=short 2>&1
V2_EXIT=$?
echo ""

# Phase 3: Codebase grep checks (belt + suspenders)
echo "--- Phase C: Grep Verification ---"
GREP_FAIL=0

check_absent() {
    local pattern="$1"
    local dir="$2"
    local desc="$3"
    if grep -rn "$pattern" "$dir" --include="*.py" 2>/dev/null | grep -v "test_" | grep -v "PLAN"; then
        echo "  FAIL: $desc"
        GREP_FAIL=1
    else
        echo "  PASS: $desc"
    fi
}

check_absent 'rag_reads = 3' backend/ "No fabricated rag_reads"
check_absent 'range(1536)' backend/ "No hardcoded embedding dimension"
check_absent '"text-embedding-3-small"' backend/ "No hardcoded embedding model"
check_absent 'port=5000' backend/api/main.py "No hardcoded port 5000"
echo ""

# Phase 4: Frontend build check
echo "--- Phase D: Frontend Build ---"
if command -v npx &>/dev/null; then
    npx tsc --noEmit 2>&1 && echo "  PASS: TypeScript compilation" || echo "  WARN: TypeScript errors (non-blocking)"
else
    echo "  SKIP: npx not available"
fi
echo ""

# Summary
echo "=============================================="
echo "  RESULTS"
echo "=============================================="
TOTAL_EXIT=0
if [ $V1_EXIT -eq 0 ]; then echo "  v1 Tests: PASS"; else echo "  v1 Tests: FAIL"; TOTAL_EXIT=1; fi
if [ $V2_EXIT -eq 0 ]; then echo "  v2 Tests: PASS"; else echo "  v2 Tests: FAIL"; TOTAL_EXIT=1; fi
if [ $GREP_FAIL -eq 0 ]; then echo "  Grep Checks: PASS"; else echo "  Grep Checks: FAIL"; TOTAL_EXIT=1; fi
echo "=============================================="

exit $TOTAL_EXIT
