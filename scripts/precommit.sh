#!/bin/bash
set -e

# DCL pre-commit hook — three categories:
#   1. ME/Convergence contamination (DCL is SE-only)
#   2. F1 code quality rules (CLAUDE.md Section F1)
#   3. Store-rebuild guardrails (mig014/015/016)
#
# Source of truth is scripts/precommit.sh. Install with:
#   cp scripts/precommit.sh .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit
#
# Do not bypass with --no-verify (CLAUDE.md C13).

HOOK_FILE=".git/hooks/pre-commit"
STAGED_FILES=$(git diff --cached --name-only --diff-filter=ACM)

if [ -z "$STAGED_FILES" ]; then
    exit 0
fi

# Global exclusions: non-code files (docs, archived prompts, the hook itself).
filter_excluded() {
    while IFS= read -r file; do
        case "$file" in
            "$HOOK_FILE") continue ;;
            scripts/precommit.sh) continue ;;
            CLAUDE.md|README.md|dcl_deferred_work.md) continue ;;
            docs/*) continue ;;
            attached_assets/*) continue ;;
            ONGOING_PROMPTS/*) continue ;;
            *) echo "$file" ;;
        esac
    done
}

FILTERED_FILES=$(echo "$STAGED_FILES" | filter_excluded)

if [ -z "$FILTERED_FILES" ]; then
    exit 0
fi

# --- Category 1: ME/Convergence contamination + F1 fixed strings -----------
FIXED_PATTERNS=(
    "convergence_triples"
    "convergence_ingest_id"
    "engagement_state"
    "from convergence"
    "import convergence"
    "/api/convergence/"
    "combining_v2"
    "ebitda_bridge_v2"
    "qoe_v2"
    "overlap_v2"
    "cross_sell_v2"
    "entity_resolution_v2"
    "cofa_mapping"
    "cofa_engine"
    "what_if_v2"
    "meridian"
    "cascadia"
    "combined_entity"
    "fact_base.json"
    "400aa910"
    "6754a9d7"
    "purge_old_runs"
    "purge-stale"
    "purge_stale_all_tenants"
)

TENANT_NAME_PATTERNS=("meridian" "cascadia")

is_exempted() {
    local file="$1"
    local pattern="$2"

    case "$file" in
        tests/*)
            for tp in "${TENANT_NAME_PATTERNS[@]}"; do
                if [ "$pattern" = "$tp" ]; then
                    return 0
                fi
            done
            ;;
    esac

    if [ "$file" = "tests/test_s1_dcl.py" ] && [ "$pattern" = "engagement_state" ]; then
        return 0
    fi

    if [ "$file" = "config/ontology_concepts.yaml" ]; then
        return 0
    fi

    # test_identity_preservation.py is the guardrail itself — it must contain
    # the banned literals as strings in order to scan for them.
    if [ "$file" = "tests/test_identity_preservation.py" ]; then
        return 0
    fi

    # Migration files may reference cross-repo consumers in comments.
    case "$file" in
        migrations/*) return 0 ;;
    esac

    return 1
}

FIXED_FOUND=""

for file in $FILTERED_FILES; do
    if file "$file" 2>/dev/null | grep -q "binary\|image data\|archive"; then
        if ! file "$file" 2>/dev/null | grep -qi "text"; then
            continue
        fi
    fi
    [ -f "$file" ] || continue

    STAGED_CONTENT=$(git show :"$file" 2>/dev/null || true)
    [ -z "$STAGED_CONTENT" ] && continue

    for pattern in "${FIXED_PATTERNS[@]}"; do
        if is_exempted "$file" "$pattern"; then
            continue
        fi
        LINE_MATCHES=$(echo "$STAGED_CONTENT" | grep -inF "$pattern" || true)
        if [ -n "$LINE_MATCHES" ]; then
            while IFS= read -r line; do
                FIXED_FOUND+="  $file:$line"$'\n'
            done <<< "$LINE_MATCHES"
        fi
    done
done

# --- Category 2: F1 structural rules ---------------------------------------
F1_FOUND=""

for file in $FILTERED_FILES; do
    case "$file" in
        *.py) ;;
        *) continue ;;
    esac
    [ -f "$file" ] || continue
    STAGED_CONTENT=$(git show :"$file" 2>/dev/null || true)
    [ -z "$STAGED_CONTENT" ] && continue

    # F1a: bare `except: pass` / `except: continue`
    BARE_EXCEPT=$(echo "$STAGED_CONTENT" | grep -nE '^[[:space:]]*except[[:space:]]*:[[:space:]]*(pass|continue)[[:space:]]*$' || true)
    if [ -n "$BARE_EXCEPT" ]; then
        while IFS= read -r line; do
            F1_FOUND+="  $file:$line (bare except pass/continue)"$'\n'
        done <<< "$BARE_EXCEPT"
    fi

    # F1b: except clause returning literal default on same line
    EXCEPT_RETURN=$(echo "$STAGED_CONTENT" | grep -nE '^[[:space:]]*except[^:]*:[[:space:]]*return[[:space:]]+(0|\[\]|\{\}|None|False|"")[[:space:]]*$' || true)
    if [ -n "$EXCEPT_RETURN" ]; then
        while IFS= read -r line; do
            F1_FOUND+="  $file:$line (except returning literal default)"$'\n'
        done <<< "$EXCEPT_RETURN"
    fi

    # F1c: bare run_id as JSON response field (colon form)
    case "$file" in
        backend/api/routes/ingest_triples.py) ;;
        backend/db/triple_store.py) ;;
        tests/*) ;;
        scripts/*) ;;
        migrations/*) ;;
        *)
            BARE_RUN_ID=$(echo "$STAGED_CONTENT" | grep -nE '(^|[^a-z_])"run_id"[[:space:]]*:' || true)
            if [ -n "$BARE_RUN_ID" ]; then
                while IFS= read -r line; do
                    F1_FOUND+="  $file:$line (bare run_id response field — use namespaced id)"$'\n'
                done <<< "$BARE_RUN_ID"
            fi
            ;;
    esac
done

# --- Category 3: store-rebuild guardrails ----------------------------------
STORE_FOUND=""

is_semantic_triples_whitelisted() {
    local file="$1"
    case "$file" in
        backend/api/routes/ingest_triples.py) return 0 ;;
        backend/db/triple_store.py) return 0 ;;
        backend/engine/dcl_engine.py) return 0 ;;
        backend/api/routes/recon_checks.py) return 0 ;;
        backend/api/routes/v2_helpers.py) return 0 ;;
        backend/api/routes/triple_monitor.py) return 0 ;;
        backend/api/main.py) return 0 ;;
        migrations/*) return 0 ;;
        scripts/apply_mig*.py) return 0 ;;
        scripts/prune_tenant_runs_cap.py) return 0 ;;
        scripts/seed_database.py) return 0 ;;
        tests/test_store_invariants.py) return 0 ;;
        tests/test_append_invariant.py) return 0 ;;
        tests/test_s1_dcl.py) return 0 ;;
        tests/test_s1_seed.py) return 0 ;;
        tests/test_pipeline_identity.py) return 0 ;;
        tests/test_identity_preservation.py) return 0 ;;
    esac
    return 1
}

is_is_active_whitelisted() {
    local file="$1"
    case "$file" in
        migrations/*) return 0 ;;
        scripts/apply_mig*.py) return 0 ;;
        tests/test_identity_preservation.py) return 0 ;;
        config/ontology_concepts.yaml) return 0 ;;
        *.md) return 0 ;;
    esac
    return 1
}

for file in $FILTERED_FILES; do
    case "$file" in
        *.py|*.ts|*.tsx|*.js|*.sql) ;;
        *) continue ;;
    esac
    [ -f "$file" ] || continue
    STAGED_CONTENT=$(git show :"$file" 2>/dev/null || true)
    [ -z "$STAGED_CONTENT" ] && continue

    if ! is_semantic_triples_whitelisted "$file"; then
        ST_HITS=$(echo "$STAGED_CONTENT" | grep -nF "semantic_triples" || true)
        if [ -n "$ST_HITS" ]; then
            while IFS= read -r line; do
                STORE_FOUND+="  $file:$line (semantic_triples outside whitelist — read from current_triples)"$'\n'
            done <<< "$ST_HITS"
        fi
    fi

    if ! is_is_active_whitelisted "$file"; then
        IA_HITS=$(echo "$STAGED_CONTENT" | grep -nF "is_active" || true)
        if [ -n "$IA_HITS" ]; then
            while IFS= read -r line; do
                STORE_FOUND+="  $file:$line (is_active — column dropped in mig016)"$'\n'
            done <<< "$IA_HITS"
        fi
    fi

    case "$file" in
        *.ts|*.tsx|*.js)
            ALERT_HTTP=$(echo "$STAGED_CONTENT" | grep -nE 'alert\([[:space:]]*(`|")HTTP' || true)
            if [ -n "$ALERT_HTTP" ]; then
                while IFS= read -r line; do
                    STORE_FOUND+="  $file:$line (alert(HTTP...) debug — throw or handle in UI)"$'\n'
                done <<< "$ALERT_HTTP"
            fi
            ;;
    esac
done

# --- Report and exit --------------------------------------------------------
EXIT_CODE=0

if [ -n "$FIXED_FOUND" ]; then
    echo ""
    echo "BLOCKED: ME/Convergence contamination or hardcoded constants in DCL."
    echo "$FIXED_FOUND"
    EXIT_CODE=1
fi

if [ -n "$F1_FOUND" ]; then
    echo ""
    echo "BLOCKED: F1 code quality violations (CLAUDE.md Section F1)."
    echo "$F1_FOUND"
    EXIT_CODE=1
fi

if [ -n "$STORE_FOUND" ]; then
    echo ""
    echo "BLOCKED: store-rebuild guardrail violations (mig014/015/016)."
    echo "$STORE_FOUND"
    EXIT_CODE=1
fi

if [ $EXIT_CODE -ne 0 ]; then
    echo ""
    echo "Do not bypass with --no-verify (CLAUDE.md C13). Fix the code."
    exit 1
fi

# ============================================================================
# Playwright Acceptance enforcement (extends B17) — see CLAUDE.md.
# Scopes:
#   - Banned patterns: ADDED lines only in staged *.spec.ts / *.spec.js
#     files, via git diff --cached --unified=0. Not whole files. Existing
#     lines not retroactively blocked.
#   - Header rule: first-line '// Operator-visible outcome: ...' required
#     on NEW spec files only (git diff --cached --diff-filter=A).
# Pattern list source of truth: .playwright-banned-patterns at repo root.
# ============================================================================
PW_PATTERNS_FILE=".playwright-banned-patterns"
if [ -f "$PW_PATTERNS_FILE" ]; then
    PW_FOUND=""

    PW_NEW=$(git diff --cached --name-only --diff-filter=A | grep -E '\.spec\.(ts|js)$' || true)
    for spec in $PW_NEW; do
        [ -f "$spec" ] || continue
        FIRST_LINE=$(git show :"$spec" 2>/dev/null | head -n1)
        if ! printf '%s' "$FIRST_LINE" | grep -qE '^//[[:space:]]*Operator-visible outcome:'; then
            PW_FOUND+="  $spec:1 (missing-operator-outcome-header) — NEW spec files must begin with '// Operator-visible outcome: <specific values>'"$'\n'
        fi
    done

    PW_STAGED=$(git diff --cached --name-only --diff-filter=ACM | grep -E '\.spec\.(ts|js)$' || true)
    for spec in $PW_STAGED; do
        [ -f "$spec" ] || continue

        ADDED=$(git diff --cached --unified=0 -- "$spec" 2>/dev/null | awk '
            /^@@ / {
                s = $0
                sub(/^.* \+/, "", s)
                sub(/[^0-9].*$/, "", s)
                newline = s + 0
                in_hunk = 1
                next
            }
            /^\+\+\+ / { next }
            /^\+/ && in_hunk {
                printf "%d\t%s\n", newline, substr($0, 2)
                newline++
                next
            }
        ')

        [ -z "$ADDED" ] && continue

        while IFS='|' read -r pw_name pw_regex; do
            case "$pw_name" in
                ''|'#'*) continue ;;
            esac
            [ -z "$pw_regex" ] && continue

            while IFS=$'\t' read -r lineno content; do
                [ -z "$content" ] && continue
                if printf '%s' "$content" | grep -qE "$pw_regex"; then
                    PW_FOUND+="  $spec:$lineno ($pw_name)"$'\n'
                fi
            done <<< "$ADDED"
        done < "$PW_PATTERNS_FILE"
    done

    if [ -n "$PW_FOUND" ]; then
        echo ""
        echo "BLOCKED: Playwright Acceptance violations on added lines."
        echo "See CLAUDE.md § Playwright Acceptance for the contract."
        echo "Offending locations (file:line + pattern):"
        printf '%s' "$PW_FOUND"
        exit 1
    fi
fi

exit 0
