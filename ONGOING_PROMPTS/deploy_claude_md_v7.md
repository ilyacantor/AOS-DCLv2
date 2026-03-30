# Deploy CLAUDE.md v7.0 + Delete HARNESS_RULES_v2.md

## Context
CLAUDE.md v7.0 merges the old CLAUDE.md v6.0 and HARNESS_RULES_v2.md into one document. HARNESS_RULES_v2.md is retired. The new CLAUDE.md is the single agent constitution deployed to every AOS repo.

The source file is at `~/code/dcl/CLAUDE.md` (already placed there). Copy it to all repos below, delete the old HARNESS_RULES file, and update inline references.

## Step 1: Copy CLAUDE.md v7.0 to all repos

```bash
SOURCE=~/code/dcl/CLAUDE.md
for repo in aam aod aos-hooks aos_site console dcl-onboarding-agent farm finops nlq platform revops; do
  cp "$SOURCE" ~/code/$repo/CLAUDE.md
  echo "Copied to $repo"
done
```

## Step 2: Delete tests/HARNESS_RULES_v2.md from all repos

```bash
for repo in aam aod aos-hooks console dcl-onboarding-agent dcl farm finops nlq platform revops; do
  rm -f ~/code/$repo/tests/HARNESS_RULES_v2.md
  echo "Deleted from $repo"
done
```

## Step 3: Update inline references in test files and app code

These files have HARNESS_RULES references in comments or docstrings. Update them to reference CLAUDE.md instead:

```bash
# aod
sed -i 's|tests/HARNESS_RULES.md|CLAUDE.md|g' ~/code/aod/tests/maestra/test_status_endpoint.py
sed -i 's|HARNESS_RULES B14|CLAUDE.md B14|g' ~/code/aod/tests/maestra/test_status_endpoint.py

# dcl
sed -i 's|tests/HARNESS_RULES.md|CLAUDE.md|g' ~/code/dcl/tests/test_maestra_status.py

# nlq
sed -i 's|HARNESS_RULES|CLAUDE.md|g' ~/code/nlq/tests/maestra/conftest.py
sed -i 's|HARNESS_RULES.md|CLAUDE.md|g' ~/code/nlq/tests/test_dashboard_harness.py
sed -i 's|HARNESS_RULES|CLAUDE.md|g' ~/code/nlq/tests/test_nlq_v2_client.py
sed -i 's|HARNESS_RULES|CLAUDE.md|g' ~/code/nlq/tests/test_nlq_full_surface.py

# platform
sed -i 's|HARNESS_RULES A1|CLAUDE.md A1|g' ~/code/platform/app/maestra/chat.py
```

## Step 4: Commit across all repos

```bash
MSG="chore: deploy CLAUDE.md v7.0, retire HARNESS_RULES_v2.md

CLAUDE.md v7.0 merges agent constitution + harness rules into one doc.
Updates: Convergence carveout, pipeline identity architecture (I1-I6),
Maestra supervised execution, RACI v8.2, spec v7.4 refs.
HARNESS_RULES_v2.md deleted — all rules now in CLAUDE.md Sections A-F."

for repo in aam aod aos-hooks aos_site console dcl-onboarding-agent dcl farm finops nlq platform revops; do
  cd ~/code/$repo
  git add -A
  git commit -m "$MSG"
  echo "Committed in $repo"
done
```

## Step 5: Verify no remaining references

```bash
grep -r "HARNESS_RULES_v2" ~/code/*/CLAUDE.md ~/code/*/tests/ --include="*.md" --include="*.py" 2>/dev/null
# Expect: 0 hits (ignore .git dirs)
```

## Not touched (intentionally):
- nlq/docs/maestra/scratch/session*.md — historical CC prompts, reference HARNESS_RULES v1. Leave as-is.
- nlq/docs/maestra/README.md and maestra_spec_v2.docx.md — historical docs. Leave as-is.
- .git/logs, .git/index, __pycache__ — immutable history and bytecode. Ignored.
- convergence_MA_spec_v7.4.md — references HARNESS_RULES by name in prose (appropriate). §14 governing docs table updated in next spec revision to note merge.
