# AOS-DCLv2 Testing

## Prerequisites
- Python 3.11+
- Dependencies: pip install -r requirements.txt
- For unit tests: no external services needed
- For integration tests: DCL backend running on :8004

## Run Unit Tests
pytest tests/unit/ -v

## Run All Integration Tests (Recommended)

```bash
# Run all phases (0, 1, 2) with a single command:
python -m tests.run_all

# Specify a custom backend URL:
python -m tests.run_all --base-url http://localhost:9000

# Run only a single phase:
python -m tests.run_all --phase 0
python -m tests.run_all --phase 1
python -m tests.run_all --phase 2
```

Exit codes: 0 = all tests passed, 1 = failures or backend unreachable.

The runner checks backend connectivity upfront and prints a clear error if the
backend is not reachable, so you don't have to wait for individual test timeouts.

## Run Individual Test Files

1. Start DCL backend: python run_backend.py
2. Run: python -m tests.test_phase0_harness
3. Run: python -m tests.test_phase1
4. Run: python -m tests.test_phase2

## Test Categories
- Phase 0 (51 tests): concept schema, hierarchy, drill-through, conflict, reporting, reconciliation
- Phase 1 (26 tests): dual entity, COFA, entity overlap, entity resolution, combining recon, backward compat
- Phase 2 (28 tests): cross-sell, EBITDA bridge, what-if, dashboards, Maestra engagement

## Notes
- Tests use a custom harness runner (not pytest discovery)
- Integration tests require the DCL backend running at http://localhost:8004
- NLQ tests belong in the AOS-NLQ repo, not here
