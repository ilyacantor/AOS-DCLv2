# DCL Test Harness

**Last Updated:** January 26, 2026

## Overview

The DCL test harness provides comprehensive testing infrastructure for the NLQ, BLL, and DCL core components. Tests are organized by layer and use pytest with JSON fixtures for deterministic, reproducible test runs.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Test Architecture                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        Test Layers                                   │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │    │
│  │  │    Unit      │  │ Integration  │  │   API/E2E    │               │    │
│  │  │    Tests     │  │    Tests     │  │    Tests     │               │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              ↓                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    Test Fixtures                                     │    │
│  │  JSON fixtures (backend/nlq/fixtures/) + Demo CSVs (dcl/demo/)      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Run All Tests
```bash
pytest
```

### Run Specific Test File
```bash
pytest tests/nlq/test_scorer.py
```

### Run Specific Test Class
```bash
pytest tests/nlq/test_scorer.py::TestAnswerabilityScorer
```

### Run Specific Test
```bash
pytest tests/nlq/test_scorer.py::TestAnswerabilityScorer::test_score_hypothesis_with_definition
```

### Run with Coverage
```bash
pytest --cov=backend --cov-report=html
```

---

## Directory Structure

```
tests/
├── __init__.py               # Test suite marker
├── nlq/                      # NLQ layer tests
│   ├── __init__.py
│   ├── test_api.py           # API endpoint integration tests
│   ├── test_compiler.py      # SQL compilation tests
│   ├── test_explainer.py     # Explanation generation tests
│   ├── test_hypothesis.py    # Hypothesis generation tests
│   ├── test_persistence.py   # Fixture loading tests
│   ├── test_registration.py  # Definition registration tests
│   ├── test_registry_services.py  # Registry service tests
│   ├── test_scorer.py        # Answerability scoring tests
│   └── test_validator.py     # Definition validation tests
├── bll/                      # BLL layer tests (planned)
└── dcl/                      # DCL core tests (planned)

pytest.ini                    # Pytest configuration
```

---

## Test Categories

### 1. Unit Tests

Test individual components in isolation with mocked dependencies.

**Example: `test_scorer.py`**
```python
class TestQuestionParser:
    """Tests for QuestionParser."""

    def setup_method(self):
        self.parser = QuestionParser()

    def test_parse_time_window_qoq(self):
        """Should extract QoQ time window from question."""
        result = self.parser.parse("Revenue is down 50% QoQ")
        assert result["time_window"] == "QOQ"

    def test_parse_metric_services_revenue(self):
        """Should extract services revenue metric hint."""
        result = self.parser.parse("Services revenue is declining")
        assert result["metric_hint"] == "services_revenue"
```

### 2. Integration Tests

Test component interactions with real fixtures.

**Example: `test_persistence.py`**
```python
class TestNLQPersistence:
    """Tests for NLQPersistence with JSON fixtures."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_get_events(self):
        """Should load canonical events from fixtures."""
        events = self.persistence.get_events()
        assert len(events) > 0
        event_ids = [e.id for e in events]
        assert "revenue_recognized" in event_ids
```

### 3. API/E2E Tests

Test full request/response cycle via FastAPI TestClient.

**Example: `test_api.py`**
```python
class TestAnswerabilityRankEndpoint:
    """Integration tests for POST /api/nlq/answerability_rank."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        from backend.api.main import app
        return TestClient(app)

    def test_rank_services_revenue_question(self, client):
        """Should return ranked circles for services revenue question."""
        response = client.post(
            "/api/nlq/answerability_rank",
            json={
                "question": "Services revenue is down 50% QoQ",
                "tenant_id": "t_123",
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert 2 <= len(data["circles"]) <= 3
```

---

## Test Fixtures

### JSON Fixtures (NLQ Layer)

Located in `backend/nlq/fixtures/`:

| File | Content | Count |
|------|---------|-------|
| `canonical_events.json` | Business event types | 45 |
| `entities.json` | Entity dimensions | 33 |
| `definitions.json` | Metric definitions | 41 |
| `definition_versions.json` | Version specs | 41 |
| `bindings.json` | Source mappings | 20+ |
| `proof_hooks.json` | Proof pointers | 10+ |

**Example Fixture (`definitions.json`):**
```json
[
  {
    "id": "services_revenue",
    "tenant_id": "default",
    "kind": "metric",
    "pack": "cfo",
    "description": "Services revenue recognized",
    "default_time_semantics_json": {
      "event": "revenue_recognized",
      "time_field": "effective_at"
    }
  }
]
```

### CSV Fixtures (BLL Layer)

Located in `dcl/demo/datasets/demo9/`:

| File | Content |
|------|---------|
| `salesforce_account.csv` | Salesforce accounts |
| `dynamics_accounts.csv` | Dynamics CRM accounts |
| `hubspot_companies.csv` | HubSpot companies |
| `netsuite_customers.csv` | NetSuite customers |
| `sap_bkpf.csv` | SAP accounting docs |
| `aws_cost_explorer.csv` | AWS cost data |

---

## Persistence Layer Testing

The `NLQPersistence` class provides fixture access for tests:

```python
from backend.nlq.persistence import NLQPersistence

class TestMyFeature:
    def setup_method(self):
        self.persistence = NLQPersistence()

    def test_something(self):
        # Load events
        events = self.persistence.get_events()
        
        # Get specific definition
        definition = self.persistence.get_definition("services_revenue")
        
        # Check bindings
        bindings = self.persistence.get_bindings_for_event("revenue_recognized")
        
        # Resolve definition from hint
        definition = self.persistence.resolve_definition(
            metric_hint="services_revenue"
        )
```

### Key Persistence Methods

| Method | Returns | Purpose |
|--------|---------|---------|
| `get_events()` | `List[CanonicalEvent]` | All canonical events |
| `get_event(id)` | `CanonicalEvent | None` | Single event by ID |
| `get_entities()` | `List[Entity]` | All entity dimensions |
| `get_definitions()` | `List[Definition]` | All metric definitions |
| `get_definition(id)` | `Definition | None` | Single definition by ID |
| `get_bindings()` | `List[Binding]` | All source bindings |
| `get_bindings_for_event(id)` | `List[Binding]` | Bindings for event |
| `get_published_version(id)` | `DefinitionVersion | None` | Published spec |
| `resolve_definition(hint, keywords)` | `Definition | None` | Find by hint/keywords |

---

## Test Patterns

### Pattern 1: Setup Method

Use `setup_method` for per-test initialization:

```python
class TestScorer:
    def setup_method(self):
        """Set up scorer with real persistence."""
        self.persistence = NLQPersistence()
        self.scorer = AnswerabilityScorer(persistence=self.persistence)

    def test_score_calculation(self):
        # Uses self.persistence and self.scorer
        pass
```

### Pattern 2: Pytest Fixtures

Use `@pytest.fixture` for shared resources:

```python
class TestAPI:
    @pytest.fixture
    def client(self):
        """Create test client."""
        from backend.api.main import app
        return TestClient(app)

    def test_endpoint(self, client):
        response = client.post("/api/endpoint", json={...})
```

### Pattern 3: Parameterized Tests

Use `@pytest.mark.parametrize` for multiple inputs:

```python
@pytest.mark.parametrize("time_window,expected", [
    ("QoQ", "QOQ"),
    ("quarter over quarter", "QOQ"),
    ("year-over-year", "YOY"),
    ("YoY", "YOY"),
])
def test_parse_time_windows(self, time_window, expected):
    result = self.parser.parse(f"Revenue is down {time_window}")
    assert result["time_window"] == expected
```

### Pattern 4: Exception Testing

Use `pytest.raises` for expected exceptions:

```python
def test_invalid_input_raises_error(self):
    with pytest.raises(ValueError, match="Invalid metric"):
        self.scorer.score(None)
```

---

## Test Coverage by Component

### NLQ Layer Coverage

| Component | Test File | Status |
|-----------|-----------|--------|
| `persistence.py` | `test_persistence.py` | ✅ Complete |
| `scorer.py` | `test_scorer.py` | ✅ Complete |
| `hypothesis.py` | `test_hypothesis.py` | ✅ Complete |
| `validator.py` | `test_validator.py` | ✅ Complete |
| `compiler.py` | `test_compiler.py` | ✅ Complete |
| `explainer.py` | `test_explainer.py` | ✅ Complete |
| `registry.py` | `test_registry_services.py` | ✅ Complete |
| `routes*.py` | `test_api.py` | ✅ Complete |
| `intent_matcher.py` | - | 🔲 Planned |
| `operator_extractor.py` | - | 🔲 Planned |
| `param_extractor.py` | - | 🔲 Planned |

### BLL Layer Coverage

| Component | Test File | Status |
|-----------|-----------|--------|
| `executor.py` | - | 🔲 Planned |
| `definitions.py` | - | 🔲 Planned |
| `routes.py` | - | 🔲 Planned |

---

## Configuration

### pytest.ini

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short
filterwarnings =
    ignore::DeprecationWarning
    ignore::UserWarning
```

### Configuration Options

| Option | Description |
|--------|-------------|
| `testpaths = tests` | Look for tests in `tests/` directory |
| `python_files = test_*.py` | Test files start with `test_` |
| `python_classes = Test*` | Test classes start with `Test` |
| `python_functions = test_*` | Test functions start with `test_` |
| `addopts = -v --tb=short` | Verbose output, short tracebacks |

---

## Running Tests

### Basic Commands

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific directory
pytest tests/nlq/

# Run with print statements visible
pytest -s

# Run failed tests only
pytest --lf

# Run tests matching pattern
pytest -k "scorer"
```

### Coverage Commands

```bash
# Run with coverage
pytest --cov=backend

# Generate HTML report
pytest --cov=backend --cov-report=html

# Show missing lines
pytest --cov=backend --cov-report=term-missing
```

### Debugging

```bash
# Stop on first failure
pytest -x

# Enter debugger on failure
pytest --pdb

# Show local variables in traceback
pytest -l
```

---

## Writing New Tests

### Step 1: Create Test File

```bash
# Create new test file
touch tests/nlq/test_my_feature.py
```

### Step 2: Write Test Class

```python
"""
Unit tests for MyFeature.

Tests [describe what is being tested].
"""

import pytest
from backend.nlq.my_feature import MyFeature
from backend.nlq.persistence import NLQPersistence


class TestMyFeature:
    """Tests for MyFeature."""

    def setup_method(self):
        """Set up test fixtures."""
        self.persistence = NLQPersistence()
        self.feature = MyFeature(persistence=self.persistence)

    def test_basic_functionality(self):
        """Should [describe expected behavior]."""
        result = self.feature.do_something()
        assert result is not None

    def test_edge_case(self):
        """Should handle [edge case description]."""
        result = self.feature.do_something(edge_case=True)
        assert result == "expected_value"
```

### Step 3: Run Tests

```bash
pytest tests/nlq/test_my_feature.py -v
```

---

## Farm Integration Testing

For deterministic integration tests, use Farm ground truth:

```python
class TestFarmIntegration:
    """Integration tests using Farm ground truth."""

    def test_top_customers_matches_farm(self):
        """DCL results should match Farm ground truth."""
        # Generate scenario
        scenario_id = generate_scenario(seed=12345)
        
        # Query DCL
        dcl_result = execute_definition(
            "crm.top_customers",
            dataset_id=f"farm:{scenario_id}",
            limit=5
        )
        
        # Query Farm directly
        farm_result = farm_client.get_top_customers(scenario_id, limit=5)
        
        # Results should match
        assert dcl_result == farm_result
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `ModuleNotFoundError` | Missing package | Run `pip install -e .` |
| `FileNotFoundError` fixtures | Wrong working dir | Run from project root |
| Tests hang | Async issues | Check for missing `await` |
| Import errors | Circular imports | Check import order |

### Debug Tips

1. **Print fixture data:**
   ```python
   def test_debug(self):
       events = self.persistence.get_events()
       print(f"Loaded {len(events)} events")
       for e in events[:5]:
           print(f"  - {e.id}")
   ```

2. **Check fixture loading:**
   ```python
   def test_fixtures_exist(self):
       from pathlib import Path
       fixtures_dir = Path("backend/nlq/fixtures")
       assert fixtures_dir.exists()
       assert (fixtures_dir / "definitions.json").exists()
   ```

---

## Related Documentation

- [NLQ_ARCHITECTURE.md](NLQ_ARCHITECTURE.md) - NLQ layer documentation
- [SEMANTIC-LAYER.md](SEMANTIC-LAYER.md) - Semantic layer documentation
- [ARCH-DCL-CURRENT.md](ARCH-DCL-CURRENT.md) - Overall DCL architecture
