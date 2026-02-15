"""
Code Quality Regression Tests
==============================
Validates that specific code quality fixes remain in place.
Run with: pytest tests/test_code_quality.py -v

No live backend required. Uses mocking for external dependencies.
"""
import os
import sys
import time
import importlib
import unittest
from unittest.mock import patch, MagicMock

# Ensure project root is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# Pre-import to avoid circular import on first access
# (pre-existing circular: semantic_mapper.__init__ -> runner -> engine.__init__ -> dcl_engine -> semantic_mapper)
import backend.engine  # noqa: E402 — warm the import chain
from backend.semantic_mapper.persist_mappings import MappingPersistence as _MP  # noqa: E402


class TestConnectionPoolRetry(unittest.TestCase):
    """Fix 1.1: Connection pool must NOT be permanently locked on failure."""

    def setUp(self):
        """Reset class-level state before each test."""
        from backend.semantic_mapper.persist_mappings import MappingPersistence
        MappingPersistence._pool = None
        MappingPersistence._pool_initialized = False
        MappingPersistence._pool_last_attempt = 0

    def test_pool_not_marked_initialized_on_failure(self):
        """If pool creation fails, _pool_initialized must stay False so retry is possible."""
        from backend.semantic_mapper.persist_mappings import MappingPersistence

        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://fake:fake@localhost/fake"}):
            with patch("backend.semantic_mapper.persist_mappings.pool.SimpleConnectionPool", side_effect=Exception("Connection refused")):
                try:
                    mp = MappingPersistence()
                except Exception:
                    pass

        # The critical assertion: pool_initialized must NOT be True when pool is None
        if MappingPersistence._pool is None:
            self.assertFalse(
                MappingPersistence._pool_initialized,
                "BUG REGRESSION: _pool_initialized is True despite _pool being None. "
                "This permanently locks the system out of the database."
            )

    def test_pool_retries_after_cooldown(self):
        """After a failure, pool creation should be retried once the cooldown expires."""
        from backend.semantic_mapper.persist_mappings import MappingPersistence

        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://fake:fake@localhost/fake"}):
            with patch("backend.semantic_mapper.persist_mappings.pool.SimpleConnectionPool", side_effect=Exception("refused")):
                try:
                    mp = MappingPersistence()
                except Exception:
                    pass

            # Set last attempt far in the past to simulate cooldown expiry
            MappingPersistence._pool_last_attempt = time.time() - 999

            mock_pool = MagicMock()
            with patch("backend.semantic_mapper.persist_mappings.pool.SimpleConnectionPool", return_value=mock_pool):
                try:
                    mp = MappingPersistence()
                except Exception:
                    pass

        # After cooldown, pool should have been retried and now be available
        self.assertIsNotNone(
            MappingPersistence._pool,
            "Pool should retry initialization after cooldown period expires"
        )

    def tearDown(self):
        from backend.semantic_mapper.persist_mappings import MappingPersistence
        MappingPersistence._pool = None
        MappingPersistence._pool_initialized = False
        MappingPersistence._pool_last_attempt = 0


class TestRAGServiceTruthfulCounts(unittest.TestCase):
    """Fix 1.2: RAG service must NOT return fake success counts on failure."""

    def _make_test_mappings(self, count=3):
        from backend.domain import Mapping
        return [
            Mapping(
                id=f"test_{i}",
                source_field=f"field_{i}",
                source_table="test_table",
                source_system="test_source",
                ontology_concept="revenue",
                confidence=0.9,
                method="heuristic",
                status="ok"
            )
            for i in range(count)
        ]

    def test_returns_zero_on_pinecone_import_error(self):
        """When pinecone package is not installed, return 0, not len(mappings)."""
        from backend.engine.rag_service import RAGService

        narration = MagicMock()
        with patch.dict(os.environ, {"PINECONE_API_KEY": "fake-key"}):
            rag = RAGService("Dev", "test-run", narration)
            rag.pinecone_enabled = True

        mappings = self._make_test_mappings(5)

        # Mock _store_to_pinecone to simulate ImportError path
        def mock_store(m):
            raise ImportError("No module named 'pinecone'")

        with patch.object(rag, '_store_to_pinecone', side_effect=ImportError("No module named 'pinecone'")):
            # store_mapping_lessons calls _store_to_pinecone internally
            # but catches the exception at a higher level - let's test the outer method
            result = rag.store_mapping_lessons(mappings)

        # The result should be 0 (truthful), not 5 (a lie)
        self.assertEqual(result, 0,
            "BUG REGRESSION: RAG service returns non-zero on Pinecone failure. "
            "This makes RunMetrics.rag_writes lie about actual storage.")

    def test_returns_zero_on_pinecone_runtime_error(self):
        """When Pinecone raises a runtime error, return 0."""
        from backend.engine.rag_service import RAGService

        narration = MagicMock()
        with patch.dict(os.environ, {"PINECONE_API_KEY": "fake-key"}):
            rag = RAGService("Dev", "test-run", narration)
            rag.pinecone_enabled = True

        mappings = self._make_test_mappings(5)

        with patch.object(rag, '_store_to_pinecone', side_effect=RuntimeError("Pinecone connection failed")):
            result = rag.store_mapping_lessons(mappings)

        self.assertEqual(result, 0,
            "RAG service should return 0 on Pinecone runtime error")


class TestCacheInvalidationLogging(unittest.TestCase):
    """Fix 1.3: Cache invalidation must log warnings, not silently pass."""

    def test_logs_warning_on_mapping_cache_failure(self):
        """_invalidate_aam_caches should log when MappingPersistence.clear_all_caches fails."""
        with patch("backend.semantic_mapper.persist_mappings.MappingPersistence.clear_all_caches",
                    side_effect=Exception("DB gone")):
            with patch("backend.api.main.logger") as mock_logger:
                from backend.api.main import _invalidate_aam_caches
                _invalidate_aam_caches()

                # Verify warning was logged (not silently swallowed)
                warning_calls = [
                    call for call in mock_logger.warning.call_args_list
                    if "mapping caches" in str(call).lower() or "aam" in str(call).lower()
                ]
                self.assertTrue(
                    len(warning_calls) > 0,
                    "BUG REGRESSION: Cache invalidation failure is silently swallowed. "
                    "Should log a warning."
                )


class TestConstantsModule(unittest.TestCase):
    """Fix 2.1: Centralized constants module has expected defaults."""

    def test_constants_exist_with_expected_defaults(self):
        """All constants should exist with their documented default values."""
        from backend.core.constants import (
            RAG_CONFIDENCE_THRESHOLD,
            LLM_VALIDATION_THRESHOLD,
            LLM_MODEL_NAME,
            PINECONE_INDEX_NAME,
            PINECONE_DIMENSION,
            PINECONE_CLOUD,
            PINECONE_REGION,
            ONTOLOGY_CACHE_TTL,
            MAPPINGS_CACHE_TTL,
            SCHEMA_CACHE_TTL,
            POOL_RETRY_COOLDOWN,
            CORS_ORIGINS,
        )

        self.assertAlmostEqual(RAG_CONFIDENCE_THRESHOLD, 0.75)
        self.assertAlmostEqual(LLM_VALIDATION_THRESHOLD, 0.80)
        self.assertEqual(LLM_MODEL_NAME, "gpt-4o-mini")
        self.assertEqual(PINECONE_INDEX_NAME, "dcl-mapping-lessons")
        self.assertEqual(PINECONE_DIMENSION, 1536)
        self.assertEqual(PINECONE_CLOUD, "aws")
        self.assertEqual(PINECONE_REGION, "us-east-1")
        self.assertAlmostEqual(ONTOLOGY_CACHE_TTL, 300.0)
        self.assertAlmostEqual(MAPPINGS_CACHE_TTL, 60.0)
        self.assertAlmostEqual(SCHEMA_CACHE_TTL, 300.0)
        self.assertAlmostEqual(POOL_RETRY_COOLDOWN, 30.0)
        self.assertEqual(CORS_ORIGINS, ["*"])

    def test_confidence_thresholds_in_valid_range(self):
        """Confidence thresholds must be between 0.0 and 1.0."""
        from backend.core.constants import (
            RAG_CONFIDENCE_THRESHOLD,
            LLM_VALIDATION_THRESHOLD,
        )

        for name, val in [
            ("RAG_CONFIDENCE_THRESHOLD", RAG_CONFIDENCE_THRESHOLD),
            ("LLM_VALIDATION_THRESHOLD", LLM_VALIDATION_THRESHOLD),
        ]:
            self.assertGreaterEqual(val, 0.0, f"{name} must be >= 0.0")
            self.assertLessEqual(val, 1.0, f"{name} must be <= 1.0")


class TestCORSDefault(unittest.TestCase):
    """Fix 2.6: CORS default must be wildcard for backward compatibility."""

    def test_cors_default_is_wildcard(self):
        from backend.core.constants import CORS_ORIGINS
        self.assertIn("*", CORS_ORIGINS,
            "Default CORS_ORIGINS must include '*' for backward compatibility")


class TestBackendImports(unittest.TestCase):
    """Catch circular imports and missing dependencies across all backend modules."""

    def test_all_backend_modules_import_cleanly(self):
        """Every Python module under backend/ should import without errors."""
        import glob

        backend_dir = os.path.join(os.path.dirname(__file__), "..", "backend")
        py_files = glob.glob(os.path.join(backend_dir, "**", "*.py"), recursive=True)

        failures = []
        for py_file in py_files:
            # Convert file path to module path
            rel_path = os.path.relpath(py_file, os.path.join(os.path.dirname(__file__), ".."))
            module_name = rel_path.replace(os.sep, ".").replace(".py", "")

            if module_name.endswith(".__init__"):
                module_name = module_name[:-9]

            # Skip test files and __pycache__
            if "__pycache__" in module_name:
                continue

            try:
                importlib.import_module(module_name)
            except Exception as e:
                # Some modules need DB connections or env vars - that's OK
                # We only care about ImportError (broken imports) and SyntaxError
                if isinstance(e, (ImportError, SyntaxError)):
                    failures.append(f"{module_name}: {type(e).__name__}: {e}")

        if failures:
            self.fail(
                f"The following modules have import errors:\n" +
                "\n".join(f"  - {f}" for f in failures)
            )


class TestConstantsUsedByConsumers(unittest.TestCase):
    """Verify that consumer modules actually use the centralized constants."""

    def test_rag_service_uses_constants(self):
        """RAG service should import from constants, not use hardcoded values."""
        import inspect
        from backend.engine.rag_service import RAGService
        source = inspect.getsource(RAGService)
        # Should NOT contain hardcoded "dcl-mapping-lessons" anymore
        self.assertNotIn(
            '"dcl-mapping-lessons"', source,
            "RAG service still has hardcoded Pinecone index name"
        )

    def test_schema_loader_uses_constants(self):
        """Schema loader cache TTL should come from constants."""
        from backend.engine.schema_loader import SchemaLoader
        from backend.core.constants import SCHEMA_CACHE_TTL
        self.assertEqual(SchemaLoader._CACHE_TTL, SCHEMA_CACHE_TTL,
            "SchemaLoader._CACHE_TTL should use the centralized constant")


if __name__ == "__main__":
    unittest.main()
