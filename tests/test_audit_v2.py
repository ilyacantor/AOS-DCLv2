"""
Code Quality Audit v2 — Regression Tests
==========================================
Validates all fixes from the v2 code quality audit remain in place.
Run with: pytest tests/test_audit_v2.py -v

No live backend, database, or API keys required.
"""
import os
import re
import sys
import inspect
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Ensure project root is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Pre-import to avoid circular import
import backend.engine  # noqa: E402

PROJECT_ROOT = Path(__file__).parent.parent


# ─── Phase 1: Critical Fixes ────────────────────────────────────────────────


class TestFabricatedMetrics(unittest.TestCase):
    """H1/H2: rag_reads must never be set to a fabricated value."""

    def test_no_fabricated_rag_reads_in_engine(self):
        """dcl_engine.py must not contain 'rag_reads = 3' or similar fabrication."""
        engine_path = PROJECT_ROOT / "backend" / "engine" / "dcl_engine.py"
        source = engine_path.read_text()
        self.assertNotIn(
            "rag_reads = 3", source,
            "BUG REGRESSION: Fabricated 'rag_reads = 3' still in dcl_engine.py. "
            "This lies to RunMetrics consumers about actual RAG activity."
        )
        # Also check there's no hardcoded rag_reads assignment with other magic numbers
        matches = re.findall(r"rag_reads\s*=\s*\d+", source)
        for match in matches:
            # Allow 'rag_reads = 0' only when it's in a legitimate reset context
            if "= 0" not in match:
                self.fail(f"Fabricated metric found: '{match}' in dcl_engine.py")


class TestEmbeddingCorruption(unittest.TestCase):
    """F3: OpenAI embedding failure must NOT fall back to mock embeddings in Prod."""

    def test_no_mock_fallback_on_openai_failure(self):
        """_create_embeddings_openai must return [] on error, not mock embeddings."""
        from backend.engine.rag_service import RAGService
        source = inspect.getsource(RAGService._create_embeddings_openai)
        self.assertNotIn(
            "_create_mock_embeddings", source,
            "BUG REGRESSION: _create_embeddings_openai still falls back to mock embeddings. "
            "This corrupts the Pinecone index with random vectors."
        )
        self.assertIn(
            "return []", source,
            "_create_embeddings_openai should return empty list on failure"
        )


class TestUTCTimestamp(unittest.TestCase):
    """H13: graph meta.generated_at must use UTC via utc_now()."""

    def test_engine_uses_utc_now(self):
        """dcl_engine.py should import and use utc_now(), not time.strftime()."""
        engine_path = PROJECT_ROOT / "backend" / "engine" / "dcl_engine.py"
        source = engine_path.read_text()
        self.assertNotIn(
            'time.strftime("%Y-%m-%dT%H:%M:%SZ")', source,
            "BUG REGRESSION: dcl_engine.py still uses time.strftime for generated_at. "
            "This produces local-time timestamps instead of UTC."
        )
        self.assertIn("utc_now", source, "dcl_engine.py should use utc_now()")

    def test_utc_now_returns_utc_format(self):
        """utc_now() must return a Z-terminated UTC timestamp."""
        from backend.core.constants import utc_now
        ts = utc_now()
        self.assertTrue(ts.endswith("Z"), f"utc_now() returned '{ts}' — must end with Z")
        # Should be parseable
        from datetime import datetime
        parsed = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        self.assertIsNotNone(parsed)


class TestConnectionPoolLeak(unittest.TestCase):
    """F7: Connection must be closed if putconn() fails."""

    def test_conn_closed_on_putconn_failure(self):
        """If putconn raises, the connection itself must be closed."""
        source_path = PROJECT_ROOT / "backend" / "semantic_mapper" / "persist_mappings.py"
        source = source_path.read_text()
        # Look for the pattern: putconn fails → conn.close()
        self.assertIn(
            "conn.close()", source,
            "BUG REGRESSION: persist_mappings.py does not close connection when putconn fails. "
            "This causes gradual pool exhaustion."
        )


# ─── Phase 2: Configuration Externalization ──────────────────────────────────


class TestSourceAliasesYAML(unittest.TestCase):
    """H3/H4/H5: Source normalizer config must be externalized to YAML."""

    def test_yaml_config_exists(self):
        """config/source_aliases.yaml must exist."""
        yaml_path = PROJECT_ROOT / "config" / "source_aliases.yaml"
        self.assertTrue(yaml_path.exists(), "config/source_aliases.yaml not found")

    def test_yaml_has_required_sections(self):
        """YAML must have alias_map, pattern_rules, and category_patterns."""
        import yaml
        yaml_path = PROJECT_ROOT / "config" / "source_aliases.yaml"
        with open(yaml_path) as f:
            cfg = yaml.safe_load(f)
        self.assertIn("alias_map", cfg, "Missing alias_map section")
        self.assertIn("pattern_rules", cfg, "Missing pattern_rules section")
        self.assertIn("category_patterns", cfg, "Missing category_patterns section")
        # Sanity: should have at least as many aliases as major vendors
        self.assertGreater(len(cfg["alias_map"]), 20,
            "alias_map should have 20+ vendor aliases")

    def test_normalizer_loads_from_yaml(self):
        """SourceNormalizer should load aliases from YAML, not just hardcoded."""
        from backend.engine.source_normalizer import SourceNormalizer
        normalizer = SourceNormalizer()
        # If YAML was loaded, we should have more than the minimal fallback
        self.assertGreater(len(normalizer.ALIAS_MAP), 5,
            "SourceNormalizer.ALIAS_MAP has too few entries — YAML not loaded?")
        self.assertIn("salesforce", normalizer.ALIAS_MAP)
        self.assertIn("hubspot", normalizer.ALIAS_MAP)


class TestNewConstants(unittest.TestCase):
    """H6/H8/H9/H14: New constants must exist in constants.py."""

    def test_all_new_constants_exist(self):
        """All Phase 2 constants must be importable with correct types."""
        from backend.core.constants import (
            CB_COOLDOWN,
            FARM_REGISTRY_TIMEOUT,
            OPENAI_EMBEDDING_MODEL,
            POOL_MIN_CONN,
            POOL_MAX_CONN,
            DB_CONNECT_TIMEOUT,
        )
        self.assertIsInstance(CB_COOLDOWN, float)
        self.assertIsInstance(FARM_REGISTRY_TIMEOUT, float)
        self.assertIsInstance(OPENAI_EMBEDDING_MODEL, str)
        self.assertIsInstance(POOL_MIN_CONN, int)
        self.assertIsInstance(POOL_MAX_CONN, int)
        self.assertIsInstance(DB_CONNECT_TIMEOUT, int)
        self.assertGreater(CB_COOLDOWN, 0)
        self.assertGreater(FARM_REGISTRY_TIMEOUT, 0)


class TestMockEmbeddingDimension(unittest.TestCase):
    """H7: Mock embeddings must use PINECONE_DIMENSION, not hardcoded 1536."""

    def test_no_hardcoded_1536_in_rag_service(self):
        """rag_service.py must not contain 'range(1536)'."""
        rag_path = PROJECT_ROOT / "backend" / "engine" / "rag_service.py"
        source = rag_path.read_text()
        self.assertNotIn(
            "range(1536)", source,
            "BUG REGRESSION: rag_service.py still has hardcoded 'range(1536)'. "
            "Should use PINECONE_DIMENSION constant."
        )

    def test_no_hardcoded_embedding_model(self):
        """rag_service.py must not contain hardcoded model string."""
        rag_path = PROJECT_ROOT / "backend" / "engine" / "rag_service.py"
        source = rag_path.read_text()
        self.assertNotIn(
            '"text-embedding-3-small"', source,
            "BUG REGRESSION: rag_service.py still has hardcoded embedding model. "
            "Should use OPENAI_EMBEDDING_MODEL constant."
        )


class TestDevServerPort(unittest.TestCase):
    """H10: Backend dev server must default to port 8000, not 5000."""

    def test_main_uses_8000(self):
        """main.py __main__ block should use port 8000 (or BACKEND_PORT env)."""
        main_path = PROJECT_ROOT / "backend" / "api" / "main.py"
        source = main_path.read_text()
        # Should NOT have hardcoded port=5000
        if 'if __name__ == "__main__"' in source:
            main_block = source[source.index('if __name__ == "__main__"'):]
            self.assertNotIn(
                "port=5000", main_block,
                "BUG REGRESSION: main.py still uses port=5000. Should use 8000."
            )


class TestPersistMappingsUsesConstants(unittest.TestCase):
    """H9: persist_mappings.py pool params must come from constants."""

    def test_pool_params_from_constants(self):
        """MappingPersistence pool params should match constants."""
        from backend.core.constants import POOL_MIN_CONN, POOL_MAX_CONN, DB_CONNECT_TIMEOUT
        from backend.semantic_mapper.persist_mappings import MappingPersistence
        self.assertEqual(MappingPersistence.POOL_MIN_CONN, POOL_MIN_CONN)
        self.assertEqual(MappingPersistence.POOL_MAX_CONN, POOL_MAX_CONN)
        self.assertEqual(MappingPersistence.CONNECT_TIMEOUT, DB_CONNECT_TIMEOUT)


# ─── Phase 3: Fallback Transparency ─────────────────────────────────────────


class TestFallbackFlags(unittest.TestCase):
    """F1/F2: RunMetrics must have db_fallback and llm_fallback fields."""

    def test_run_metrics_has_fallback_fields(self):
        """RunMetrics model must expose db_fallback and llm_fallback."""
        from backend.domain.models import RunMetrics
        metrics = RunMetrics()
        self.assertFalse(metrics.db_fallback, "db_fallback default should be False")
        self.assertFalse(metrics.llm_fallback, "llm_fallback default should be False")

    def test_engine_sets_db_fallback_on_failure(self):
        """dcl_engine.py must set metrics.db_fallback = True when DB fails."""
        engine_path = PROJECT_ROOT / "backend" / "engine" / "dcl_engine.py"
        source = engine_path.read_text()
        self.assertIn(
            "db_fallback = True", source,
            "dcl_engine.py does not set db_fallback flag when DB is unavailable"
        )

    def test_engine_sets_llm_fallback_on_failure(self):
        """dcl_engine.py must set metrics.llm_fallback = True when LLM fails."""
        engine_path = PROJECT_ROOT / "backend" / "engine" / "dcl_engine.py"
        source = engine_path.read_text()
        self.assertIn(
            "llm_fallback = True", source,
            "dcl_engine.py does not set llm_fallback flag when LLM validation fails"
        )


class TestCHROPersona(unittest.TestCase):
    """H11: CHRO persona must be available in backend and frontend types."""

    def test_backend_has_chro(self):
        """Backend Persona enum must include CHRO."""
        from backend.domain.models import Persona
        self.assertIn("CHRO", [p.value for p in Persona],
            "Backend Persona enum missing CHRO")

    def test_frontend_types_has_chro(self):
        """Frontend types.ts must include CHRO in PersonaId."""
        types_path = PROJECT_ROOT / "src" / "types.ts"
        source = types_path.read_text()
        self.assertIn("CHRO", source,
            "Frontend types.ts missing CHRO in PersonaId type")

    def test_app_tsx_includes_chro(self):
        """App.tsx ALL_PERSONAS must include CHRO."""
        app_path = PROJECT_ROOT / "src" / "App.tsx"
        source = app_path.read_text()
        self.assertIn("'CHRO'", source,
            "App.tsx ALL_PERSONAS array missing CHRO")


class TestFrontendRunMetricsTypes(unittest.TestCase):
    """Frontend RunMetrics type must include fallback fields."""

    def test_types_ts_has_fallback_fields(self):
        """types.ts RunMetrics must have dbFallback and llmFallback."""
        types_path = PROJECT_ROOT / "src" / "types.ts"
        source = types_path.read_text()
        self.assertIn("dbFallback", source, "types.ts RunMetrics missing dbFallback")
        self.assertIn("llmFallback", source, "types.ts RunMetrics missing llmFallback")


# ─── Phase 4: Comprehensive Grep Checks ─────────────────────────────────────


class TestCodebaseGrepChecks(unittest.TestCase):
    """Verify no prohibited patterns remain in codebase."""

    def _grep_backend(self, pattern: str, exclude_files: list = None) -> list:
        """Search all Python files under backend/ for a pattern."""
        exclude_files = exclude_files or []
        results = []
        backend_dir = PROJECT_ROOT / "backend"
        for py_file in backend_dir.rglob("*.py"):
            if any(excl in str(py_file) for excl in exclude_files):
                continue
            content = py_file.read_text()
            for i, line in enumerate(content.splitlines(), 1):
                if re.search(pattern, line):
                    results.append(f"{py_file.relative_to(PROJECT_ROOT)}:{i}: {line.strip()}")
        return results

    def test_no_fabricated_rag_reads(self):
        """No 'rag_reads = 3' or similar fabrication in backend."""
        matches = self._grep_backend(r"rag_reads\s*=\s*[1-9]")
        self.assertEqual(matches, [],
            f"Fabricated rag_reads found:\n" + "\n".join(matches))

    def test_no_hardcoded_embedding_dimension(self):
        """No 'range(1536)' in backend."""
        matches = self._grep_backend(r"range\(1536\)")
        self.assertEqual(matches, [],
            f"Hardcoded embedding dimension found:\n" + "\n".join(matches))

    def test_no_hardcoded_embedding_model(self):
        """No hardcoded 'text-embedding-3-small' in backend (except constants.py default)."""
        matches = self._grep_backend(r'"text-embedding-3-small"', exclude_files=["constants.py"])
        self.assertEqual(matches, [],
            f"Hardcoded embedding model found:\n" + "\n".join(matches))

    def test_no_local_time_strftime_in_engine(self):
        """No time.strftime in dcl_engine.py."""
        engine_path = PROJECT_ROOT / "backend" / "engine" / "dcl_engine.py"
        source = engine_path.read_text()
        self.assertNotIn("time.strftime", source,
            "dcl_engine.py still uses time.strftime instead of utc_now()")


if __name__ == "__main__":
    unittest.main()
