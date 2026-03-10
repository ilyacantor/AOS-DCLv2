"""
Module-level engine computation cache.

Caches the expensive engine outputs (cross-sell, EBITDA bridge, QofE) so they
are computed once and reused across requests until the underlying data files
change on disk.

Invalidation: tracks mtime of every source data file.  If any mtime advances,
the entire cache is flushed.  This is correct because the engines form a
dependency chain (cross-sell → bridge → qoe → dashboards) — a change to any
input invalidates all downstream results.

Thread-safety: uses a threading.Lock around cache reads and writes.  The
computations themselves are CPU-bound and short (~1-2s total for the full
chain), so holding the lock during computation is acceptable.

This module does NOT introduce silent fallbacks.  If a computation fails, the
exception propagates to the caller.  No empty results, no default data, no
swallowed errors.
"""

import json
import threading
import time
from pathlib import Path
from typing import Any

from backend.utils.log_utils import get_logger

logger = get_logger("engine_cache")

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# Source data files that feed the engine chain.
_SOURCE_FILES = [
    _DATA_DIR / "combining_statements.json",
    _DATA_DIR / "entity_overlap.json",
    _DATA_DIR / "customer_profiles.json",
]

_lock = threading.Lock()

# Cache state
_cached_results: dict[str, Any] = {}
_cached_mtimes: dict[str, float] = {}  # filepath → mtime at cache time
_cache_built_at: float = 0.0


def _current_mtimes() -> dict[str, float]:
    """Read current mtimes for all source files.  Raises FileNotFoundError if missing."""
    mtimes = {}
    for path in _SOURCE_FILES:
        if not path.exists():
            raise FileNotFoundError(
                f"Required data file not found: {path}. "
                f"Run scripts/generate_combining_data.py first."
            )
        mtimes[str(path)] = path.stat().st_mtime
    return mtimes


def _cache_is_valid() -> bool:
    """Check if cached results are still valid (source files unchanged)."""
    if not _cached_results:
        return False
    try:
        current = _current_mtimes()
    except FileNotFoundError:
        return False
    return current == _cached_mtimes


def _build_cache() -> None:
    """Compute all engine outputs and populate the cache.

    Called under _lock.  Any exception propagates — no fallbacks.
    """
    global _cached_results, _cached_mtimes, _cache_built_at

    t0 = time.monotonic()

    # Snapshot mtimes BEFORE computation so we know what data we computed against
    mtimes = _current_mtimes()

    # Load shared data files once
    logger.info("[engine_cache] Building cache — loading data files")
    with open(_DATA_DIR / "combining_statements.json") as f:
        combining = json.load(f)
    with open(_DATA_DIR / "entity_overlap.json") as f:
        overlap = json.load(f)

    # Cross-sell (depends on customer_profiles.json + entity_overlap.json)
    t_xs = time.monotonic()
    from backend.engine.cross_sell import run_cross_sell_engine
    pipeline = run_cross_sell_engine()
    cross_sell = pipeline.to_dict()
    logger.info("[engine_cache]   cross_sell: %.0fms", (time.monotonic() - t_xs) * 1000)

    # EBITDA bridge (depends on combining + overlap + cross_sell)
    t_eb = time.monotonic()
    from backend.engine.ebitda_bridge import compute_ebitda_bridge
    bridge = compute_ebitda_bridge(cross_sell_pipeline=cross_sell)
    logger.info("[engine_cache]   ebitda_bridge: %.0fms", (time.monotonic() - t_eb) * 1000)

    # QofE (depends on bridge + combining + overlap + customers)
    t_qoe = time.monotonic()
    from backend.engine.qoe import compute_qoe
    qoe = compute_qoe()
    logger.info("[engine_cache]   qoe: %.0fms", (time.monotonic() - t_qoe) * 1000)

    elapsed = (time.monotonic() - t0) * 1000

    _cached_results = {
        "combining": combining,
        "overlap": overlap,
        "cross_sell": cross_sell,
        "bridge": bridge,
        "qoe": qoe,
    }
    _cached_mtimes = mtimes
    _cache_built_at = time.monotonic()

    logger.info(
        "[engine_cache] Cache built in %.0fms — 5 results cached, "
        "source mtimes: %s",
        elapsed,
        {Path(k).name: v for k, v in mtimes.items()},
    )


def get(key: str) -> Any:
    """Get a cached engine result by key.

    Valid keys: "combining", "overlap", "cross_sell", "bridge", "qoe".

    If the cache is stale or empty, recomputes the full chain.
    Raises FileNotFoundError if source data files are missing.
    Raises any computation error without catching.
    """
    with _lock:
        if not _cache_is_valid():
            logger.info("[engine_cache] Cache miss — rebuilding")
            _build_cache()
        result = _cached_results.get(key)

    if result is None:
        raise KeyError(
            f"Unknown engine cache key '{key}'. "
            f"Valid keys: {sorted(_cached_results.keys())}"
        )
    return result


def invalidate() -> None:
    """Force-invalidate the cache.  Next get() will recompute."""
    global _cached_results, _cached_mtimes, _cache_built_at
    with _lock:
        _cached_results = {}
        _cached_mtimes = {}
        _cache_built_at = 0.0
        logger.info("[engine_cache] Cache invalidated")


def cache_age_seconds() -> float:
    """Return seconds since cache was last built, or -1 if never built."""
    if _cache_built_at == 0.0:
        return -1.0
    return time.monotonic() - _cache_built_at
