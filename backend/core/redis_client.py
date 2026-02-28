"""
Shared Redis client for the DCL backend.

All modules that need Redis should import from here instead of creating
their own connections. This keeps the connection count predictable.

Usage:
    from backend.core.redis_client import get_redis, is_redis_available

    r = get_redis()
    if r is None:
        return  # Redis unavailable — degrade gracefully
    r.set("key", "value")
"""

import os
from typing import Optional

from backend.utils.log_utils import get_logger

try:
    import redis as _redis_lib
except ImportError:
    _redis_lib = None  # type: ignore[assignment]

logger = get_logger(__name__)

_client: Optional[_redis_lib.Redis] = None  # type: ignore[union-attr]
_initialized: bool = False
_redis_available: bool = False


def get_redis():
    """Return the shared Redis client, or None if unavailable."""
    global _client, _initialized, _redis_available

    if _initialized:
        return _client

    _initialized = True

    if _redis_lib is None:
        return None

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        logger.warning("[redis] REDIS_URL not set — running without Redis")
        return None

    try:
        _client = _redis_lib.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        _client.ping()
        _redis_available = True
        logger.info("[redis] Shared Redis client connected")
        return _client
    except Exception as e:
        logger.error(
            f"[redis] Redis unavailable: {e} — running without Redis. "
            f"Data will be stored in-memory only and lost on restart. "
            f"REDIS_URL={redis_url[:20]}..."
        )
        _client = None
        _redis_available = False
        return None


def is_redis_available() -> bool:
    """Check if Redis is currently reachable.

    Re-pings on each call so the health endpoint reflects real-time state.
    """
    if _client is None:
        return False
    try:
        _client.ping()
        return True
    except Exception:
        return False
