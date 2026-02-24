"""
Shared Postgres connection pool for the DCL backend.

All modules that need a database connection should use this module
instead of creating their own pools. This keeps the total connection
count predictable (one pool per worker process).

Usage:
    from backend.core.db import get_connection, close_pool

    with get_connection() as conn:
        if conn is None:
            return  # DB unavailable — degrade gracefully
        with conn.cursor() as cur:
            cur.execute("SELECT ...")
"""

import os
import time
from contextlib import contextmanager
from typing import Optional

from backend.core.constants import (
    POOL_MIN_CONN,
    POOL_MAX_CONN,
    DB_CONNECT_TIMEOUT,
    POOL_RETRY_COOLDOWN,
)
from backend.utils.log_utils import get_logger

try:
    import psycopg2
    from psycopg2.pool import SimpleConnectionPool
except ImportError:
    psycopg2 = None  # type: ignore[assignment]
    SimpleConnectionPool = None  # type: ignore[assignment]

logger = get_logger(__name__)

# Module-level singleton state
_pool: Optional[SimpleConnectionPool] = None
_pool_initialized: bool = False
_pool_last_attempt: float = 0


def _ensure_pool() -> Optional[SimpleConnectionPool]:
    """Lazily initialise the shared pool. Returns the pool or None."""
    global _pool, _pool_initialized, _pool_last_attempt

    if psycopg2 is None:
        return None

    if _pool_initialized and _pool is not None:
        return _pool

    now = time.time()
    if (_pool is None
            and _pool_last_attempt > 0
            and (now - _pool_last_attempt) < POOL_RETRY_COOLDOWN):
        return None

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return None

    try:
        _pool_last_attempt = now
        _pool = SimpleConnectionPool(
            minconn=POOL_MIN_CONN,
            maxconn=POOL_MAX_CONN,
            dsn=database_url,
            connect_timeout=DB_CONNECT_TIMEOUT,
        )
        _pool_initialized = True
        logger.info(
            f"[db] Shared Postgres pool initialised "
            f"(min={POOL_MIN_CONN}, max={POOL_MAX_CONN})"
        )
        return _pool
    except Exception as e:
        logger.warning(f"[db] Postgres pool failed: {e}")
        _pool = None
        return None


@contextmanager
def get_connection():
    """Borrow a connection from the shared pool.

    Yields a ``psycopg2`` connection, or ``None`` when the database is
    unavailable (callers should handle the None case gracefully).
    """
    pg_pool = _ensure_pool()
    if pg_pool is None:
        yield None
        return

    conn = None
    try:
        conn = pg_pool.getconn()
        if conn.closed:
            pg_pool.putconn(conn, close=True)
            conn = pg_pool.getconn()
        yield conn
    except Exception as e:
        logger.warning(f"[db] Connection error: {e}")
        yield None
    finally:
        if conn is not None and pg_pool is not None:
            try:
                pg_pool.putconn(conn)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass


def close_pool() -> None:
    """Close the shared pool. Call once on shutdown."""
    global _pool, _pool_initialized
    if _pool is not None:
        try:
            _pool.closeall()
            logger.info("[db] Shared Postgres pool closed")
        except Exception as e:
            logger.warning(f"[db] Error closing pool: {e}")
        finally:
            _pool = None
            _pool_initialized = False
