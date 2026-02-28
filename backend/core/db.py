"""
Shared Postgres connection pool for the DCL backend.

All modules that need a database connection should use this module
instead of creating their own pools. This keeps the total connection
count predictable (one pool per worker process).

Uses ThreadedConnectionPool for thread safety — uvicorn runs sync
endpoints in a threadpool, so multiple threads may borrow connections
concurrently. SimpleConnectionPool is NOT safe for this.

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
import threading
from contextlib import contextmanager
from typing import Optional

from backend.core.constants import (
    POOL_MIN_CONN,
    POOL_MAX_CONN,
    DB_CONNECT_TIMEOUT,
    POOL_RETRY_COOLDOWN,
    POOL_GETCONN_TIMEOUT,
)
from backend.utils.log_utils import get_logger

try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
except ImportError:
    psycopg2 = None  # type: ignore[assignment]
    ThreadedConnectionPool = None  # type: ignore[assignment]

logger = get_logger(__name__)


class PoolExhausted(Exception):
    """Raised when all connections are checked out and getconn() times out."""


# Module-level singleton state
_pool: Optional[ThreadedConnectionPool] = None
_pool_initialized: bool = False
_pool_last_attempt: float = 0


def _ensure_pool() -> Optional[ThreadedConnectionPool]:
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
        _pool = ThreadedConnectionPool(
            minconn=POOL_MIN_CONN,
            maxconn=POOL_MAX_CONN,
            dsn=database_url,
            connect_timeout=DB_CONNECT_TIMEOUT,
        )
        _pool_initialized = True
        logger.info(
            f"[db] Shared Postgres pool initialised "
            f"(min={POOL_MIN_CONN}, max={POOL_MAX_CONN}, "
            f"connect_timeout={DB_CONNECT_TIMEOUT}s, "
            f"getconn_timeout={POOL_GETCONN_TIMEOUT}s)"
        )
        return _pool
    except Exception as e:
        logger.warning(f"[db] Postgres pool failed: {e}")
        _pool = None
        return None


def _getconn_with_timeout(pool: ThreadedConnectionPool, timeout: float):
    """Borrow a connection with a timeout.

    ThreadedConnectionPool.getconn() blocks indefinitely when all
    connections are checked out. This wrapper uses a thread + Event
    to enforce a maximum wait time.
    """
    result = [None]
    error = [None]
    done = threading.Event()

    def _fetch():
        try:
            result[0] = pool.getconn()
        except Exception as e:
            error[0] = e
        finally:
            done.set()

    t = threading.Thread(target=_fetch, daemon=True)
    t.start()

    if not done.wait(timeout=timeout):
        raise PoolExhausted(
            f"Connection pool exhausted ({POOL_MAX_CONN}/{POOL_MAX_CONN} in use). "
            f"Timed out after {timeout}s waiting for a free connection. "
            f"Graph build or ingest may be holding connections. "
            f"Check for long-running transactions."
        )

    if error[0] is not None:
        raise error[0]

    return result[0]


@contextmanager
def get_connection():
    """Borrow a connection from the shared pool.

    Yields a ``psycopg2`` connection, or ``None`` when the database is
    unavailable (callers should handle the None case gracefully).

    Raises PoolExhausted if all connections are checked out and the
    wait exceeds POOL_GETCONN_TIMEOUT seconds.
    """
    pg_pool = _ensure_pool()
    if pg_pool is None:
        yield None
        return

    conn = None
    try:
        conn = _getconn_with_timeout(pg_pool, POOL_GETCONN_TIMEOUT)
        if conn.closed:
            pg_pool.putconn(conn, close=True)
            conn = _getconn_with_timeout(pg_pool, POOL_GETCONN_TIMEOUT)
        yield conn
    except PoolExhausted:
        raise  # Let callers handle pool exhaustion explicitly
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
                except Exception as e:
                    logger.error(
                        f"[db] Failed to close connection during pool return: {e}. "
                        "Pool may be corrupted — monitor borrow failures.",
                        exc_info=True
                    )


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
