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
        with conn.cursor() as cur:
            cur.execute("SELECT ...")

    # get_connection() raises RuntimeError or PoolExhausted on failure —
    # it never returns None. Do not check `if conn is None`.
"""

import os
import select
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
    QUERY_STATEMENT_TIMEOUT_MS,
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

    # Extract host for diagnostic messages (mask password)
    try:
        from urllib.parse import urlparse
        parsed = urlparse(database_url)
        db_host = parsed.hostname or "unknown"
    except Exception:
        db_host = "unknown"

    try:
        _pool_last_attempt = now
        _pool = ThreadedConnectionPool(
            minconn=POOL_MIN_CONN,
            maxconn=POOL_MAX_CONN,
            dsn=database_url,
            connect_timeout=DB_CONNECT_TIMEOUT,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
            options=f"-c statement_timeout={QUERY_STATEMENT_TIMEOUT_MS}",
        )

        # Startup validation: verify we can actually use the pool
        test_conn = _pool.getconn()
        try:
            with test_conn.cursor() as cur:
                cur.execute("SELECT 1")
        finally:
            _pool.putconn(test_conn)

        _pool_initialized = True
        logger.info(
            f"[db] Shared Postgres pool initialised "
            f"(min={POOL_MIN_CONN}, max={POOL_MAX_CONN}, "
            f"connect_timeout={DB_CONNECT_TIMEOUT}s, "
            f"getconn_timeout={POOL_GETCONN_TIMEOUT}s, "
            f"host={db_host})"
        )
        return _pool
    except Exception as e:
        if _pool is not None:
            try:
                _pool.closeall()
            except Exception:
                pass
        _pool = None
        raise RuntimeError(
            f"DCL startup: cannot connect to database. "
            f"POOL_MAX_CONN={POOL_MAX_CONN}, host={db_host}. "
            f"Check DATABASE_URL and instance max_connections. "
            f"Error: {e}"
        ) from e


def _getconn_with_timeout(pool: ThreadedConnectionPool, timeout: float):
    """Borrow a connection with a timeout.

    ThreadedConnectionPool.getconn() blocks indefinitely when all
    connections are checked out. This wrapper uses a thread + Event
    to enforce a maximum wait time.

    If the caller times out, the daemon thread may still eventually
    acquire a connection. The ``timed_out`` flag ensures the thread
    returns that connection to the pool instead of leaking it.
    """
    result = [None]
    error = [None]
    timed_out = [False]
    done = threading.Event()

    def _fetch():
        try:
            result[0] = pool.getconn()
        except Exception as e:
            error[0] = e
        finally:
            done.set()
            # If the caller already timed out, return the connection
            # so it isn't permanently leaked from the pool.
            if timed_out[0] and result[0] is not None:
                try:
                    pool.putconn(result[0])
                    logger.warning(
                        "[db] Returned orphaned connection after caller timeout — "
                        "pool leak averted"
                    )
                except Exception:
                    pass

    t = threading.Thread(target=_fetch, daemon=True)
    t.start()

    if not done.wait(timeout=timeout):
        timed_out[0] = True
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

    Yields a ``psycopg2`` connection.

    Raises RuntimeError if the database is unavailable.
    Raises PoolExhausted if all connections are checked out and the
    wait exceeds POOL_GETCONN_TIMEOUT seconds.
    """
    pg_pool = _ensure_pool()
    if pg_pool is None:
        raise RuntimeError(
            "[db] Connection pool unavailable (within retry cooldown). "
            "Check DATABASE_URL and Supabase connectivity."
        )

    conn = None
    try:
        conn = _getconn_with_timeout(pg_pool, POOL_GETCONN_TIMEOUT)
        # Detect server-side connection closes without a network round-trip.
        # When Supabase drops an idle connection it sends a TCP FIN.
        # select() with timeout=0 checks for pending FIN/RST on the socket
        # in microseconds — no latency cost. A readable idle connection
        # means the server closed it; discard and borrow a fresh one.
        # conn.closed alone cannot detect this (it only reflects local state).
        if conn.closed:
            pg_pool.putconn(conn, close=True)
            conn = _getconn_with_timeout(pg_pool, POOL_GETCONN_TIMEOUT)
        else:
            try:
                fd = conn.fileno()
                if fd >= 0 and select.select([fd], [], [], 0)[0]:
                    pg_pool.putconn(conn, close=True)
                    conn = _getconn_with_timeout(pg_pool, POOL_GETCONN_TIMEOUT)
            except Exception as exc:
                logger.warning("Stale connection detection failed: %s", exc)
        yield conn
    except PoolExhausted:
        raise  # Let callers handle pool exhaustion explicitly
    except Exception as e:
        # Re-raise exceptions thrown from inside the `with` block (e.g.
        # HTTPException from route handlers).  Only true connection-setup
        # errors (before the yield) should be caught, but @contextmanager
        # funnels caller exceptions through the same except clause.
        # Since the yield already happened at this point, the only safe
        # action is to let the exception propagate.
        raise
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
