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
    POOL_PREPING_IDLE_S,
    QUERY_STATEMENT_TIMEOUT_MS,
)
from backend.utils.log_utils import get_logger

try:
    import psycopg2
    from psycopg2 import extensions as _pg_extensions
    from psycopg2.pool import ThreadedConnectionPool
except ImportError:
    psycopg2 = None  # type: ignore[assignment]
    _pg_extensions = None  # type: ignore[assignment]
    ThreadedConnectionPool = None  # type: ignore[assignment]

logger = get_logger(__name__)


class PoolExhausted(Exception):
    """Raised when all connections are checked out and getconn() times out."""


# ---------------------------------------------------------------------------
# Read statement_timeout binding (dcl ledger #62)
#
# The Supabase pooler ignores connection-string startup options on BOTH ports,
# so the pool-init `-c statement_timeout` never reaches dev connections — the
# 15s read ceiling was silently absent (role default 2min applied). The
# txn-safe mechanism is SET LOCAL, but a dedicated per-borrow execute costs a
# full pooler round trip (~76ms p50 from this bench — a B18 violation on read
# endpoints). Instead, cursors prefix the SET LOCAL onto the FIRST statement
# of each transaction: psycopg2 sends multi-statement simple-query strings in
# ONE round trip, so the ceiling binds at zero marginal cost.
#
#  - The prefix fires only when the connection's transaction status is IDLE
#    (a fresh transaction); statements after the first ride the same txn.
#  - Write paths that need a bigger budget keep working unchanged: their own
#    `SET LOCAL statement_timeout` becomes "SET LOCAL 15s; SET LOCAL <theirs>"
#    — the later SET LOCAL wins for the rest of the transaction.
#  - copy_expert() bypasses execute(); every COPY path in this repo issues an
#    explicit SET LOCAL execute first, which the prefix combines with.
#  - Parameter mogrification applies to the combined string; the prefix
#    carries no placeholders, so client-side interpolation is unaffected.
#  - Works for any requested cursor class (incl. RealDictCursor): the factory
#    wraps whatever class the caller asked for, cached per base class.
# ---------------------------------------------------------------------------

_bound_factory_cache: dict = {}


def _bound_cursor_factory(base):
    """Return (cached) subclass of `base` that binds the read ceiling on the
    first statement of every transaction."""
    cached = _bound_factory_cache.get(base)
    if cached is not None:
        return cached

    prefix = f"SET LOCAL statement_timeout = {int(QUERY_STATEMENT_TIMEOUT_MS)}; "

    class _TimeoutBoundCursor(base):  # type: ignore[misc,valid-type]
        def execute(self, query, vars=None):
            try:
                fresh_txn = (
                    self.connection.info.transaction_status
                    == _pg_extensions.TRANSACTION_STATUS_IDLE
                )
            except Exception:
                fresh_txn = False
            if fresh_txn and isinstance(query, str):
                query = prefix + query
            return super().execute(query, vars)

    _TimeoutBoundCursor.__name__ = f"TimeoutBound{getattr(base, '__name__', 'Cursor')}"
    _bound_factory_cache[base] = _TimeoutBoundCursor
    return _TimeoutBoundCursor


def _timeout_bound_connection_class():
    """Connection class whose cursor() wraps WHATEVER cursor class the caller
    requests — an explicit cursor_factory argument (e.g. canonical_registry's
    RealDictCursor) would bypass a connection-level cursor_factory attribute,
    so the wrap must happen inside cursor() itself."""
    class _TimeoutBoundConnection(_pg_extensions.connection):
        def cursor(self, *args, **kwargs):
            base = (kwargs.pop("cursor_factory", None)
                    or self.cursor_factory
                    or _pg_extensions.cursor)
            kwargs["cursor_factory"] = _bound_cursor_factory(base)
            return super().cursor(*args, **kwargs)

    return _TimeoutBoundConnection


# Module-level singleton state
_pool: Optional[ThreadedConnectionPool] = None
_pool_initialized: bool = False
_pool_last_attempt: float = 0

# ---------------------------------------------------------------------------
# Idle-age-gated pre-ping (ledger #67)
#
# The FIN-sniff in get_connection() is free but only sees drops whose FIN
# already reached our socket buffer. Two observed #67 modes escape it: the
# pooler's server-side leg dying (client TCP looks healthy; the next query
# 500s with "SSL connection has been closed unexpectedly") and silent drops
# (the next query hangs). Both strike connections that sat IDLE past the
# pooler's reap horizon — so the validating SELECT 1 round trip is paid only
# on borrows whose connection has been unused > POOL_PREPING_IDLE_S.
# Connections cycling hot pay a dict lookup (no round trip): the #62 bench
# put an unconditional per-borrow execute at ~76ms p50 — a B18 violation
# this gate avoids. Unknown connections (fresh minconn stock, pool-created
# replacements) have no recorded return time and are pinged — the safe
# default. Ping failure → discard, reborrrow once, ping again; a second
# failure raises loudly with both errors.
#
# The ping is sent as BYTES (b"SELECT 1"): the TimeoutBound cursor wrapper
# prefixes SET LOCAL onto str queries only, and SET LOCAL outside a
# transaction (the ping runs in autocommit to leave no open txn behind)
# would emit a server warning on every validation.
# ---------------------------------------------------------------------------

_last_returned: dict = {}
_last_returned_lock = threading.Lock()


def _record_return(conn) -> None:
    try:
        with _last_returned_lock:
            _last_returned[id(conn)] = time.monotonic()
    except Exception:
        pass


def _ping(conn) -> None:
    """One SELECT 1 round trip in autocommit (no txn left open, no SET LOCAL
    prefix — bytes query bypasses the TimeoutBound str-only wrapper)."""
    prior_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(b"SELECT 1")
            cur.fetchone()
    finally:
        conn.autocommit = prior_autocommit


def _validate_if_idle(pg_pool, conn):
    """Pre-ping connections idle past POOL_PREPING_IDLE_S; reconnect once on
    failure. Returns a validated (or hot, assumed-good) connection. Raises
    RuntimeError if a fresh connection fails the ping too."""
    with _last_returned_lock:
        returned_at = _last_returned.get(id(conn))
    if returned_at is not None and (time.monotonic() - returned_at) <= POOL_PREPING_IDLE_S:
        return conn  # hot path: no round trip

    try:
        _ping(conn)
        return conn
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as first_err:
        logger.warning(
            "[db] Pre-ping failed on idle connection (%s) — discarding and "
            "borrowing a fresh one (ledger #67 reconnect-once)", first_err
        )
        try:
            pg_pool.putconn(conn, close=True)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
        fresh = _getconn_with_timeout(pg_pool, POOL_GETCONN_TIMEOUT)
        try:
            _ping(fresh)
            return fresh
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as second_err:
            try:
                pg_pool.putconn(fresh, close=True)
            except Exception:
                pass
            raise RuntimeError(
                f"[db] Pre-ping failed twice — database unreachable through the "
                f"pool. First (stale connection): {first_err}. Second (fresh "
                f"connection): {second_err}. Check Supabase pooler health."
            ) from second_err


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
            # Binds the read statement_timeout on the first statement of every
            # transaction at zero marginal round trips (ledger #62) — see
            # _bound_cursor_factory above.
            connection_factory=_timeout_bound_connection_class(),
            connect_timeout=DB_CONNECT_TIMEOUT,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
            # Honored on prod's direct db.<ref> host; the Supabase pooler
            # IGNORES startup options on both ports (dcl ledger #62, verified
            # 2026-06-11) — in dev the binding mechanism is the SET LOCAL
            # issued per borrow in get_connection() below.
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
        # Idle-age-gated pre-ping (ledger #67): hot connections skip with a
        # dict lookup; idle/unknown ones pay one validating round trip and
        # reconnect-once on failure.
        conn = _validate_if_idle(pg_pool, conn)
        # Read statement_timeout binding happens inside the connection class
        # (connection_factory at pool init) — nothing to do per borrow.
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
            _record_return(conn)
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
