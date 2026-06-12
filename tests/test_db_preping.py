"""Ledger #67 — idle-age-gated pre-ping at the pool boundary (backend/core/db.py).

Pure unit tests with fakes (no database): the gate's contract is
 - hot connections (returned within POOL_PREPING_IDLE_S) borrow with ZERO
   round trips (no ping executed — the #62 bench makes an unconditional
   per-borrow execute a B18 violation),
 - idle or never-seen connections pay exactly one validating SELECT 1,
 - a failed ping discards the connection and reconnects ONCE,
 - a second failure raises loudly, carrying both errors (no silent retry
   loop, no swallowed error — A1).
"""

import time

import psycopg2
import pytest

from backend.core import db as core_db


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, vars=None):
        self._conn.executed.append(query)
        if self._conn.fail_pings_remaining > 0:
            self._conn.fail_pings_remaining -= 1
            raise psycopg2.OperationalError("SSL connection has been closed unexpectedly")

    def fetchone(self):
        return (1,)


class FakeConn:
    def __init__(self, fail_pings=0):
        self.autocommit = False
        self.executed = []
        self.fail_pings_remaining = fail_pings
        self.closed_by_close = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed_by_close = True


class FakePool:
    def __init__(self):
        self.putconn_calls = []

    def putconn(self, conn, close=False):
        self.putconn_calls.append((conn, close))


def _mark_hot(conn):
    with core_db._last_returned_lock:
        core_db._last_returned[id(conn)] = time.monotonic()


def _mark_idle(conn):
    with core_db._last_returned_lock:
        core_db._last_returned[id(conn)] = (
            time.monotonic() - core_db.POOL_PREPING_IDLE_S - 1.0
        )


def test_hot_connection_skips_ping_entirely():
    """A connection returned moments ago borrows with zero round trips."""
    pool, conn = FakePool(), FakeConn()
    _mark_hot(conn)
    out = core_db._validate_if_idle(pool, conn)
    assert out is conn
    assert conn.executed == [], "hot-path borrow must not execute anything (B18)"
    assert pool.putconn_calls == []


def test_idle_connection_pings_once_and_passes():
    """An idle connection pays exactly one SELECT 1 and is handed out."""
    pool, conn = FakePool(), FakeConn()
    _mark_idle(conn)
    out = core_db._validate_if_idle(pool, conn)
    assert out is conn
    assert conn.executed == [b"SELECT 1"]
    assert conn.autocommit is False, "autocommit must be restored after the ping"
    assert pool.putconn_calls == []


def test_never_seen_connection_is_pinged():
    """No recorded return time = unknown idle age = validate (safe default)."""
    pool, conn = FakePool(), FakeConn()
    with core_db._last_returned_lock:
        core_db._last_returned.pop(id(conn), None)
    out = core_db._validate_if_idle(pool, conn)
    assert out is conn
    assert conn.executed == [b"SELECT 1"]


def test_stale_connection_discarded_and_replaced_once(monkeypatch):
    """First ping fails -> stale conn discarded with close=True, fresh conn
    borrowed, pinged, and returned."""
    pool = FakePool()
    stale = FakeConn(fail_pings=1)
    fresh = FakeConn()
    _mark_idle(stale)
    monkeypatch.setattr(core_db, "_getconn_with_timeout", lambda p, t: fresh)

    out = core_db._validate_if_idle(pool, stale)

    assert out is fresh
    assert (stale, True) in pool.putconn_calls, "stale conn must be discarded (close=True)"
    assert fresh.executed == [b"SELECT 1"], "the replacement must be validated too"


def test_double_ping_failure_raises_loudly(monkeypatch):
    """Both pings failing raises RuntimeError naming BOTH errors — never a
    silent retry loop, never a swallowed failure."""
    pool = FakePool()
    stale = FakeConn(fail_pings=1)
    also_dead = FakeConn(fail_pings=1)
    _mark_idle(stale)
    monkeypatch.setattr(core_db, "_getconn_with_timeout", lambda p, t: also_dead)

    with pytest.raises(RuntimeError) as exc:
        core_db._validate_if_idle(pool, stale)

    msg = str(exc.value)
    assert "Pre-ping failed twice" in msg
    assert "First (stale connection)" in msg
    assert "Second (fresh connection)" in msg
    assert (also_dead, True) in pool.putconn_calls, "the dead replacement must not leak"
