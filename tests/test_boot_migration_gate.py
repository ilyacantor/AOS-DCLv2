"""Boot-time migration gate (ledger #82 fail-loud ruling + #85 posture gate).

Operator-visible outcome: a dev boot whose migration pass fails REFUSES to
serve (the process aborts with the runner's error named); a boot against the
pre-gate prod store SKIPS the pass with an ERROR log naming the #70 gate and
keeps serving reads; a fresh store provisions. The pass can never mutate a
pre-gate store as a boot side effect again (how prod accidentally gained
mig016/017 — ledger #85).
"""

import subprocess
import sys
import uuid
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

from backend.api import main as dcl_main


class _FakeCompleted:
    def __init__(self, returncode: int, stderr: str = "", stdout: str = ""):
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout


def test_dev_store_posture_is_migrated():
    """The live aos-dev store carries the mig023 sentinel — posture 'migrated'.

    Grounds the sentinel choice against the real store, not a fixture."""
    assert dcl_main._store_migration_posture() == "migrated"


def test_migrated_store_green_pass_boots(monkeypatch):
    monkeypatch.setattr(dcl_main, "_store_migration_posture", lambda: "migrated")
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: _FakeCompleted(0)
    )
    assert dcl_main._boot_migration_check() is True


def test_migrated_store_failed_pass_aborts_boot(monkeypatch):
    monkeypatch.setattr(dcl_main, "_store_migration_posture", lambda: "migrated")
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **k: _FakeCompleted(1, stderr="MIGRATION FAILED: boom"),
    )
    with pytest.raises(RuntimeError) as exc:
        dcl_main._boot_migration_check()
    msg = str(exc.value)
    assert "aborting boot" in msg
    assert "MIGRATION FAILED: boom" in msg
    assert "#82" in msg


def test_pre_gate_store_skips_pass_and_serves(monkeypatch):
    monkeypatch.setattr(dcl_main, "_store_migration_posture", lambda: "pre_gate")

    def _must_not_run(*a, **k):
        raise AssertionError(
            "the migration runner must NEVER execute against a pre-gate "
            "store — that is exactly how prod gained mig016/017 (ledger #85)"
        )

    monkeypatch.setattr(subprocess, "run", _must_not_run)
    assert dcl_main._boot_migration_check() is False


def test_fresh_store_provisions(monkeypatch):
    monkeypatch.setattr(dcl_main, "_store_migration_posture", lambda: "fresh")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _FakeCompleted(0))
    assert dcl_main._boot_migration_check() is True


def test_unreachable_store_aborts_boot(monkeypatch):
    def _probe_fails():
        raise ConnectionError(
            f"could not connect to store (probe {uuid.uuid4().hex[:6]})"
        )

    monkeypatch.setattr(dcl_main, "_store_migration_posture", _probe_fails)
    with pytest.raises(ConnectionError):
        dcl_main._boot_migration_check()
