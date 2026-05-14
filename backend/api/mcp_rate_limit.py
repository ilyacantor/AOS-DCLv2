"""
Per-tenant rate limiting for external MCP calls (Plan B WP5, §11.4).

In-memory token bucket. 60 requests-per-minute default per tenant.
v1 is in-process only — for multi-process deployments, persist with Redis
(deferred). Filed in dcl_deferred_work.md.
"""

from __future__ import annotations

import os
import threading
import time
from collections import deque
from dataclasses import dataclass


def _default_rpm() -> int:
    raw = os.environ.get("DCL_MCP_RATE_LIMIT_RPM", "60")
    try:
        return max(1, int(raw))
    except ValueError:
        return 60


@dataclass
class RateLimitDecision:
    allowed: bool
    remaining: int
    retry_after_seconds: float


class TenantRateLimiter:
    """Sliding-window counter per tenant. Thread-safe."""

    def __init__(self, default_rpm: int | None = None) -> None:
        self._default_rpm = default_rpm if default_rpm is not None else _default_rpm()
        self._buckets: dict[str, deque[float]] = {}
        self._overrides: dict[str, int] = {}
        self._lock = threading.Lock()

    def set_tenant_rpm(self, tenant_id: str, rpm: int) -> None:
        """Override the per-minute ceiling for one tenant."""
        with self._lock:
            self._overrides[tenant_id] = max(1, int(rpm))

    def rpm_for(self, tenant_id: str) -> int:
        return self._overrides.get(tenant_id, self._default_rpm)

    def check(self, tenant_id: str) -> RateLimitDecision:
        """Check-and-consume one slot for tenant_id.

        Returns allowed=False when the 60s window is full. Audit
        callers must record outcome='rate_limited'.
        """
        now = time.monotonic()
        rpm = self.rpm_for(tenant_id)
        window = 60.0
        with self._lock:
            bucket = self._buckets.setdefault(tenant_id, deque())
            # Drop entries older than the window
            while bucket and (now - bucket[0]) >= window:
                bucket.popleft()
            if len(bucket) >= rpm:
                retry_after = window - (now - bucket[0])
                return RateLimitDecision(
                    allowed=False,
                    remaining=0,
                    retry_after_seconds=max(0.0, retry_after),
                )
            bucket.append(now)
            return RateLimitDecision(
                allowed=True,
                remaining=rpm - len(bucket),
                retry_after_seconds=0.0,
            )

    def reset(self, tenant_id: str | None = None) -> None:
        """Test helper. Reset one tenant or all."""
        with self._lock:
            if tenant_id is None:
                self._buckets.clear()
            else:
                self._buckets.pop(tenant_id, None)


# Module-level singleton — same instance shared by HTTP path and MCP server
_GLOBAL = TenantRateLimiter()


def global_limiter() -> TenantRateLimiter:
    return _GLOBAL
