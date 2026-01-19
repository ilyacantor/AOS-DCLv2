"""
Live Telemetry Metrics Collector for DCL Industrial Mode.

Broadcasts real-time processing metrics to Redis for dashboard visualization.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Optional
import redis.asyncio as redis


REDIS_TELEMETRY_KEY = "dcl.telemetry"
BROADCAST_INTERVAL = 0.5


@dataclass
class TelemetryMetrics:
    """Live processing metrics for Industrial Dashboard."""
    total_processed: int = 0
    toxic_blocked: int = 0
    drift_detected: int = 0
    repaired_success: int = 0
    repair_failed: int = 0
    verified_count: int = 0
    verified_failed: int = 0
    
    _start_time: float = field(default_factory=time.time)
    _last_processed_count: int = 0
    _last_tps_time: float = field(default_factory=time.time)
    _current_tps: float = 0.0

    def calculate_tps(self) -> float:
        """Calculate current throughput (records per second)."""
        now = time.time()
        elapsed = now - self._last_tps_time
        if elapsed >= 1.0:
            processed_delta = self.total_processed - self._last_processed_count
            self._current_tps = processed_delta / elapsed
            self._last_processed_count = self.total_processed
            self._last_tps_time = now
        return self._current_tps

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self._start_time

    @property
    def quality_score(self) -> float:
        """Calculate quality score (verified / total repaired)."""
        if self.repaired_success == 0:
            return 100.0
        return (self.verified_count / self.repaired_success) * 100

    @property
    def repair_rate(self) -> float:
        """Calculate repair success rate."""
        total_repairs = self.repaired_success + self.repair_failed
        if total_repairs == 0:
            return 100.0
        return (self.repaired_success / total_repairs) * 100

    def to_dict(self) -> dict:
        return {
            "total_processed": self.total_processed,
            "toxic_blocked": self.toxic_blocked,
            "drift_detected": self.drift_detected,
            "repaired_success": self.repaired_success,
            "repair_failed": self.repair_failed,
            "verified_count": self.verified_count,
            "verified_failed": self.verified_failed,
            "tps": round(self.calculate_tps(), 1),
            "quality_score": round(self.quality_score, 1),
            "repair_rate": round(self.repair_rate, 1),
            "uptime_seconds": round(self.uptime_seconds, 1),
        }


class MetricsCollector:
    """
    Collects and broadcasts live telemetry metrics to Redis.
    
    Usage:
        collector = MetricsCollector(redis_url)
        await collector.start()
        
        # In processing loop:
        collector.record_processed()
        collector.record_toxic_blocked()
        collector.record_repaired()
        collector.record_verified()
        
        await collector.stop()
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.metrics = TelemetryMetrics()
        self._redis: Optional[redis.Redis] = None
        self._running = False
        self._broadcast_task: Optional[asyncio.Task] = None
    
    async def connect(self) -> None:
        """Establish Redis connection."""
        if self._redis is None:
            self._redis = redis.from_url(self.redis_url, decode_responses=True)
            await self._redis.ping()
    
    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
    
    async def start(self) -> None:
        """Start the metrics broadcast loop."""
        await self.connect()
        self._running = True
        self._broadcast_task = asyncio.create_task(self._broadcast_loop())
    
    async def stop(self) -> None:
        """Stop the metrics broadcast loop."""
        self._running = False
        if self._broadcast_task:
            self._broadcast_task.cancel()
            try:
                await self._broadcast_task
            except asyncio.CancelledError:
                pass
        await self.disconnect()
    
    async def _broadcast_loop(self) -> None:
        """Continuously broadcast metrics to Redis."""
        while self._running:
            try:
                await self._broadcast()
                await asyncio.sleep(BROADCAST_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1.0)
    
    async def _broadcast(self) -> None:
        """Push current metrics to Redis."""
        if self._redis is None:
            return
        
        telemetry = {
            "ts": int(time.time() * 1000),
            "metrics": self.metrics.to_dict()
        }
        
        await self._redis.set(REDIS_TELEMETRY_KEY, json.dumps(telemetry))
    
    def record_processed(self) -> None:
        """Record a successfully processed record."""
        self.metrics.total_processed += 1
    
    def record_toxic_blocked(self) -> None:
        """Record a toxic/malformed record that was blocked."""
        self.metrics.toxic_blocked += 1
    
    def record_drift_detected(self) -> None:
        """Record drift detection."""
        self.metrics.drift_detected += 1
    
    def record_repaired(self, success: bool = True) -> None:
        """Record a repair attempt."""
        if success:
            self.metrics.repaired_success += 1
        else:
            self.metrics.repair_failed += 1
    
    def record_verified(self, success: bool = True) -> None:
        """Record a verification attempt."""
        if success:
            self.metrics.verified_count += 1
        else:
            self.metrics.verified_failed += 1
    
    def get_snapshot(self) -> dict:
        """Get current metrics snapshot."""
        return self.metrics.to_dict()
