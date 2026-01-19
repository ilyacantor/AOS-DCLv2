"""
Ingest Sidecar Agent for DCL Engine.

This module implements a fault-tolerant ingestion pipeline that:
1. Connects to Farm's streaming endpoint
2. Validates incoming JSON data (drops malformed records)
3. Wraps valid records in AOS_Envelope structure
4. Buffers envelopes to Redis Stream for downstream processing

The Sidecar implements "The Airlock" pattern - isolating the core DCL
from toxic enterprise data streams.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional, AsyncIterator

import httpx
import redis.asyncio as redis
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("IngestSidecar")

REDIS_STREAM_KEY = "dcl.ingest.raw"


@dataclass
class IngestMetrics:
    """Tracks ingestion statistics."""
    records_received: int = 0
    records_valid: int = 0
    records_dropped: int = 0
    circuit_breaker_trips: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def drop_rate(self) -> float:
        if self.records_received == 0:
            return 0.0
        return self.records_dropped / self.records_received

    def to_dict(self) -> dict:
        return {
            "records_received": self.records_received,
            "records_valid": self.records_valid,
            "records_dropped": self.records_dropped,
            "circuit_breaker_trips": self.circuit_breaker_trips,
            "uptime_seconds": round(self.uptime_seconds, 2),
            "drop_rate": round(self.drop_rate, 4),
        }


@dataclass
class AOSEnvelope:
    """
    Standard envelope for AOS platform data.
    Wraps raw payloads with metadata for tracing and auditing.
    """
    meta: dict
    payload: dict

    @classmethod
    def create(cls, payload: dict, source: str) -> "AOSEnvelope":
        return cls(
            meta={
                "ingest_ts": int(time.time() * 1000),
                "source": source,
                "trace_id": str(uuid.uuid4()),
            },
            payload=payload,
        )

    def to_dict(self) -> dict:
        return {
            "meta": self.meta,
            "payload": self.payload,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class CircuitBreakerOpen(Exception):
    """Raised when circuit breaker is open."""
    pass


class IngestSidecar:
    """
    Fault-tolerant ingestion sidecar for DCL Engine.

    Implements:
    - Circuit breaker pattern with exponential backoff
    - JSON validation (drops malformed records)
    - AOS_Envelope wrapping
    - Redis Stream buffering
    """

    def __init__(
        self,
        source_url: str,
        redis_url: str = "redis://localhost:6379",
        source_name: str = "mulesoft_mock",
        max_retries: int = 5,
        cooldown_seconds: int = 60,
    ):
        self.source_url = source_url
        self.redis_url = redis_url
        self.source_name = source_name
        self.max_retries = max_retries
        self.cooldown_seconds = cooldown_seconds

        self.metrics = IngestMetrics()
        self._redis: Optional[redis.Redis] = None
        self._running = False
        self._circuit_open = False
        self._circuit_open_until: float = 0

    async def connect_redis(self) -> None:
        """Establish Redis connection."""
        if self._redis is None:
            self._redis = redis.from_url(self.redis_url, decode_responses=True)
            await self._redis.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")

    async def disconnect_redis(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            logger.info("Disconnected from Redis")

    def _check_circuit_breaker(self) -> None:
        """Check if circuit breaker is open."""
        if self._circuit_open:
            if time.time() < self._circuit_open_until:
                remaining = int(self._circuit_open_until - time.time())
                raise CircuitBreakerOpen(
                    f"Circuit breaker open. Retry in {remaining}s"
                )
            else:
                logger.info("Circuit breaker reset. Resuming connection.")
                self._circuit_open = False

    def _trip_circuit_breaker(self) -> None:
        """Trip the circuit breaker after max failures."""
        self._circuit_open = True
        self._circuit_open_until = time.time() + self.cooldown_seconds
        self.metrics.circuit_breaker_trips += 1
        logger.warning(
            f"Circuit Breaker Tripped! Pausing for {self.cooldown_seconds}s. "
            f"Total trips: {self.metrics.circuit_breaker_trips}"
        )

    async def _connect_to_stream_with_retry(self, client: httpx.AsyncClient):
        """
        Connect to the source stream with tenacity retry logic.
        Uses exponential backoff (5 retries), trips circuit breaker on exhaustion.
        """
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
            before_sleep=lambda retry_state: logger.warning(
                f"Connection failed, retrying in {retry_state.next_action.sleep:.1f}s... "
                f"(attempt {retry_state.attempt_number}/{self.max_retries})"
            ),
        )
        async def _do_connect():
            response = await client.send(
                client.build_request("GET", self.source_url),
                stream=True,
            )
            response.raise_for_status()
            return response
        
        return await _do_connect()

    async def _stream_with_retry(self) -> AsyncIterator[str]:
        """
        Stream data from source with tenacity-based retry logic.
        Uses exponential backoff (5 retries) and 60s circuit breaker cooldown.
        """
        while self._running:
            try:
                self._check_circuit_breaker()

                async with httpx.AsyncClient(timeout=30.0) as client:
                    try:
                        response = await self._connect_to_stream_with_retry(client)
                        logger.info(f"Connected to stream: {self.source_url}")

                        async for line in response.aiter_lines():
                            if not self._running:
                                break
                            if line.strip():
                                yield line

                        await response.aclose()

                    except RetryError as e:
                        self._trip_circuit_breaker()
                        logger.warning(
                            f"Max retries exhausted after {self.max_retries} attempts. "
                            f"Circuit breaker tripped for {self.cooldown_seconds}s."
                        )

            except CircuitBreakerOpen as e:
                logger.warning(str(e))
                await asyncio.sleep(5)

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error: {e.response.status_code}")
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                await asyncio.sleep(5)

    def _validate_and_parse(self, line: str) -> Optional[dict]:
        """
        Validate and parse a JSON line.
        Returns None for malformed records.
        """
        self.metrics.records_received += 1

        try:
            data = json.loads(line)
            if not isinstance(data, dict):
                raise ValueError("Record must be a JSON object")
            self.metrics.records_valid += 1
            return data

        except (json.JSONDecodeError, ValueError) as e:
            self.metrics.records_dropped += 1
            logger.debug(f"Toxic Record Dropped: {str(e)[:100]}")
            return None

    async def _buffer_to_redis(self, envelope: AOSEnvelope) -> None:
        """Push envelope to Redis Stream."""
        if self._redis is None:
            raise RuntimeError("Redis not connected")

        await self._redis.xadd(
            REDIS_STREAM_KEY,
            {"data": envelope.to_json()},
            maxlen=10000,
        )

    async def run(self) -> None:
        """Main ingestion loop."""
        logger.info(f"Starting IngestSidecar for {self.source_url}")
        logger.info(f"Buffering to Redis stream: {REDIS_STREAM_KEY}")

        await self.connect_redis()
        self._running = True

        try:
            async for line in self._stream_with_retry():
                record = self._validate_and_parse(line)
                if record is None:
                    continue

                envelope = AOSEnvelope.create(record, self.source_name)
                await self._buffer_to_redis(envelope)

                if self.metrics.records_valid % 100 == 0:
                    logger.info(
                        f"Progress: {self.metrics.records_valid} valid, "
                        f"{self.metrics.records_dropped} dropped"
                    )

        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        finally:
            self._running = False
            await self.disconnect_redis()
            logger.info(f"Final metrics: {self.metrics.to_dict()}")

    def stop(self) -> None:
        """Signal the sidecar to stop."""
        self._running = False


async def main():
    """Entry point for running the Ingest Sidecar."""
    source_url = os.environ.get(
        "SOURCE_URL",
        "https://autonomos.farm/api/stream/synthetic/mulesoft"
    )
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    source_name = os.environ.get("SOURCE_NAME", "mulesoft_mock")

    logger.info("=" * 60)
    logger.info("DCL Ingest Sidecar Starting")
    logger.info(f"Source URL: {source_url}")
    logger.info(f"Redis URL: {redis_url}")
    logger.info(f"Source Name: {source_name}")
    logger.info("=" * 60)

    sidecar = IngestSidecar(
        source_url=source_url,
        redis_url=redis_url,
        source_name=source_name,
    )

    await sidecar.run()


if __name__ == "__main__":
    asyncio.run(main())
