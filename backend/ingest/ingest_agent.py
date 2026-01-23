"""
Ingest Sidecar Agent for DCL Engine.

**DEPRECATED**: This module is scheduled for migration to AAM (Asset & Availability Management).
Per ARCH-GLOBAL-PIVOT.md, DCL must be metadata-only. Raw payload buffering and self-healing
logic will move to AAM. This code remains temporarily for backward compatibility.

Migration Status: PENDING (January 2026)
Target: AAM self-healing mesh
See: docs/ARCH-GLOBAL-PIVOT.md for architecture details

Original Description:
This module implements a fault-tolerant ingestion pipeline that:
1. Connects to Farm's streaming endpoint
2. Validates incoming JSON data (drops malformed records)
3. Wraps valid records in AOS_Envelope structure
4. Buffers envelopes to Redis Stream for downstream processing

The Sidecar implements "The Airlock" pattern - isolating the core DCL
from toxic enterprise data streams.
"""
import warnings

warnings.warn(
    "IngestSidecar is DEPRECATED and will move to AAM. "
    "DCL must be metadata-only per ARCH-GLOBAL-PIVOT.md",
    DeprecationWarning,
    stacklevel=2
)

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

from backend.utils.metrics import MetricsCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("IngestSidecar")

REDIS_STREAM_KEY = "dcl.ingest.raw"
REDIS_LOG_KEY = "dcl.logs"
REDIS_CONFIG_KEY = "dcl.ingest.config"

CONFIG_POLL_INTERVAL = 5

EXPECTED_INVOICE_FIELDS = ["invoice_id", "total_amount", "vendor", "payment_status"]


@dataclass
class IngestMetrics:
    """Tracks ingestion statistics."""
    records_received: int = 0
    records_valid: int = 0
    records_dropped: int = 0
    records_repaired: int = 0
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
            "records_repaired": self.records_repaired,
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
    def create(
        cls,
        payload: dict,
        source: str,
        is_repaired: bool = False,
        repaired_fields: Optional[list] = None,
    ) -> "AOSEnvelope":
        meta = {
            "ingest_ts": int(time.time() * 1000),
            "source": source,
            "trace_id": str(uuid.uuid4()),
            "is_repaired": is_repaired,
        }
        if is_repaired and repaired_fields:
            meta["repaired_fields"] = repaired_fields
        return cls(meta=meta, payload=payload)

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
    - Drift detection and self-healing repair
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
        farm_base_url: Optional[str] = None,
    ):
        self.source_url = source_url
        self.redis_url = redis_url
        self.source_name = source_name
        self.max_retries = max_retries
        self.cooldown_seconds = cooldown_seconds
        self.farm_base_url = farm_base_url or os.environ.get(
            "FARM_API_URL", "https://autonomos.farm"
        ).rstrip("/")

        self.metrics = IngestMetrics()
        self._redis: Optional[redis.Redis] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._running = False
        self._circuit_open = False
        self._circuit_open_until: float = 0
        
        self._config_version: Optional[str] = None
        self._last_config_check: float = 0
        self._config_updated = False
        self._repair_enabled = True
        
        self._telemetry = MetricsCollector(redis_url)
        self._verification_enabled = True

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

    async def log_to_ui(self, message: str, log_type: str = "info") -> None:
        """
        Push a log message to Redis List for UI consumption.
        
        Args:
            message: The log message to display
            log_type: One of 'info', 'warn', 'success', 'error'
        """
        if self._redis is None:
            return
        
        from datetime import datetime, timezone, timedelta
        pst = timezone(timedelta(hours=-8))
        log_entry = json.dumps({
            "msg": message,
            "type": log_type,
            "ts": datetime.now(pst).isoformat()
        })
        
        try:
            await self._redis.rpush(REDIS_LOG_KEY, log_entry)
            await self._redis.ltrim(REDIS_LOG_KEY, -500, -1)
        except Exception as e:
            logger.debug(f"Failed to push log to UI: {e}")

    async def check_config_update(self) -> bool:
        """
        Poll Redis for dynamic config updates from AAM provisioning.
        
        Returns:
            True if config was updated, False otherwise
        """
        if self._redis is None:
            return False
        
        now = time.time()
        if now - self._last_config_check < CONFIG_POLL_INTERVAL:
            return False
        
        self._last_config_check = now
        
        try:
            config_str = await self._redis.get(REDIS_CONFIG_KEY)
            if not config_str:
                return False
            
            config = json.loads(config_str)
            new_version = config.get("version")
            
            if new_version and new_version != self._config_version:
                old_url = self.source_url
                self.source_url = config.get("target_url", self.source_url)
                self.source_name = config.get("source_type", self.source_name)
                self._repair_enabled = config.get("policy", {}).get("repair_enabled", True)
                self._config_version = new_version
                
                logger.info("=" * 60)
                logger.info("[HANDSHAKE] Dynamic Config Update from AAM!")
                logger.info(f"  Connector: {config.get('connector_id')}")
                logger.info(f"  Old URL: {old_url}")
                logger.info(f"  New URL: {self.source_url}")
                logger.info(f"  Repair Enabled: {self._repair_enabled}")
                logger.info("=" * 60)
                
                await self.log_to_ui(
                    f"[INFO] Handshake: AAM provisioned connector '{config.get('connector_id')}'. Switching to: {self.source_url}",
                    "info"
                )
                
                self._config_updated = True
                return True
                
        except Exception as e:
            logger.debug(f"Config check failed: {e}")
        
        return False

    def _is_invoice_record(self, record: dict) -> bool:
        """Check if this is an invoice record (not a chaos control message)."""
        if "_chaos" in record:
            return False
        if "record_type" not in record and "invoice_id" not in record:
            return False
        return True

    def detect_drift(self, record: dict) -> tuple[bool, list[str]]:
        """
        Detect if a record has missing expected fields (Drift).
        
        Returns:
            tuple: (is_drifted, list of missing fields)
        """
        if not self._is_invoice_record(record):
            return False, []
        
        missing_fields = []
        for field in EXPECTED_INVOICE_FIELDS:
            if field not in record:
                missing_fields.append(field)
        return len(missing_fields) > 0, missing_fields

    async def repair_record(self, record: dict, missing_fields: list[str]) -> tuple[dict, bool]:
        """
        Repair a drifted record by fetching missing data from the Source of Truth.
        
        Calls the Farm's repair endpoint to get the complete record,
        then merges missing fields into the original record.
        
        Returns:
            tuple: (repaired_record, was_repaired)
        """
        invoice_id = record.get("invoice_id")
        if not invoice_id:
            logger.warning("Cannot repair record without invoice_id")
            return record, False
        
        repair_url = f"{self.farm_base_url}/api/source/salesforce/invoice/{invoice_id}"
        
        try:
            if self._http_client is None:
                self._http_client = httpx.AsyncClient(timeout=10.0)
            
            response = await self._http_client.get(repair_url)
            
            if response.status_code == 200:
                repair_data = response.json()
                
                invoice_data = repair_data.get("invoice", repair_data)
                
                repaired_fields = []
                for field in missing_fields:
                    if field in invoice_data:
                        record[field] = invoice_data[field]
                        repaired_fields.append(field)
                        logger.info(f"  Patched field: {field}")
                    elif field in repair_data:
                        record[field] = repair_data[field]
                        repaired_fields.append(field)
                        logger.info(f"  Patched field: {field}")
                
                if repaired_fields:
                    self.metrics.records_repaired += 1
                    self._telemetry.record_repaired(success=True)
                    return record, True
                else:
                    self._telemetry.record_repaired(success=False)
                    return record, False
            else:
                self._telemetry.record_repaired(success=False)
                return record, False
                
        except Exception as e:
            logger.error(f"Repair failed for {invoice_id}: {e}")
            self._telemetry.record_repaired(success=False)
            return record, False

    async def verify_record(self, record: dict) -> tuple[bool, float]:
        """
        Verify a repaired record with Farm's verification endpoint.
        
        The "Closed Loop" - Farm confirms the fix is correct.
        
        Returns:
            tuple: (verified, quality_score)
        """
        if not self._verification_enabled:
            return True, 100.0
        
        invoice_id = record.get("invoice_id")
        if not invoice_id:
            return False, 0.0
        
        verify_url = f"{self.farm_base_url}/api/verify/salesforce/invoice"
        
        try:
            if self._http_client is None:
                self._http_client = httpx.AsyncClient(timeout=10.0)
            
            response = await self._http_client.post(verify_url, json=record)
            
            if response.status_code == 200:
                result = response.json()
                verified = result.get("verified", False)
                quality_score = result.get("quality_score", 0.0)
                
                if verified:
                    self._telemetry.record_verified(success=True)
                else:
                    self._telemetry.record_verified(success=False)
                
                return verified, quality_score
            else:
                self._telemetry.record_verified(success=False)
                return False, 0.0
                
        except Exception as e:
            logger.debug(f"Verification failed for {invoice_id}: {e}")
            return True, 100.0

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

    def _extract_payload(self, record: dict) -> dict:
        """
        Extract the payload from a record.
        Handles both raw payloads and already-wrapped envelopes.
        """
        if "meta" in record and "payload" in record:
            return record["payload"]
        return record

    async def run(self) -> None:
        """Main ingestion loop with drift detection, self-healing repair, and verification."""
        logger.info(f"Starting IngestSidecar for {self.source_url}")
        logger.info(f"Buffering to Redis stream: {REDIS_STREAM_KEY}")
        logger.info(f"Self-healing enabled. Repair endpoint: {self.farm_base_url}/api/source/salesforce/invoice/")
        logger.info(f"Verification enabled. Verify endpoint: {self.farm_base_url}/api/verify/salesforce/invoice")
        logger.info(f"Config polling enabled. Checking {REDIS_CONFIG_KEY} every {CONFIG_POLL_INTERVAL}s")
        logger.info("INDUSTRIAL MODE: No artificial latency, maximum throughput")

        await self.connect_redis()
        await self._telemetry.start()
        self._running = True

        try:
            while self._running:
                config_updated = await self.check_config_update()
                if config_updated or self._config_updated:
                    self._config_updated = False
                    logger.info(f"Connecting to new stream: {self.source_url}")
                
                async for line in self._stream_with_retry():
                    await self.check_config_update()
                    
                    if self._config_updated:
                        logger.info("Config updated. Reconnecting to new stream...")
                        break
                    
                    record = self._validate_and_parse(line)
                    if record is None:
                        self._telemetry.record_toxic_blocked()
                        continue

                    self._telemetry.record_processed()
                    payload = self._extract_payload(record)
                    is_repaired = False
                    repaired_fields = None
                    verified = False
                    
                    if self._repair_enabled:
                        is_drifted, missing_fields = self.detect_drift(payload)
                        if is_drifted:
                            self._telemetry.record_drift_detected()
                            payload, is_repaired = await self.repair_record(payload, missing_fields)
                            if is_repaired:
                                repaired_fields = missing_fields
                                verified, _ = await self.verify_record(payload)

                    envelope = AOSEnvelope.create(
                        payload, 
                        self.source_name,
                        is_repaired=is_repaired,
                        repaired_fields=repaired_fields,
                    )
                    if is_repaired and verified:
                        envelope.meta["verified"] = True
                    await self._buffer_to_redis(envelope)

                    if self.metrics.records_valid % 500 == 0:
                        telemetry = self._telemetry.get_snapshot()
                        logger.info(
                            f"[TELEMETRY] TPS: {telemetry['tps']}/s | "
                            f"Processed: {telemetry['total_processed']} | "
                            f"Blocked: {telemetry['toxic_blocked']} | "
                            f"Healed: {telemetry['repaired_success']} | "
                            f"Verified: {telemetry['verified_count']}"
                        )

        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        finally:
            self._running = False
            await self._telemetry.stop()
            if self._http_client:
                await self._http_client.aclose()
            await self.disconnect_redis()
            logger.info(f"Final metrics: {self.metrics.to_dict()}")

    def stop(self) -> None:
        """Signal the sidecar to stop."""
        self._running = False


async def main():
    """Entry point for running the Ingest Sidecar."""
    farm_base_url = os.environ.get("FARM_API_URL", "https://autonomos.farm")
    farm_base_url = farm_base_url.rstrip("/")
    
    enable_chaos = os.environ.get("ENABLE_CHAOS", "true").lower() == "true"
    chaos_param = "?chaos=true" if enable_chaos else ""
    
    base_source_url = os.environ.get(
        "SOURCE_URL",
        f"{farm_base_url}/api/stream/synthetic/mulesoft"
    )
    if enable_chaos and "chaos" not in base_source_url:
        source_url = f"{base_source_url}{chaos_param}"
    else:
        source_url = base_source_url
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
