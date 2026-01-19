"""
DCL Ingest Consumer - Redis Stream Consumer for testing.

This module reads from the Redis stream `dcl.ingest.raw` and logs
processed records. It's a stub for testing the ingest pipeline
before semantic mapping is implemented.
"""

import asyncio
import json
import logging
import os
from typing import Optional

import redis.asyncio as redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("IngestConsumer")

REDIS_STREAM_KEY = "dcl.ingest.raw"
CONSUMER_GROUP = "dcl_engine"
CONSUMER_NAME = "consumer_1"


class IngestConsumer:
    """
    Consumer that reads from the Redis ingest stream.
    
    Uses Redis Consumer Groups for reliable message delivery
    and acknowledgment.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        batch_size: int = 10,
        block_ms: int = 5000,
    ):
        self.redis_url = redis_url
        self.batch_size = batch_size
        self.block_ms = block_ms
        self._redis: Optional[redis.Redis] = None
        self._running = False
        self.processed_count = 0

    async def connect(self) -> None:
        """Establish Redis connection and create consumer group."""
        self._redis = redis.from_url(self.redis_url, decode_responses=True)
        await self._redis.ping()
        logger.info(f"Connected to Redis at {self.redis_url}")

        try:
            await self._redis.xgroup_create(
                REDIS_STREAM_KEY,
                CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )
            logger.info(f"Created consumer group: {CONSUMER_GROUP}")
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group {CONSUMER_GROUP} already exists")
            else:
                raise

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            logger.info("Disconnected from Redis")

    async def process_record(self, message_id: str, data: dict) -> None:
        """
        Process a single record from the stream.
        
        Currently just logs - will be extended for semantic mapping.
        """
        try:
            envelope = json.loads(data.get("data", "{}"))
            trace_id = envelope.get("meta", {}).get("trace_id", "unknown")
            source = envelope.get("meta", {}).get("source", "unknown")
            payload_keys = list(envelope.get("payload", {}).keys())

            self.processed_count += 1
            logger.info(
                f"Processed Record [{trace_id[:8]}...] "
                f"from {source} | "
                f"Fields: {payload_keys[:5]}{'...' if len(payload_keys) > 5 else ''}"
            )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse envelope: {e}")

    async def run(self) -> None:
        """Main consumer loop."""
        logger.info("=" * 60)
        logger.info("DCL Ingest Consumer Starting")
        logger.info(f"Stream: {REDIS_STREAM_KEY}")
        logger.info(f"Consumer Group: {CONSUMER_GROUP}")
        logger.info("=" * 60)

        await self.connect()
        self._running = True

        try:
            while self._running:
                messages = await self._redis.xreadgroup(
                    CONSUMER_GROUP,
                    CONSUMER_NAME,
                    {REDIS_STREAM_KEY: ">"},
                    count=self.batch_size,
                    block=self.block_ms,
                )

                if not messages:
                    continue

                for stream_name, stream_messages in messages:
                    for message_id, data in stream_messages:
                        await self.process_record(message_id, data)

                        await self._redis.xack(
                            REDIS_STREAM_KEY,
                            CONSUMER_GROUP,
                            message_id,
                        )

        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        finally:
            self._running = False
            await self.disconnect()
            logger.info(f"Total records processed: {self.processed_count}")

    def stop(self) -> None:
        """Signal the consumer to stop."""
        self._running = False


async def get_stream_info(redis_url: str = "redis://localhost:6379") -> dict:
    """Get information about the ingest stream."""
    r = redis.from_url(redis_url, decode_responses=True)
    try:
        info = await r.xinfo_stream(REDIS_STREAM_KEY)
        return {
            "length": info.get("length", 0),
            "first_entry": info.get("first-entry"),
            "last_entry": info.get("last-entry"),
        }
    except redis.ResponseError:
        return {"length": 0, "error": "Stream does not exist"}
    finally:
        await r.aclose()


async def main():
    """Entry point for running the consumer."""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")

    stream_info = await get_stream_info(redis_url)
    logger.info(f"Stream info: {stream_info}")

    consumer = IngestConsumer(redis_url=redis_url)
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
