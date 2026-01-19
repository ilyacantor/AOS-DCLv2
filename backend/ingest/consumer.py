"""
DCL Ingest Consumer - Redis Stream Consumer with Semantic Mapping.

This module reads from the Redis stream `dcl.ingest.raw`, infers schema
from JSON payloads, and runs the HeuristicMapper to create semantic mappings.
The mappings are persisted to PostgreSQL for the DCLEngine to visualize.
"""

import asyncio
import json
import logging
import os
from typing import Optional, Dict, Any, List, Set

import redis.asyncio as redis

from backend.domain.models import (
    SourceSystem, TableSchema, FieldSchema,
    DiscoveryStatus, ResolutionType
)
from backend.semantic_mapper.heuristic_mapper import HeuristicMapper
from backend.semantic_mapper.persist_mappings import MappingPersistence

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("IngestConsumer")

REDIS_STREAM_KEY = "dcl.ingest.raw"
CONSUMER_GROUP = "dcl_engine"
CONSUMER_NAME = "consumer_1"


def infer_field_type(value: Any) -> str:
    """Infer the field type from a sample value."""
    if value is None:
        return "string"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    value_str = str(value)
    if value_str.startswith("20") and ("T" in value_str or "-" in value_str):
        return "datetime"
    return "string"


def flatten_json(data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Flatten nested JSON into dot-notation keys."""
    result = {}
    for key, value in data.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict) and not key.startswith("_"):
            result.update(flatten_json(value, full_key))
        else:
            result[full_key] = value
    return result


def infer_schema_from_payload(payload: Dict[str, Any]) -> List[FieldSchema]:
    """Infer FieldSchema list from a JSON payload."""
    flattened = flatten_json(payload)
    fields = []
    
    for field_name, value in flattened.items():
        if field_name.startswith("_"):
            continue
            
        field_type = infer_field_type(value)
        
        semantic_hint = None
        field_lower = field_name.lower()
        if "amount" in field_lower or "total" in field_lower or "price" in field_lower:
            semantic_hint = "amount"
        elif "_id" in field_lower or field_lower.endswith("id"):
            semantic_hint = "id"
        elif "date" in field_lower or "time" in field_lower:
            semantic_hint = "datetime"
        elif "status" in field_lower:
            semantic_hint = "status"
        
        fields.append(FieldSchema(
            name=field_name,
            type=field_type,
            semantic_hint=semantic_hint,
            nullable=True,
            sample_values=[value] if not isinstance(value, (dict, list)) else None
        ))
    
    return fields


def load_ontology_concepts() -> List[Dict[str, Any]]:
    """Load ontology concepts from database or config file."""
    try:
        persistence = MappingPersistence()
        concepts = persistence.get_ontology_concepts()
        if concepts:
            return concepts
    except Exception as e:
        logger.warning(f"Could not load concepts from DB: {e}")
    
    import yaml
    config_path = "config/ontology_concepts.yaml"
    if os.path.exists(config_path):
        with open(config_path) as f:
            config = yaml.safe_load(f)
            return config.get("concepts", [])
    
    return []


class IngestConsumer:
    """
    Consumer that reads from the Redis ingest stream,
    infers schema, and creates semantic mappings.
    
    Uses Redis Consumer Groups for reliable message delivery.
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
        
        self._seen_sources: Set[str] = set()
        self._source_record_counts: Dict[str, int] = {}
        self._source_schemas: Dict[str, SourceSystem] = {}
        
        self._ontology_concepts: List[Dict[str, Any]] = []
        self._persistence: Optional[MappingPersistence] = None
        self._mapper: Optional[HeuristicMapper] = None

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
        
        self._ontology_concepts = load_ontology_concepts()
        logger.info(f"Loaded {len(self._ontology_concepts)} ontology concepts")
        
        if self._ontology_concepts:
            self._mapper = HeuristicMapper(self._ontology_concepts)
            logger.info("Initialized HeuristicMapper")
        
        try:
            self._persistence = MappingPersistence()
            logger.info("Connected to PostgreSQL for mapping persistence")
        except Exception as e:
            logger.warning(f"Could not connect to database: {e}")
            self._persistence = None

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            logger.info("Disconnected from Redis")

    def _register_source(self, source_id: str, payload: Dict[str, Any]) -> None:
        """Register a new source by inferring schema and creating mappings."""
        logger.info(f"Registering new source: {source_id}")
        
        fields = infer_schema_from_payload(payload)
        logger.info(f"  Inferred {len(fields)} fields from payload")
        
        record_type = payload.get("record_type", "stream_record")
        source_system_name = payload.get("source_system", source_id)
        
        table = TableSchema(
            id=f"{source_id}_{record_type}",
            system_id=source_id,
            name=record_type,
            fields=fields,
            record_count=1
        )
        
        source = SourceSystem(
            id=source_id,
            name=source_system_name.replace("_", " ").title(),
            type="stream",
            tags=["real-time", "farm-synced"],
            tables=[table],
            discovery_status=DiscoveryStatus.CUSTOM,
            resolution_type=ResolutionType.PATTERN,
            trust_score=75,
            data_quality_score=80,
            vendor="MuleSoft",
            category="Integration",
            entities=[record_type]
        )
        
        self._source_schemas[source_id] = source
        
        if self._mapper and self._persistence:
            try:
                mappings = self._mapper.create_mappings([source])
                logger.info(f"  Created {len(mappings)} semantic mappings")
                
                for mapping in mappings[:5]:
                    logger.info(f"    {mapping.source_field} -> {mapping.ontology_concept} ({mapping.confidence:.2f})")
                if len(mappings) > 5:
                    logger.info(f"    ... and {len(mappings) - 5} more")
                
                saved = self._persistence.save_mappings(mappings, clear_existing=True)
                logger.info(f"  Persisted {saved} mappings to database")
                
                self._save_source_to_db(source)
                
            except Exception as e:
                logger.error(f"  Failed to create/save mappings: {e}")
        else:
            logger.warning("  Mapper or persistence not available - mappings not created")
        
        self._seen_sources.add(source_id)
        self._source_record_counts[source_id] = 0
    
    def _save_source_to_db(self, source: SourceSystem) -> None:
        """Save the source system to the database for DCLEngine visibility."""
        if not self._persistence:
            return
            
        try:
            import psycopg2
            conn = psycopg2.connect(self._persistence.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO source_systems (id, name, type, vendor, category, trust_score, discovery_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    type = EXCLUDED.type,
                    vendor = EXCLUDED.vendor,
                    category = EXCLUDED.category,
                    trust_score = EXCLUDED.trust_score,
                    discovery_status = EXCLUDED.discovery_status,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                source.id,
                source.name,
                source.type,
                source.vendor,
                source.category,
                source.trust_score,
                source.discovery_status.value
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"  Saved source {source.id} to database")
            
        except Exception as e:
            logger.error(f"  Failed to save source to database: {e}")

    async def process_record(self, message_id: str, data: dict) -> None:
        """Process a single record from the stream."""
        try:
            envelope = json.loads(data.get("data", "{}"))
            meta = envelope.get("meta", {})
            trace_id = meta.get("trace_id", "unknown")
            source_id = meta.get("source", "unknown")
            payload = envelope.get("payload", {})

            if source_id not in self._seen_sources:
                self._register_source(source_id, payload)
            
            self._source_record_counts[source_id] = self._source_record_counts.get(source_id, 0) + 1
            self.processed_count += 1
            
            if self.processed_count % 50 == 0:
                logger.info(
                    f"Progress: {self.processed_count} records | "
                    f"Sources: {list(self._source_record_counts.items())}"
                )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse envelope: {e}")

    async def run(self) -> None:
        """Main consumer loop."""
        logger.info("=" * 60)
        logger.info("DCL Ingest Consumer Starting (with Semantic Mapping)")
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
            logger.info(f"Sources registered: {list(self._seen_sources)}")
            for source_id, count in self._source_record_counts.items():
                logger.info(f"  {source_id}: {count} records")

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
