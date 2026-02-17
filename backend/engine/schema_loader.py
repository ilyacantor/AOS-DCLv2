import os
import csv
import json
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import httpx
import psycopg2
from backend.domain import SourceSystem, TableSchema, FieldSchema, DiscoveryStatus, ResolutionType, Mapping
from backend.engine.source_normalizer import get_normalizer, NormalizationResult
from backend.utils.log_utils import get_logger
from backend.core.constants import SCHEMA_CACHE_TTL

logger = get_logger(__name__)


class SchemaLoader:
    
    _demo_cache: Optional[List[SourceSystem]] = None
    _stream_cache: Optional[List[SourceSystem]] = None
    _cache_time: float = 0
    _CACHE_TTL: float = SCHEMA_CACHE_TTL

    _aam_cache: Optional[Tuple[List[SourceSystem], Dict[str, Any]]] = None
    _aam_cache_time: float = 0
    _AAM_CACHE_TTL: float = 120
    
    @staticmethod
    def load_demo_schemas(narration=None, run_id: Optional[str] = None) -> List[SourceSystem]:
        import time
        now = time.time()
        if SchemaLoader._demo_cache is not None and (now - SchemaLoader._cache_time) < SchemaLoader._CACHE_TTL:
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Using cached demo schemas ({len(SchemaLoader._demo_cache)} sources)")
            # Return a copy to prevent callers from modifying the cache
            return list(SchemaLoader._demo_cache)
        
        schemas_path = "schemas/schemas"
        if not os.path.exists(schemas_path):
            return []
        
        normalizer = get_normalizer()
        # Skip Farm API registry call for Demo mode — it only uses local CSV
        # files and built-in aliases are sufficient for normalization.
        normalizer._registry_loaded = True

        sources = []

        source_dirs = {
            "salesforce": ("salesforce", "CRM"),
            "dynamics": ("dynamics", "CRM"),
            "hubspot": ("hubspot", "CRM"),
            "sap": ("sap", "ERP"),
            "netsuite": ("netsuite", "Financial"),
            "snowflake": ("snowflake", "DataWarehouse"),
            "legacy_sql": ("legacy_sql", "DataWarehouse"),
            "mongodb": ("mongodb", "NoSQL"),
            "supabase": ("supabase", "Database")
        }
        
        for dir_name, (sys_name, sys_type) in source_dirs.items():
            dir_path = os.path.join(schemas_path, dir_name)
            if not os.path.exists(dir_path):
                continue
            
            tables = []
            csv_files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
            
            for csv_file in csv_files:
                table_name = csv_file.replace('.csv', '')
                csv_path = os.path.join(dir_path, csv_file)
                
                try:
                    df = pd.read_csv(csv_path)
                    fields = []
                    
                    for col in df.columns:
                        series = df[col]
                        dtype = str(series.dtype)
                        semantic_hint = SchemaLoader._detect_semantic_hint(col, series)
                        has_nulls = bool(series.isnull().any())
                        
                        field = FieldSchema(
                            name=col,
                            type=dtype,
                            semantic_hint=semantic_hint,
                            nullable=has_nulls,
                            distinct_count=int(series.nunique()) if len(df) > 0 else 0,
                            null_percent=float(series.isnull().sum() / len(df) * 100) if len(df) > 0 else 0.0,
                            sample_values=series.dropna().head(3).tolist() if len(df) > 0 else []
                        )
                        fields.append(field)
                    
                    table_id = f"{sys_name}.{table_name}"
                    table = TableSchema(
                        id=table_id,
                        system_id=sys_name,
                        name=table_name,
                        fields=fields,
                        record_count=len(df),
                        stats={"columns": len(df.columns)}
                    )
                    tables.append(table)
                
                except Exception as e:
                    logger.error(f"Failed to load CSV schema {csv_path}: {e}", exc_info=True)
                    continue
            
            if tables:
                norm_result = normalizer.normalize(sys_name, narration, run_id)
                canonical = norm_result.canonical_source
                
                source = SourceSystem(
                    id=norm_result.canonical_id,
                    name=canonical.name,
                    type=canonical.category.upper() if canonical.category else sys_type,
                    tags=["demo", canonical.category or sys_type.lower()],
                    tables=tables,
                    canonical_id=norm_result.canonical_id,
                    raw_id=sys_name,
                    discovery_status=DiscoveryStatus(canonical.discovery_status.value),
                    resolution_type=ResolutionType(norm_result.resolution_type.value),
                    trust_score=canonical.trust_score,
                    data_quality_score=canonical.data_quality_score,
                    vendor=canonical.vendor,
                    category=canonical.category,
                    entities=canonical.entities,
                )
                sources.append(source)
        
        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", f"Loaded {len(sources)} demo sources with normalization")
        
        import time
        SchemaLoader._demo_cache = sources
        SchemaLoader._cache_time = time.time()
        
        # Return a copy to prevent caller from mutating the cache
        return list(sources)

    @staticmethod
    def load_farm_schemas(narration=None, run_id: Optional[str] = None, source_limit: int = 1000) -> List[SourceSystem]:
        from backend.core.constants import FARM_API_URL
        farm_url = FARM_API_URL
        
        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", f"Fetching Farm data from {farm_url}/api/browser/*")
        
        normalizer = get_normalizer()
        registry_count = normalizer.load_registry(narration, run_id)
        
        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", f"Registry loaded: {registry_count} canonical sources")
        
        browser_endpoints = [
            {"endpoint": "/api/browser/customers", "table_name": "customers", "entity_type": "Customer"},
            {"endpoint": "/api/browser/invoices", "table_name": "invoices", "entity_type": "Invoice"},
            {"endpoint": "/api/synthetic", "table_name": "assets", "entity_type": "Asset"},
            {"endpoint": "/api/synthetic/events", "table_name": "events", "entity_type": "Event"},
            {"endpoint": "/api/synthetic/crm/accounts", "table_name": "crm_accounts", "entity_type": "Account"},
        ]
        
        source_records: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        source_norm_cache: Dict[str, NormalizationResult] = {}
        
        for endpoint_config in browser_endpoints:
            records = SchemaLoader._fetch_browser_endpoint(
                farm_url, endpoint_config, narration, run_id, limit=500
            )
            
            if not records:
                continue
            
            if narration and run_id:
                narration.add_message(
                    run_id, "SchemaLoader", 
                    f"Fetched {len(records)} records from {endpoint_config['endpoint']}"
                )
            
            for record in records:
                raw_source = record.get("sourceSystem", "unknown")
                table_name = endpoint_config["table_name"]
                
                if raw_source not in source_norm_cache:
                    norm_result = normalizer.normalize(raw_source, narration, run_id)
                    source_norm_cache[raw_source] = norm_result
                
                canonical_id = source_norm_cache[raw_source].canonical_id
                
                if canonical_id not in source_records:
                    source_records[canonical_id] = {}
                if table_name not in source_records[canonical_id]:
                    source_records[canonical_id][table_name] = []
                source_records[canonical_id][table_name].append(record)
        
        sources = []
        for canonical_id, tables_data in source_records.items():
            raw_sources = [raw for raw, norm in source_norm_cache.items() 
                          if norm.canonical_id == canonical_id]
            first_raw = raw_sources[0] if raw_sources else canonical_id
            norm_result = source_norm_cache.get(first_raw)
            
            if not norm_result:
                continue
            
            canonical = norm_result.canonical_source
            
            tables = []
            for table_name, records in tables_data.items():
                table_schema = SchemaLoader._infer_table_schema_from_json(
                    records, canonical_id, table_name, len(records)
                )
                tables.append(table_schema)
            
            all_raw_ids = ", ".join(sorted(set(raw_sources))) if len(raw_sources) > 1 else first_raw
            
            source = SourceSystem(
                id=canonical_id,
                name=canonical.name,
                type=canonical.category.upper() if canonical.category else "Unknown",
                tags=["farm", "browser", canonical.category or "unknown"] + raw_sources,
                tables=tables,
                canonical_id=canonical_id,
                raw_id=all_raw_ids,
                discovery_status=DiscoveryStatus(canonical.discovery_status.value),
                resolution_type=ResolutionType(norm_result.resolution_type.value),
                trust_score=canonical.trust_score,
                data_quality_score=canonical.data_quality_score,
                vendor=canonical.vendor,
                category=canonical.category,
                entities=canonical.entities,
            )
            sources.append(source)
            
            if narration and run_id:
                total_fields = sum(len(t.fields) for t in tables)
                total_records = sum(len(records) for records in tables_data.values())
                status_icon = "✓" if canonical.discovery_status.value == "canonical" else "?"
                narration.add_message(
                    run_id, "SchemaLoader", 
                    f"{status_icon} {canonical.name}: {len(tables)} tables, {total_fields} fields, {total_records} records"
                )
        
        sources.sort(key=lambda s: (
            0 if s.discovery_status == DiscoveryStatus.CANONICAL else 1,
            -s.trust_score,
            s.name
        ))
        
        # Apply source_limit after sorting (prioritizes canonical + high trust sources)
        total_available = len(sources)
        if source_limit and source_limit < total_available:
            sources = sources[:source_limit]
            if narration and run_id:
                narration.add_message(
                    run_id, "SchemaLoader", 
                    f"Limited to {source_limit} sources (from {total_available} available)"
                )
        
        norm_stats = normalizer.get_stats()
        if narration and run_id:
            narration.add_message(
                run_id, "SchemaLoader", 
                f"Normalization complete: {norm_stats['registry_sources']} canonical, "
                f"{norm_stats['discovered_sources']} discovered, {len(sources)} returned"
            )
        
        return sources
    
    @staticmethod
    def load_aam_schemas(narration=None, run_id: Optional[str] = None, source_limit: int = 1000, aod_run_id: Optional[str] = None) -> Tuple[List[SourceSystem], Dict[str, Any]]:
        """
        Load schemas from AAM's pipe export via the AAM Ingress Adapter.

        All AAM data is validated and normalized at the ingress boundary.
        No ad-hoc normalization happens in this method.
        """
        import time as _time
        import copy

        now = _time.time()
        if SchemaLoader._aam_cache is not None and (now - SchemaLoader._aam_cache_time) < SchemaLoader._AAM_CACHE_TTL:
            cached_sources, cached_kpis = SchemaLoader._aam_cache
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Using cached AAM schemas ({len(cached_sources)} sources)")
            return copy.deepcopy(cached_sources), dict(cached_kpis)

        from backend.aam.client import get_aam_client
        from backend.aam.ingress import AAMIngressAdapter

        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", "Fetching pipes from AAM...")

        try:
            aam_client = get_aam_client()
            pipes_data = aam_client.get_pipes(aod_run_id=aod_run_id)
        except Exception as e:
            logger.error(f"Failed to fetch from AAM: {e}")
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"⚠ AAM fetch failed: {e}")
            if SchemaLoader._aam_cache is not None:
                logger.info("AAM fetch failed, falling back to stale cache")
                cached_sources, cached_kpis = SchemaLoader._aam_cache
                return copy.deepcopy(cached_sources), dict(cached_kpis)
            return [], {"fabrics": 0, "pipes": 0, "sources": 0, "unpipedCount": 0, "totalAamConnections": 0}

        adapter = AAMIngressAdapter()
        payload = adapter.ingest_pipes(pipes_data)

        if narration and run_id:
            narration.add_message(
                run_id, "SchemaLoader",
                f"Received {len(payload.planes)} fabric planes with {payload.total_connections_actual} connections from AAM"
            )

        SchemaLoader._sync_pipe_definition_store(payload, aod_run_id, narration, run_id)

        sources = []

        for plane in payload.planes:
            if narration and run_id:
                narration.add_message(
                    run_id, "SchemaLoader",
                    f"Processing {plane.plane_type} plane ({plane.vendor}): {plane.pipe_count} connections"
                )

            for pipe in plane.pipes:
                # Create FieldSchema objects from field names
                fields = []
                for field_name in pipe.fields:
                    semantic_hint = SchemaLoader._infer_semantic_hint_from_name(field_name)
                    field = FieldSchema(
                        name=field_name,
                        type="string",  # Default type, AAM doesn't provide this yet
                        semantic_hint=semantic_hint,
                        nullable=True,
                        distinct_count=0,
                        null_percent=0.0,
                        sample_values=[]
                    )
                    fields.append(field)

                # Create table schema
                table_id = f"{pipe.display_name}.{pipe.fabric_plane}_data"
                table = TableSchema(
                    id=table_id,
                    system_id=pipe.display_name,
                    name=f"{pipe.fabric_plane}_data",
                    fields=fields,
                    record_count=0,
                    stats={"plane": pipe.fabric_plane, "vendor": pipe.vendor}
                )

                # Create SourceSystem from adapter-normalized pipe data
                # canonical_id, trust_score, data_quality_score all come from the adapter
                source = SourceSystem(
                    id=pipe.canonical_id,
                    name=pipe.display_name,
                    type=pipe.category.upper(),
                    tags=["aam", pipe.fabric_plane, pipe.vendor.lower(), pipe.governance_status],
                    tables=[table],
                    canonical_id=pipe.canonical_id,
                    raw_id=pipe.display_name,
                    discovery_status=DiscoveryStatus.CANONICAL,
                    resolution_type=ResolutionType.EXACT,
                    trust_score=pipe.trust_score,
                    data_quality_score=pipe.data_quality_score,
                    vendor=pipe.vendor,
                    category=pipe.category,
                    fabric_plane=pipe.fabric_plane,
                    entities=[],
                )
                sources.append(source)

                if narration and run_id:
                    status_icon = "✓" if pipe.governance_status == "governed" else "⚠"
                    narration.add_message(
                        run_id, "SchemaLoader",
                        f"{status_icon} {pipe.display_name} ({pipe.fabric_plane}): {pipe.field_count} fields"
                    )

        # Sort by trust score and governance
        sources.sort(key=lambda s: (
            0 if "governed" in s.tags else 1,
            -s.trust_score,
            s.name
        ))

        # Compute KPIs from FULL source list BEFORE truncation
        total_available = len(sources)
        total_piped = sum(1 for s in sources if any(len(t.fields) > 0 for t in s.tables))
        total_unpiped = sum(1 for s in sources if all(len(t.fields) == 0 for t in s.tables))

        # Apply source_limit
        if source_limit and source_limit < total_available:
            sources = sources[:source_limit]
            if narration and run_id:
                narration.add_message(
                    run_id, "SchemaLoader",
                    f"Limited to {source_limit} sources (from {total_available} available)"
                )

        if narration and run_id:
            narration.add_message(
                run_id, "SchemaLoader",
                f"AAM schema loading complete: {len(sources)} sources loaded"
            )

        kpis = {
            "fabrics": len(payload.planes),
            "pipes": total_piped,
            "sources": total_available,
            "unpipedCount": total_unpiped,
            "totalAamConnections": payload.total_connections_reported,
            "limited": source_limit < total_available if source_limit else False,
            "loadedSources": len(sources),
            "fabricPlaneVendors": [f"{p.plane_type}:{p.vendor}" for p in payload.planes],
            "snapshotName": payload.snapshot_name,
        }

        SchemaLoader._aam_cache = (copy.deepcopy(sources), dict(kpis))
        SchemaLoader._aam_cache_time = _time.time()

        return sources, kpis
    
    @staticmethod
    def _sync_pipe_definition_store(payload, aod_run_id=None, narration=None, run_id=None):
        """Bridge: populate PipeDefinitionStore from the AAM pull path.

        This ensures the ingest guard sees the same pipes that the
        graph/dashboard pipeline uses — one data flow, one store.
        """
        from datetime import datetime, timezone as tz
        try:
            from backend.api.pipe_store import PipeDefinition, get_pipe_store
            pipe_store = get_pipe_store()
            now = datetime.now(tz.utc).isoformat()
            definitions = []

            for plane in payload.planes:
                for pipe in plane.pipes:
                    if not pipe.pipe_id:
                        continue
                    defn = PipeDefinition(
                        pipe_id=pipe.pipe_id,
                        candidate_id=getattr(pipe, "candidate_id", ""),
                        source_name=pipe.display_name,
                        vendor=pipe.vendor,
                        category=pipe.category,
                        governance_status=pipe.governance_status,
                        fields=pipe.fields,
                        entity_scope=getattr(pipe, "entity_scope", None),
                        identity_keys=getattr(pipe, "identity_keys", []),
                        transport_kind=getattr(pipe, "transport_kind", None),
                        modality=getattr(pipe, "modality", None),
                        change_semantics=getattr(pipe, "change_semantics", None),
                        health=getattr(pipe, "health", "unknown"),
                        last_sync=getattr(pipe, "last_sync", None),
                        asset_key=getattr(pipe, "asset_key", ""),
                        aod_asset_id=getattr(pipe, "aod_asset_id", None),
                        fabric_plane=pipe.fabric_plane,
                        received_at=now,
                    )
                    definitions.append(defn)

            if definitions:
                receipt = pipe_store.register_batch(
                    definitions=definitions,
                    aod_run_id=aod_run_id,
                    source="aam-pull",
                )
                logger.info(
                    f"[SchemaLoader] Synced {len(definitions)} pipes into "
                    f"PipeDefinitionStore (aod_run_id={aod_run_id})"
                )
                if narration and run_id:
                    narration.add_message(
                        run_id, "SchemaLoader",
                        f"Synced {len(definitions)} pipe definitions to ingest guard"
                    )
            else:
                logger.warning("[SchemaLoader] No pipes with pipe_id to sync to PipeDefinitionStore")
        except Exception as e:
            logger.error(f"[SchemaLoader] PipeDefinitionStore sync failed: {e}")

    @staticmethod
    def _get_pool():
        try:
            from backend.semantic_mapper.persist_mappings import MappingPersistence
            persistence = MappingPersistence()
            return persistence
        except Exception as e:
            logger.warning(f"Failed to get connection pool: {e}")
            return None
    
    @staticmethod
    def load_stream_sources(narration=None, run_id: Optional[str] = None) -> List[SourceSystem]:
        """Load stream sources from the database (registered by Consumer)."""
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            return []
        
        pool = SchemaLoader._get_pool()
        if pool is None:
            return []
        
        try:
            with pool._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, name, type, vendor, category, trust_score, discovery_status
                        FROM source_systems
                        WHERE type = 'stream'
                    """)
                    
                    sources = []
                    rows = cursor.fetchall()
                    
                for row in rows:
                    source_id = row[0]
                    
                    with pool._get_connection() as conn2:
                        with conn2.cursor() as cursor2:
                            cursor2.execute("""
                                SELECT DISTINCT table_name, field_name, concept_id, confidence
                                FROM field_concept_mappings
                                WHERE source_id = %s
                                ORDER BY table_name, field_name
                            """, (source_id,))
                            
                            tables_data: Dict[str, List[FieldSchema]] = {}
                            for mapping_row in cursor2.fetchall():
                                table_name = mapping_row[0]
                                field_name = mapping_row[1]
                                
                                if table_name not in tables_data:
                                    tables_data[table_name] = []
                                
                                tables_data[table_name].append(FieldSchema(
                                    name=field_name,
                                    type="string",
                                    semantic_hint=None,
                                    nullable=True
                                ))
                    
                    tables = []
                    for table_name, fields in tables_data.items():
                        tables.append(TableSchema(
                            id=f"{source_id}.{table_name}",
                            system_id=source_id,
                            name=table_name,
                            fields=fields
                        ))
                    
                    source = SourceSystem(
                        id=source_id,
                        name=row[1],
                        type=row[2] or "stream",
                        tags=["stream", "real-time", "farm-synced"],
                        tables=tables,
                        discovery_status=DiscoveryStatus.CUSTOM,
                        resolution_type=ResolutionType.PATTERN,
                        trust_score=row[5] or 75,
                        vendor=row[3],
                        category=row[4],
                    )
                    sources.append(source)
                    
                    if narration and run_id:
                        total_fields = sum(len(t.fields) for t in tables)
                        narration.add_message(
                            run_id, "SchemaLoader",
                            f"Stream source: {row[1]} - {len(tables)} tables, {total_fields} fields"
                        )
                
                return sources
            
        except Exception as e:
            logger.warning(f"Failed to load stream sources: {e}")
            return []
    
    @staticmethod
    def _fetch_browser_endpoint(
        base_url: str, 
        config: Dict[str, Any], 
        narration=None, 
        run_id: Optional[str] = None, 
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        endpoint = config["endpoint"]
        params = {"limit": limit}
        
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(f"{base_url}{endpoint}", params=params)
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "data" in data:
                    return data["data"]
                elif isinstance(data, dict) and "error" in data:
                    if narration and run_id:
                        narration.add_message(run_id, "SchemaLoader", f"Browser {endpoint}: {data['error']}")
                    return []
                else:
                    return []
                    
        except httpx.HTTPStatusError as e:
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Browser {endpoint}: HTTP {e.response.status_code}")
            return []
        except httpx.TimeoutException:
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Browser {endpoint}: Timeout")
            return []
        except Exception as e:
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Browser {endpoint}: {str(e)}")
            return []
    
    @staticmethod
    def _infer_table_schema_from_json(
        records: List[Dict[str, Any]],
        system_id: str,
        table_name: str,
        record_count: int
    ) -> TableSchema:
        if not records:
            return TableSchema(
                id=f"{system_id}.{table_name}",
                system_id=system_id,
                name=table_name,
                fields=[],
                record_count=0
            )
        
        all_field_names = set()
        for record in records:
            all_field_names.update(record.keys())
        
        fields = []
        
        for field_name in sorted(all_field_names):
            non_null_values = [r.get(field_name) for r in records if r.get(field_name) is not None]
            
            if non_null_values:
                field_type = SchemaLoader._infer_json_type(non_null_values[0])
            else:
                field_type = "string"
            
            semantic_hint = SchemaLoader._infer_semantic_hint_from_name(field_name)
            
            sample_values = [
                r.get(field_name) 
                for r in records[:3] 
                if r.get(field_name) is not None
            ]
            
            has_nulls = any(r.get(field_name) is None for r in records)
            distinct_values = len(set(str(r.get(field_name)) for r in records if r.get(field_name) is not None))
            
            field = FieldSchema(
                name=field_name,
                type=field_type,
                semantic_hint=semantic_hint,
                nullable=has_nulls,
                distinct_count=distinct_values,
                null_percent=0.0,
                sample_values=sample_values[:3]
            )
            fields.append(field)
        
        table_id = f"{system_id}.{table_name}"
        return TableSchema(
            id=table_id,
            system_id=system_id,
            name=table_name,
            fields=fields,
            record_count=record_count,
            stats={"fields": len(fields)}
        )
    
    @staticmethod
    def _infer_json_type(value: Any) -> str:
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
        return "string"
    
    @staticmethod
    def _infer_semantic_hint_from_name(field_name: str) -> str:
        name_lower = field_name.lower()
        
        if name_lower.endswith('id') or name_lower == 'id':
            return "id"
        if 'name' in name_lower or 'title' in name_lower:
            return "name"
        if 'email' in name_lower or 'mail' in name_lower:
            return "email"
        if any(k in name_lower for k in ['amount', 'cost', 'price', 'revenue', 'spend']):
            return "amount"
        if any(k in name_lower for k in ['date', 'time', 'timestamp', 'created', 'updated', 'at']):
            return "timestamp"
        if any(k in name_lower for k in ['status', 'state', 'stage', 'tier']):
            return "status"
        if any(k in name_lower for k in ['region', 'country', 'location', 'environment']):
            return "region"
        if any(k in name_lower for k in ['type', 'category', 'class', 'industry', 'criticality', 'severity']):
            return "category"
        
        return "generic"
    
    @staticmethod
    def _detect_semantic_hint(col_name: str, series: Any) -> str:
        col_lower = col_name.lower()
        
        if any(k in col_lower for k in ['id', '_id', 'key']):
            return "id"
        if any(k in col_lower for k in ['name', 'title']):
            return "name"
        if any(k in col_lower for k in ['email', 'mail']):
            return "email"
        if any(k in col_lower for k in ['amount', 'cost', 'price', 'revenue', 'spend']):
            return "amount"
        if any(k in col_lower for k in ['date', 'time', 'timestamp', 'created', 'updated']):
            return "timestamp"
        if any(k in col_lower for k in ['status', 'state', 'stage']):
            return "status"
        if any(k in col_lower for k in ['region', 'country', 'location']):
            return "region"
        if any(k in col_lower for k in ['type', 'category', 'class']):
            return "category"
        
        return "generic"
