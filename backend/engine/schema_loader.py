import os
import csv
import json
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
import httpx
from backend.domain import SourceSystem, TableSchema, FieldSchema, DiscoveryStatus, ResolutionType
from backend.engine.source_normalizer import get_normalizer, NormalizationResult

logger = logging.getLogger(__name__)


class SchemaLoaderError(Exception):
    """Structured error with machine-readable context."""

    def __init__(self, reason: str, missing_dependency: str, resolution: str):
        self.reason = reason
        self.missing_dependency = missing_dependency
        self.resolution = resolution
        super().__init__(
            f"{reason} | missing: {missing_dependency} | fix: {resolution}"
        )


class SchemaLoader:
    
    @staticmethod
    def load_demo_schemas(narration=None, run_id: Optional[str] = None) -> List[SourceSystem]:
        schemas_path = "schemas/schemas"
        if not os.path.exists(schemas_path):
            return []
        
        normalizer = get_normalizer()
        normalizer.load_registry(narration, run_id)
        
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
        
        return sources

    @staticmethod
    def load_farm_schemas(narration=None, run_id: Optional[str] = None, source_limit: int = 50) -> List[SourceSystem]:
        farm_url = os.getenv("FARM_API_URL", "https://autonomos.farm")
        
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
    def load_farm_schemas_from_pipes(
        narration=None, run_id: Optional[str] = None, source_limit: int = 50
    ) -> List[SourceSystem]:
        """Load Farm schemas from AAM's /api/dcl/export-pipes endpoint.

        Expected response shape from AAM:
            {
              "fabric_planes": [
                {
                  "plane_type": "api_gateway",
                  "vendor": "apigee",
                  "connections": [
                    {
                      "pipe_id": "uuid",
                      "source_name": "Hubspot",
                      "vendor": "hubspot inc",
                      "category": "crm",
                      "fields": ["account_id", "account_name", "revenue", ...],
                      "governance_status": "governed",
                      "health": "unknown",
                      "last_sync": "2026-02-16T..."
                    }
                  ]
                }
              ],
              "total_connections": 739
            }

        Each connection is turned into a table under a SourceSystem keyed by
        normalised source_name. The `fields` list (strings) becomes FieldSchema
        objects with inferred semantic hints.
        """
        aam_url = os.getenv("AAM_API_URL")
        if not aam_url:
            raise SchemaLoaderError(
                reason="Farm mode requires AAM_API_URL but it is not set",
                missing_dependency="AAM_API_URL environment variable",
                resolution="Set AAM_API_URL to the AAM base URL (e.g. https://aam.autonomos.farm)",
            )

        export_url = f"{aam_url}/api/dcl/export-pipes"
        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", f"Fetching pipe schemas from {export_url}")

        # -- fetch --
        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.get(export_url)
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPStatusError as e:
            raise SchemaLoaderError(
                reason=f"AAM export-pipes returned HTTP {e.response.status_code}",
                missing_dependency=export_url,
                resolution="Check that AAM is running and the /api/dcl/export-pipes endpoint is deployed",
            )
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            raise SchemaLoaderError(
                reason=f"Cannot reach AAM at {aam_url}: {type(e).__name__}",
                missing_dependency=export_url,
                resolution="Check AAM_API_URL and network connectivity",
            )

        fabric_planes = payload.get("fabric_planes", [])
        total_connections = payload.get("total_connections", 0)
        if narration and run_id:
            narration.add_message(
                run_id, "SchemaLoader",
                f"Received {len(fabric_planes)} fabric planes, {total_connections} total connections",
            )

        # -- normalise & group connections by canonical source --
        normalizer = get_normalizer()
        normalizer.load_registry(narration, run_id)

        # canonical_id → { table_name → connection_dict }
        source_connections: Dict[str, Dict[str, Dict[str, Any]]] = {}
        source_norm_cache: Dict[str, NormalizationResult] = {}
        piped_count = 0
        empty_count = 0

        for plane in fabric_planes:
            plane_type = plane.get("plane_type", "unknown")
            for conn in plane.get("connections", []):
                raw_source = conn.get("source_name", "unknown")
                pipe_id = conn.get("pipe_id", "unknown")
                fields = conn.get("fields", [])

                if not fields:
                    empty_count += 1
                    continue
                piped_count += 1

                if raw_source not in source_norm_cache:
                    source_norm_cache[raw_source] = normalizer.normalize(raw_source, narration, run_id)

                canonical_id = source_norm_cache[raw_source].canonical_id

                # Use pipe_id as the table name (each connection = one table/pipe)
                table_name = conn.get("table_name") or pipe_id
                if canonical_id not in source_connections:
                    source_connections[canonical_id] = {}
                source_connections[canonical_id][table_name] = conn

        if narration and run_id:
            narration.add_message(
                run_id, "SchemaLoader",
                f"Pipes: {piped_count} with fields, {empty_count} empty (skipped)",
            )

        # -- build SourceSystem list --
        sources: List[SourceSystem] = []
        for canonical_id, tables_map in source_connections.items():
            raw_sources = [raw for raw, norm in source_norm_cache.items()
                           if norm.canonical_id == canonical_id]
            first_raw = raw_sources[0] if raw_sources else canonical_id
            norm_result = source_norm_cache.get(first_raw)
            if not norm_result:
                continue

            canonical = norm_result.canonical_source

            tables: List[TableSchema] = []
            for table_name, conn in tables_map.items():
                field_names = conn.get("fields", [])
                field_schemas = [
                    FieldSchema(
                        name=fname,
                        type="string",  # AAM doesn't send types yet
                        semantic_hint=SchemaLoader._infer_semantic_hint_from_name(fname),
                        nullable=True,
                    )
                    for fname in field_names
                ]
                tables.append(TableSchema(
                    id=f"{canonical_id}.{table_name}",
                    system_id=canonical_id,
                    name=table_name,
                    fields=field_schemas,
                    record_count=None,
                    stats={
                        "fields": len(field_schemas),
                        "pipe_id": conn.get("pipe_id"),
                        "governance_status": conn.get("governance_status"),
                        "health": conn.get("health"),
                    },
                ))

            all_raw_ids = ", ".join(sorted(set(raw_sources))) if len(raw_sources) > 1 else first_raw
            source = SourceSystem(
                id=canonical_id,
                name=canonical.name,
                type=canonical.category.upper() if canonical.category else "Unknown",
                tags=["farm", "pipes", canonical.category or "unknown"] + raw_sources,
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
                status_icon = "✓" if canonical.discovery_status.value == "canonical" else "?"
                narration.add_message(
                    run_id, "SchemaLoader",
                    f"{status_icon} {canonical.name}: {len(tables)} pipes, {total_fields} fields",
                )

        # Sort: canonical + high-trust first
        sources.sort(key=lambda s: (
            0 if s.discovery_status == DiscoveryStatus.CANONICAL else 1,
            -s.trust_score,
            s.name,
        ))

        total_available = len(sources)
        if source_limit and source_limit < total_available:
            sources = sources[:source_limit]
            if narration and run_id:
                narration.add_message(
                    run_id, "SchemaLoader",
                    f"Limited to {source_limit} sources (from {total_available} available)",
                )

        norm_stats = normalizer.get_stats()
        if narration and run_id:
            narration.add_message(
                run_id, "SchemaLoader",
                f"Pipe import complete: {norm_stats['registry_sources']} canonical, "
                f"{norm_stats['discovered_sources']} discovered, {len(sources)} returned",
            )

        return sources

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
            with httpx.Client(timeout=60.0) as client:
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
        if any(k in name_lower for k in ['date', 'time', 'timestamp', 'created', 'updated', '_at']):
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
