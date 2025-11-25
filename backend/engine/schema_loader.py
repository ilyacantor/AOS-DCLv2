import os
import csv
import json
from typing import List, Dict, Any, Optional
import pandas as pd
import httpx
from backend.domain import SourceSystem, TableSchema, FieldSchema


class SchemaLoader:
    
    @staticmethod
    def load_demo_schemas() -> List[SourceSystem]:
        schemas_path = "schemas/schemas"
        if not os.path.exists(schemas_path):
            return []
        
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
                source = SourceSystem(
                    id=sys_name,
                    name=sys_name.title(),
                    type=sys_type,
                    tags=["demo", sys_type.lower()],
                    tables=tables
                )
                sources.append(source)
        
        return sources
    
    @staticmethod
    def load_farm_schemas(narration=None, run_id: Optional[str] = None, sample_limit: int = 5) -> List[SourceSystem]:
        farm_url = os.getenv("FARM_API_URL", "https://autonomos.farm")
        
        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", f"Fetching Farm schemas from {farm_url} (limit={sample_limit})")
        
        farm_sources = [
            {
                "id": "farm_assets",
                "name": "Enterprise Assets",
                "type": "Assets",
                "endpoint": "/api/synthetic",
                "table_name": "assets",
                "generate_params": {}
            },
            {
                "id": "farm_customers",
                "name": "CRM Customers",
                "type": "CRM",
                "endpoint": "/api/synthetic/customers",
                "table_name": "customers",
                "generate_params": {"generate": "true", "scale": "medium"}
            },
            {
                "id": "farm_invoices",
                "name": "ERP Invoices",
                "type": "ERP",
                "endpoint": "/api/synthetic/invoices",
                "table_name": "invoices",
                "generate_params": {"generate": "true"}
            },
            {
                "id": "farm_events",
                "name": "Time-Series Events",
                "type": "Events",
                "endpoint": "/api/synthetic/events",
                "table_name": "events",
                "generate_params": {"generate": "force", "eventType": "log", "pattern": "hourly"}
            },
            {
                "id": "farm_crm_mock",
                "name": "Mock CRM API",
                "type": "CRM",
                "endpoint": "/api/synthetic/crm/accounts",
                "table_name": "accounts",
                "generate_params": {}
            }
        ]
        
        sources = []
        
        for source_config in farm_sources:
            source_system = SchemaLoader._fetch_farm_source(farm_url, source_config, narration, run_id, sample_limit)
            sources.append(source_system)
        
        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", f"Loaded {len(sources)} Farm sources (including empty ones)")
        
        return sources
    
    @staticmethod
    def _fetch_farm_source(base_url: str, config: Dict[str, Any], narration=None, run_id: Optional[str] = None, sample_limit: int = 5) -> SourceSystem:
        endpoint = config["endpoint"]
        params = {**config.get("generate_params", {}), "limit": sample_limit}
        
        sample_records = []
        total_count = 0
        error_message = None
        
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(f"{base_url}{endpoint}", params=params)
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, list):
                    sample_records = data
                    total_count = len(data)
                elif isinstance(data, dict) and "data" in data:
                    sample_records = data["data"]
                    total_count = data.get("total", len(sample_records))
                elif isinstance(data, dict) and "error" in data:
                    error_message = f"API returned error: {data['error']}"
                    if narration and run_id:
                        narration.add_message(run_id, "SchemaLoader", f"Farm source {config['id']}: {error_message}")
                else:
                    error_message = f"Unexpected response format: {type(data)}"
                    if narration and run_id:
                        narration.add_message(run_id, "SchemaLoader", f"Farm source {config['id']}: {error_message}")
                
                if not sample_records:
                    if narration and run_id:
                        narration.add_message(run_id, "SchemaLoader", f"Farm source {config['id']}: Empty dataset returned, creating minimal schema")
                
        except httpx.HTTPStatusError as e:
            error_message = f"HTTP {e.response.status_code} error: {str(e)}"
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Farm source {config['id']}: {error_message}")
        except httpx.TimeoutException as e:
            error_message = f"Request timeout: {str(e)}"
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Farm source {config['id']}: {error_message}")
        except Exception as e:
            error_message = f"Unexpected error: {str(e)}"
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Farm source {config['id']}: {error_message}")
        
        table_schema = SchemaLoader._infer_table_schema_from_json(
            sample_records,
            config["id"],
            config["table_name"],
            total_count
        )
        
        source_system = SourceSystem(
            id=config["id"],
            name=config["name"],
            type=config["type"],
            tags=["farm", "synthetic", config["type"].lower()],
            tables=[table_schema]
        )
        
        if narration and run_id and sample_records:
            narration.add_message(run_id, "SchemaLoader", f"Farm source {config['id']}: Loaded {len(sample_records)} sample records, inferred {len(table_schema.fields)} fields")
        
        return source_system
    
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
