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
    
    VENDOR_METADATA = {
        "salesforce": {"name": "Salesforce", "type": "CRM"},
        "dynamics": {"name": "Dynamics 365", "type": "CRM"},
        "hubspot": {"name": "HubSpot", "type": "CRM"},
        "zoho": {"name": "Zoho CRM", "type": "CRM"},
        "netsuite": {"name": "NetSuite", "type": "ERP"},
        "sap": {"name": "SAP", "type": "ERP"},
        "oracle": {"name": "Oracle", "type": "ERP"},
        "xero": {"name": "Xero", "type": "Accounting"},
        "quickbooks": {"name": "QuickBooks", "type": "Accounting"},
        "snowflake": {"name": "Snowflake", "type": "DataWarehouse"},
        "databricks": {"name": "Databricks", "type": "DataWarehouse"},
        "bigquery": {"name": "BigQuery", "type": "DataWarehouse"},
        "mongodb": {"name": "MongoDB", "type": "NoSQL"},
        "postgres": {"name": "PostgreSQL", "type": "Database"},
        "mysql": {"name": "MySQL", "type": "Database"},
        "supabase": {"name": "Supabase", "type": "Database"},
    }

    @staticmethod
    def load_farm_schemas(narration=None, run_id: Optional[str] = None, sample_limit: int = 5) -> List[SourceSystem]:
        farm_url = os.getenv("FARM_API_URL", "https://autonomos.farm")
        
        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", f"Fetching Farm schemas from {farm_url} (limit={sample_limit})")
        
        farm_endpoints = [
            {
                "endpoint": "/api/synthetic",
                "table_name": "assets",
                "generate_params": {},
                "fallback_vendor": "farm_assets",
                "fallback_type": "Assets"
            },
            {
                "endpoint": "/api/synthetic/customers",
                "table_name": "customers",
                "generate_params": {"generate": "true", "scale": "medium"},
                "fallback_vendor": "farm_customers",
                "fallback_type": "CRM"
            },
            {
                "endpoint": "/api/synthetic/invoices",
                "table_name": "invoices",
                "generate_params": {"generate": "true"},
                "fallback_vendor": "farm_invoices",
                "fallback_type": "ERP"
            },
            {
                "endpoint": "/api/synthetic/events",
                "table_name": "events",
                "generate_params": {"generate": "force", "eventType": "log", "pattern": "hourly"},
                "fallback_vendor": "farm_events",
                "fallback_type": "Events"
            },
            {
                "endpoint": "/api/synthetic/crm/accounts",
                "table_name": "accounts",
                "generate_params": {},
                "fallback_vendor": "farm_crm_mock",
                "fallback_type": "CRM"
            }
        ]
        
        vendor_records: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        
        for endpoint_config in farm_endpoints:
            records = SchemaLoader._fetch_farm_endpoint(
                farm_url, endpoint_config, narration, run_id, sample_limit
            )
            
            if not records:
                continue
            
            for record in records:
                vendor = record.get("sourceSystem", endpoint_config["fallback_vendor"])
                table_name = endpoint_config["table_name"]
                
                if vendor not in vendor_records:
                    vendor_records[vendor] = {}
                if table_name not in vendor_records[vendor]:
                    vendor_records[vendor][table_name] = []
                vendor_records[vendor][table_name].append(record)
        
        sources = []
        for vendor_id, tables_data in vendor_records.items():
            vendor_meta = SchemaLoader.VENDOR_METADATA.get(vendor_id, {})
            vendor_name = vendor_meta.get("name", vendor_id.replace("_", " ").title())
            vendor_type = vendor_meta.get("type", "Unknown")
            
            tables = []
            for table_name, records in tables_data.items():
                table_schema = SchemaLoader._infer_table_schema_from_json(
                    records, vendor_id, table_name, len(records)
                )
                tables.append(table_schema)
            
            source = SourceSystem(
                id=vendor_id,
                name=vendor_name,
                type=vendor_type,
                tags=["farm", "synthetic", vendor_type.lower()],
                tables=tables
            )
            sources.append(source)
            
            if narration and run_id:
                total_fields = sum(len(t.fields) for t in tables)
                narration.add_message(
                    run_id, "SchemaLoader", 
                    f"Vendor {vendor_name}: {len(tables)} tables, {total_fields} fields"
                )
        
        if narration and run_id:
            narration.add_message(run_id, "SchemaLoader", f"Loaded {len(sources)} vendor sources from Farm")
        
        return sources
    
    @staticmethod
    def _fetch_farm_endpoint(
        base_url: str, 
        config: Dict[str, Any], 
        narration=None, 
        run_id: Optional[str] = None, 
        sample_limit: int = 5
    ) -> List[Dict[str, Any]]:
        endpoint = config["endpoint"]
        params = {**config.get("generate_params", {}), "limit": sample_limit}
        
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(f"{base_url}{endpoint}", params=params)
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "data" in data:
                    return data["data"]
                elif isinstance(data, dict) and "error" in data:
                    if narration and run_id:
                        narration.add_message(run_id, "SchemaLoader", f"Farm {endpoint}: {data['error']}")
                    return []
                else:
                    return []
                    
        except httpx.HTTPStatusError as e:
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Farm {endpoint}: HTTP {e.response.status_code}")
            return []
        except httpx.TimeoutException:
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Farm {endpoint}: Timeout")
            return []
        except Exception as e:
            if narration and run_id:
                narration.add_message(run_id, "SchemaLoader", f"Farm {endpoint}: {str(e)}")
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
