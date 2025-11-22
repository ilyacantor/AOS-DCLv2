import os
import csv
import json
from typing import List, Dict, Any
import pandas as pd
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
                    print(f"Error loading {csv_path}: {e}")
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
    def load_farm_schemas() -> List[SourceSystem]:
        return [
            SourceSystem(
                id="farm_crm",
                name="Farm CRM System",
                type="CRM",
                tags=["farm", "synthetic", "crm"],
                tables=[
                    TableSchema(
                        id="farm_crm.accounts",
                        system_id="farm_crm",
                        name="accounts",
                        fields=[
                            FieldSchema(name="account_id", type="string", semantic_hint="id"),
                            FieldSchema(name="account_name", type="string", semantic_hint="name"),
                            FieldSchema(name="industry", type="string", semantic_hint="category"),
                            FieldSchema(name="revenue", type="float", semantic_hint="amount"),
                        ],
                        record_count=10000
                    )
                ]
            ),
            SourceSystem(
                id="farm_aws",
                name="Farm AWS Resources",
                type="Cloud",
                tags=["farm", "synthetic", "cloud"],
                tables=[
                    TableSchema(
                        id="farm_aws.resources",
                        system_id="farm_aws",
                        name="resources",
                        fields=[
                            FieldSchema(name="resource_id", type="string", semantic_hint="id"),
                            FieldSchema(name="resource_type", type="string", semantic_hint="category"),
                            FieldSchema(name="cost", type="float", semantic_hint="amount"),
                        ],
                        record_count=50000
                    )
                ]
            )
        ]
    
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
