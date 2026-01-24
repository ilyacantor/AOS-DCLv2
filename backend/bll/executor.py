"""
BLL Demo Executor - Executes definitions against demo datasets using Pandas.
No persistence, in-process only.

ARCHITECTURE NOTE:
This executor is for DEMO MODE ONLY. It directly loads CSV files to simulate
the response that would come from Fabric Planes in production.

In production (Farm mode), the BLL contract endpoints would:
1. Use DCL's FabricPointerBuffer to get pointer references
2. JIT-fetch actual data from Fabric Planes (Snowflake, Kafka, etc.)
3. Never store payloads in DCL

The demo executor provides the same API contract surface so BLL consumers
can develop and test against stable schemas without needing live Fabric Planes.
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .models import (
    Definition, ExecuteRequest, ExecuteResponse, ExecuteMetadata,
    QualityMetrics, LineageReference, ProofResponse, ProofBreadcrumb,
    ColumnSchema, FilterSpec, ComputedSummary
)
from .definitions import get_definition


DATASET_ID = os.environ.get("DCL_DATASET_ID", "demo9")


def _load_manifest(dataset_id: str) -> dict:
    manifest_path = Path(f"dcl/demo/datasets/{dataset_id}/manifest.json")
    if not manifest_path.exists():
        raise FileNotFoundError(f"Dataset manifest not found: {dataset_id}")
    with open(manifest_path) as f:
        return json.load(f)


def _get_table_path(manifest: dict, source_id: str, table_id: str) -> str | None:
    for source in manifest.get("sources", []):
        if source["source_id"] == source_id:
            for table in source.get("tables", []):
                if table["table_id"] == table_id:
                    return table["file"]
    return None


def _load_table(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        return pd.DataFrame()
    return pd.read_csv(file_path)


def _format_currency(amount: float) -> str:
    """Format a number as currency."""
    if amount >= 1_000_000:
        return f"${amount/1_000_000:,.2f}M"
    elif amount >= 1_000:
        return f"${amount/1_000:,.1f}K"
    else:
        return f"${amount:,.2f}"


def _compute_summary(df: pd.DataFrame, definition: Definition) -> ComputedSummary:
    """Compute aggregations and generate human-readable answer based on definition type."""
    aggregations: dict[str, Any] = {}
    answer = ""
    
    amount_cols = [c for c in df.columns if any(x in c.lower() for x in 
                   ['amount', 'revenue', 'cost', 'spend', 'value', 'monthly_cost', 'annual'])]
    
    defn_id = definition.definition_id.lower()
    
    if 'arr' in defn_id or 'revenue' in defn_id:
        if amount_cols:
            total = df[amount_cols[0]].sum()
            aggregations['total_arr'] = float(total)
            aggregations['deal_count'] = len(df)
            answer = f"Your current ARR is {_format_currency(total)} across {len(df)} deals/opportunities."
        else:
            aggregations['row_count'] = len(df)
            answer = f"Found {len(df)} revenue records."
    
    elif 'burn' in defn_id:
        if amount_cols:
            total = df[amount_cols[0]].sum()
            monthly_avg = total / 12 if total > 0 else 0
            aggregations['total_spend'] = float(total)
            aggregations['monthly_avg'] = float(monthly_avg)
            answer = f"Your current burn rate is approximately {_format_currency(monthly_avg)}/month ({_format_currency(total)} total)."
        else:
            answer = f"Found {len(df)} cost records."
    
    elif 'spend' in defn_id or 'cost' in defn_id:
        if amount_cols:
            total = df[amount_cols[0]].sum()
            aggregations['total_spend'] = float(total)
            aggregations['transaction_count'] = len(df)
            answer = f"Total spend is {_format_currency(total)} across {len(df)} transactions."
        else:
            answer = f"Found {len(df)} spend records."
    
    elif 'customer' in defn_id or 'account' in defn_id:
        aggregations['customer_count'] = len(df)
        if amount_cols:
            total = df[amount_cols[0]].sum()
            aggregations['total_revenue'] = float(total)
            answer = f"Top {len(df)} customers with {_format_currency(total)} in total revenue."
        else:
            answer = f"Found {len(df)} customers."
    
    elif 'pipeline' in defn_id or 'deal' in defn_id:
        aggregations['deal_count'] = len(df)
        if amount_cols:
            total = df[amount_cols[0]].sum()
            aggregations['pipeline_value'] = float(total)
            answer = f"Pipeline contains {len(df)} deals worth {_format_currency(total)}."
        else:
            answer = f"Pipeline contains {len(df)} deals."
    
    elif 'zombie' in defn_id or 'idle' in defn_id:
        aggregations['resource_count'] = len(df)
        if amount_cols:
            total = df[amount_cols[0]].sum()
            aggregations['wasted_spend'] = float(total)
            answer = f"Found {len(df)} idle/zombie resources costing {_format_currency(total)}."
        else:
            answer = f"Found {len(df)} idle/zombie resources."
    
    elif 'finding' in defn_id or 'security' in defn_id:
        aggregations['finding_count'] = len(df)
        answer = f"Found {len(df)} security findings."
    
    else:
        aggregations['row_count'] = len(df)
        if amount_cols:
            total = df[amount_cols[0]].sum()
            aggregations['total'] = float(total)
            answer = f"Retrieved {len(df)} records with total value {_format_currency(total)}."
        else:
            answer = f"Retrieved {len(df)} records."
    
    return ComputedSummary(answer=answer, aggregations=aggregations)


def _apply_filter(df: pd.DataFrame, f: FilterSpec) -> pd.DataFrame:
    if f.column not in df.columns:
        return df
    
    if f.operator == "eq":
        return df[df[f.column] == f.value]
    elif f.operator == "ne":
        return df[df[f.column] != f.value]
    elif f.operator == "gt":
        return df[df[f.column] > f.value]
    elif f.operator == "gte":
        return df[df[f.column] >= f.value]
    elif f.operator == "lt":
        return df[df[f.column] < f.value]
    elif f.operator == "lte":
        return df[df[f.column] <= f.value]
    elif f.operator == "in":
        return df[df[f.column].isin(f.value)]
    elif f.operator == "is_null":
        return df[df[f.column].isna()]
    elif f.operator == "is_not_null":
        return df[df[f.column].notna()]
    elif f.operator == "contains":
        return df[df[f.column].astype(str).str.contains(str(f.value), na=False)]
    return df


def execute_definition(request: ExecuteRequest) -> ExecuteResponse:
    start_time = time.time()
    
    definition = get_definition(request.definition_id)
    if not definition:
        raise ValueError(f"Definition not found: {request.definition_id}")
    
    manifest = _load_manifest(request.dataset_id)
    
    tables: dict[str, pd.DataFrame] = {}
    lineage: list[LineageReference] = []
    
    for source_ref in definition.sources:
        table_key = f"{source_ref.source_id}.{source_ref.table_id}"
        file_path = _get_table_path(manifest, source_ref.source_id, source_ref.table_id)
        
        if file_path:
            df = _load_table(file_path)
            available_cols = [c for c in source_ref.columns if c in df.columns]
            if available_cols:
                tables[table_key] = df
                lineage.append(LineageReference(
                    source_id=source_ref.source_id,
                    table_id=source_ref.table_id,
                    columns_used=available_cols,
                    row_contribution=len(df)
                ))
    
    if not tables:
        return _empty_response(request, definition, start_time)
    
    result_df = _execute_query(definition, tables, request)
    
    if request.filters:
        for f in request.filters:
            result_df = _apply_filter(result_df, f)
    
    if definition.default_filters:
        for f in definition.default_filters:
            result_df = _apply_filter(result_df, f)
    
    total_rows = len(result_df)
    result_df = result_df.iloc[request.offset:request.offset + request.limit]
    
    summary = _compute_summary(result_df, definition)
    
    result_df = result_df.fillna("")
    data = result_df.to_dict(orient="records")
    
    execution_time_ms = int((time.time() - start_time) * 1000)
    
    null_count = result_df.isna().sum().sum()
    total_cells = result_df.size if result_df.size > 0 else 1
    null_percentage = (null_count / total_cells) * 100
    
    schema = [
        ColumnSchema(name=col, dtype=str(result_df[col].dtype))
        for col in result_df.columns
    ]
    
    return ExecuteResponse(
        data=data,
        metadata=ExecuteMetadata(
            dataset_id=request.dataset_id,
            definition_id=request.definition_id,
            version=definition.version,
            executed_at=datetime.utcnow(),
            execution_time_ms=execution_time_ms,
            row_count=total_rows,
            result_schema=schema
        ),
        quality=QualityMetrics(
            completeness=1.0 - (null_percentage / 100),
            freshness_hours=0.0,
            row_count=total_rows,
            null_percentage=null_percentage
        ),
        lineage=lineage,
        summary=summary
    )


def _execute_query(definition: Definition, tables: dict[str, pd.DataFrame], request: ExecuteRequest) -> pd.DataFrame:
    if len(tables) == 1:
        return list(tables.values())[0]
    
    if definition.joins:
        result = None
        for join in definition.joins:
            left_key = f"{_find_source_for_table(definition, join.left_table)}.{join.left_table}"
            right_key = f"{_find_source_for_table(definition, join.right_table)}.{join.right_table}"
            
            left_df = tables.get(left_key)
            right_df = tables.get(right_key)
            
            if left_df is None or right_df is None:
                continue
            
            if result is None:
                result = left_df.copy()
            
            how = "inner" if join.join_type == "inner" else "left"
            
            if join.left_key in result.columns and join.right_key in right_df.columns:
                result = pd.merge(
                    result, right_df,
                    left_on=join.left_key,
                    right_on=join.right_key,
                    how=how,
                    suffixes=("", "_right")
                )
        
        return result if result is not None else pd.DataFrame()
    
    return pd.concat(list(tables.values()), ignore_index=True)


def _find_source_for_table(definition: Definition, table_id: str) -> str:
    for source_ref in definition.sources:
        if source_ref.table_id == table_id:
            return source_ref.source_id
    return ""


def _empty_response(request: ExecuteRequest, definition: Definition, start_time: float) -> ExecuteResponse:
    execution_time_ms = int((time.time() - start_time) * 1000)
    return ExecuteResponse(
        data=[],
        metadata=ExecuteMetadata(
            dataset_id=request.dataset_id,
            definition_id=request.definition_id,
            version=definition.version,
            executed_at=datetime.utcnow(),
            execution_time_ms=execution_time_ms,
            row_count=0,
            result_schema=definition.output_schema
        ),
        quality=QualityMetrics(
            completeness=0.0,
            freshness_hours=0.0,
            row_count=0,
            null_percentage=0.0
        ),
        lineage=[]
    )


def generate_proof(definition_id: str) -> ProofResponse:
    definition = get_definition(definition_id)
    if not definition:
        raise ValueError(f"Definition not found: {definition_id}")
    
    breadcrumbs: list[ProofBreadcrumb] = []
    
    breadcrumbs.append(ProofBreadcrumb(
        step=1,
        action="source_load",
        details={
            "sources": [
                {"source_id": s.source_id, "table_id": s.table_id, "columns": s.columns}
                for s in definition.sources
            ]
        }
    ))
    
    if definition.joins:
        breadcrumbs.append(ProofBreadcrumb(
            step=2,
            action="join",
            details={
                "joins": [
                    {
                        "left": f"{j.left_table}.{j.left_key}",
                        "right": f"{j.right_table}.{j.right_key}",
                        "type": j.join_type
                    }
                    for j in definition.joins
                ]
            }
        ))
    
    if definition.default_filters:
        breadcrumbs.append(ProofBreadcrumb(
            step=len(breadcrumbs) + 1,
            action="filter",
            details={
                "filters": [
                    {"column": f.column, "operator": f.operator, "value": f.value}
                    for f in definition.default_filters
                ]
            }
        ))
    
    breadcrumbs.append(ProofBreadcrumb(
        step=len(breadcrumbs) + 1,
        action="project",
        details={
            "output_columns": [c.name for c in definition.output_schema]
        }
    ))
    
    sql = _generate_sql_equivalent(definition)
    
    return ProofResponse(
        definition_id=definition_id,
        version=definition.version,
        generated_at=datetime.utcnow(),
        breadcrumbs=breadcrumbs,
        sql_equivalent=sql
    )


def _generate_sql_equivalent(definition: Definition) -> str:
    columns = ", ".join([c.name for c in definition.output_schema])
    
    if len(definition.sources) == 1:
        src = definition.sources[0]
        return f"SELECT {columns} FROM {src.source_id}.{src.table_id}"
    
    if definition.joins:
        first_src = definition.sources[0]
        sql = f"SELECT {columns}\nFROM {first_src.source_id}.{first_src.table_id}"
        
        for join in definition.joins:
            right_source = _find_source_for_table(definition, join.right_table)
            sql += f"\n{join.join_type.upper()} JOIN {right_source}.{join.right_table}"
            sql += f"\n  ON {join.left_table}.{join.left_key} = {join.right_table}.{join.right_key}"
        
        if definition.default_filters:
            conditions = []
            for f in definition.default_filters:
                if f.operator == "is_null":
                    conditions.append(f"{f.column} IS NULL")
                elif f.operator == "is_not_null":
                    conditions.append(f"{f.column} IS NOT NULL")
                else:
                    conditions.append(f"{f.column} {f.operator.upper()} {repr(f.value)}")
            sql += f"\nWHERE {' AND '.join(conditions)}"
        
        return sql
    
    return f"SELECT {columns} FROM (UNION of {len(definition.sources)} sources)"
