"""
Query Execution Bridge for NLQ Semantic Layer.

Provides:
- Execution of compiled SQL against data warehouses
- Query result caching
- Execution audit logging
- Connection management for different backends
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import uuid
import hashlib
import json
from abc import ABC, abstractmethod

from backend.utils.log_utils import get_logger
from backend.nlq.persistence import NLQPersistence
from backend.nlq.compiler import SQLCompiler
from backend.nlq.models import DefinitionVersionSpec

logger = get_logger(__name__)


class ExecutionStatus(Enum):
    """Status of a query execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BackendType(Enum):
    """Supported data warehouse backends."""
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    POSTGRES = "postgres"
    DUCKDB = "duckdb"
    MOCK = "mock"  # For testing


@dataclass
class QueryResult:
    """Result of a query execution."""
    execution_id: str
    status: ExecutionStatus
    sql: str
    params: List[Any]
    rows: List[Dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    columns: List[str] = field(default_factory=list)
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    cached: bool = False
    executed_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "status": self.status.value,
            "sql": self.sql,
            "params": self.params,
            "rows": self.rows,
            "row_count": self.row_count,
            "columns": self.columns,
            "execution_time_ms": self.execution_time_ms,
            "error_message": self.error_message,
            "metadata": self.metadata,
            "cached": self.cached,
            "executed_at": self.executed_at,
        }


@dataclass
class ExecutionAudit:
    """Audit record for query execution."""
    execution_id: str
    tenant_id: str
    definition_id: Optional[str]
    version: Optional[str]
    sql_hash: str
    sql_text: str
    params: Dict[str, Any]
    status: ExecutionStatus
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    row_count: Optional[int] = None
    execution_time_ms: Optional[float] = None
    error_message: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "tenant_id": self.tenant_id,
            "definition_id": self.definition_id,
            "version": self.version,
            "sql_hash": self.sql_hash,
            "sql_text": self.sql_text,
            "params": self.params,
            "status": self.status.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "row_count": self.row_count,
            "execution_time_ms": self.execution_time_ms,
            "error_message": self.error_message,
            "created_at": self.created_at,
        }


class DatabaseBackend(ABC):
    """Abstract base class for database backends."""

    @abstractmethod
    def execute(self, sql: str, params: List[Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Execute SQL and return rows and column names."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is working."""
        pass

    @abstractmethod
    def get_backend_type(self) -> BackendType:
        """Get the backend type."""
        pass


class MockBackend(DatabaseBackend):
    """Mock backend for testing without a real database."""

    def __init__(self, mock_data: Optional[Dict[str, List[Dict[str, Any]]]] = None):
        """
        Initialize mock backend.

        Args:
            mock_data: Dict mapping query hashes to mock results
        """
        self.mock_data = mock_data or {}

    def execute(self, sql: str, params: List[Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Return mock data based on SQL hash."""
        sql_hash = self._hash_query(sql, params)

        if sql_hash in self.mock_data:
            rows = self.mock_data[sql_hash]
            columns = list(rows[0].keys()) if rows else []
            return rows, columns

        # Generate synthetic data
        return self._generate_synthetic_data(sql), ["dimension", "metric_value"]

    def test_connection(self) -> bool:
        """Mock connection always succeeds."""
        return True

    def get_backend_type(self) -> BackendType:
        return BackendType.MOCK

    def _hash_query(self, sql: str, params: List[Any]) -> str:
        """Generate hash for query."""
        content = json.dumps({"sql": sql, "params": [str(p) for p in params]}, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _generate_synthetic_data(self, sql: str) -> List[Dict[str, Any]]:
        """Generate synthetic data based on SQL structure."""
        # Parse simple patterns from SQL
        rows = []

        # Check for common patterns
        if "customer" in sql.lower():
            rows = [
                {"dimension": "Customer A", "metric_value": 100000.0},
                {"dimension": "Customer B", "metric_value": 75000.0},
                {"dimension": "Customer C", "metric_value": 50000.0},
            ]
        elif "service_line" in sql.lower():
            rows = [
                {"dimension": "Professional Services", "metric_value": 250000.0},
                {"dimension": "Managed Services", "metric_value": 150000.0},
                {"dimension": "Support", "metric_value": 100000.0},
            ]
        elif "region" in sql.lower():
            rows = [
                {"dimension": "North America", "metric_value": 500000.0},
                {"dimension": "EMEA", "metric_value": 300000.0},
                {"dimension": "APAC", "metric_value": 200000.0},
            ]
        else:
            rows = [
                {"dimension": "Total", "metric_value": 1000000.0},
            ]

        return rows


class SnowflakeBackend(DatabaseBackend):
    """Snowflake backend for production use."""

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str = "PUBLIC"
    ):
        """
        Initialize Snowflake backend.

        Args:
            account: Snowflake account identifier
            user: Username
            password: Password
            warehouse: Warehouse name
            database: Database name
            schema: Schema name
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self._connection = None

    def _get_connection(self):
        """Get or create Snowflake connection."""
        if self._connection is None:
            try:
                import snowflake.connector
                self._connection = snowflake.connector.connect(
                    account=self.account,
                    user=self.user,
                    password=self.password,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema,
                )
            except ImportError:
                raise ImportError("snowflake-connector-python not installed")
        return self._connection

    def execute(self, sql: str, params: List[Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Execute SQL on Snowflake."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return rows, columns
        finally:
            cursor.close()

    def test_connection(self) -> bool:
        """Test Snowflake connection."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return False

    def get_backend_type(self) -> BackendType:
        return BackendType.SNOWFLAKE


class QueryExecutor:
    """
    Bridge for executing queries against data warehouses.

    Provides:
    - SQL compilation from definition specs
    - Execution against configured backend
    - Result caching
    - Execution audit logging
    """

    def __init__(
        self,
        persistence: Optional[NLQPersistence] = None,
        backend: Optional[DatabaseBackend] = None,
        enable_cache: bool = True,
        cache_ttl_seconds: int = 300
    ):
        """
        Initialize query executor.

        Args:
            persistence: NLQPersistence instance
            backend: Database backend (uses mock if not provided)
            enable_cache: Whether to enable result caching
            cache_ttl_seconds: Cache TTL in seconds
        """
        self.persistence = persistence or NLQPersistence()
        self.backend = backend or MockBackend()
        self.compiler = SQLCompiler(self.persistence)
        self.enable_cache = enable_cache
        self.cache_ttl_seconds = cache_ttl_seconds

        # In-memory cache
        self._cache: Dict[str, Tuple[QueryResult, float]] = {}

        # Audit log (in production would write to database)
        self._audit_log: List[ExecutionAudit] = []

    def execute_definition(
        self,
        definition_id: str,
        version: str = "v1",
        requested_dims: Optional[List[str]] = None,
        time_window: Optional[str] = None,
        additional_filters: Optional[Dict[str, Any]] = None,
        tenant_id: str = "default",
        skip_cache: bool = False
    ) -> QueryResult:
        """
        Execute a query for a definition.

        Args:
            definition_id: Definition ID
            version: Definition version
            requested_dims: Dimensions to group by
            time_window: Time window (QoQ, YoY, etc.)
            additional_filters: Additional runtime filters
            tenant_id: Tenant ID
            skip_cache: Whether to skip cache

        Returns:
            QueryResult with execution results
        """
        execution_id = str(uuid.uuid4())[:8]
        requested_dims = requested_dims or []
        additional_filters = additional_filters or {}

        # Get definition version
        def_version = self.persistence.get_definition_version(
            definition_id, version, tenant_id
        )

        if not def_version:
            return QueryResult(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                sql="",
                params=[],
                error_message=f"Definition version not found: {definition_id} {version}",
            )

        # Compile SQL
        sql, params, metadata = self.compiler.compile(
            spec=def_version.spec,
            definition_id=definition_id,
            requested_dims=requested_dims,
            time_window=time_window,
            additional_filters=additional_filters,
            tenant_id=tenant_id,
        )

        # Check cache
        cache_key = self._cache_key(sql, params)
        if self.enable_cache and not skip_cache:
            cached_result = self._get_cached(cache_key)
            if cached_result:
                cached_result.cached = True
                return cached_result

        # Execute
        result = self._execute_sql(
            execution_id=execution_id,
            sql=sql,
            params=params,
            definition_id=definition_id,
            version=version,
            tenant_id=tenant_id,
            metadata=metadata,
        )

        # Cache result if successful
        if result.status == ExecutionStatus.COMPLETED and self.enable_cache:
            self._set_cached(cache_key, result)

        return result

    def execute_raw_sql(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        tenant_id: str = "default"
    ) -> QueryResult:
        """
        Execute raw SQL (for advanced users).

        Args:
            sql: SQL query
            params: Query parameters
            tenant_id: Tenant ID

        Returns:
            QueryResult with execution results
        """
        execution_id = str(uuid.uuid4())[:8]
        params = params or []

        return self._execute_sql(
            execution_id=execution_id,
            sql=sql,
            params=params,
            definition_id=None,
            version=None,
            tenant_id=tenant_id,
            metadata={},
        )

    def _execute_sql(
        self,
        execution_id: str,
        sql: str,
        params: List[Any],
        definition_id: Optional[str],
        version: Optional[str],
        tenant_id: str,
        metadata: Dict[str, Any]
    ) -> QueryResult:
        """Internal SQL execution with audit logging."""
        sql_hash = self.compiler.generate_query_hash(sql, params)
        started_at = datetime.utcnow()

        # Create audit record
        audit = ExecutionAudit(
            execution_id=execution_id,
            tenant_id=tenant_id,
            definition_id=definition_id,
            version=version,
            sql_hash=sql_hash,
            sql_text=sql,
            params={"values": params},
            status=ExecutionStatus.RUNNING,
            started_at=started_at.isoformat(),
        )
        self._audit_log.append(audit)

        try:
            # Execute query
            rows, columns = self.backend.execute(sql, params)
            completed_at = datetime.utcnow()
            execution_time_ms = (completed_at - started_at).total_seconds() * 1000

            # Update audit
            audit.status = ExecutionStatus.COMPLETED
            audit.completed_at = completed_at.isoformat()
            audit.row_count = len(rows)
            audit.execution_time_ms = execution_time_ms

            return QueryResult(
                execution_id=execution_id,
                status=ExecutionStatus.COMPLETED,
                sql=sql,
                params=params,
                rows=rows,
                row_count=len(rows),
                columns=columns,
                execution_time_ms=execution_time_ms,
                metadata=metadata,
            )

        except Exception as e:
            completed_at = datetime.utcnow()
            execution_time_ms = (completed_at - started_at).total_seconds() * 1000

            # Update audit
            audit.status = ExecutionStatus.FAILED
            audit.completed_at = completed_at.isoformat()
            audit.error_message = str(e)
            audit.execution_time_ms = execution_time_ms

            logger.error(f"Query execution failed: {e}")

            return QueryResult(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                sql=sql,
                params=params,
                execution_time_ms=execution_time_ms,
                error_message=str(e),
                metadata=metadata,
            )

    def _cache_key(self, sql: str, params: List[Any]) -> str:
        """Generate cache key for query."""
        content = json.dumps({"sql": sql, "params": [str(p) for p in params]}, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()

    def _get_cached(self, cache_key: str) -> Optional[QueryResult]:
        """Get cached result if still valid."""
        if cache_key in self._cache:
            result, cached_at = self._cache[cache_key]
            if datetime.utcnow().timestamp() - cached_at < self.cache_ttl_seconds:
                return result
            else:
                del self._cache[cache_key]
        return None

    def _set_cached(self, cache_key: str, result: QueryResult) -> None:
        """Cache a result."""
        self._cache[cache_key] = (result, datetime.utcnow().timestamp())

    def clear_cache(self) -> None:
        """Clear all cached results."""
        self._cache.clear()

    def get_audit_log(
        self,
        tenant_id: Optional[str] = None,
        definition_id: Optional[str] = None,
        status: Optional[ExecutionStatus] = None,
        limit: int = 100
    ) -> List[ExecutionAudit]:
        """
        Get execution audit log with optional filters.

        Args:
            tenant_id: Filter by tenant
            definition_id: Filter by definition
            status: Filter by status
            limit: Max results

        Returns:
            List of audit records
        """
        results = self._audit_log

        if tenant_id:
            results = [a for a in results if a.tenant_id == tenant_id]
        if definition_id:
            results = [a for a in results if a.definition_id == definition_id]
        if status:
            results = [a for a in results if a.status == status]

        # Sort by created_at descending
        results = sorted(results, key=lambda a: a.created_at, reverse=True)

        return results[:limit]

    def get_execution_stats(self, tenant_id: str = "default") -> Dict[str, Any]:
        """
        Get execution statistics.

        Args:
            tenant_id: Tenant ID

        Returns:
            Dict with execution statistics
        """
        tenant_audits = [a for a in self._audit_log if a.tenant_id == tenant_id]

        total = len(tenant_audits)
        completed = len([a for a in tenant_audits if a.status == ExecutionStatus.COMPLETED])
        failed = len([a for a in tenant_audits if a.status == ExecutionStatus.FAILED])

        avg_time = 0.0
        if completed > 0:
            times = [a.execution_time_ms for a in tenant_audits if a.execution_time_ms]
            avg_time = sum(times) / len(times) if times else 0.0

        return {
            "total_executions": total,
            "completed": completed,
            "failed": failed,
            "success_rate": completed / total if total > 0 else 0.0,
            "average_execution_time_ms": avg_time,
            "cache_size": len(self._cache),
            "backend_type": self.backend.get_backend_type().value,
        }

    def test_backend_connection(self) -> bool:
        """Test backend connection."""
        return self.backend.test_connection()
