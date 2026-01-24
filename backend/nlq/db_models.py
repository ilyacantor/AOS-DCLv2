"""
SQLAlchemy database models for NLQ Semantic Layer.

Provides PostgreSQL persistence with:
- Row-level tenant isolation
- JSONB for flexible schema storage
- Proper indexes for query performance
- Audit timestamps
"""

from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy import (
    Column, String, Float, DateTime, ForeignKey, Index, Text,
    UniqueConstraint, CheckConstraint, event
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


# =============================================================================
# Tenant Isolation Mixin
# =============================================================================

class TenantMixin:
    """Mixin providing tenant isolation and audit fields."""

    tenant_id = Column(String(64), nullable=False, default="default", index=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


# =============================================================================
# Core Semantic Layer Tables
# =============================================================================

class CanonicalEventDB(Base, TenantMixin):
    """
    Canonical event types in the semantic model.

    Events are system-agnostic business event types like:
    revenue_recognized, invoice_posted, subscription_started
    """
    __tablename__ = "canonical_events"

    id = Column(String(128), primary_key=True)
    tenant_id = Column(String(64), primary_key=True, default="default")

    description = Column(Text, nullable=True)
    schema_json = Column(JSONB, nullable=False, default=dict)
    time_semantics_json = Column(JSONB, nullable=False, default=dict)

    # Relationships
    bindings = relationship("BindingDB", back_populates="canonical_event", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_canonical_events_tenant", "tenant_id"),
    )


class EntityDB(Base, TenantMixin):
    """
    Business entities (dimensions) in the semantic model.

    Entities are things like: customer, service_line, region, contract
    """
    __tablename__ = "entities"

    id = Column(String(128), primary_key=True)
    tenant_id = Column(String(64), primary_key=True, default="default")

    description = Column(Text, nullable=True)
    identifiers_json = Column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        Index("ix_entities_tenant", "tenant_id"),
    )


class BindingDB(Base, TenantMixin):
    """
    Maps source systems to canonical events/entities.

    Bindings define how source system fields map to canonical fields.
    """
    __tablename__ = "bindings"

    id = Column(String(128), primary_key=True)
    tenant_id = Column(String(64), primary_key=True, default="default")

    source_system = Column(String(128), nullable=False, index=True)
    canonical_event_id = Column(String(128), nullable=False, index=True)

    mapping_json = Column(JSONB, nullable=False, default=dict)
    dims_coverage_json = Column(JSONB, nullable=False, default=dict)

    quality_score = Column(Float, nullable=False, default=0.5)
    freshness_score = Column(Float, nullable=False, default=0.5)

    # Relationships
    canonical_event = relationship("CanonicalEventDB", back_populates="bindings")

    __table_args__ = (
        ForeignKey(
            ["canonical_event_id", "tenant_id"],
            ["canonical_events.id", "canonical_events.tenant_id"],
            name="fk_binding_event"
        ),
        Index("ix_bindings_tenant", "tenant_id"),
        Index("ix_bindings_event", "canonical_event_id"),
        Index("ix_bindings_source", "source_system"),
        CheckConstraint("quality_score >= 0 AND quality_score <= 1", name="ck_quality_score"),
        CheckConstraint("freshness_score >= 0 AND freshness_score <= 1", name="ck_freshness_score"),
    )


class DefinitionDB(Base, TenantMixin):
    """
    Semantic definitions (metrics or views).

    Definitions describe business metrics like: services_revenue, ARR, DSO
    """
    __tablename__ = "definitions"

    id = Column(String(128), primary_key=True)
    tenant_id = Column(String(64), primary_key=True, default="default")

    kind = Column(String(32), nullable=False, default="metric")
    pack = Column(String(64), nullable=True, index=True)  # cfo, cto, coo, ceo
    description = Column(Text, nullable=True)
    default_time_semantics_json = Column(JSONB, nullable=False, default=dict)

    # Relationships
    versions = relationship("DefinitionVersionDB", back_populates="definition", cascade="all, delete-orphan")
    proof_hooks = relationship("ProofHookDB", back_populates="definition", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_definitions_tenant", "tenant_id"),
        Index("ix_definitions_pack", "pack"),
        Index("ix_definitions_kind", "kind"),
        CheckConstraint("kind IN ('metric', 'view')", name="ck_definition_kind"),
    )


class DefinitionVersionDB(Base, TenantMixin):
    """
    Versioned definition specifications.

    Each version contains the full spec for computing a metric.
    """
    __tablename__ = "definition_versions"

    id = Column(String(128), primary_key=True)
    tenant_id = Column(String(64), primary_key=True, default="default")

    definition_id = Column(String(128), nullable=False)
    version = Column(String(32), nullable=False, default="v1")
    status = Column(String(32), nullable=False, default="draft")

    spec_json = Column(JSONB, nullable=False, default=dict)
    published_at = Column(DateTime, nullable=True)

    # Relationships
    definition = relationship("DefinitionDB", back_populates="versions")

    __table_args__ = (
        ForeignKey(
            ["definition_id", "tenant_id"],
            ["definitions.id", "definitions.tenant_id"],
            name="fk_version_definition"
        ),
        Index("ix_definition_versions_tenant", "tenant_id"),
        Index("ix_definition_versions_definition", "definition_id"),
        Index("ix_definition_versions_status", "status"),
        UniqueConstraint("definition_id", "version", "tenant_id", name="uq_definition_version"),
        CheckConstraint("status IN ('draft', 'published', 'deprecated')", name="ck_version_status"),
    )


class ProofHookDB(Base, TenantMixin):
    """
    Proof hooks linking definitions to source system evidence.
    """
    __tablename__ = "proof_hooks"

    id = Column(String(128), primary_key=True)
    tenant_id = Column(String(64), primary_key=True, default="default")

    definition_id = Column(String(128), nullable=False)
    pointer_template_json = Column(JSONB, nullable=False, default=dict)
    availability_score = Column(Float, nullable=False, default=0.5)

    # Relationships
    definition = relationship("DefinitionDB", back_populates="proof_hooks")

    __table_args__ = (
        ForeignKey(
            ["definition_id", "tenant_id"],
            ["definitions.id", "definitions.tenant_id"],
            name="fk_proof_hook_definition"
        ),
        Index("ix_proof_hooks_tenant", "tenant_id"),
        Index("ix_proof_hooks_definition", "definition_id"),
        CheckConstraint("availability_score >= 0 AND availability_score <= 1", name="ck_availability_score"),
    )


# =============================================================================
# Lineage Tracking Tables
# =============================================================================

class LineageEdgeDB(Base, TenantMixin):
    """
    Tracks dependencies between semantic layer objects.

    Used for impact analysis and dependency graphs.
    """
    __tablename__ = "lineage_edges"

    id = Column(String(256), primary_key=True)  # "{source_type}:{source_id}->{target_type}:{target_id}"
    tenant_id = Column(String(64), primary_key=True, default="default")

    source_type = Column(String(64), nullable=False)  # definition, event, entity, binding
    source_id = Column(String(128), nullable=False)
    target_type = Column(String(64), nullable=False)
    target_id = Column(String(128), nullable=False)

    edge_type = Column(String(64), nullable=False)  # requires, produces, joins_to, binds_to
    metadata_json = Column(JSONB, nullable=False, default=dict)

    __table_args__ = (
        Index("ix_lineage_tenant", "tenant_id"),
        Index("ix_lineage_source", "source_type", "source_id"),
        Index("ix_lineage_target", "target_type", "target_id"),
        Index("ix_lineage_edge_type", "edge_type"),
    )


# =============================================================================
# Query Execution Audit Tables
# =============================================================================

class QueryExecutionDB(Base, TenantMixin):
    """
    Audit log for query executions.

    Tracks all queries executed through the semantic layer.
    """
    __tablename__ = "query_executions"

    id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), primary_key=True, default="default")

    definition_id = Column(String(128), nullable=True)
    version = Column(String(32), nullable=True)

    sql_hash = Column(String(64), nullable=False, index=True)
    sql_text = Column(Text, nullable=False)
    params_json = Column(JSONB, nullable=False, default=dict)

    status = Column(String(32), nullable=False, default="pending")  # pending, running, completed, failed
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    row_count = Column(Float, nullable=True)
    execution_time_ms = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_query_exec_tenant", "tenant_id"),
        Index("ix_query_exec_definition", "definition_id"),
        Index("ix_query_exec_status", "status"),
        Index("ix_query_exec_hash", "sql_hash"),
    )


# =============================================================================
# Validation/Consistency Tables
# =============================================================================

class ConsistencyCheckDB(Base, TenantMixin):
    """
    Records of consistency check runs.
    """
    __tablename__ = "consistency_checks"

    id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), primary_key=True, default="default")

    check_type = Column(String(64), nullable=False)  # orphan_events, orphan_definitions, binding_coverage, cycles
    status = Column(String(32), nullable=False, default="passed")  # passed, warning, failed

    issues_json = Column(JSONB, nullable=False, default=list)
    summary = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_consistency_tenant", "tenant_id"),
        Index("ix_consistency_type", "check_type"),
        Index("ix_consistency_status", "status"),
    )


# =============================================================================
# Helper functions
# =============================================================================

def get_all_models():
    """Return all database model classes."""
    return [
        CanonicalEventDB,
        EntityDB,
        BindingDB,
        DefinitionDB,
        DefinitionVersionDB,
        ProofHookDB,
        LineageEdgeDB,
        QueryExecutionDB,
        ConsistencyCheckDB,
    ]
