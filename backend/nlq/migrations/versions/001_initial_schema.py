"""
Initial schema for NLQ Semantic Layer.

Revision ID: 001_initial
Revises:
Create Date: 2025-01-24
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = '001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ==========================================================================
    # Canonical Events
    # ==========================================================================
    op.create_table(
        'canonical_events',
        sa.Column('id', sa.String(128), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('schema_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('time_semantics_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id')
    )
    op.create_index('ix_canonical_events_tenant', 'canonical_events', ['tenant_id'])

    # ==========================================================================
    # Entities
    # ==========================================================================
    op.create_table(
        'entities',
        sa.Column('id', sa.String(128), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('identifiers_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id')
    )
    op.create_index('ix_entities_tenant', 'entities', ['tenant_id'])

    # ==========================================================================
    # Bindings
    # ==========================================================================
    op.create_table(
        'bindings',
        sa.Column('id', sa.String(128), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('source_system', sa.String(128), nullable=False),
        sa.Column('canonical_event_id', sa.String(128), nullable=False),
        sa.Column('mapping_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('dims_coverage_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('quality_score', sa.Float(), nullable=False, server_default='0.5'),
        sa.Column('freshness_score', sa.Float(), nullable=False, server_default='0.5'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id'),
        sa.ForeignKeyConstraint(
            ['canonical_event_id', 'tenant_id'],
            ['canonical_events.id', 'canonical_events.tenant_id'],
            name='fk_binding_event'
        ),
        sa.CheckConstraint('quality_score >= 0 AND quality_score <= 1', name='ck_quality_score'),
        sa.CheckConstraint('freshness_score >= 0 AND freshness_score <= 1', name='ck_freshness_score'),
    )
    op.create_index('ix_bindings_tenant', 'bindings', ['tenant_id'])
    op.create_index('ix_bindings_event', 'bindings', ['canonical_event_id'])
    op.create_index('ix_bindings_source', 'bindings', ['source_system'])

    # ==========================================================================
    # Definitions
    # ==========================================================================
    op.create_table(
        'definitions',
        sa.Column('id', sa.String(128), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('kind', sa.String(32), nullable=False, server_default='metric'),
        sa.Column('pack', sa.String(64), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('default_time_semantics_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id'),
        sa.CheckConstraint("kind IN ('metric', 'view')", name='ck_definition_kind'),
    )
    op.create_index('ix_definitions_tenant', 'definitions', ['tenant_id'])
    op.create_index('ix_definitions_pack', 'definitions', ['pack'])
    op.create_index('ix_definitions_kind', 'definitions', ['kind'])

    # ==========================================================================
    # Definition Versions
    # ==========================================================================
    op.create_table(
        'definition_versions',
        sa.Column('id', sa.String(128), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('definition_id', sa.String(128), nullable=False),
        sa.Column('version', sa.String(32), nullable=False, server_default='v1'),
        sa.Column('status', sa.String(32), nullable=False, server_default='draft'),
        sa.Column('spec_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('published_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id'),
        sa.ForeignKeyConstraint(
            ['definition_id', 'tenant_id'],
            ['definitions.id', 'definitions.tenant_id'],
            name='fk_version_definition'
        ),
        sa.UniqueConstraint('definition_id', 'version', 'tenant_id', name='uq_definition_version'),
        sa.CheckConstraint("status IN ('draft', 'published', 'deprecated')", name='ck_version_status'),
    )
    op.create_index('ix_definition_versions_tenant', 'definition_versions', ['tenant_id'])
    op.create_index('ix_definition_versions_definition', 'definition_versions', ['definition_id'])
    op.create_index('ix_definition_versions_status', 'definition_versions', ['status'])

    # ==========================================================================
    # Proof Hooks
    # ==========================================================================
    op.create_table(
        'proof_hooks',
        sa.Column('id', sa.String(128), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('definition_id', sa.String(128), nullable=False),
        sa.Column('pointer_template_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('availability_score', sa.Float(), nullable=False, server_default='0.5'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id'),
        sa.ForeignKeyConstraint(
            ['definition_id', 'tenant_id'],
            ['definitions.id', 'definitions.tenant_id'],
            name='fk_proof_hook_definition'
        ),
        sa.CheckConstraint('availability_score >= 0 AND availability_score <= 1', name='ck_availability_score'),
    )
    op.create_index('ix_proof_hooks_tenant', 'proof_hooks', ['tenant_id'])
    op.create_index('ix_proof_hooks_definition', 'proof_hooks', ['definition_id'])

    # ==========================================================================
    # Lineage Edges
    # ==========================================================================
    op.create_table(
        'lineage_edges',
        sa.Column('id', sa.String(256), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('source_type', sa.String(64), nullable=False),
        sa.Column('source_id', sa.String(128), nullable=False),
        sa.Column('target_type', sa.String(64), nullable=False),
        sa.Column('target_id', sa.String(128), nullable=False),
        sa.Column('edge_type', sa.String(64), nullable=False),
        sa.Column('metadata_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id'),
    )
    op.create_index('ix_lineage_tenant', 'lineage_edges', ['tenant_id'])
    op.create_index('ix_lineage_source', 'lineage_edges', ['source_type', 'source_id'])
    op.create_index('ix_lineage_target', 'lineage_edges', ['target_type', 'target_id'])
    op.create_index('ix_lineage_edge_type', 'lineage_edges', ['edge_type'])

    # ==========================================================================
    # Query Executions
    # ==========================================================================
    op.create_table(
        'query_executions',
        sa.Column('id', sa.String(64), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('definition_id', sa.String(128), nullable=True),
        sa.Column('version', sa.String(32), nullable=True),
        sa.Column('sql_hash', sa.String(64), nullable=False),
        sa.Column('sql_text', sa.Text(), nullable=False),
        sa.Column('params_json', postgresql.JSONB(), nullable=False, server_default='{}'),
        sa.Column('status', sa.String(32), nullable=False, server_default='pending'),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('row_count', sa.Float(), nullable=True),
        sa.Column('execution_time_ms', sa.Float(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id'),
    )
    op.create_index('ix_query_exec_tenant', 'query_executions', ['tenant_id'])
    op.create_index('ix_query_exec_definition', 'query_executions', ['definition_id'])
    op.create_index('ix_query_exec_status', 'query_executions', ['status'])
    op.create_index('ix_query_exec_hash', 'query_executions', ['sql_hash'])

    # ==========================================================================
    # Consistency Checks
    # ==========================================================================
    op.create_table(
        'consistency_checks',
        sa.Column('id', sa.String(64), nullable=False),
        sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'),
        sa.Column('check_type', sa.String(64), nullable=False),
        sa.Column('status', sa.String(32), nullable=False, server_default='passed'),
        sa.Column('issues_json', postgresql.JSONB(), nullable=False, server_default='[]'),
        sa.Column('summary', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id', 'tenant_id'),
    )
    op.create_index('ix_consistency_tenant', 'consistency_checks', ['tenant_id'])
    op.create_index('ix_consistency_type', 'consistency_checks', ['check_type'])
    op.create_index('ix_consistency_status', 'consistency_checks', ['status'])


def downgrade() -> None:
    op.drop_table('consistency_checks')
    op.drop_table('query_executions')
    op.drop_table('lineage_edges')
    op.drop_table('proof_hooks')
    op.drop_table('definition_versions')
    op.drop_table('definitions')
    op.drop_table('bindings')
    op.drop_table('entities')
    op.drop_table('canonical_events')
