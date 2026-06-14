"""NormalizationPolicyStore — data access for tenant_normalization_policy
(Migration 028).

The write-time value normalizer loads a tenant's canonical currency + FX rate
book ONCE per ingest call and passes it to backend.resolver.value_normalizer.
This store does no business logic — normalization math lives in the resolver.

The '*' fallback mirrors backend/db/conflict_store.py ConflictStore.load_policy
exactly: tenant row else the '*' default, one query, LIMIT 1.
"""

import json
from typing import Any

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class NormalizationPolicyStore:

    def load_policy(self, tenant_id: str) -> dict[str, Any]:
        """Effective normalization policy: the tenant's own row else the '*'
        default. Returns {"canonical_currency": str, "fx_rates": dict}.

        Raises RuntimeError if neither the tenant nor the '*' default exists —
        migration 028 seeds '*', so a missing default is a broken store, not a
        silently-tolerated empty policy (A1)."""
        sql = """
            SELECT tenant_id, canonical_currency, fx_rates
            FROM tenant_normalization_policy
            WHERE tenant_id IN ('*', %s)
            ORDER BY CASE WHEN tenant_id = '*' THEN 1 ELSE 0 END
            LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (str(tenant_id),))
                row = cur.fetchone()
        if row is None:
            raise RuntimeError(
                "tenant_normalization_policy has no '*' default row — migration "
                "028 seeds it; the policy table is required for value "
                "normalization at ingest."
            )
        fx = row[2]
        # psycopg2 returns JSONB as a parsed dict already; tolerate a raw str
        # if the adapter is not registered for this connection.
        if isinstance(fx, str):
            fx = json.loads(fx)
        return {
            "policy_tenant": row[0],
            "canonical_currency": row[1],
            "fx_rates": fx or {},
        }

    def put_policy(
        self, tenant_id: str, canonical_currency: str, fx_rates: dict,
    ) -> None:
        """Upsert a tenant's normalization policy. Used by tests/operators to
        configure a non-USD canonical or an FX rate book."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tenant_normalization_policy
                        (tenant_id, canonical_currency, fx_rates)
                    VALUES (%s, %s, %s::jsonb)
                    ON CONFLICT (tenant_id) DO UPDATE SET
                        canonical_currency = EXCLUDED.canonical_currency,
                        fx_rates = EXCLUDED.fx_rates,
                        updated_at = now()
                    """,
                    (str(tenant_id), canonical_currency, json.dumps(fx_rates or {})),
                )
                conn.commit()
