"""
Mai CoA Seeding — populates a Chart of Accounts lookup table.

Maps each account_code to its element (asset, liability, equity, revenue, expense).
The lookup is used by validation rule V-002 to verify element classification.

Data source: Farm's ingest output in Supabase PostgreSQL.
"""

import logging
from typing import Literal

logger = logging.getLogger(__name__)

Element = Literal["asset", "liability", "equity", "revenue", "expense"]


class CoALookup:
    """In-memory Chart of Accounts lookup table.

    Populated from Farm's chart_of_accounts data via seed_from_supabase()
    or directly via seed_from_records() for testing.
    """

    def __init__(self) -> None:
        self._table: dict[str, Element] = {}
        self._entity_id: str | None = None

    @property
    def entity_id(self) -> str | None:
        return self._entity_id

    def is_empty(self) -> bool:
        return len(self._table) == 0

    def get_element(self, account_code: str) -> Element | None:
        return self._table.get(account_code)

    def has_account(self, account_code: str) -> bool:
        return account_code in self._table

    def all_accounts(self) -> dict[str, Element]:
        return dict(self._table)

    def seed_from_records(
        self,
        entity_id: str,
        records: list[dict],
    ) -> None:
        """Populate the lookup table from a list of dicts.

        Each dict must have 'account_code' and 'element' keys.
        Idempotent: clears existing data for this entity before seeding.

        Raises:
            ValueError: If no records provided for entity.
            ValueError: If a record is missing required fields.
        """
        if not records:
            raise ValueError(
                f"No Chart of Accounts available for entity {entity_id}. "
                f"Cannot validate element boundaries."
            )

        valid_elements = {"asset", "liability", "equity", "revenue", "expense"}

        # Clear existing data for idempotency
        self._table.clear()
        self._entity_id = entity_id

        for i, record in enumerate(records):
            account_code = record.get("account_code")
            element = record.get("element")

            if not account_code:
                raise ValueError(
                    f"CoA record {i} for entity {entity_id} missing 'account_code': {record}"
                )
            if not element:
                raise ValueError(
                    f"CoA record {i} for entity {entity_id} missing 'element': {record}"
                )
            if element not in valid_elements:
                raise ValueError(
                    f"CoA record {i} for entity {entity_id} has invalid element "
                    f"'{element}' for account_code '{account_code}'. "
                    f"Valid elements: {valid_elements}"
                )

            self._table[account_code] = element

        logger.info(
            "CoA lookup seeded for entity %s: %d accounts loaded",
            entity_id,
            len(self._table),
        )

    def seed_from_supabase(
        self,
        entity_id: str,
        supabase_url: str,
        supabase_key: str,
        table_name: str = "chart_of_accounts",
    ) -> None:
        """Populate the lookup table from Supabase PostgreSQL.

        Queries the chart_of_accounts table for records matching entity_id,
        then delegates to seed_from_records.

        Raises:
            ValueError: If no CoA data exists for entity.
            RuntimeError: If Supabase connection fails.
        """
        try:
            from supabase import create_client
        except ImportError as e:
            raise RuntimeError(
                f"supabase-py is required for CoA seeding from Supabase. "
                f"Install it in src/mai/requirements.txt. Import error: {e}"
            ) from e

        try:
            client = create_client(supabase_url, supabase_key)
        except Exception as e:
            raise RuntimeError(
                f"Failed to connect to Supabase at {supabase_url} "
                f"for CoA seeding of entity {entity_id}: {e}"
            ) from e

        try:
            response = (
                client.table(table_name)
                .select("account_code, element")
                .eq("entity_id", entity_id)
                .execute()
            )
        except Exception as e:
            raise RuntimeError(
                f"Supabase query failed for CoA data — "
                f"table={table_name}, entity_id={entity_id}: {e}"
            ) from e

        records = response.data if response.data else []

        # seed_from_records will raise ValueError if records is empty
        self.seed_from_records(entity_id, records)
