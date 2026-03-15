"""
COFA Completeness Gate
======================
Validates that a COFA mapping covers every source account before accepting it.
This is a deterministic gate — Maestra cannot bypass it.

If incomplete, returns the orphan list so Maestra can be told exactly which
accounts she missed. She gets a second attempt with specific feedback.
"""

from typing import Any


class COFACompletionGate:
    """Validates that a COFA mapping covers every source account."""

    def validate_mapping_completeness(
        self,
        source_coa: list[dict[str, Any]],
        mapping_entries: list[dict[str, Any]],
        source_key: str = "account_number",
    ) -> dict[str, Any]:
        """Check that every account in source_coa has a mapping.

        Args:
            source_coa: List of source accounts, each with at minimum
                        'account_number' and 'account_name'.
            mapping_entries: The COFA mapping entries Maestra produced.
                             Each entry may reference source accounts via
                             'entity_a_account_number', 'entity_b_account_number',
                             'source_account_number', or 'source_account'.
            source_key: Field name for the account identifier in source_coa.

        Returns:
            {
                "complete": True/False,
                "source_count": N,
                "mapped_count": N,
                "orphaned_accounts": [{account_number, account_name}, ...],
                "message": "PASS: all N accounts mapped" or
                           "FAIL: N account(s) not mapped — see orphaned_accounts"
            }
        """
        # Extract account identifiers from source CoA
        source_accounts = set()
        source_lookup: dict[str, dict] = {}
        for acct in source_coa:
            key = str(acct.get(source_key, "")).strip()
            if key:
                source_accounts.add(key)
                source_lookup[key] = acct

        # Extract mapped account identifiers from mapping entries
        mapped_accounts = set()
        # Support multiple field naming conventions
        mapping_fields = [
            "entity_a_account_number",
            "entity_b_account_number",
            "source_account_number",
            "source_account",
            "account_number",
            "unified_account_number",
        ]
        for entry in mapping_entries:
            for field_name in mapping_fields:
                val = str(entry.get(field_name, "")).strip()
                if val:
                    mapped_accounts.add(val)
            # Also check if account number is encoded in a concept name
            concept = str(entry.get("concept", ""))
            if concept.startswith("cofa.mapping."):
                acct_ref = concept.split("cofa.mapping.")[-1]
                if acct_ref:
                    mapped_accounts.add(acct_ref)

        # Compute orphans
        orphaned = source_accounts - mapped_accounts
        orphaned_details = [
            {
                "account_number": source_lookup[k].get("account_number", k),
                "account_name": source_lookup[k].get("account_name", ""),
            }
            for k in sorted(orphaned)
            if k in source_lookup
        ]

        source_count = len(source_accounts)
        mapped_count = source_count - len(orphaned)

        return {
            "complete": len(orphaned) == 0,
            "source_count": source_count,
            "mapped_count": mapped_count,
            "orphaned_accounts": orphaned_details,
            "message": (
                f"PASS: all {source_count} accounts mapped"
                if len(orphaned) == 0
                else (
                    f"FAIL: {len(orphaned)} account(s) not mapped"
                    f" — see orphaned_accounts"
                )
            ),
        }

    def validate_and_reject(
        self,
        source_coa: list[dict[str, Any]],
        mapping_entries: list[dict[str, Any]],
        source_key: str = "account_number",
    ) -> dict[str, Any]:
        """Validate and return rejection detail if incomplete.

        If incomplete, returns the orphan list so Maestra can be told
        exactly which accounts she missed. She gets a second attempt
        with specific feedback.
        """
        result = self.validate_mapping_completeness(
            source_coa, mapping_entries, source_key
        )

        if not result["complete"]:
            orphan_names = [
                f"{a['account_number']} {a['account_name']}"
                for a in result["orphaned_accounts"]
            ]
            result["rejection_message"] = (
                f"COFA mapping rejected: {len(result['orphaned_accounts'])} "
                f"source account(s) not mapped. Missing: "
                f"{', '.join(orphan_names)}. "
                f"Re-run mapping to include these accounts."
            )

        return result
