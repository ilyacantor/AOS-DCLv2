"""
Mai Layer 0 — Deterministic validation rules.

Each rule is a standalone function that takes a FinancialOutput and returns
a list of ValidationError. The validate() orchestrator runs all rules and
returns a ValidationResult.

Rules:
    V-001: Journal entry debit/credit balance (zero tolerance)
    V-002: Line item element matches CoA lookup
    V-003: Balance sheet accounting equation (A = L + E)
    V-004: Elimination entries net to zero
    V-005: Line item sign matches natural balance direction (warning)
    V-006: Every line item has a non-null element
    V-007: Period start presence based on statement type
"""

from decimal import Decimal

from .schema import FinancialOutput, ValidationError, ValidationResult
from .seed_coa import CoALookup


def v001_journal_balance(output: FinancialOutput) -> list[ValidationError]:
    """V-001: For each journal entry, sum(debit) must equal sum(credit). Zero tolerance."""
    errors = []
    for entry in output.journal_entries:
        total_debit = sum(line.debit for line in entry.lines)
        total_credit = sum(line.credit for line in entry.lines)
        if total_debit != total_credit:
            variance = total_debit - total_credit
            errors.append(ValidationError(
                rule_code="V-001",
                message=(
                    f"Journal entry '{entry.entry_id}' is unbalanced: "
                    f"total_debit={total_debit}, total_credit={total_credit}, "
                    f"variance={variance}"
                ),
                severity="halt",
                failing_data={
                    "entry_id": entry.entry_id,
                    "total_debit": str(total_debit),
                    "total_credit": str(total_credit),
                },
                variance=abs(variance),
            ))
    return errors


def v002_element_matches_coa(
    output: FinancialOutput, coa: CoALookup
) -> list[ValidationError]:
    """V-002: Each line item's element must match the CoA lookup for that account_code."""
    errors = []

    if coa.is_empty():
        errors.append(ValidationError(
            rule_code="V-002",
            message=(
                f"CoA lookup table is empty for entity '{output.entity_id}'. "
                f"Cannot validate element boundaries. "
                f"Run seed_coa.py before validation."
            ),
            severity="halt",
            failing_data={"entity_id": output.entity_id},
        ))
        return errors

    for item in output.line_items:
        expected_element = coa.get_element(item.account_code)
        if expected_element is None:
            errors.append(ValidationError(
                rule_code="V-002",
                message=(
                    f"Account '{item.account_code}' not found in CoA lookup "
                    f"for entity '{output.entity_id}'. Cannot verify element."
                ),
                severity="halt",
                failing_data={
                    "account_code": item.account_code,
                    "claimed_element": item.element,
                    "entity_id": output.entity_id,
                },
            ))
        elif item.element != expected_element:
            errors.append(ValidationError(
                rule_code="V-002",
                message=(
                    f"Account '{item.account_code}' has element '{item.element}' "
                    f"but CoA says '{expected_element}'."
                ),
                severity="halt",
                failing_data={
                    "account_code": item.account_code,
                    "claimed_element": item.element,
                    "expected_element": expected_element,
                },
            ))
    return errors


def v003_accounting_equation(output: FinancialOutput) -> list[ValidationError]:
    """V-003: For balance sheets, sum(assets) == sum(liabilities) + sum(equity)."""
    if output.statement_type != "balance_sheet":
        return []

    assets = sum(
        item.amount for item in output.line_items if item.element == "asset"
    )
    liabilities = sum(
        item.amount for item in output.line_items if item.element == "liability"
    )
    equity = sum(
        item.amount for item in output.line_items if item.element == "equity"
    )

    if assets != liabilities + equity:
        variance = assets - (liabilities + equity)
        return [ValidationError(
            rule_code="V-003",
            message=(
                f"Balance sheet doesn't balance: "
                f"assets={assets}, liabilities+equity={liabilities + equity}, "
                f"variance={variance}"
            ),
            severity="halt",
            failing_data={
                "assets": str(assets),
                "liabilities": str(liabilities),
                "equity": str(equity),
                "liabilities_plus_equity": str(liabilities + equity),
            },
            variance=abs(variance),
        )]
    return []


def v004_elimination_balance(output: FinancialOutput) -> list[ValidationError]:
    """V-004: Elimination entries must net to zero (sum debits == sum credits)."""
    elimination_items = [
        item for item in output.line_items if item.source == "elimination"
    ]
    if not elimination_items:
        return []

    # For elimination line items, debit-natured amounts should sum equal to credit-natured
    # We check via journal entries tagged as eliminations
    # But the spec says "each set of elimination entries" from line_items
    # Use signed amounts: debits are positive for debit-natural, credits positive for credit-natural
    # Actually, the spec says sum(debits) == sum(credits) for elimination entries.
    # Line items have amounts (signed) and natural_balance.
    # We interpret: group by natural_balance, sum amounts for each side.
    total_debit = sum(
        item.amount for item in elimination_items if item.natural_balance == "debit"
    )
    total_credit = sum(
        item.amount for item in elimination_items if item.natural_balance == "credit"
    )

    # Elimination entries should net to zero: total debit-sided amounts == total credit-sided amounts
    if total_debit != total_credit:
        residual = total_debit - total_credit
        return [ValidationError(
            rule_code="V-004",
            message=(
                f"Elimination entries do not net to zero: "
                f"debit_total={total_debit}, credit_total={total_credit}, "
                f"residual={residual}"
            ),
            severity="halt",
            failing_data={
                "debit_total": str(total_debit),
                "credit_total": str(total_credit),
                "elimination_count": len(elimination_items),
            },
            variance=abs(residual),
        )]
    return []


def v005_sign_convention(output: FinancialOutput) -> list[ValidationError]:
    """V-005: Line item sign should match natural balance direction. Warning only."""
    errors = []
    for item in output.line_items:
        # Natural debit accounts (assets, expenses) should have positive amounts
        # Natural credit accounts (liabilities, equity, revenue) should have positive amounts
        # "Matches natural balance direction" means:
        #   - debit natural_balance → amount >= 0
        #   - credit natural_balance → amount >= 0
        # A negative amount means contra to natural balance
        if item.amount < 0:
            errors.append(ValidationError(
                rule_code="V-005",
                message=(
                    f"Account '{item.account_code}' ({item.account_name}) has "
                    f"amount={item.amount} which is contra to its "
                    f"natural_balance='{item.natural_balance}'. "
                    f"Review for correctness — not auto-corrected."
                ),
                severity="warning",
                failing_data={
                    "account_code": item.account_code,
                    "account_name": item.account_name,
                    "amount": str(item.amount),
                    "natural_balance": item.natural_balance,
                },
            ))
    return errors


def v006_element_present(output: FinancialOutput) -> list[ValidationError]:
    """V-006: Every line item must have a non-null element.

    Note: With Pydantic's Literal type, element can never be None at runtime
    if the model validates. This rule catches cases where upstream code
    might bypass Pydantic validation or use raw dicts.
    """
    errors = []
    for item in output.line_items:
        if not item.element:
            errors.append(ValidationError(
                rule_code="V-006",
                message=(
                    f"Line item '{item.account_code}' has null/empty element."
                ),
                severity="halt",
                failing_data={
                    "account_code": item.account_code,
                    "account_name": item.account_name,
                    "element": str(item.element),
                },
            ))
    return errors


def v007_period_start(output: FinancialOutput) -> list[ValidationError]:
    """V-007: IS requires period_start; BS requires period_start to be null."""
    if output.statement_type == "income_statement" and output.period_start is None:
        return [ValidationError(
            rule_code="V-007",
            message=(
                "Income statement is missing period_start. "
                "Income statements require both period_start and period_end "
                "to define the reporting period."
            ),
            severity="halt",
            failing_data={
                "statement_type": output.statement_type,
                "period_start": None,
                "period_end": str(output.period_end),
            },
        )]
    if output.statement_type == "balance_sheet" and output.period_start is not None:
        return [ValidationError(
            rule_code="V-007",
            message=(
                f"Balance sheet has period_start={output.period_start} but "
                f"balance sheets are point-in-time and must have null period_start."
            ),
            severity="halt",
            failing_data={
                "statement_type": output.statement_type,
                "period_start": str(output.period_start),
                "period_end": str(output.period_end),
            },
        )]
    return []


def validate(
    output: FinancialOutput,
    coa: CoALookup | None = None,
) -> ValidationResult:
    """Run all validation rules and return a ValidationResult.

    Args:
        output: The financial output to validate.
        coa: CoA lookup table for V-002. If None, V-002 is skipped
             (caller must ensure CoA is available for production use).

    Returns:
        ValidationResult with valid=True only if zero halt-level errors.
    """
    all_errors: list[ValidationError] = []

    # Run all rules
    all_errors.extend(v001_journal_balance(output))

    if coa is not None:
        all_errors.extend(v002_element_matches_coa(output, coa))

    all_errors.extend(v003_accounting_equation(output))
    all_errors.extend(v004_elimination_balance(output))
    all_errors.extend(v005_sign_convention(output))
    all_errors.extend(v006_element_present(output))
    all_errors.extend(v007_period_start(output))

    has_halt = any(e.severity == "halt" for e in all_errors)

    return ValidationResult(
        valid=not has_halt,
        errors=all_errors,
    )
