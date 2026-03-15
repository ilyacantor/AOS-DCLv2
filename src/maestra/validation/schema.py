"""
Maestra Layer 0 — Financial output schema and validation result types.

All monetary values use Decimal. Never float.
"""

from datetime import date
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, field_validator


class LineItem(BaseModel):
    account_code: str
    account_name: str
    element: Literal["asset", "liability", "equity", "revenue", "expense"]
    natural_balance: Literal["debit", "credit"]
    amount: Decimal
    source: Literal["entity_a", "entity_b", "elimination", "adjustment"]


class JournalLine(BaseModel):
    account_code: str
    element: Literal["asset", "liability", "equity", "revenue", "expense"]
    debit: Decimal
    credit: Decimal

    @field_validator("debit", "credit")
    @classmethod
    def non_negative(cls, v: Decimal, info) -> Decimal:
        if v < 0:
            raise ValueError(f"{info.field_name} must be >= 0, got {v}")
        return v


class JournalEntry(BaseModel):
    entry_id: str
    description: str
    lines: list[JournalLine]


class Flag(BaseModel):
    severity: Literal["halt", "warning"]
    code: str
    message: str
    affected_accounts: list[str]


class FinancialOutput(BaseModel):
    statement_type: Literal["income_statement", "balance_sheet"]
    entity_id: str
    period_end: date
    period_start: Optional[date] = None
    currency: str
    line_items: list[LineItem]
    journal_entries: list[JournalEntry]
    flags: list[Flag]


class ValidationError(BaseModel):
    rule_code: str
    message: str
    severity: Literal["halt", "warning"]
    failing_data: dict
    variance: Optional[Decimal] = None


class ValidationResult(BaseModel):
    valid: bool
    errors: list[ValidationError]
