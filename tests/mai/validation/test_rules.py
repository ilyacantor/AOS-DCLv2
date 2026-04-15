"""
Unit tests for Maestra Layer 0 validation rules.

Covers V-001 through V-007, multiple simultaneous violations,
and the reprompt loop (success + exhaustion).
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal

import pytest

import sys
import os

# Ensure src/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from maestra.validation.schema import (
    FinancialOutput,
    Flag,
    JournalEntry,
    JournalLine,
    LineItem,
)
from maestra.validation.rules import (
    v001_journal_balance,
    v002_element_matches_coa,
    v003_accounting_equation,
    v004_elimination_balance,
    v005_sign_convention,
    v006_element_present,
    v007_period_start,
    validate,
)
from maestra.validation.seed_coa import CoALookup
from maestra.validation.reprompt import reprompt_loop


def _run_async(coro):
    """Run an async coroutine from sync code, even when another event loop is active (e.g. Playwright)."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_bs(
    line_items: list[LineItem] | None = None,
    journal_entries: list[JournalEntry] | None = None,
    period_start: date | None = None,
) -> FinancialOutput:
    """Helper to build a balance sheet FinancialOutput."""
    return FinancialOutput(
        statement_type="balance_sheet",
        entity_id="test-entity",
        period_end=date(2025, 12, 31),
        period_start=period_start,
        currency="USD",
        line_items=line_items or [],
        journal_entries=journal_entries or [],
        flags=[],
    )


def _make_is(
    line_items: list[LineItem] | None = None,
    journal_entries: list[JournalEntry] | None = None,
    period_start: date | None = date(2025, 1, 1),
) -> FinancialOutput:
    """Helper to build an income statement FinancialOutput."""
    return FinancialOutput(
        statement_type="income_statement",
        entity_id="test-entity",
        period_end=date(2025, 12, 31),
        period_start=period_start,
        currency="USD",
        line_items=line_items or [],
        journal_entries=journal_entries or [],
        flags=[],
    )


def _make_line(
    account_code: str = "1000",
    account_name: str = "Cash",
    element: str = "asset",
    natural_balance: str = "debit",
    amount: Decimal = Decimal("100000"),
    source: str = "entity_a",
) -> LineItem:
    return LineItem(
        account_code=account_code,
        account_name=account_name,
        element=element,
        natural_balance=natural_balance,
        amount=amount,
        source=source,
    )


def _make_journal(
    entry_id: str = "JE-001",
    description: str = "Test entry",
    lines: list[JournalLine] | None = None,
) -> JournalEntry:
    return JournalEntry(
        entry_id=entry_id,
        description=description,
        lines=lines or [],
    )


def _make_coa(mapping: dict[str, str]) -> CoALookup:
    """Build a CoALookup from {account_code: element} dict."""
    coa = CoALookup()
    records = [
        {"account_code": code, "element": elem}
        for code, elem in mapping.items()
    ]
    coa.seed_from_records("test-entity", records)
    return coa


# ── V-001: Journal Entry Balance ────────────────────────────────────────────


class TestV001:
    def test_balanced_entry(self):
        """DR=CR passes."""
        entry = _make_journal(lines=[
            JournalLine(account_code="1000", element="asset", debit=Decimal("5000"), credit=Decimal("0")),
            JournalLine(account_code="2000", element="liability", debit=Decimal("0"), credit=Decimal("5000")),
        ])
        output = _make_bs(journal_entries=[entry])
        errors = v001_journal_balance(output)
        assert errors == []

    def test_unbalanced_entry(self):
        """DR!=CR fails with exact variance."""
        entry = _make_journal(lines=[
            JournalLine(account_code="1000", element="asset", debit=Decimal("5000"), credit=Decimal("0")),
            JournalLine(account_code="2000", element="liability", debit=Decimal("0"), credit=Decimal("4999.50")),
        ])
        output = _make_bs(journal_entries=[entry])
        errors = v001_journal_balance(output)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-001"
        assert errors[0].severity == "halt"
        assert errors[0].variance == Decimal("0.50")
        assert "5000" in errors[0].message
        assert "4999.50" in errors[0].message


# ── V-002: Element Matches CoA ──────────────────────────────────────────────


class TestV002:
    def test_correct_element(self):
        """Element matches CoA — passes."""
        coa = _make_coa({"1000": "asset", "4000": "revenue"})
        output = _make_bs(line_items=[
            _make_line(account_code="1000", element="asset"),
        ])
        errors = v002_element_matches_coa(output, coa)
        assert errors == []

    def test_wrong_element(self):
        """Element doesn't match CoA — fails with account_code and conflicting element."""
        coa = _make_coa({"1000": "asset"})
        output = _make_bs(line_items=[
            _make_line(account_code="1000", element="revenue"),
        ])
        errors = v002_element_matches_coa(output, coa)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-002"
        assert errors[0].severity == "halt"
        assert errors[0].failing_data["account_code"] == "1000"
        assert errors[0].failing_data["claimed_element"] == "revenue"
        assert errors[0].failing_data["expected_element"] == "asset"

    def test_empty_coa(self):
        """CoA table empty — halt-level error."""
        coa = CoALookup()  # empty
        output = _make_bs(line_items=[
            _make_line(account_code="1000", element="asset"),
        ])
        errors = v002_element_matches_coa(output, coa)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-002"
        assert errors[0].severity == "halt"
        assert "empty" in errors[0].message.lower()


# ── V-003: Accounting Equation ──────────────────────────────────────────────


class TestV003:
    def test_balanced_bs(self):
        """A = L + E passes."""
        output = _make_bs(line_items=[
            _make_line(account_code="1000", element="asset", amount=Decimal("1500000")),
            _make_line(account_code="2000", element="liability", amount=Decimal("1000000")),
            _make_line(account_code="3000", element="equity", amount=Decimal("500000")),
        ])
        errors = v003_accounting_equation(output)
        assert errors == []

    def test_unbalanced_bs(self):
        """A != L + E fails with exact variance."""
        output = _make_bs(line_items=[
            _make_line(account_code="1000", element="asset", amount=Decimal("1500000")),
            _make_line(account_code="2000", element="liability", amount=Decimal("1000000")),
            _make_line(account_code="3000", element="equity", amount=Decimal("497500")),
        ])
        errors = v003_accounting_equation(output)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-003"
        assert errors[0].variance == Decimal("2500")
        assert "1500000" in errors[0].message
        assert "1497500" in errors[0].message

    def test_skipped_for_is(self):
        """V-003 does not run on income statements."""
        output = _make_is(line_items=[
            _make_line(account_code="4000", element="revenue", natural_balance="credit", amount=Decimal("100000")),
        ])
        errors = v003_accounting_equation(output)
        assert errors == []


# ── V-004: Elimination Balance ──────────────────────────────────────────────


class TestV004:
    def test_elimination_nets_zero(self):
        """Elimination entries net to zero — passes."""
        output = _make_bs(line_items=[
            _make_line(account_code="1100", element="asset", natural_balance="debit",
                       amount=Decimal("50000"), source="elimination"),
            _make_line(account_code="4000", element="revenue", natural_balance="credit",
                       amount=Decimal("50000"), source="elimination"),
        ])
        errors = v004_elimination_balance(output)
        assert errors == []

    def test_elimination_residual(self):
        """Elimination entries don't net — fails with residual."""
        output = _make_bs(line_items=[
            _make_line(account_code="1100", element="asset", natural_balance="debit",
                       amount=Decimal("50000"), source="elimination"),
            _make_line(account_code="4000", element="revenue", natural_balance="credit",
                       amount=Decimal("48000"), source="elimination"),
        ])
        errors = v004_elimination_balance(output)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-004"
        assert errors[0].variance == Decimal("2000")


# ── V-005: Sign Convention ──────────────────────────────────────────────────


class TestV005:
    def test_correct_sign(self):
        """Natural debit balance is positive — passes."""
        output = _make_bs(line_items=[
            _make_line(account_code="1000", element="asset", natural_balance="debit",
                       amount=Decimal("100000")),
        ])
        errors = v005_sign_convention(output)
        assert errors == []

    def test_wrong_sign(self):
        """Negative amount — warning, not halt."""
        output = _make_bs(line_items=[
            _make_line(account_code="1000", element="asset", natural_balance="debit",
                       amount=Decimal("-5000")),
        ])
        errors = v005_sign_convention(output)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-005"
        assert errors[0].severity == "warning"


# ── V-006: Null Element ─────────────────────────────────────────────────────


class TestV006:
    def test_null_element(self):
        """Line item with null element fails.

        Since Pydantic Literal enforces element at model creation,
        we test V-006 by constructing the object with model_construct
        to bypass validation (simulating upstream bypass).
        """
        item = LineItem.model_construct(
            account_code="9999",
            account_name="Unknown",
            element=None,
            natural_balance="debit",
            amount=Decimal("1000"),
            source="entity_a",
        )
        output = _make_bs(line_items=[item])
        errors = v006_element_present(output)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-006"
        assert errors[0].severity == "halt"


# ── V-007: Period Start ─────────────────────────────────────────────────────


class TestV007:
    def test_is_has_period_start(self):
        """IS with period_start — passes."""
        output = _make_is(period_start=date(2025, 1, 1))
        errors = v007_period_start(output)
        assert errors == []

    def test_is_missing_period_start(self):
        """IS without period_start — fails."""
        output = _make_is(period_start=None)
        errors = v007_period_start(output)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-007"
        assert errors[0].severity == "halt"

    def test_bs_has_no_period_start(self):
        """BS with null period_start — passes."""
        output = _make_bs(period_start=None)
        errors = v007_period_start(output)
        assert errors == []

    def test_bs_has_period_start(self):
        """BS with period_start set — fails."""
        output = _make_bs(period_start=date(2025, 1, 1))
        errors = v007_period_start(output)
        assert len(errors) == 1
        assert errors[0].rule_code == "V-007"
        assert errors[0].severity == "halt"


# ── Multiple Violations ─────────────────────────────────────────────────────


class TestMultipleViolations:
    def test_multiple_violations(self):
        """Output with 3+ simultaneous violations — all reported."""
        # Violation 1: V-001 — unbalanced journal
        unbalanced_je = _make_journal(lines=[
            JournalLine(account_code="1000", element="asset",
                        debit=Decimal("1000"), credit=Decimal("0")),
            JournalLine(account_code="2000", element="liability",
                        debit=Decimal("0"), credit=Decimal("999")),
        ])

        # Violation 2: V-003 — A != L + E
        # Violation 3: V-005 — wrong sign
        output = _make_bs(
            line_items=[
                _make_line(account_code="1000", element="asset",
                           amount=Decimal("100000")),
                _make_line(account_code="2000", element="liability",
                           amount=Decimal("90000")),
                _make_line(account_code="3000", element="equity",
                           amount=Decimal("-5000")),  # V-005 warning + V-003 imbalance
            ],
            journal_entries=[unbalanced_je],
        )

        coa = _make_coa({
            "1000": "asset",
            "2000": "liability",
            "3000": "equity",
        })

        result = validate(output, coa=coa)
        assert not result.valid

        rule_codes = [e.rule_code for e in result.errors]
        assert "V-001" in rule_codes  # unbalanced journal
        assert "V-003" in rule_codes  # accounting equation
        assert "V-005" in rule_codes  # sign convention


# ── Reprompt Loop ────────────────────────────────────────────────────────────


class TestRepromptLoop:
    def test_reprompt_succeeds(self):
        """Mock agent fails once then succeeds — verify reprompt works."""
        call_count = 0

        async def mock_agent(prompt: str) -> FinancialOutput:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First attempt: unbalanced BS
                return _make_bs(line_items=[
                    _make_line(account_code="1000", element="asset",
                               amount=Decimal("100000")),
                    _make_line(account_code="2000", element="liability",
                               amount=Decimal("99000")),
                    _make_line(account_code="3000", element="equity",
                               amount=Decimal("500")),
                ])
            else:
                # Second attempt: balanced BS
                return _make_bs(line_items=[
                    _make_line(account_code="1000", element="asset",
                               amount=Decimal("100000")),
                    _make_line(account_code="2000", element="liability",
                               amount=Decimal("60000")),
                    _make_line(account_code="3000", element="equity",
                               amount=Decimal("40000")),
                ])

        output, results = _run_async(
            reprompt_loop(mock_agent, "Generate BS", max_attempts=3)
        )

        assert output is not None
        assert len(results) == 2
        assert not results[0].valid
        assert results[1].valid
        assert call_count == 2

    def test_reprompt_exhausted(self):
        """Mock agent fails 3 times — verify halt."""

        async def mock_agent(prompt: str) -> FinancialOutput:
            # Always return unbalanced BS
            return _make_bs(line_items=[
                _make_line(account_code="1000", element="asset",
                           amount=Decimal("100000")),
                _make_line(account_code="2000", element="liability",
                           amount=Decimal("50000")),
                _make_line(account_code="3000", element="equity",
                           amount=Decimal("10000")),
            ])

        output, results = _run_async(
            reprompt_loop(mock_agent, "Generate BS", max_attempts=3)
        )

        assert output is None
        assert len(results) == 3
        assert all(not r.valid for r in results)
