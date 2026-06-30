"""Write-time value normalizer — pure, deterministic, no DB.

Every TriplePayload is normalized to the tenant canonical (USD, base unit, one
period representation) at the single ingest persist chokepoint, just before the
row is written, with the raw original preserved in ``normalization_metadata``.
Conflict detection then compares values that are already in one shape — so a
cross-source gap is the REAL gap, never an artifact of one source reporting in
thousands and another in dollars, or one in EUR and another in USD, or one
period spelled "Mar-2026" and another "2026-03".

Fail-loud contract (no silent fallback — constitution A1):
  - An UNKNOWN unit on a NUMERIC value raises ValueError. We refuse to write a
    number whose scale we cannot place: silently treating "furlongs" as base
    would let an unconverted value masquerade as canonical and either hide a
    real conflict or invent a spurious one.
  - A non-empty period that cannot be parsed raises ValueError. An unparsed
    period that *should* have matched another source's period would split one
    coordinate group into two single-source groups and hide a real conflict.
  - A non-USD currency with NO configured FX rate raises ValueError. We refuse
    to compare across currencies we cannot convert — assuming parity is a
    silent fallback that corrupts every downstream materiality number.

Non-numeric values (JSON objects, strings, booleans) pass through unit and
currency steps unchanged — scaling/converting a non-number is meaningless, not
an error. Period canonicalization still applies (it is representation-only).
"""

from __future__ import annotations

import re
from typing import Any, Optional, Tuple


# ---------------------------------------------------------------------------
# Unit-scale table — multiply-to-base factor, matched case-insensitively.
# Base units (factor 1.0) are already in the smallest meaningful unit; scaled
# units are multiples of the base. The keys are the unit strings as sources
# stamp them; lookup lowercases the incoming unit first.
# ---------------------------------------------------------------------------

_THOUSAND = 1_000.0
_MILLION = 1_000_000.0
_BILLION = 1_000_000_000.0

# Decimal places to round a normalized numeric to, killing float-representation
# noise (100 * 1.10 == 110.00000000000001) so stored canonical values are
# clean and deterministic. Matches conflict_detection's round(..., 10) on
# deltas — lossless for any realistic financial magnitude.
_ROUND_DP = 10


def _round_num(v: Any) -> Any:
    """Round a numeric to _ROUND_DP places; pass non-numerics through."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return v
    return round(float(v), _ROUND_DP)

# unit (as sources stamp it) -> (multiply-to-base factor, canonical base-unit
# name). Lookup lowercases the incoming unit first. Only UNAMBIGUOUS scale
# units are recognized: currency-scoped (usd_thousands/…) and the spelled-out
# words (thousands/millions/billions). Single-letter abbreviations (k/m/b/mm/bn)
# are deliberately NOT recognized — "m" could be metres, "b" bytes; silently
# scaling one of those by 1e6/1e9 would corrupt a value, which A1 forbids. An
# unrecognized unit on a numeric fails loud instead of guessing the scale.
BASE_UNIT_SCALES: dict[Optional[str], Tuple[float, Optional[str]]] = {
    # base (already smallest unit) — factor 1.0, base unit == the unit itself
    "usd": (1.0, "usd"),
    "dollars": (1.0, "dollars"),
    "dollar": (1.0, "dollar"),
    "count": (1.0, "count"),
    "each": (1.0, "each"),
    "percent": (1.0, "percent"),
    "pct": (1.0, "pct"),
    "ratio": (1.0, "ratio"),
    # dimensional base measures — already the smallest meaningful unit, factor
    # 1.0, base unit == the unit itself. These are MEASURES (a duration, a count
    # of points, a score, a throughput), NOT magnitude SCALES — there is no
    # 1000x/1e6 multiplier to guess, so passing them through is exact, not a
    # silent fallback. The records path (operational_records_aggregator: days,
    # hours, points, score) and the fabric/event-stream generators (seconds,
    # messages_per_second) stamp these; the unit is preserved on the stored row
    # so conflict detection compares same-unit values (each concept is
    # single-unit, e.g. sales.cycle_days is always "days"). Currency-scale
    # aliases ("dollars_millions"/"millions_usd") are deliberately NOT here:
    # they occur only in stale shared-tenant fixture artifacts ($M-scale,
    # flagged broken in the constitution) — a numeric in one of those fails
    # loud, which is the correct A1 outcome, not a gap to paper over.
    "days": (1.0, "days"),
    "days_outstanding": (1.0, "days_outstanding"),
    "hours": (1.0, "hours"),
    "seconds": (1.0, "seconds"),
    "score": (1.0, "score"),
    "points": (1.0, "points"),
    "story_points": (1.0, "story_points"),
    "messages_per_second": (1.0, "messages_per_second"),
    # cloud_spend estate per-team OUTPUT + derived efficiency — MEASURES, not
    # magnitude scales (factor 1.0, base == itself; exact pass-through, like the
    # operational measures above). "deploys" is a deploy count;
    # "usd_per_deploys_per_month" is cost ÷ output, a derived rate. No 1000x/1e6
    # multiplier to guess, so this is exact, not a silent fallback (A1).
    "deploys": (1.0, "deploys"),
    "usd_per_deploys_per_month": (1.0, "usd_per_deploys_per_month"),
    None: (1.0, None),
    "": (1.0, ""),
    # currency-scoped scales -> base unit "usd"
    "usd_thousands": (_THOUSAND, "usd"),
    "usd_millions": (_MILLION, "usd"),
    "usd_billions": (_BILLION, "usd"),
    # spelled-out scales naming no measure -> base unit unknown (None). The
    # scale is applied; the raw original is preserved in normalization_metadata.
    "thousands": (_THOUSAND, None),
    "millions": (_MILLION, None),
    "billions": (_BILLION, None),
}


def _unit_key(unit: Optional[str]):
    """Normalize a unit string for table lookup. None/'' map to the None key."""
    if unit is None:
        return None
    s = unit.strip().lower()
    return s if s != "" else ""


def _is_number(value: Any) -> bool:
    """True only for real numerics — bool is excluded (it is not a measure)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def scale_to_base(value: Any, unit: Optional[str]) -> Tuple[Any, float]:
    """Scale a numeric value to its base unit.

    Returns (base_value, factor). Non-numeric values pass through unchanged
    with factor 1.0 (scaling a non-number is meaningless). A numeric value with
    an UNKNOWN unit raises ValueError — we refuse to write a number whose scale
    we cannot place.
    """
    key = _unit_key(unit)
    if key in BASE_UNIT_SCALES:
        factor, _base = BASE_UNIT_SCALES[key]
    else:
        if _is_number(value):
            raise ValueError(
                "unknown unit-scale %r — cannot normalize; configure it or fix "
                "the source" % unit
            )
        # Non-numeric value with an unrecognized unit: nothing to scale.
        return value, 1.0

    if not _is_number(value):
        return value, 1.0
    if factor == 1.0:
        return value, 1.0
    return _round_num(value * factor), factor


# ---------------------------------------------------------------------------
# Period canonicalization — REPRESENTATION normalization only. We map the many
# textual shapes of the same period to ONE shape. We never reinterpret
# fiscal↔calendar (that would change meaning, not representation).
#
# Canonical shapes:
#   month   -> "YYYY-MM"   (e.g. "2026-03")
#   quarter -> "YYYY-QN"   (e.g. "2026-Q1")
#   year    -> "YYYY"      (e.g. "2026")
# ---------------------------------------------------------------------------

_MONTH_NAMES = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

# Numeric year-month: "2026-03", "2026-3", "2026/03", "2026.3"
_RE_YEAR_MONTH = re.compile(r"^(\d{4})[-/.](\d{1,2})$")
# Full date collapsing to its month: "2026-03-01", "2026/03/15"
_RE_YEAR_MONTH_DAY = re.compile(r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$")
# Month-name forms: "Mar-2026", "Mar 2026", "March 2026", "March, 2026"
_RE_NAME_YEAR = re.compile(r"^([A-Za-z]+)[\s,\-]+(\d{4})$")
# Reversed month-name forms: "2026 March", "2026-Mar"
_RE_YEAR_NAME = re.compile(r"^(\d{4})[\s,\-]+([A-Za-z]+)$")
# Quarter forms: "2026-Q1", "2026 Q1", "2026Q1", "Q1 2026", "Q1-2026"
_RE_YEAR_Q = re.compile(r"^(\d{4})[-\s]?Q([1-4])$", re.IGNORECASE)
_RE_Q_YEAR = re.compile(r"^Q([1-4])[-\s]?(\d{4})$", re.IGNORECASE)
# Bare year: "2026"
_RE_YEAR = re.compile(r"^(\d{4})$")


def canon_period(period: Optional[str]) -> Optional[str]:
    """Canonicalize a period's REPRESENTATION to one shape.

    None/empty -> None. A non-empty period that cannot be parsed raises
    ValueError (no silent pass — an unparsed period that should have matched
    another source would hide a real conflict).
    """
    if period is None:
        return None
    s = period.strip()
    if s == "":
        return None

    m = _RE_YEAR_MONTH_DAY.match(s)
    if m:
        return _fmt_month(int(m.group(1)), int(m.group(2)), s)

    m = _RE_YEAR_MONTH.match(s)
    if m:
        return _fmt_month(int(m.group(1)), int(m.group(2)), s)

    m = _RE_NAME_YEAR.match(s)
    if m:
        mon = _MONTH_NAMES.get(m.group(1).lower())
        if mon is not None:
            return _fmt_month(int(m.group(2)), mon, s)

    m = _RE_YEAR_NAME.match(s)
    if m:
        mon = _MONTH_NAMES.get(m.group(2).lower())
        if mon is not None:
            return _fmt_month(int(m.group(1)), mon, s)

    m = _RE_YEAR_Q.match(s)
    if m:
        return f"{int(m.group(1)):04d}-Q{int(m.group(2))}"

    m = _RE_Q_YEAR.match(s)
    if m:
        return f"{int(m.group(2)):04d}-Q{int(m.group(1))}"

    m = _RE_YEAR.match(s)
    if m:
        return f"{int(m.group(1)):04d}"

    raise ValueError(
        "unparseable period %r — cannot canonicalize; an unparsed period that "
        "should match another source would hide a real conflict. Fix the source "
        "format or extend the period parser." % period
    )


def _fmt_month(year: int, month: int, raw: str) -> str:
    if not (1 <= month <= 12):
        raise ValueError(
            "period %r has month %d out of range 1-12 — cannot canonicalize"
            % (raw, month)
        )
    return f"{year:04d}-{month:02d}"


# ---------------------------------------------------------------------------
# Currency conversion. fx_rates shape: {"EUR": 1.08} means 1 EUR = 1.08
# canonical. Value in source currency × rate = value in canonical currency.
# ---------------------------------------------------------------------------

def convert_currency(
    value: Any, currency: Optional[str], canonical_currency: str,
    fx_rates: dict,
) -> Tuple[Any, float]:
    """Convert a numeric value from its currency to the tenant canonical.

    Returns (value_in_canonical, rate). currency None or already canonical ->
    (value, 1.0). A non-canonical currency with NO configured rate raises
    ValueError — we refuse to compare across currencies we cannot convert.
    Non-numeric values pass through unchanged.
    """
    if currency is None or currency == canonical_currency:
        return value, 1.0

    rate = (fx_rates or {}).get(currency)
    if rate is None:
        raise ValueError(
            "no FX rate %s->%s configured — cannot normalize; refusing to "
            "compare across currencies" % (currency, canonical_currency)
        )

    if not _is_number(value):
        return value, float(rate)
    return _round_num(value * float(rate)), float(rate)


# ---------------------------------------------------------------------------
# The one entry point the chokepoint calls.
# ---------------------------------------------------------------------------

def normalize(
    *, value: Any, unit: Optional[str], currency: Optional[str],
    period: Optional[str], policy: dict,
) -> dict:
    """Normalize one value to the tenant canonical. Order: scale unit -> convert
    currency -> canonicalize period.

    ``policy`` is {"canonical_currency": str, "fx_rates": dict}.

    Returns {value, unit, currency, period, metadata}. ``metadata`` is None when
    NOTHING changed (the no-op base/USD/already-canonical case — no row gets a
    metadata stamp it does not need). Otherwise it is a dict capturing ONLY what
    changed: {raw_value, raw_unit, raw_currency, raw_period, scale_factor,
    fx_rate}. The returned unit becomes the base unit, currency the canonical,
    period the canonical representation.
    """
    canonical_currency = policy["canonical_currency"]
    fx_rates = policy.get("fx_rates") or {}

    # 1) scale unit -> base
    base_value, scale_factor = scale_to_base(value, unit)

    # 2) convert currency -> canonical
    conv_value, fx_rate = convert_currency(
        base_value, currency, canonical_currency, fx_rates,
    )

    # 3) canonicalize period representation
    canon = canon_period(period)

    unit_changed = scale_factor != 1.0
    currency_changed = (
        currency is not None and currency != canonical_currency
    )
    period_changed = canon != period and not (canon is None and period is None)

    if not (unit_changed or currency_changed or period_changed):
        # Nothing materially changed. Still return the canonical period when it
        # is byte-identical (it is, by definition here) — value/unit/currency
        # are unchanged, metadata stays None.
        return {
            "value": value,
            "unit": _canonical_unit(unit, unit_changed),
            "currency": currency,
            "period": canon,
            "metadata": None,
        }

    metadata = {
        "raw_value": value,
        "raw_unit": unit,
        "raw_currency": currency,
        "raw_period": period,
        "scale_factor": scale_factor,
        "fx_rate": fx_rate,
    }
    return {
        "value": conv_value,
        "unit": _canonical_unit(unit, unit_changed),
        "currency": canonical_currency if currency_changed else currency,
        "period": canon,
        "metadata": metadata,
    }


def base_unit_for(unit: Optional[str]) -> Optional[str]:
    """The canonical base-unit name for a unit string ('usd_thousands' -> 'usd';
    'thousands' -> None). Unknown units pass through unchanged — only reached
    for non-numeric values, which scale_to_base leaves untouched anyway."""
    key = _unit_key(unit)
    if key in BASE_UNIT_SCALES:
        return BASE_UNIT_SCALES[key][1]
    return unit


def _canonical_unit(unit: Optional[str], unit_changed: bool) -> Optional[str]:
    """When a unit was scaled away from base, the stored unit becomes that
    unit's canonical base ('usd_thousands' -> 'usd'; 'thousands' -> None). A
    non-scaled unit keeps its original string (already base)."""
    if unit_changed:
        return base_unit_for(unit)
    return unit
