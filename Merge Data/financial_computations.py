"""
Financial computation layer for the iXBRL parser pipeline.

Process order (all applied via ``compute()``):
  1. Sign normalisation  — flip fields that violate sign conventions
  2. Gap-fills           — derive null fields algebraically
  3. Single-row derivations — compute analytical metrics
  4. Validations         — flag internal inconsistencies into validation_warnings

Cross-period metrics (YoY growth, CAGR) run separately via ``compute_cross_period()``.

This module operates on plain dicts (one per year), NOT ORM objects.
A "row" is a flat dict of all financial fields for a single year, drawn from
the three statement dicts (income_statement, balance_sheet, cash_flow_statement)
plus any derived fields.

Usage:
    from financial_computations import compute, compute_cross_period

    # After iXBRL extraction, for each year-row:
    extracted = parse_ixbrl_multi(...)
    extracted = compute(extracted)          # mutates in place, returns same dict

    # Cross-period (needs all years already computed):
    compute_cross_period(extracted)
"""
from __future__ import annotations

import logging
import math
from typing import Any

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Sign convention constants
# ═══════════════════════════════════════════════════════════════════════════════

_NEGATIVE_FIELDS: frozenset[str] = frozenset([
    "cost_of_sales", "distribution_costs", "admin_expenses",
    "finance_costs", "tax_expense", "depreciation", "amortisation",
    "employee_costs", "total_current_liabilities", "total_noncurrent_liabilities",
    "total_liabilities", "trade_payables", "other_payables",
    "short_term_borrowings", "long_term_borrowings",
    "lease_liabilities_noncurrent", "provisions", "pension_obligations",
    "capex_ppe", "capex_intangibles", "repayment_borrowings",
    "dividends_paid_cf", "lease_payments", "tax_paid",
    "net_cash_investing", "net_cash_financing",
])

_POSITIVE_FIELDS: frozenset[str] = frozenset([
    "revenue", "gross_profit", "total_assets", "total_current_assets",
    "total_fixed_assets", "cash", "inventories", "trade_receivables",
    "net_assets", "total_equity", "share_capital",
    "proceeds_borrowings", "proceeds_disposal_ppe",
])


# ═══════════════════════════════════════════════════════════════════════════════
# Dict-access helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _g(row: dict, field: str) -> float | None:
    """Get a field value from a row dict, returning None if absent."""
    return row.get(field)


def _s(row: dict, field: str, value: float | None) -> None:
    """Set a field value on a row dict."""
    row[field] = value


# ═══════════════════════════════════════════════════════════════════════════════
# Arithmetic helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _sum_required(row: dict, *fields: str) -> float | None:
    vals = [_g(row, f) for f in fields]
    if any(v is None for v in vals):
        return None
    return sum(vals)


def _sum_with_optional(
    row: dict,
    required: list[str],
    optional: list[str],
    min_optional: int = 0,
) -> float | None:
    req_vals = [_g(row, f) for f in required]
    if any(v is None for v in req_vals):
        return None
    opt_vals = [_g(row, f) for f in optional if _g(row, f) is not None]
    if len(opt_vals) < min_optional:
        return None
    return sum(req_vals) + sum(opt_vals)


def _sum_components(row: dict, components: list[str], min_components: int = 2) -> float | None:
    vals = [_g(row, f) for f in components if _g(row, f) is not None]
    if len(vals) < min_components:
        return None
    return sum(vals)


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


# ═══════════════════════════════════════════════════════════════════════════════
# Gap-fill rule table  (15 rules)
# ═══════════════════════════════════════════════════════════════════════════════

_GAP_FILLS: list[dict] = [
    # ── Income Statement ──────────────────────────────────────────────────────
    {
        "target": "gross_profit",
        "fn": lambda r: _sum_required(r, "revenue", "cost_of_sales"),
        "note": "gap_fill: gross_profit = revenue + cost_of_sales (cost_of_sales is negative)",
    },
    {
        "target": "revenue",
        "fn": lambda r: (
            None if _g(r, "gross_profit") is None or _g(r, "cost_of_sales") is None
            else _g(r, "gross_profit") - _g(r, "cost_of_sales")
        ),
        "note": "gap_fill: revenue = gross_profit - cost_of_sales",
    },
    {
        "target": "cost_of_sales",
        "fn": lambda r: (
            None if _g(r, "gross_profit") is None or _g(r, "revenue") is None
            else _g(r, "gross_profit") - _g(r, "revenue")
        ),
        "note": "gap_fill: cost_of_sales = gross_profit - revenue",
    },
    {
        "target": "operating_profit",
        "fn": lambda r: _sum_with_optional(
            r,
            required=["gross_profit"],
            optional=["distribution_costs", "admin_expenses", "other_operating_income"],
            min_optional=1,
        ),
        "note": "gap_fill: operating_profit = gross_profit + opex lines (min 1 required)",
    },
    {
        "target": "profit_before_tax",
        "fn": lambda r: _sum_with_optional(
            r,
            required=["operating_profit"],
            optional=["finance_income", "finance_costs"],
            min_optional=0,
        ),
        "note": "gap_fill: profit_before_tax = operating_profit + finance items",
    },
    {
        "target": "profit_after_tax",
        "fn": lambda r: _sum_required(r, "profit_before_tax", "tax_expense"),
        "note": "gap_fill: profit_after_tax = profit_before_tax + tax_expense (tax is negative)",
    },
    {
        "target": "tax_expense",
        "fn": lambda r: (
            None if _g(r, "profit_after_tax") is None or _g(r, "profit_before_tax") is None
            else _g(r, "profit_after_tax") - _g(r, "profit_before_tax")
        ),
        "note": "gap_fill: tax_expense = profit_after_tax - profit_before_tax",
    },
    # ── Balance Sheet: Non-current assets ─────────────────────────────────────
    {
        "target": "total_fixed_assets",
        "fn": lambda r: _sum_components(
            r,
            ["intangible_assets", "goodwill", "tangible_fixed_assets",
             "right_of_use_assets", "investment_properties", "investments_fixed"],
            min_components=2,
        ),
        "note": "gap_fill: total_fixed_assets = sum of non-current asset components (min 2)",
    },
    # ── Balance Sheet: Current assets ─────────────────────────────────────────
    {
        "target": "total_current_assets",
        "fn": lambda r: _sum_components(
            r,
            ["inventories", "trade_receivables", "other_receivables",
             "cash", "short_term_investments"],
            min_components=2,
        ),
        "note": "gap_fill: total_current_assets = sum of current asset components (min 2)",
    },
    # ── Balance Sheet: Totals ─────────────────────────────────────────────────
    {
        "target": "total_assets",
        "fn": lambda r: _sum_required(r, "total_fixed_assets", "total_current_assets"),
        "note": "gap_fill: total_assets = total_fixed_assets + total_current_assets",
    },
    {
        "target": "total_liabilities",
        "fn": lambda r: _sum_required(r, "total_current_liabilities", "total_noncurrent_liabilities"),
        "note": "gap_fill: total_liabilities = current_liabilities + noncurrent_liabilities (both negative)",
    },
    {
        "target": "net_assets",
        "fn": lambda r: _sum_required(r, "total_assets", "total_liabilities"),
        "note": "gap_fill: net_assets = total_assets + total_liabilities (liabilities negative)",
    },
    {
        "target": "total_equity",
        "fn": lambda r: _sum_with_optional(
            r,
            required=["share_capital", "retained_earnings"],
            optional=["share_premium", "other_reserves", "minority_interest"],
            min_optional=0,
        ),
        "note": "gap_fill: total_equity = share_capital + retained_earnings + optional equity items",
    },
    # ── Cash Flow ─────────────────────────────────────────────────────────────
    {
        "target": "net_change_cash",
        "fn": lambda r: (
            None if _g(r, "closing_cash") is None or _g(r, "opening_cash") is None
            else _g(r, "closing_cash") - _g(r, "opening_cash")
        ),
        "note": "gap_fill: net_change_cash = closing_cash - opening_cash",
    },
    {
        "target": "closing_cash",
        "fn": lambda r: _sum_required(r, "opening_cash", "net_change_cash"),
        "note": "gap_fill: closing_cash = opening_cash + net_change_cash",
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# Derivation rule table  (20 single-row analytical metrics)
# ═══════════════════════════════════════════════════════════════════════════════

def _total_capex_fn(r: dict) -> float | None:
    capex_ppe = _g(r, "capex_ppe")
    if capex_ppe is None:
        return None
    capex_int = _g(r, "capex_intangibles") or 0.0
    return abs(capex_ppe) + abs(capex_int)


def _net_debt_fn(r: dict) -> float | None:
    cash = _g(r, "cash")
    if cash is None:
        return None
    stb = abs(_g(r, "short_term_borrowings") or 0.0)
    ltb = abs(_g(r, "long_term_borrowings") or 0.0)
    ll = abs(_g(r, "lease_liabilities_noncurrent") or 0.0)
    sti = _g(r, "short_term_investments") or 0.0
    return stb + ltb + ll - cash - sti


def _quick_ratio_fn(r: dict) -> float | None:
    tca = _g(r, "total_current_assets")
    tcl = _g(r, "total_current_liabilities")
    if tca is None or tcl is None or tcl == 0:
        return None
    inv = _g(r, "inventories") or 0.0
    return (tca - inv) / abs(tcl)


def _debt_to_equity_fn(r: dict) -> float | None:
    eq = _g(r, "total_equity")
    if eq is None or eq == 0:
        return None
    stb = abs(_g(r, "short_term_borrowings") or 0.0)
    ltb = abs(_g(r, "long_term_borrowings") or 0.0)
    return (stb + ltb) / eq


_DERIVATIONS: list[dict] = [
    # ── Profitability ─────────────────────────────────────────────────────────
    {
        "name": "gross_margin_pct",
        "requires": ["gross_profit", "revenue"],
        "fn": lambda r: _safe_div(_g(r, "gross_profit"), _g(r, "revenue")),
    },
    {
        "name": "operating_margin_pct",
        "requires": ["operating_profit", "revenue"],
        "fn": lambda r: _safe_div(_g(r, "operating_profit"), _g(r, "revenue")),
    },
    {
        "name": "net_margin_pct",
        "requires": ["profit_after_tax", "revenue"],
        "fn": lambda r: _safe_div(_g(r, "profit_after_tax"), _g(r, "revenue")),
    },
    {
        "name": "ebitda",
        "requires": ["operating_profit", "depreciation", "amortisation"],
        "fn": lambda r: (
            _g(r, "operating_profit")
            + abs(_g(r, "depreciation"))
            + abs(_g(r, "amortisation"))
        ),
    },
    {
        # Fallback: use operating_loss when operating_profit is absent
        "name": "ebitda",
        "requires": ["operating_loss", "depreciation", "amortisation"],
        "fn": lambda r: (
            None if _g(r, "operating_profit") is not None
            else _g(r, "operating_loss")
            + abs(_g(r, "depreciation"))
            + abs(_g(r, "amortisation"))
        ),
    },
    {
        "name": "ebitda_margin",
        "requires": ["ebitda", "revenue"],
        "fn": lambda r: _safe_div(_g(r, "ebitda"), _g(r, "revenue")),
    },
    {
        "name": "return_on_assets",
        "requires": ["profit_after_tax", "total_assets"],
        "fn": lambda r: _safe_div(_g(r, "profit_after_tax"), _g(r, "total_assets")),
    },
    {
        "name": "return_on_equity",
        "requires": ["profit_after_tax", "total_equity"],
        "fn": lambda r: _safe_div(_g(r, "profit_after_tax"), _g(r, "total_equity")),
    },
    # ── Cash Flow & Capital ───────────────────────────────────────────────────
    {
        "name": "total_capex",
        "requires": ["capex_ppe"],
        "fn": _total_capex_fn,
    },
    {
        "name": "fcf",
        "requires": ["net_cash_operating", "total_capex"],
        "fn": lambda r: (
            None if _g(r, "net_cash_operating") is None or _g(r, "total_capex") is None
            else _g(r, "net_cash_operating") - _g(r, "total_capex")
        ),
    },
    {
        # Fallback FCF using operating_cash_flow if net_cash_operating is absent
        "name": "fcf",
        "requires": ["operating_cash_flow", "total_capex"],
        "fn": lambda r: (
            None if _g(r, "operating_cash_flow") is None
            or _g(r, "total_capex") is None
            or _g(r, "net_cash_operating") is not None
            else _g(r, "operating_cash_flow") - _g(r, "total_capex")
        ),
    },
    {
        "name": "cash_conversion",
        "requires": ["fcf", "ebitda"],
        "fn": lambda r: (
            None if _g(r, "ebitda") is None or _g(r, "ebitda") <= 0
            else 0 if _g(r, "fcf") is not None and _g(r, "fcf") < 0
            else _safe_div(_g(r, "fcf"), _g(r, "ebitda"))
        ),
    },
    {
        "name": "capex_to_revenue",
        "requires": ["total_capex", "revenue"],
        "fn": lambda r: _safe_div(_g(r, "total_capex"), _g(r, "revenue")),
    },
    # ── Leverage & Liquidity ──────────────────────────────────────────────────
    {
        "name": "net_debt",
        "requires": ["cash"],
        "fn": _net_debt_fn,
    },
    {
        "name": "net_debt_to_ebitda",
        "requires": ["net_debt", "ebitda"],
        "fn": lambda r: (
            None if _g(r, "ebitda") is None or _g(r, "ebitda") <= 0
            else _safe_div(_g(r, "net_debt"), _g(r, "ebitda"))
        ),
    },
    {
        "name": "current_ratio",
        "requires": ["total_current_assets", "total_current_liabilities"],
        "fn": lambda r: (
            None if _g(r, "total_current_liabilities") is None
            or _g(r, "total_current_liabilities") == 0
            else abs(_g(r, "total_current_assets")) / abs(_g(r, "total_current_liabilities"))
        ),
    },
    {
        "name": "quick_ratio",
        "requires": ["total_current_assets", "total_current_liabilities"],
        "fn": _quick_ratio_fn,
    },
    {
        "name": "debt_to_equity",
        "requires": ["total_equity"],
        "fn": _debt_to_equity_fn,
    },
    {
        "name": "interest_cover",
        "requires": ["operating_profit", "finance_costs"],
        "fn": lambda r: (
            None if _g(r, "finance_costs") is None or _g(r, "finance_costs") == 0
            else _g(r, "operating_profit") / abs(_g(r, "finance_costs"))
        ),
    },
    # ── Efficiency ────────────────────────────────────────────────────────────
    {
        "name": "asset_turnover",
        "requires": ["revenue", "total_assets"],
        "fn": lambda r: _safe_div(_g(r, "revenue"), _g(r, "total_assets")),
    },
    {
        "name": "revenue_per_employee",
        "requires": ["revenue", "employees"],
        "fn": lambda r: _safe_div(_g(r, "revenue"), _g(r, "employees")),
    },
    {
        "name": "profit_per_employee",
        "requires": ["operating_profit", "employees"],
        "fn": lambda r: _safe_div(_g(r, "operating_profit"), _g(r, "employees")),
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# Validation rule table  (6 checks)
# ═══════════════════════════════════════════════════════════════════════════════

_VALIDATIONS: list[dict] = [
    {
        "name": "bs_balance",
        "fields": ["total_assets", "total_liabilities", "net_assets"],
        "tolerance_pct": 0.02,
        "fn": lambda r: (
            _g(r, "total_assets") + _g(r, "total_liabilities"),
            _g(r, "net_assets"),
        ),
    },
    {
        "name": "equity_balance",
        "fields": ["net_assets", "total_equity"],
        "tolerance_pct": 0.02,
        "fn": lambda r: (_g(r, "net_assets"), _g(r, "total_equity")),
    },
    {
        "name": "pl_gross_profit",
        "fields": ["revenue", "cost_of_sales", "gross_profit"],
        "tolerance_pct": 0.02,
        "fn": lambda r: (
            _g(r, "revenue") + _g(r, "cost_of_sales"),
            _g(r, "gross_profit"),
        ),
    },
    {
        "name": "pl_profit_after_tax",
        "fields": ["profit_before_tax", "tax_expense", "profit_after_tax"],
        "tolerance_pct": 0.02,
        "fn": lambda r: (
            _g(r, "profit_before_tax") + _g(r, "tax_expense"),
            _g(r, "profit_after_tax"),
        ),
    },
    {
        "name": "cf_cash_movement",
        "fields": ["opening_cash", "net_change_cash", "closing_cash"],
        "tolerance_pct": 0.02,
        "fn": lambda r: (
            _g(r, "opening_cash") + _g(r, "net_change_cash"),
            _g(r, "closing_cash"),
        ),
    },
    {
        "name": "cf_net_change",
        "fields": ["net_cash_operating", "net_cash_investing", "net_cash_financing", "net_change_cash"],
        "tolerance_pct": 0.05,
        "fn": lambda r: (
            _g(r, "net_cash_operating")
            + _g(r, "net_cash_investing")
            + _g(r, "net_cash_financing"),
            _g(r, "net_change_cash"),
        ),
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# Which fields belong to which statement (including new fields)
# ═══════════════════════════════════════════════════════════════════════════════

INCOME_STATEMENT_FIELDS_EXT = [
    "revenue", "cost_of_sales", "gross_profit", "distribution_costs",
    "admin_expenses", "other_operating_income", "employee_costs",
    "depreciation", "amortisation", "operating_profit",
    "finance_income", "finance_costs", "profit_before_tax",
    "tax_expense", "profit_after_tax",
]

BALANCE_SHEET_FIELDS_EXT = [
    "intangible_assets", "goodwill", "tangible_fixed_assets",
    "right_of_use_assets", "investment_properties", "investments_fixed",
    "total_fixed_assets", "inventories", "trade_receivables",
    "other_receivables", "cash", "short_term_investments",
    "total_current_assets", "total_assets",
    "trade_payables", "other_payables", "short_term_borrowings",
    "long_term_borrowings", "lease_liabilities_noncurrent",
    "provisions", "pension_obligations",
    "total_current_liabilities", "total_noncurrent_liabilities",
    "total_liabilities", "net_assets",
    "share_capital", "share_premium", "retained_earnings",
    "other_reserves", "minority_interest", "total_equity",
]

CASH_FLOW_FIELDS_EXT = [
    "operating_cash_flow", "net_cash_operating",
    "capex_ppe", "capex_intangibles", "net_cash_investing",
    "repayment_borrowings", "proceeds_borrowings",
    "proceeds_disposal_ppe", "dividends_paid_cf",
    "lease_payments", "tax_paid", "net_cash_financing",
    "opening_cash", "closing_cash", "net_change_cash",
]

# All derivation output fields (display columns)
DERIVED_FIELDS = [
    "gross_margin_pct", "operating_margin_pct", "net_margin_pct",
    "ebitda", "ebitda_margin", "return_on_assets", "return_on_equity",
    "total_capex", "fcf", "cash_conversion", "capex_to_revenue",
    "net_debt", "net_debt_to_ebitda", "current_ratio", "quick_ratio",
    "debt_to_equity", "interest_cover", "asset_turnover",
    "revenue_per_employee", "profit_per_employee",
]

# Cross-period display columns
CROSS_PERIOD_FIELDS = [
    "revenue_yoy_growth", "ebitda_yoy_growth", "profit_yoy_growth",
    "revenue_cagr_3yr", "revenue_cagr_5yr",
]


# ═══════════════════════════════════════════════════════════════════════════════
# New XBRL tag mappings for fields not in the original tag map
# ═══════════════════════════════════════════════════════════════════════════════

EXTENDED_IXBRL_TAG_MAP = {
    # --- Profit before tax ---
    "ProfitLossOnOrdinaryActivitiesBeforeTax": "profit_before_tax",
    "ProfitLossBeforeTax": "profit_before_tax",
    "ProfitLossBeforeTaxation": "profit_before_tax",

    # --- Profit after tax ---
    "ProfitLossForPeriod": "profit_after_tax",
    "ProfitLossForYear": "profit_after_tax",
    "ProfitLossOnOrdinaryActivitiesAfterTax": "profit_after_tax",
    "ProfitLossAfterTax": "profit_after_tax",
    "ProfitLossAttributableToOwnersOfParent": "profit_after_tax",
    "ProfitLoss": "profit_after_tax",
    "RetainedProfitLossForFinancialYear": "profit_after_tax",

    # --- Other operating income ---
    "OtherOperatingIncome": "other_operating_income",
    "OtherOperatingIncomeExpense": "other_operating_income",
    "OtherIncome": "other_operating_income",

    # --- Finance income ---
    "InterestReceivableSimilarIncome": "finance_income",
    "InterestReceivable": "finance_income",
    "InterestReceivableAndSimilarIncome": "finance_income",
    "FinanceIncome": "finance_income",
    "InterestIncome": "finance_income",

    # --- Opening / closing cash ---
    "CashCashEquivalentsAtBeginningOfPeriod": "opening_cash",
    "CashCashEquivalentsBeginningPeriod": "opening_cash",
    "CashEquivalentsAtBeginningOfPeriod": "opening_cash",
    "CashCashEquivalentsAtEndOfPeriod": "closing_cash",
    "CashCashEquivalentsEndPeriod": "closing_cash",
    "CashEquivalentsAtEndOfPeriod": "closing_cash",

    # --- Net change in cash ---
    "IncreaseDecreaseInCashCashEquivalents": "net_change_cash",
    "NetIncreaseDecreaseInCashCashEquivalents": "net_change_cash",
    "ChangeInCashCashEquivalents": "net_change_cash",
    "IncreaseDecreaseInCashEquivalents": "net_change_cash",

    # --- Net cash from operating (alternative to operating_cash_flow) ---
    "NetCashFromOperatingActivities": "net_cash_operating",
    "CashFlowsFromUsedInOperatingActivitiesTotal": "net_cash_operating",

    # --- Net cash from investing ---
    "NetCashFlowsFromUsedInInvestingActivities": "net_cash_investing",
    "CashFlowsFromUsedInInvestingActivities": "net_cash_investing",
    "NetCashFromInvestingActivities": "net_cash_investing",
    "NetCashUsedInInvestingActivities": "net_cash_investing",

    # --- Net cash from financing ---
    "NetCashFlowsFromUsedInFinancingActivities": "net_cash_financing",
    "CashFlowsFromUsedInFinancingActivities": "net_cash_financing",
    "NetCashFromFinancingActivities": "net_cash_financing",
    "NetCashUsedInFinancingActivities": "net_cash_financing",

    # --- Retained earnings ---
    "RetainedEarningsAccumulatedLosses": "retained_earnings",
    "RetainedEarnings": "retained_earnings",
    "ProfitLossAccountReserve": "retained_earnings",
    "ProfitAndLossAccount": "retained_earnings",
    "ProfitAndLossAccountBalance": "retained_earnings",
    "AccumulatedProfitLoss": "retained_earnings",

    # --- Share premium ---
    "SharePremiumAccount": "share_premium",
    "SharePremium": "share_premium",
    "SharePremiumReserve": "share_premium",

    # --- Other reserves ---
    "OtherReserves": "other_reserves",
    "OtherReservesTotal": "other_reserves",
    "RevaluationReserve": "other_reserves",
    "CapitalRedemptionReserve": "other_reserves",
    "MergerReserve": "other_reserves",

    # --- Minority interest ---
    "MinorityInterest": "minority_interest",
    "NonControllingInterest": "minority_interest",
    "NonControllingInterests": "minority_interest",
    "EquityAttributableToNonControllingInterests": "minority_interest",

    # --- Intangible assets ---
    "IntangibleAssets": "intangible_assets",
    "IntangibleAssetsNet": "intangible_assets",
    "TotalIntangibleFixedAssets": "intangible_assets",
    "IntangibleAssetsOtherThanGoodwill": "intangible_assets",

    # --- Goodwill ---
    "Goodwill": "goodwill",
    "GoodwillNet": "goodwill",
    "GoodwillGross": "goodwill",

    # --- Tangible fixed assets ---
    "TangibleFixedAssets": "tangible_fixed_assets",
    "PropertyPlantEquipment": "tangible_fixed_assets",
    "PropertyPlantAndEquipment": "tangible_fixed_assets",
    "TotalTangibleFixedAssets": "tangible_fixed_assets",
    "PropertyPlantEquipmentNet": "tangible_fixed_assets",

    # --- Right of use assets ---
    "RightOfUseAssets": "right_of_use_assets",
    "RightOfUseAssetsNet": "right_of_use_assets",
    "LeasedAssetsRightOfUse": "right_of_use_assets",

    # --- Investment properties ---
    "InvestmentProperty": "investment_properties",
    "InvestmentProperties": "investment_properties",
    "InvestmentPropertyFairValue": "investment_properties",

    # --- Fixed asset investments ---
    "InvestmentsFixedAssets": "investments_fixed",
    "FixedAssetInvestments": "investments_fixed",
    "Investments": "investments_fixed",
    "InvestmentsInSubsidiaries": "investments_fixed",
    "InvestmentsInAssociates": "investments_fixed",
    "OtherInvestmentsNoncurrent": "investments_fixed",

    # --- Other receivables ---
    "OtherDebtors": "other_receivables",
    "OtherReceivables": "other_receivables",
    "OtherReceivablesCurrent": "other_receivables",
    "PrepaymentsAccruedIncome": "other_receivables",
    "Prepayments": "other_receivables",

    # --- Short-term investments ---
    "ShortTermInvestments": "short_term_investments",
    "CurrentAssetInvestments": "short_term_investments",
    "OtherFinancialAssetsCurrent": "short_term_investments",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Processing functions
# ═══════════════════════════════════════════════════════════════════════════════

def _flatten_year_row(data: dict, year_str: str) -> dict:
    """
    Build a flat dict of all financial fields for a given year by merging
    income_statement, balance_sheet, and cash_flow_statement rows.
    Also pulls in employees from employees_history.
    """
    row = {}
    for stmt_key in ("income_statement", "balance_sheet", "cash_flow_statement"):
        stmt = data.get(stmt_key, {})
        year_data = stmt.get(year_str, {})
        row.update(year_data)

    # Pull employees from history
    emp_hist = data.get("employees_history", {})
    if year_str in emp_hist and emp_hist[year_str] is not None:
        row["employees"] = emp_hist[year_str]

    return row


def _write_back_row(data: dict, year_str: str, row: dict) -> None:
    """
    Write the flat row back into the three statement dicts and a separate
    derived_metrics dict. Creates statement entries if they don't exist.
    """
    # Collect all statement field names for routing
    is_fields = set(INCOME_STATEMENT_FIELDS_EXT)
    bs_fields = set(BALANCE_SHEET_FIELDS_EXT)
    cf_fields = set(CASH_FLOW_FIELDS_EXT)

    for stmt_key, field_set in [
        ("income_statement", is_fields),
        ("balance_sheet", bs_fields),
        ("cash_flow_statement", cf_fields),
    ]:
        if stmt_key not in data:
            data[stmt_key] = {}
        if year_str not in data[stmt_key]:
            data[stmt_key][year_str] = {}
        for field in field_set:
            if field in row:
                data[stmt_key][year_str][field] = row[field]


def _normalize_signs(row: dict, audit_log: list[dict], year_str: str) -> None:
    """Enforce sign conventions. Append entries to audit_log for each flip."""
    for field in _NEGATIVE_FIELDS:
        val = _g(row, field)
        if val is not None and val > 0:
            _s(row, field, -val)
            audit_log.append({
                "type": "sign_correction",
                "field": field,
                "year": year_str,
                "original": val,
                "corrected": -val,
            })
            logger.debug("Sign corrected %s → negative (year %s)", field, year_str)

    for field in _POSITIVE_FIELDS:
        val = _g(row, field)
        if val is not None and val < 0:
            _s(row, field, -val)
            audit_log.append({
                "type": "sign_correction",
                "field": field,
                "year": year_str,
                "original": val,
                "corrected": -val,
            })
            logger.debug("Sign corrected %s → positive (year %s)", field, year_str)


def _apply_gap_fills(row: dict, audit_log: list[dict], year_str: str) -> None:
    """Apply gap-fill rules. Never overwrite non-null values."""
    for rule in _GAP_FILLS:
        target = rule["target"]
        if _g(row, target) is not None:
            continue

        result = rule["fn"](row)
        if result is None:
            continue

        _s(row, target, result)
        audit_log.append({
            "type": "gap_fill",
            "field": target,
            "year": year_str,
            "value": result,
            "note": rule["note"],
        })
        logger.debug("Gap-filled %s = %s (year %s)", target, result, year_str)


def _apply_derivations(row: dict) -> None:
    """Compute analytical metrics. Skip if any required input is null."""
    for rule in _DERIVATIONS:
        # Don't overwrite if already set (handles fcf fallback)
        if _g(row, rule["name"]) is not None:
            continue
        if any(_g(row, f) is None for f in rule["requires"]):
            continue
        result = rule["fn"](row)
        if result is not None:
            _s(row, rule["name"], result)


def _apply_validations(row: dict, year_str: str) -> dict | None:
    """
    Run consistency checks. Returns a validation_warnings dict if any fail,
    or None if all pass.
    """
    failed: dict[str, dict] = {}

    for rule in _VALIDATIONS:
        if any(_g(row, f) is None for f in rule["fields"]):
            continue

        try:
            computed, expected = rule["fn"](row)
        except Exception:
            continue

        if computed is None or expected is None:
            continue

        reference = max(abs(expected), abs(computed), 1.0)
        diff_pct = abs(computed - expected) / reference
        if diff_pct > rule["tolerance_pct"]:
            failed[rule["name"]] = {
                "expected": round(expected, 4),
                "actual": round(computed, 4),
                "diff_pct": round(diff_pct, 4),
            }
            logger.warning(
                "Validation '%s' failed for year %s: diff=%.2f%%",
                rule["name"], year_str, diff_pct * 100,
            )

    if failed:
        return {"failed": list(failed.keys()), "details": failed}
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Cross-period helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _yoy_growth(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None or prior == 0:
        return None
    return (current - prior) / abs(prior)


def _cagr_from_history(history: dict[str, float | None], current_year: str, lookback_years: int) -> float | None:
    """Compute CAGR from a year-keyed history dict."""
    try:
        current_yr = int(current_year)
    except (ValueError, TypeError):
        return None

    current_val = history.get(current_year)
    if current_val is None or current_val <= 0:
        return None

    target_yr = current_yr - lookback_years
    target_key = str(target_yr)

    # Find closest year at or before target
    candidates = {
        yr: val for yr, val in history.items()
        if val is not None and val > 0
    }
    try:
        valid = {int(yr): val for yr, val in candidates.items() if int(yr) <= target_yr}
    except ValueError:
        return None

    if not valid:
        return None

    base_yr = max(valid.keys())
    base_val = valid[base_yr]

    years = current_yr - base_yr
    if years < 1:
        return None

    try:
        return (current_val / base_val) ** (1.0 / years) - 1.0
    except (ZeroDivisionError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Public entry points
# ═══════════════════════════════════════════════════════════════════════════════

def compute(data: dict) -> dict:
    """
    Run the full single-row pipeline on every year in the extracted data dict:
      sign normalisation → gap-fills → derivations → validations.

    Mutates ``data`` in place and returns it.
    Adds/updates:
      - derivation_log: list of all sign corrections and gap-fills (audit trail)
      - validation_warnings: per-year validation failure flags
      - derived metrics written into statement JSONB objects
      - display-level fields (ebitda, fcf, etc.) for the most recent year
    """
    # Collect all years across statements
    all_years: set[str] = set()
    for stmt_key in ("income_statement", "balance_sheet", "cash_flow_statement"):
        stmt = data.get(stmt_key, {})
        all_years.update(stmt.keys())

    if not all_years:
        return data

    sorted_years = sorted(all_years)
    current_year = sorted_years[-1]

    audit_log: list[dict] = []
    validation_warnings: dict[str, dict] = {}

    # Process each year
    for year_str in sorted_years:
        row = _flatten_year_row(data, year_str)

        # 1. Sign normalisation
        _normalize_signs(row, audit_log, year_str)

        # 2. Gap-fills
        _apply_gap_fills(row, audit_log, year_str)

        # 3. Single-row derivations
        _apply_derivations(row)

        # 4. Validations
        warnings = _apply_validations(row, year_str)
        if warnings:
            validation_warnings[year_str] = warnings

        # Write the processed row back into the statement dicts
        _write_back_row(data, year_str, row)

        # For the most recent year, populate display-level columns
        if year_str == current_year:
            _set_display_columns(data, row)

    # Store the full audit log
    existing_log = data.get("derivation_log", {})
    if isinstance(existing_log, dict):
        existing_log["computation_audit"] = audit_log
    else:
        data["derivation_log"] = {"computation_audit": audit_log}

    if validation_warnings:
        data["validation_warnings"] = validation_warnings

    return data


def _set_display_columns(data: dict, row: dict) -> None:
    """Set top-level display columns from the most recent year's computed row."""
    # Revenue
    if _g(row, "revenue") is not None:
        data["revenue"] = _g(row, "revenue")

    # EBITDA
    if _g(row, "ebitda") is not None:
        data["ebitda"] = _g(row, "ebitda")

    # EBITDA margin
    if _g(row, "ebitda_margin") is not None:
        data["ebitda_margin"] = round(_g(row, "ebitda_margin"), 4)

    # FCF
    if _g(row, "fcf") is not None:
        data["fcf"] = _g(row, "fcf")

    # Cash conversion
    if _g(row, "cash_conversion") is not None:
        data["cash_conversion"] = round(_g(row, "cash_conversion"), 4)

    # Employees
    if _g(row, "employees") is not None:
        data["employees"] = int(_g(row, "employees"))

    # All derived metric display columns
    for field in DERIVED_FIELDS:
        val = _g(row, field)
        if val is not None:
            if isinstance(val, float):
                data[field] = round(val, 4)
            else:
                data[field] = val


def compute_cross_period(data: dict) -> dict:
    """
    Compute YoY growth rates and CAGRs across all years in the data.
    Must be called AFTER compute() so derived fields exist.
    Mutates ``data`` in place.
    """
    all_years: set[str] = set()
    for stmt_key in ("income_statement", "balance_sheet", "cash_flow_statement"):
        all_years.update(data.get(stmt_key, {}).keys())

    sorted_years = sorted(all_years)
    if len(sorted_years) < 2:
        return data

    # Build per-year lookup of key metrics
    revenue_by_year = {}
    ebitda_by_year = {}
    pat_by_year = {}  # profit_after_tax

    for yr in sorted_years:
        row = _flatten_year_row(data, yr)
        if _g(row, "revenue") is not None:
            revenue_by_year[yr] = _g(row, "revenue")
        if _g(row, "ebitda") is not None:
            ebitda_by_year[yr] = _g(row, "ebitda")
        if _g(row, "profit_after_tax") is not None:
            pat_by_year[yr] = _g(row, "profit_after_tax")

    # For the most recent year, compute YoY and CAGR
    current_yr = sorted_years[-1]
    prev_yr = sorted_years[-2] if len(sorted_years) >= 2 else None

    # YoY growth
    data["revenue_yoy_growth"] = _yoy_growth(
        revenue_by_year.get(current_yr),
        revenue_by_year.get(prev_yr) if prev_yr else None,
    )
    data["ebitda_yoy_growth"] = _yoy_growth(
        ebitda_by_year.get(current_yr),
        ebitda_by_year.get(prev_yr) if prev_yr else None,
    )
    data["profit_yoy_growth"] = _yoy_growth(
        pat_by_year.get(current_yr),
        pat_by_year.get(prev_yr) if prev_yr else None,
    )

    # CAGR
    data["revenue_cagr_3yr"] = _cagr_from_history(revenue_by_year, current_yr, 3)
    data["revenue_cagr_5yr"] = _cagr_from_history(revenue_by_year, current_yr, 5)

    # Also update the older revenue_cagr/ebitda_cagr if history dicts exist
    rev_hist = data.get("revenue_history", {})
    if rev_hist and len(rev_hist) >= 2:
        years = sorted(rev_hist.keys())
        first_yr, last_yr = years[0], years[-1]
        first_val, last_val = rev_hist[first_yr], rev_hist[last_yr]
        n = int(last_yr) - int(first_yr)
        if n > 0 and first_val and first_val > 0 and last_val and last_val > 0:
            data["revenue_cagr"] = round((last_val / first_val) ** (1 / n) - 1, 4)

    ebitda_hist = data.get("ebitda_history", {})
    if ebitda_hist and len(ebitda_hist) >= 2:
        years = sorted(ebitda_hist.keys())
        first_yr, last_yr = years[0], years[-1]
        first_val, last_val = ebitda_hist[first_yr], ebitda_hist[last_yr]
        n = int(last_yr) - int(first_yr)
        if n > 0 and first_val and first_val > 0 and last_val and last_val > 0:
            data["ebitda_cagr"] = round((last_val / first_val) ** (1 / n) - 1, 4)

    # Round YoY/CAGR values
    for field in CROSS_PERIOD_FIELDS:
        if data.get(field) is not None:
            data[field] = round(data[field], 4)

    return data
