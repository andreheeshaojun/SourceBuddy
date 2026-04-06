import os
import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BATCH_SIZE = 50
CH_BASE_URL = "https://api.companieshouse.gov.uk"
# 600 requests per 5 min = 2 req/sec max. 0.6s delay gives comfortable margin.
REQUEST_DELAY = 0.6
RATE_LIMIT_BACKOFF = 300  # 5 minutes on 429

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", "keys.env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CH_API_KEY = os.getenv("CH_API_KEY")

def _check_env():
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_KEY")
    if not CH_API_KEY:
        missing.append("CH_API_KEY")
    if missing:
        raise EnvironmentError(
            f"Missing environment variables: {', '.join(missing)}. "
            "Check config/keys.env (see .env.example)."
        )

# ---------------------------------------------------------------------------
# Companies House helpers
# ---------------------------------------------------------------------------
ch_session = requests.Session()

def _init_ch_session():
    ch_session.auth = (CH_API_KEY, "")

def ch_get(url, params=None):
    """GET request to Companies House with rate-limit handling."""
    time.sleep(REQUEST_DELAY)
    resp = ch_session.get(url, params=params)

    if resp.status_code == 429:
        log.warning("Rate limited by Companies House. Backing off %ds...", RATE_LIMIT_BACKOFF)
        time.sleep(RATE_LIMIT_BACKOFF)
        resp = ch_session.get(url, params=params)

    resp.raise_for_status()
    return resp

# ---------------------------------------------------------------------------
# Step 1 — Get accounts filings
# ---------------------------------------------------------------------------
def get_accounts_filings(company_number, count=5):
    """Return up to `count` most recent actual accounts filings (newest first).

    Fetches extra items from the API to account for non-account filings
    (AA01 reference-date changes, etc.) that Companies House returns under
    the 'accounts' category. Only items with type 'AA' (annual accounts)
    are kept.
    """
    url = f"{CH_BASE_URL}/company/{company_number}/filing-history"
    # Over-fetch so we still get `count` real accounts after filtering
    resp = ch_get(url, params={"category": "accounts", "items_per_page": str(count * 3)})
    items = resp.json().get("items", [])
    return [f for f in items if f.get("type") == "AA"][:count]


def get_latest_accounts_filing(company_number):
    """Return the most recent accounts filing item, or None."""
    filings = get_accounts_filings(company_number, count=1)
    return filings[0] if filings else None

# ---------------------------------------------------------------------------
# Step 2 — Get document metadata (format availability)
# ---------------------------------------------------------------------------
def get_document_metadata(filing):
    """Return document metadata dict from a filing item."""
    meta_url = filing.get("links", {}).get("document_metadata")
    if not meta_url:
        return None
    # Document metadata lives on a different host — needs full URL
    if not meta_url.startswith("http"):
        meta_url = f"https://document-api.companieshouse.gov.uk{meta_url}"
    resp = ch_get(meta_url)
    return resp.json()

# ---------------------------------------------------------------------------
# Step 3 — Determine filing format
# ---------------------------------------------------------------------------
def determine_filing_format(metadata, filing):
    """Return one of: 'ixbrl', 'electronic_pdf', 'scanned_pdf'."""
    resources = metadata.get("resources", {})
    has_xhtml = "application/xhtml+xml" in resources
    paper_filed = filing.get("paper_filed", False)

    if has_xhtml:
        return "ixbrl"
    elif not paper_filed:
        return "electronic_pdf"
    else:
        return "scanned_pdf"

# ---------------------------------------------------------------------------
# Step 4 — Download document content
# ---------------------------------------------------------------------------
def download_document(metadata, filing_format):
    """Download the document bytes. Returns (content_bytes, content_type)."""
    doc_url = metadata.get("links", {}).get("document")
    if not doc_url:
        raise ValueError("No document download link in metadata")

    if filing_format == "ixbrl":
        accept = "application/xhtml+xml"
    else:
        accept = "application/pdf"

    time.sleep(REQUEST_DELAY)
    resp = ch_session.get(doc_url, headers={"Accept": accept})

    if resp.status_code == 429:
        log.warning("Rate limited on download. Backing off %ds...", RATE_LIMIT_BACKOFF)
        time.sleep(RATE_LIMIT_BACKOFF)
        resp = ch_session.get(doc_url, headers={"Accept": accept})

    resp.raise_for_status()
    return resp.content, resp.headers.get("Content-Type", "")

# ---------------------------------------------------------------------------
# Step 5 — Parse iXBRL
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Comprehensive XBRL tag-name -> canonical field mapping.
#
# Covers: UK GAAP (core), FRS 102, FRS 105, and IFRS taxonomies as used in
# UK Companies House inline-XBRL filings.  Keys are the LOCAL part of the
# tag name (after the namespace prefix), e.g. "TurnoverRevenue" not
# "core:TurnoverRevenue".
#
# Sources: FRC taxonomy schemas published at xbrl.frc.org.uk, HMRC CT
# taxonomy, IFRS taxonomy (as adopted for UK filing), and observed real
# filings.  Tag names are case-sensitive and must match exactly.
# ---------------------------------------------------------------------------
IXBRL_TAG_MAP = {

    # =======================================================================
    # INCOME STATEMENT
    # =======================================================================

    # --- Revenue / Turnover ---
    "TurnoverRevenue": "revenue",                              # FRS 102 core (most common)
    "TurnoverGrossRevenue": "revenue",                         # FRS 102 core (gross before deductions)
    "Turnover": "revenue",                                     # UK GAAP / old taxonomy
    "TurnoverNetRevenue": "revenue",                           # FRS 102 (net of deductions)
    "Revenue": "revenue",                                      # IFRS
    "RevenueFromContractsWithCustomers": "revenue",            # IFRS 15
    "RevenueFromRenderingOfServices": "revenue",               # IFRS
    "RevenueFromSaleOfGoods": "revenue",                       # IFRS
    "TurnoverFromAllActivities": "revenue",                    # FRS 105 micro-entities
    "NetTurnoverRevenue": "revenue",                           # FRS 102 variant

    # --- Cost of sales ---
    "CostSales": "cost_of_sales",                              # FRS 102 core
    "CostOfSales": "cost_of_sales",                            # IFRS / variant
    "CostSalesBeforeExceptionalItems": "cost_of_sales",        # FRS 102

    # --- Gross profit ---
    "GrossProfitLoss": "gross_profit",                         # FRS 102 core
    "GrossProfit": "gross_profit",                             # UK GAAP / variant
    "GrossProfitOrLoss": "gross_profit",                       # variant

    # --- Distribution costs ---
    "DistributionCosts": "distribution_costs",                 # FRS 102 core
    "DistributionCostsBeforeExceptionalItems": "distribution_costs",  # FRS 102

    # --- Administrative expenses ---
    "AdministrativeExpenses": "admin_expenses",                # FRS 102 core
    "AdministrativeExpensesBeforeExceptionalItems": "admin_expenses",  # FRS 102

    # --- Employee / staff costs ---
    "StaffCosts": "employee_costs",                            # FRS 102 core
    "StaffCostsEmployeeBenefitsExpense": "employee_costs",     # FRS 102 detailed
    "EmployeeBenefitsExpense": "employee_costs",               # IFRS
    "WagesAndSalaries": "employee_costs",                      # FRS 102 sub-component (use if total missing)
    "AverageNumberEmployeesDuringPeriod": "employees",         # FRS 102 core (headcount, not cost)

    # --- Depreciation ---
    "DepreciationExpensePropertyPlantEquipment": "depreciation",       # FRS 102
    "DepreciationAmortisationImpairment": "depreciation",              # FRS 102 combined line
    "Depreciation": "depreciation",                                    # UK GAAP / generic
    "DepreciationOfPropertyPlantAndEquipment": "depreciation",         # IFRS
    "DepreciationExpense": "depreciation",                             # IFRS variant
    "DepreciationChargeOnTangibleFixedAssets": "depreciation",         # old UK GAAP

    # --- Amortisation ---
    "AmortisationExpenseIntangibleAssets": "amortisation",             # FRS 102
    "AmortisationOfIntangibleAssets": "amortisation",                  # IFRS
    "Amortisation": "amortisation",                                    # generic
    "AmortisationExpense": "amortisation",                             # IFRS variant
    "AmortisationChargeOnIntangibleFixedAssets": "amortisation",       # old UK GAAP

    # --- Operating profit ---
    "OperatingProfitLoss": "operating_profit",                         # FRS 102 core
    "OperatingProfit": "operating_profit",                             # variant
    "ProfitLossFromOperatingActivities": "operating_profit",           # IFRS
    "OperatingProfitLossBeforeExceptionalItems": "operating_profit",   # FRS 102

    # --- Finance costs / interest payable ---
    "InterestPayableSimilarCharges": "finance_costs",                  # FRS 102 core
    "InterestPayable": "finance_costs",                                # UK GAAP
    "InterestPayableAndSimilarCharges": "finance_costs",               # FRS 102 variant
    "FinanceCosts": "finance_costs",                                   # IFRS
    "FinanceCostsTotal": "finance_costs",                              # IFRS variant
    "InterestExpense": "finance_costs",                                # IFRS
    "InterestPayableSimilarExpenses": "finance_costs",                 # FRS 102 variant

    # --- Tax expense ---
    "TaxOnProfitOrLossOnOrdinaryActivities": "tax_expense",           # FRS 102 core
    "TaxOnProfit": "tax_expense",                                      # FRS 102 variant
    "TaxExpenseCredit": "tax_expense",                                 # FRS 102
    "IncomeTaxExpenseContinuingOperations": "tax_expense",             # IFRS
    "IncomeTaxExpense": "tax_expense",                                 # IFRS variant
    "TaxOnProfitOnOrdinaryActivities": "tax_expense",                 # old UK GAAP
    "TotalTaxChargeCreditToIncomeStatement": "tax_expense",            # FRS 102

    # --- Profit before tax (useful derived metric) ---
    "ProfitLossOnOrdinaryActivitiesBeforeTax": "profit_before_tax",   # FRS 102 core
    "ProfitLossBeforeTax": "profit_before_tax",                        # IFRS
    "ProfitLossBeforeTaxation": "profit_before_tax",                   # variant

    # =======================================================================
    # BALANCE SHEET
    # =======================================================================

    # --- Total fixed assets (non-current assets) ---
    "FixedAssets": "total_fixed_assets",                               # FRS 102 core
    "TotalFixedAssets": "total_fixed_assets",                          # FRS 102 variant
    "NonCurrentAssets": "total_fixed_assets",                          # IFRS
    "TotalNonCurrentAssets": "total_fixed_assets",                     # IFRS variant
    "NoncurrentAssets": "total_fixed_assets",                          # IFRS (no hyphen)

    # --- Total current assets ---
    "CurrentAssets": "total_current_assets",                           # FRS 102 core
    "TotalCurrentAssets": "total_current_assets",                      # FRS 102 variant

    # --- Cash ---
    "CashBankOnHand": "cash",                                          # FRS 102 core
    "CashBankInHand": "cash",                                          # FRS 102 variant (very common)
    "CashCashEquivalentsCashFlowValue": "cash",                        # IFRS
    "CashCashEquivalents": "cash",                                     # IFRS variant
    "CashAndCashEquivalents": "cash",                                  # IFRS variant
    "CashAtBankInHand": "cash",                                        # old UK GAAP
    "CashEquivalents": "cash",                                         # IFRS variant

    # --- Inventories / stocks ---
    "Stocks": "inventories",                                           # FRS 102 core
    "Inventories": "inventories",                                      # IFRS
    "StocksIncludingWorkInProgress": "inventories",                    # FRS 102 variant

    # --- Trade receivables / trade debtors ---
    "TradeDebtors": "trade_receivables",                               # FRS 102 core
    "TradeReceivables": "trade_receivables",                           # IFRS
    "TradeAndOtherReceivablesCurrent": "trade_receivables",            # IFRS
    "Debtors": "trade_receivables",                                    # UK GAAP (total debtors line)
    "DebtorsTradeReceivables": "trade_receivables",                    # FRS 102 variant

    # --- Total assets ---
    "TotalAssets": "total_assets",                                     # FRS 102 / IFRS
    "TotalAssetsLessCurrentLiabilities": "net_assets_before_lt_liab",  # FRS 102 (not total assets)
    "Assets": "total_assets",                                          # IFRS variant

    # --- Trade payables / trade creditors ---
    "TradeCreditors": "trade_payables",                                # FRS 102 core
    "TradePayables": "trade_payables",                                 # IFRS
    "TradeAndOtherPayablesCurrent": "trade_payables",                  # IFRS
    "CreditorsTradePayables": "trade_payables",                        # FRS 102 variant

    # --- Other payables / other creditors ---
    "OtherCreditors": "other_payables",                                # FRS 102 core
    "OtherPayables": "other_payables",                                 # IFRS
    "AccrualsAndDeferredIncome": "other_payables",                     # FRS 102 (often grouped with other creditors)
    "Accruals": "other_payables",                                      # FRS 102

    # --- Short-term borrowings ---
    "BankLoansOverdrafts": "short_term_borrowings",                    # FRS 102 core (within 1 year)
    "BankLoansOverdraftsDueWithinOneYear": "short_term_borrowings",    # FRS 102 variant
    "BankBorrowings": "short_term_borrowings",                         # FRS 102
    "BorrowingsDueWithinOneYear": "short_term_borrowings",             # FRS 102
    "CurrentPortionOfBorrowings": "short_term_borrowings",             # IFRS
    "BankLoansAndOverdrafts": "short_term_borrowings",                 # UK GAAP variant
    "LoansAndOverdraftsDueWithinOneYear": "short_term_borrowings",     # FRS 102 variant

    # --- Long-term borrowings ---
    "BankLoansOverdraftsDueAfterOneYear": "long_term_borrowings",      # FRS 102
    "BorrowingsDueAfterOneYear": "long_term_borrowings",               # FRS 102
    "BankLoansAndOverdraftsDueAfterMoreThanOneYear": "long_term_borrowings",  # UK GAAP
    "NonCurrentBorrowings": "long_term_borrowings",                    # IFRS
    "NoncurrentPortionOfBorrowings": "long_term_borrowings",           # IFRS
    "LongTermBorrowings": "long_term_borrowings",                      # variant

    # --- Lease liabilities (non-current) ---
    "FinanceLeaseLiabilitiesDueAfterOneYear": "lease_liabilities_noncurrent",  # FRS 102
    "ObligationsUnderFinanceLeases": "lease_liabilities_noncurrent",           # FRS 102
    "ObligationsUnderFinanceLeasesDueAfterOneYear": "lease_liabilities_noncurrent",  # FRS 102
    "LeaseLiabilitiesNoncurrent": "lease_liabilities_noncurrent",              # IFRS 16
    "NonCurrentLeaseLiabilities": "lease_liabilities_noncurrent",              # IFRS 16 variant

    # --- Provisions ---
    "Provisions": "provisions",                                        # generic
    "ProvisionsForLiabilities": "provisions",                          # FRS 102 core
    "ProvisionsForLiabilitiesCharges": "provisions",                   # FRS 102 variant
    "ProvisionsForLiabilitiesAndCharges": "provisions",                # UK GAAP
    "TotalProvisionsForLiabilities": "provisions",                     # FRS 102 variant
    "NoncurrentProvisions": "provisions",                              # IFRS

    # --- Pension obligations ---
    "DefinedBenefitObligationPresentValue": "pension_obligations",     # FRS 102
    "PensionProvision": "pension_obligations",                         # FRS 102 variant
    "NetDefinedBenefitLiabilityAsset": "pension_obligations",          # FRS 102
    "EmployeeBenefitObligations": "pension_obligations",               # IFRS
    "NetPensionLiability": "pension_obligations",                      # variant
    "RetirementBenefitObligations": "pension_obligations",             # FRS 102

    # --- Total current liabilities ---
    "CreditorsDueWithinOneYear": "total_current_liabilities",          # FRS 102 core
    "CurrentLiabilities": "total_current_liabilities",                 # IFRS
    "TotalCurrentLiabilities": "total_current_liabilities",            # IFRS variant

    # --- Total non-current liabilities ---
    "CreditorsDueAfterOneYear": "total_noncurrent_liabilities",        # FRS 102 core
    "NonCurrentLiabilities": "total_noncurrent_liabilities",           # IFRS
    "TotalNonCurrentLiabilities": "total_noncurrent_liabilities",      # IFRS variant
    "NoncurrentLiabilities": "total_noncurrent_liabilities",           # IFRS (no hyphen)

    # --- Total liabilities ---
    "TotalLiabilities": "total_liabilities",                           # IFRS
    "Liabilities": "total_liabilities",                                # IFRS variant
    "TotalCreditors": "total_liabilities",                             # FRS 102 variant

    # --- Net assets ---
    "NetAssetsLiabilities": "net_assets",                              # FRS 102 core
    "NetAssets": "net_assets",                                         # IFRS / variant
    "TotalNetAssets": "net_assets",                                    # variant

    # --- Total equity ---
    "TotalEquity": "total_equity",                                     # IFRS
    "Equity": "total_equity",                                          # IFRS variant
    "TotalShareholdersFunds": "total_equity",                          # FRS 102
    "ShareholdersFunds": "total_equity",                               # FRS 102 core
    "ShareholderFunds": "total_equity",                                # variant (typo common in filings)
    "EquityAttributableToOwnersOfParent": "total_equity",              # IFRS
    "MembersShareInterest": "total_equity",                            # FRS 102 (LLP)
    "TotalMembersInterests": "total_equity",                           # FRS 102 (LLP variant)

    # --- Share capital ---
    "CalledUpShareCapital": "share_capital",                           # FRS 102 core
    "CalledUpShareCapitalNotPaid": "share_capital",                    # FRS 102 variant
    "ShareCapital": "share_capital",                                   # IFRS
    "IssuedShareCapital": "share_capital",                             # variant
    "CalledUpShareCapitalIncludingSharePremium": "share_capital",      # FRS 102 variant
    "AllottedCalledUpFullyPaidShareCapital": "share_capital",          # FRS 102

    # =======================================================================
    # CASH FLOW STATEMENT
    # =======================================================================

    # --- Operating cash flow ---
    "NetCashFlowsFromUsedInOperatingActivities": "operating_cash_flow",         # FRS 102
    "NetCashGeneratedFromOperations": "operating_cash_flow",                    # FRS 102 variant
    "CashGeneratedFromOperations": "operating_cash_flow",                       # IFRS
    "NetCashFromOperatingActivities": "operating_cash_flow",                    # IFRS variant
    "CashFlowsFromUsedInOperatingActivities": "operating_cash_flow",           # IFRS
    "NetCashInflowOutflowFromOperatingActivities": "operating_cash_flow",      # old UK GAAP

    # --- Capex: property, plant & equipment ---
    "PurchasePropertyPlantEquipment": "capex_ppe",                             # FRS 102
    "PurchaseOfPropertyPlantAndEquipment": "capex_ppe",                        # IFRS
    "PurchaseTangibleFixedAssets": "capex_ppe",                                # FRS 102 variant
    "PurchaseOfTangibleFixedAssets": "capex_ppe",                              # variant
    "AcquisitionsDisposalsOfPropertyPlantEquipment": "capex_ppe",              # FRS 102 net variant

    # --- Capex: intangible assets ---
    "PurchaseIntangibleAssets": "capex_intangibles",                           # FRS 102
    "PurchaseOfIntangibleAssets": "capex_intangibles",                         # IFRS
    "PurchaseOfOtherIntangibleAssets": "capex_intangibles",                    # IFRS variant

    # --- Profit after tax ---
    "ProfitLossForPeriod": "profit_after_tax",                                # IFRS
    "ProfitLossForYear": "profit_after_tax",                                  # FRS 102
    "ProfitLossOnOrdinaryActivitiesAfterTax": "profit_after_tax",             # FRS 102
    "ProfitLossAfterTax": "profit_after_tax",                                 # variant
    "ProfitLossAttributableToOwnersOfParent": "profit_after_tax",             # IFRS
    "ProfitLoss": "profit_after_tax",                                         # IFRS generic
    "RetainedProfitLossForFinancialYear": "profit_after_tax",                 # FRS 102

    # --- Other operating income ---
    "OtherOperatingIncome": "other_operating_income",                         # FRS 102
    "OtherOperatingIncomeExpense": "other_operating_income",                  # IFRS
    "OtherIncome": "other_operating_income",                                  # variant

    # --- Finance income ---
    "InterestReceivableSimilarIncome": "finance_income",                      # FRS 102 core
    "InterestReceivable": "finance_income",                                   # UK GAAP
    "InterestReceivableAndSimilarIncome": "finance_income",                   # FRS 102 variant
    "FinanceIncome": "finance_income",                                        # IFRS
    "InterestIncome": "finance_income",                                       # IFRS variant

    # --- Opening / closing cash ---
    "CashCashEquivalentsAtBeginningOfPeriod": "opening_cash",                 # IFRS
    "CashCashEquivalentsBeginningPeriod": "opening_cash",                     # variant
    "CashEquivalentsAtBeginningOfPeriod": "opening_cash",                     # variant
    "CashCashEquivalentsAtEndOfPeriod": "closing_cash",                       # IFRS
    "CashCashEquivalentsEndPeriod": "closing_cash",                           # variant
    "CashEquivalentsAtEndOfPeriod": "closing_cash",                           # variant

    # --- Net change in cash ---
    "IncreaseDecreaseInCashCashEquivalents": "net_change_cash",               # IFRS
    "NetIncreaseDecreaseInCashCashEquivalents": "net_change_cash",            # variant
    "ChangeInCashCashEquivalents": "net_change_cash",                         # FRS 102
    "IncreaseDecreaseInCashEquivalents": "net_change_cash",                   # variant

    # --- Net cash from operating ---
    "NetCashFromOperatingActivities": "net_cash_operating",                   # IFRS
    "CashFlowsFromUsedInOperatingActivitiesTotal": "net_cash_operating",      # variant

    # --- Net cash from investing ---
    "NetCashFlowsFromUsedInInvestingActivities": "net_cash_investing",        # FRS 102
    "CashFlowsFromUsedInInvestingActivities": "net_cash_investing",           # IFRS
    "NetCashFromInvestingActivities": "net_cash_investing",                   # IFRS variant
    "NetCashUsedInInvestingActivities": "net_cash_investing",                 # variant

    # --- Net cash from financing ---
    "NetCashFlowsFromUsedInFinancingActivities": "net_cash_financing",        # FRS 102
    "CashFlowsFromUsedInFinancingActivities": "net_cash_financing",           # IFRS
    "NetCashFromFinancingActivities": "net_cash_financing",                   # IFRS variant
    "NetCashUsedInFinancingActivities": "net_cash_financing",                 # variant

    # --- Retained earnings ---
    "RetainedEarningsAccumulatedLosses": "retained_earnings",                 # IFRS
    "RetainedEarnings": "retained_earnings",                                  # variant
    "ProfitLossAccountReserve": "retained_earnings",                          # FRS 102
    "ProfitAndLossAccount": "retained_earnings",                              # FRS 102 core
    "ProfitAndLossAccountBalance": "retained_earnings",                       # variant
    "AccumulatedProfitLoss": "retained_earnings",                             # IFRS

    # --- Share premium ---
    "SharePremiumAccount": "share_premium",                                   # FRS 102 core
    "SharePremium": "share_premium",                                          # IFRS
    "SharePremiumReserve": "share_premium",                                   # variant

    # --- Other reserves ---
    "OtherReserves": "other_reserves",                                        # IFRS
    "OtherReservesTotal": "other_reserves",                                   # variant
    "RevaluationReserve": "other_reserves",                                   # FRS 102
    "CapitalRedemptionReserve": "other_reserves",                             # FRS 102
    "MergerReserve": "other_reserves",                                        # FRS 102

    # --- Minority interest ---
    "MinorityInterest": "minority_interest",                                  # FRS 102
    "NonControllingInterest": "minority_interest",                            # IFRS
    "NonControllingInterests": "minority_interest",                           # variant
    "EquityAttributableToNonControllingInterests": "minority_interest",       # IFRS

    # --- Intangible assets ---
    "IntangibleAssets": "intangible_assets",                                  # FRS 102 / IFRS
    "IntangibleAssetsNet": "intangible_assets",                               # variant
    "TotalIntangibleFixedAssets": "intangible_assets",                        # FRS 102
    "IntangibleAssetsOtherThanGoodwill": "intangible_assets",                 # IFRS

    # --- Goodwill ---
    "Goodwill": "goodwill",                                                   # FRS 102 / IFRS
    "GoodwillNet": "goodwill",                                                # variant
    "GoodwillGross": "goodwill",                                              # variant

    # --- Tangible fixed assets ---
    "TangibleFixedAssets": "tangible_fixed_assets",                           # FRS 102 core
    "PropertyPlantEquipment": "tangible_fixed_assets",                        # IFRS
    "PropertyPlantAndEquipment": "tangible_fixed_assets",                     # IFRS variant
    "TotalTangibleFixedAssets": "tangible_fixed_assets",                      # FRS 102
    "PropertyPlantEquipmentNet": "tangible_fixed_assets",                     # variant

    # --- Right of use assets ---
    "RightOfUseAssets": "right_of_use_assets",                                # IFRS 16
    "RightOfUseAssetsNet": "right_of_use_assets",                             # variant
    "LeasedAssetsRightOfUse": "right_of_use_assets",                          # variant

    # --- Investment properties ---
    "InvestmentProperty": "investment_properties",                            # IFRS
    "InvestmentProperties": "investment_properties",                          # variant
    "InvestmentPropertyFairValue": "investment_properties",                   # IFRS variant

    # --- Fixed asset investments ---
    "InvestmentsFixedAssets": "investments_fixed",                            # FRS 102
    "FixedAssetInvestments": "investments_fixed",                             # FRS 102 variant
    "Investments": "investments_fixed",                                       # generic
    "InvestmentsInSubsidiaries": "investments_fixed",                         # FRS 102
    "InvestmentsInAssociates": "investments_fixed",                           # FRS 102
    "OtherInvestmentsNoncurrent": "investments_fixed",                        # IFRS

    # --- Other receivables ---
    "OtherDebtors": "other_receivables",                                      # FRS 102
    "OtherReceivables": "other_receivables",                                  # IFRS
    "OtherReceivablesCurrent": "other_receivables",                           # IFRS variant
    "PrepaymentsAccruedIncome": "other_receivables",                          # FRS 102
    "Prepayments": "other_receivables",                                       # variant

    # --- Short-term investments ---
    "ShortTermInvestments": "short_term_investments",                         # generic
    "CurrentAssetInvestments": "short_term_investments",                      # FRS 102
    "OtherFinancialAssetsCurrent": "short_term_investments",                  # IFRS

    # --- Repayment of borrowings ---
    "RepaymentBorrowings": "repayment_borrowings",                             # FRS 102
    "RepaymentsOfBorrowings": "repayment_borrowings",                          # IFRS
    "RepaymentOfBorrowings": "repayment_borrowings",                           # variant
    "RepaymentOfLoans": "repayment_borrowings",                                # FRS 102 variant
    "RepaymentsOfLoans": "repayment_borrowings",                               # variant

    # --- Proceeds from borrowings ---
    "ProceedsFromBorrowings": "proceeds_borrowings",                           # IFRS
    "NewBankLoansReceived": "proceeds_borrowings",                             # FRS 102
    "ProceedsFromLoans": "proceeds_borrowings",                                # variant
    "ProceedsOfNewBorrowings": "proceeds_borrowings",                          # FRS 102 variant
    "NewBorrowings": "proceeds_borrowings",                                    # FRS 102

    # --- Proceeds from disposal of PPE ---
    "ProceedsFromDisposalOfPropertyPlantEquipment": "proceeds_disposal_ppe",   # IFRS
    "ProceedsSalePropertyPlantEquipment": "proceeds_disposal_ppe",             # FRS 102
    "ProceedsFromSaleOfTangibleFixedAssets": "proceeds_disposal_ppe",          # FRS 102 variant
    "ProceedsOfDisposalsOfPropertyPlantEquipment": "proceeds_disposal_ppe",    # variant
    "ProceedsSaleTangibleFixedAssets": "proceeds_disposal_ppe",                # FRS 102 variant

    # --- Dividends paid ---
    "DividendsPaid": "dividends_paid_cf",                                      # FRS 102 / IFRS
    "DividendsPaidToEquityHolders": "dividends_paid_cf",                       # IFRS variant
    "EquityDividendsPaid": "dividends_paid_cf",                                # FRS 102 variant
    "DividendsPaidClassifiedAsFinancingActivities": "dividends_paid_cf",       # IFRS

    # --- Lease payments ---
    "PaymentsOfFinanceLeaseObligations": "lease_payments",                     # FRS 102
    "PaymentsOfFinanceLeaseLiabilities": "lease_payments",                     # IFRS
    "CapitalElementFinanceLeaseRentalPayments": "lease_payments",              # FRS 102 variant
    "RepaymentsOfObligationsUnderFinanceLeases": "lease_payments",             # FRS 102
    "PaymentsForLeaseLiabilities": "lease_payments",                           # IFRS 16
    "LeaseLiabilityPayments": "lease_payments",                                # IFRS 16 variant

    # --- Tax paid ---
    "TaxPaid": "tax_paid",                                                     # FRS 102
    "IncomeTaxesPaid": "tax_paid",                                             # IFRS
    "TaxesPaid": "tax_paid",                                                   # variant
    "TaxPaidRefundedClassifiedAsOperatingActivities": "tax_paid",              # IFRS
    "UKCorporationTaxPaid": "tax_paid",                                        # FRS 102 variant
}


def _parse_ixbrl_value(tag):
    """Extract the numeric value from an ix:nonFraction tag, applying scale and sign."""
    raw = tag.text.strip().replace(",", "").replace(" ", "")
    if not raw or raw == "-":
        return None
    try:
        value = float(raw)
    except ValueError:
        return None

    # Apply scale attribute (e.g. scale="3" means thousands)
    scale = int(tag.get("scale", "0"))
    if scale:
        value *= 10 ** scale

    # Apply sign attribute
    if tag.get("sign") == "-":
        value = -value

    return value


def _resolve_contexts(soup):
    """Build a dict mapping context id -> {'year': int, 'type': 'duration'|'instant', 'dimensional': bool}.

    We extract the end-date year for duration contexts and the instant year for
    instant contexts. Contexts with segment/scenario dimensions are flagged.
    """
    contexts = {}
    years_seen = set()

    for ctx in soup.find_all("xbrli:context"):
        ctx_id = ctx.get("id", "")
        period = ctx.find("xbrli:period")
        if not period:
            continue

        # Check if this context has dimensional qualifiers (segments)
        has_dimension = ctx.find("xbrli:segment") is not None

        instant = period.find("xbrli:instant")
        end_date = period.find("xbrli:enddate")

        if end_date:
            year = _extract_year(end_date.text.strip())
            contexts[ctx_id] = {"year": year, "type": "duration", "dimensional": has_dimension}
            if year:
                years_seen.add(year)
        elif instant:
            year = _extract_year(instant.text.strip())
            contexts[ctx_id] = {"year": year, "type": "instant", "dimensional": has_dimension}
            if year:
                years_seen.add(year)

    return contexts, years_seen


def _extract_year(date_str):
    """Pull a 4-digit year from a date string like '2025-04-30'."""
    m = re.match(r"(\d{4})", date_str)
    return int(m.group(1)) if m else None


# Which canonical fields belong to which statement
INCOME_STATEMENT_FIELDS = [
    "revenue", "cost_of_sales", "gross_profit", "distribution_costs",
    "admin_expenses", "other_operating_income", "employee_costs",
    "depreciation", "amortisation", "operating_profit",
    "finance_income", "finance_costs", "profit_before_tax",
    "tax_expense", "profit_after_tax",
]
BALANCE_SHEET_FIELDS = [
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
CASH_FLOW_FIELDS = [
    "operating_cash_flow", "net_cash_operating",
    "capex_ppe", "capex_intangibles", "net_cash_investing",
    "repayment_borrowings", "proceeds_borrowings",
    "proceeds_disposal_ppe", "dividends_paid_cf",
    "lease_payments", "tax_paid", "net_cash_financing",
    "opening_cash", "closing_cash", "net_change_cash",
]

# ---------------------------------------------------------------------------
# iXBRL Part B — Qualitative section extraction constants
# ---------------------------------------------------------------------------

IXBRL_SECTION_IDS = [
    "strategic_report",
    "section_172",
    "principal_risks",
    "viability_statement",
    "directors_report",
    "principal_activity",
    "going_concern_directors",
    "directors_responsibilities",
    "auditor_report",
    "accounting_policies",
    "critical_estimates",
    "going_concern_note",
]

# Stage 2 — Boolean/string flag tags for filing classification
FLAG_TAGS = {
    "bus:EntityDormantTruefalse":                           "entity_dormant",
    "bus:AccountsStatusAuditedOrUnaudited":                 "audit_status",
    "bus:AccountsType":                                     "accounts_type",
    "bus:ReportIncludesStrategicReportTruefalse":           "has_strategic_report",
    "bus:ReportIncludesDetailedProfitLossStatementTruefalse": "has_detailed_pl",
    "direp:EntityHasTakenExemptionUnderCompaniesActInNotPublishingItsOwnProfitLossAccountTruefalse": "pl_omitted_exemption",
    "core:FinancialStatementsArePreparedOnGoing-concernBasisTruefalse": "going_concern_asserted",
    "bus:ApplicableLegislation":                            "applicable_legislation",
    "bus:AccountingStandardsApplied":                       "accounting_standards",
}

# Stage 2 — Presence-only declaration tags (reliable on small filings)
DECLARATION_TAGS = {
    "direp:StatementThatAccountsHaveBeenPreparedInAccordanceWithProvisionsSmallCompaniesRegime": "small_regime",
    "direp:StatementThatCompanyEntitledToExemptionFromAuditUnderSection477CompaniesAct2006RelatingToSmallCompanies": "audit_exempt_s477",
    "direp:StatementThatMembersHaveNotRequiredCompanyToObtainAnAudit": "members_waived_audit",
    "direp:StatementThatDirectorsHaveElectedNotToDeliverProfitLossAccountUnderSection4445ACompaniesAct2006": "filleted_s444_5a",
    "direp:StatementThatCompanyHasActedAsSmallCompanyForPreparationAccounts": "prepared_as_small",
    "direp:StatementThatAccountsHaveBeenPreparedInAccordanceWithProvisionsMediumSizedCompaniesRegime": "medium_regime",
}

# Stage 3 — Narrative tag map: (family, local_name) -> (section_id, subsection_key)
NARRATIVE_TAG_MAP = {
    # Principal Activity
    ("bus",   "DescriptionPrincipalActivities"):                                    ("principal_activity", None),

    # Directors' Responsibilities
    ("direp", "StatementThatDirectorsAcknowledgeTheirResponsibilitiesUnderCompaniesAct"): ("directors_responsibilities", None),
    ("direp", "StatementOnQualityCompletenessInformationProvidedToAuditors"):      ("directors_responsibilities", "audit_info_quality"),

    # Auditor's Report (6 sub-sections)
    ("aurep", "OpinionAuditorsOnEntity"):                                          ("auditor_report", "opinion"),
    ("aurep", "BasisForOpinionAuditorsOnEntity"):                                  ("auditor_report", "basis_for_opinion"),
    ("aurep", "StatementOnScopeAuditReport"):                                      ("auditor_report", "scope"),
    ("aurep", "StatementResponsibilitiesManagementThoseChargedWithCorporateGovernance"): ("auditor_report", "management_responsibilities"),
    ("aurep", "StatementAuditorsResponsibilitiesRelatingToOtherInformation"):      ("auditor_report", "auditors_responsibilities_other_info"),
    ("aurep", "StatementOnMattersOnWhichAuditorReportsByException"):               ("auditor_report", "matters_by_exception"),

    # Critical Estimates and Judgements
    ("core",  "GeneralDescriptionCriticalEstimatesJudgements"):                    ("critical_estimates", None),

    # Accounting Policies (per-topic, multi-tag concatenation)
    ("core",  "RevenueRecognitionPolicy"):                                         ("accounting_policies", "revenue_recognition"),
    ("core",  "PropertyPlantEquipmentPolicy"):                                     ("accounting_policies", "ppe"),
    ("core",  "ProvisionsPolicy"):                                                 ("accounting_policies", "provisions"),
    ("core",  "ImpairmentNon-financialAssetsPolicy"):                              ("accounting_policies", "impairment"),
    ("core",  "FinancialInstrumentsRecognitionMeasurementPolicy"):                 ("accounting_policies", "financial_instruments"),
    ("core",  "CurrentIncomeTaxPolicy"):                                           ("accounting_policies", "current_tax"),
    ("core",  "DeferredTaxPolicy"):                                                ("accounting_policies", "deferred_tax"),
    ("core",  "DefinedContributionPensionsPolicy"):                                ("accounting_policies", "pensions_dc"),
    ("core",  "DefinedBenefitPensionsPolicy"):                                     ("accounting_policies", "pensions_db"),
    ("core",  "LesseeFinanceLeasePolicy"):                                         ("accounting_policies", "leases_finance_lessee"),
    ("core",  "LessorOperatingLeasePolicy"):                                       ("accounting_policies", "leases_operating_lessor"),
    ("core",  "ForeignCurrencyTranslationOperationsPolicy"):                       ("accounting_policies", "foreign_currency"),
    ("core",  "FunctionalPresentationCurrencyPolicy"):                             ("accounting_policies", "functional_currency"),
    ("core",  "GovernmentGrantsOtherGovernmentAssistancePolicy"):                  ("accounting_policies", "government_grants"),
    ("core",  "StatementComplianceWithApplicableReportingFramework"):              ("accounting_policies", "compliance_framework"),
    ("core",  "GeneralDescriptionBasisMeasurementUsedInPreparingFinancialStatements"): ("accounting_policies", "basis_of_preparation"),

    # Section 172 (large filings only)
    ("core",  "StatementOnSection172CompaniesAct2006"):                            ("section_172", None),

    # Principal Risks (when tagged; rare below large)
    ("core",  "DescriptionPrincipalRisksUncertaintiesFacingEntity"):               ("principal_risks", None),

    # Viability Statement (large listed only)
    ("core",  "StatementViabilityEntity"):                                         ("viability_statement", None),
}

AUDITOR_SUBSECTION_ORDER = {
    "opinion": 1,
    "basis_for_opinion": 2,
    "scope": 3,
    "management_responsibilities": 4,
    "auditors_responsibilities_other_info": 5,
    "matters_by_exception": 6,
}

# Stage 4 — Hard anchors for text-layer fallback (same regexes as pdf_parser)
_IXBRL_HARD_ANCHORS = {
    "strategic_report":          r"\bSTRATEGIC\s+REPORT\b",
    "directors_report":          r"\bDIRECTORS[\u2019']?\s*REPORT\b|\bREPORT\s+OF\s+THE\s+DIRECTORS\b",
    "section_172":               r"\bSECTION\s*172(?:\s*\(1\))?\b|\bS\.?\s*172\b",
    "principal_risks":           r"\bPRINCIPAL\s+RISKS(?:\s+AND\s+UNCERTAINTIES)?\b",
    "viability_statement":       r"\bVIABILITY\s+STATEMENT\b",
    "going_concern_directors":   r"\bGOING\s+CONCERN\b",
    "going_concern_note":        r"\bGOING\s+CONCERN\b",
    "directors_responsibilities": r"STATEMENT\s+OF\s+DIRECTORS[\u2019']?\s*RESPONSIBILITIES",
    "auditor_report":            r"INDEPENDENT\s+AUDITOR[\u2019']?S?[\u2019']?\s*REPORT",
    "principal_activity":        r"\bPRINCIPAL\s+ACTIVIT(?:Y|IES)\b",
    "accounting_policies":       r"\bACCOUNTING\s+POLICIES\b|\bBASIS\s+OF\s+PREPARATION\b",
    "critical_estimates":        r"\bCRITICAL\s+(?:ACCOUNTING\s+)?(?:JUDGEMENTS|ESTIMATES)\b|\bKEY\s+SOURCES\s+OF\s+ESTIMATION\b",
}


def _extract_field_values(soup, contexts):
    """Extract all mapped numeric values from ix:nonFraction tags.

    Returns field_values dict: {field_name: {year(int): value}}
    """
    field_values = {}

    for tag in soup.find_all("ix:nonfraction"):
        name = tag.get("name", "")
        local_name = name.split(":")[-1] if ":" in name else name

        field = IXBRL_TAG_MAP.get(local_name)
        if not field:
            continue

        ctx_id = tag.get("contextref", "")
        ctx = contexts.get(ctx_id)
        if not ctx or not ctx["year"]:
            continue

        if ctx.get("dimensional"):
            continue

        value = _parse_ixbrl_value(tag)
        if value is None:
            continue

        year = ctx["year"]
        if field not in field_values:
            field_values[field] = {}
        if year not in field_values[field]:
            field_values[field][year] = value

    return field_values


def _build_statement(field_values, field_list, years):
    """Build a JSONB-style dict keyed by year string, each year containing all fields (null if missing)."""
    statement = {}
    for year in sorted(years):
        yr_str = str(year)
        row = {}
        for field in field_list:
            row[field] = field_values.get(field, {}).get(year)
        statement[yr_str] = row
    return statement


def parse_ixbrl(content, company_number):
    """Parse an iXBRL document and return a dict matching the DB schema.

    Returns None if no useful data could be extracted.
    """
    soup = BeautifulSoup(content, "html.parser")
    contexts, years_seen = _resolve_contexts(soup)

    if not contexts:
        log.warning("  No xbrli:context elements found")
        return None

    field_values = _extract_field_values(soup, contexts)

    if not field_values:
        log.warning("  No recognised financial tags found in iXBRL")
        return None

    result = {}
    derivation_log = {}

    sorted_years = sorted(years_seen, reverse=True)
    current_year = sorted_years[0] if sorted_years else None

    def get_val(field, year=None):
        if year is None:
            year = current_year
        return field_values.get(field, {}).get(year)

    # --- Build the 3 statement JSONB objects ---
    result["income_statement"] = _build_statement(field_values, INCOME_STATEMENT_FIELDS, years_seen)
    result["balance_sheet"] = _build_statement(field_values, BALANCE_SHEET_FIELDS, years_seen)
    result["cash_flow_statement"] = _build_statement(field_values, CASH_FLOW_FIELDS, years_seen)

    # --- Display columns (current year) ---
    revenue = get_val("revenue")
    if revenue is not None:
        result["revenue"] = revenue

    # EBITDA = operating_profit + depreciation + amortisation
    op = get_val("operating_profit")
    dep = get_val("depreciation") or 0
    amort = get_val("amortisation") or 0
    if op is not None:
        ebitda = op + abs(dep) + abs(amort)
        result["ebitda"] = ebitda
        derivation_log["ebitda"] = f"operating_profit ({op}) + |depreciation| ({abs(dep)}) + |amortisation| ({abs(amort)})"

    emp = get_val("employees")
    if emp is not None:
        result["employees"] = int(emp)

    # FCF = operating cashflow - capex
    ocf = get_val("operating_cash_flow")
    capex_ppe = get_val("capex_ppe")
    capex_int = get_val("capex_intangibles")
    total_capex = (abs(capex_ppe) if capex_ppe else 0) + (abs(capex_int) if capex_int else 0)
    if ocf is not None:
        result["fcf"] = ocf - total_capex
        derivation_log["fcf"] = f"operating_cash_flow ({ocf}) - |capex_ppe| ({abs(capex_ppe) if capex_ppe else 0}) - |capex_intangibles| ({abs(capex_int) if capex_int else 0})"

    # --- History columns ---
    rev_history = {}
    for year, val in sorted(field_values.get("revenue", {}).items()):
        rev_history[str(year)] = val
    if rev_history:
        result["revenue_history"] = rev_history

    ebitda_history = {}
    op_by_year = field_values.get("operating_profit", {})
    dep_by_year = field_values.get("depreciation", {})
    amort_by_year = field_values.get("amortisation", {})
    for year in sorted(op_by_year.keys()):
        y_op = op_by_year[year]
        y_dep = dep_by_year.get(year, 0)
        y_amort = amort_by_year.get(year, 0)
        ebitda_history[str(year)] = y_op + abs(y_dep) + abs(y_amort)
    if ebitda_history:
        result["ebitda_history"] = ebitda_history
        derivation_log["ebitda_history"] = "operating_profit + |depreciation| + |amortisation| per year"

    # FCF history
    ocf_by_year = field_values.get("operating_cash_flow", {})
    capex_ppe_by_year = field_values.get("capex_ppe", {})
    capex_int_by_year = field_values.get("capex_intangibles", {})
    fcf_history = {}
    for year in sorted(ocf_by_year.keys()):
        y_ocf = ocf_by_year[year]
        y_capex = abs(capex_ppe_by_year.get(year, 0)) + abs(capex_int_by_year.get(year, 0))
        fcf_history[str(year)] = y_ocf - y_capex
    if fcf_history:
        result["fcf_history"] = fcf_history
        derivation_log["fcf_history"] = "operating_cash_flow - |capex_ppe| - |capex_intangibles| per year"

    # Employees history
    emp_history = {}
    for year, val in sorted(field_values.get("employees", {}).items()):
        emp_history[str(year)] = int(val)
    if emp_history:
        result["employees_history"] = emp_history

    result["derivation_log"] = derivation_log

    return result


# ---------------------------------------------------------------------------
# iXBRL Part B — Qualitative section extraction functions
# ---------------------------------------------------------------------------

def _classify_ixbrl_filing(soup):
    """Stage 2: Classify filing using boolean flags and declaration tag presence.

    Returns dict with keys: size, mode, audited, dormant, going_concern_asserted,
    flags, declarations.
    """
    # Source A — boolean/string flag tags
    flags = {}
    for tag_name, field in FLAG_TAGS.items():
        el = soup.find("ix:nonnumeric", {"name": tag_name})
        if el is None:
            flags[field] = None
        else:
            txt = el.get_text(" ", strip=True)
            flags[field] = txt if txt else ""

    # Source B — declaration presence tags
    decl = {field: (soup.find("ix:nonnumeric", {"name": tag}) is not None)
            for tag, field in DECLARATION_TAGS.items()}

    # Size
    if decl.get("small_regime") or decl.get("prepared_as_small") or decl.get("audit_exempt_s477"):
        size = "small"
    elif decl.get("medium_regime") or flags.get("has_strategic_report") == "true":
        size = "medium"
    else:
        size = "unknown"

    # Mode
    if decl.get("filleted_s444_5a") or flags.get("pl_omitted_exemption") == "true":
        mode = "filleted"
    else:
        mode = "full"

    # Audit status
    if decl.get("audit_exempt_s477") or decl.get("members_waived_audit"):
        audited = False
    elif flags.get("audit_status") == "audited":
        audited = True
    else:
        audited = None

    return {
        "size": size, "mode": mode, "audited": audited,
        "dormant": flags.get("entity_dormant") == "true",
        "going_concern_asserted": flags.get("going_concern_asserted") == "true",
        "flags": flags, "declarations": decl,
    }


def _extract_tag_layer(soup):
    """Stage 3: Walk ix:nonNumeric elements and map to sections via NARRATIVE_TAG_MAP.

    Returns (sections, source_tags, subsections).
    """
    sections = {k: [] for k in IXBRL_SECTION_IDS}
    source_tags = {k: [] for k in IXBRL_SECTION_IDS}
    subsections = {k: {} for k in IXBRL_SECTION_IDS}
    unknown_tags = []

    for el in soup.find_all("ix:nonnumeric"):
        name = el.get("name", "")
        if not name or ":" not in name:
            continue

        prefix, local_name = name.split(":", 1)

        # Check if this is a flag/declaration tag (skip — handled by Stage 2)
        if name in FLAG_TAGS or name in DECLARATION_TAGS:
            continue

        # Look up in narrative tag map using prefix as family
        key = (prefix, local_name)
        mapping = NARRATIVE_TAG_MAP.get(key)
        if mapping is None:
            unknown_tags.append(name)
            continue

        section_id, subsection_key = mapping
        text = el.get_text(" ", strip=True)
        if not text:
            continue

        sort_order = AUDITOR_SUBSECTION_ORDER.get(subsection_key, 999)
        sections[section_id].append((sort_order, text))
        source_tags[section_id].append(name)
        if subsection_key:
            subsections[section_id][subsection_key] = text

    # Sort by canonical order and concatenate
    for sid in IXBRL_SECTION_IDS:
        sections[sid].sort(key=lambda x: x[0])
        sections[sid] = "\n\n".join(t for _, t in sections[sid])

    if unknown_tags:
        log.debug("  Unknown narrative tags (%d): %s", len(unknown_tags),
                  ", ".join(sorted(set(unknown_tags))[:10]))

    return sections, source_tags, subsections


def _extract_text_layer(body_text, sections_already_found):
    """Stage 4: Regex fallback for sections not populated by tag layer.

    Searches body_text for statutory phrase headings and slices text between them.
    Returns {section_id: {"text": str, "offsets": [start, end], "source": "ixbrl_text"}}.
    """
    results = {}

    # Find all heading matches with their character offsets
    matches = []
    for section_id, pattern in _IXBRL_HARD_ANCHORS.items():
        if section_id in sections_already_found:
            continue
        for m in re.finditer(pattern, body_text, re.IGNORECASE):
            matches.append((m.start(), section_id, m))

    # Sort by position in document
    matches.sort(key=lambda x: x[0])

    # For each match, slice text from heading to the next heading (or end)
    for i, (start_offset, section_id, _match) in enumerate(matches):
        # Skip if we already have this section (e.g. going_concern matches twice)
        if section_id in results:
            continue

        # End at next heading or end of document
        if i + 1 < len(matches):
            end_offset = matches[i + 1][0]
        else:
            end_offset = len(body_text)

        text = body_text[start_offset:end_offset].strip()
        if text:
            results[section_id] = {
                "text": text,
                "offsets": [start_offset, end_offset],
                "source": "ixbrl_text",
            }

    return results


def _ixbrl_expected_sections(classification):
    """Stage 5: Which sections should exist given (size, mode, audited).

    Returns {section_id: True (expected) | False (not expected)}.
    """
    size = classification.get("size", "unknown")
    mode = classification.get("mode", "full")
    audited = classification.get("audited")

    is_small = size == "small"
    is_medium = size in ("medium",)
    is_large = size == "large" or size == "unknown"
    is_filleted = mode == "filleted"

    expected = {}
    expected["strategic_report"] = is_medium or is_large
    expected["section_172"] = is_large
    expected["principal_risks"] = is_medium or is_large
    expected["viability_statement"] = False  # listed only — rare
    expected["directors_report"] = (is_small and not is_filleted) or is_medium or is_large
    expected["principal_activity"] = (is_small and not is_filleted) or is_medium or is_large
    expected["going_concern_directors"] = (is_small and not is_filleted) or is_medium or is_large
    expected["directors_responsibilities"] = True  # always expected
    expected["auditor_report"] = audited is True or is_medium or is_large
    expected["accounting_policies"] = True
    expected["critical_estimates"] = not is_small or (is_small and not is_filleted)
    expected["going_concern_note"] = True

    return expected


def _resolve_ixbrl_sections(tag_sections, text_sections, classification,
                            source_tags, subsections):
    """Stages 5+6: Resolve each section and emit output dict."""
    expected = _ixbrl_expected_sections(classification)
    output = {}

    # Reasons for not_present by section
    not_present_reasons = {
        "strategic_report": "Not required for {size} filings",
        "section_172": "Not required for {size} filings (large only)",
        "principal_risks": "Not required for {size} filings",
        "viability_statement": "Only required for premium-listed companies",
        "directors_report": "Not delivered in {mode} filings",
        "principal_activity": "Not delivered in {mode} filings",
        "going_concern_directors": "Not delivered in {mode} filings",
        "directors_responsibilities": "Expected but not found",
        "auditor_report": "Company is audit-exempt",
        "accounting_policies": "Expected but not found",
        "critical_estimates": "Not required for {size} {mode} filings",
        "going_concern_note": "Expected but not found",
    }

    for sid in IXBRL_SECTION_IDS:
        tag_text = tag_sections.get(sid, "")
        text_result = text_sections.get(sid)

        if tag_text:
            output[sid] = {
                "status": "found",
                "text": tag_text,
                "source": "ixbrl_tag",
                "offsets": None,
                "pages": None,
                "signals": ["ixbrl_tag"],
                "source_tags": source_tags.get(sid, []),
                "subsections": subsections.get(sid) or None,
                "confidence": "high",
            }
        elif text_result:
            output[sid] = {
                "status": "found",
                "text": text_result["text"],
                "source": "ixbrl_text",
                "offsets": text_result.get("offsets"),
                "pages": None,
                "signals": ["hard_anchor"],
                "source_tags": [],
                "subsections": None,
                "confidence": "medium",
            }
        elif not expected.get(sid, True):
            reason = not_present_reasons.get(sid, "Not expected for this filing type")
            reason = reason.format(
                size=classification.get("size", "unknown"),
                mode=classification.get("mode", "full"),
            )
            output[sid] = {
                "status": "not_present",
                "reason": reason,
                "source": None, "text": None, "offsets": None, "pages": None,
            }
        else:
            output[sid] = {
                "status": "not_found",
                "reason": None,
                "source": None, "text": None, "offsets": None, "pages": None,
            }

    # Special case: going_concern_directors as flag assertion
    if (output["going_concern_directors"]["status"] != "found"
            and classification.get("going_concern_asserted")):
        output["going_concern_directors"] = {
            "status": "found",
            "text": "Financial statements are prepared on a going concern basis.",
            "source": "ixbrl_flag",
            "offsets": None,
            "pages": None,
            "signals": ["ixbrl_flag"],
            "source_tags": ["core:FinancialStatementsArePreparedOnGoing-concernBasisTruefalse"],
            "subsections": None,
            "confidence": "high",
        }

    return output


def _extract_ixbrl_sections(soup, content):
    """Orchestrate iXBRL Part B qualitative extraction (Stages 1-6).

    Fail-soft: returns None on any error so Part A is never affected.
    """
    try:
        # Stage 1 — strip ix:header for body text (separate soup copy)
        soup_for_text = BeautifulSoup(content, "html.parser")
        for header in soup_for_text.find_all("ix:header"):
            header.decompose()
        body_el = soup_for_text.find("body") or soup_for_text
        body_text = body_el.get_text("\n", strip=True)

        # Stage 2 — classify filing
        classification = _classify_ixbrl_filing(soup)

        # Stage 3 — tag-layer extraction
        tag_sections, source_tags, subsections = _extract_tag_layer(soup)

        # Stage 4 — text-layer fallback for unpopulated sections
        populated = {sid for sid, txt in tag_sections.items() if txt}
        text_sections = _extract_text_layer(body_text, populated)

        # Stages 5+6 — resolve and emit
        sections = _resolve_ixbrl_sections(
            tag_sections, text_sections, classification,
            source_tags, subsections)

        found_count = sum(1 for s in sections.values() if s.get("status") == "found")
        log.info("  Part B: %d/%d sections found (classification: %s/%s/%s)",
                 found_count, len(IXBRL_SECTION_IDS),
                 classification["size"], classification["mode"],
                 "audited" if classification["audited"] else
                 "unaudited" if classification["audited"] is False else "unknown")

        return {
            "_classification": classification,
            "sections": sections,
        }
    except Exception as e:
        log.warning("  iXBRL Part B (qualitative) failed: %s", e)
        return None


def _merge_statement(target, source):
    """Merge source statement into target. Only adds years/fields not already present."""
    for yr_str, row in source.items():
        if yr_str not in target:
            target[yr_str] = row
        else:
            for field, val in row.items():
                if target[yr_str].get(field) is None and val is not None:
                    target[yr_str][field] = val


def _merge_history(target, source):
    """Merge source {yr_str: val} into target, keeping earliest occurrence."""
    for yr_str, val in source.items():
        if yr_str not in target:
            target[yr_str] = val


def parse_ixbrl_multi(company_number, filings):
    """Fetch and parse up to 5 iXBRL filings, merging all data across them.

    Returns (merged_result, filing_format, last_accounts_date, ixbrl_sections)
    or (None, None, None, None).
    """
    merged_income = {}
    merged_balance = {}
    merged_cashflow = {}
    merged_rev_hist = {}
    merged_ebitda_hist = {}
    merged_fcf_hist = {}
    merged_emp_hist = {}

    latest_result = None
    filing_format = None
    last_accounts_date = None
    ixbrl_sections = None

    for i, filing in enumerate(filings):
        # Skip pre-2015 filings (pre-FRS 102 label vocabulary)
        filing_date = filing.get("date", "")
        if filing_date and filing_date < "2015":
            log.info("    Filing %d: %s — skipping (pre-2015)", i + 1, filing_date)
            continue

        metadata = get_document_metadata(filing)
        if not metadata:
            continue

        fmt = determine_filing_format(metadata, filing)
        if fmt != "ixbrl":
            log.info("    Filing %d is %s, skipping", i + 1, fmt)
            continue

        try:
            content, _ = download_document(metadata, fmt)
        except Exception as e:
            log.warning("    Filing %d download failed: %s", i + 1, e)
            continue

        log.info("    Filing %d: %s (%d bytes)", i + 1, filing.get("date", "?"), len(content))

        parsed = parse_ixbrl(content, company_number)
        if parsed is None:
            continue

        if filing_format is None:
            filing_format = fmt
            last_accounts_date = filing.get("date")
            latest_result = parsed
            # Extract qualitative sections from the most recent filing only
            try:
                first_soup = BeautifulSoup(content, "html.parser")
                ixbrl_sections = _extract_ixbrl_sections(first_soup, content)
            except Exception as e:
                log.warning("  iXBRL Part B failed for filing %d: %s", i + 1, e)

        # Merge statements
        _merge_statement(merged_income, parsed.get("income_statement", {}))
        _merge_statement(merged_balance, parsed.get("balance_sheet", {}))
        _merge_statement(merged_cashflow, parsed.get("cash_flow_statement", {}))

        # Merge histories
        _merge_history(merged_rev_hist, parsed.get("revenue_history", {}))
        _merge_history(merged_ebitda_hist, parsed.get("ebitda_history", {}))
        _merge_history(merged_fcf_hist, parsed.get("fcf_history", {}))
        _merge_history(merged_emp_hist, parsed.get("employees_history", {}))

    if latest_result is None:
        return None, None, None, ixbrl_sections

    # Replace with merged data (sorted by year)
    def _sort_dict(d):
        return {k: d[k] for k in sorted(d.keys())}

    latest_result["income_statement"] = _sort_dict(merged_income)
    latest_result["balance_sheet"] = _sort_dict(merged_balance)
    latest_result["cash_flow_statement"] = _sort_dict(merged_cashflow)

    if merged_rev_hist:
        latest_result["revenue_history"] = _sort_dict(merged_rev_hist)
    if merged_ebitda_hist:
        latest_result["ebitda_history"] = _sort_dict(merged_ebitda_hist)
    if merged_fcf_hist:
        latest_result["fcf_history"] = _sort_dict(merged_fcf_hist)
    if merged_emp_hist:
        latest_result["employees_history"] = _sort_dict(merged_emp_hist)

    return latest_result, filing_format, last_accounts_date, ixbrl_sections


# ---------------------------------------------------------------------------
# Step 5b — PDF extraction (non-iXBRL path)
# ---------------------------------------------------------------------------
# Calls the `pdf_parser` module which implements Parts A (quantitative) and
# B (qualitative) of `Claude skills/PDF-extraction.md`. A single iXBRL-shape
# dict is produced so it flows into calculate_derived_metrics() unchanged.
#
# Key difference from the iXBRL path:
#   - parse_ixbrl_multi: fetches up to 5 annual filings and merges them
#     into multi-year revenue/EBITDA/FCF history.
#   - parse_pdf_multi: parses each filing's PDF independently via
#     pdf_parser.parse_pdf_full (single OCR/text-layer pass per filing),
#     then merges the per-filing results year-by-year.

# PDF-parser field name -> canonical financial_computations field name.
# Used to translate Part A's output into the same flat-per-year shape that
# the iXBRL path produces.
_PDF_INCOME_FIELD_MAP = {
    # UK GAAP
    "turnover": "revenue",
    "cost_of_sales": "cost_of_sales",
    "gross_profit": "gross_profit",
    "distribution_costs": "distribution_costs",
    "administrative_expenses": "admin_expenses",
    "operating_profit": "operating_profit",
    "interest_payable": "finance_costs",
    "interest_receivable": "finance_income",
    "profit_before_taxation": "profit_before_tax",
    "tax_on_profit": "tax_expense",
    "profit_for_financial_year": "profit_for_year",
    # IFRS
    "revenue": "revenue",
    "finance_costs": "finance_costs",
    "finance_income": "finance_income",
    "taxation": "tax_expense",
}

_PDF_BALANCE_FIELD_MAP = {
    # UK GAAP (nested sub-sections will be flattened before lookup)
    "tangible_assets": "property_plant_equipment",
    "intangible_assets": "intangible_assets",
    "fixed_asset_investments": "investments",
    "debtors": "trade_receivables",
    "cash_at_bank": "cash",
    "stock": "inventories",
    "net_current_assets": "net_current_assets",
    "total_assets_less_current_liabilities": "total_assets_less_current_liabilities",
    "net_assets": "net_assets",
    "called_up_share_capital": "share_capital",
    "share_premium": "share_premium",
    "retained_earnings": "retained_earnings",
    "shareholders_funds": "total_equity",
    "creditors_within_one_year": "trade_payables",
    # IFRS
    "property_plant_equipment": "property_plant_equipment",
    "right_of_use_assets": "right_of_use_assets",
    "investments": "investments",
    "trade_and_other_receivables": "trade_receivables",
    "inventories": "inventories",
    "cash_and_equivalents": "cash",
    "total_current_assets": "total_current_assets",
    "total_non_current_assets": "total_fixed_assets",
    "total_assets": "total_assets",
    "trade_and_other_payables": "trade_payables",
    "borrowings": "short_term_borrowings",
    "lease_liabilities": "lease_liabilities_noncurrent",
    "provisions": "provisions",
    "total_current_liabilities": "total_current_liabilities",
    "total_non_current_liabilities": "total_noncurrent_liabilities",
    "total_liabilities": "total_liabilities",
    "share_capital": "share_capital",
    "total_equity": "total_equity",
}

# Cash flow field names are already canonical in the pdf_parser CASHFLOW_LABEL_MAP
# (operating_cash_flow, capex_ppe, etc.), matching financial_computations directly.
_PDF_CASHFLOW_FIELD_MAP = {
    "operating_cash_flow": "operating_cash_flow",
    "net_cash_operating": "net_cash_operating",
    "tax_paid": "tax_paid",
    "capex_ppe": "capex_ppe",
    "capex_intangibles": "capex_intangibles",
    "proceeds_disposal_ppe": "proceeds_disposal_ppe",
    "net_cash_investing": "net_cash_investing",
    "proceeds_borrowings": "proceeds_borrowings",
    "repayment_borrowings": "repayment_borrowings",
    "lease_payments": "lease_payments",
    "dividends_paid_cf": "dividends_paid_cf",
    "net_cash_financing": "net_cash_financing",
    "opening_cash": "opening_cash",
    "closing_cash": "closing_cash",
    "net_change_cash": "net_change_cash",
}


def _flatten_pdf_balance(balance: dict) -> dict[str, dict]:
    """Flatten the UK-GAAP nested balance sheet into {field: {year: val}}.

    For IFRS output the balance sheet is already flat, so this is a no-op.
    """
    if not isinstance(balance, dict):
        return {}
    flat: dict[str, dict] = {}
    for key, value in balance.items():
        if isinstance(value, dict) and value and all(
            isinstance(v, dict) or v is None for v in value.values()
        ):
            # Nested sub-section (e.g. "fixed_assets", "current_assets")
            for sub_key, sub_val in value.items():
                if isinstance(sub_val, dict):
                    flat[sub_key] = sub_val
        elif isinstance(value, dict):
            flat[key] = value
    return flat


def _normalise_pdf_extraction(pdf_output: dict) -> dict:
    """Translate the `pdf_parser.parse_pdf` output into the iXBRL-shape dict
    that `calculate_derived_metrics` consumes.

    Produces:
      - income_statement / balance_sheet / cash_flow_statement keyed by year_str
        with canonical field names
      - top-level revenue / employees (current year display fields)
      - revenue_history, employees_history where available
      - derivation_log noting PDF origin
    """
    financials = pdf_output.get("financials") if "financials" in pdf_output else pdf_output
    if not financials:
        return {}

    current_year = financials.get("current_year")
    prior_year = financials.get("prior_year")
    years: set[str] = {y for y in (current_year, prior_year) if y and y != "Unknown"}

    result: dict = {
        "income_statement": {},
        "balance_sheet": {},
        "cash_flow_statement": {},  # PDFs rarely yield usable cash-flow data
        "derivation_log": {"source": "pdf_parser", "dialect": financials.get("dialect")},
    }

    # --- Income statement ---
    income = financials.get("income_statement") or {}
    if isinstance(income, dict):
        for pdf_field, year_vals in income.items():
            canonical = _PDF_INCOME_FIELD_MAP.get(pdf_field)
            if canonical is None or not isinstance(year_vals, dict):
                continue
            for yr, val in year_vals.items():
                if val is None:
                    continue
                yr_str = str(yr)
                years.add(yr_str)
                result["income_statement"].setdefault(yr_str, {})[canonical] = val

    # --- Balance sheet ---
    balance_flat = _flatten_pdf_balance(financials.get("balance_sheet") or {})
    for pdf_field, year_vals in balance_flat.items():
        canonical = _PDF_BALANCE_FIELD_MAP.get(pdf_field)
        if canonical is None or not isinstance(year_vals, dict):
            continue
        for yr, val in year_vals.items():
            if val is None:
                continue
            yr_str = str(yr)
            years.add(yr_str)
            result["balance_sheet"].setdefault(yr_str, {})[canonical] = val

    # --- Cash flow statement ---
    cashflow = financials.get("cash_flow_statement") or {}
    if isinstance(cashflow, dict):
        for pdf_field, year_vals in cashflow.items():
            canonical = _PDF_CASHFLOW_FIELD_MAP.get(pdf_field)
            if canonical is None or not isinstance(year_vals, dict):
                continue
            for yr, val in year_vals.items():
                if val is None:
                    continue
                yr_str = str(yr)
                years.add(yr_str)
                result["cash_flow_statement"].setdefault(yr_str, {})[canonical] = val

    # Ensure every year row exists in both statements (gap-fills rely on it)
    for yr in years:
        result["income_statement"].setdefault(yr, {})
        result["balance_sheet"].setdefault(yr, {})
        result["cash_flow_statement"].setdefault(yr, {})

    # Sort statements chronologically
    def _sort_stmt(stmt):
        return {k: stmt[k] for k in sorted(stmt.keys())}
    result["income_statement"] = _sort_stmt(result["income_statement"])
    result["balance_sheet"] = _sort_stmt(result["balance_sheet"])
    result["cash_flow_statement"] = _sort_stmt(result["cash_flow_statement"])

    # --- Top-level display fields (current year) ---
    if current_year and current_year in result["income_statement"]:
        rev = result["income_statement"][current_year].get("revenue")
        if rev is not None:
            result["revenue"] = rev

    # --- Revenue history (2-year window from a single PDF) ---
    rev_history = {
        yr: row.get("revenue")
        for yr, row in result["income_statement"].items()
        if row.get("revenue") is not None
    }
    if rev_history:
        result["revenue_history"] = rev_history

    return result


def _merge_pdf_extraction(target: dict, source: dict) -> None:
    """Merge a normalised PDF extraction *source* into *target* (latest-wins
    priority: target is assumed to hold the newest filing, so `source` only
    fills gaps)."""
    for stmt_key in ("income_statement", "balance_sheet", "cash_flow_statement"):
        _merge_statement(target.setdefault(stmt_key, {}), source.get(stmt_key, {}))
    if "revenue_history" in source:
        _merge_history(target.setdefault("revenue_history", {}), source["revenue_history"])


def parse_pdf_multi(company_number, filings):
    """Fetch and parse PDF filings, merging all data across them.

    Returns (merged_result, filing_format, last_accounts_date, sections)
    or (None, None, None, None).

    `sections` holds the Part B qualitative extraction of the MOST RECENT
    filing only (section text is not merged across filings — the latest
    filing's narrative is what downstream consumers want).

    Filings dated before 2015 are skipped — pre-FRS-102 label vocabulary
    does not match and OCR time is wasted on unextractable content.
    """
    try:
        import pdf_parser  # local import so ixbrl-only runs don't need fitz
    except ImportError as e:
        log.error("pdf_parser import failed: %s", e)
        return None, None, None, None

    latest_result: dict | None = None
    filing_format: str | None = None
    last_accounts_date: str | None = None
    latest_sections: dict | None = None

    for i, filing in enumerate(filings):
        # Skip pre-2015 filings (pre-FRS 102 label vocabulary)
        filing_date = filing.get("date", "")
        if filing_date and filing_date < "2015":
            log.info("    Filing %d: %s — skipping (pre-2015)", i + 1, filing_date)
            continue

        metadata = get_document_metadata(filing)
        if not metadata:
            continue

        fmt = determine_filing_format(metadata, filing)
        if fmt == "ixbrl":
            # Mixed filing history — skip iXBRL filings here; the pipeline
            # should have routed to parse_ixbrl_multi for the latest filing.
            log.info("    Filing %d is iXBRL, skipping in PDF path", i + 1)
            continue

        try:
            content, _ctype = download_document(metadata, fmt)
        except Exception as e:
            log.warning("    Filing %d PDF download failed: %s", i + 1, e)
            continue

        log.info("    Filing %d: %s (%d bytes, %s)",
                 i + 1, filing.get("date", "?"), len(content), fmt)

        try:
            full = pdf_parser.parse_pdf_full(content)
        except Exception as e:
            log.warning("    Filing %d PDF parse failed: %s", i + 1, e)
            continue

        # Capture sections from the first filing that yields them,
        # regardless of whether financials pass validation.
        if latest_sections is None:
            filing_sections = full.get("sections")
            if filing_sections:
                latest_sections = filing_sections

        normalised = _normalise_pdf_extraction(full.get("financials", {}))
        # Accept if we have either an income statement or balance sheet —
        # filleted filings and some scanned PDFs may only deliver the BS.
        has_income = any(
            row for row in (normalised.get("income_statement") or {}).values()
            if isinstance(row, dict) and row
        )
        has_balance = any(
            row for row in (normalised.get("balance_sheet") or {}).values()
            if isinstance(row, dict) and row
        )
        if not normalised or not (has_income or has_balance):
            log.info("    Filing %d yielded no usable financials", i + 1)
            continue

        if latest_result is None:
            latest_result = normalised
            filing_format = fmt
            last_accounts_date = filing.get("date")
        else:
            _merge_pdf_extraction(latest_result, normalised)

    if latest_result is None:
        return None, None, None, latest_sections

    return latest_result, filing_format, last_accounts_date, latest_sections

# ---------------------------------------------------------------------------
# Step 6 — Calculate derived metrics
# ---------------------------------------------------------------------------
def _cagr(history):
    """Calculate CAGR from a {year_str: value} dict. Returns None if insufficient data."""
    if not history or len(history) < 2:
        return None
    years = sorted(history.keys())
    first_year, last_year = years[0], years[-1]
    first_val, last_val = history[first_year], history[last_year]
    n_years = int(last_year) - int(first_year)
    if n_years <= 0 or not first_val or first_val <= 0 or not last_val or last_val <= 0:
        return None
    return round((last_val / first_val) ** (1 / n_years) - 1, 4)


def calculate_derived_metrics(data):
    """Run the full financial computation pipeline on extracted data.

    Steps:
      1. Sign normalisation (per year)
      2. Gap-fills (per year)
      3. Single-row derivations — margins, EBITDA, FCF, ratios (per year)
      4. Validations (per year)
      5. Cross-period metrics — YoY growth, CAGR (across years)
      6. Legacy CAGR for history dicts

    All audit trails are logged to derivation_log.computation_audit.
    """
    from financial_computations import compute, compute_cross_period

    # Run per-year pipeline: signs → gap-fills → derivations → validations
    data = compute(data)

    # Run cross-period metrics: YoY growth and CAGR
    data = compute_cross_period(data)

    # Legacy: ensure ebitda_margin and cash_conversion are set
    revenue = data.get("revenue")
    ebitda = data.get("ebitda")
    if revenue and ebitda and revenue != 0 and data.get("ebitda_margin") is None:
        data["ebitda_margin"] = round(ebitda / revenue, 4)

    fcf = data.get("fcf")
    if fcf is not None and ebitda and ebitda != 0 and data.get("cash_conversion") is None:
        data["cash_conversion"] = round(fcf / ebitda, 4)

    # Legacy CAGR from history dicts
    if data.get("revenue_cagr") is None:
        rev_cagr = _cagr(data.get("revenue_history"))
        if rev_cagr is not None:
            data["revenue_cagr"] = rev_cagr

    if data.get("ebitda_cagr") is None:
        ebitda_cagr = _cagr(data.get("ebitda_history"))
        if ebitda_cagr is not None:
            data["ebitda_cagr"] = ebitda_cagr

    return data

# ---------------------------------------------------------------------------
# Step 7 — Write results back to Supabase
# ---------------------------------------------------------------------------
def update_company(supabase, company_number, column_updates):
    """Update typed columns on a company row directly.

    *column_updates* is a flat dict mapping column names to values,
    e.g. {"pipeline_status": "failed", "filing_format": "ixbrl"}.
    """
    supabase.table("companies").update(column_updates).eq(
        "company_number", company_number
    ).execute()


def update_company_metadata_blob(supabase, company_number, patch):
    """Deep-merge *patch* into the metadata JSONB column.

    Used for derived ratios, YoY/CAGR, validation_warnings, and other fields
    that don't have their own typed column.
    """
    supabase.rpc("update_company_metadata", {
        "p_company_number": company_number,
        "p_patch": patch,
    }).execute()


# ---------------------------------------------------------------------------
# Step 7 — Build the Supabase write payload from extraction results
# ---------------------------------------------------------------------------

# Typed top-level columns that map 1:1 from extraction output
_TYPED_COLUMN_KEYS = {
    "revenue", "ebitda", "ebitda_margin", "fcf", "cash_conversion", "employees",
    "revenue_history", "ebitda_history", "fcf_history", "employees_history",
    "income_statement", "balance_sheet", "cash_flow_statement", "derivation_log",
}

# Extraction keys that route into metadata JSONB
_METADATA_KEYS = {
    # Derived ratios
    "gross_margin_pct", "operating_margin_pct", "net_margin_pct",
    "return_on_assets", "return_on_equity",
    "total_capex", "capex_to_revenue",
    "net_debt", "net_debt_to_ebitda",
    "current_ratio", "quick_ratio", "debt_to_equity", "interest_cover",
    "asset_turnover", "revenue_per_employee", "profit_per_employee",
    # YoY growth
    "revenue_yoy_growth", "ebitda_yoy_growth", "profit_yoy_growth",
    # CAGR variants
    "revenue_cagr", "ebitda_cagr", "revenue_cagr_3yr", "revenue_cagr_5yr",
    # Validation
    "validation_warnings",
}


def _build_write_payload(extracted, filing_format, last_accounts_date):
    """Split extraction results into typed-column dict and metadata dict.

    Returns (column_updates, metadata_patch).
    """
    columns = {
        "pipeline_status": "extracted",
        "filing_format": filing_format,
        "last_accounts_date": last_accounts_date,
    }

    # Map revenue_cagr_Xyr → revenue_cagr_Xy (actual column names)
    if "revenue_cagr_3yr" in extracted and extracted["revenue_cagr_3yr"] is not None:
        columns["revenue_cagr_3y"] = extracted["revenue_cagr_3yr"]
    if "revenue_cagr_5yr" in extracted and extracted["revenue_cagr_5yr"] is not None:
        columns["revenue_cagr_5y"] = extracted["revenue_cagr_5yr"]

    for key in _TYPED_COLUMN_KEYS:
        val = extracted.get(key)
        if val is not None:
            columns[key] = val

    meta_patch = {}
    for key in _METADATA_KEYS:
        val = extracted.get(key)
        if val is not None:
            meta_patch[key] = val

    return columns, meta_patch


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def process_batch(supabase, limit=BATCH_SIZE):
    """Query pending companies, extract financials, and write results."""
    log.info("Fetching up to %d pending companies...", limit)
    result = (
        supabase.table("companies")
        .select("company_number, company_name")
        .eq("pipeline_status", "pending")
        .limit(limit)
        .execute()
    )
    companies = result.data
    log.info("Got %d companies to process.", len(companies))

    counts = {"extracted": 0, "no_filing": 0, "failed": 0, "skipped_pdf": 0}

    for i, company in enumerate(companies, 1):
        cn = company["company_number"]
        name = company.get("company_name", "Unknown")
        log.info("[%d/%d] Processing %s (%s)", i, len(companies), cn, name)

        try:
            # Step 1 — filing history (latest only for initial backfill)
            filings = get_accounts_filings(cn, count=5)
            if not filings:
                log.info("  No accounts filing found — marking no_filing")
                update_company(supabase, cn, {"pipeline_status": "no_filing"})
                counts["no_filing"] += 1
                continue

            # Step 2 — check format of latest filing
            first_metadata = get_document_metadata(filings[0])
            if first_metadata is None:
                log.warning("  No document metadata link — marking failed")
                update_company(supabase, cn, {"pipeline_status": "failed"})
                counts["failed"] += 1
                continue

            fmt = determine_filing_format(first_metadata, filings[0])
            log.info("  Filing format: %s", fmt)

            # Step 3-5 — extract based on format
            pdf_sections = None
            ixbrl_sections = None
            if fmt == "ixbrl":
                extracted, filing_format, last_date, ixbrl_sections = parse_ixbrl_multi(cn, filings)
                if extracted is None:
                    log.warning("  iXBRL parsing returned no data — marking failed")
                    update_company(supabase, cn, {
                        "pipeline_status": "failed",
                        "filing_format": fmt,
                    })
                    # Still write qualitative sections if available
                    if ixbrl_sections is not None:
                        update_company(supabase, cn, {"company_profile": ixbrl_sections})
                        log.info("  Qualitative sections saved despite financial extraction failure")
                    counts["failed"] += 1
                    continue
            else:
                extracted, filing_format, last_date, pdf_sections = parse_pdf_multi(cn, filings)
                if extracted is None:
                    log.warning("  PDF parsing returned no data — marking failed")
                    update_company(supabase, cn, {
                        "pipeline_status": "failed",
                        "filing_format": fmt,
                        "last_accounts_date": filings[0].get("date"),
                    })
                    # Still write qualitative sections if available (Part B decoupled from Part A)
                    if pdf_sections is not None:
                        update_company(supabase, cn, {"company_profile": pdf_sections})
                        log.info("  Qualitative sections saved despite financial extraction failure")
                    counts["failed"] += 1
                    continue

            # Step 6 — compute derived metrics
            extracted = calculate_derived_metrics(extracted)

            # Step 7 — write to Supabase
            columns, meta_patch = _build_write_payload(extracted, filing_format, last_date)

            # Part B qualitative sections go into company_profile JSONB
            sections_output = pdf_sections or ixbrl_sections
            if sections_output is not None:
                columns["company_profile"] = sections_output

            update_company(supabase, cn, columns)
            if meta_patch:
                update_company_metadata_blob(supabase, cn, meta_patch)

            log.info("  Extracted and saved — revenue=%s ebitda=%s",
                     columns.get("revenue"), columns.get("ebitda"))
            counts["extracted"] += 1

        except requests.exceptions.HTTPError as e:
            log.error("  HTTP error for %s: %s", cn, e)
            update_company(supabase, cn, {"pipeline_status": "failed"})
            counts["failed"] += 1
        except Exception as e:
            log.error("  Unexpected error for %s: %s", cn, e, exc_info=True)
            update_company(supabase, cn, {"pipeline_status": "failed"})
            counts["failed"] += 1

    # Batch summary
    log.info("--- Batch summary ---")
    for status, count in counts.items():
        if count:
            log.info("  %-16s %d", status, count)
    log.info("---------------------")


def main():
    _check_env()
    _init_ch_session()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    log.info("Connected to Supabase. Starting pipeline...")
    process_batch(supabase)
    log.info("Batch complete.")


if __name__ == "__main__":
    main()
