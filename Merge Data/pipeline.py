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
    """Return up to `count` most recent accounts filing items (newest first), or empty list."""
    url = f"{CH_BASE_URL}/company/{company_number}/filing-history"
    resp = ch_get(url, params={"category": "accounts", "items_per_page": str(count)})
    return resp.json().get("items", [])


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

    Returns (merged_result, filing_format, last_accounts_date) or (None, None, None).
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

    for i, filing in enumerate(filings):
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
        return None, None, None

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

    return latest_result, filing_format, last_accounts_date


# TODO: implement electronic PDF parser
# TODO: implement scanned PDF handler

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
def update_company(supabase, company_number, metadata_patch):
    """Deep-merge *metadata_patch* into the metadata JSONB for a company.

    Uses the update_company_metadata RPC (see sql/update_company_metadata.sql).
    """
    supabase.rpc("update_company_metadata", {
        "p_company_number": company_number,
        "p_patch": metadata_patch,
    }).execute()

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def process_batch(supabase, limit=BATCH_SIZE):
    """Query pending companies, triage their filing format, and log results."""
    log.info("Fetching up to %d pending companies...", limit)
    result = (
        supabase.table("companies")
        .select("company_number, company_name")
        .eq("metadata->pipeline->>status", "pending")
        .limit(limit)
        .execute()
    )
    companies = result.data
    log.info("Got %d companies to process.", len(companies))

    # Track format counts for end-of-batch summary
    counts = {"ixbrl": 0, "electronic_pdf": 0, "scanned_pdf": 0, "no_filing": 0, "failed": 0}

    for i, company in enumerate(companies, 1):
        cn = company["company_number"]
        name = company.get("company_name", "Unknown")
        log.info("[%d/%d] Processing %s (%s)", i, len(companies), cn, name)

        try:
            # Step 1 — filing history
            filing = get_latest_accounts_filing(cn)
            if filing is None:
                log.info("  No accounts filing found — marking no_filing")
                update_company(supabase, cn, {"pipeline": {"status": "no_filing"}})
                counts["no_filing"] += 1
                continue

            last_accounts_date = filing.get("date")

            # Step 2 — document metadata
            metadata = get_document_metadata(filing)
            if metadata is None:
                log.warning("  No document metadata link — marking failed")
                update_company(supabase, cn, {"pipeline": {"status": "failed"}})
                counts["failed"] += 1
                continue

            # Step 3 — determine format
            fmt = determine_filing_format(metadata, filing)
            log.info("  Filing format: %s  |  Last accounts date: %s", fmt, last_accounts_date)

            # Step 4 — download (verify the document is accessible)
            content, content_type = download_document(metadata, fmt)
            log.info("  Downloaded %d bytes (%s)", len(content), content_type)

            # Write format and date back — keep status pending for future parsing
            update_company(supabase, cn, {
                "filing_format": fmt,
                "last_accounts_date": last_accounts_date,
            })  # these keys land inside metadata JSONB
            counts[fmt] += 1
            log.info("  Saved filing_format='%s' — still pending for parsing", fmt)

        except requests.exceptions.HTTPError as e:
            log.error("  HTTP error for %s: %s", cn, e)
            update_company(supabase, cn, {"pipeline": {"status": "failed"}})
            counts["failed"] += 1
        except Exception as e:
            log.error("  Unexpected error for %s: %s", cn, e, exc_info=True)
            update_company(supabase, cn, {"pipeline": {"status": "failed"}})
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
