use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomeRecord {
    period_end: String,
    announce_date: Option<String>,
    revenue: Option<f64>,
    operating_cost: Option<f64>,
    selling_expense: Option<f64>,
    admin_expense: Option<f64>,
    rd_expense: Option<f64>,
    operating_profit: Option<f64>,
    financial_expense: Option<f64>,
    non_operating_income: Option<f64>,
    non_operating_expense: Option<f64>,
    total_profit: Option<f64>,
    income_tax: Option<f64>,
    net_income: Option<f64>,
    net_income_attributable: Option<f64>,
    net_income_deducted: Option<f64>,
    basic_eps: Option<f64>,
    diluted_eps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceSheetRecord {
    period_end: String,
    announce_date: Option<String>,
    total_assets: Option<f64>,
    total_current_assets: Option<f64>,
    cash_and_equivalents: Option<f64>,
    accounts_receivable: Option<f64>,
    total_non_current_assets: Option<f64>,
    fixed_assets: Option<f64>,
    intangible_assets: Option<f64>,
    total_liabilities: Option<f64>,
    total_current_liabilities: Option<f64>,
    short_term_borrowing: Option<f64>,
    total_equity: Option<f64>,
    equity_attributable: Option<f64>,
    retained_earnings: Option<f64>,
    accounts_payable: Option<f64>,
    goodwill: Option<f64>,
    inventory: Option<f64>,
    long_term_borrowing: Option<f64>,
    minority_interest: Option<f64>,
    total_non_current_liabilities: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CashFlowRecord {
    period_end: String,
    announce_date: Option<String>,
    net_operating_cash_flow: Option<f64>,
    net_investing_cash_flow: Option<f64>,
    net_financing_cash_flow: Option<f64>,
    net_cash_change: Option<f64>,
    capex: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsRecord {
    period_end: String,
    announce_date: Option<String>,
    eps_basic: Option<f64>,
    eps_diluted: Option<f64>,
    bps: Option<f64>,
    ocfps: Option<f64>,
    roe: Option<f64>,
    net_margin: Option<f64>,
    revenue_yoy: Option<f64>,
    net_income_yoy: Option<f64>,
    debt_to_asset_ratio: Option<f64>,
    gross_margin: Option<f64>,
    inventory_turnover: Option<f64>,
    operating_cash_to_revenue: Option<f64>,
    roa: Option<f64>,
    roe_diluted: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharesRecord {
    period_end: String,
    announce_date: Option<String>,
    total_shares: Option<f64>,
    float_shares: Option<f64>,
}
