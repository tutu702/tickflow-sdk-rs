use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::symbol::Region;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub symbol: String,
    pub last_price: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub prev_close: f64,
    pub volume: i64,
    pub amount: f64,
    pub timestamp: i64,
    pub ext: Option<QuoteExtension>,
    pub region: Region,
    pub session: Option<SessionStatus>,
}

#[derive(Serialize)]
pub struct QuoteParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbols: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub universes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchQuotesResponse {
    pub data: Vec<Quote>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum QuoteExtension {
    #[serde(rename = "cn_equity")]
    Cn(QuoteExt),
    #[serde(rename = "us_equity")]
    Us(QuoteExt),
    #[serde(rename = "hk_equity")]
    Hk(QuoteExt),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteExt {
    pub amplitude: Option<f64>,
    pub change_amount: Option<f64>,
    pub change_pct: Option<f64>,
    pub name: Option<String>,
    pub turnover_rate: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    PreMarket,
    Regular,
    AfterHours,
    Closed,
    Halted,
    LunchBreak,
}

impl SessionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SessionStatus::PreMarket => "pre_market",
            SessionStatus::Regular => "regular",
            SessionStatus::AfterHours => "after_hours",
            SessionStatus::Closed => "closed",
            SessionStatus::Halted => "halted",
            SessionStatus::LunchBreak => "lunch_break",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pre_market" => Some(Self::PreMarket),
            "regular" => Some(Self::Regular),
            "after_hours" => Some(Self::AfterHours),
            "closed" => Some(Self::Closed),
            "halted" => Some(Self::Halted),
            "lunch_break" => Some(Self::LunchBreak),
            _ => None,
        }
    }
}

impl std::fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Outcome of a quotes request.
///
/// `Raw` is the unchanged `symbol -> Quote` map; `DataFrame` is a long-format
/// polars frame (one row per symbol) — region-aware `trade_date` /
/// `trade_time`, flattened `ext.*` columns, mirroring the Python
/// `_quotes_to_dataframe` helper.
#[derive(Debug)]
pub enum QuoteResponse {
    Raw(HashMap<String, Quote>),
    DataFrame(DataFrame),
}
