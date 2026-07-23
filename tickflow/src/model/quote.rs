use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize)]
pub struct BatchQuotesRequest {
    pub symbols: Vec<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Region {
    CN,
    HK,
    US,
}

impl Region {
    pub fn as_str(&self) -> &'static str {
        match self {
            Region::CN => "CN",
            Region::HK => "HK",
            Region::US => "US",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "CN" => Some(Region::CN),
            "HK" => Some(Region::HK),
            "US" => Some(Region::US),
            _ => None,
        }
    }
}

impl From<Region> for &'static str {
    fn from(val: Region) -> Self {
        val.as_str()
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
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

    pub fn from_str(s: &str) -> Option<Self> {
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
