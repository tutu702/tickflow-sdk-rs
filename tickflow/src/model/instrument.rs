use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstrumentType {
    /// Common stock.
    Stock,
    /// Exchange-traded fund.
    Etf,
    /// Market index.
    Index,
    /// Bond.
    Bond,
    /// Fund.
    Fund,
    /// Options.
    Options,
    /// Other.
    Other,
}

impl InstrumentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stock => "stock",
            Self::Etf => "etf",
            Self::Index => "index",
            Self::Bond => "bond",
            Self::Fund => "fund",
            Self::Options => "options",
            Self::Other => "other",
        }
    }
}

impl fmt::Display for InstrumentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instrument {
    pub code: String,
    pub symbol: String,
    pub name: Option<String>,
    pub exchange: String,
    pub region: String,
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub ext: InstrumentExt,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstrumentsResponse {
    pub data: Vec<Instrument>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InstrumentExt {
    #[serde(rename = "cn_equity")]
    Cn(CnEquityInstrumentExt),
    #[serde(rename = "us_equity")]
    Us(UsEquityInstrumentExt),
    #[serde(rename = "hk_equity")]
    Hk(HkEquityInstrumentExt),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CnEquityInstrumentExt {
    pub float_shares: Option<f64>,
    pub limit_down: Option<f64>,
    pub limit_up: Option<f64>,
    pub listing_date: Option<String>,
    pub name_en: Option<String>,
    pub tick_size: Option<f64>,
    pub total_shares: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsEquityInstrumentExt {
    pub float_shares: Option<f64>,
    pub total_shares: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HkEquityInstrumentExt {
    pub float_shares: Option<f64>,
    pub lot_size: Option<usize>,
    pub total_shares: Option<f64>,
}
