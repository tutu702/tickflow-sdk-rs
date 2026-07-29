use serde::{Deserialize, Serialize};

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
    pub float_shares: f64,
    pub limit_down: f64,
    pub limit_up: f64,
    pub listing_date: String,
    pub name_en: Option<String>,
    pub tick_size: f64,
    pub total_shares: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsEquityInstrumentExt {
    pub float_shares: f64,
    pub total_shares: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HkEquityInstrumentExt {
    pub float_shares: f64,
    pub lot_size: usize,
    pub total_shares: f64,
}
