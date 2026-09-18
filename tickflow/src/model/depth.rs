use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::symbol::Region;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDepth {
    pub symbol: String,
    pub region: Region,
    pub timestamp: i64,
    pub bid_prices: Vec<f64>,
    pub bid_volumes: Vec<usize>,
    pub ask_prices: Vec<f64>,
    pub ask_volumes: Vec<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DepthResponse {
    pub data: MarketDepth,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchDepthResponse {
    pub data: HashMap<String, MarketDepth>,
}
