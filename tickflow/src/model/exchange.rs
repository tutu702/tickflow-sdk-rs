use serde::{Deserialize, Serialize};

use crate::model::quote::Region;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeData {
    pub exchange: String,
    pub region: Region,
    pub count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeListResponse {
    pub data: Vec<ExchangeData>,
}
