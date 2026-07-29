use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Universe {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub region: String,
    pub category: String,
    #[serde(default)]
    pub symbol_count: Option<u64>,
    pub symbols: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UniverseResponse {
    pub data: Universe,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchUniversesResponse {
    pub data: Vec<Universe>,
}
