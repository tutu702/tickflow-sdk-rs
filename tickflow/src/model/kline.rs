use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineData {
    pub amount: Vec<f64>,
    pub volume: Vec<i64>,
    pub timestamp: Vec<i64>,
    pub open: Vec<f64>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub close: Vec<f64>,
    #[serde(default)]
    pub open_interest: Option<Vec<f64>>,
    #[serde(default)]
    pub prev_close: Option<Vec<f64>>,
    #[serde(default)]
    pub settlement_price: Option<Vec<f64>>,
}

impl KlineData {
    pub fn len(&self) -> usize {
        self.timestamp.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamp.is_empty()
    }

    pub fn last_close(&self) -> Option<f64> {
        self.close.last().copied()
    }

    /// Validate that all parallel arrays have the same length.
    pub fn validate(&self) -> Result<(), &'static str> {
        let n = self.timestamp.len();
        for (_name, len) in [
            ("open", self.open.len()),
            ("high", self.high.len()),
            ("low", self.low.len()),
            ("close", self.close.len()),
            ("volume", self.volume.len()),
            ("amount", self.amount.len()),
        ] {
            if len != n {
                return Err("column length mismatch");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KlinesResponse {
    pub data: KlineData,
}
