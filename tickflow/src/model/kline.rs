use std::collections::HashMap;

use chrono_tz::Tz;
use polars::prelude::*;
use serde::{Deserialize, Serialize};

use super::{AdjustType, Period};

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

/// Outcome of a single-symbol kline request.
///
/// `Raw` carries the compact columnar payload; `DataFrame` carries the polars
/// conversion (region-aware `trade_date` / `trade_time`, instrument `name`).
#[derive(Debug)]
pub enum KlineResponse {
    Raw(KlineData),
    DataFrame(DataFrame),
}

/// Outcome of a batch kline request.
#[derive(Debug)]
pub enum BatchKlineResponse {
    Raw(HashMap<String, KlineData>),
    DataFrame(HashMap<String, DataFrame>),
}

/// Outcome of a single-symbol intraday kline request.
#[derive(Debug)]
pub enum IntradayResponse {
    Raw(KlineData),
    DataFrame(DataFrame),
}

/// Outcome of a batch intraday kline request.
#[derive(Debug)]
pub enum BatchIntradayResponse {
    Raw(HashMap<String, KlineData>),
    DataFrame(HashMap<String, DataFrame>),
}

/// Outcome of an ex-factors request.
///
/// `Raw` carries the per-symbol factor lists; `DataFrame` is the long-format
/// polars frame (`symbol`, `timestamp`, `trade_date`, `ex_factor`).
#[derive(Debug)]
pub enum ExfactorsResponse {
    Raw(HashMap<String, Vec<ExfactorsData>>),
    DataFrame(DataFrame),
}

/// HTTP response wrapper for `GET /v1/klines` and `/v1/klines/intraday`.
#[derive(Debug, Clone, Deserialize)]
pub struct KlinesResponse {
    pub data: KlineData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExfactorsData {
    pub timestamp: i64,
    pub ex_factor: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct KlinesParams {
    /// Symbol code for single-symbol requests; `None` for batch requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// K-line period (1m / 5m / 1d / …). Defaults differ per endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period: Option<Period>,
    /// Number of bars. Defaults differ per endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjust: Option<AdjustType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_dataframe: Option<bool>,
}

impl KlinesParams {
    /// Defaults for the daily-kline endpoint: `period=1d`, `count=100`,
    /// `adjust=forward`. `symbol` is filled in by the single-symbol builder.
    pub fn klines_default() -> Self {
        Self {
            symbol: None,
            period: Some(Period::default()),
            count: Some(100),
            adjust: Some(AdjustType::default()),
            start_time: None,
            end_time: None,
            as_dataframe: None,
        }
    }

    /// Defaults for the intraday endpoint: `period=1m`, `count=240`. No
    /// `adjust` (the intraday API does not accept it).
    pub fn intraday_default() -> Self {
        Self {
            symbol: None,
            period: Some(Period::M1),
            count: Some(240),
            adjust: None,
            start_time: None,
            end_time: None,
            as_dataframe: None,
        }
    }
}

/// Broadcast a scalar value to a column of `len` rows.
///
/// The empty-input case (`Some(_)` with `len == 0`) yields an empty vector
/// rather than `vec![None; 0]` — preserves the intent that "the scalar was
/// provided" when polars later appends to a zero-row frame.
pub fn broadcast_to(scalar: Option<&str>, len: usize) -> Vec<Option<&str>> {
    match (scalar, len) {
        (Some(_), 0) => Vec::new(),
        (Some(s), _) => vec![Some(s); len],
        (None, _) => vec![None; len],
    }
}

/// Compute `(trade_date, trade_time)` columns aligned to `timestamps`.
///
/// When the symbol's region has a known timezone, both columns hold strings
/// formatted in that local zone (DST-aware via `chrono-tz`); when no zone is
/// available the columns are filled with `None`.
pub fn format_trade_columns(
    timestamps: &[i64],
    tz: Option<Tz>,
) -> (Vec<Option<String>>, Vec<Option<String>>) {
    let mut dates = Vec::with_capacity(timestamps.len());
    let mut times = Vec::with_capacity(timestamps.len());
    if let Some(tz) = tz {
        for &ts in timestamps {
            let dt = chrono::DateTime::from_timestamp_millis(ts).map(|d| d.with_timezone(&tz));
            match dt {
                Some(dt) => {
                    dates.push(Some(dt.format("%Y-%m-%d").to_string()));
                    times.push(Some(dt.format("%Y-%m-%d %H:%M:%S").to_string()));
                }
                None => {
                    dates.push(None);
                    times.push(None);
                }
            }
        }
    } else {
        dates.resize(timestamps.len(), None);
        times.resize(timestamps.len(), None);
    }
    (dates, times)
}
