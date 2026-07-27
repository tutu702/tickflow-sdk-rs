use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Period {
    #[serde(rename = "1m")]
    M1,
    #[serde(rename = "5m")]
    M5,
    #[serde(rename = "10m")]
    M10,
    #[serde(rename = "15m")]
    M15,
    #[serde(rename = "30m")]
    M30,
    #[serde(rename = "60m")]
    M60,
    #[serde(rename = "1d")]
    D1,
    #[serde(rename = "1w")]
    W1,
    #[serde(rename = "1M")]
    Mo1,
    #[serde(rename = "1Q")]
    Q1,
    #[serde(rename = "1Y")]
    Y1,
}

impl Period {
    pub fn as_str(&self) -> &'static str {
        match self {
            Period::M1 => "1m",
            Period::M5 => "5m",
            Period::M10 => "10m",
            Period::M15 => "15m",
            Period::M30 => "30m",
            Period::M60 => "60m",
            Period::D1 => "1d",
            Period::W1 => "1w",
            Period::Mo1 => "1M",
            Period::Q1 => "1Q",
            Period::Y1 => "1Y",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "1m" => Period::M1,
            "5m" => Period::M5,
            "10m" => Period::M10,
            "15m" => Period::M15,
            "30m" => Period::M30,
            "60m" => Period::M60,
            "1d" => Period::D1,
            "1w" => Period::W1,
            "1M" => Period::Mo1,
            "1Q" => Period::Q1,
            "1y" => Period::Y1,
            _ => return None,
        })
    }
}

impl From<Period> for &'static str {
    fn from(val: Period) -> Self {
        val.as_str()
    }
}

impl fmt::Display for Period {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
