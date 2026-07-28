use crate::error::{ConfigError, Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Region {
    CN,
    US,
    HK,
}

impl Region {
    pub fn as_str(&self) -> &'static str {
        match self {
            Region::CN => "CN",
            Region::US => "US",
            Region::HK => "HK",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "CN" => Region::CN,
            "US" => Region::US,
            "HK" => Region::HK,
            _ => return None,
        })
    }
}

impl From<Region> for &'static str {
    fn from(val: Region) -> Self {
        val.as_str()
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        validate(&s)?;
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<Symbol> for String {
    fn from(s: Symbol) -> Self {
        s.0
    }
}

impl From<&Symbol> for String {
    fn from(s: &Symbol) -> Self {
        s.as_str().to_owned()
    }
}

fn validate(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::Config(ConfigError::new("symbol cannot be empty")));
    }

    if !s.contains('.') {
        return Err(Error::Config(ConfigError::new(format!(
            "symbol missing region suffix: {s:?}"
        ))));
    }

    Ok(())
}
