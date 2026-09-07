use crate::error::{ConfigError, Error, Result};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

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

    /// Market region derived from the symbol's exchange suffix, or `None`
    /// if the suffix is not one TickFlow recognises.
    pub fn region(&self) -> Option<Region> {
        region_for_symbol(&self.0)
    }

    /// Timezone of the market this symbol trades in.
    pub fn tz(&self) -> Option<Tz> {
        self.region().and_then(tz_for_region)
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

/// A market region supported by TickFlow: mainland China (`CN`), Hong Kong
/// (`HK`), or the United States (`US`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Region {
    Cn,
    Hk,
    Us,
}

impl Region {
    /// Canonical two-letter code, matching the JSON wire format.
    pub fn as_str(&self) -> &'static str {
        match self {
            Region::Cn => "CN",
            Region::Hk => "HK",
            Region::Us => "US",
        }
    }

    /// Parse a region from its two-letter code. Returns `None` for
    /// unrecognised codes.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "CN" => Some(Region::Cn),
            "HK" => Some(Region::Hk),
            "US" => Some(Region::Us),
            _ => None,
        }
    }

    /// Timezone of the region's primary exchange(s).
    pub fn tz(self) -> Option<Tz> {
        match self {
            Region::Cn => Some(Tz::Asia__Shanghai),
            Region::Hk => Some(Tz::Asia__Hong_Kong),
            Region::Us => Some(Tz::America__New_York),
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
        f.write_str(self.as_str())
    }
}

/// Map a symbol's exchange suffix to a [`Region`].
pub fn region_for_symbol(symbol: &str) -> Option<Region> {
    let suffix = symbol.rsplit('.').next()?;
    match suffix {
        "SH" | "SZ" | "BJ" => Some(Region::Cn),
        "US" => Some(Region::Us),
        "HK" => Some(Region::Hk),
        _ => None,
    }
}

/// Resolve the timezone for a [`Region`].
pub fn tz_for_region(region: Region) -> Option<Tz> {
    region.tz()
}
