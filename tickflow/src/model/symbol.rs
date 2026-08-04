use crate::error::{ConfigError, Error, Result};
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
