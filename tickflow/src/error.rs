//! Unified error type.

use thiserror::Error;

/// Convenience alias for `std::result::Result<T, Error>`.
///
/// All fallible operations in this crate return this alias — including
/// network requests, JSON parsing, and DataFrame assembly — so callers
/// only have to handle a single error type.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that any SDK call can surface.
#[derive(Error, Debug)]
pub enum Error {
    /// Invalid configuration — bad URL, malformed API key, or a
    /// [`crate::model::symbol::Symbol`] that failed validation.
    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    /// Transport-level failure from `reqwest` (DNS, TLS, connection
    /// reset, etc.).
    #[error("network error {0}")]
    Network(#[from] reqwest::Error),

    /// The remote API returned a non-2xx response. The wrapped string
    /// carries the status code and the raw body for diagnostics.
    #[error("API error {0}")]
    Api(String),

    /// Failed to parse a response body. The wrapped string includes the
    /// underlying `serde_json` error and a body excerpt.
    #[error("parse error: {0}")]
    Parse(String),

    /// Failed to assemble a polars [`polars::prelude::DataFrame`] from
    /// the decoded payload — typically a column-length mismatch or an
    /// unsupported value type.
    #[error("dataframe error: {0}")]
    DataFrame(String),
}

/// Configuration error.
///
/// Distinct from [`Error::Config`] only by ownership: this is the type
/// produced by [`ConfigError::new`] and the one carried inside
/// [`Error::Config`].
#[derive(Debug, Error)]
#[error("config error: {message}")]
pub struct ConfigError {
    /// Human-readable error message.
    pub message: String,
}

impl ConfigError {
    /// Build a `ConfigError` from any string-like value.
    ///
    /// Used by the SDK's own configuration validators; consumers rarely
    /// need to construct one directly.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
