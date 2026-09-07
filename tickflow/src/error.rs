use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    /// Invalid configuration.
    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    #[error("network error {0}")]
    Network(#[from] reqwest::Error),

    #[error("API error {0}")]
    Api(String),

    /// Failed to parse a response body.
    #[error("parse error: {0}")]
    Parse(String),

    /// Failed to assemble a DataFrame from API data.
    #[error("dataframe error: {0}")]
    DataFrame(String),
}

#[derive(Debug, Error)]
#[error("config error: {message}")]
pub struct ConfigError {
    pub message: String,
}

impl ConfigError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
