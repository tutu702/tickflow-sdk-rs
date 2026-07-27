use std::sync::Arc;

use crate::{
    error::Result,
    http::HttpClient,
    resources::{klines::Klines, quotes::Quotes},
};

mod builder;
mod config;

pub use builder::TickFlowBuilder;
pub use config::Config;

pub struct TickFlow {
    pub(crate) http: Arc<HttpClient>,
    pub(crate) config: Arc<Config>,
    pub(crate) quotes: Quotes,
    pub(crate) klines: Klines,
}

impl TickFlow {
    pub fn builder() -> TickFlowBuilder {
        TickFlowBuilder::default()
    }

    pub fn new(api_key: impl Into<String>) -> Result<Self> {
        Self::builder().api_key(api_key).build()
    }

    pub fn free() -> Result<Self> {
        Self::builder().free_tier().build()
    }

    pub fn http(&self) -> &HttpClient {
        &self.http
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn quotes(&self) -> &Quotes {
        &self.quotes
    }

    pub fn klines(&self) -> &Klines {
        &self.klines
    }
}
