use std::{sync::Arc, time::Duration};

use url::Url;

use crate::{
    client::{Config, TickFlow, config::derive_ws_url},
    error::Result,
    http::HttpClient,
    resources,
};

#[derive(Debug, Clone)]
pub struct TickFlowBuilder {
    config: Config,
}

impl TickFlowBuilder {}

impl Default for TickFlowBuilder {
    fn default() -> Self {
        Self {
            config: Config::new().merge_env(),
        }
    }
}

impl TickFlowBuilder {
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.config.api_key = Some(api_key.into());
        self
    }

    pub fn base_url(mut self, base_url: impl AsRef<str>) -> Self {
        if let Ok(parsed) = Url::parse(base_url.as_ref()) {
            self.config.base_url = parsed.clone();
            self.config.ws_url = derive_ws_url(&parsed);
        }
        self
    }

    pub fn free_tier(mut self) -> Self {
        self.config = Config::new();
        if let Ok(parsed) = Url::parse(Config::FREE_BASE_URL) {
            self.config.base_url = parsed.clone();
            self.config.ws_url = derive_ws_url(&parsed);
        }
        self
    }

    pub fn timeout(mut self, t: Duration) -> Self {
        self.config.timeout = t;
        self
    }

    pub fn build(self) -> Result<TickFlow> {
        let config = Arc::new(self.config);
        let http = Arc::new(HttpClient::new(Arc::clone(&config))?);
        let instruments = resources::Instruments::new(Arc::clone(&http));
        let quotes = resources::Quotes::new(Arc::clone(&http));
        let klines = resources::Klines::new(Arc::clone(&http));
        let universes = resources::Universes::new(Arc::clone(&http));
        let depth = resources::Depth::new(Arc::clone(&http));

        Ok(TickFlow {
            http,
            config,
            instruments,
            quotes,
            klines,
            universes,
            depth,
        })
    }
}
