use std::{env, time::Duration};
use url::Url;

#[derive(Debug, Clone)]
pub struct Config {
    /// Optional API key for authentication.
    pub api_key: Option<String>,
    /// Base URL for the API Server. Defaults to https://api.tickflow.org.
    pub base_url: Url,
    /// WebSocket base URL (derived from `base_url` by default).
    pub ws_url: Url,
    /// Per-request timeout in seconds.
    pub timeout: Duration,
    /// Maximum number of retry attempts for failed requests.
    pub max_retries: u32,
}

impl Config {
    pub const DEFAULT_BASE_URL: &'static str = "https://api.tickflow.org";
    pub const FREE_BASE_URL: &'static str = "https://free-api.tickflow.org";

    pub fn new() -> Self {
        let base_url = Url::parse(Self::DEFAULT_BASE_URL).expect("default base url is valid");
        let ws_url = derive_ws_url(&base_url);
        Self {
            api_key: None,
            base_url,
            ws_url,
            timeout: Duration::from_secs(30),
            max_retries: 3,
        }
    }

    pub fn merge_env(mut self) -> Self {
        if let Ok(v) = env::var("TICKFLOW_API_KEY") {
            self.api_key = Some(v);
        }

        if let Ok(v) = env::var("TICKFLOW_BASE_URL")
            && let Ok(parsed) = Url::parse(&v)
        {
            self.base_url = parsed.clone();
            self.ws_url = derive_ws_url(&parsed)
        }

        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn derive_ws_url(base: &Url) -> Url {
    let mut ws = base.clone();
    match ws.scheme() {
        "https" => ws.set_scheme("wss").expect("https -> wss"),
        "http" => ws.set_scheme("ws").expect("http -> ws"),
        _ => {}
    }
    ws
}
