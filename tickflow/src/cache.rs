//! In-memory cache that lazily resolves `symbol -> instrument name` by
//! calling the `/v1/instruments` endpoint on demand.
//!
//! Mirrors the Python SDK's [`InstrumentNameCache`], minus the on-disk
//! persistence layer. Subsequent lookups for the same symbol skip the
//! network entirely.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::Mutex;

use crate::{
    error::Result,
    http::HttpClient,
    model::{InstrumentsResponse, KlineData},
    resources::BATCH_CHUNK_SIZE,
};

/// Thread-safe in-memory cache of `symbol -> name`.
pub struct InstrumentNameCache {
    http: Arc<HttpClient>,
    names: Mutex<HashMap<String, String>>,
}

impl InstrumentNameCache {
    pub fn new(http: Arc<HttpClient>) -> Self {
        Self {
            http,
            names: Mutex::new(HashMap::new()),
        }
    }

    /// Resolve names for `symbols`, fetching any missing ones from the
    /// instruments API. Returns a map containing only the symbols that
    /// resolved (i.e. the API returned a name for them).
    pub async fn resolve(&self, symbols: &[String]) -> Result<HashMap<String, String>> {
        let missing: Vec<String> = {
            let cache = self.names.lock().await;
            symbols
                .iter()
                .filter(|s| !cache.contains_key(*s))
                .cloned()
                .collect()
        };

        if !missing.is_empty() {
            let fresh = self.fetch_names(&missing).await?;
            if !fresh.is_empty() {
                let mut cache = self.names.lock().await;
                cache.extend(fresh);
            }
        }

        let cache = self.names.lock().await;
        Ok(symbols
            .iter()
            .filter_map(|s| cache.get(s).map(|n| (s.clone(), n.clone())))
            .collect())
    }

    /// Fetch `symbol -> name` mappings for the given codes by POSTing to
    /// `/v1/instruments` in batches of [`BATCH_CHUNK_SIZE`]. Symbols the
    /// API doesn't return a name for are simply absent from the result.
    async fn fetch_names(&self, symbols: &[String]) -> Result<HashMap<String, String>> {
        #[derive(Serialize)]
        struct Req {
            symbols: Vec<String>,
        }

        if symbols.is_empty() {
            return Ok(HashMap::new());
        }

        let mut out = HashMap::new();
        for chunk in symbols.chunks(BATCH_CHUNK_SIZE) {
            let req = Req {
                symbols: chunk.to_vec(),
            };
            let resp: InstrumentsResponse = self.http.post("/v1/instruments", &req).await?;
            for inst in resp.data {
                if let Some(name) = inst.name {
                    out.insert(inst.symbol, name);
                }
            }
        }
        Ok(out)
    }
}

/// Pick the name for `symbol` from `names`, if present.
pub(crate) fn pick_name<'a>(symbol: &str, names: &'a HashMap<String, String>) -> Option<&'a str> {
    names.get(symbol).map(String::as_str)
}

/// Convenience: resolve names for the symbols in a batch kline response.
pub(crate) async fn resolve_for_kline_batch(
    cache: &InstrumentNameCache,
    map: &HashMap<String, KlineData>,
) -> Result<HashMap<String, String>> {
    let symbols: Vec<String> = map.keys().cloned().collect();
    cache.resolve(&symbols).await
}
