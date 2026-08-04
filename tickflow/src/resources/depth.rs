use std::{collections::HashMap, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream};
use serde::Serialize;

use crate::{
    error::{Error, Result},
    http::{HttpClient, merge_maps},
    model::depth::{BatchDepthResponse, DepthResponse, MarketDepth},
    resources::{BATCH_CHUNK_SIZE, BATCH_CONCURRENCY},
};

pub struct Depth {
    http: Arc<HttpClient>,
}

impl Depth {
    pub(crate) fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    /// Start a builder for a single-symbol depth request.
    pub fn get(&self, symbol: impl Into<String>) -> SingleDepthBookBuilder<'_> {
        SingleDepthBookBuilder {
            client: self,
            symbol: symbol.into(),
        }
    }

    /// Start a builder for a batch depth request.
    pub fn batch<I, S>(&self, symbols: I) -> BatchDepthBookBuilder<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        BatchDepthBookBuilder {
            client: self,
            symbols: symbols.into_iter().map(Into::into).collect(),
        }
    }

    async fn execute_single(&self, symbol: String) -> Result<MarketDepth> {
        let resp: DepthResponse = self
            .http
            .get("/v1/depth", &GetDepthRequest { symbol })
            .await?;
        Ok(resp.data)
    }

    async fn execute_batch(&self, symbols: Vec<String>) -> Result<HashMap<String, MarketDepth>> {
        if symbols.is_empty() {
            return Ok(HashMap::new());
        }

        let chunks: Vec<String> = symbols
            .chunks(BATCH_CHUNK_SIZE)
            .map(|c| c.join(","))
            .collect();

        let http = Arc::clone(&self.http);
        let result: Vec<HashMap<String, MarketDepth>> = stream::iter(chunks)
            .map(move |chunk| {
                let http = http.clone();
                async move {
                    let out: BatchDepthResponse = http
                        .get("/v1/depth/batch", &BatchDepthRequest { symbols: chunk })
                        .await?;
                    Ok::<_, Error>(out.data)
                }
            })
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect()
            .await?;

        Ok(merge_maps(result))
    }
}

#[derive(Serialize)]
struct GetDepthRequest {
    symbol: String,
}

#[derive(Serialize)]
struct BatchDepthRequest {
    symbols: String,
}

/// Builder for a single-symbol depth request.
pub struct SingleDepthBookBuilder<'a> {
    client: &'a Depth,
    symbol: String,
}

impl SingleDepthBookBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<MarketDepth> {
        self.client.execute_single(self.symbol).await
    }
}

/// Builder for a batch depth request.
pub struct BatchDepthBookBuilder<'a> {
    client: &'a Depth,
    symbols: Vec<String>,
}

impl BatchDepthBookBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<HashMap<String, MarketDepth>> {
        self.client.execute_batch(self.symbols).await
    }
}
