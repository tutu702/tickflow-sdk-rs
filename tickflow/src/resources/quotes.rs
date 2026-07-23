use crate::{
    error::{Error, Result},
    http::HttpClient,
    model::{BatchQuotesResponse, Quote, quote::BatchQuotesRequest},
};
use futures::{StreamExt, TryStreamExt, stream};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct Quotes {
    http: Arc<HttpClient>,
}

impl Quotes {
    pub(crate) fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    /// Get real-time quotes for symbols or universes.
    /// Must provide either `symbols` or `universes`, but not both.
    pub async fn get<I, S>(&self, symbols: I) -> Result<HashMap<String, Quote>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        const CHUNK_SIZE: usize = 500;
        let all: Vec<String> = symbols.into_iter().map(|v| v.into()).collect();
        let chunks: Vec<Vec<String>> = all.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();

        let http = Arc::clone(&self.http);
        let results = stream::iter(chunks)
            .map(|chunk| {
                let http = http.clone();
                async move {
                    let req = BatchQuotesRequest { symbols: chunk };
                    let resp: BatchQuotesResponse = http.post("/v1/quotes", &req).await?;
                    let map: HashMap<String, Quote> = resp
                        .data
                        .into_iter()
                        .map(|q| (q.symbol.clone(), q))
                        .collect();
                    Ok::<_, Error>(map)
                }
            })
            .buffer_unordered(5)
            .try_collect::<Vec<_>>()
            .await?;

        let mut out: HashMap<String, Quote> = HashMap::new();
        for r in results {
            out.extend(r);
        }
        Ok(out)
    }
}
