use futures::{StreamExt, TryStreamExt, stream};
use serde::Serialize;
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::{Error, Result},
    http::{HttpClient, merge_maps},
    model::{
        AdjustType, KlineData, KlinesParams, Period,
        kline::{BatchKlinesResponse, KlinesResponse},
    },
    resources::BATCH_CONCURRENCY,
};

const MAX_SYMBOLS_PER_BATCH: usize = 100;

pub struct Klines {
    http: Arc<HttpClient>,
}

impl Klines {
    pub(crate) fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    /// Start a builder for a single-symbol kline request.
    pub fn get(&self, symbol: impl Into<String>) -> SingleKlinesBuilder<'_> {
        SingleKlinesBuilder {
            client: self,
            symbol: symbol.into(),
            params: KlinesParams::default(),
        }
    }

    /// Start a builder for a batch kline request.
    pub fn batch<I, S>(&self, symbols: I) -> BatchKlinesBuilder<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        BatchKlinesBuilder {
            client: self,
            symbols: symbols.into_iter().map(Into::into).collect(),
            params: KlinesParams::default(),
        }
    }

    async fn execute_single(&self, symbol: String, params: KlinesParams) -> Result<KlineData> {
        let response: KlinesResponse = self
            .http
            .get("/v1/klines", &GetKlinesRequest { symbol, params })
            .await?;
        response
            .data
            .validate()
            .map_err(|message| Error::Parse(message.to_owned()))?;
        Ok(response.data)
    }

    async fn execute_batch(
        &self,
        symbols: Vec<String>,
        params: KlinesParams,
    ) -> Result<HashMap<String, KlineData>> {
        if symbols.is_empty() {
            return Ok(HashMap::new());
        }

        let chunks: Vec<String> = symbols
            .chunks(MAX_SYMBOLS_PER_BATCH)
            .map(|c| c.join(","))
            .collect();

        let http = Arc::clone(&self.http);

        let results: Vec<HashMap<String, KlineData>> = stream::iter(chunks)
            .map(move |symbols_csv| {
                let http = Arc::clone(&http);
                let params = params.clone();
                async move {
                    let resp: BatchKlinesResponse = http
                        .get(
                            "/v1/klines/batch",
                            &BatchKlinesRequest {
                                symbols: symbols_csv,
                                params,
                            },
                        )
                        .await?;
                    Ok::<_, Error>(resp.data)
                }
            })
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect()
            .await?;

        Ok(merge_maps(results))
    }
}

#[derive(Serialize)]
struct GetKlinesRequest {
    symbol: String,
    #[serde(flatten)]
    params: KlinesParams,
}

#[derive(Serialize)]
struct BatchKlinesRequest {
    symbols: String,
    #[serde(flatten)]
    params: KlinesParams,
}

/// Shared setter methods for the single-symbol and batch kline builders.
pub trait KlinesBuilderExt: Sized {
    fn params_mut(&mut self) -> &mut KlinesParams;

    fn period(mut self, p: Period) -> Self {
        self.params_mut().period = p;
        self
    }

    fn count(mut self, n: u32) -> Self {
        self.params_mut().count = n;
        self
    }

    fn adjust(mut self, a: AdjustType) -> Self {
        self.params_mut().adjust = a;
        self
    }

    fn start_time(mut self, t: i64) -> Self {
        self.params_mut().start_time = Some(t);
        self
    }

    fn end_time(mut self, t: i64) -> Self {
        self.params_mut().end_time = Some(t);
        self
    }
}

/// Builder for a single-symbol kline request.
pub struct SingleKlinesBuilder<'a> {
    client: &'a Klines,
    symbol: String,
    params: KlinesParams,
}

impl KlinesBuilderExt for SingleKlinesBuilder<'_> {
    fn params_mut(&mut self) -> &mut KlinesParams {
        &mut self.params
    }
}

impl SingleKlinesBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<KlineData> {
        self.client.execute_single(self.symbol, self.params).await
    }
}

/// Builder for a batch kline request.
pub struct BatchKlinesBuilder<'a> {
    client: &'a Klines,
    symbols: Vec<String>,
    params: KlinesParams,
}

impl KlinesBuilderExt for BatchKlinesBuilder<'_> {
    fn params_mut(&mut self) -> &mut KlinesParams {
        &mut self.params
    }
}

impl BatchKlinesBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<HashMap<String, KlineData>> {
        self.client.execute_batch(self.symbols, self.params).await
    }
}
