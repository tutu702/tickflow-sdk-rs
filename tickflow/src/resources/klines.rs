use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::{Error, Result},
    http::{HttpClient, merge_maps},
    model::{
        AdjustType, KlineData, KlinesParams, Period,
        kline::{ExfactorsData, KlinesResponse},
    },
    resources::{BATCH_CHUNK_SIZE, BATCH_CONCURRENCY},
};

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

    /// Start a builder for a single-symbol intraday kline request.
    pub fn intraday(&self, symbol: impl Into<String>) -> SingleIntradayBuilder<'_> {
        SingleIntradayBuilder {
            client: self,
            params: IntradayParams {
                symbol: symbol.into(),
                period: None,
                count: None,
            },
        }
    }

    /// Start a builder for a batch intraday kline request.
    pub fn intraday_batch<I, S>(&self, symbols: I) -> BatchIntradayBuilder<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        BatchIntradayBuilder {
            client: self,
            symbols: symbols.into_iter().map(Into::into).collect(),
            params: IntradayParams::default(),
        }
    }

    /// Start a builder for a batch ex-factors request.
    pub fn ex_factors<I, S>(&self, symbols: I) -> ExfactorsBuilder<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ExfactorsBuilder {
            client: self,
            symbols: symbols.into_iter().map(Into::into).collect(),
            start_time: None,
            end_time: None,
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

    async fn dispatch_batch<T, B, F>(
        &self,
        symbols: Vec<String>,
        path: &'static str,
        build: F,
    ) -> Result<HashMap<String, T>>
    where
        T: DeserializeOwned + Send + 'static,
        B: Serialize,
        F: Fn(&[String]) -> B + Send + Sync + 'static,
    {
        if symbols.is_empty() {
            return Ok(HashMap::new());
        }

        let build = Arc::new(build);
        let chunks: Vec<Vec<String>> = symbols
            .chunks(BATCH_CHUNK_SIZE)
            .map(|c| c.to_vec())
            .collect();
        let http = Arc::clone(&self.http);

        let results: Vec<HashMap<String, T>> = stream::iter(chunks)
            .map(move |chunk| {
                let build = Arc::clone(&build);
                let http = http.clone();
                async move {
                    let body = build(&chunk);
                    let resp: DataResponse<T> = http.get(path, &body).await?;
                    Ok::<_, Error>(resp.data)
                }
            })
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect()
            .await?;

        Ok(merge_maps(results))
    }

    async fn execute_batch(
        &self,
        symbols: Vec<String>,
        params: KlinesParams,
    ) -> Result<HashMap<String, KlineData>> {
        self.dispatch_batch(symbols, "/v1/klines/batch", move |chunk| {
            BatchKlinesRequest {
                symbols: chunk.join(","),
                params: params.clone(),
            }
        })
        .await
    }

    async fn execute_intraday(&self, params: IntradayParams) -> Result<KlineData> {
        let resp: KlinesResponse = self.http.get("/v1/klines/intraday", &params).await?;
        resp.data
            .validate()
            .map_err(|message| Error::Parse(message.to_owned()))?;
        Ok(resp.data)
    }

    async fn execute_intraday_batch(
        &self,
        symbols: Vec<String>,
        params: IntradayParams,
    ) -> Result<HashMap<String, KlineData>> {
        let kline_params = KlinesParams {
            period: params.period.unwrap_or_default(),
            count: params.count.unwrap_or(100),
            adjust: None,
            start_time: None,
            end_time: None,
        };
        self.dispatch_batch(symbols, "/v1/klines/batch", move |chunk| {
            BatchKlinesRequest {
                symbols: chunk.join(","),
                params: kline_params.clone(),
            }
        })
        .await
    }

    async fn execute_ex_factors(
        &self,
        symbols: Vec<String>,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<HashMap<String, Vec<ExfactorsData>>> {
        self.dispatch_batch(symbols, "/v1/klines/ex-factors", move |chunk| {
            ExfactorsRequest {
                symbols: chunk.join(","),
                start_time,
                end_time,
            }
        })
        .await
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

#[derive(Deserialize)]
struct DataResponse<T> {
    data: HashMap<String, T>,
}

#[derive(Default, Serialize)]
pub struct IntradayParams {
    symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    period: Option<Period>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<u32>,
}

#[derive(Serialize)]
struct ExfactorsRequest {
    symbols: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_time: Option<i64>,
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
        self.params_mut().adjust = Some(a);
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

/// Shared setter methods for the single-symbol and batch intraday builders.
pub trait IntradayBuilderExt: Sized {
    fn params_mut(&mut self) -> &mut IntradayParams;

    fn period(mut self, p: Period) -> Self {
        self.params_mut().period = Some(p);
        self
    }

    fn count(mut self, n: u32) -> Self {
        self.params_mut().count = Some(n);
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

/// Builder for a single-symbol intraday kline request.
pub struct SingleIntradayBuilder<'a> {
    client: &'a Klines,
    params: IntradayParams,
}

impl IntradayBuilderExt for SingleIntradayBuilder<'_> {
    fn params_mut(&mut self) -> &mut IntradayParams {
        &mut self.params
    }
}

impl SingleIntradayBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<KlineData> {
        self.client.execute_intraday(self.params).await
    }
}

/// Builder for a batch intraday kline request.
pub struct BatchIntradayBuilder<'a> {
    client: &'a Klines,
    symbols: Vec<String>,
    params: IntradayParams,
}

impl IntradayBuilderExt for BatchIntradayBuilder<'_> {
    fn params_mut(&mut self) -> &mut IntradayParams {
        &mut self.params
    }
}

impl BatchIntradayBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<HashMap<String, KlineData>> {
        self.client
            .execute_intraday_batch(self.symbols, self.params)
            .await
    }
}

/// Builder for a batch ex-factors request.
pub struct ExfactorsBuilder<'a> {
    client: &'a Klines,
    symbols: Vec<String>,
    start_time: Option<i64>,
    end_time: Option<i64>,
}

impl ExfactorsBuilder<'_> {
    pub fn start_time(mut self, t: i64) -> Self {
        self.start_time = Some(t);
        self
    }

    pub fn end_time(mut self, t: i64) -> Self {
        self.end_time = Some(t);
        self
    }

    #[must_use]
    pub async fn send(self) -> Result<HashMap<String, Vec<ExfactorsData>>> {
        self.client
            .execute_ex_factors(self.symbols, self.start_time, self.end_time)
            .await
    }
}
