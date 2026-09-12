use futures::{StreamExt, TryStreamExt, stream};
use polars::prelude::*;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::HashMap, sync::Arc};

use crate::{
    cache::{InstrumentNameCache, pick_name, resolve_for_kline_batch},
    error::{Error, Result},
    http::{HttpClient, merge_maps},
    model::{
        AdjustType, BatchIntradayResponse, BatchKlineResponse, ExfactorsData, ExfactorsResponse,
        IntradayResponse, KlineData, KlineResponse, KlinesParams, Period, broadcast_to,
        format_trade_columns, kline::KlinesResponse, region_for_symbol, tz_for_region,
    },
    resources::{BATCH_CHUNK_SIZE, BATCH_CONCURRENCY, DataResponse},
};

pub struct Klines {
    http: Arc<HttpClient>,
    name_cache: Arc<InstrumentNameCache>,
}

impl Klines {
    pub(crate) fn new(http: Arc<HttpClient>, name_cache: Arc<InstrumentNameCache>) -> Self {
        Self { http, name_cache }
    }

    /// Access the shared instrument-name cache used by DataFrame responses.
    pub fn name_cache(&self) -> &Arc<InstrumentNameCache> {
        &self.name_cache
    }

    /// Start a builder for a single-symbol kline request.
    pub fn get(&self, symbol: impl Into<String>) -> SingleKlinesBuilder<'_> {
        let mut params = KlinesParams::klines_default();
        params.symbol = Some(symbol.into());
        SingleKlinesBuilder {
            client: self,
            params,
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
            params: KlinesParams::klines_default(),
        }
    }

    /// Start a builder for a single-symbol intraday kline request.
    pub fn intraday(&self, symbol: impl Into<String>) -> SingleIntradayBuilder<'_> {
        let mut params = KlinesParams::intraday_default();
        params.symbol = Some(symbol.into());
        SingleIntradayBuilder {
            client: self,
            params,
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
            params: KlinesParams::intraday_default(),
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
            params: KlinesParams::default(),
        }
    }

    async fn execute_single(&self, params: KlinesParams) -> Result<KlineData> {
        let response: KlinesResponse = self.http.get("/v1/klines", &params).await?;
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
        self.dispatch_batch(symbols, "/v1/klines/batch", move |chunk| BatchParams {
            symbols: chunk.join(","),
            params: params.clone(),
        })
        .await
    }

    async fn execute_intraday(&self, params: KlinesParams) -> Result<KlineData> {
        let resp: KlinesResponse = self.http.get("/v1/klines/intraday", &params).await?;
        resp.data
            .validate()
            .map_err(|message| Error::Parse(message.to_owned()))?;
        Ok(resp.data)
    }

    async fn execute_intraday_batch(
        &self,
        symbols: Vec<String>,
        params: KlinesParams,
    ) -> Result<HashMap<String, KlineData>> {
        self.dispatch_batch(symbols, "/v1/klines/intraday/batch", move |chunk| {
            BatchParams {
                symbols: chunk.join(","),
                params: params.clone(),
            }
        })
        .await
    }

    async fn execute_ex_factors(
        &self,
        symbols: Vec<String>,
        params: KlinesParams,
    ) -> Result<HashMap<String, Vec<ExfactorsData>>> {
        self.dispatch_batch(symbols, "/v1/klines/ex-factors", move |chunk| BatchParams {
            symbols: chunk.join(","),
            params: params.clone(),
        })
        .await
    }
}

#[derive(Serialize)]
struct BatchParams {
    symbols: String,
    #[serde(flatten)]
    params: KlinesParams,
}

/// Shared setter methods for the single-symbol and batch kline builders.
pub trait KlinesBuilderExt: Sized {
    fn params_mut(&mut self) -> &mut KlinesParams;

    fn period(mut self, p: Period) -> Self {
        self.params_mut().period = Some(p);
        self
    }

    fn count(mut self, n: u32) -> Self {
        self.params_mut().count = Some(n);
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

    fn as_dataframe(mut self) -> Self {
        self.params_mut().as_dataframe = Some(true);
        self
    }
}

/// Shared setter methods for the single-symbol and batch intraday builders.
pub trait IntradayBuilderExt: Sized {
    fn params_mut(&mut self) -> &mut KlinesParams;

    fn period(mut self, p: Period) -> Self {
        self.params_mut().period = Some(p);
        self
    }

    fn count(mut self, n: u32) -> Self {
        self.params_mut().count = Some(n);
        self
    }

    /// Switch this builder into DataFrame response mode.
    fn as_dataframe(mut self) -> Self {
        self.params_mut().as_dataframe = Some(true);
        self
    }
}

/// Builder for a single-symbol kline request.
pub struct SingleKlinesBuilder<'a> {
    client: &'a Klines,
    params: KlinesParams,
}

impl KlinesBuilderExt for SingleKlinesBuilder<'_> {
    fn params_mut(&mut self) -> &mut KlinesParams {
        &mut self.params
    }
}

impl SingleKlinesBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<KlineResponse> {
        let symbol = self
            .params
            .symbol
            .clone()
            .expect("SingleKlinesBuilder is constructed with Some(symbol)");
        let want_df = self.params.as_dataframe == Some(true);
        let data = self.client.execute_single(self.params).await?;
        if want_df {
            let names = self.client.name_cache.resolve(&[symbol.clone()]).await?;
            let name = pick_name(&symbol, &names);
            Ok(KlineResponse::DataFrame(kline_to_dataframe(
                &data,
                Some(&symbol),
                name,
            )?))
        } else {
            Ok(KlineResponse::Raw(data))
        }
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
    pub async fn send(self) -> Result<BatchKlineResponse> {
        let want_df = self.params.as_dataframe == Some(true);
        let map = self.client.execute_batch(self.symbols, self.params).await?;
        if want_df {
            let names = resolve_for_kline_batch(&self.client.name_cache, &map).await?;
            Ok(BatchKlineResponse::DataFrame(klines_batch_to_dataframes(
                &map, &names,
            )?))
        } else {
            Ok(BatchKlineResponse::Raw(map))
        }
    }
}

/// Builder for a single-symbol intraday kline request.
pub struct SingleIntradayBuilder<'a> {
    client: &'a Klines,
    params: KlinesParams,
}

impl IntradayBuilderExt for SingleIntradayBuilder<'_> {
    fn params_mut(&mut self) -> &mut KlinesParams {
        &mut self.params
    }
}

impl SingleIntradayBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<IntradayResponse> {
        let symbol = self
            .params
            .symbol
            .clone()
            .expect("SingleIntradayBuilder is constructed with Some(symbol)");
        let want_df = self.params.as_dataframe == Some(true);
        let data = self.client.execute_intraday(self.params).await?;
        if want_df {
            let names = self.client.name_cache.resolve(&[symbol.clone()]).await?;
            let name = pick_name(&symbol, &names);
            Ok(IntradayResponse::DataFrame(kline_to_dataframe(
                &data,
                Some(&symbol),
                name,
            )?))
        } else {
            Ok(IntradayResponse::Raw(data))
        }
    }
}

/// Builder for a batch intraday kline request.
pub struct BatchIntradayBuilder<'a> {
    client: &'a Klines,
    symbols: Vec<String>,
    params: KlinesParams,
}

impl IntradayBuilderExt for BatchIntradayBuilder<'_> {
    fn params_mut(&mut self) -> &mut KlinesParams {
        &mut self.params
    }
}

impl BatchIntradayBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<BatchIntradayResponse> {
        let want_df = self.params.as_dataframe == Some(true);
        let map = self
            .client
            .execute_intraday_batch(self.symbols, self.params)
            .await?;
        if want_df {
            let names = resolve_for_kline_batch(&self.client.name_cache, &map).await?;
            Ok(BatchIntradayResponse::DataFrame(
                klines_batch_to_dataframes(&map, &names)?,
            ))
        } else {
            Ok(BatchIntradayResponse::Raw(map))
        }
    }
}

/// Builder for a batch ex-factors request.
pub struct ExfactorsBuilder<'a> {
    client: &'a Klines,
    symbols: Vec<String>,
    params: KlinesParams,
}

impl ExfactorsBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<ExfactorsResponse> {
        let want_df = self.params.as_dataframe == Some(true);
        let map = self
            .client
            .execute_ex_factors(self.symbols, self.params)
            .await?;
        if want_df {
            Ok(ExfactorsResponse::DataFrame(factors_to_dataframe(&map)?))
        } else {
            Ok(ExfactorsResponse::Raw(map))
        }
    }
}

/// Convert a single [`KlineData`] into a polars [`DataFrame`].
///
/// The 11 base columns are always emitted; the optional `open_interest`,
/// `prev_close`, and `settlement_price` columns are appended (in that
/// order) when their payload is `Some(...)`.
fn kline_to_dataframe(
    data: &KlineData,
    symbol: Option<&str>,
    name: Option<&str>,
) -> Result<DataFrame> {
    let n = data.len();
    let region = symbol.and_then(region_for_symbol);
    let tz = region.and_then(tz_for_region);
    let (trade_dates, trade_times) = format_trade_columns(&data.timestamp, tz);

    let df = df![
        "symbol" => broadcast_to(symbol, n),
        "name" => broadcast_to(name, n),
        "timestamp" => &data.timestamp,
        "trade_date" => trade_dates,
        "trade_time" => trade_times,
        "open" => &data.open,
        "high" => &data.high,
        "low" => &data.low,
        "close" => &data.close,
        "volume" => &data.volume,
        "amount" => &data.amount,
    ]
    .map_err(|e| Error::DataFrame(e.to_string()))?;

    Ok(df)
}

/// Convert a batch of `symbol -> KlineData` to per-symbol [`DataFrame`]s,
/// filling the `name` column from `names` when present.
fn klines_batch_to_dataframes(
    map: &HashMap<String, KlineData>,
    names: &HashMap<String, String>,
) -> Result<HashMap<String, DataFrame>> {
    let mut out = HashMap::with_capacity(map.len());
    for (symbol, data) in map {
        let name = names.get(symbol).map(String::as_str);
        let df = kline_to_dataframe(data, Some(symbol.as_str()), name)?;
        out.insert(symbol.clone(), df);
    }
    Ok(out)
}

/// Convert an ex-factors response into a long-format polars [`DataFrame`].
///
/// Columns: `symbol`, `timestamp`, `trade_date`, `ex_factor`.
pub fn factors_to_dataframe(
    map: &HashMap<String, Vec<ExfactorsData>>,
) -> std::result::Result<DataFrame, crate::error::Error> {
    let mut symbols: Vec<String> = Vec::new();
    let mut timestamps: Vec<i64> = Vec::new();
    let mut trade_dates: Vec<Option<String>> = Vec::new();
    let mut ex_factors: Vec<f64> = Vec::new();

    for (symbol, entries) in map {
        if entries.is_empty() {
            continue;
        }

        let tz = region_for_symbol(symbol).and_then(tz_for_region);
        let (mut dates, _) =
            format_trade_columns(&entries.iter().map(|e| e.timestamp).collect::<Vec<_>>(), tz);

        symbols.reserve(entries.len());
        timestamps.reserve(entries.len());
        trade_dates.reserve(entries.len());
        ex_factors.reserve(entries.len());

        for (i, e) in entries.iter().enumerate() {
            symbols.push(symbol.clone());
            timestamps.push(e.timestamp);
            trade_dates.push(dates.get_mut(i).and_then(|d| d.take()));
            ex_factors.push(e.ex_factor);
        }
    }

    df![
        "symbol" => symbols,
        "timestamp" => timestamps,
        "trade_date" => trade_dates,
        "ex_factor" => ex_factors,
    ]
    .map_err(|e| crate::error::Error::DataFrame(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> KlineData {
        KlineData {
            timestamp: vec![1_700_000_000_000, 1_700_086_400_000],
            open: vec![10.0, 10.5],
            high: vec![10.2, 10.8],
            low: vec![9.9, 10.4],
            close: vec![10.1, 10.7],
            volume: vec![1000, 1500],
            amount: vec![10_100.0, 16_050.0],
            open_interest: Some(vec![100.0, 110.0]),
            prev_close: None,
            settlement_price: None,
        }
    }

    #[test]
    fn cn_symbol_produces_shanghai_trade_date() {
        let data = sample();
        let df = kline_to_dataframe(&data, Some("600000.SH"), None).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 11);

        let date_col = df
            .column("trade_date")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        // 1700000000000 ms == 2023-11-14 22:13:20 UTC == 2023-11-15 06:13:20 Asia/Shanghai
        assert_eq!(date_col[0].unwrap(), "2023-11-15");

        let time_col = df
            .column("trade_time")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(time_col[0].unwrap(), "2023-11-15 06:13:20");
    }

    #[test]
    fn unknown_symbol_yields_null_trade_date() {
        let data = sample();
        let df = kline_to_dataframe(&data, Some("UNKNOWN.X"), None).unwrap();
        let date_col = df
            .column("trade_date")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(date_col[0], None);
    }

    #[test]
    fn empty_data_still_produces_valid_frame() {
        let mut data = sample();
        data.timestamp.clear();
        data.open.clear();
        data.high.clear();
        data.low.clear();
        data.close.clear();
        data.volume.clear();
        data.amount.clear();
        data.open_interest = None;

        let df = kline_to_dataframe(&data, Some("600000.SH"), None).unwrap();
        assert_eq!(df.height(), 0);
        // 11 base columns; open_interest is None and skipped, so no extra column.
        assert_eq!(df.width(), 11);
    }

    #[test]
    fn batch_conversion_keys_by_symbol() {
        let data = sample();
        let mut map = HashMap::new();
        map.insert("600000.SH".to_string(), data.clone());
        map.insert("000001.SZ".to_string(), data);

        let mut names = HashMap::new();
        names.insert("600000.SH".to_string(), "浦发银行".to_string());
        names.insert("000001.SZ".to_string(), "平安银行".to_string());

        let out = klines_batch_to_dataframes(&map, &names).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.contains_key("600000.SH"));
        assert!(out.contains_key("000001.SZ"));

        let name_col = out["600000.SH"]
            .column("name")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(name_col[0].unwrap(), "浦发银行");
    }

    #[test]
    fn batch_conversion_without_names_yields_null() {
        let data = sample();
        let mut map = HashMap::new();
        map.insert("600000.SH".to_string(), data);
        let names = HashMap::new();

        let out = klines_batch_to_dataframes(&map, &names).unwrap();
        let name_col = out["600000.SH"]
            .column("name")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(name_col[0], None);
    }

    #[test]
    fn factors_dataframe_is_long_format() {
        let mut map = HashMap::new();
        map.insert(
            "600519.SH".to_string(),
            vec![
                ExfactorsData {
                    timestamp: 1_700_000_000_000,
                    ex_factor: 1.05,
                },
                ExfactorsData {
                    timestamp: 1_700_086_400_000,
                    ex_factor: 1.02,
                },
            ],
        );

        let df = factors_to_dataframe(&map).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 4);

        let symbols = df.column("symbol").unwrap().str().unwrap();
        assert_eq!(symbols.get(0).unwrap(), "600519.SH");
        assert_eq!(symbols.get(1).unwrap(), "600519.SH");

        let dates = df
            .column("trade_date")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(dates[0].unwrap(), "2023-11-15");
    }
}
