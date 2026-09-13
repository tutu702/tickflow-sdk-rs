use crate::{
    error::{Error, Result},
    http::HttpClient,
    model::{
        BatchQuotesResponse, Quote, QuoteExt, QuoteParams, QuoteResponse, SessionStatus,
        tz_for_region,
    },
    resources::{BATCH_CHUNK_SIZE, BATCH_CONCURRENCY},
};
use futures::{StreamExt, TryStreamExt, stream};
use polars::prelude::*;
use reqwest::Method;
use serde::{Serialize, Serializer};
use std::{collections::HashMap, sync::Arc};
#[derive(Clone)]
pub struct Quotes {
    http: Arc<HttpClient>,
}

impl Quotes {
    pub(crate) fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    pub fn symbol(&self, symbol: impl Into<String>) -> QuotesBuilder<'_> {
        QuotesBuilder {
            client: self,
            method: Method::GET,
            mode: QueryMode::Symbols(vec![symbol.into()]),
            as_dataframe: false,
        }
    }

    pub fn universe(&self, universe: impl Into<String>) -> QuotesBuilder<'_> {
        QuotesBuilder {
            client: self,
            method: Method::GET,
            mode: QueryMode::Universes(vec![universe.into()]),
            as_dataframe: false,
        }
    }

    pub fn batch_symbols<I, S>(&self, symbols: I) -> QuotesBuilder<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        QuotesBuilder {
            client: self,
            method: Method::POST,
            mode: QueryMode::Symbols(symbols.into_iter().map(Into::into).collect()),
            as_dataframe: false,
        }
    }

    pub fn batch_universes<I, S>(&self, universes: I) -> QuotesBuilder<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        QuotesBuilder {
            client: self,
            method: Method::POST,
            mode: QueryMode::Universes(universes.into_iter().map(Into::into).collect()),
            as_dataframe: false,
        }
    }

    async fn execute_single(
        &self,
        method: Method,
        mode: QueryMode,
    ) -> Result<HashMap<String, Quote>> {
        let (item, is_universes) = match mode {
            QueryMode::Symbols(v) => match v.into_iter().next() {
                Some(item) => (item, false),
                None => return Ok(HashMap::new()),
            },
            QueryMode::Universes(v) => match v.into_iter().next() {
                Some(item) => (item, true),
                None => return Ok(HashMap::new()),
            },
        };

        let resp: BatchQuotesResponse = send_request(
            &self.http,
            method,
            std::slice::from_ref(&item),
            is_universes,
        )
        .await?;

        let map: HashMap<String, Quote> = resp
            .data
            .into_iter()
            .map(|q| (q.symbol.clone(), q))
            .collect();
        Ok(map)
    }

    async fn execute_batch(
        &self,
        method: Method,
        mode: QueryMode,
    ) -> Result<HashMap<String, Quote>> {
        let items = match &mode {
            QueryMode::Symbols(v) | QueryMode::Universes(v) => v.clone(),
        };

        if items.is_empty() {
            return Ok(HashMap::new());
        }

        let is_universes = matches!(mode, QueryMode::Universes(_));
        let chunks: Vec<Vec<String>> = items.chunks(BATCH_CHUNK_SIZE).map(|c| c.to_vec()).collect();

        let http = Arc::clone(&self.http);
        let results = stream::iter(chunks)
            .map(|chunk| {
                let http = http.clone();
                let method = method.clone();
                async move {
                    let resp: BatchQuotesResponse =
                        send_request(&http, method, &chunk, is_universes).await?;
                    let map: HashMap<String, Quote> = resp
                        .data
                        .into_iter()
                        .map(|q| (q.symbol.clone(), q))
                        .collect();
                    Ok::<_, Error>(map)
                }
            })
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        let mut out: HashMap<String, Quote> = HashMap::new();
        for r in results {
            out.extend(r);
        }
        Ok(out)
    }
}

/// Shared request dispatcher. `items` is one element for single and a chunk
/// for batch. `is_universes` controls which field the request populates.
async fn send_request(
    http: &HttpClient,
    method: Method,
    items: &[String],
    is_universes: bool,
) -> Result<BatchQuotesResponse> {
    match method {
        Method::GET => {
            let joined = items.join(",");
            let query = CsvQuery {
                symbols: (!is_universes).then(|| CommaSeparated(&joined)),
                universes: is_universes.then(|| CommaSeparated(&joined)),
            };
            http.get("/v1/quotes", &query).await
        }
        Method::POST => {
            let body = QuoteParams {
                symbols: (!is_universes).then(|| items.to_vec()),
                universes: is_universes.then(|| items.to_vec()),
            };
            http.post("/v1/quotes", &body).await
        }
        _ => unreachable!("Quotes only supports GET and POST"),
    }
}

/// Serializes as a single string. Used in the GET query so multiple values
/// become `A,B` rather than repeated keys.
struct CommaSeparated<'a>(&'a str);

impl Serialize for CommaSeparated<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0)
    }
}

#[derive(Serialize)]
struct CsvQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    symbols: Option<CommaSeparated<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    universes: Option<CommaSeparated<'a>>,
}

pub struct QuotesBuilder<'a> {
    client: &'a Quotes,
    method: Method,
    mode: QueryMode,
    as_dataframe: bool,
}

enum QueryMode {
    Symbols(Vec<String>),
    Universes(Vec<String>),
}

impl QuotesBuilder<'_> {
    /// Switch this builder into DataFrame response mode. Mirrors the
    /// `KlinesBuilderExt::as_dataframe()` toggle style.
    #[must_use]
    pub fn as_dataframe(mut self) -> Self {
        self.as_dataframe = true;
        self
    }

    /// Execute the request and return a [`QuoteResponse`].
    ///
    /// When `as_dataframe()` was called, returns a long-format polars
    /// [`DataFrame`] (one row per symbol, region-aware `trade_date` /
    /// `trade_time`, flattened `ext.*` columns); otherwise returns the
    /// unchanged `symbol -> Quote` map.
    #[must_use]
    pub async fn send(self) -> Result<QuoteResponse> {
        let map = match self.method {
            Method::GET => self.client.execute_single(self.method, self.mode).await,
            Method::POST => self.client.execute_batch(self.method, self.mode).await,
            _ => unreachable!("Quotes only supports GET and POST"),
        }?;

        if self.as_dataframe {
            Ok(QuoteResponse::DataFrame(quotes_to_dataframe(&map)?))
        } else {
            Ok(QuoteResponse::Raw(map))
        }
    }
}

fn quote_ext_inner(q: &Quote) -> Option<&QuoteExt> {
    match &q.ext {
        Some(crate::model::QuoteExtension::Cn(e))
        | Some(crate::model::QuoteExtension::Us(e))
        | Some(crate::model::QuoteExtension::Hk(e)) => Some(e),
        _ => None,
    }
}

/// Convert a `symbol -> Quote` map into a long-format polars [`DataFrame`].
fn quotes_to_dataframe(map: &HashMap<String, Quote>) -> Result<DataFrame> {
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort_unstable();

    let mut symbols: Vec<String> = Vec::with_capacity(keys.len());
    let mut regions: Vec<String> = Vec::with_capacity(keys.len());
    let mut last_price: Vec<f64> = Vec::with_capacity(keys.len());
    let mut opens: Vec<f64> = Vec::with_capacity(keys.len());
    let mut highs: Vec<f64> = Vec::with_capacity(keys.len());
    let mut lows: Vec<f64> = Vec::with_capacity(keys.len());
    let mut prev_close: Vec<f64> = Vec::with_capacity(keys.len());
    let mut volume: Vec<i64> = Vec::with_capacity(keys.len());
    let mut amount: Vec<f64> = Vec::with_capacity(keys.len());
    let mut timestamp: Vec<i64> = Vec::with_capacity(keys.len());
    let mut trade_date: Vec<Option<String>> = Vec::with_capacity(keys.len());
    let mut trade_time: Vec<Option<String>> = Vec::with_capacity(keys.len());
    let mut session: Vec<Option<&'static str>> = Vec::with_capacity(keys.len());
    let mut ext_amplitude: Vec<Option<f64>> = Vec::with_capacity(keys.len());
    let mut ext_change_amount: Vec<Option<f64>> = Vec::with_capacity(keys.len());
    let mut ext_change_pct: Vec<Option<f64>> = Vec::with_capacity(keys.len());
    let mut ext_name: Vec<Option<String>> = Vec::with_capacity(keys.len());
    let mut ext_turnover_rate: Vec<Option<f64>> = Vec::with_capacity(keys.len());

    for key in keys {
        let q = &map[key];
        let tz = tz_for_region(q.region);

        symbols.push(key.clone());
        regions.push(q.region.as_str().to_owned());
        last_price.push(q.last_price);
        opens.push(q.open);
        highs.push(q.high);
        lows.push(q.low);
        prev_close.push(q.prev_close);
        volume.push(q.volume);
        amount.push(q.amount);
        timestamp.push(q.timestamp);

        match tz.and_then(|tz| {
            chrono::DateTime::from_timestamp_millis(q.timestamp).map(|d| d.with_timezone(&tz))
        }) {
            Some(local) => {
                trade_date.push(Some(local.format("%Y-%m-%d").to_string()));
                trade_time.push(Some(local.format("%Y-%m-%d %H:%M:%S").to_string()));
            }
            None => {
                trade_date.push(None);
                trade_time.push(None);
            }
        }

        session.push(q.session.as_ref().map(SessionStatus::as_str));

        match quote_ext_inner(q) {
            Some(e) => {
                ext_amplitude.push(e.amplitude);
                ext_change_amount.push(e.change_amount);
                ext_change_pct.push(e.change_pct);
                ext_name.push(e.name.clone());
                ext_turnover_rate.push(e.turnover_rate);
            }
            None => {
                ext_amplitude.push(None);
                ext_change_amount.push(None);
                ext_change_pct.push(None);
                ext_name.push(None);
                ext_turnover_rate.push(None);
            }
        }
    }

    df![
        "symbol" => symbols,
        "region" => regions,
        "last_price" => last_price,
        "open" => opens,
        "high" => highs,
        "low" => lows,
        "prev_close" => prev_close,
        "volume" => volume,
        "amount" => amount,
        "timestamp" => timestamp,
        "trade_date" => trade_date,
        "trade_time" => trade_time,
        "session" => session,
        "ext.amplitude" => ext_amplitude,
        "ext.change_amount" => ext_change_amount,
        "ext.change_pct" => ext_change_pct,
        "ext.name" => ext_name,
        "ext.turnover_rate" => ext_turnover_rate,
    ]
    .map_err(|e| Error::DataFrame(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{QuoteExtension, Region, SessionStatus};

    fn cn_quote() -> Quote {
        Quote {
            symbol: "600000.SH".to_string(),
            last_price: 10.5,
            open: 10.0,
            high: 10.7,
            low: 9.9,
            prev_close: 10.0,
            volume: 1_000,
            amount: 10_500.0,
            // 2023-11-15 06:13:20 Asia/Shanghai = 2023-11-14 22:13:20 UTC
            timestamp: 1_700_000_000_000,
            ext: Some(QuoteExtension::Cn(QuoteExt {
                amplitude: Some(0.08),
                change_amount: Some(0.5),
                change_pct: Some(0.05),
                name: Some("浦发银行".to_string()),
                turnover_rate: Some(0.012),
            })),
            region: Region::Cn,
            session: Some(SessionStatus::Regular),
        }
    }

    fn us_quote() -> Quote {
        Quote {
            symbol: "AAPL.US".to_string(),
            last_price: 190.0,
            open: 188.0,
            high: 191.0,
            low: 187.5,
            prev_close: 187.0,
            volume: 50_000,
            amount: 9_500_000.0,
            // 2023-11-14 22:13:20 UTC = 2023-11-14 17:13:20 America/New_York
            timestamp: 1_700_000_000_000,
            ext: Some(QuoteExtension::Us(QuoteExt {
                amplitude: Some(0.018),
                change_amount: Some(3.0),
                change_pct: Some(0.016),
                name: Some("Apple Inc.".to_string()),
                turnover_rate: None,
            })),
            region: Region::Us,
            session: Some(SessionStatus::PreMarket),
        }
    }

    #[test]
    fn quote_to_dataframe_basic() {
        let mut map = HashMap::new();
        map.insert("600000.SH".to_string(), cn_quote());
        map.insert("AAPL.US".to_string(), us_quote());

        let df = quotes_to_dataframe(&map).unwrap();

        // 18 columns: 12 base + session + 5 ext.*
        assert_eq!(df.width(), 18);
        assert_eq!(df.height(), 2);

        // Sort order: "600000.SH" < "AAPL.US" — rows emitted in symbol order.
        let symbol_col: Vec<Option<&str>> = df
            .column("symbol")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(symbol_col, vec![Some("600000.SH"), Some("AAPL.US")]);

        // CN row: Shanghai tz.
        let dates: Vec<Option<&str>> = df
            .column("trade_date")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        let times: Vec<Option<&str>> = df
            .column("trade_time")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(dates[0].unwrap(), "2023-11-15");
        assert_eq!(times[0].unwrap(), "2023-11-15 06:13:20");

        // US row: New_York tz — same UTC instant falls on 2023-11-14 there.
        assert_eq!(dates[1].unwrap(), "2023-11-14");
        assert_eq!(times[1].unwrap(), "2023-11-14 17:13:20");

        // ext.* columns populated for both rows.
        let amplitudes: Vec<Option<f64>> = df
            .column("ext.amplitude")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(amplitudes[0], Some(0.08));
        assert_eq!(amplitudes[1], Some(0.018));

        let names: Vec<Option<&str>> = df
            .column("ext.name")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(names[0].unwrap(), "浦发银行");
        assert_eq!(names[1].unwrap(), "Apple Inc.");

        // turnover_rate: Some for CN, None for US.
        let turnover: Vec<Option<f64>> = df
            .column("ext.turnover_rate")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(turnover[0], Some(0.012));
        assert_eq!(turnover[1], None);
    }

    #[test]
    fn quote_to_dataframe_no_ext() {
        let mut q = cn_quote();
        q.ext = None;
        let mut map = HashMap::new();
        map.insert("600000.SH".to_string(), q);

        let df = quotes_to_dataframe(&map).unwrap();
        assert_eq!(df.height(), 1);

        for col in [
            "ext.amplitude",
            "ext.change_amount",
            "ext.change_pct",
            "ext.name",
            "ext.turnover_rate",
        ] {
            let values: Vec<Option<f64>> = if col == "ext.name" {
                // Column is string-typed; check via a parallel probe below.
                continue;
            } else {
                df.column(col).unwrap().f64().unwrap().into_iter().collect()
            };
            assert_eq!(values, vec![None], "{col} should be null");
        }

        // ext.name column is string-typed and should be null too.
        let names: Vec<Option<&str>> = df
            .column("ext.name")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(names, vec![None]);
    }

    #[test]
    fn quote_to_dataframe_empty() {
        let map: HashMap<String, Quote> = HashMap::new();
        let df = quotes_to_dataframe(&map).unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 18);

        // Downstream `.column("symbol")` must not panic on an empty frame.
        let symbol_col = df.column("symbol").unwrap();
        assert_eq!(symbol_col.len(), 0);
    }

    #[test]
    fn quote_to_dataframe_session_serialized() {
        let mut q = cn_quote();
        q.session = Some(SessionStatus::PreMarket);
        let mut map = HashMap::new();
        map.insert("600000.SH".to_string(), q);

        let df = quotes_to_dataframe(&map).unwrap();
        let sessions: Vec<Option<&str>> = df
            .column("session")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(sessions, vec![Some("pre_market")]);

        // Also verify `regular` for the default cn_quote fixture.
        let mut map2 = HashMap::new();
        map2.insert("600000.SH".to_string(), cn_quote());
        let df2 = quotes_to_dataframe(&map2).unwrap();
        let sessions2: Vec<Option<&str>> = df2
            .column("session")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(sessions2, vec![Some("regular")]);
    }
}
