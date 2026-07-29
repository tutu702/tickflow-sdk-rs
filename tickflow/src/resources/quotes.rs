use crate::{
    error::{Error, Result},
    http::HttpClient,
    model::{BatchQuotesResponse, Quote, QuoteParams},
    resources::BATCH_CONCURRENCY,
};
use futures::{StreamExt, TryStreamExt, stream};
use reqwest::Method;
use serde::{Serialize, Serializer};
use std::{collections::HashMap, sync::Arc};

const CHUNK_SIZE: usize = 500;

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
        }
    }

    pub fn universe(&self, universe: impl Into<String>) -> QuotesBuilder<'_> {
        QuotesBuilder {
            client: self,
            method: Method::GET,
            mode: QueryMode::Universes(vec![universe.into()]),
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
        let chunks: Vec<Vec<String>> = items.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();

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
}

enum QueryMode {
    Symbols(Vec<String>),
    Universes(Vec<String>),
}

impl QuotesBuilder<'_> {
    /// Execute the request and return a map keyed by symbol.
    #[must_use]
    pub async fn send(self) -> Result<HashMap<String, Quote>> {
        match self.method {
            Method::GET => self.client.execute_single(self.method, self.mode).await,
            Method::POST => self.client.execute_batch(self.method, self.mode).await,
            _ => unreachable!("Quotes only supports GET and POST"),
        }
    }
}
