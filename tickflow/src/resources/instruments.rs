use std::sync::Arc;

use serde::Serialize;

use crate::{
    error::{Error, Result},
    http::HttpClient,
    model::{Instrument, InstrumentsResponse},
    resources::BATCH_CHUNK_SIZE,
};

pub struct Instruments {
    http: Arc<HttpClient>,
}

impl Instruments {
    pub(crate) fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    /// Start a builder for a single-symbol instrument request.
    pub fn get(&self, symbol: impl Into<String>) -> SingleInstrumentsBuilder<'_> {
        SingleInstrumentsBuilder {
            client: self,
            symbol: symbol.into(),
        }
    }

    /// Start a builder for a batch instrument request.
    pub fn batch<I, S>(&self, symbols: I) -> BatchInstrumentsBuilder<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        BatchInstrumentsBuilder {
            client: self,
            symbols: symbols.into_iter().map(Into::into).collect(),
        }
    }

    async fn execute_single(&self, symbol: String) -> Result<Instrument> {
        let resp: InstrumentsResponse = self
            .http
            .get(
                "/v1/instruments",
                &GetInstrumentsRequest { symbols: symbol },
            )
            .await?;
        resp.data.into_iter().next().ok_or_else(|| {
            Error::Parse(
                "instruments response contained no data for the requested symbol".to_string(),
            )
        })
    }

    async fn execute_batch(&self, symbols: Vec<String>) -> Result<Vec<Instrument>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(symbols.len());
        for chunk in symbols.chunks(BATCH_CHUNK_SIZE) {
            let req = BatchInstrumentsRequest {
                symbols: chunk.to_vec(),
            };
            let resp: InstrumentsResponse = self.http.post("/v1/instruments", &req).await?;
            out.extend(resp.data);
        }
        Ok(out)
    }
}

#[derive(Serialize)]
struct GetInstrumentsRequest {
    symbols: String,
}

#[derive(Serialize)]
struct BatchInstrumentsRequest {
    symbols: Vec<String>,
}

/// Builder for a single-symbol instrument request.
pub struct SingleInstrumentsBuilder<'a> {
    client: &'a Instruments,
    symbol: String,
}

impl SingleInstrumentsBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<Instrument> {
        self.client.execute_single(self.symbol).await
    }
}

/// Builder for a batch instrument request.
pub struct BatchInstrumentsBuilder<'a> {
    client: &'a Instruments,
    symbols: Vec<String>,
}

impl BatchInstrumentsBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<Vec<Instrument>> {
        self.client.execute_batch(self.symbols).await
    }
}
