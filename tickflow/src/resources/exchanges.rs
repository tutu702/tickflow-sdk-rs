use serde::Serialize;
use std::sync::Arc;

use crate::{
    error::Result,
    http::HttpClient,
    model::{
        Instrument, InstrumentsResponse,
        exchange::{ExchangeData, ExchangeListResponse},
        instrument::InstrumentType,
    },
};

pub struct Exchanges {
    http: Arc<HttpClient>,
}

impl Exchanges {
    pub(crate) fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    /// List every exchange available on the tickflow API.
    pub async fn list(&self) -> Result<Vec<ExchangeData>> {
        let out: ExchangeListResponse = self.http.get("/v1/exchanges", &()).await?;
        Ok(out.data)
    }

    /// List instruments traded on the given exchange, optionally filtered by type.
    pub async fn get_instruments(
        &self,
        exchange: &str,
        instrument_type: Option<InstrumentType>,
    ) -> Result<Vec<Instrument>> {
        let path = format!("/v1/exchanges/{}/instruments", exchange);
        let out: InstrumentsResponse = self
            .http
            .get(
                &path,
                &GetInstrumentsParams {
                    r#type: instrument_type,
                },
            )
            .await?;
        Ok(out.data)
    }
}

#[derive(Serialize)]
struct GetInstrumentsParams {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    r#type: Option<InstrumentType>,
}
