use serde::Serialize;
use std::sync::Arc;

use crate::{
    error::{Error, Result},
    http::HttpClient,
    model::{AdjustType, KlineData, Period, Symbol, kline::KlinesResponse},
};

#[derive(Serialize)]
struct GetKlinesRequest {
    symbol: String,
    period: Period,
    count: u32,
    adjust: AdjustType,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_time: Option<i64>,
}

pub struct Klines {
    pub http: Arc<HttpClient>,
}

impl Klines {
    pub fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    pub fn get(&self, symbol: &Symbol) -> GetKlinesBuilder<'_> {
        GetKlinesBuilder {
            client: self,
            params: GetKlinesRequest {
                symbol: symbol.as_str().to_owned(),
                period: Period::D1,
                count: 100,
                adjust: AdjustType::default(),
                start_time: None,
                end_time: None,
            },
        }
    }

    async fn execute(&self, request: &GetKlinesRequest) -> Result<KlineData> {
        let response: KlinesResponse = self.http.get("/v1/klines", request).await?;

        response
            .data
            .validate()
            .map_err(|message| Error::Parse(message.to_owned()))?;

        Ok(response.data)
    }
}

pub struct GetKlinesBuilder<'a> {
    client: &'a Klines,
    params: GetKlinesRequest,
}

impl<'a> GetKlinesBuilder<'a> {
    pub fn period(mut self, p: Period) -> Self {
        self.params.period = p;
        self
    }

    pub fn count(mut self, n: u32) -> Self {
        self.params.count = n;
        self
    }

    pub fn adjust(mut self, a: AdjustType) -> Self {
        self.params.adjust = a;
        self
    }

    pub fn start_time(mut self, t: i64) -> Self {
        self.params.start_time = Some(t);
        self
    }

    pub fn end_time(mut self, t: i64) -> Self {
        self.params.end_time = Some(t);
        self
    }

    pub async fn send(self) -> Result<KlineData> {
        self.client.execute(&self.params).await
    }
}
