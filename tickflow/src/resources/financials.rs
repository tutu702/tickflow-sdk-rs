use std::{collections::HashMap, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    error::{Error, Result},
    http::{HttpClient, merge_maps},
    model::{
        BalanceSheetRecord, CashFlowRecord, MetricsRecord, SharesRecord, financial::IncomeRecord,
    },
    resources::{BATCH_CHUNK_SIZE, BATCH_CONCURRENCY},
};

/// Financial statement type — covers all five Python endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Statement {
    /// Income statement (`/v1/financials/income`).
    Income,
    /// Balance sheet (`/v1/financials/balance-sheet`).
    BalanceSheet,
    /// Cash-flow statement (`/v1/financials/cash-flow`).
    CashFlow,
    /// Core financial metrics (ROE, EPS, …) — `/v1/financials/metrics`.
    Metrics,
    /// Share count over time — `/v1/financials/shares`.
    Shares,
}

impl Statement {
    /// HTTP path for this statement type.
    fn endpoint(self) -> &'static str {
        match self {
            Self::Income => "/v1/financials/income",
            Self::BalanceSheet => "/v1/financials/balance-sheet",
            Self::CashFlow => "/v1/financials/cash-flow",
            Self::Metrics => "/v1/financials/metrics",
            Self::Shares => "/v1/financials/shares",
        }
    }
}

/// Optional filters shared by every financial-statement endpoint.
#[derive(Debug, Default, Clone, Serialize)]
pub struct StatementParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest: Option<bool>,
}

impl StatementParams {
    /// Start building a fresh parameter set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn start_date(mut self, date: impl Into<String>) -> Self {
        self.start_date = Some(date.into());
        self
    }

    #[must_use]
    pub fn end_date(mut self, date: impl Into<String>) -> Self {
        self.end_date = Some(date.into());
        self
    }

    #[must_use]
    pub fn latest(mut self, latest: bool) -> Self {
        self.latest = Some(latest);
        self
    }

    fn into_query(self, symbols: String) -> QueryParams {
        QueryParams {
            symbols,
            start_date: self.start_date,
            end_date: self.end_date,
            latest: self.latest,
        }
    }
}

#[derive(Serialize)]
struct QueryParams {
    symbols: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latest: Option<bool>,
}

pub struct Financials {
    http: Arc<HttpClient>,
}

impl Financials {
    pub(crate) fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    async fn query<I, S, T>(
        &self,
        stmt: Statement,
        symbols: I,
        params: Option<StatementParams>,
    ) -> Result<HashMap<String, T>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        T: DeserializeOwned + Send + 'static,
    {
        let symbols: Vec<String> = symbols.into_iter().map(Into::into).collect();
        if symbols.is_empty() {
            return Ok(HashMap::new());
        }

        let chunks: Vec<String> = symbols
            .chunks(BATCH_CHUNK_SIZE)
            .map(|c| c.join(","))
            .collect();
        let http = Arc::clone(&self.http);

        let result: Vec<HashMap<String, T>> = stream::iter(chunks)
            .map(move |chunk| {
                let http = Arc::clone(&http);
                let params = params.clone();
                async move {
                    let out: DataResponse<T> = http
                        .get(
                            stmt.endpoint(),
                            &params.unwrap_or_default().into_query(chunk),
                        )
                        .await?;
                    Ok::<_, Error>(out.data)
                }
            })
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect()
            .await?;

        Ok(merge_maps(result))
    }

    #[must_use]
    pub async fn income<I, S>(
        &self,
        symbols: I,
        params: Option<StatementParams>,
    ) -> Result<HashMap<String, Vec<IncomeRecord>>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.query(Statement::Income, symbols, params).await
    }

    #[must_use]
    pub async fn balance_sheet<I, S>(
        &self,
        symbols: I,
        params: Option<StatementParams>,
    ) -> Result<HashMap<String, Vec<BalanceSheetRecord>>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.query(Statement::BalanceSheet, symbols, params).await
    }

    #[must_use]
    pub async fn cash_flow<I, S>(
        &self,
        symbols: I,
        params: Option<StatementParams>,
    ) -> Result<HashMap<String, Vec<CashFlowRecord>>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.query(Statement::CashFlow, symbols, params).await
    }

    #[must_use]
    pub async fn metrics<I, S>(
        &self,
        symbols: I,
        params: Option<StatementParams>,
    ) -> Result<HashMap<String, Vec<MetricsRecord>>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.query(Statement::Metrics, symbols, params).await
    }

    #[must_use]
    pub async fn shares<I, S>(
        &self,
        symbols: I,
        params: Option<StatementParams>,
    ) -> Result<HashMap<String, Vec<SharesRecord>>>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.query(Statement::Shares, symbols, params).await
    }
}

#[derive(Deserialize)]
struct DataResponse<T> {
    data: HashMap<String, T>,
}
