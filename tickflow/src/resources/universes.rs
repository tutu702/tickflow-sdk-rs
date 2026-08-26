use serde::Serialize;
use std::sync::Arc;

use crate::{
    error::Result,
    http::HttpClient,
    model::{BatchUniversesResponse, Universe, UniverseResponse},
    resources::BATCH_CHUNK_SIZE,
};

pub struct Universes {
    http: Arc<HttpClient>,
}

impl Universes {
    pub(crate) fn new(http: Arc<HttpClient>) -> Self {
        Self { http }
    }

    /// List every universe available on the tickflow API.
    pub async fn list(&self) -> Result<BatchUniversesResponse> {
        self.http.get("/v1/universes", &()).await
    }

    /// Start a builder for a single-universe request.
    pub fn get(&self, id: impl Into<String>) -> SingleUniversesBuilder<'_> {
        SingleUniversesBuilder {
            client: self,
            id: id.into(),
        }
    }

    /// Start a builder for a batch universe request.
    pub fn batch<I, S>(&self, ids: I) -> BatchUniversesBuilder<'_>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        BatchUniversesBuilder {
            client: self,
            ids: ids.into_iter().map(Into::into).collect(),
        }
    }

    async fn execute_single(&self, id: String) -> Result<Universe> {
        let path = format!("/v1/universes/{}", id);
        let resp: UniverseResponse = self.http.get(&path, &()).await?;
        Ok(resp.data)
    }

    async fn execute_batch(&self, ids: Vec<String>) -> Result<Vec<Universe>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(BATCH_CHUNK_SIZE) {
            let req = BatchUniversesRequest {
                ids: chunk.to_vec(),
            };
            let resp: BatchUniversesResponse = self.http.post("/v1/universes/batch", &req).await?;
            out.extend(resp.data);
        }
        Ok(out)
    }
}

/// Builder for a single-universe request.
pub struct SingleUniversesBuilder<'a> {
    client: &'a Universes,
    id: String,
}

impl SingleUniversesBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<Universe> {
        self.client.execute_single(self.id).await
    }
}

/// Builder for a batch universe request.
pub struct BatchUniversesBuilder<'a> {
    client: &'a Universes,
    ids: Vec<String>,
}

impl BatchUniversesBuilder<'_> {
    #[must_use]
    pub async fn send(self) -> Result<Vec<Universe>> {
        self.client.execute_batch(self.ids).await
    }
}

#[derive(Serialize)]
struct BatchUniversesRequest {
    ids: Vec<String>,
}
