use reqwest::{
    Method, RequestBuilder,
    header::{HeaderMap, HeaderValue},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{sync::Arc, time::Duration};

use crate::{
    client::Config,
    error::{ConfigError, Error, Result},
};

/// Header name the tickflow API expects for authentication.
const API_KEY_HEADER: &str = "x-api-key";

pub struct HttpClient {
    inner: reqwest::Client,
    config: Arc<Config>,
}

impl HttpClient {
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        if let Some(key) = &config.api_key {
            let value = HeaderValue::from_str(key)
                .map_err(|e| Error::Config(ConfigError::new(e.to_string())))?;
            headers.insert(API_KEY_HEADER, value);
        }

        let inner = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(config.timeout)
            .connect_timeout(Duration::from_secs(10))
            .gzip(true)
            .deflate(true)
            .build()
            .map_err(|e| Error::Config(ConfigError::new(e.to_string())))?;

        Ok(Self { inner, config })
    }

    fn request(&self, method: Method, path: &str) -> Result<RequestBuilder> {
        let url = self
            .config
            .base_url
            .join(path)
            .map_err(|e| Error::Config(ConfigError::new(e.to_string())))?;

        Ok(self.inner.request(method, url))
    }

    async fn execute_json<T: DeserializeOwned>(&self, req: RequestBuilder) -> Result<T> {
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(Error::Api(format!("status: {status} {body}")));
        }
        serde_json::from_str(&body).map_err(|e| Error::Parse(format!("{e}; body={}", &body)))
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let req = self.request(Method::GET, path)?;
        self.execute_json(req).await
    }

    pub async fn post<B, T>(&self, path: &str, body: &B) -> Result<T>
    where
        B: Serialize,
        T: DeserializeOwned,
    {
        let req = self.request(Method::POST, path)?.json(body);
        self.execute_json(req).await
    }
}
