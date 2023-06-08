use std::str::FromStr;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use crate::error::Error;

pub struct ClickhouseClient {
    url: String,
    client: reqwest::Client
}

impl ClickhouseClient {
    pub fn from_env() -> Result<Self, Error> {
        let username = std::env::var("CLICKHOUSE_USERNAME")?;
        let password = std::env::var("CLICKHOUSE_PASSWORD")?;
        let database = std::env::var("CLICKHOUSE_DATABASE")?;
        let url = std::env::var("CLICKHOUSE_URL")?;
        let url = format!("{url}/?database={database}&enable_http_compression=1");
        let client = reqwest::Client::builder()
            .default_headers(HeaderMap::from_iter([
                (HeaderName::from_str("X-ClickHouse-User")?, HeaderValue::from_str(&username)?),
                (HeaderName::from_str("X-ClickHouse-Key")?, HeaderValue::from_str(&password)?)
        ]))
            .gzip(true)
            .build()?;
        Ok(Self {
            client,
            url
        })
    }
    pub async fn fetch_one<R: DeserializeOwned>(&self, query: &str) -> Result<Option<R>, Error> {
        let response = self.fetch(query).await?;
        Ok(response.into_iter().next())
    }
    pub async fn fetch_many<R: DeserializeOwned>(&self, query: &str) -> Result<Vec<R>, Error> {
        let response = self.fetch(query).await?;
        Ok(response)
    }
    async fn fetch<R: DeserializeOwned>(&self, query: &str) -> Result<Vec<R>, Error> {
        let Self {client, url, .. } = self;
        let query = format!("{query} format JSON");
        let response = client.post(url).body(query).send().await?;
        let response = response.bytes().await?;
        let Response {data}  = serde_json::from_slice(&response)?;
        Ok(data)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
pub struct Response<T> {
    pub data: Vec<T>,
}
