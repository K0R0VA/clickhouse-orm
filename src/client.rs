use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::StatusCode;
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
    pub async fn fetch_one<R: DeserializeOwned + Debug>(&self, query: &str) -> Result<Option<R>, Error> {
        let response = self.fetch(query).await?;
        Ok(response.into_iter().next())
    }
    pub async fn fetch_many<R: DeserializeOwned + Debug>(&self, query: &str) -> Result<Vec<R>, Error> {
        let response = self.fetch(query).await?;
        Ok(response)
    }
    async fn fetch<R: DeserializeOwned + Debug>(&self, query: &str) -> Result<Vec<R>, Error> {
        let Self {client, url, .. } = self;
        let query = format!("{query} format JSON");
        let response = client.post(url).body(query.clone()).send().await?;
        let status = response.status();
        if !status.is_success() {
            let message = response.text().await?;
            return Err(Error::Database(DatabaseError {status, message, failed_query: query}))
        }
        let response = response.bytes().await?;
        let Response {data}  = match serde_json::from_slice(&response) {
            Ok(response) => response,
            Err(error) => {
                let body = String::from_utf8_lossy(&response).to_string();
                let error: DeserializeError<R> = DeserializeError {
                    failed_kind: PhantomData::default(),
                    failed_query: query,
                    error,
                    body
                };
                let error = format!("{:?}", error);
                return Err(Error::DeserializeError(error))
            }
        };
        Ok(data)
    }
}

#[derive(Debug)]
pub struct DeserializeError<T> {
    failed_kind: PhantomData<T>,
    failed_query: String,
    error: serde_json::Error,
    body: String
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
pub struct Response<T> {
    pub data: Vec<T>,
}

#[derive(thiserror::Error, Debug)]
#[error("Database Error")]
pub struct DatabaseError {
    message: String,
    failed_query: String,
    status: StatusCode,
}
