#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Http (#[from] reqwest::Error),
    #[error(transparent)]
    Header(#[from] reqwest::header::InvalidHeaderValue),
    #[error(transparent)]
    HeaderName(#[from] reqwest::header::InvalidHeaderName),
    #[error(transparent)]
    Env (#[from] std::env::VarError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}