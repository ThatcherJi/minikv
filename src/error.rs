use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("no healthy volumes")]
    NoHealthyVolumes,
    #[error("insufficient replicas: needed {needed}, got {got}")]
    InsufficientReplicas { needed: usize, got: usize },
    #[error("volume not found: {0}")]
    VolumeNotFound(String),
    #[error("invalid address: {0}")]
    InvalidAddr(String),
}

pub type Result<T> = std::result::Result<T, Error>;
