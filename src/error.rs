use thiserror::Error;

/// Core error types for all PGit operations.
#[derive(Error, Debug)]
#[allow(dead_code)] // Some variants reserved for future use
pub enum PgitError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Protobuf decode error: {0}")]
    Protobuf(#[from] prost::DecodeError),

    #[error("Protobuf encode error: {0}")]
    ProtobufEncode(#[from] prost::EncodeError),

    #[error("Statistical error: {0}")]
    Statistical(String),

    #[error("Drift detected: {0}")]
    DriftDetected(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Remote storage error: {0}")]
    Remote(String),

    #[error("AWS error: {0}")]
    Aws(String),

    #[error("No baseline found for dataset '{0}'. Run 'pgit commit' first.")]
    NoBaseline(String),

    #[error(".pgit database not found. Run 'pgit init' first.")]
    DatabaseNotFound,

    #[error("Unsupported file format: '{0}'. Use .csv or .parquet.")]
    UnsupportedFormat(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

/// Convenience type alias — use throughout all modules instead of `Box<dyn Error>`.
pub type PgitResult<T> = std::result::Result<T, PgitError>;

impl From<aws_sdk_s3::Error> for PgitError {
    fn from(err: aws_sdk_s3::Error) -> Self {
        PgitError::Remote(err.to_string())
    }
}

// Note: The aws-config 0.55 version used here does not expose RegionProviderChainError
// publicly, so we handle AWS region errors via the Remote variant instead.
