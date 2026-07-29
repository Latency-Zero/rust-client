use std::time::Duration;

/// Errors produced by the LatZero client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("DSN must look like latzero://client-id")]
    InvalidDsn,

    #[error("could not connect to latzero server at {endpoint}: {source}")]
    Connection {
        endpoint: String,
        #[source]
        source: std::io::Error,
    },

    #[error("client is disconnected")]
    Disconnected,

    #[error("timed out after {timeout:?} waiting for request {request_id}")]
    Timeout {
        request_id: String,
        timeout: Duration,
    },

    #[error("pool authentication failed: {0}")]
    Authentication(String),

    #[error("server error {code}: {message}")]
    Server { code: String, message: String },

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("JSON serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("I/O failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("event handler failed: {0}")]
    Handler(String),
}

pub type Result<T> = std::result::Result<T, Error>;
