use thiserror::Error;
use protolith_core::error::Error as protolith_error;

/// Errors produced when loading a `Config` struct.
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("internal error: {0}")]
    Internal(protolith_error),
    #[error(transparent)]
    OpError(OpError),
}

#[derive(Debug, Error)]
pub enum OpError {
    #[error("database {0} already exists")]
    DatabaseAlreadyExists(String),
    #[error("database {0} not exists")]
    DatabaseNotFound(String),
    #[error("collection {0} not exists on {1}")]   
    CollectionNotFound(String, String),
    #[error("{0}")]
    KeyAlreadyExists(protolith_error),
    #[error("collection {1} already exists on {0}")]
    CollectionAlreadyExists(String, String),
    #[error("user {0} not found")]
    UserNotFound(String),
}