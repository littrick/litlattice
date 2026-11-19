#![forbid(unsafe_code)]

//! Errors shared by the migrator modules.

/// A migration-system failure. Messages mirror the Go migrator where callers
/// observe them (invalid definitions, dirty logs, unknown schema, ...).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MigratorError {
    #[error("{0}")]
    Message(String),
    #[error("unknown schema {0:?}")]
    UnknownSchema(String),
    #[error("malformed migration definitions {schema:?}: {detail}")]
    MalformedDefinitions { schema: String, detail: String },
}

impl MigratorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}
