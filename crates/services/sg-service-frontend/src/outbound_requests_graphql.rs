#![forbid(unsafe_code)]

//! `Query.outboundRequests` seam (`cmd/frontend/graphqlbackend/outbound_requests.go:50`).
//!
//! The baseline reads the frontend's outbound HTTP request log from Redis
//! (`database.OutboundRequestLogStore`), which the Rust frontend does not
//! record yet. The resolver therefore stays unported: M9.16c can inject an
//! [`OutboundRequestLog`] when the HTTP client logging middleware lands.
//! Returning `None` from [`prefetch_outbound_requests`] keeps the pre-slice
//! behaviour (no prefetched value).

use sg_graphql::exec::RootField;

/// One outbound request log entry (`types.OutboundRequestLogItem`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundRequestLogItem {
    pub id: String,
    pub created_at: time::OffsetDateTime,
    pub url: String,
    pub method: String,
    pub status_code: i32,
    pub duration_ms: i64,
    pub request_headers: Vec<(String, String)>,
    pub response_headers: Vec<(String, String)>,
    pub error: Option<String>,
}

/// `db.OutboundRequestLogStore.List`.
pub trait OutboundRequestLog: Send + Sync {
    fn list(&self, after: Option<&str>, limit: i64) -> Result<Vec<OutboundRequestLogItem>, String>;
}

/// Always unported in this slice; the seam is left for M9.16c.
pub fn prefetch_outbound_requests(
    _log: Option<&dyn OutboundRequestLog>,
    _fields: &[RootField],
) -> Option<Result<Vec<OutboundRequestLogItem>, String>> {
    None
}
