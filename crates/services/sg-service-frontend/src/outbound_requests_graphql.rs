#![forbid(unsafe_code)]

//! `Query.outboundRequests` (`cmd/frontend/graphqlbackend/outbound_requests.go:50`)
//! and its Redis-backed log source.
//!
//! The Go frontend reads the outbound HTTP request log with
//! `httpcli.GetOutboundRequestLogItems`, which decodes the FIFO list at
//! `v2:outbound-requests` written by `redisLoggerMiddleware`. The middleware
//! port lives in `sg-httpcli`; this module adapts it to the GraphQL item shape
//! (`graphql_inventory_b::OutboundRequestItem`) and reads the site-config limit
//! (`siteConfig.outboundRequestLogLimit`, `0` disables logging) at call time,
//! exactly like the baseline's `outboundRequestLogLimit()`.

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_redis::KeyValue;
use tokio::sync::Mutex;

use crate::graphql_inventory_b::OutboundRequestItem;

/// The reader seam `Query.outboundRequests` uses. `list` receives the
/// already-unmarshalled relay `after` cursor and must apply the Go
/// `GetOutboundRequestLogItems` semantics (items with `ID > after`,
/// ascending, truncated to the configured limit).
#[async_trait]
pub trait OutboundRequestLogSource: Send + Sync {
    async fn list(&self, after: &str) -> Result<Vec<OutboundRequestItem>, String>;
}

/// Production source: the frontend's Redis cache pool (the same
/// `redispool.Cache` the Go middleware writes through).
pub struct RedisOutboundRequestLogStore {
    endpoint: String,
    live: Arc<LiveConfig>,
    cached: Mutex<Option<KeyValue>>,
}

impl RedisOutboundRequestLogStore {
    /// `redispool.Cache`'s endpoint (`REDIS_CACHE_ENDPOINT`, else
    /// `REDIS_ENDPOINT`, else `redis-cache:6379`).
    pub fn new(endpoint: String, live: Arc<LiveConfig>) -> Self {
        Self {
            endpoint,
            live,
            cached: Mutex::new(None),
        }
    }

    pub fn from_env(live: Arc<LiveConfig>) -> Self {
        Self::new(sg_redis::cache_endpoint(), live)
    }

    async fn kv(&self) -> Result<KeyValue, String> {
        if let Some(kv) = self.cached.lock().await.clone() {
            return Ok(kv);
        }
        let kv = KeyValue::connect(&self.endpoint)
            .await
            .map_err(|error| error.to_string())?;
        *self.cached.lock().await = Some(kv.clone());
        Ok(kv)
    }

    /// `outboundRequestLogLimit()`: `siteConfig.OutboundRequestLogLimit`
    /// (`0` disables the log).
    fn limit(&self) -> i32 {
        self.live
            .current()
            .site
            .raw()
            .get("outboundRequestLogLimit")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32
    }
}

#[async_trait]
impl OutboundRequestLogSource for RedisOutboundRequestLogStore {
    async fn list(&self, after: &str) -> Result<Vec<OutboundRequestItem>, String> {
        let limit = self.limit();
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let kv = self.kv().await?;
        let fifo = sg_httpcli::outbound_requests_fifo(kv, limit);
        let items = sg_httpcli::get_outbound_request_log_items(&fifo, after, limit).await?;
        Ok(items.iter().map(convert_item).collect())
    }
}

/// `types.OutboundRequestLogItem` -> the GraphQL item.
///
/// `DurationMs` is `int32(math.Round(duration * 1000))`
/// (`outbound_requests.go:178`).
fn convert_item(item: &sg_httpcli::OutboundRequestLogItem) -> OutboundRequestItem {
    OutboundRequestItem {
        id: item.id.clone(),
        started_at: item.started_at,
        method: item.method.clone(),
        url: item.url.clone(),
        status_code: item.status_code,
        duration_ms: (item.duration * 1000.0).round() as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(duration: f64) -> sg_httpcli::OutboundRequestLogItem {
        sg_httpcli::OutboundRequestLogItem {
            id: "2021-01-01T00_00_00".to_owned(),
            started_at: time::OffsetDateTime::from_unix_timestamp(1_600_000_000)
                .expect("timestamp"),
            method: "GET".to_owned(),
            url: "http://example.com".to_owned(),
            request_headers: Default::default(),
            request_body: String::new(),
            status_code: 200,
            response_headers: None,
            duration,
            error_message: String::new(),
            creation_stack_frame: String::new(),
            call_stack_frame: String::new(),
        }
    }

    #[test]
    fn duration_ms_rounds_like_go() {
        assert_eq!(convert_item(&item(0.0015)).duration_ms, 2);
        assert_eq!(convert_item(&item(0.0014)).duration_ms, 1);
        assert_eq!(convert_item(&item(0.0)).duration_ms, 0);
    }
}
