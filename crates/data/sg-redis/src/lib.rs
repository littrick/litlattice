#![forbid(unsafe_code)]

//! `internal/redispool`: the Redis connection pool and the `KeyValue`
//! convenience API that most of the Go codebase uses.
//!
//! The Go package exposes two pools: `redispool.Cache` (recomputable data) and
//! `redispool.Store` (persistent data). Both are plain `KeyValue`s over the
//! same command surface, distinguished only by the endpoint env vars and pool
//! sizing, so this crate exposes one [`KeyValue`] type plus
//! [`cache_endpoint`] / [`store_endpoint`].
//!
//! The command set here is the subset the ported callers need (`Get`/`Set`/
//! `SetEx`, hashes, FIFO lists, expiry and counters). `WithContext` is
//! unnecessary in async Rust (every call already takes the ambient future's
//! cancellation) and `WithLatencyRecorder`/`Pool` are not ported.

use std::collections::HashMap;

use redis::aio::ConnectionManager;
use redis::{Cmd, FromRedisValue, RedisError};

pub const CRATE_NAME: &str = "sg-redis";

/// Fallback endpoint when neither `REDIS_<KIND>_ENDPOINT` nor
/// `REDIS_ENDPOINT` is set (`redispool.addresses`).
pub const DEFAULT_CACHE_ENDPOINT: &str = "redis-cache:6379";
pub const DEFAULT_STORE_ENDPOINT: &str = "redis-store:6379";

/// The pool a caller wants (`redispool.Cache` vs `redispool.Store`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pool {
    Cache,
    Store,
}

/// Resolve a pool endpoint exactly like `redispool.addresses`:
/// `REDIS_<KIND>_ENDPOINT`, else `REDIS_ENDPOINT`, else the Kubernetes
/// service name.
pub fn endpoint_from_env(pool: Pool) -> String {
    let (specific, default) = match pool {
        Pool::Cache => ("REDIS_CACHE_ENDPOINT", DEFAULT_CACHE_ENDPOINT),
        Pool::Store => ("REDIS_STORE_ENDPOINT", DEFAULT_STORE_ENDPOINT),
    };
    if let Ok(value) = std::env::var(specific) {
        return value;
    }
    if let Ok(value) = std::env::var("REDIS_ENDPOINT") {
        if !value.is_empty() {
            return value;
        }
    }
    default.to_owned()
}

/// `redispool.Cache`'s address.
pub fn cache_endpoint() -> String {
    endpoint_from_env(Pool::Cache)
}

/// `redispool.Store`'s address.
pub fn store_endpoint() -> String {
    endpoint_from_env(Pool::Store)
}

/// A cloneable handle to a Redis connection pool.
///
/// `ConnectionManager` reconnects on demand, which is the equivalent of
/// redigo's pool with `TestOnBorrow`.
#[derive(Clone)]
pub struct KeyValue {
    manager: ConnectionManager,
    prefix: String,
}

/// `redispool.Value.Bytes()` made explicit: `None` is redigo's `redis.ErrNil`.
pub type Value = Option<Vec<u8>>;

impl KeyValue {
    /// Dial `endpoint`. The Go code accepts both `host:port` and
    /// `redis://`/`rediss://` URLs (`dialRedis`); the scheme-less form is
    /// turned into a `redis://` URL here.
    pub async fn connect(endpoint: &str) -> Result<Self, RedisError> {
        let client = redis::Client::open(normalize_endpoint(endpoint))?;
        let manager = client.get_connection_manager().await?;
        Ok(Self {
            manager,
            prefix: String::new(),
        })
    }

    /// `KeyValue.WithPrefix`: every key is sent as `<parent prefix><prefix>:`.
    pub fn with_prefix(&self, prefix: &str) -> Self {
        Self {
            manager: self.manager.clone(),
            prefix: join_prefix(&self.prefix, prefix),
        }
    }

    /// The fully-prefixed key, for tests and the FIFO list.
    pub fn key(&self, key: &str) -> String {
        join_key(&self.prefix, key)
    }

    async fn run<T: FromRedisValue>(&self, cmd: &mut Cmd) -> Result<T, RedisError> {
        cmd.query_async(&mut self.manager.clone()).await
    }

    pub async fn ping(&self) -> Result<(), RedisError> {
        let _: String = self.run(&mut redis::cmd("PING")).await?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Value, RedisError> {
        self.run(redis::cmd("GET").arg(self.key(key))).await
    }

    /// `Get` decoded as a UTF-8 string (redigo's `Value.String()`).
    pub async fn get_string(&self, key: &str) -> Result<Option<String>, RedisError> {
        let Some(bytes) = self.get(key).await? else {
            return Ok(None);
        };
        Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
    }

    pub async fn set(&self, key: &str, value: impl AsRef<[u8]>) -> Result<(), RedisError> {
        let _: () = self
            .run(redis::cmd("SET").arg(self.key(key)).arg(value.as_ref()))
            .await?;
        Ok(())
    }

    /// Go's `Set` treats a boolean the way redigo's `Args.Add` does (`"1"` /
    /// `"0"`), which licensing relies on.
    pub async fn set_bool(&self, key: &str, value: bool) -> Result<(), RedisError> {
        self.set(key, if value { "1" } else { "0" }).await
    }

    pub async fn set_ex(
        &self,
        key: &str,
        ttl_seconds: i64,
        value: impl AsRef<[u8]>,
    ) -> Result<(), RedisError> {
        let _: () = self
            .run(
                redis::cmd("SETEX")
                    .arg(self.key(key))
                    .arg(ttl_seconds)
                    .arg(value.as_ref()),
            )
            .await?;
        Ok(())
    }

    pub async fn del(&self, key: &str) -> Result<(), RedisError> {
        let _: i64 = self.run(redis::cmd("DEL").arg(self.key(key))).await?;
        Ok(())
    }

    pub async fn expire(&self, key: &str, ttl_seconds: i64) -> Result<(), RedisError> {
        let _: i64 = self
            .run(redis::cmd("EXPIRE").arg(self.key(key)).arg(ttl_seconds))
            .await?;
        Ok(())
    }

    pub async fn ttl(&self, key: &str) -> Result<i64, RedisError> {
        self.run(redis::cmd("TTL").arg(self.key(key))).await
    }

    pub async fn incr(&self, key: &str) -> Result<i64, RedisError> {
        self.run(redis::cmd("INCR").arg(self.key(key))).await
    }

    pub async fn incr_by(&self, key: &str, value: i64) -> Result<i64, RedisError> {
        self.run(redis::cmd("INCRBY").arg(self.key(key)).arg(value))
            .await
    }

    pub async fn decr_by(&self, key: &str, value: i64) -> Result<i64, RedisError> {
        self.run(redis::cmd("DECRBY").arg(self.key(key)).arg(value))
            .await
    }

    pub async fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RedisError> {
        self.run(redis::cmd("HGET").arg(self.key(key)).arg(field))
            .await
    }

    /// `HGET` with raw field bytes: license signatures are binary strings used
    /// as hash fields (`licensing.maxusercount`). `None` is redigo's
    /// `redis.ErrNil`.
    pub async fn hget_bytes(
        &self,
        key: &str,
        field: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, RedisError> {
        self.run(redis::cmd("HGET").arg(self.key(key)).arg(field.as_ref()))
            .await
    }

    pub async fn hset(
        &self,
        key: &str,
        field: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), RedisError> {
        let _: i64 = self
            .run(
                redis::cmd("HSET")
                    .arg(self.key(key))
                    .arg(field.as_ref())
                    .arg(value.as_ref()),
            )
            .await?;
        Ok(())
    }

    pub async fn hdel(&self, key: &str, field: impl AsRef<[u8]>) -> Result<i64, RedisError> {
        self.run(redis::cmd("HDEL").arg(self.key(key)).arg(field.as_ref()))
            .await
    }

    pub async fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, RedisError> {
        self.run(redis::cmd("HGETALL").arg(self.key(key))).await
    }

    pub async fn lpush(&self, key: &str, value: impl AsRef<[u8]>) -> Result<(), RedisError> {
        let _: i64 = self
            .run(redis::cmd("LPUSH").arg(self.key(key)).arg(value.as_ref()))
            .await?;
        Ok(())
    }

    pub async fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<(), RedisError> {
        let _: () = self
            .run(redis::cmd("LTRIM").arg(self.key(key)).arg(start).arg(stop))
            .await?;
        Ok(())
    }

    pub async fn llen(&self, key: &str) -> Result<i64, RedisError> {
        self.run(redis::cmd("LLEN").arg(self.key(key))).await
    }

    pub async fn lrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, RedisError> {
        self.run(redis::cmd("LRANGE").arg(self.key(key)).arg(start).arg(stop))
            .await
    }

    /// `KEYS` takes a glob pattern and is O(keyspace); only debugging and
    /// tests use it (docs on `redispool.KeyValue.Keys`).
    pub async fn keys(&self, pattern: &str) -> Result<Vec<String>, RedisError> {
        self.run(redis::cmd("KEYS").arg(self.key(pattern))).await
    }

    /// `MGET` over fully-qualified keys (the ones `KEYS` returns, which already
    /// include this handle's prefix). `None` is redigo's `redis.ErrNil` for a
    /// key that disappeared between `KEYS` and `MGET`.
    pub async fn mget(&self, keys: &[String]) -> Result<Vec<Value>, RedisError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut cmd = redis::cmd("MGET");
        for key in keys {
            cmd.arg(key);
        }
        self.run(&mut cmd).await
    }
}

/// `redisKeyValue.prefix + prefix + ":"` (`WithPrefix`).
fn join_prefix(parent: &str, prefix: &str) -> String {
    format!("{parent}{prefix}:")
}

/// `redisKeyValue.prefix + key` (`prefixedKeys`).
fn join_key(prefix: &str, key: &str) -> String {
    format!("{prefix}{key}")
}

/// `dialRedis`: accept `redis://`/`rediss://` URLs, otherwise treat the value
/// as `host:port` (and reject values containing `/`, as the baseline does).
fn normalize_endpoint(endpoint: &str) -> String {
    let has_scheme = endpoint.find("://").is_some_and(|idx| {
        idx > 0
            && endpoint[..idx]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic())
    });
    if has_scheme {
        endpoint.to_owned()
    } else {
        format!("redis://{endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_follow_the_redispool_precedence() {
        // The environment is process-wide; use a lock so the assertions do not
        // race with other tests in this binary.
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().expect("lock environment");

        std::env::remove_var("REDIS_CACHE_ENDPOINT");
        std::env::remove_var("REDIS_STORE_ENDPOINT");
        std::env::remove_var("REDIS_ENDPOINT");
        assert_eq!(cache_endpoint(), DEFAULT_CACHE_ENDPOINT);
        assert_eq!(store_endpoint(), DEFAULT_STORE_ENDPOINT);

        std::env::set_var("REDIS_ENDPOINT", "127.0.0.1:6379");
        assert_eq!(cache_endpoint(), "127.0.0.1:6379");
        assert_eq!(store_endpoint(), "127.0.0.1:6379");

        std::env::set_var("REDIS_CACHE_ENDPOINT", "127.0.0.1:6479");
        std::env::set_var("REDIS_STORE_ENDPOINT", "127.0.0.1:6579");
        assert_eq!(cache_endpoint(), "127.0.0.1:6479");
        assert_eq!(store_endpoint(), "127.0.0.1:6579");

        std::env::remove_var("REDIS_CACHE_ENDPOINT");
        std::env::remove_var("REDIS_STORE_ENDPOINT");
        std::env::remove_var("REDIS_ENDPOINT");
    }

    #[test]
    fn prefixes_match_with_prefix() {
        assert_eq!(join_key("", "k"), "k");
        assert_eq!(join_key(&join_prefix("", "v2"), "k"), "v2:k");
        assert_eq!(
            join_key(&join_prefix(&join_prefix("", "v2"), "x"), "k"),
            "v2:x:k"
        );
    }

    #[test]
    fn normalize_endpoint_accepts_urls_and_hostports() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:6379"),
            "redis://127.0.0.1:6379"
        );
        assert_eq!(
            normalize_endpoint("redis://user:pw@host:6379/1"),
            "redis://user:pw@host:6379/1"
        );
        assert_eq!(
            normalize_endpoint("rediss://host:6379"),
            "rediss://host:6379"
        );
    }
}
