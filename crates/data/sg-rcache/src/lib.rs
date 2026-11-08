#![forbid(unsafe_code)]

//! `internal/rcache`: a namespaced Redis cache plus the `FIFOList` helper.
//!
//! Every key is stored under `v2:<cache prefix>:<key>` (`dataVersion` +
//! `keyPrefix` in the baseline). `FIFOList` keys are additionally prefixed by
//! the same `dataVersion` (the Go `FIFOList.globalPrefixKey` prepends
//! `globalPrefix` to a key that already contains it), which is why list keys
//! in Redis look like `v2:v2:<cache prefix>:<list key>`.
//!
//! Methods return `Result` instead of the baseline's log-and-continue
//! behaviour; callers that want the baseline semantics ignore the error and
//! log, exactly as the Go methods do internally.

use std::collections::HashMap;

use redis::RedisError;
use sg_redis::KeyValue;

/// `rcache.dataVersion`: bumping this resets every cached value.
pub const DATA_VERSION: &str = "v2";

/// `Cache.rkeyPrefix`: `v2:<cache prefix>:`.
pub fn cache_key_prefix(key_prefix: &str) -> String {
    format!("{}:{}:", DATA_VERSION, key_prefix)
}

/// The full Redis key for `key` in a cache with this prefix.
pub fn cache_redis_key(key_prefix: &str, key: &str) -> String {
    format!("{}{}", cache_key_prefix(key_prefix), key)
}

/// `FIFOList.globalPrefixKey`: the data version precedes an already
/// namespaced list key.
pub fn fifo_global_prefix_key(list_key: &str) -> String {
    format!("{}:{}", DATA_VERSION, list_key)
}

/// `FIFOList.MaxSize`: a negative configured size behaves like zero.
pub fn fifo_max_size(configured: i64) -> i64 {
    configured.max(0)
}

/// A namespaced view over a [`KeyValue`].
#[derive(Clone)]
pub struct Cache {
    kv: KeyValue,
    key_prefix: String,
    ttl_seconds: i64,
}

impl Cache {
    /// `rcache.New`: no TTL on writes.
    pub fn new(kv: KeyValue, key_prefix: &str) -> Self {
        Self {
            kv,
            key_prefix: key_prefix.to_owned(),
            ttl_seconds: 0,
        }
    }

    /// `rcache.NewWithTTL`: `SETEX` on every write.
    pub fn new_with_ttl(kv: KeyValue, key_prefix: &str, ttl_seconds: i64) -> Self {
        Self {
            kv,
            key_prefix: key_prefix.to_owned(),
            ttl_seconds,
        }
    }

    pub fn ttl(&self) -> i64 {
        self.ttl_seconds
    }

    /// `rkeyPrefix`: `v2:<prefix>:`.
    pub fn key_prefix(&self) -> String {
        cache_key_prefix(&self.key_prefix)
    }

    /// The full Redis key for `key` in this cache.
    pub fn redis_key(&self, key: &str) -> String {
        cache_redis_key(&self.key_prefix, key)
    }

    /// `Cache.Get`: `None` when the key does not exist.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, RedisError> {
        self.kv.get(&self.redis_key(key)).await
    }

    /// `Cache.Get` decoded as a string.
    pub async fn get_string(&self, key: &str) -> Result<Option<String>, RedisError> {
        self.kv.get_string(&self.redis_key(key)).await
    }

    /// `Cache.Set`: plain `SET` without a TTL, `SETEX` when the cache was
    /// created `new_with_ttl`.
    pub async fn set(&self, key: &str, value: impl AsRef<[u8]>) -> Result<(), RedisError> {
        if self.ttl_seconds == 0 {
            self.kv.set(&self.redis_key(key), value).await
        } else {
            self.set_with_ttl(key, value, self.ttl_seconds).await
        }
    }

    pub async fn set_with_ttl(
        &self,
        key: &str,
        value: impl AsRef<[u8]>,
        ttl_seconds: i64,
    ) -> Result<(), RedisError> {
        self.kv
            .set_ex(&self.redis_key(key), ttl_seconds, value)
            .await
    }

    /// `Cache.Increase`: `INCR`, then refresh the TTL when one is configured.
    pub async fn increase(&self, key: &str) -> Result<i64, RedisError> {
        let value = self.kv.incr(&self.redis_key(key)).await?;
        if self.ttl_seconds > 0 {
            self.kv
                .expire(&self.redis_key(key), self.ttl_seconds)
                .await?;
        }
        Ok(value)
    }

    /// `Cache.IncrByInt64`/`DecrByInt64`: counter plus TTL refresh.
    pub async fn incr_by(&self, key: &str, value: i64) -> Result<i64, RedisError> {
        let new_value = self.kv.incr_by(&self.redis_key(key), value).await?;
        if self.ttl_seconds > 0 {
            self.kv
                .expire(&self.redis_key(key), self.ttl_seconds)
                .await?;
        }
        Ok(new_value)
    }

    pub async fn decr_by(&self, key: &str, value: i64) -> Result<i64, RedisError> {
        let new_value = self.kv.decr_by(&self.redis_key(key), value).await?;
        if self.ttl_seconds > 0 {
            self.kv
                .expire(&self.redis_key(key), self.ttl_seconds)
                .await?;
        }
        Ok(new_value)
    }

    /// `Cache.KeyTTL`.
    pub async fn key_ttl(&self, key: &str) -> Result<i64, RedisError> {
        self.kv.ttl(&self.redis_key(key)).await
    }

    /// `Cache.ListAllKeys` (`KEYS <prefix>*`).
    pub async fn list_all_keys(&self) -> Result<Vec<String>, RedisError> {
        self.kv.keys(&format!("{}*", self.key_prefix())).await
    }

    /// `Cache.FIFOList`.
    pub fn fifo_list(&self, key: &str, max_size: i64) -> FifoList {
        FifoList::new(self.kv.clone(), self.redis_key(key), max_size)
    }

    /// `Cache.SetHashItem`.
    pub async fn set_hash_item(
        &self,
        key: &str,
        hash_key: &str,
        hash_value: impl AsRef<[u8]>,
    ) -> Result<(), RedisError> {
        self.kv
            .hset(&self.redis_key(key), hash_key, hash_value)
            .await
    }

    /// `Cache.GetHashItem`.
    pub async fn get_hash_item(
        &self,
        key: &str,
        hash_key: &str,
    ) -> Result<Option<String>, RedisError> {
        self.kv.hget(&self.redis_key(key), hash_key).await
    }

    /// `Cache.DeleteHashItem`.
    pub async fn delete_hash_item(&self, key: &str, hash_key: &str) -> Result<i64, RedisError> {
        self.kv.hdel(&self.redis_key(key), hash_key).await
    }

    /// `Cache.GetHashAll`.
    pub async fn get_hash_all(&self, key: &str) -> Result<HashMap<String, String>, RedisError> {
        self.kv.hgetall(&self.redis_key(key)).await
    }

    /// `Cache.Delete`.
    pub async fn delete(&self, key: &str) -> Result<(), RedisError> {
        self.kv.del(&self.redis_key(key)).await
    }
}

/// `rcache.FIFOList`: keeps the most recently inserted items, discarding the
/// oldest when the list grows past `max_size`.
pub struct FifoList {
    kv: KeyValue,
    key: String,
    max_size: i64,
}

impl FifoList {
    /// `rcache.NewFIFOList`: `key` is the already-namespaced list key.
    pub fn new(kv: KeyValue, key: String, max_size: i64) -> Self {
        Self { kv, key, max_size }
    }

    /// `globalPrefixKey`: the list key carries the data version twice.
    pub fn global_prefix_key(&self) -> String {
        fifo_global_prefix_key(&self.key)
    }

    /// `MaxSize`: a negative configured size behaves like zero.
    pub fn max_size(&self) -> i64 {
        fifo_max_size(self.max_size)
    }

    /// `Insert`: `LPUSH` the item, then trim to the configured size. A size of
    /// zero keeps the list empty (`LTRIM 0 0` on an empty list is a no-op).
    pub async fn insert(&self, value: impl AsRef<[u8]>) -> Result<(), RedisError> {
        let key = self.global_prefix_key();
        let max_size = self.max_size();
        if max_size == 0 {
            self.kv.ltrim(&key, 0, 0).await?;
            return Ok(());
        }
        self.kv.lpush(&key, value).await?;
        self.kv.ltrim(&key, 0, max_size - 1).await
    }

    /// `Size`.
    pub async fn size(&self) -> Result<i64, RedisError> {
        self.kv.llen(&self.global_prefix_key()).await
    }

    /// `IsEmpty`.
    pub async fn is_empty(&self) -> Result<bool, RedisError> {
        Ok(self.size().await? == 0)
    }

    /// `All`: every stored item, newest first (`LRANGE 0 -1`).
    pub async fn all(&self) -> Result<Vec<Vec<u8>>, RedisError> {
        self.slice(0, -1).await
    }

    /// `Slice`: `LRANGE from to`, truncated to the configured size.
    pub async fn slice(&self, from: i64, to: i64) -> Result<Vec<Vec<u8>>, RedisError> {
        let mut items = self.kv.lrange(&self.global_prefix_key(), from, to).await?;
        let max_size = self.max_size();
        if items.len() as i64 > max_size {
            items.truncate(max_size as usize);
        }
        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_prefixes_match_the_baseline() {
        assert_eq!(
            cache_key_prefix("background-job-logger"),
            "v2:background-job-logger:"
        );
        assert_eq!(
            cache_redis_key("background-job-logger", "knownJobNames"),
            "v2:background-job-logger:knownJobNames"
        );

        // The FIFO list key passed to `fifo_list` is already namespaced, and
        // the list prepends the data version again.
        let list_key = cache_redis_key(
            "background-job-logger",
            "webhook-log-janitor:batchchanges.webhook-log-janitor:worker:recentRuns",
        );
        assert_eq!(
            fifo_global_prefix_key(&list_key),
            "v2:v2:background-job-logger:\
             webhook-log-janitor:batchchanges.webhook-log-janitor:worker:recentRuns"
        );
    }

    #[test]
    fn fifo_list_max_size_clamps_negative_values() {
        assert_eq!(fifo_max_size(-3), 0);
        assert_eq!(fifo_max_size(0), 0);
        assert_eq!(fifo_max_size(100), 100);
    }
}
