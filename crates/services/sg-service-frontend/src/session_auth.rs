#![forbid(unsafe_code)]

//! Production adapters for [`sg_session::auth`]: the Redis session store and
//! the `users` lookup.
//!
//! * [`RedisSessionStore`] is `redistore.load`/`save`/`delete`: `GET`,
//!   `SETEX` and `DEL` on `session_<id>`, with a lazily established
//!   `sg_redis::KeyValue`. The baseline's redigo pool is also lazy and
//!   reconnects on demand; a failed connect makes this request fail with
//!   [`sg_session::SessionStoreError`] (the HTTP layer answers the baseline's
//!   500 `failed to read session actor` and keeps the cookie).
//! * [`PgSessionUsers`] is `db.Users().GetByID` restricted to the columns the
//!   session check and `User` resolvers read.

use async_trait::async_trait;
use sqlx::PgPool;
use tokio::sync::Mutex;

use sg_redis::KeyValue;
use sg_session::auth::{SessionStore, SessionStoreError, SessionUser, SessionUserLookup};
use sg_session::{KEY_PREFIX, TTL_SECONDS};

use crate::graphql_resolvers::{UserData, UserProfiles};

/// `newSessionStore().Get` → `redistore.load`.
pub struct RedisSessionStore {
    endpoint: String,
    cached: Mutex<Option<KeyValue>>,
}

impl RedisSessionStore {
    /// `redispool.Store`'s endpoint (`REDIS_STORE_ENDPOINT`, else
    /// `REDIS_ENDPOINT`, else `redis-store:6379`).
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            cached: Mutex::new(None),
        }
    }

    pub fn from_env() -> Self {
        Self::new(sg_redis::store_endpoint())
    }

    async fn kv(&self) -> Result<KeyValue, SessionStoreError> {
        if let Some(kv) = self.cached.lock().await.clone() {
            return Ok(kv);
        }
        let kv = KeyValue::connect(&self.endpoint)
            .await
            .map_err(|error| SessionStoreError(error.to_string()))?;
        *self.cached.lock().await = Some(kv.clone());
        Ok(kv)
    }

    /// `session_<id>` (the `session_` prefix is part of the key, not a Redis
    /// key namespace separator).
    pub fn key(id: &str) -> String {
        format!("{KEY_PREFIX}{id}")
    }
}

#[async_trait]
impl SessionStore for RedisSessionStore {
    async fn load(&self, id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
        let kv = self.kv().await?;
        kv.get(&Self::key(id))
            .await
            .map_err(|error| SessionStoreError(error.to_string()))
    }

    /// `redistore.save`: `SETEX session_<id> <MaxAge|1200> <gob>`.
    async fn save(
        &self,
        id: &str,
        payload: Vec<u8>,
        ttl_seconds: i64,
    ) -> Result<(), SessionStoreError> {
        let kv = self.kv().await?;
        kv.set_ex(&Self::key(id), ttl_seconds, payload)
            .await
            .map_err(|error| SessionStoreError(error.to_string()))
    }

    /// `redistore.delete`: `DEL session_<id>`.
    async fn delete(&self, id: &str) -> Result<(), SessionStoreError> {
        let kv = self.kv().await?;
        kv.del(&Self::key(id))
            .await
            .map_err(|error| SessionStoreError(error.to_string()))
    }
}

/// `db.Users().GetByID` for the session check.
pub struct PgSessionUsers {
    pool: PgPool,
}

impl PgSessionUsers {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SessionUserLookup for PgSessionUsers {
    async fn get(&self, uid: i32) -> Result<Option<SessionUser>, String> {
        sg_db::users::get_session_user(&self.pool, uid)
            .await
            .map_err(|error| error.to_string())
            .map(|record| {
                record.map(|record| SessionUser {
                    id: record.id,
                    created_at: record.created_at,
                    invalidated_sessions_at: record.invalidated_sessions_at,
                })
            })
    }
}

/// `db.Users().GetByCurrentAuthUser`'s row for the `User` resolvers. Shares
/// `get_session_user`'s query and `deleted_at IS NULL` predicate.
#[async_trait]
impl UserProfiles for PgSessionUsers {
    async fn profile(&self, uid: i32) -> Result<Option<UserData>, String> {
        sg_db::users::get_session_user(&self.pool, uid)
            .await
            .map_err(|error| error.to_string())
            .map(|record| {
                record.map(|record| UserData {
                    id: record.id,
                    username: record.username,
                    display_name: record.display_name,
                    avatar_url: record.avatar_url,
                    created_at: record.created_at,
                    updated_at: record.updated_at,
                    site_admin: record.site_admin,
                    tos_accepted: record.tos_accepted,
                    primary_email: record.email,
                })
            })
    }
}

/// The `SETEX` TTL, re-exported for the future write path and tests.
pub const SESSION_TTL_SECONDS: i64 = TTL_SECONDS;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_uses_the_baseline_prefix() {
        assert_eq!(
            RedisSessionStore::key("ABC"),
            "session_ABC",
            "redistore.SetKeyPrefix(\"session_\")"
        );
    }
}
