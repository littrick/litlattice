#![forbid(unsafe_code)]

//! `internal/featureflag/cache.go`: the Redis-backed evaluated-feature-flag
//! cache, plus the lazy `FlagSet` loader behind
//! `internal/featureflag/middleware.go`.
//!
//! The Go cache is a plain Redis hash per flag:
//!
//! * key: `ff_<name>` (`getFlagCacheKey`, `cache.go:56-58`);
//! * field: `uid_<id>` for an authenticated actor, `auid_<anonymousUID>`
//!   otherwise (`getVisitorIDForActor`, `cache.go:46-54`);
//! * value: `strconv.FormatBool(value)` = `"true"` / `"false"`
//!   (`setEvaluatedFlagToCache`, `cache.go:34-44`).
//!
//! `GetBool` writes the evaluated value (`flagset.go:41-51`),
//! `GetEvaluatedFlagSet` reads it back for every flag in the actor's FlagSet
//! (`cache.go:16-32`), and `ClearEvaluatedFlagFromCache` deletes the whole
//! hash (`cache.go:60-63`). All Redis errors are best-effort: reads that fail
//! are treated as "not evaluated", writes and deletes are ignored.

use std::collections::HashMap;

use async_trait::async_trait;

use sg_graphql::exec::Actor;
use sg_redis::KeyValue;

/// `getFlagCacheKey`: `"ff_" + name` (no separator, no extra prefix).
pub fn get_flag_cache_key(name: &str) -> String {
    format!("ff_{name}")
}

/// The actor attributes the cache keys on. Go's `actor.Actor` exposes a
/// numeric `UID` (0 = none) and a semi-stable `AnonymousUID`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlagActor {
    pub uid: i32,
    pub anonymous_uid: String,
}

impl FlagActor {
    /// `(*actor.Actor).IsAuthenticated`: `UID != 0`.
    pub fn is_authenticated(&self) -> bool {
        self.uid != 0
    }

    /// Build from the executor actor plus the request's resolved anonymous UID.
    /// `Actor::User` carries the UID; `Anonymous`/`Internal` have UID 0.
    pub fn from_exec(actor: Actor, anonymous_uid: &str) -> Self {
        Self {
            uid: actor.uid().unwrap_or(0),
            anonymous_uid: anonymous_uid.to_owned(),
        }
    }
}

/// `getVisitorIDForActor` (`cache.go:46-54`).
pub fn get_visitor_id_for_actor(actor: &FlagActor) -> Result<String, String> {
    if actor.is_authenticated() {
        Ok(format!("uid_{}", actor.uid))
    } else if !actor.anonymous_uid.is_empty() {
        Ok(format!("auid_{}", actor.anonymous_uid))
    } else {
        Err("UID/AnonymousUID are empty for the given actor.".to_owned())
    }
}

/// `strconv.FormatBool`.
pub fn format_go_bool(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

/// `strconv.ParseBool`: accepts `1, t, T, TRUE, true, True, 0, f, F, FALSE,
/// false, False`. Every other string is a parse error (which the Go cache
/// treats as "not evaluated").
pub fn parse_go_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// The Redis operations the cache needs. Kept narrow so the differential
/// harness and unit tests can inject an in-memory implementation.
#[async_trait]
pub trait EvaluatedFlagCache: Send + Sync {
    /// `HGet(key, field).Bool()`: `None` for a missing field *or* an
    /// unparseable value.
    async fn hget_bool(&self, key: &str, field: &str) -> Result<Option<bool>, String>;
    /// `HSet(key, field, strconv.FormatBool(value))`.
    async fn hset_bool(&self, key: &str, field: &str, value: bool) -> Result<(), String>;
    /// `Del(key)`.
    async fn del(&self, key: &str) -> Result<(), String>;
}

/// Production adapter over `redispool.Store`'s Redis endpoint. The connection
/// is established lazily (like redigo's pool), so a frontend starting before
/// Redis is reachable simply treats cache operations as best-effort.
pub struct RedisEvaluatedFlagCache {
    endpoint: String,
    cached: tokio::sync::Mutex<Option<KeyValue>>,
}

impl RedisEvaluatedFlagCache {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// `redispool.Store`'s endpoint (`REDIS_STORE_ENDPOINT`, else
    /// `REDIS_ENDPOINT`, else `redis-store:6379`).
    pub fn from_env() -> Self {
        Self::new(sg_redis::store_endpoint())
    }

    /// Use an already-connected handle (differential harness/tests).
    pub fn from_key_value(kv: KeyValue) -> Self {
        Self {
            endpoint: String::new(),
            cached: tokio::sync::Mutex::new(Some(kv)),
        }
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
}

#[async_trait]
impl EvaluatedFlagCache for RedisEvaluatedFlagCache {
    async fn hget_bool(&self, key: &str, field: &str) -> Result<Option<bool>, String> {
        let raw = self
            .kv()
            .await?
            .hget(key, field)
            .await
            .map_err(|error| error.to_string())?;
        Ok(raw.as_deref().and_then(parse_go_bool))
    }

    async fn hset_bool(&self, key: &str, field: &str, value: bool) -> Result<(), String> {
        self.kv()
            .await?
            .hset(key, field, format_go_bool(value))
            .await
            .map_err(|error| error.to_string())
    }

    async fn del(&self, key: &str) -> Result<(), String> {
        self.kv()
            .await?
            .del(key)
            .await
            .map_err(|error| error.to_string())
    }
}

/// `getEvaluatedFlagSetFromCache` (`cache.go:16-32`): for every flag in the
/// actor's FlagSet, read the cached value. A missing actor identity yields an
/// empty set; a missing/unparseable/failed read skips that flag.
pub async fn get_evaluated_flag_set_from_cache(
    cache: &dyn EvaluatedFlagCache,
    flags: &HashMap<String, bool>,
    actor: &FlagActor,
) -> HashMap<String, bool> {
    let mut evaluated = HashMap::new();
    let Ok(visitor_id) = get_visitor_id_for_actor(actor) else {
        return evaluated;
    };
    for flag in flags.keys() {
        if let Ok(Some(value)) = cache
            .hget_bool(&get_flag_cache_key(flag), &visitor_id)
            .await
        {
            evaluated.insert(flag.clone(), value);
        }
    }
    evaluated
}

/// `setEvaluatedFlagToCache` (`cache.go:34-44`); errors are ignored.
pub async fn set_evaluated_flag_to_cache(
    cache: &dyn EvaluatedFlagCache,
    actor: &FlagActor,
    flag_name: &str,
    value: bool,
) {
    let Ok(visitor_id) = get_visitor_id_for_actor(actor) else {
        return;
    };
    let _ = cache
        .hset_bool(&get_flag_cache_key(flag_name), &visitor_id, value)
        .await;
}

/// `ClearEvaluatedFlagFromCache` (`cache.go:60-63`); errors are ignored.
pub async fn clear_evaluated_flag_from_cache(cache: &dyn EvaluatedFlagCache, flag_name: &str) {
    let _ = cache.del(&get_flag_cache_key(flag_name)).await;
}

/// `(*FlagSet).GetBool` (`flagset.go:41-51`): if the actor's FlagSet contains
/// the flag, write the value to the cache and return it; otherwise `None`
/// (Go's `(false, false)`).
pub async fn get_bool(
    cache: Option<&dyn EvaluatedFlagCache>,
    flags: &HashMap<String, bool>,
    actor: &FlagActor,
    flag: &str,
) -> Option<bool> {
    let value = flags.get(flag).copied()?;
    if let Some(cache) = cache {
        set_evaluated_flag_to_cache(cache, actor, flag, value).await;
    }
    Some(value)
}

/// `flagSetFetcher.fetchForActor` (`middleware.go:67-91`): authenticated →
/// user flags, else anonymous (when an anonymous UID exists), else global.
/// Each failing step falls through to the next; when everything fails the set
/// is empty (Go's `FlagSet{flags: nil}`).
///
/// Takes the GraphQL `FeatureFlagStore` directly so a `&dyn FeatureFlagStore`
/// can be passed without a trait-object upcast.
pub async fn load_flags_for_actor<S>(store: &S, actor: &FlagActor) -> HashMap<String, bool>
where
    S: crate::graphql_resolvers::FeatureFlagStore + ?Sized,
{
    if actor.is_authenticated() {
        if let Ok(flags) = store.get_user_flags(actor.uid).await {
            return flags;
        }
    }
    if !actor.anonymous_uid.is_empty() {
        if let Ok(flags) = store.get_anonymous_flags(&actor.anonymous_uid).await {
            return flags;
        }
    }
    store.get_global_flags().await.unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemoryCache {
        hashes: Mutex<HashMap<(String, String), String>>,
    }

    #[async_trait]
    impl EvaluatedFlagCache for MemoryCache {
        async fn hget_bool(&self, key: &str, field: &str) -> Result<Option<bool>, String> {
            Ok(self
                .hashes
                .lock()
                .unwrap()
                .get(&(key.to_owned(), field.to_owned()))
                .and_then(|value| parse_go_bool(value)))
        }

        async fn hset_bool(&self, key: &str, field: &str, value: bool) -> Result<(), String> {
            self.hashes.lock().unwrap().insert(
                (key.to_owned(), field.to_owned()),
                format_go_bool(value).to_owned(),
            );
            Ok(())
        }

        async fn del(&self, key: &str) -> Result<(), String> {
            self.hashes
                .lock()
                .unwrap()
                .retain(|(stored_key, _), _| stored_key != key);
            Ok(())
        }
    }

    #[test]
    fn visitor_ids_match_go() {
        assert_eq!(
            get_visitor_id_for_actor(&FlagActor {
                uid: 42,
                anonymous_uid: "anon".to_owned()
            })
            .unwrap(),
            "uid_42"
        );
        assert_eq!(
            get_visitor_id_for_actor(&FlagActor {
                uid: 0,
                anonymous_uid: "anon".to_owned()
            })
            .unwrap(),
            "auid_anon"
        );
        assert!(get_visitor_id_for_actor(&FlagActor::default()).is_err());
    }

    #[test]
    fn parse_go_bool_matches_strconv() {
        for (text, expected) in [
            ("1", Some(true)),
            ("t", Some(true)),
            ("T", Some(true)),
            ("TRUE", Some(true)),
            ("true", Some(true)),
            ("True", Some(true)),
            ("0", Some(false)),
            ("f", Some(false)),
            ("F", Some(false)),
            ("FALSE", Some(false)),
            ("false", Some(false)),
            ("False", Some(false)),
            ("yes", None),
            ("", None),
            ("2", None),
        ] {
            assert_eq!(parse_go_bool(text), expected, "{text}");
        }
    }

    #[tokio::test]
    async fn get_bool_writes_and_evaluated_reads() {
        let cache = MemoryCache::default();
        let actor = FlagActor {
            uid: 7,
            anonymous_uid: String::new(),
        };
        let flags = HashMap::from([("alpha".to_owned(), true), ("beta".to_owned(), false)]);

        assert_eq!(get_bool(Some(&cache), &flags, &actor, "alpha").await, Some(true));
        // Missing flag: no write.
        assert_eq!(get_bool(Some(&cache), &flags, &actor, "missing").await, None);

        let evaluated = get_evaluated_flag_set_from_cache(&cache, &flags, &actor).await;
        assert_eq!(evaluated, HashMap::from([("alpha".to_owned(), true)]));

        // Another actor does not see it.
        let other = FlagActor {
            uid: 8,
            anonymous_uid: String::new(),
        };
        assert!(get_evaluated_flag_set_from_cache(&cache, &flags, &other)
            .await
            .is_empty());

        // Clear removes the whole hash.
        clear_evaluated_flag_from_cache(&cache, "alpha").await;
        assert!(get_evaluated_flag_set_from_cache(&cache, &flags, &actor)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn anonymous_actor_without_uid_never_writes() {
        let cache = MemoryCache::default();
        let actor = FlagActor::default();
        let flags = HashMap::from([("alpha".to_owned(), true)]);
        assert_eq!(get_bool(Some(&cache), &flags, &actor, "alpha").await, Some(true));
        assert!(cache.hashes.lock().unwrap().is_empty());
        assert!(get_evaluated_flag_set_from_cache(&cache, &flags, &actor)
            .await
            .is_empty());
    }
}
