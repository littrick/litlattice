//! M9.8fu differential of the anonymous GraphQL rate limiter against the Go
//! oracle (`rate_limit.go:409-487`, `throttled` v2.12.0 GCRA,
//! `search.GuessSource` and `redigostore`).
//!
//! The algorithm cases use the same deterministic fake store as the oracle
//! (`GetWithTime` advances a 1ms clock per call), so results are byte
//! comparable. The Redis cases compare key naming, SETNX/EXPIRE,
//! compare-and-swap and TTL classes, never absolute times.
//!
//! Env:
//!
//! * `SG_M9FU_DIFF=1`
//! * `M9FU_CORPUS`, `M9FU_OUT`
//! * `M9FU_SOURCE_CORPUS`, `M9FU_SOURCE_OUT`
//! * `M9FU_REDIS_ADDR` (default `127.0.0.1:6379`), `M9FU_REDIS_OUT`

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sg_redis::KeyValue;
use sg_service_frontend::rate_limit::{
    BasicLimitWatcher, GcraRateLimiter, GcraStore, RateLimitResult, RedisGcraStore,
};
use sg_service_frontend::search_source::guess_source;

use sg_service_frontend::graphql_api::LimiterArgs;

const NANOS_PER_HOUR: i64 = 3_600_000_000_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const REDIS_TEST_PREFIX: &str = "gql:rl:m9fu";

/// Same deterministic store as the Go oracle: a 1ms clock per `GetWithTime`,
/// TTLs ignored, optional always-failing CAS.
struct FakeStore {
    values: Mutex<BTreeMap<String, i64>>,
    now: Mutex<i64>,
    fail_cas: bool,
}

impl FakeStore {
    fn new(preload: &BTreeMap<String, i64>, fail_cas: bool) -> Self {
        Self {
            values: Mutex::new(preload.clone()),
            now: Mutex::new(1_600_000_000_000_000_000),
            fail_cas,
        }
    }

    fn snapshot(&self) -> BTreeMap<String, i64> {
        self.values.lock().expect("values").clone()
    }
}

#[async_trait]
impl GcraStore for FakeStore {
    async fn get_with_time(&self, key: &str) -> Result<(i64, i64), String> {
        let mut now = self.now.lock().expect("now");
        *now += 1_000_000;
        let value = self
            .values
            .lock()
            .expect("values")
            .get(key)
            .copied()
            .unwrap_or(-1);
        Ok((value, *now))
    }

    async fn set_if_not_exists_with_ttl(
        &self,
        key: &str,
        value: i64,
        _ttl_ns: i64,
    ) -> Result<bool, String> {
        let mut values = self.values.lock().expect("values");
        if values.contains_key(key) {
            return Ok(false);
        }
        values.insert(key.to_owned(), value);
        Ok(true)
    }

    async fn compare_and_swap_with_ttl(
        &self,
        key: &str,
        old: i64,
        new: i64,
        _ttl_ns: i64,
    ) -> Result<bool, String> {
        if self.fail_cas {
            return Ok(false);
        }
        let mut values = self.values.lock().expect("values");
        match values.get(key) {
            Some(value) if *value == old => {
                values.insert(key.to_owned(), new);
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

#[derive(Deserialize)]
struct Case {
    name: String,
    limit: i64,
    #[serde(default)]
    fail_cas: bool,
    #[serde(default)]
    preload: BTreeMap<String, i64>,
    #[serde(default)]
    gcra: Vec<GcraCall>,
    #[serde(default)]
    basic: Vec<BasicCall>,
}

#[derive(Deserialize)]
struct GcraCall {
    key: String,
    quantity: i64,
}

#[derive(Deserialize)]
struct BasicCall {
    anonymous: bool,
    request_name: String,
    request_source: String,
}

#[derive(Serialize)]
struct ResultOut {
    limited: bool,
    limit: i64,
    remaining: i64,
    #[serde(rename = "resetAfter")]
    reset_after: i64,
    #[serde(rename = "retryAfter")]
    retry_after: i64,
    error: String,
}

impl ResultOut {
    fn from_result(limited: bool, result: RateLimitResult, error: String) -> Self {
        Self {
            limited,
            limit: result.limit,
            remaining: result.remaining,
            reset_after: result.reset_after_ns,
            retry_after: result.retry_after_ns,
            error,
        }
    }
}

#[derive(Serialize)]
struct BasicOut {
    enabled: bool,
    result: Option<ResultOut>,
}

#[derive(Serialize)]
struct Out {
    enabled: bool,
    gcra: Vec<ResultOut>,
    basic: Vec<BasicOut>,
    state: BTreeMap<String, i64>,
}

#[tokio::test]
async fn graphql_rate_limit_harness() {
    if std::env::var("SG_M9FU_DIFF").as_deref() != Ok("1") {
        return;
    }
    if let (Ok(corpus), Ok(out_dir)) = (std::env::var("M9FU_CORPUS"), std::env::var("M9FU_OUT")) {
        let cases: Vec<Case> =
            serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
        std::fs::create_dir_all(&out_dir).expect("out dir");

        for case in cases {
            let store = Arc::new(FakeStore::new(&case.preload, case.fail_cas));
            let mut out = Out {
                enabled: false,
                gcra: Vec::new(),
                basic: Vec::new(),
                state: BTreeMap::new(),
            };

            if !case.gcra.is_empty() {
                let limiter = GcraRateLimiter::new(
                    store.clone(),
                    NANOS_PER_HOUR / case.limit,
                    (case.limit as f64 * 0.2) as i64,
                )
                .expect("limiter");
                for call in &case.gcra {
                    let entry = match limiter.rate_limit(&call.key, call.quantity).await {
                        Ok((limited, result)) => {
                            ResultOut::from_result(limited, result, String::new())
                        }
                        Err(error) => ResultOut::from_result(false, error.result, error.message),
                    };
                    out.gcra.push(entry);
                }
            }

            let watcher = BasicLimitWatcher::new(store.clone());
            watcher.update_from_config(case.limit);
            out.enabled = watcher.enabled();
            for call in &case.basic {
                if !out.enabled {
                    out.basic.push(BasicOut {
                        enabled: false,
                        result: None,
                    });
                    continue;
                }
                let observed = watcher
                    .check_detailed(LimiterArgs {
                        is_ip: false,
                        anonymous: call.anonymous,
                        request_name: &call.request_name,
                        request_source: &call.request_source,
                    })
                    .await;
                let result = observed.map(|(outcome, result)| {
                    let limited = matches!(
                        outcome,
                        sg_service_frontend::graphql_api::RateLimitOutcome::Limited { .. }
                    );
                    ResultOut::from_result(limited, result, String::new())
                });
                out.basic.push(BasicOut {
                    enabled: true,
                    result,
                });
            }

            out.state = store.snapshot();
            let encoded = serde_json::to_vec(&out).expect("encode");
            std::fs::write(
                Path::new(&out_dir).join(format!("{}.json", case.name)),
                &encoded,
            )
            .expect("write");
            println!("RUST {} len={}", case.name, encoded.len());
        }
    }

    // search.GuessSource (optional, only when the oracle env is present).
    if let (Ok(source_corpus), Ok(source_out)) = (
        std::env::var("M9FU_SOURCE_CORPUS"),
        std::env::var("M9FU_SOURCE_OUT"),
    ) {
        #[derive(Deserialize)]
        struct UaCase {
            ua: String,
        }
        let ua_cases: Vec<UaCase> =
            serde_json::from_slice(&std::fs::read(source_corpus).expect("source corpus"))
                .expect("source corpus json");
        let sources: Vec<String> = ua_cases.iter().map(|case| guess_source(&case.ua)).collect();
        std::fs::create_dir_all(&source_out).expect("source out dir");
        std::fs::write(
            Path::new(&source_out).join("sources.json"),
            serde_json::to_vec(&sources).expect("encode sources"),
        )
        .expect("write sources");
        println!("RUST sources len={}", sources.len());
    }

    // Redis store (optional, only when the oracle env is present).
    let Ok(redis_out) = std::env::var("M9FU_REDIS_OUT") else {
        return;
    };
    let addr = std::env::var("M9FU_REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    let raw = KeyValue::connect(&addr).await.expect("redis connect");
    cleanup_redis(&raw).await;
    let store = RedisGcraStore::with_key_prefix(raw.clone(), REDIS_TEST_PREFIX);

    let mut results = Vec::new();
    let (missing, _) = store.get_with_time("missing").await.expect("get missing");

    results.push(
        store
            .set_if_not_exists_with_ttl("keyA", 100, 100 * NANOS_PER_SECOND)
            .await
            .expect("setnx A"),
    );

    results.push(
        store
            .set_if_not_exists_with_ttl("keyA", 200, 100 * NANOS_PER_SECOND)
            .await
            .expect("setnx A again"),
    );

    results.push(
        store
            .compare_and_swap_with_ttl("keyA", 100, 300, 100 * NANOS_PER_SECOND)
            .await
            .expect("cas hit"),
    );

    results.push(
        store
            .compare_and_swap_with_ttl("keyA", 100, 400, 100 * NANOS_PER_SECOND)
            .await
            .expect("cas miss"),
    );
    results.push(
        store
            .compare_and_swap_with_ttl("missing", 1, 2, 100 * NANOS_PER_SECOND)
            .await
            .expect("cas missing"),
    );
    results.push(
        store
            .set_if_not_exists_with_ttl("keyB", 1, 100_000_000)
            .await
            .expect("setnx B"),
    );

    let kv = raw.with_prefix(REDIS_TEST_PREFIX);
    let mut keys = kv.keys("*").await.expect("keys");
    keys.sort();
    let mut values = BTreeMap::new();
    let mut ttls = BTreeMap::new();
    for full_key in &keys {
        let logical = full_key
            .strip_prefix(&format!("{REDIS_TEST_PREFIX}:"))
            .unwrap_or(full_key)
            .to_owned();
        let (value, _) = store.get_with_time(&logical).await.expect("get key");
        let ttl = kv.ttl(&logical).await.expect("ttl");
        values.insert(logical.clone(), value);
        ttls.insert(logical, ttl_class(ttl).to_owned());
    }
    cleanup_redis(&raw).await;

    #[derive(Serialize)]
    struct RedisOut {
        #[serde(rename = "missingValue")]
        missing_value: i64,
        keys: Vec<String>,
        values: BTreeMap<String, i64>,
        ttls: BTreeMap<String, String>,
        results: Vec<bool>,
    }
    let encoded = serde_json::to_vec(&RedisOut {
        missing_value: missing,
        keys,
        values,
        ttls,
        results,
    })
    .expect("encode redis");
    std::fs::create_dir_all(&redis_out).expect("redis out dir");
    std::fs::write(Path::new(&redis_out).join("redis.json"), &encoded).expect("write redis");
    println!("RUST redis len={}", encoded.len());
}

fn ttl_class(seconds: i64) -> &'static str {
    if seconds >= 50 {
        "long"
    } else if seconds >= 1 {
        "short"
    } else {
        "gone"
    }
}

async fn cleanup_redis(kv: &KeyValue) {
    let keys = kv.keys("*").await.expect("cleanup keys");
    for key in keys {
        kv.del(&key).await.expect("cleanup del");
    }
}
