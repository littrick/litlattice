//! M9.21c differential of the Redis evaluated-feature-flag cache
//! (`internal/featureflag/cache.go`, `middleware.go:67-91`,
//! `flagset.go:41-51`) against the Go oracle over the same local Redis.
//!
//! Both sides replay `cases.json`: `get_bool` mirrors `FlagSet.GetBool`
//! (including its cache write), `evaluated` mirrors
//! `getEvaluatedFlagSetFromCache`, `clear` mirrors
//! `ClearEvaluatedFlagFromCache`, `hset_raw` injects arbitrary hash values
//! (for `strconv.ParseBool` semantics), and every step dumps the raw `ff_*`
//! keyspace (keys, fields, values).
//!
//! Env:
//!
//! * `SG_M921C_DIFF=1`
//! * `M921C_CORPUS`, `M921C_OUT`
//! * `M921C_REDIS_ADDR` (default `127.0.0.1:6379`)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use serde::{Deserialize, Serialize};

use sg_redis::KeyValue;
use sg_service_frontend::feature_flag_cache::{
    clear_evaluated_flag_from_cache, get_bool, get_evaluated_flag_set_from_cache, FlagActor,
    RedisEvaluatedFlagCache,
};

#[derive(Default, Deserialize)]
struct ActorIn {
    #[serde(default)]
    uid: i32,
    #[serde(rename = "anonymousUid", default)]
    anonymous_uid: String,
}

#[derive(Deserialize)]
struct Step {
    op: String,
    #[serde(default)]
    flag: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    field: String,
    #[serde(default)]
    value: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    actor: ActorIn,
    #[serde(default)]
    flags: BTreeMap<String, bool>,
    #[serde(default)]
    steps: Vec<Step>,
}

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Serialize)]
struct StepOut {
    op: String,
    flag: String,
    found: bool,
    value: bool,
    flags: Option<BTreeMap<String, bool>>,
    redis: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Serialize)]
struct CaseOut {
    name: String,
    steps: Vec<StepOut>,
}

#[derive(Serialize)]
struct Output {
    cases: Vec<CaseOut>,
}

async fn dump(kv: &KeyValue) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut keys = kv.keys("ff_*").await.expect("KEYS");
    keys.sort();
    let mut out = BTreeMap::new();
    for key in keys {
        let values = kv.hgetall(&key).await.expect("HGETALL");
        out.insert(key, values.into_iter().collect());
    }
    out
}

async fn cleanup(kv: &KeyValue) {
    for key in kv.keys("ff_*").await.expect("KEYS") {
        kv.del(&key).await.expect("DEL");
    }
}

#[tokio::test]
async fn featureflag_cache_harness() {
    if std::env::var("SG_M921C_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M921C_CORPUS").expect("M921C_CORPUS");
    let out = std::env::var("M921C_OUT").expect("M921C_OUT");
    let addr = std::env::var("M921C_REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    std::fs::create_dir_all(&out).expect("out dir");

    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(&corpus).expect("corpus")).expect("corpus json");
    let cases = corpus.cases;

    let kv = KeyValue::connect(&addr).await.expect("connect redis");
    cleanup(&kv).await;
    let cache = RedisEvaluatedFlagCache::from_key_value(kv.clone());

    let mut output = Output { cases: Vec::new() };
    for case in cases {
        let actor = FlagActor {
            uid: case.actor.uid,
            anonymous_uid: case.actor.anonymous_uid.clone(),
        };
        let flags: HashMap<String, bool> = case.flags.iter().map(|(k, v)| (k.clone(), *v)).collect();
        let mut case_out = CaseOut {
            name: case.name.clone(),
            steps: Vec::new(),
        };
        for step in case.steps {
            let mut step_out = StepOut {
                op: step.op.clone(),
                flag: step.flag.clone(),
                found: false,
                value: false,
                flags: None,
                redis: BTreeMap::new(),
            };
            match step.op.as_str() {
                "get_bool" => {
                    let value = get_bool(Some(&cache), &flags, &actor, &step.flag).await;
                    step_out.found = value.is_some();
                    step_out.value = value.unwrap_or(false);
                }
                "evaluated" => {
                    let evaluated =
                        get_evaluated_flag_set_from_cache(&cache, &flags, &actor).await;
                    step_out.flags = Some(evaluated.into_iter().collect());
                }
                "clear" => clear_evaluated_flag_from_cache(&cache, &step.flag).await,
                "hset_raw" => {
                    kv.hset(&step.key, &step.field, &step.value)
                        .await
                        .expect("HSET");
                }
                "dump" => {}
                other => panic!("unknown op {other}"),
            }
            step_out.redis = dump(&kv).await;
            case_out.steps.push(step_out);
        }
        output.cases.push(case_out);
    }

    cleanup(&kv).await;

    let encoded = serde_json::to_string_pretty(&output).expect("encode");
    std::fs::write(
        Path::new(&out).join("featureflag_cache.json"),
        encoded.as_bytes(),
    )
    .expect("write");
    println!("RUST cases={}", output.cases.len());
}
