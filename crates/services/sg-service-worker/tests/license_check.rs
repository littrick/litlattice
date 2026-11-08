#![forbid(unsafe_code)]

//! M9.8z differential harness (Rust side): the `license-check` job.
//!
//! Mirrors `cmd/worker/internal/licensecheck/license_harness_test.go`. Both
//! sides print the job/routine contract, the `license_user_count:*` hashes
//! written by the max-user-count handler, and the licensing keys, license
//! source calls and HTTP request left by the license checker.
//!
//! `test_ws/p9-go/run_license_check_diff.sh` prepares the scratch database
//! (`global_state` and `users` fixtures), starts a dedicated scratch Redis and
//! compares the canonical lines. Freshly written max-users timestamps are
//! printed as `now`; `global_state` read errors are reduced to
//! `db-global-state` because the database drivers word them differently.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_http::client::HttpResponse;
use sg_licensing::{
    Info, LICENSE_INVALID_REASON, LICENSE_VALIDITY_STORE_KEY, MAX_USERS_KEY, MAX_USERS_TIME_KEY,
};
use sg_redis::KeyValue;
use sg_service_worker::framework::periodic::Handler;
use sg_service_worker::jobs::licensecheck::{
    LicenseCheckDoer, LicenseCheckerHandler, LicenseSource, MaxUserCountHandler,
    StaticLicenseSource,
};
use sg_service_worker::jobs::{Job, JobDeps};
use sqlx::postgres::PgPoolOptions;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

const LAST_CALLED_AT_STORE_KEY: &str = "licensing:last_called_at";
const PREV_LICENSE_TOKEN_KEY: &str = "licensing:prev_license_hash";
const LICENSE_URL: &str = "https://license.test";
const SITE_ID: &str = "00000000-0000-4000-8000-000000000001";

fn enterprise_info() -> Info {
    Info {
        tags: vec!["plan:enterprise-0".to_owned()],
        ..Info::default()
    }
}

fn air_gapped_info() -> Info {
    Info {
        tags: vec!["plan:enterprise-air-gap-0".to_owned()],
        ..Info::default()
    }
}

fn tagged_info(tag: &str) -> Info {
    Info {
        tags: vec![tag.to_owned()],
        ..Info::default()
    }
}

fn free_info() -> Info {
    Info {
        tags: vec!["plan:free-1".to_owned()],
        ..Info::default()
    }
}

/// A scripted [`LicenseSource`]; the last step repeats.
struct ScriptedSource {
    steps: Vec<Step>,
    calls: AtomicUsize,
}

struct Step {
    info: Option<Info>,
    signature: Vec<u8>,
    error: Option<String>,
}

impl Step {
    fn ok(info: Info, signature: &str) -> Self {
        Self {
            info: Some(info),
            signature: signature.as_bytes().to_vec(),
            error: None,
        }
    }

    fn err(message: &str) -> Self {
        Self {
            info: None,
            signature: Vec::new(),
            error: Some(message.to_owned()),
        }
    }
}

impl ScriptedSource {
    fn new(steps: Vec<Step>) -> Self {
        Self {
            steps,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LicenseSource for ScriptedSource {
    async fn configured_info(&self) -> Result<(Info, Vec<u8>), String> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let step = &self.steps[call.min(self.steps.len() - 1)];
        match &step.error {
            Some(error) => Err(error.clone()),
            None => Ok((
                step.info.clone().expect("step info"),
                step.signature.clone(),
            )),
        }
    }
}

/// A scripted [`LicenseCheckDoer`] that records the requests it saw.
struct ScriptedDoer {
    status: u16,
    body: Vec<u8>,
    error: Option<String>,
    requests: Mutex<Vec<String>>,
}

impl ScriptedDoer {
    fn new(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.as_bytes().to_vec(),
            error: None,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn failing(error: &str) -> Self {
        Self {
            status: 0,
            body: Vec::new(),
            error: Some(error.to_owned()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("lock").clone()
    }
}

#[async_trait]
impl LicenseCheckDoer for ScriptedDoer {
    async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<HttpResponse, String> {
        let header = |name: &str| {
            headers
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        self.requests.lock().expect("lock").push(format!(
            "req method=POST url={url} auth={} ctype={} body={}",
            header("Authorization"),
            header("Content-Type"),
            String::from_utf8_lossy(&body)
        ));
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok(HttpResponse {
            status: self.status,
            headers: BTreeMap::new(),
            body: self.body.clone(),
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn license_check_matches_the_go_harness() {
    if std::env::var("SG_LICENSE_HARNESS_ON").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect database");
    let Ok(kv) = KeyValue::connect(&sg_redis::store_endpoint()).await else {
        eprintln!("cannot connect to REDIS_STORE_ENDPOINT");
        return;
    };

    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(Unified {
            site: SiteConfiguration::default(),
            service_connections: ServiceConnections::default(),
        })),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let job = sg_service_worker::jobs::licensecheck::LicenseCheckJob;
    println!("jjob description={:?}", job.description());
    println!("jconfig count={}", job.configs().len());
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 2, "SOURCEGRAPHDOTCOM_MODE must be unset");
    println!(
        "imax name={} description={:?} interval={}",
        routines[0].name(),
        routines[0].description(),
        routines[0].interval().as_secs()
    );
    println!(
        "icheck name={} description={:?} interval={}",
        routines[1].name(),
        routines[1].description(),
        routines[1].interval().as_secs()
    );

    restore_global_state(&pool).await;

    run_max_scenarios(&pool, &kv).await;
    run_check_scenarios(&pool, &kv).await;
}

async fn restore_global_state(pool: &sqlx::PgPool) {
    sqlx::query(
        "INSERT INTO global_state (site_id, initialized) \
         SELECT $1::uuid, true \
         WHERE NOT EXISTS (SELECT FROM global_state)",
    )
    .bind(SITE_ID)
    .execute(pool)
    .await
    .expect("restore global_state");
}

async fn clear_licensing_keys(kv: &KeyValue) {
    for key in [
        LICENSE_VALIDITY_STORE_KEY,
        LICENSE_INVALID_REASON,
        LAST_CALLED_AT_STORE_KEY,
        PREV_LICENSE_TOKEN_KEY,
        MAX_USERS_KEY,
        MAX_USERS_TIME_KEY,
    ] {
        let _ = kv.del(key).await;
    }
}

async fn hash_field(kv: &KeyValue, key: &str, field: &[u8]) -> String {
    match kv.hget_bytes(key, field).await {
        Ok(Some(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
        Ok(None) => "nil".to_owned(),
        Err(_) => "nil".to_owned(),
    }
}

async fn normalize_max_field(kv: &KeyValue, key: &str, field: &[u8]) -> String {
    normalize_max_time(&hash_field(kv, key, field).await)
}

/// A freshly written `2006-01-02 15:04:05 UTC` timestamp is printed as `now`.
fn normalize_max_time(value: &str) -> String {
    let Ok(items) = time::format_description::parse_borrowed::<2>(
        "[year]-[month]-[day] [hour]:[minute]:[second] UTC",
    ) else {
        return value.to_owned();
    };
    let Ok(parsed) = time::PrimitiveDateTime::parse(value, &items) else {
        return value.to_owned();
    };
    let delta = (OffsetDateTime::now_utc() - parsed.assume_utc()).whole_seconds();
    if delta.abs() < 600 {
        "now".to_owned()
    } else {
        value.to_owned()
    }
}

async fn key_state(kv: &KeyValue, key: &str) -> String {
    match kv.get_string(key).await {
        Ok(Some(value)) => format!("{value:?}"),
        _ => "nil".to_owned(),
    }
}

async fn set_state(kv: &KeyValue, key: &str) -> String {
    match kv.get_string(key).await {
        Ok(Some(_)) => "set".to_owned(),
        _ => "nil".to_owned(),
    }
}

fn error_class(error: Option<&str>) -> String {
    match error {
        None => "ok".to_owned(),
        Some(message) if message.starts_with("error reading global state from DB") => {
            "db-global-state".to_owned()
        }
        Some(message) => message.to_owned(),
    }
}

async fn run_max_scenarios(pool: &sqlx::PgPool, kv: &KeyValue) {
    struct Scenario {
        name: &'static str,
        step: Step,
        seed: &'static [(&'static str, &'static str)],
    }

    let scenarios = [
        Scenario {
            name: "initial",
            step: Step::ok(enterprise_info(), "sig-a"),
            seed: &[],
        },
        Scenario {
            name: "repeat",
            step: Step::ok(enterprise_info(), "sig-a"),
            seed: &[],
        },
        Scenario {
            name: "lower-existing",
            step: Step::ok(enterprise_info(), "sig-b"),
            seed: &[("sig-b", "5")],
        },
        Scenario {
            name: "empty-field",
            step: Step::ok(enterprise_info(), "sig-c"),
            seed: &[("sig-c", "")],
        },
        Scenario {
            name: "parse-error",
            step: Step::ok(enterprise_info(), "sig-d"),
            seed: &[("sig-d", "abc")],
        },
        Scenario {
            name: "no-signature",
            step: Step::ok(enterprise_info(), ""),
            seed: &[],
        },
        Scenario {
            name: "source-error",
            step: Step::err("bad license"),
            seed: &[],
        },
    ];

    for scenario in scenarios {
        clear_licensing_keys(kv).await;
        for (field, value) in scenario.seed {
            kv.hset(MAX_USERS_KEY, field, value)
                .await
                .expect("seed max");
            kv.hset(MAX_USERS_TIME_KEY, field, "2000-01-01 00:00:00 UTC")
                .await
                .expect("seed max time");
        }

        let license: Arc<dyn LicenseSource> = match &scenario.step.error {
            Some(error) => Arc::new(StaticLicenseSource::error(error.clone())),
            None => Arc::new(StaticLicenseSource::new(
                scenario.step.info.clone().expect("step info"),
                scenario.step.signature.clone(),
            )),
        };
        let handler = MaxUserCountHandler::new(pool.clone(), kv.clone(), license);
        let _ = handler.handle(CancellationToken::new()).await;

        let field = scenario.step.signature.clone();
        println!(
            "max scenario={} max={} maxtime={}",
            scenario.name,
            hash_field(kv, MAX_USERS_KEY, &field).await,
            normalize_max_field(kv, MAX_USERS_TIME_KEY, &field).await
        );
    }
}

async fn run_check_scenarios(pool: &sqlx::PgPool, kv: &KeyValue) {
    struct Scenario {
        name: &'static str,
        steps: Vec<Step>,
        license_key: &'static str,
        doer: Doer,
        seed: Vec<(&'static str, String)>,
        drop_global: bool,
    }

    enum Doer {
        Ok(u16, &'static str),
        Fail(&'static str),
    }

    fn scenario(name: &'static str, steps: Vec<Step>) -> Scenario {
        Scenario {
            name,
            steps,
            license_key: "",
            doer: Doer::Ok(200, "{}"),
            seed: Vec::new(),
            drop_global: false,
        }
    }

    let now = OffsetDateTime::now_utc();
    let rfc3339 = |time: OffsetDateTime| time.format(&Rfc3339).expect("format");
    let key1_token = sg_licensing::license_key_based_access_token("key1");
    let old_token = sg_licensing::license_key_based_access_token("old");

    let mut scenarios = vec![
        scenario("airgapped-plan", vec![Step::ok(air_gapped_info(), "")]),
        scenario(
            "airgapped-tag",
            vec![Step::ok(tagged_info("allow-air-gapped"), "")],
        ),
        scenario("dev", vec![Step::ok(tagged_info("dev"), "")]),
        scenario("internal", vec![Step::ok(tagged_info("internal"), "")]),
        scenario("free", vec![Step::ok(free_info(), "")]),
    ];

    let with_doer = |name, body: &'static str| {
        let mut sc = scenario(name, vec![Step::ok(enterprise_info(), "")]);
        sc.license_key = "key1";
        sc.doer = Doer::Ok(200, body);
        sc
    };
    scenarios.push(with_doer("valid", r#"{"data":{"is_valid":true}}"#));
    scenarios.push(with_doer(
        "invalid",
        r#"{"data":{"is_valid":false,"reason":"expired"}}"#,
    ));
    let mut status500 = scenario("status-500", vec![Step::ok(enterprise_info(), "")]);
    status500.license_key = "key1";
    status500.doer = Doer::Ok(500, "nope");
    scenarios.push(status500);
    scenarios.push(with_doer("body-error", r#"{"error":"license server sad"}"#));
    scenarios.push(with_doer("nil-data", "{}"));
    let mut doer_error = scenario("doer-error", vec![Step::ok(enterprise_info(), "")]);
    doer_error.license_key = "key1";
    doer_error.doer = Doer::Fail("network down");
    scenarios.push(doer_error);
    scenarios.push(scenario("source-error", vec![Step::err("bad license")]));
    scenarios.push(scenario(
        "second-call-error",
        vec![Step::ok(enterprise_info(), ""), Step::err("second call")],
    ));

    let mut recent = scenario("recent-skip", vec![Step::ok(enterprise_info(), "")]);
    recent.license_key = "key1";
    recent.seed = vec![
        (PREV_LICENSE_TOKEN_KEY, key1_token.clone()),
        (
            LAST_CALLED_AT_STORE_KEY,
            rfc3339(now - time::Duration::hours(1)),
        ),
    ];
    scenarios.push(recent);

    let mut due = scenario("due-after-13h", vec![Step::ok(enterprise_info(), "")]);
    due.license_key = "key1";
    due.doer = Doer::Ok(200, r#"{"data":{"is_valid":true}}"#);
    due.seed = vec![
        (PREV_LICENSE_TOKEN_KEY, key1_token.clone()),
        (
            LAST_CALLED_AT_STORE_KEY,
            rfc3339(now - time::Duration::hours(13)),
        ),
    ];
    scenarios.push(due);

    let mut changed = scenario("changed-key", vec![Step::ok(enterprise_info(), "")]);
    changed.license_key = "key1";
    changed.doer = Doer::Ok(200, r#"{"data":{"is_valid":true}}"#);
    changed.seed = vec![
        (PREV_LICENSE_TOKEN_KEY, old_token),
        (
            LAST_CALLED_AT_STORE_KEY,
            rfc3339(now - time::Duration::hours(1)),
        ),
    ];
    scenarios.push(changed);

    let mut validity_false = scenario("validity-false", vec![Step::ok(air_gapped_info(), "")]);
    validity_false.license_key = "key1";
    validity_false.doer = Doer::Ok(200, r#"{"data":{"is_valid":true}}"#);
    validity_false.seed = vec![(LICENSE_VALIDITY_STORE_KEY, "0".to_owned())];
    scenarios.push(validity_false);

    let mut db_missing = scenario("db-missing", vec![Step::ok(air_gapped_info(), "")]);
    db_missing.drop_global = true;
    scenarios.push(db_missing);

    for sc in scenarios {
        clear_licensing_keys(kv).await;
        for (key, value) in &sc.seed {
            kv.set(key, value).await.expect("seed key");
        }
        if sc.drop_global {
            sqlx::query("DELETE FROM global_state")
                .execute(pool)
                .await
                .expect("drop global_state");
        }

        let live_config = Arc::new(LiveConfig::new(Unified {
            site: SiteConfiguration::parse(&format!(r#"{{"licenseKey": "{}"}}"#, sc.license_key))
                .expect("site config"),
            service_connections: ServiceConnections::default(),
        }));

        let source = Arc::new(ScriptedSource::new(sc.steps));
        let doer = Arc::new(match &sc.doer {
            Doer::Ok(status, body) => ScriptedDoer::new(*status, body),
            Doer::Fail(error) => ScriptedDoer::failing(error),
        });
        let handler = LicenseCheckerHandler::with_script(
            pool.clone(),
            live_config,
            kv.clone(),
            doer.clone(),
            LICENSE_URL.to_owned(),
            source.clone(),
        );

        let result = handler.handle(CancellationToken::new()).await;
        let error = result
            .as_ref()
            .err()
            .map(|error| error.message().to_owned());

        println!(
            "check scenario={} err={} valid={} reason={} last={} prev={} calls={} reqs={}",
            sc.name,
            error_class(error.as_deref()),
            key_state(kv, LICENSE_VALIDITY_STORE_KEY).await,
            key_state(kv, LICENSE_INVALID_REASON).await,
            set_state(kv, LAST_CALLED_AT_STORE_KEY).await,
            key_state(kv, PREV_LICENSE_TOKEN_KEY).await,
            source.calls(),
            doer.requests().len()
        );
        for request in doer.requests() {
            println!("{request}");
        }

        if sc.drop_global {
            restore_global_state(pool).await;
        }
    }
}
