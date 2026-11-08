#![forbid(unsafe_code)]

//! M9.8aa differential harness (Rust side): the `export-usage-telemetry` job.
//!
//! Mirrors `cmd/worker/internal/telemetry/telemetry_harness_test.go`. Both
//! sides print the job/routine contract, the outcome and exported payload of
//! `TelemetryHandler::handle`, the `event_logs_scrape_state` rows it leaves
//! behind, and the queue-size gauges.
//!
//! `test_ws/p9-go/run_usage_telemetry_diff.sh` prepares the scratch database
//! (`event_logs`, `event_logs_export_allowlist`, `event_logs_scrape_state`,
//! `users`, `user_emails`, `global_state` fixtures) and compares the canonical
//! lines. Driver error wrappers (`pq: ` vs `error returned from database: `)
//! are normalized away; the Postgres message itself is kept.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_worker::framework::periodic::Handler;
use sg_service_worker::jobs::usage_telemetry::{
    marshal_events, BigQueryEvent, EventSender, ExportSettings, ExportUsageTelemetryJob,
    InstanceMetadata, QueueSizeHandler, TelemetryHandler, TopicConfig,
};
use sg_service_worker::jobs::{Job, JobDeps};
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio_util::sync::CancellationToken;

const SITE_ID: &str = "00000000-0000-4000-8000-000000000001";

fn normalize(error: Option<&str>) -> String {
    match error {
        None => "ok".to_owned(),
        Some(message) => message
            .replace("error returned from database: ", "")
            .replace("pq: ", ""),
    }
}

struct ScriptedSender {
    scenario: &'static str,
    fail: Option<String>,
    sends: AtomicUsize,
}

impl ScriptedSender {
    fn new(scenario: &'static str, fail: Option<&str>) -> Self {
        Self {
            scenario,
            fail: fail.map(str::to_owned),
            sends: AtomicUsize::new(0),
        }
    }

    fn sends(&self) -> usize {
        self.sends.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl EventSender for ScriptedSender {
    async fn send(
        &self,
        events: &[BigQueryEvent],
        _config: &TopicConfig,
        metadata: &InstanceMetadata,
    ) -> Result<(), String> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        println!(
            "payload scenario={} {}",
            self.scenario,
            marshal_events(events).expect("marshal payload")
        );
        println!(
            "meta scenario={} site_id={} license_key={} deploy_type={} version={} admin={}",
            self.scenario,
            metadata.site_id,
            metadata.license_key,
            metadata.deploy_type,
            metadata.version,
            metadata.initial_admin_email
        );
        match &self.fail {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}

fn live_config(batch_size: i64) -> Arc<LiveConfig> {
    let site = SiteConfiguration::parse(&format!(
        r#"{{"licenseKey":"license-key-1","exportUsageTelemetry":{{"batchSize":{batch_size}}}}}"#
    ))
    .expect("site config");
    Arc::new(LiveConfig::new(Unified {
        site,
        service_connections: ServiceConnections::default(),
    }))
}

fn settings(enabled: bool, topic: &str, project: &str) -> ExportSettings {
    ExportSettings {
        enabled,
        topic_name: topic.to_owned(),
        project_name: project.to_owned(),
        deploy_type: "test-deploy".to_owned(),
        version: "test-version".to_owned(),
    }
}

struct FixtureEvent {
    id: i32,
    name: &'static str,
    user_id: i32,
    anonymous_user_id: &'static str,
    public_argument: &'static str,
    version: &'static str,
    timestamp: &'static str,
    feature_flags: Option<&'static str>,
    cohort_id: Option<&'static str>,
    first_source_url: Option<&'static str>,
    last_source_url: Option<&'static str>,
    referrer: Option<&'static str>,
    device_id: Option<&'static str>,
    insert_id: Option<&'static str>,
}

fn fixture_events() -> Vec<FixtureEvent> {
    vec![
        FixtureEvent {
            id: 1,
            name: "allowed-a",
            user_id: 42,
            anonymous_user_id: "",
            public_argument: r#"{"foo": "bar"}"#,
            version: "1.2.3",
            timestamp: "2022-07-22T00:00:00Z",
            feature_flags: Some(r#"{"b": true, "a": false}"#),
            cohort_id: Some("2022-01-02"),
            first_source_url: Some("https://first.example"),
            last_source_url: Some("https://last.example"),
            referrer: Some("ref"),
            device_id: Some("device-1"),
            insert_id: Some("insert-1"),
        },
        FixtureEvent {
            id: 2,
            name: "blocked",
            user_id: 7,
            anonymous_user_id: "",
            public_argument: "{}",
            version: "0.0.1",
            timestamp: "2022-07-22T01:00:00Z",
            feature_flags: None,
            cohort_id: None,
            first_source_url: Some(""),
            last_source_url: Some(""),
            referrer: Some(""),
            device_id: None,
            insert_id: None,
        },
        FixtureEvent {
            id: 3,
            name: "allowed-b",
            user_id: 0,
            anonymous_user_id: "anon-3",
            public_argument: r#"{"nested": {"x": 1}}"#,
            version: "2.0.0",
            timestamp: "2023-01-01T12:34:56Z",
            feature_flags: Some("{}"),
            cohort_id: None,
            first_source_url: None,
            last_source_url: None,
            referrer: None,
            device_id: Some("device-3"),
            insert_id: Some("insert-3"),
        },
        FixtureEvent {
            id: 4,
            name: "allowed-c",
            user_id: 99,
            anonymous_user_id: "",
            public_argument: r#"{"html": "<b>&</b>"}"#,
            version: "3.1.4",
            timestamp: "2023-06-15T08:00:00.250Z",
            feature_flags: Some(r#"{"z": true, "y": true}"#),
            cohort_id: Some("2023-06-15"),
            first_source_url: None,
            last_source_url: Some("https://last.example/2"),
            referrer: Some(""),
            device_id: None,
            insert_id: None,
        },
        FixtureEvent {
            id: 5,
            name: "allowed-d",
            user_id: 3,
            anonymous_user_id: "",
            public_argument: "{}",
            version: "0.0.0+dev",
            timestamp: "2024-02-29T23:59:59Z",
            feature_flags: None,
            cohort_id: None,
            first_source_url: None,
            last_source_url: None,
            referrer: None,
            device_id: None,
            insert_id: None,
        },
    ]
}

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect(query);
}

async fn reset_tables(pool: &PgPool) {
    exec(pool, "DELETE FROM event_logs").await;
    exec(pool, "DELETE FROM event_logs_scrape_state").await;
    exec(pool, "DELETE FROM event_logs_export_allowlist").await;
    exec(
        pool,
        "DELETE FROM user_emails WHERE user_id BETWEEN 9801 AND 9899",
    )
    .await;
    exec(pool, "DELETE FROM users WHERE id BETWEEN 9801 AND 9899").await;
    exec(pool, "DELETE FROM global_state").await;
}

async fn seed_base(pool: &PgPool) {
    exec(
        pool,
        "INSERT INTO users (id, username, site_admin) VALUES \
         (9801, 'ut-admin', true), (9802, 'ut-member', false)",
    )
    .await;
    exec(
        pool,
        "INSERT INTO user_emails (user_id, email, is_primary) VALUES \
         (9801, 'admin@example.com', true), \
         (9801, 'old-admin@example.com', false), \
         (9802, 'member@example.com', true)",
    )
    .await;
    sqlx::query("INSERT INTO global_state (site_id, initialized) VALUES ($1::uuid, true)")
        .bind(SITE_ID)
        .execute(pool)
        .await
        .expect("global_state");
}

async fn seed_events(pool: &PgPool) {
    for event in fixture_events() {
        sqlx::query(
            "INSERT INTO event_logs \
                (id, name, url, user_id, anonymous_user_id, source, argument, \
                 public_argument, version, timestamp, feature_flags, cohort_id, \
                 first_source_url, last_source_url, referrer, device_id, insert_id) \
             VALUES ($1, $2, 'https://sourcegraph.example/search', $3, $4, 'test', \
                 '{}', $5::jsonb, $6, $7::timestamptz, $8::jsonb, $9::date, \
                 $10, $11, $12, $13, $14)",
        )
        .bind(event.id)
        .bind(event.name)
        .bind(event.user_id)
        .bind(event.anonymous_user_id)
        .bind(event.public_argument)
        .bind(event.version)
        .bind(event.timestamp)
        .bind(event.feature_flags)
        .bind(event.cohort_id)
        .bind(event.first_source_url)
        .bind(event.last_source_url)
        .bind(event.referrer)
        .bind(event.device_id)
        .bind(event.insert_id)
        .execute(pool)
        .await
        .expect("insert event");
    }
    exec(pool, "SELECT setval('event_logs_id_seq', 5)").await;
}

async fn seed_allowlist(pool: &PgPool, names: &[&str]) {
    for name in names {
        sqlx::query("INSERT INTO event_logs_export_allowlist (event_name) VALUES ($1)")
            .bind(name)
            .execute(pool)
            .await
            .expect("allowlist");
    }
}

async fn set_scrape(pool: &PgPool, bookmarks: &[i32]) {
    for bookmark in bookmarks {
        sqlx::query("INSERT INTO event_logs_scrape_state (bookmark_id) VALUES ($1)")
            .bind(bookmark)
            .execute(pool)
            .await
            .expect("scrape state");
    }
}

async fn read_bookmarks(pool: &PgPool) -> Vec<i32> {
    sqlx::query_scalar("SELECT bookmark_id FROM event_logs_scrape_state ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read bookmarks")
}

fn bookmarks_string(values: &[i32]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn gauge_value(name: &str) -> f64 {
    for family in sg_metrics::registry().gather() {
        if family.get_name() == name {
            if let Some(metric) = family.get_metric().first() {
                return metric.get_gauge().get_value();
            }
        }
    }
    panic!("metric {name} is not registered");
}

#[tokio::test]
async fn usage_telemetry_harness() {
    if std::env::var("SG_TELEMETRY_HARNESS_ON").as_deref() != Ok("1") {
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

    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: live_config(1000),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };

    // -- contract -------------------------------------------------------
    let job = ExportUsageTelemetryJob::with_settings(settings(true, "topic-name", "project-name"));
    println!("jjob description={:?}", job.description());
    println!("jconfig count={}", job.configs().len());

    let disabled = ExportUsageTelemetryJob::with_settings(settings(false, "", ""));
    let routines = disabled
        .routines(&deps, CancellationToken::new())
        .await
        .expect("disabled routines");
    println!("jroutines disabled={}", routines.len());

    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 2, "enabled job has two routines");
    for routine in &routines {
        println!(
            "iroutine name={} description={:?} interval={}",
            routine.name(),
            routine.description(),
            routine.interval().as_secs()
        );
    }

    // -- handler scenarios ---------------------------------------------
    struct Scenario {
        name: &'static str,
        enabled: bool,
        topic: &'static str,
        project: &'static str,
        batch_size: i64,
        fail_send: Option<&'static str>,
    }

    fn sc(
        name: &'static str,
        enabled: bool,
        topic: &'static str,
        project: &'static str,
        batch_size: i64,
        fail_send: Option<&'static str>,
    ) -> Scenario {
        Scenario {
            name,
            enabled,
            topic,
            project,
            batch_size,
            fail_send,
        }
    }

    let scenarios = [
        sc("disabled", false, "topic-name", "project-name", 1000, None),
        sc("missing-topic", true, "", "project-name", 1000, None),
        sc("missing-project", true, "topic-name", "", 1000, None),
        sc(
            "seed-bookmark",
            true,
            "topic-name",
            "project-name",
            1000,
            None,
        ),
        sc("no-events", true, "topic-name", "project-name", 1000, None),
        sc(
            "allowlist-empty",
            true,
            "topic-name",
            "project-name",
            1000,
            None,
        ),
        sc("batch-all", true, "topic-name", "project-name", 1000, None),
        sc("batch-size-1", true, "topic-name", "project-name", 1, None),
        sc("second-batch", true, "topic-name", "project-name", 1, None),
        sc(
            "no-new-events",
            true,
            "topic-name",
            "project-name",
            1000,
            None,
        ),
        sc(
            "send-error",
            true,
            "topic-name",
            "project-name",
            1000,
            Some("telemetry backend down"),
        ),
        sc(
            "multi-row-bookmark",
            true,
            "topic-name",
            "project-name",
            1000,
            None,
        ),
        sc(
            "empty-tables",
            true,
            "topic-name",
            "project-name",
            1000,
            None,
        ),
        sc("no-admin", true, "topic-name", "project-name", 1000, None),
        sc(
            "uninitialized",
            true,
            "topic-name",
            "project-name",
            1000,
            None,
        ),
    ];

    for scenario in &scenarios {
        match scenario.name {
            "disabled" | "missing-topic" | "missing-project" => reset_tables(&pool).await,
            "seed-bookmark" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
                seed_events(&pool).await;
                seed_allowlist(&pool, &["allowed-a", "allowed-b", "allowed-c", "allowed-d"]).await;
            }
            "no-events" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
                seed_events(&pool).await;
                seed_allowlist(&pool, &["allowed-a"]).await;
                set_scrape(&pool, &[5]).await;
            }
            "allowlist-empty" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
                seed_events(&pool).await;
                set_scrape(&pool, &[0]).await;
            }
            "batch-all" | "batch-size-1" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
                seed_events(&pool).await;
                seed_allowlist(&pool, &["allowed-a", "allowed-b", "allowed-c", "allowed-d"]).await;
                set_scrape(&pool, &[0]).await;
            }
            "second-batch" => {}
            "no-new-events" => {
                exec(&pool, "UPDATE event_logs_scrape_state SET bookmark_id = 5").await;
            }
            "send-error" => {
                exec(&pool, "UPDATE event_logs_scrape_state SET bookmark_id = 0").await;
            }
            "multi-row-bookmark" => {
                exec(&pool, "DELETE FROM event_logs_scrape_state").await;
                set_scrape(&pool, &[4, 1]).await;
                exec(&pool, "SELECT setval('event_logs_scrape_state_id_seq', 20)").await;
            }
            "empty-tables" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
            }
            "no-admin" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
                exec(&pool, "UPDATE users SET site_admin = false WHERE id = 9801").await;
                seed_events(&pool).await;
                seed_allowlist(&pool, &["allowed-a"]).await;
                set_scrape(&pool, &[0]).await;
            }
            "uninitialized" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
                exec(&pool, "UPDATE global_state SET initialized = false").await;
                seed_events(&pool).await;
                seed_allowlist(&pool, &["allowed-a", "allowed-b", "allowed-c", "allowed-d"]).await;
                set_scrape(&pool, &[0]).await;
            }
            other => panic!("unknown scenario {other}"),
        }

        let live_config = live_config(scenario.batch_size);
        let sender = Arc::new(ScriptedSender::new(scenario.name, scenario.fail_send));
        let handler = TelemetryHandler::new(
            pool.clone(),
            live_config,
            sender.clone(),
            settings(scenario.enabled, scenario.topic, scenario.project),
        );
        let result = handler.handle(CancellationToken::new()).await;

        let bookmarks = read_bookmarks(&pool).await;
        println!(
            "handle scenario={} err={} sends={} bookmarks={} rows={}",
            scenario.name,
            normalize(result.as_ref().err().map(|error| error.message())),
            sender.sends(),
            bookmarks_string(&bookmarks),
            bookmarks.len()
        );
    }

    // -- queue size handler --------------------------------------------
    struct QueueScenario {
        name: &'static str,
        batch_size: i64,
    }

    let queue_scenarios = [
        QueueScenario {
            name: "default",
            batch_size: 1000,
        },
        QueueScenario {
            name: "batch-two",
            batch_size: 2,
        },
        QueueScenario {
            name: "empty-tables",
            batch_size: 1000,
        },
    ];

    for scenario in &queue_scenarios {
        match scenario.name {
            "default" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
                seed_events(&pool).await;
                seed_allowlist(&pool, &["allowed-a", "allowed-b", "allowed-c", "allowed-d"]).await;
                set_scrape(&pool, &[1]).await;
            }
            "batch-two" => {}
            "empty-tables" => {
                reset_tables(&pool).await;
                seed_base(&pool).await;
            }
            other => panic!("unknown queue scenario {other}"),
        }

        let handler = QueueSizeHandler::new(pool.clone(), live_config(scenario.batch_size));
        let result = handler.handle(CancellationToken::new()).await;
        match result {
            Ok(()) => println!(
                "queue scenario={} err=ok size={} throughput={:.6}",
                scenario.name,
                gauge_value("src_telemetry_job_queue_size_total") as i64,
                gauge_value("src_telemetry_job_max_throughput")
            ),
            Err(error) => println!(
                "queue scenario={} err={} size=- throughput=-",
                scenario.name,
                normalize(Some(error.message()))
            ),
        }
    }
}
