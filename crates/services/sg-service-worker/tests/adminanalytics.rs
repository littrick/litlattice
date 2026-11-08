#![forbid(unsafe_code)]

//! M9.8ab differential harness (Rust side): the `refresh-analytics-cache` job.
//!
//! Mirrors `cmd/worker/internal/adminanalytics/adminanalytics_harness_test.go`.
//! Both sides print the job/routine contract, every cache access made by the
//! refresh (key, hit/miss, `SETEX` ttl + value), the two `NoopCache` groups'
//! results, and an empty-fixture pass.
//!
//! `test_ws/p9-go/run_analytics_diff.sh` prepares the scratch database and
//! compares the canonical lines. The environment must be `TZ=UTC`: the Go
//! baseline formats `time.Now()` in the local zone.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_worker::jobs::adminanalytics::{
    make_event_logs_queries, marshal_go, refresh_analytics_cache, AnalyticsCache, AnalyticsFetcher,
    NoopCache, RefreshAnalyticsCacheJob, DAILY, LAST_WEEK,
};
use sg_service_worker::jobs::{Job, JobDeps};
use sqlx::postgres::{PgPool, PgPoolOptions};
use time::format_description::well_known::Rfc3339;
use time::Duration as TimeDuration;
use time::{OffsetDateTime, Time};
use tokio_util::sync::CancellationToken;

struct RecordingCache {
    scenario: String,
    values: Mutex<HashMap<String, String>>,
}

impl RecordingCache {
    fn new(scenario: &str) -> Self {
        Self {
            scenario: scenario.to_owned(),
            values: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl AnalyticsCache for RecordingCache {
    async fn get(&self, key: &str) -> Option<String> {
        let values = self.values.lock().expect("cache lock");
        match values.get(key) {
            None => {
                println!("get scenario={} key={} hit=0", self.scenario, key);
                None
            }
            Some(value) => {
                println!("get scenario={} key={} hit=1", self.scenario, key);
                Some(value.clone())
            }
        }
    }

    async fn set_ex(&self, key: &str, ttl_seconds: i64, value: &str) -> Result<(), String> {
        println!(
            "set scenario={} key={} ttl={} value={}",
            self.scenario, key, ttl_seconds, value
        );
        self.values
            .lock()
            .expect("cache lock")
            .insert(key.to_owned(), value.to_owned());
        Ok(())
    }
}

struct FixtureEvent {
    id: i64,
    name: &'static str,
    user_id: i32,
    anonymous_user_id: &'static str,
    source: &'static str,
    argument: &'static str,
    public_argument: &'static str,
    referrer: &'static str,
    days_ago: i64,
}

fn fixture_events() -> Vec<FixtureEvent> {
    vec![
        FixtureEvent {
            id: 970001,
            name: "SearchResultsQueried",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 2,
        },
        FixtureEvent {
            id: 970002,
            name: "SearchResultsQueried",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 10,
        },
        FixtureEvent {
            id: 970003,
            name: "SearchResultsQueried",
            user_id: 0,
            anonymous_user_id: "anon-1",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 40,
        },
        FixtureEvent {
            id: 970004,
            name: "ViewBlob",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 2,
        },
        FixtureEvent {
            id: 970005,
            name: "CodeCopied",
            user_id: 9704,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 5,
        },
        FixtureEvent {
            id: 970006,
            name: "SearchResultClicked",
            user_id: 9704,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 3,
        },
        FixtureEvent {
            id: 970007,
            name: "SearchNotebookCreated",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 6,
        },
        FixtureEvent {
            id: 970008,
            name: "SearchNotebookPageViewed",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 7,
        },
        FixtureEvent {
            id: 970009,
            name: "SearchNotebookRunAllBlocks",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 3,
        },
        FixtureEvent {
            id: 970010,
            name: "SearchNotebookRunBlock",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 11,
        },
        FixtureEvent {
            id: 970011,
            name: "goToDefinition.preloaded",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: r#"{"repositoryId": "97001", "languageId": "rust"}"#,
            public_argument: "{}",
            referrer: "",
            days_ago: 2,
        },
        FixtureEvent {
            id: 970012,
            name: "goToDefinition",
            user_id: 9704,
            anonymous_user_id: "",
            source: "CODEHOSTINTEGRATION",
            argument: r#"{"repositoryId": "97001", "languageId": "rust"}"#,
            public_argument: "{}",
            referrer: "",
            days_ago: 4,
        },
        FixtureEvent {
            id: 970013,
            name: "findReferences",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: r#"{"repositoryId": "97002", "languageId": "go"}"#,
            public_argument: "{}",
            referrer: "",
            days_ago: 3,
        },
        FixtureEvent {
            id: 970014,
            name: "codeintel.searchDefinitions",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: r#"{"repositoryId": "97001", "languageId": "rust"}"#,
            public_argument: "{}",
            referrer: "",
            days_ago: 8,
        },
        FixtureEvent {
            id: 970015,
            name: "codeintel.searchReferences.xrepo",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: r#"{"repositoryId": "97002", "languageId": "go"}"#,
            public_argument: "{}",
            referrer: "",
            days_ago: 20,
        },
        FixtureEvent {
            id: 970016,
            name: "codeintel.lsifDefinitions.xrepo",
            user_id: 9704,
            anonymous_user_id: "",
            source: "WEB",
            argument: r#"{"repositoryId": "97001", "languageId": "rust"}"#,
            public_argument: "{}",
            referrer: "",
            days_ago: 25,
        },
        FixtureEvent {
            id: 970017,
            name: "codeintel.lsifReferences",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: r#"{"repositoryId": "97002", "languageId": "go"}"#,
            public_argument: "{}",
            referrer: "",
            days_ago: 30,
        },
        FixtureEvent {
            id: 970018,
            name: "InsightHover",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 2,
        },
        FixtureEvent {
            id: 970019,
            name: "InsightDataPointClick",
            user_id: 9704,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 9,
        },
        FixtureEvent {
            id: 970020,
            name: "IDESearchSubmitted",
            user_id: 9701,
            anonymous_user_id: "",
            source: "IDEEXTENSION",
            argument: "{}",
            public_argument: "{}",
            referrer: "JETBRAINS",
            days_ago: 3,
        },
        FixtureEvent {
            id: 970021,
            name: "VSCESearchSubmitted",
            user_id: 9704,
            anonymous_user_id: "",
            source: "IDEEXTENSION",
            argument: "{}",
            public_argument: "{}",
            referrer: "VSCE",
            days_ago: 4,
        },
        // Excluded rows: each exercises one `BuildCommonUsageConds` branch.
        FixtureEvent {
            id: 970022,
            name: "ViewSignIn",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 2,
        },
        FixtureEvent {
            id: 970023,
            name: "SearchResultsQueried",
            user_id: 9702,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 2,
        },
        FixtureEvent {
            id: 970024,
            name: "SearchResultsQueried",
            user_id: 9703,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 3,
        },
        FixtureEvent {
            id: 970025,
            name: "SearchResultsQueried",
            user_id: 0,
            anonymous_user_id: "backend",
            source: "WEB",
            argument: "{}",
            public_argument: "{}",
            referrer: "",
            days_ago: 3,
        },
        FixtureEvent {
            id: 970026,
            name: "SearchResultsQueried",
            user_id: 9701,
            anonymous_user_id: "",
            source: "WEB",
            argument: "{}",
            public_argument: r#"{"sourcegraph_operator": true}"#,
            referrer: "",
            days_ago: 4,
        },
    ]
}

fn timestamp(days_ago: i64) -> String {
    let now = OffsetDateTime::now_utc();
    let base = now
        .replace_time(Time::from_hms(12, 0, 0).expect("valid time"))
        .replace_nanosecond(0)
        .expect("valid nanosecond");
    (base - TimeDuration::days(days_ago))
        .format(&Rfc3339)
        .expect("RFC3339")
}

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect(query);
}

async fn reset_tables(pool: &PgPool) {
    for query in [
        "DELETE FROM changeset_events WHERE id BETWEEN 970001 AND 970099",
        "DELETE FROM changesets WHERE id BETWEEN 970001 AND 970099",
        "DELETE FROM batch_changes WHERE id BETWEEN 970001 AND 970099",
        "DELETE FROM batch_specs WHERE id BETWEEN 970001 AND 970099",
        "DELETE FROM lsif_uploads WHERE repository_id BETWEEN 97001 AND 97099",
        "DELETE FROM repo WHERE id BETWEEN 97001 AND 97099",
        "DELETE FROM event_logs WHERE id BETWEEN 970001 AND 970099",
        "DELETE FROM user_emails WHERE user_id BETWEEN 9701 AND 9799",
        "DELETE FROM users WHERE id BETWEEN 9701 AND 9799",
    ] {
        exec(pool, query).await;
    }
}

async fn seed_base(pool: &PgPool) {
    exec(
        pool,
        "INSERT INTO users (id, username, site_admin) VALUES \
         (9701, 'aa-normal', false), (9702, 'sourcegraph-admin', true), \
         (9703, 'managed-alice', false), (9704, 'aa-member2', false)",
    )
    .await;
    exec(
        pool,
        "INSERT INTO user_emails (user_id, email, is_primary) VALUES \
         (9701, 'normal@example.com', true), (9702, 'admin@sourcegraph.com', true), \
         (9703, 'alice@sourcegraph.com', true), (9704, 'm2@example.com', true)",
    )
    .await;
}

async fn seed_events(pool: &PgPool) {
    for event in fixture_events() {
        sqlx::query(
            "INSERT INTO event_logs \
                (id, name, url, user_id, anonymous_user_id, source, argument, \
                 public_argument, version, timestamp, referrer) \
             VALUES ($1, $2, 'https://sourcegraph.example/search', $3, $4, $5, \
                 $6::jsonb, $7::jsonb, '0.0.0+test', $8::timestamptz, $9)",
        )
        .bind(event.id)
        .bind(event.name)
        .bind(event.user_id)
        .bind(event.anonymous_user_id)
        .bind(event.source)
        .bind(event.argument)
        .bind(event.public_argument)
        .bind(timestamp(event.days_ago))
        .bind(event.referrer)
        .execute(pool)
        .await
        .expect("insert event");
    }
}

async fn seed_repos(pool: &PgPool) {
    exec(
        pool,
        "INSERT INTO repo (id, name) VALUES \
         (97001, 'github.com/example/aa-rust'), (97002, 'github.com/example/aa-go')",
    )
    .await;
    exec(
        pool,
        "INSERT INTO lsif_uploads \
            (id, commit, repository_id, indexer, num_parts, uploaded_parts, state) \
         VALUES (970001, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 97001, 'scip-rust', 1, '{0}', 'completed')",
    )
    .await;
    exec(
        pool,
        "INSERT INTO lsif_uploads \
            (id, commit, repository_id, indexer, num_parts, uploaded_parts, state) \
         VALUES (970002, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 97002, 'scip-go', 1, '{0}', 'queued')",
    )
    .await;
}

async fn seed_batch_changes(pool: &PgPool) {
    exec(
        pool,
        "INSERT INTO batch_specs (id, rand_id, raw_spec, namespace_user_id) VALUES (970001, 'aa-rand-0001', '{}'::jsonb, 9701)",
    )
    .await;
    exec(
        pool,
        "INSERT INTO batch_changes (id, name, creator_id, namespace_user_id, batch_spec_id) \
         VALUES (970001, 'aa-batch', 9701, 9701, 970001)",
    )
    .await;
    sqlx::query(
        "INSERT INTO changesets \
            (id, repo_id, external_service_type, computed_state, publication_state, \
             created_at, owned_by_batch_change_id) \
         VALUES (970001, 97001, 'github', 'open', 'PUBLISHED', $1::timestamptz, 970001), \
                (970002, 97002, 'github', 'open', 'DRAFT', $2::timestamptz, 970001)",
    )
    .bind(timestamp(5))
    .bind(timestamp(6))
    .execute(pool)
    .await
    .expect("changesets");
    sqlx::query(
        "INSERT INTO changeset_events (id, changeset_id, kind, key, created_at) \
         VALUES (970001, 970001, 'github:merged', 'merge', $1::timestamptz), \
                (970002, 970001, 'github:opened', 'open', $2::timestamptz)",
    )
    .bind(timestamp(4))
    .bind(timestamp(5))
    .execute(pool)
    .await
    .expect("changeset events");
}

async fn seed_all(pool: &PgPool) {
    reset_tables(pool).await;
    seed_base(pool).await;
    seed_events(pool).await;
    seed_repos(pool).await;
    seed_batch_changes(pool).await;
}

#[tokio::test]
async fn adminanalytics_harness() {
    if std::env::var("SG_ANALYTICS_HARNESS_ON").as_deref() != Ok("1") {
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
        live_config: Arc::new(LiveConfig::new(Unified {
            site: SiteConfiguration::parse("{}").expect("site config"),
            service_connections: ServiceConnections::default(),
        })),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };

    // -- contract -------------------------------------------------------
    let job = RefreshAnalyticsCacheJob;
    println!("jjob description={:?}", job.description());
    println!("jconfig count={}", job.configs().len());

    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    for routine in &routines {
        println!(
            "iroutine name={} description={:?} interval={}",
            routine.name(),
            routine.description(),
            routine.interval().as_secs()
        );
    }

    // -- passes ---------------------------------------------------------
    seed_all(&pool).await;
    let full = RecordingCache::new("full");
    match refresh_analytics_cache(&pool, &full).await {
        Ok(()) => println!("refresh scenario=full err=ok"),
        Err(error) => println!("refresh scenario=full err={}", normalize(&error)),
    }
    match refresh_analytics_cache(&pool, &full).await {
        Ok(()) => println!("refresh scenario=full-hit err=ok"),
        Err(error) => println!("refresh scenario=full-hit err={}", normalize(&error)),
    }

    // `NoopCache` groups never reach the recording cache; exercise their
    // queries directly so the diff still covers them.
    let noop = NoopCache;
    let checks = [
        (
            "Search:ResultClicked",
            make_event_logs_queries(LAST_WEEK, DAILY, &["SearchResultClicked"], &[]),
        ),
        (
            "Extensions:Jetbrains",
            make_event_logs_queries(
                LAST_WEEK,
                DAILY,
                &["IDESearchSubmitted", "VSCESearchSubmitted"],
                &["source = 'IDEEXTENSION' AND referrer = 'JETBRAINS'"],
            ),
        ),
    ];
    for (group, queries) in checks {
        match queries {
            Err(error) => println!("noop scenario=full group={group} err={}", normalize(&error)),
            Ok((nodes_query, summary_query)) => {
                let fetcher = AnalyticsFetcher::new(
                    &pool,
                    &noop,
                    group,
                    LAST_WEEK,
                    DAILY,
                    nodes_query,
                    summary_query,
                );
                match (fetcher.nodes().await, fetcher.summary().await) {
                    (Ok(nodes), Ok(summary)) => {
                        println!(
                            "noop scenario=full group={} nodes={} summary={}",
                            group,
                            marshal_go(&nodes).expect("nodes json"),
                            marshal_go(&summary).expect("summary json")
                        );
                    }
                    (Err(error), _) | (_, Err(error)) => {
                        println!("noop scenario=full group={group} err={}", normalize(&error))
                    }
                }
            }
        }
    }

    // -- empty fixtures -------------------------------------------------
    reset_tables(&pool).await;
    let empty = RecordingCache::new("empty");
    match refresh_analytics_cache(&pool, &empty).await {
        Ok(()) => println!("refresh scenario=empty err=ok"),
        Err(error) => println!("refresh scenario=empty err={}", normalize(&error)),
    }
}

fn normalize(error: &str) -> String {
    error
        .replace("error returned from database: ", "")
        .replace("pq: ", "")
}
