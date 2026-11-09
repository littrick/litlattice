#![forbid(unsafe_code)]

//! M9.8bj differential harness (Rust side): the `bitbucket-project-permissions`
//! worker job, the `bitbucketserver` `ProjectRepos` subset and the
//! `MapUsers` permissions-store lookup.
//
// Mirrors `cmd/worker/internal/permissions/bitbucket_projects_harness_test.go`;
// outputs are compared line for line by
// `test_ws/p9-go/run_bitbucket_project_permissions_diff.sh`. Both harnesses
// seed the same fixtures (repo 9100-9199, external services 9500-9599, users
// 9800-9899, jobs 9700-9799) into a scratch copy of the frontend schema. The
// scripted Bitbucket Server API routes by project key, the base path
// (`/bitbucket`) is exercised by the `base-path` scenario, the server URL is
// normalised to `SERVER`, newlines inside error values are escaped and the
// JSON decoder text is masked (`encoding/json` vs `serde_json`, docs §74.3).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sg_config::{LiveConfig, SiteConfiguration, Unified};
use sg_core::EnvReader;
use sg_db::connect;
use sg_http::{serve_http, HttpHandler};
use sg_keyring::Keyring;
use sg_service_worker::framework::dbworker::{DbResetter, DbWorkerHandler, DbWorkerStore};
use sg_service_worker::jobs::bitbucket_project_permissions::{
    is_non_retryable, BitbucketProjectPermissionJob, BitbucketProjectPermissionsConfig,
    BitbucketProjectPermissionsHandler, BitbucketProjectPermissionsStore, UserPermission,
    CONCURRENCY_VAR, POLL_INTERVAL_VAR, RETRY_INTERVAL_VAR,
};
use sg_service_worker::jobs::JobConfig;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

const SOURCEGRAPH_SERVICE_ID: &str = "https://sourcegraph.com/";

// ---------------------------------------------------------------------------
// Scripted Bitbucket Server API

fn route(path: &str, query: &str) -> (u16, &'static str) {
    let Some(index) = path.find("/rest/api/1.0/projects/") else {
        return (404, "not found");
    };
    let rest = &path[index + "/rest/api/1.0/projects/".len()..];
    let Some(key) = rest.strip_suffix("/repos") else {
        return (404, "not found");
    };
    match key {
        "EMAIL" => (
            200,
            r#"{"isLastPage":true,"limit":1000,"start":0,"values":[{"id":11},{"id":12},{"id":13},{"id":14}]}"#,
        ),
        "USERNAME" | "UNRESTRICTED" => (
            200,
            r#"{"isLastPage":true,"limit":1000,"start":0,"values":[{"id":11},{"id":12}]}"#,
        ),
        "PAGE" => {
            if query.contains("start=2") {
                (
                    200,
                    r#"{"isLastPage":true,"limit":1000,"start":2,"values":[{"id":12}]}"#,
                )
            } else {
                (
                    200,
                    r#"{"isLastPage":false,"limit":1000,"start":0,"nextPageStart":2,"values":[{"id":11}]}"#,
                )
            }
        }
        "E401" => (401, "nope"),
        "E500" => (500, "boom"),
        "BADJSON" => (200, "x"),
        "EMPTY" => (
            200,
            r#"{"isLastPage":true,"limit":1000,"start":0,"values":[]}"#,
        ),
        "PAD" | "INVALIDMAP" | "DISABLEDUSER" => (
            200,
            r#"{"isLastPage":true,"limit":1000,"start":0,"values":[{"id":11}]}"#,
        ),
        "BASEPATH" => (
            200,
            r#"{"isLastPage":true,"limit":1000,"start":0,"values":[{"id":17}]}"#,
        ),
        _ => (404, "not found"),
    }
}

fn response(status: u16, body: &str) -> http::Response<tonic::body::BoxBody> {
    http::Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(tonic::body::boxed(http_body_util::Full::new(
            hyper::body::Bytes::from(body.to_owned()),
        )))
        .expect("response")
}

async fn start_server(records: Arc<Mutex<Vec<String>>>) -> (String, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted server");
    let addr = listener.local_addr().expect("server addr");
    let base = format!("http://{addr}");
    let http: HttpHandler = Arc::new(move |req: http::Request<hyper::body::Incoming>| {
        let records = Arc::clone(&records);
        Box::pin(async move {
            let target = req
                .uri()
                .path_and_query()
                .map(|path| path.as_str())
                .unwrap_or("")
                .to_owned();
            let auth = req
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
                .unwrap_or("<none>")
                .to_owned();
            records
                .lock()
                .expect("lock")
                .push(format!("{} {target} auth={auth}", req.method()));
            let (path, query) = target.split_once('?').unwrap_or((&target, ""));
            let (status, body) = route(path, query);
            response(status, body)
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(serve_http(listener, http, shutdown.clone()));
    (base, shutdown)
}

// ---------------------------------------------------------------------------
// Fixtures

async fn clean_permissions(pool: &PgPool) -> Result<(), sqlx::Error> {
    for query in [
        "DELETE FROM user_pending_permissions WHERE service_type = 'sourcegraph' AND service_id = 'https://sourcegraph.com/' AND object_type = 'repos'",
        "DELETE FROM repo_pending_permissions WHERE repo_id BETWEEN 9100 AND 9199",
        "DELETE FROM user_repo_permissions WHERE repo_id BETWEEN 9100 AND 9199",
        "DELETE FROM repo_permissions WHERE repo_id BETWEEN 9100 AND 9199",
        "DELETE FROM explicit_permissions_bitbucket_projects_jobs WHERE id BETWEEN 9700 AND 9799",
        // `repo_pending_permissions.user_ids_ints` stores
        // `user_pending_permissions.id`; restart the sequence so both
        // harnesses see the same values on every run.
        "ALTER SEQUENCE user_pending_permissions_id_seq RESTART WITH 1",
    ] {
        sqlx::query(query).execute(pool).await?;
    }
    Ok(())
}

async fn clean_fixtures(pool: &PgPool) -> Result<(), sqlx::Error> {
    clean_permissions(pool).await?;
    for query in [
        "DELETE FROM user_emails WHERE user_id BETWEEN 9800 AND 9899",
        "DELETE FROM users WHERE id BETWEEN 9800 AND 9899",
        "DELETE FROM repo WHERE id BETWEEN 9100 AND 9199",
        "DELETE FROM external_services WHERE id BETWEEN 9500 AND 9599",
    ] {
        sqlx::query(query).execute(pool).await?;
    }
    Ok(())
}

async fn seed(pool: &PgPool, base: &str) -> Result<(), sqlx::Error> {
    let normalized = format!("{base}/");
    sqlx::query(
        r#"INSERT INTO external_services (id, kind, display_name, config) VALUES
            (9500, 'BITBUCKETSERVER', 'bbs', $1),
            (9501, 'BITBUCKETSERVER', 'bbs-bad-config', 'not json'),
            (9502, 'GITHUB', 'github', $2),
            (9503, 'BITBUCKETSERVER', 'bbs-oauth1', $3),
            (9504, 'BITBUCKETSERVER', 'bbs-base', $4)"#,
    )
    .bind(format!(r#"{{"url":"{normalized}","token":"tok"}}"#))
    .bind(format!(r#"{{"url":"{normalized}","token":"tok"}}"#))
    .bind(format!(
        r#"{{"url":"{normalized}","authorization":{{"oauth":{{"consumerKey":"ck","signingKey":"not-base64!!"}}}}}}"#
    ))
    .bind(format!(r#"{{"url":"{base}/bitbucket"}}"#))
    .execute(pool)
    .await?;

    #[allow(clippy::too_many_arguments)] // mirrors the fixture columns
    async fn repo(
        pool: &PgPool,
        id: i32,
        external_id: &str,
        service_type: &str,
        service_id: &str,
        private: bool,
        deleted_at: Option<OffsetDateTime>,
        blocked: Option<serde_json::Value>,
    ) -> Result<(), sqlx::Error> {
        let name = format!("bbs/repo-{id}");
        sqlx::query(
            "INSERT INTO repo (id, name, uri, external_id, external_service_type, external_service_id, private, deleted_at, blocked)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(id)
        .bind(&name)
        .bind(&name)
        .bind(external_id)
        .bind(service_type)
        .bind(service_id)
        .bind(private)
        .bind(deleted_at)
        .bind(blocked)
        .execute(pool)
        .await?;
        Ok(())
    }

    repo(
        pool,
        9101,
        "11",
        "bitbucketServer",
        &normalized,
        true,
        None,
        None,
    )
    .await?;
    repo(
        pool,
        9102,
        "12",
        "bitbucketServer",
        &normalized,
        true,
        None,
        None,
    )
    .await?;
    repo(
        pool,
        9103,
        "13",
        "bitbucketServer",
        &normalized,
        false,
        Some(OffsetDateTime::now_utc()),
        None,
    )
    .await?;
    repo(
        pool,
        9104,
        "14",
        "bitbucketServer",
        &normalized,
        false,
        None,
        Some(serde_json::json!({"reason": "blocked"})),
    )
    .await?;
    repo(
        pool,
        9105,
        "11",
        "github",
        "https://github.com/",
        false,
        None,
        None,
    )
    .await?;
    repo(
        pool,
        9106,
        "15",
        "bitbucketServer",
        &format!("{base}/other/"),
        false,
        None,
        None,
    )
    .await?;
    repo(
        pool,
        9107,
        "17",
        "bitbucketServer",
        &format!("{base}/bitbucket/"),
        false,
        None,
        None,
    )
    .await?;

    sqlx::query(
        "INSERT INTO users (id, username, deleted_at) VALUES
            (9801, 'alice', NULL),
            (9802, 'bob', now()),
            (9803, 'Carol', NULL)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO user_emails (user_id, email, verified_at) VALUES
            (9801, 'alice@example.com', now()),
            (9802, 'bob@example.com', NULL),
            (9803, 'carol@example.com', now()),
            (9801, 'unverified-alice@example.com', NULL)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenarios

struct Scenario {
    name: &'static str,
    external_service_id: i64,
    project_key: &'static str,
    permissions: Vec<UserPermission>,
    unrestricted: bool,
    site_json: &'static str,
    direct_repo_id: Option<i32>,
}

fn perms(pairs: &[(&str, &str)]) -> Vec<UserPermission> {
    pairs
        .iter()
        .map(|(bind_id, permission)| UserPermission {
            bind_id: (*bind_id).to_owned(),
            permission: (*permission).to_owned(),
        })
        .collect()
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "email",
            external_service_id: 9500,
            project_key: "EMAIL",
            permissions: perms(&[
                ("alice@example.com", "read"),
                ("ALICE@example.com", "read"),
                ("bob@example.com", "read"),
                ("ghost@example.com", "read"),
            ]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "username",
            external_service_id: 9500,
            project_key: "USERNAME",
            permissions: perms(&[
                ("alice", "read"),
                ("bob", "read"),
                ("carol", "read"),
                ("dave", "read"),
            ]),
            unrestricted: false,
            site_json: r#"{"permissions.userMapping":{"enabled":true,"bindID":"username"}}"#,
            direct_repo_id: None,
        },
        Scenario {
            name: "unrestricted",
            external_service_id: 9500,
            project_key: "UNRESTRICTED",
            permissions: Vec::new(),
            unrestricted: true,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "pagination",
            external_service_id: 9500,
            project_key: "PAGE",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "http-401",
            external_service_id: 9500,
            project_key: "E401",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "http-500",
            external_service_id: 9500,
            project_key: "E500",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "malformed-json",
            external_service_id: 9500,
            project_key: "BADJSON",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "empty-project",
            external_service_id: 9500,
            project_key: "EMPTY",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "padded-bind",
            external_service_id: 9500,
            project_key: "PAD",
            permissions: perms(&[(" alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "invalid-mapping-falls-back-to-email",
            external_service_id: 9500,
            project_key: "INVALIDMAP",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: r#"{"permissions.userMapping":{"enabled":true,"bindID":"login"}}"#,
            direct_repo_id: None,
        },
        Scenario {
            name: "disabled-username-mapping",
            external_service_id: 9500,
            project_key: "DISABLEDUSER",
            permissions: perms(&[("alice", "read")]),
            unrestricted: false,
            site_json: r#"{"permissions.userMapping":{"enabled":false,"bindID":"username"}}"#,
            direct_repo_id: None,
        },
        Scenario {
            name: "base-path",
            external_service_id: 9504,
            project_key: "BASEPATH",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "missing-service",
            external_service_id: 9599,
            project_key: "MISSING",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "wrong-kind",
            external_service_id: 9502,
            project_key: "WRONGKIND",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "bad-config",
            external_service_id: 9501,
            project_key: "BADCONFIG",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "oauth1",
            external_service_id: 9503,
            project_key: "OAUTH1",
            permissions: perms(&[("alice@example.com", "read")]),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: None,
        },
        Scenario {
            name: "repo-not-found",
            external_service_id: 0,
            project_key: "NOTFOUND",
            permissions: Vec::new(),
            unrestricted: false,
            site_json: "{}",
            direct_repo_id: Some(9999),
        },
    ]
}

// ---------------------------------------------------------------------------
// Canonical output

fn canon(text: &str, server: &str) -> String {
    let mut text = if server.is_empty() {
        text.to_owned()
    } else {
        text.replace(server, "SERVER")
    };
    text = text.replace('\n', "\\n");
    if let Some(index) = text.find("external service id=9501 config error: ") {
        text.truncate(index);
        text.push_str("external service id=9501 config error: <json-error>");
    }
    if let Some(index) = text.find("failed to unmarshal response to JSON: ") {
        text.truncate(index);
        text.push_str("failed to unmarshal response to JSON: <json-error>");
    }
    if let Some(index) = text.find("authorization.oauth.signingKey") {
        text.truncate(index);
        text.push_str("authorization.oauth.signingKey: <signing-key>");
    }
    text
}

fn ints32(values: &[i32]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let parts: Vec<String> = sorted.iter().map(i32::to_string).collect();
    format!("[{}]", parts.join(","))
}

fn ints64(values: &[i64]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let parts: Vec<String> = sorted.iter().map(i64::to_string).collect();
    format!("[{}]", parts.join(","))
}

fn null(value: Option<i32>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "NULL".to_owned(),
    }
}

fn set(value: bool) -> &'static str {
    if value {
        "set"
    } else {
        "NULL"
    }
}

async fn readback(pool: &PgPool, scenario: &str) -> Vec<String> {
    let mut lines = Vec::new();

    let rows = sqlx::query(
        "SELECT repo_id, permission, unrestricted, user_ids_ints FROM repo_permissions WHERE repo_id BETWEEN 9100 AND 9199",
    )
    .fetch_all(pool)
    .await
    .expect("select repo_permissions");
    for row in rows {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let permission: String = row.try_get("permission").expect("permission");
        let unrestricted: bool = row.try_get("unrestricted").expect("unrestricted");
        let users: Vec<i32> = row.try_get("user_ids_ints").expect("user_ids_ints");
        lines.push(format!(
            "bbs scenario={scenario} repo_permissions repo={repo_id} perm={permission} unrestricted={unrestricted} users={}",
            ints32(&users)
        ));
    }

    let rows = sqlx::query(
        "SELECT repo_id, user_id, user_external_account_id, source FROM user_repo_permissions WHERE repo_id BETWEEN 9100 AND 9199",
    )
    .fetch_all(pool)
    .await
    .expect("select user_repo_permissions");
    for row in rows {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let user_id: Option<i32> = row.try_get("user_id").expect("user_id");
        let account_id: Option<i32> = row.try_get("user_external_account_id").expect("account_id");
        let source: String = row.try_get("source").expect("source");
        lines.push(format!(
            "bbs scenario={scenario} user_repo_permissions repo={repo_id} user={} uea={} source={source}",
            null(user_id),
            null(account_id)
        ));
    }

    let rows = sqlx::query(
        "SELECT repo_id, permission, user_ids_ints FROM repo_pending_permissions WHERE repo_id BETWEEN 9100 AND 9199",
    )
    .fetch_all(pool)
    .await
    .expect("select repo_pending_permissions");
    for row in rows {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let permission: String = row.try_get("permission").expect("permission");
        let users: Vec<i64> = row.try_get("user_ids_ints").expect("user_ids_ints");
        lines.push(format!(
            "bbs scenario={scenario} repo_pending_permissions repo={repo_id} perm={permission} users={}",
            ints64(&users)
        ));
    }

    let rows = sqlx::query(
        "SELECT bind_id, permission, object_type, service_type, service_id, object_ids_ints
         FROM user_pending_permissions
         WHERE service_type = 'sourcegraph' AND service_id = $1 AND object_type = 'repos'
         ORDER BY bind_id",
    )
    .bind(SOURCEGRAPH_SERVICE_ID)
    .fetch_all(pool)
    .await
    .expect("select user_pending_permissions");
    for row in rows {
        let bind_id: String = row.try_get("bind_id").expect("bind_id");
        let permission: String = row.try_get("permission").expect("permission");
        let object_type: String = row.try_get("object_type").expect("object_type");
        let service_type: String = row.try_get("service_type").expect("service_type");
        let service_id: String = row.try_get("service_id").expect("service_id");
        let object_ids: Vec<i32> = row.try_get("object_ids_ints").expect("object_ids_ints");
        lines.push(format!(
            "bbs scenario={scenario} user_pending_permissions bind={bind_id} perm={permission} obj={object_type} service={service_type}|{service_id} ids={}",
            ints32(&object_ids)
        ));
    }

    lines.sort();
    lines
}

// ---------------------------------------------------------------------------
// Config

/// `(name, poll, concurrency, retry)`; `None` leaves the variable unset.
type ConfigCombo = (
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
    Option<&'static str>,
);

fn run_config_harness() {
    let combos: &[ConfigCombo] = &[
        ("defaults", None, None, None),
        ("negative-poll", Some("-1s"), None, None),
        ("zero-concurrency", None, Some("0"), None),
        ("bad-duration", Some("bogus"), None, None),
        ("empty-duration", Some(""), None, None),
        ("bad-int", None, Some("bogus"), None),
        ("two-errors", Some("-2s"), Some("0"), None),
    ];
    for (name, poll, concurrency, retry) in combos {
        for (var, value) in [
            (POLL_INTERVAL_VAR, poll),
            (CONCURRENCY_VAR, concurrency),
            (RETRY_INTERVAL_VAR, retry),
        ] {
            match value {
                Some(value) => std::env::set_var(var, value),
                None => std::env::remove_var(var),
            }
        }

        let config = BitbucketProjectPermissionsConfig::default();
        let mut env = EnvReader::new();
        config.load(&mut env);
        let text = match config.validate() {
            Ok(()) => "ok".to_owned(),
            Err(err) => canon(&err.to_string(), ""),
        };
        println!(
            "bbs config {name} poll_secs={} concurrency={} retry_secs={} error={text}",
            // `int64(time.Duration.Seconds())`, truncating toward zero.
            config.poll_interval_nanos() / 1_000_000_000,
            config.concurrency(),
            config.retry_interval_nanos() / 1_000_000_000
        );
    }
    for var in [POLL_INTERVAL_VAR, CONCURRENCY_VAR, RETRY_INTERVAL_VAR] {
        std::env::remove_var(var);
    }
}

// ---------------------------------------------------------------------------
// Store transitions

fn record(id: i64) -> BitbucketProjectPermissionJob {
    BitbucketProjectPermissionJob {
        id,
        state: "queued".to_owned(),
        failure_message: None,
        queued_at: OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        process_after: None,
        num_resets: 0,
        num_failures: 0,
        last_heartbeat_at: None,
        worker_hostname: String::new(),
        project_key: "PROJ".to_owned(),
        external_service_id: 9500,
        permissions: Vec::new(),
        unrestricted: false,
    }
}

async fn insert_job(
    pool: &PgPool,
    id: i32,
    state: &str,
    cancel: bool,
    heartbeat: Option<OffsetDateTime>,
) {
    sqlx::query(
        r#"INSERT INTO explicit_permissions_bitbucket_projects_jobs
             (id, state, project_key, external_service_id, permissions, unrestricted, cancel, queued_at, last_heartbeat_at)
             VALUES ($1, $2, 'PROJ', 9500, '[{"bindID":"alice@example.com","permission":"read"}]'::json[], false, $3, now(), $4)"#,
    )
    .bind(id)
    .bind(state)
    .bind(cancel)
    .bind(heartbeat)
    .execute(pool)
    .await
    .expect("insert job");
}

async fn state_of(pool: &PgPool, id: i32) -> (String, i32, Option<String>) {
    let row = sqlx::query(
        "SELECT state, num_failures, failure_message FROM explicit_permissions_bitbucket_projects_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("select job");
    (
        row.try_get("state").expect("state"),
        row.try_get("num_failures").expect("num_failures"),
        row.try_get("failure_message").expect("failure_message"),
    )
}

fn message(value: &Option<String>) -> String {
    value.clone().unwrap_or_default()
}

fn job_perms(perms: &[UserPermission]) -> String {
    perms
        .iter()
        .map(|perm| format!("{}:{}", perm.bind_id, perm.permission))
        .collect::<Vec<_>>()
        .join(",")
}

fn join_ids(ids: &[String]) -> String {
    let mut sorted = ids.to_vec();
    sorted.sort();
    format!("[{}]", sorted.join(","))
}

fn join_ages(ages: &[(i64, Duration)]) -> String {
    let mut ids: Vec<i64> = ages.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    let parts: Vec<String> = ids.iter().map(i64::to_string).collect();
    format!("[{}]", parts.join(","))
}

async fn run_store_harness(pool: &PgPool) {
    sqlx::query(
        "DELETE FROM explicit_permissions_bitbucket_projects_jobs WHERE id BETWEEN 9700 AND 9799",
    )
    .execute(pool)
    .await
    .expect("clean jobs");
    let store = BitbucketProjectPermissionsStore::new(pool.clone(), Duration::from_secs(30));

    insert_job(pool, 9701, "queued", false, None).await;
    let job = store
        .dequeue("host-1")
        .await
        .expect("dequeue")
        .expect("job");
    println!(
        "bbs store dequeue id={} state={} host={} project={} perms={} unrestricted={} failures={} resets={} started={} heartbeat={}",
        job.id,
        job.state,
        job.worker_hostname,
        job.project_key,
        job_perms(&job.permissions),
        job.unrestricted,
        job.num_failures,
        job.num_resets,
        set(job.started_at.is_some()),
        set(job.last_heartbeat_at.is_some())
    );

    async fn mark_errored(
        pool: &PgPool,
        store: &BitbucketProjectPermissionsStore,
        id: i64,
        failure_text: &str,
    ) {
        store
            .mark_errored(&record(id), failure_text)
            .await
            .expect("mark errored");
        let (state, failures, failure_message) = state_of(pool, id as i32).await;
        println!(
            "bbs store errored id={id} state={state} failures={failures} msg={}",
            message(&failure_message)
        );
    }

    mark_errored(pool, &store, 9701, "boom1").await;
    let gated = store.dequeue("host-1").await.expect("dequeue");
    assert!(gated.is_none(), "dequeue must be gated by retryAfter");
    println!("bbs store dequeue-gated found=0");

    for attempt in 2..=5 {
        sqlx::query(
            "UPDATE explicit_permissions_bitbucket_projects_jobs SET finished_at = now() - interval '1 hour' WHERE id = 9701",
        )
        .execute(pool)
        .await
        .expect("age finished_at");
        let job = store
            .dequeue("host-1")
            .await
            .expect("dequeue")
            .expect("job");
        if attempt == 2 {
            println!(
                "bbs store dequeue-retry id={} failures={}",
                job.id, job.num_failures
            );
        }
        mark_errored(pool, &store, 9701, &format!("boom{attempt}")).await;
    }

    insert_job(
        pool,
        9702,
        "processing",
        true,
        Some(OffsetDateTime::now_utc()),
    )
    .await;
    let (known, canceled) = store
        .heartbeat(&["9702".to_owned()])
        .await
        .expect("heartbeat");
    println!(
        "bbs store heartbeat known={} canceled={}",
        join_ids(&known),
        join_ids(&canceled)
    );
    mark_errored(pool, &store, 9702, "canceled by user").await;

    let (known, canceled) = store
        .heartbeat(&["9799".to_owned()])
        .await
        .expect("heartbeat");
    println!(
        "bbs store heartbeat-unknown known={} canceled={}",
        join_ids(&known),
        join_ids(&canceled)
    );

    insert_job(
        pool,
        9703,
        "processing",
        false,
        Some(OffsetDateTime::now_utc()),
    )
    .await;
    let updated = store
        .mark_complete(&record(9703))
        .await
        .expect("mark complete");
    assert!(updated, "mark complete must update 9703");
    let (state, failures, _) = state_of(pool, 9703).await;
    println!("bbs store complete id=9703 state={state} failures={failures}");

    insert_job(
        pool,
        9704,
        "processing",
        false,
        Some(OffsetDateTime::now_utc()),
    )
    .await;
    let updated = store
        .mark_failed(&record(9704), "failed now")
        .await
        .expect("mark failed");
    assert!(updated, "mark failed must update 9704");
    let (state, failures, failure_message) = state_of(pool, 9704).await;
    println!(
        "bbs store failed id=9704 state={state} failures={failures} msg={}",
        message(&failure_message)
    );

    insert_job(
        pool,
        9705,
        "processing",
        false,
        Some(OffsetDateTime::now_utc() - time::Duration::minutes(2)),
    )
    .await;
    insert_job(
        pool,
        9706,
        "processing",
        false,
        Some(OffsetDateTime::now_utc()),
    )
    .await;
    let (reset, failed) = store.reset_stalled().await.expect("reset stalled");
    println!(
        "bbs store reset queued={} failed={}",
        join_ages(&reset),
        join_ages(&failed)
    );
    let (state, failures, failure_message) = state_of(pool, 9705).await;
    println!(
        "bbs store reset-row id=9705 state={state} failures={failures} msg={}",
        message(&failure_message)
    );
    let (state, _, _) = state_of(pool, 9706).await;
    println!("bbs store reset-row id=9706 state={state}");
}

// ---------------------------------------------------------------------------
// Entry point

#[tokio::test(flavor = "multi_thread")]
async fn bitbucket_project_permissions_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_BBS_TEST_DSN") else {
        return;
    };

    let pool = connect(&dsn, 2).await.expect("connect scratch database");

    run_config_harness();

    let records = Arc::new(Mutex::new(Vec::<String>::new()));
    let (base, shutdown) = start_server(Arc::clone(&records)).await;
    clean_fixtures(&pool).await.expect("clean fixtures");
    seed(&pool, &base).await.expect("seed fixtures");

    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");

    for scenario in scenarios() {
        clean_permissions(&pool).await.expect("clean permissions");
        records.lock().expect("lock").clear();

        let site = SiteConfiguration::parse(scenario.site_json).expect("parse site config");
        let live = Arc::new(LiveConfig::new(Unified {
            site,
            service_connections: Default::default(),
        }));
        let handler = BitbucketProjectPermissionsHandler::new(
            pool.clone(),
            Arc::new(Keyring::default()),
            live,
            http.clone(),
        );

        let raw_error = match scenario.direct_repo_id {
            Some(repo_id) => handler
                .set_repo_permissions(
                    repo_id,
                    &BTreeSet::from([9801]),
                    &["pending@example.com".to_owned()],
                )
                .await
                .err(),
            None => {
                let job = BitbucketProjectPermissionJob {
                    state: "queued".to_owned(),
                    project_key: scenario.project_key.to_owned(),
                    external_service_id: scenario.external_service_id,
                    permissions: scenario.permissions.clone(),
                    unrestricted: scenario.unrestricted,
                    ..record(1)
                };
                handler.handle(CancellationToken::new(), &job).await.err()
            }
        };

        let (result, non_retryable) = match &raw_error {
            None => ("ok".to_owned(), false),
            Some(message) => (canon(message, &base), is_non_retryable(message)),
        };
        let requests = {
            let taken = records.lock().expect("lock");
            if taken.is_empty() {
                "<none>".to_owned()
            } else {
                taken.join(" >> ").replace(&base, "SERVER")
            }
        };

        println!("bbs scenario={} result={result}", scenario.name);
        println!(
            "bbs scenario={} nonretryable={non_retryable}",
            scenario.name
        );
        println!("bbs scenario={} requests={requests}", scenario.name);
        for line in readback(&pool, scenario.name).await {
            println!("{line}");
        }
    }

    shutdown.cancel();
    run_store_harness(&pool).await;
}
