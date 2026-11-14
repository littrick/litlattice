//! M9.33c differential: the inbound GitHub App installation webhook and the
//! real `/githubapp/*` site-admin actor chain
//! (`cmd/frontend/internal/webhooks/zz_m933c_inbound_oracle_test.go` and
//! `cmd/frontend/internal/githubapp/zz_m933c_siteadmin_oracle_test.go`, driven
//! by `run_githubapp_inbound_diff.sh`).
//!
//! Env:
//!
//! * `SG_M933C_GITHUBAPP_INBOUND_DIFF=1`
//! * `M933C_CORPUS` — `corpus.json`
//! * `M933C_OUT` — output directory (one `<name>.json` per case)
//! * `SG_QTEST_DSN` — scratch database DSN
//! * `M933C_REDIS` — Redis endpoint (default `127.0.0.1:6379`)

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use http::{header, Method, Request};
use http_body_util::{BodyExt, Full};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use sg_service_frontend::githubapp_http_api::{
    route, ActorIdentity, GitHubAppServer, PgSiteAdminCheck,
};
use sg_service_frontend::githubapp_inbound::{
    inbound_webhook_response, sign as sign_inbound, PgInboundDeps,
};

#[derive(Deserialize)]
struct Fixtures {
    apps: Vec<FixtureApp>,
    webhooks: Vec<FixtureWebhook>,
    users: Vec<FixtureUser>,
}

#[derive(Deserialize)]
struct FixtureApp {
    id: i32,
    #[serde(rename = "appID")]
    app_id: i32,
    name: String,
    domain: String,
    kind: String,
    #[serde(rename = "baseURL")]
    base_url: String,
    #[serde(rename = "creatorID", default)]
    creator_id: i32,
    #[serde(rename = "privateKey", default = "default_private_key")]
    private_key: String,
}

fn default_private_key() -> String {
    "pem".to_owned()
}

#[derive(Deserialize)]
struct FixtureWebhook {
    uuid: String,
    secret: String,
    #[serde(rename = "codeHostKind")]
    code_host_kind: String,
    #[serde(rename = "codeHostURN")]
    code_host_urn: String,
}

#[derive(Deserialize)]
struct FixtureUser {
    id: i32,
    username: String,
    #[serde(rename = "siteAdmin")]
    site_admin: bool,
    #[serde(default)]
    deleted: bool,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    uuid: String,
    #[serde(default)]
    event: String,
    #[serde(rename = "eventKey", default)]
    event_key: String,
    #[serde(default)]
    body: String,
    #[serde(rename = "contentType", default)]
    content_type: String,
    #[serde(rename = "signatureMode", default)]
    signature_mode: String,
    #[serde(default)]
    path: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    route: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    actor: String,
}

#[derive(Deserialize)]
struct Corpus {
    fixtures: Fixtures,
    cases: Vec<Case>,
}

#[derive(Serialize)]
struct InstallRow {
    #[serde(rename = "appID")]
    app_id: i32,
    #[serde(rename = "installationID")]
    installation_id: i32,
    url: String,
    #[serde(rename = "accountLogin")]
    account_login: String,
    #[serde(rename = "accountAvatarURL")]
    account_avatar_url: String,
    #[serde(rename = "accountURL")]
    account_url: String,
    #[serde(rename = "accountType")]
    account_type: String,
}

#[derive(Serialize)]
struct UserCredentialRow {
    domain: String,
    #[serde(rename = "userID")]
    user_id: i32,
    #[serde(rename = "externalServiceType")]
    external_service_type: String,
    #[serde(rename = "externalServiceID")]
    external_service_id: String,
    credential: String,
    #[serde(rename = "encryptionKeyID")]
    encryption_key_id: String,
    #[serde(rename = "githubAppID")]
    github_app_id: i32,
}

#[derive(Serialize)]
struct SiteCredentialRow {
    #[serde(rename = "externalServiceType")]
    external_service_type: String,
    #[serde(rename = "externalServiceID")]
    external_service_id: String,
    credential: String,
    #[serde(rename = "encryptionKeyID")]
    encryption_key_id: String,
    #[serde(rename = "githubAppID")]
    github_app_id: i32,
}

#[derive(Serialize)]
struct InboundOutput {
    status: u16,
    body: String,
    installs: Vec<InstallRow>,
    #[serde(rename = "userCredentials")]
    user_credentials: Vec<UserCredentialRow>,
    #[serde(rename = "siteCredentials")]
    site_credentials: Vec<SiteCredentialRow>,
}

#[derive(Serialize)]
struct WebhookRow {
    uuid: String,
    name: String,
    #[serde(rename = "codeHostKind")]
    code_host_kind: String,
    #[serde(rename = "codeHostURN")]
    code_host_urn: String,
    secret: String,
    #[serde(rename = "encryptionKeyID")]
    encryption_key_id: String,
    #[serde(rename = "createdByUserID")]
    created_by_user_id: i32,
    #[serde(rename = "updatedByUserID")]
    updated_by_user_id: i32,
}

#[derive(Serialize)]
struct SiteOutput {
    status: u16,
    body: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    webhooks: Vec<WebhookRow>,
}

fn normalize(value: &str) -> String {
    let uuid = Regex::new(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").unwrap();
    let text = uuid.replace_all(value, "<uuid>").into_owned();
    let state = Regex::new(r"[0-9a-f]{64}").unwrap();
    state.replace_all(&text, "<state>").into_owned()
}

fn actor_identity(spec: &str) -> ActorIdentity {
    if spec == "internal" {
        return ActorIdentity::Internal;
    }
    if let Some(uid) = spec.strip_prefix("user:") {
        return ActorIdentity::User(uid.parse().unwrap());
    }
    ActorIdentity::Anonymous
}

async fn seed_database(pool: &sqlx::PgPool, corpus: &Corpus) {
    for query in [
        "DELETE FROM github_app_installs",
        "DELETE FROM user_credentials WHERE domain = 'batches'",
        "DELETE FROM batch_changes_site_credentials",
        "DELETE FROM github_apps",
        "DELETE FROM webhooks",
        "DELETE FROM users WHERE id IN (101, 102, 103, 201, 202)",
    ] {
        sqlx::query(query).execute(pool).await.unwrap();
    }
    for app in &corpus.fixtures.apps {
        sqlx::query(
            "INSERT INTO github_apps \
                 (id, app_id, name, slug, base_url, client_id, client_secret, private_key, \
                  encryption_key_id, logo, app_url, domain, kind, creator_id) \
             VALUES ($1, $2, $3, $3, $4, 'cid', 'csec', $5, '', 'logo', $6, \
                     $7, $8::github_app_kind, $9)",
        )
        .bind(app.id)
        .bind(app.app_id)
        .bind(&app.name)
        .bind(&app.base_url)
        .bind(&app.private_key)
        .bind(format!("https://example.com/apps/{}", app.name))
        .bind(&app.domain)
        .bind(&app.kind)
        .bind(app.creator_id)
        .execute(pool)
        .await
        .unwrap();
    }
    for hook in &corpus.fixtures.webhooks {
        let secret: Option<&str> = if hook.secret.is_empty() {
            None
        } else {
            Some(hook.secret.as_str())
        };
        sqlx::query(
            "INSERT INTO webhooks (uuid, code_host_kind, code_host_urn, name, secret) \
             VALUES ($1::uuid, $2, $3, 'seed-webhook', $4)",
        )
        .bind(&hook.uuid)
        .bind(&hook.code_host_kind)
        .bind(&hook.code_host_urn)
        .bind(secret)
        .execute(pool)
        .await
        .unwrap();
    }
    for user in &corpus.fixtures.users {
        let deleted_at = if user.deleted {
            Some("2020-01-01T00:00:00Z")
        } else {
            None
        };
        sqlx::query(
            "INSERT INTO users (id, username, site_admin, deleted_at) \
             VALUES ($1, $2, $3, $4::timestamptz)",
        )
        .bind(user.id)
        .bind(&user.username)
        .bind(user.site_admin)
        .bind(deleted_at)
        .execute(pool)
        .await
        .unwrap();
    }
}

/// The Go site-admin oracle restarts its own DB (it clears apps/webhooks and
/// re-seeds only users); the Rust run must cross the same boundary between the
/// webhook cases and the site-admin cases.
async fn seed_site_database(pool: &sqlx::PgPool, corpus: &Corpus) {
    for query in [
        "DELETE FROM github_app_installs",
        "DELETE FROM user_credentials WHERE domain = 'batches'",
        "DELETE FROM batch_changes_site_credentials",
        "DELETE FROM github_apps",
        "DELETE FROM webhooks",
        "DELETE FROM users WHERE id IN (101, 102, 103, 201, 202)",
    ] {
        sqlx::query(query).execute(pool).await.unwrap();
    }
    for user in &corpus.fixtures.users {
        let deleted_at = if user.deleted {
            Some("2020-01-01T00:00:00Z")
        } else {
            None
        };
        sqlx::query(
            "INSERT INTO users (id, username, site_admin, deleted_at) \
             VALUES ($1, $2, $3, $4::timestamptz)",
        )
        .bind(user.id)
        .bind(&user.username)
        .bind(user.site_admin)
        .bind(deleted_at)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn snapshot_installs(pool: &sqlx::PgPool) -> Vec<InstallRow> {
    let rows = sqlx::query(
        "SELECT app_id, installation_id, COALESCE(url,'') AS url, \
                COALESCE(account_login,'') AS account_login, \
                COALESCE(account_avatar_url,'') AS account_avatar_url, \
                COALESCE(account_url,'') AS account_url, \
                COALESCE(account_type,'') AS account_type \
         FROM github_app_installs ORDER BY app_id, installation_id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.iter()
        .map(|row| InstallRow {
            app_id: row.get("app_id"),
            installation_id: row.get("installation_id"),
            url: row.get("url"),
            account_login: row.get("account_login"),
            account_avatar_url: row.get("account_avatar_url"),
            account_url: row.get("account_url"),
            account_type: row.get("account_type"),
        })
        .collect()
}

async fn snapshot_user_credentials(pool: &sqlx::PgPool) -> Vec<UserCredentialRow> {
    let rows = sqlx::query(
        "SELECT domain, user_id, external_service_type, external_service_id, credential, \
                encryption_key_id, COALESCE(github_app_id,0)::int4 AS github_app_id \
         FROM user_credentials WHERE domain = 'batches' \
         ORDER BY user_id, external_service_type, external_service_id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.iter()
        .map(|row| UserCredentialRow {
            domain: row.get("domain"),
            user_id: row.get("user_id"),
            external_service_type: row.get("external_service_type"),
            external_service_id: row.get("external_service_id"),
            credential: String::from_utf8_lossy(&row.get::<Vec<u8>, _>("credential")).into_owned(),
            encryption_key_id: row.get("encryption_key_id"),
            github_app_id: row.get("github_app_id"),
        })
        .collect()
}

async fn snapshot_site_credentials(pool: &sqlx::PgPool) -> Vec<SiteCredentialRow> {
    let rows = sqlx::query(
        "SELECT external_service_type, external_service_id, credential, encryption_key_id, \
                COALESCE(github_app_id,0)::int4 AS github_app_id \
         FROM batch_changes_site_credentials \
         ORDER BY external_service_type, external_service_id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.iter()
        .map(|row| SiteCredentialRow {
            external_service_type: row.get("external_service_type"),
            external_service_id: row.get("external_service_id"),
            credential: String::from_utf8_lossy(&row.get::<Vec<u8>, _>("credential")).into_owned(),
            encryption_key_id: row.get("encryption_key_id"),
            github_app_id: row.get("github_app_id"),
        })
        .collect()
}

async fn snapshot_webhooks(pool: &sqlx::PgPool) -> Vec<WebhookRow> {
    let rows = sqlx::query(
        "SELECT uuid::text AS uuid, COALESCE(name,'') AS name, \
                COALESCE(code_host_kind,'') AS code_host_kind, \
                COALESCE(code_host_urn,'') AS code_host_urn, \
                COALESCE(secret,'') AS secret, \
                COALESCE(encryption_key_id,'') AS encryption_key_id, \
                COALESCE(created_by_user_id,0)::int4 AS created_by_user_id, \
                COALESCE(updated_by_user_id,0)::int4 AS updated_by_user_id \
         FROM webhooks ORDER BY uuid",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.iter()
        .map(|row| WebhookRow {
            uuid: normalize(row.get::<String, _>("uuid").as_str()),
            name: normalize(row.get::<String, _>("name").as_str()),
            code_host_kind: row.get("code_host_kind"),
            code_host_urn: row.get("code_host_urn"),
            secret: row.get("secret"),
            encryption_key_id: row.get("encryption_key_id"),
            created_by_user_id: row.get("created_by_user_id"),
            updated_by_user_id: row.get("updated_by_user_id"),
        })
        .collect()
}

fn write_output(out_dir: &str, name: &str, value: &impl Serialize) {
    let encoded = serde_json::to_vec(value).unwrap();
    std::fs::write(
        std::path::Path::new(out_dir).join(format!("{name}.json")),
        encoded,
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn githubapp_inbound_diff() {
    if std::env::var("SG_M933C_GITHUBAPP_INBOUND_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M933C_CORPUS").expect("M933C_CORPUS");
    let out_dir = std::env::var("M933C_OUT").expect("M933C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let redis = std::env::var("M933C_REDIS").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    std::fs::create_dir_all(&out_dir).unwrap();
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).unwrap()).unwrap();

    let pool = sqlx::PgPool::connect(&dsn).await.expect("connect postgres");
    seed_database(&pool, &corpus).await;

    let keyring = Arc::new(sg_keyring::Keyring::default());
    let inbound = PgInboundDeps::new(pool.clone(), keyring.clone());

    // Redis is only needed by the `new-app-state` cases.
    let needs_redis = corpus.cases.iter().any(|case| case.kind == "newappstate");
    let kv = if needs_redis {
        Some(
            sg_redis::KeyValue::connect(&redis)
                .await
                .expect("connect redis"),
        )
    } else {
        None
    };

    // The actor-less site-admin gate (the real middleware is exercised per
    // case through `site_admin_gate_for`).
    let site_server = {
        let mut server = GitHubAppServer::offline();
        server.site_admin = Arc::new(PgSiteAdminCheck::new(
            pool.clone(),
            ActorIdentity::Anonymous,
        ));
        server
    };

    let mut site_phase_seeded = false;
    for case in &corpus.cases {
        if case.kind != "webhook" && !site_phase_seeded {
            seed_site_database(&pool, &corpus).await;
            site_phase_seeded = true;
        }
        match case.kind.as_str() {
            "webhook" => {
                let secret = corpus
                    .fixtures
                    .webhooks
                    .iter()
                    .find(|hook| hook.uuid == case.uuid)
                    .map(|hook| hook.secret.clone())
                    .unwrap_or_default();
                let body = Bytes::from(case.body.clone().into_bytes());
                let path = case
                    .path
                    .clone()
                    .unwrap_or_else(|| format!("/.api/webhooks/{}", case.uuid));
                let mut request = Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .body(Full::new(body.clone()))
                    .unwrap();
                if !case.content_type.is_empty() {
                    request.headers_mut().insert(
                        header::CONTENT_TYPE,
                        case.content_type.parse().unwrap(),
                    );
                }
                if !case.event.is_empty() {
                    request
                        .headers_mut()
                        .insert("x-github-event", case.event.parse().unwrap());
                }
                if !case.event_key.is_empty() {
                    request
                        .headers_mut()
                        .insert("x-event-key", case.event_key.parse().unwrap());
                }
                if let Some(signature) =
                    sign_inbound(&case.signature_mode, secret.as_bytes(), &body)
                {
                    let name = if signature.starts_with("sha1=") {
                        "x-hub-signature"
                    } else {
                        "x-hub-signature-256"
                    };
                    request
                        .headers_mut()
                        .insert(name, signature.parse().unwrap());
                }
                let response = inbound_webhook_response(&mut request, &inbound).await;
                let status = response.status().as_u16();
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes();
                let output = InboundOutput {
                    status,
                    body: String::from_utf8_lossy(&body).into_owned(),
                    installs: snapshot_installs(&pool).await,
                    user_credentials: snapshot_user_credentials(&pool).await,
                    site_credentials: snapshot_site_credentials(&pool).await,
                };
                write_output(&out_dir, &case.name, &output);
                println!("RUST {} status={status}", case.name);
            }
            "siteadmin" => {
                let identity = actor_identity(&case.actor);
                let (status, body) = match site_server.site_admin_gate_for(identity) {
                    Some(response) => {
                        let status = response.status().as_u16();
                        let bytes = response.into_body().collect().await.unwrap().to_bytes();
                        (status, String::from_utf8_lossy(&bytes).into_owned())
                    }
                    None => (200u16, "NEXT\n".to_owned()),
                };
                let output = SiteOutput {
                    status,
                    body,
                    webhooks: Vec::new(),
                };
                write_output(&out_dir, &case.name, &output);
                println!("RUST {} status={status}", case.name);
            }
            "newappstate" => {
                let identity = actor_identity(&case.actor);
                let server = GitHubAppServer::production_with_actor(
                    pool.clone(),
                    keyring.clone(),
                    kv.as_ref().expect("redis for newappstate").clone(),
                    identity,
                );
                let mut target = "/githubapp/new-app-state".to_owned();
                if !case.query.is_empty() {
                    target.push('?');
                    target.push_str(&case.query);
                }
                let request = Request::builder()
                    .method(Method::GET)
                    .uri(target)
                    .body(())
                    .unwrap();
                let response = server.serve_with_actor(
                    &request,
                    route("/githubapp/new-app-state").unwrap(),
                    identity,
                );
                let status = response.status().as_u16();
                let bytes = response.into_body().collect().await.unwrap().to_bytes();
                let body = normalize(&String::from_utf8_lossy(&bytes));
                let output = SiteOutput {
                    status,
                    body,
                    webhooks: snapshot_webhooks(&pool).await,
                };
                write_output(&out_dir, &case.name, &output);
                println!("RUST {} status={status}", case.name);
            }
            other => panic!("unknown case kind {other}"),
        }
    }
}
