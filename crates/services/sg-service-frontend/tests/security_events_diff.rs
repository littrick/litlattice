//! M9.22c differential of the frontend security-event write sites
//! (`OrganizationViewed` / `OrganizationListViewed` / `OrganizationCreated` /
//! `OrganizationUpdated` / `OutboundRequestViewed` /
//! `User{,Code}CompletionQuotaUpdated`).
//!
//! Both sides run the same corpus against the same scratch database with
//! `log.securityEventLog.location=database`: the Go oracle drives the real
//! `schemaResolver`; this harness drives the assembled OSS schema through
//! [`SchemaGraphqlDeps::exec`]. Each case records the `security_event_logs`
//! rows it appended (`name`, `user_id`, canonical `argument` text, `source`).
//!
//! Env:
//!
//! * `SG_M9_22C_DIFF=1`
//! * `M9_22C_CORPUS` — JSON `[{name, query, variables?, uid, auditlog}]`
//! * `M9_22C_OUT` — output directory (one `<name>.json` per case)
//! * `M9_22C_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::PgPool;

use sg_graphql::schema::assemble_oss;
use sg_service_frontend::admin_mutations::AdminMutationDeps;
use sg_service_frontend::admin_store::PgAdminStore;
use sg_service_frontend::admin_store::{
    AccessTokenExpirationOptions, AccessTokensAllowMode, AdminEffects, UserRow,
};
use sg_service_frontend::graphql_api::{GraphqlDeps, GraphqlExecRequest, SchemaGraphqlDeps};

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    variables: Map<String, Value>,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    auditlog: bool,
}

#[derive(Serialize)]
struct Row {
    name: String,
    user_id: i32,
    argument: String,
    source: String,
}

#[derive(Serialize)]
struct ResultRows {
    rows: Vec<Row>,
}

/// Production-shaped effects that actually persist `audit` events (the
/// `PgAdminEffects` implementation in `admin_store.rs` is owned by M9.22a and
/// currently returns `auditlog_expansion() == false`, so the differential
/// injects this instead of exercising that seam).
struct RealEffects {
    pool: PgPool,
    auditlog: AtomicBool,
}

#[async_trait]
impl AdminEffects for RealEffects {
    fn can_send_email(&self) -> bool {
        false
    }
    fn email_verification_required(&self) -> bool {
        false
    }
    fn reset_password_enabled(&self) -> bool {
        false
    }
    fn access_tokens_allow(&self) -> AccessTokensAllowMode {
        AccessTokensAllowMode::All
    }
    fn access_token_expiration_options(&self) -> AccessTokenExpirationOptions {
        AccessTokenExpirationOptions {
            allow_no_expiration: true,
            max_duration_seconds: Ok(90 * 86400),
        }
    }
    fn dotcom(&self) -> bool {
        false
    }
    fn external_services_writable(&self) -> bool {
        false
    }
    fn auth_enable_username_changes(&self) -> bool {
        true
    }
    fn auditlog_expansion(&self) -> bool {
        self.auditlog.load(Ordering::SeqCst)
    }
    fn make_email_verification_code(&self) -> Result<String, String> {
        Ok("verification-code".to_owned())
    }
    async fn send_email(&self, _user_id: i32, _email: &str, _kind: &str) -> Result<(), String> {
        Ok(())
    }
    async fn delete_password_reset_code(&self, _user_id: i32) -> Result<(), String> {
        Ok(())
    }
    async fn delete_stale_perforce(&self, _user_id: i32, _email: &str) -> Result<(), String> {
        Ok(())
    }
    async fn schedule_perms_sync(&self, _user_ids: &[i32], _reason: &str) -> Result<(), String> {
        Ok(())
    }
    async fn validate_external_service(&self, _kind: &str, _config: &str) -> Result<(), String> {
        Ok(())
    }
    async fn enqueue_external_service_sync(&self, _id: i64) -> Result<(), String> {
        Ok(())
    }
    async fn reset_password_url(
        &self,
        _user: &UserRow,
        _email: &str,
        _email_verified: bool,
    ) -> Result<Option<String>, String> {
        Ok(None)
    }
    async fn audit(&self, name: &str, user_id: i32, argument: Value) {
        if !self.auditlog.load(Ordering::SeqCst) {
            return;
        }
        sg_service_frontend::security_event_logs::write_backend_security_event(
            &self.pool, name, user_id, argument,
        )
        .await;
    }
    async fn audit_account_modified(&self, _user_id: i32, _actor_user_id: i32, _fields: &[String]) {
    }
}

async fn set_flag(pool: &PgPool, on: bool) {
    sqlx::query("DELETE FROM feature_flags WHERE flag_name = 'auditlog-expansion'")
        .execute(pool)
        .await
        .expect("delete flag");
    if on {
        sqlx::query(
            "INSERT INTO feature_flags (flag_name, flag_type, bool_value) \
             VALUES ('auditlog-expansion', 'bool', true)",
        )
        .execute(pool)
        .await
        .expect("insert flag");
    }
}

#[tokio::test]
async fn security_events_diff_harness() {
    if std::env::var("SG_M9_22C_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9_22C_CORPUS").expect("M9_22C_CORPUS");
    let out = std::env::var("M9_22C_OUT").expect("M9_22C_OUT");
    let dsn = std::env::var("M9_22C_DSN").expect("M9_22C_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    let schema = Arc::new(assemble_oss().expect("oss schema"));
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let effects = Arc::new(RealEffects {
        pool: pool.clone(),
        auditlog: AtomicBool::new(false),
    });
    let deps =
        SchemaGraphqlDeps::with_session(Arc::clone(&schema), pool.clone()).with_admin_mutations(
            AdminMutationDeps::new(Arc::new(PgAdminStore::new(pool.clone())), effects.clone()),
        );

    for case in cases {
        set_flag(&pool, case.auditlog).await;
        effects.auditlog.store(case.auditlog, Ordering::SeqCst);
        let before: (i64,) = sqlx::query_as("SELECT COALESCE(MAX(id), 0) FROM security_event_logs")
            .fetch_one(&pool)
            .await
            .expect("max id");

        let actor = if case.uid == 0 {
            sg_graphql::exec::Actor::Anonymous
        } else {
            sg_graphql::exec::Actor::User { uid: case.uid }
        };
        let response = deps
            .exec(GraphqlExecRequest {
                query: &case.query,
                operation_name: "",
                variables: &case.variables,
                actor,
                anonymous_uid: "",
            })
            .await;
        if !response.errors.is_empty() {
            println!("RAWERR {}: {:?}", case.name, response.to_json());
        }

        let rows: Vec<(String, i32, String, String)> = sqlx::query_as(
            "SELECT name, user_id, argument::text, source FROM security_event_logs \
             WHERE id > $1 ORDER BY id",
        )
        .bind(before.0)
        .fetch_all(&pool)
        .await
        .expect("select events");
        let result = ResultRows {
            rows: rows
                .into_iter()
                .map(|(name, user_id, argument, source)| Row {
                    name,
                    user_id,
                    argument,
                    source,
                })
                .collect(),
        };
        let file = serde_json::to_string(&result).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} rows={}", case.name, result.rows.len());
    }
}
