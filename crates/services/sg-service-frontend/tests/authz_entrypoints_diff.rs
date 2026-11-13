#![forbid(unsafe_code)]

//! M9.22a differential harness (Rust side): pending-permission effects of the
//! newly wired entry points (`verify-email`, GraphQL `SetVerified`,
//! `createUser`, OAuth signup/upsert).
//!
//! The Go oracle is the untracked
//! `internal/database/zz_m922a_authz_entrypoints_oracle_test.go`, run by
//! `test_ws/p9-go/run_authz_pending_permissions_diff.sh`. That script applies
//! `fixtures.sql` followed by `entrypoint_fixtures.sql` immediately before
//! each side and passes the shared case list via `M922A_ENTRY_CASES`.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;
use serde_json::Value;

use sg_graphql::exec::{root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::admin_store::{
    AccessTokenExpirationOptions, AccessTokensAllowMode, AdminEffects, AdminMutationOutcome,
    AdminStore, PgAdminStore, UserRow, UserUpdate,
};
use sg_service_frontend::app_handlers_more::{verify_email, MoreDeps, PgVerifyEmailDb};
use sg_service_frontend::user_admin_graphql::prefetch_user_admin_mutations;
use sg_service_frontend::user_emails_graphql::prefetch_user_email_mutations;
use sg_service_frontend::user_provisioning::{
    get_or_create_user, ExternalIdentity, PgProvisionStore, ProvisionDeps, UsernameSuffixSource,
};
use sqlx::postgres::PgPool;
use sqlx::Row;

const SDL: &str = "
schema { query: Query mutation: Mutation }

type Query { _noop: String }

type Mutation {
  createUser(username: String!, email: String, verifiedEmail: Boolean): CreateUserResult!
  setUserEmailVerified(user: ID!, email: String!, verified: Boolean!): EmptyResponse!
}

type CreateUserResult { user: User! }
type User { id: ID! }
type EmptyResponse { alwaysNil: String }
";

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    email: String,
    #[serde(default)]
    code: String,
    #[serde(default)]
    verified: bool,
    #[serde(default)]
    username: String,
    #[serde(default)]
    creator: i32,
    #[serde(default)]
    service_type: String,
    #[serde(default)]
    service_id: String,
    #[serde(default)]
    account_id: String,
    #[serde(default)]
    org_name: String,
    /// `permissions.userMapping.bindID` (normalized like `conf.Get()`).
    #[serde(default = "default_bind_id")]
    bind_id: String,
    /// `log.securityEventLog.location` (`all`/`database`/`none`).
    #[serde(default = "default_event_location")]
    security_event_location: String,
}

fn default_bind_id() -> String {
    "email".to_owned()
}

fn default_event_location() -> String {
    "all".to_owned()
}

/// The per-case site configuration both sides read through `conf.Get()`.
fn case_site(bind_id: &str, location: &str) -> sg_config::SiteConfiguration {
    let raw = serde_json::json!({
        "permissions.userMapping": {"enabled": true, "bindID": bind_id},
        "log": {"securityEventLog": {"location": location}},
    });
    sg_config::SiteConfiguration::parse(&raw.to_string()).expect("site configuration")
}

/// The harness never sends e-mail, audits, or schedules syncs; every effect
/// returns its benign default exactly like the baseline's log-and-continue
/// paths.
struct NoopEffects;

#[async_trait]
impl AdminEffects for NoopEffects {
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
        true
    }
    fn auth_enable_username_changes(&self) -> bool {
        true
    }
    fn auditlog_expansion(&self) -> bool {
        false
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
    async fn validate_external_service(
        &self,
        _kind: &str,
        _config: &str,
    ) -> Result<(), String> {
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
    async fn audit(&self, _name: &str, _user_id: i32, _argument: Value) {}
    async fn audit_account_modified(&self, _user_id: i32, _actor_user_id: i32, _fields: &[String]) {
    }
}

struct FixedSuffix;

impl UsernameSuffixSource for FixedSuffix {
    fn add_random_suffix(&self, username: &str) -> Result<String, String> {
        Ok(format!("{username}-m922a"))
    }
}

fn relay_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("User:{id}"))
}

fn ids(column: &str) -> String {
    format!(
        "COALESCE(array_to_string(ARRAY(SELECT unnest({column}) ORDER BY 1), ','), '') AS ids"
    )
}

async fn snapshot(pool: &PgPool) {
    for row in sqlx::query(
        "SELECT id, username::text AS username, site_admin, deleted_at IS NULL AS live \
         FROM users WHERE id BETWEEN 9960 AND 10099 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("select users")
    {
        let id: i32 = row.try_get("id").expect("id");
        let username: String = row.try_get("username").expect("username");
        let site_admin: bool = row.try_get("site_admin").expect("site_admin");
        let live: bool = row.try_get("live").expect("live");
        println!("ep user id={id} name={username} admin={site_admin} live={live}");
    }

    for row in sqlx::query(
        "SELECT user_id, email::text AS email, verified_at IS NOT NULL AS verified, \
         is_primary, COALESCE(verification_code, '-') AS code \
         FROM user_emails WHERE user_id BETWEEN 9960 AND 10099 \
         ORDER BY user_id, email, verification_code NULLS FIRST",
    )
    .fetch_all(pool)
    .await
    .expect("select user_emails")
    {
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let email: String = row.try_get("email").expect("email");
        let verified: bool = row.try_get("verified").expect("verified");
        let primary: bool = row.try_get("is_primary").expect("is_primary");
        let code: String = row.try_get("code").expect("code");
        println!(
            "ep email uid={user_id} email={email} verified={verified} primary={primary} code={code}"
        );
    }

    for row in sqlx::query(
        "SELECT user_id, service_type, account_id, deleted_at IS NOT NULL AS deleted \
         FROM user_external_accounts WHERE user_id BETWEEN 9960 AND 10099 \
         ORDER BY user_id, service_type, account_id",
    )
    .fetch_all(pool)
    .await
    .expect("select user_external_accounts")
    {
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let service_type: String = row.try_get("service_type").expect("service_type");
        let account_id: String = row.try_get("account_id").expect("account_id");
        let deleted: bool = row.try_get("deleted").expect("deleted");
        println!("ep ext uid={user_id} st={service_type} aid={account_id} deleted={deleted}");
    }

    for row in sqlx::query(
        "SELECT ur.user_id, r.name FROM user_roles ur JOIN roles r ON r.id = ur.role_id \
         WHERE ur.user_id BETWEEN 9960 AND 10099 ORDER BY ur.user_id, r.name",
    )
    .fetch_all(pool)
    .await
    .expect("select user_roles")
    {
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let name: String = row.try_get("name").expect("name");
        println!("ep role uid={user_id} name={name}");
    }

    for row in sqlx::query(
        "SELECT user_id, repo_id FROM sub_repo_permissions \
         WHERE user_id BETWEEN 9960 AND 10099 ORDER BY user_id, repo_id",
    )
    .fetch_all(pool)
    .await
    .expect("select sub_repo_permissions")
    {
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        println!("ep srp uid={user_id} repo={repo_id}");
    }

    for row in sqlx::query(
        "SELECT COALESCE(user_id::text, '-') AS user_id, \
         COALESCE(org_id::text, '-') AS org_id, name::text AS name FROM names \
         WHERE user_id BETWEEN 9960 AND 10099 OR org_id BETWEEN 9960 AND 10099 \
         ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .expect("select names")
    {
        let user_id: String = row.try_get("user_id").expect("user_id");
        let org_id: String = row.try_get("org_id").expect("org_id");
        let name: String = row.try_get("name").expect("name");
        println!("ep name name={name} uid={user_id} oid={org_id}");
    }

    for row in sqlx::query("SELECT id, name::text AS name FROM orgs WHERE id BETWEEN 9960 AND 10099 ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("select orgs")
    {
        let id: i32 = row.try_get("id").expect("id");
        let name: String = row.try_get("name").expect("name");
        println!("ep org id={id} name={name}");
    }

    for row in sqlx::query(
        "SELECT name::text AS name, url, user_id, source, argument::text AS argument \
         FROM security_event_logs WHERE name = 'EmailVerified' \
         AND user_id BETWEEN 9960 AND 10099 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("select security_event_logs")
    {
        let name: String = row.try_get("name").expect("name");
        let url: String = row.try_get("url").expect("url");
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let source: String = row.try_get("source").expect("source");
        let argument: String = row.try_get("argument").expect("argument");
        println!("ep sec name={name} uid={user_id} url={url} src={source} arg={argument}");
    }

    let query = format!(
        "SELECT user_id, permission, object_type, {} FROM user_permissions \
         WHERE user_id BETWEEN 9810 AND 10099 ORDER BY user_id, permission, object_type",
        ids("object_ids_ints")
    );
    for row in sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select user_permissions")
    {
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let permission: String = row.try_get("permission").expect("permission");
        let object_type: String = row.try_get("object_type").expect("object_type");
        let ids: String = row.try_get("ids").expect("ids");
        println!("ep up uid={user_id} perm={permission} type={object_type} ids={ids}");
    }

    let query = format!(
        "SELECT repo_id, permission, unrestricted, {} FROM repo_permissions \
         WHERE repo_id BETWEEN 9810 AND 10099 ORDER BY repo_id, permission",
        ids("user_ids_ints")
    );
    for row in sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select repo_permissions")
    {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let permission: String = row.try_get("permission").expect("permission");
        let unrestricted: bool = row.try_get("unrestricted").expect("unrestricted");
        let ids: String = row.try_get("ids").expect("ids");
        println!("ep rp repo={repo_id} perm={permission} unrestricted={unrestricted} ids={ids}");
    }

    for row in sqlx::query(
        "SELECT COALESCE(user_id::text, '-') AS user_id, \
         COALESCE(user_external_account_id::text, '-') AS external_account_id, \
         repo_id, source FROM user_repo_permissions \
         WHERE user_id BETWEEN 9810 AND 10099 OR repo_id BETWEEN 9810 AND 10099 \
         ORDER BY user_repo_permissions.user_id NULLS FIRST, \
         user_repo_permissions.user_external_account_id NULLS FIRST, \
         user_repo_permissions.repo_id",
    )
    .fetch_all(pool)
    .await
    .expect("select user_repo_permissions")
    {
        let user_id: String = row.try_get("user_id").expect("user_id");
        let external_account_id: String = row
            .try_get("external_account_id")
            .expect("external_account_id");
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let source: String = row.try_get("source").expect("source");
        println!("ep urp uid={user_id} ext={external_account_id} repo={repo_id} source={source}");
    }

    let query = format!(
        "SELECT id, bind_id, service_type, service_id, {} FROM user_pending_permissions \
         WHERE id BETWEEN 98000 AND 98999 ORDER BY id",
        ids("object_ids_ints")
    );
    for row in sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select user_pending_permissions")
    {
        let id: i64 = row.try_get("id").expect("id");
        let bind_id: String = row.try_get("bind_id").expect("bind_id");
        let service_type: String = row.try_get("service_type").expect("service_type");
        let service_id: String = row.try_get("service_id").expect("service_id");
        let ids: String = row.try_get("ids").expect("ids");
        println!("ep upp id={id} bind={bind_id} st={service_type} sid={service_id} ids={ids}");
    }

    let query = format!(
        "SELECT repo_id, permission, {} FROM repo_pending_permissions \
         WHERE repo_id BETWEEN 9810 AND 10099 ORDER BY repo_id, permission",
        ids("user_ids_ints")
    );
    for row in sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select repo_pending_permissions")
    {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let permission: String = row.try_get("permission").expect("permission");
        let ids: String = row.try_get("ids").expect("ids");
        println!("ep rpp repo={repo_id} perm={permission} ids={ids}");
    }
}

#[tokio::test]
async fn authz_entrypoints_diff() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN is not set; skipping");
        return;
    };
    let cases_path = std::env::var("M922A_ENTRY_CASES").expect("M922A_ENTRY_CASES is not set");
    let data = std::fs::read_to_string(&cases_path).expect("read cases");
    let cases: Vec<Case> = serde_json::from_str(&data).expect("parse cases");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let schema = assemble_sdl(SDL).expect("schema");
    let effects = NoopEffects;

    for case in cases {
        let site = case_site(&case.bind_id, &case.security_event_location);
        let store = PgAdminStore::new(pool.clone()).with_site_configuration(&site);
        let error = match case.kind.as_str() {
            "verify_email" => {
                let deps = MoreDeps {
                    actor: Actor::User {
                        uid: case.user_id,
                    },
                    verify_email: Some(Arc::new(
                        PgVerifyEmailDb::new(
                            pool.clone(),
                            Actor::User {
                                uid: case.user_id,
                            },
                        )
                        .with_site_configuration(&site),
                    )),
                    ..MoreDeps::default()
                };
                let request = http::Request::builder()
                    .uri(format!(
                        "/-/verify-email?email={}&code={}",
                        case.email, case.code
                    ))
                    .body(())
                    .expect("request");
                let response = verify_email(&request, &deps).await;
                response.status() != http::StatusCode::FOUND
            }
            "set_verified" => {
                let query = format!(
                    "mutation {{ setUserEmailVerified(user: \"{}\", email: \"{}\", verified: {}) \
                     {{ alwaysNil }} }}",
                    relay_id(case.user_id),
                    case.email,
                    case.verified
                );
                let actor = Actor::User {
                    uid: case.creator,
                };
                let request = ExecRequest {
                    query: &query,
                    operation_name: "",
                    variables: &serde_json::Map::new(),
                    actor,
                };
                let fields = root_fields(&schema, &request);
                let outcomes =
                    prefetch_user_email_mutations(&store, &effects, &fields, actor, true).await;
                matches!(
                    outcomes.first(),
                    Some((_, AdminMutationOutcome::Error(_))) | None
                )
            }
            "create_user" => {
                let query = format!(
                    "mutation {{ createUser(username: \"{}\", email: \"{}\", verifiedEmail: true) \
                     {{ user {{ id }} }} }}",
                    case.username, case.email
                );
                let actor = Actor::User {
                    uid: case.creator,
                };
                let request = ExecRequest {
                    query: &query,
                    operation_name: "",
                    variables: &serde_json::Map::new(),
                    actor,
                };
                let fields = root_fields(&schema, &request);
                let outcomes =
                    prefetch_user_admin_mutations(&store, &effects, &fields, actor, true).await;
                matches!(
                    outcomes.first(),
                    Some((_, AdminMutationOutcome::Error(_))) | None
                )
            }
            "update_username" => {
                let update = UserUpdate {
                    username: Some(case.username.clone()),
                    display_name: None,
                    avatar_url: None,
                };
                store.update_user(case.user_id, &update).await.is_err()
            }
            "create_org" => store.create_org(&case.org_name, None).await.is_err(),
            "oauth_signup" | "oauth_upsert" => {
                let provision =
                    PgProvisionStore::new(pool.clone()).with_site_configuration(&site);
                let suffix = FixedSuffix;
                let deps = ProvisionDeps {
                    store: &provision,
                    suffix: &suffix,
                    sourcegraph_dotcom: false,
                };
                let identity = ExternalIdentity {
                    service_type: &case.service_type,
                    service_id: &case.service_id,
                    client_id: "",
                    account_id: &case.account_id,
                    login: &case.username,
                    email: &case.email,
                    email_verified: true,
                    display_name: "",
                    avatar_url: "",
                    create_if_not_exist: true,
                };
                get_or_create_user(&deps, &identity).await.is_err()
            }
            other => panic!("unknown case kind {other}"),
        };
        println!("ep case={} error={}", case.name, error);
        snapshot(&pool).await;
    }
}
