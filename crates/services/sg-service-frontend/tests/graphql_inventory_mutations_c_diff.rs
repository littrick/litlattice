//! M9.18a2 differential of the 19 batch-C GraphQL mutations against
//! graph-gophers + the real `schemaResolver`, backed by the same scratch
//! database fixtures as the Go oracle (`zz_m9_18a2_mutations_oracle_test.go`).
//!
//! Covered mutations:
//!
//! `changeCodyPlan` / `createWebhook` / `updateWebhook` / `deleteWebhook` /
//! `checkMirrorRepositoryConnection` / `updateMirrorRepository` /
//! `randomizeUserPassword` / `setTosAccepted` / `addPhabricatorRepo` /
//! `resolvePhabricatorDiff` / `setUserIsSiteAdmin` / `reloadSite` /
//! `triggerObservabilityTestAlert` / `sendTestEmail` / `syncExternalService` /
//! `cancelExternalServiceSync` / `addPackageRepoFilter` /
//! `updatePackageRepoFilter` / `deletePackageRepoFilter`.
//!
//! Env:
//!
//! * `SG_M9_18A2_MUTATIONS_DIFF=1`
//! * `M9_18A2_MUTATIONS_CORPUS` — JSON `[{name, query, variables?, uid?, mode?}]`
//! * `M9_18A2_MUTATIONS_OUT` — output directory (one `<name>.json` per case)
//! * `M9_18A2_MUTATIONS_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;

use sg_graphql::schema::assemble_oss;
use sg_service_frontend::graphql_api::{GraphqlDeps, GraphqlExecRequest, SchemaGraphqlDeps};
use sg_service_frontend::graphql_inventory_c::{
    InventoryCDeps, PasswordResetEmailSender, PasswordResetUrlProvider, PhabricatorGit,
    PhabricatorPatchRequest, RepoMirrorChecker, RepoUpdateEnqueuer, TestEmailSender,
};

/// The fake gitserver commit returned by `ResolveRevision` /
/// `RepositoryResolver.Commit` (matches the Go oracle).
const COMMIT_ID: &str = "0123456789abcdef0123456789abcdef01234567";

#[derive(Serialize)]
struct Envelope {
    data: Value,
    errors: Vec<Value>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    variables: Map<String, Value>,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    mode: String,
}

struct FakeMirror {
    error: Option<&'static str>,
}

#[async_trait::async_trait]
impl RepoMirrorChecker for FakeMirror {
    async fn is_repo_cloneable(&self, _repo_name: &str) -> Result<(), String> {
        match self.error {
            Some(message) => Err(message.to_owned()),
            None => Ok(()),
        }
    }
}

struct FakeEnqueuer {
    error: Option<&'static str>,
}

#[async_trait::async_trait]
impl RepoUpdateEnqueuer for FakeEnqueuer {
    async fn enqueue_repo_update(&self, _repo_name: &str) -> Result<(), String> {
        match self.error {
            Some(message) => Err(message.to_owned()),
            None => Ok(()),
        }
    }
}

struct FakeTestEmail {
    error: Option<&'static str>,
}

#[async_trait::async_trait]
impl TestEmailSender for FakeTestEmail {
    async fn send_test_email(&self, _to: &str, _test_id: &str) -> Result<(), String> {
        match self.error {
            Some(message) => Err(message.to_owned()),
            None => Ok(()),
        }
    }
}

struct FakePhabricatorGit {
    revision_found: bool,
    created: AtomicBool,
}

#[async_trait::async_trait]
impl PhabricatorGit for FakePhabricatorGit {
    async fn resolve_revision(&self, _repo: &str, _rev: &str) -> Result<bool, String> {
        Ok(self.revision_found || self.created.load(Ordering::SeqCst))
    }

    async fn create_commit_from_patch(
        &self,
        _request: &PhabricatorPatchRequest,
    ) -> Result<(), String> {
        self.created.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn commit_oid(&self, _repo: &str, _rev: &str) -> Result<Option<String>, String> {
        if self.revision_found || self.created.load(Ordering::SeqCst) {
            Ok(Some(COMMIT_ID.to_owned()))
        } else {
            Ok(None)
        }
    }
}

struct FakePasswordResetUrl;

#[async_trait::async_trait]
impl PasswordResetUrlProvider for FakePasswordResetUrl {
    async fn make_password_reset_url(&self, user_id: i32, _email: &str) -> Result<String, String> {
        // Mirrors the Go oracle's `backend.MockMakePasswordResetURL`.
        Ok(format!("/password-reset?userID={user_id}&code=fixed"))
    }
}

struct FakePasswordResetEmail {
    error: Option<&'static str>,
}

#[async_trait::async_trait]
impl PasswordResetEmailSender for FakePasswordResetEmail {
    async fn send_reset_password_email(
        &self,
        _user_id: i32,
        _reset_url: &str,
    ) -> Result<(), String> {
        match self.error {
            Some(message) => Err(message.to_owned()),
            None => Ok(()),
        }
    }
}

/// Per-case seam/configuration selection, mirroring `m9a2Configure` in the Go
/// oracle.
fn deps_for_mode(mode: &str) -> InventoryCDeps {
    let mut deps = InventoryCDeps {
        mirror_checker: Arc::new(FakeMirror { error: None }),
        repo_updater: Arc::new(FakeEnqueuer { error: None }),
        test_email: Arc::new(FakeTestEmail { error: None }),
        phabricator_git: Arc::new(FakePhabricatorGit {
            revision_found: false,
            created: AtomicBool::new(false),
        }),
        password_reset_url: Arc::new(FakePasswordResetUrl),
        password_reset_email: Arc::new(FakePasswordResetEmail { error: None }),
        ..InventoryCDeps::default()
    };
    match mode {
        "dotcom" => deps.dotcom = true,
        "reset" => deps.reset_password_enabled = true,
        "reset_email_ok" => {
            deps.reset_password_enabled = true;
            deps.can_send_email = true;
        }
        "reset_email_err" => {
            deps.reset_password_enabled = true;
            deps.can_send_email = true;
            deps.password_reset_email = Arc::new(FakePasswordResetEmail {
                error: Some("smtp connection refused"),
            });
        }
        "reset_dotcom_no_email" => {
            deps.reset_password_enabled = true;
            deps.dotcom = true;
        }
        "reset_email_dotcom_ok" => {
            deps.reset_password_enabled = true;
            deps.dotcom = true;
            deps.can_send_email = true;
        }
        "reset_email_dotcom_err" => {
            deps.reset_password_enabled = true;
            deps.dotcom = true;
            deps.can_send_email = true;
            deps.password_reset_email = Arc::new(FakePasswordResetEmail {
                error: Some("smtp connection refused"),
            });
        }
        "mirror_err" => {
            deps.mirror_checker = Arc::new(FakeMirror {
                error: Some("mirror unreachable"),
            });
        }
        "enqueue_err" => {
            deps.repo_updater = Arc::new(FakeEnqueuer {
                error: Some("repo updater unavailable"),
            });
        }
        "reload" => deps.can_reload_site = true,
        "test_email_err" => {
            deps.test_email = Arc::new(FakeTestEmail {
                error: Some("smtp connection refused"),
            });
        }
        "phab_revision_found" => {
            deps.phabricator_git = Arc::new(FakePhabricatorGit {
                revision_found: true,
                created: AtomicBool::new(false),
            });
        }
        // `phab_patch`, `phab_no_client`, `phab_origin_missing`, `default`,
        // `test_email_ok` and `` use the plain seams above.
        _ => {}
    }
    deps
}

#[tokio::test]
async fn graphql_inventory_mutations_c_harness() {
    if std::env::var("SG_M9_18A2_MUTATIONS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9_18A2_MUTATIONS_CORPUS").expect("M9_18A2_MUTATIONS_CORPUS");
    let out = std::env::var("M9_18A2_MUTATIONS_OUT").expect("M9_18A2_MUTATIONS_OUT");
    let dsn = std::env::var("M9_18A2_MUTATIONS_DSN").expect("M9_18A2_MUTATIONS_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = Arc::new(assemble_oss().expect("oss schema"));
    let pool = PgPool::connect(&dsn).await.expect("connect");

    for case in cases {
        let deps = SchemaGraphqlDeps::with_session(Arc::clone(&schema), pool.clone())
            .with_inventory_c_deps(deps_for_mode(&case.mode));
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
            })
            .await;

        let data = response
            .data
            .as_ref()
            .map(|data| serde_json::from_str::<Value>(data).expect("data json"))
            .unwrap_or(Value::Null);
        let errors = response
            .errors
            .iter()
            .map(|_| json!({"message": "error"}))
            .collect::<Vec<_>>();
        let envelope = Envelope { data, errors };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
