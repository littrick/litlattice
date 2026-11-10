//! M9.8gh differential of `Query.externalServices` and the read-only
//! `ExternalService` fields against the Go oracle
//! (`cmd/frontend/graphqlbackend/zz_m9gh_external_services_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9GH_DIFF=1`
//! * `M9GH_CORPUS` — JSON `[{name, uid, first?, afterID?, afterRaw?, repoID?, repoRaw?}]`
//! * `M9GH_OUT` — output directory (one `<name>.json` per case)
//! * `M9GH_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use time::{OffsetDateTime, UtcOffset};

use sg_encryption::{Key, NoopKey};
use sg_graphql::exec::Actor;
use sg_service_frontend::external_services::{
    external_services, marshal_external_service_id, ExternalServiceNodeData, ExternalServicesArgs,
    ExternalServicesConnectionLookup, PgExternalServiceConnectionStore,
};
use sg_service_frontend::graphql_resolvers::{UserData, UserKey, UserLookupStore};
use sg_service_frontend::user_store::PgUserStore;

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    first: Option<i32>,
    #[serde(default, rename = "afterID")]
    after_id: Option<i64>,
    #[serde(default, rename = "afterRaw")]
    after_raw: Option<String>,
    #[serde(default, rename = "repoID")]
    repo_id: Option<i32>,
    #[serde(default, rename = "repoRaw")]
    repo_raw: Option<String>,
}

/// Field order matches the Go oracle's `m9ghEnvelope`.
#[derive(Serialize)]
struct Envelope {
    error: bool,
    nodes: Vec<Node>,
    #[serde(rename = "totalCount")]
    total_count: i64,
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
}

#[derive(Serialize, Default)]
struct PageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

#[derive(Serialize)]
struct Node {
    id: String,
    kind: String,
    #[serde(rename = "displayName")]
    display_name: String,
    config: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
    creator: Option<String>,
    #[serde(rename = "lastUpdater")]
    last_updater: Option<String>,
    warning: Option<String>,
    #[serde(rename = "lastSyncError")]
    last_sync_error: Option<String>,
    #[serde(rename = "repoCount")]
    repo_count: i32,
    #[serde(rename = "lastSyncAt")]
    last_sync_at: Option<String>,
    #[serde(rename = "nextSyncAt")]
    next_sync_at: Option<String>,
    unrestricted: bool,
    #[serde(rename = "supportsRepoExclusion")]
    supports_repo_exclusion: bool,
}

impl Envelope {
    fn error() -> Self {
        Self {
            error: true,
            nodes: Vec::new(),
            total_count: 0,
            page_info: PageInfo::default(),
        }
    }
}

impl From<ExternalServiceNodeData> for Node {
    fn from(data: ExternalServiceNodeData) -> Self {
        Self {
            id: marshal_external_service_id(data.id),
            kind: data.kind,
            display_name: data.display_name,
            config: data.config,
            created_at: datetime(data.created_at),
            updated_at: datetime(data.updated_at),
            creator: data.creator.as_ref().map(|user| user.username.clone()),
            last_updater: data.last_updater.as_ref().map(|user| user.username.clone()),
            warning: data.warning,
            last_sync_error: data.last_sync_error,
            repo_count: data.repo_count,
            last_sync_at: data.last_sync_at.map(datetime),
            next_sync_at: data.next_sync_at.map(datetime),
            unrestricted: data.unrestricted,
            supports_repo_exclusion: data.supports_repo_exclusion,
        }
    }
}

/// `gqlutil.DateTime.MarshalJSON`: UTC RFC3339, seconds precision.
fn datetime(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

fn marshal_repository_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Repository:{id}"))
}

#[tokio::test]
async fn external_services_harness() {
    if std::env::var("SG_M9GH_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GH_CORPUS").expect("M9GH_CORPUS");
    let out = std::env::var("M9GH_OUT").expect("M9GH_OUT");
    let dsn = std::env::var("M9GH_DSN").expect("M9GH_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let store = PgExternalServiceConnectionStore::new(pool.clone());
    let users = PgUserStore::new(pool);
    let key: Arc<dyn Key> = Arc::new(NoopKey::default());

    for case in cases {
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let current_user: Option<UserData> = if case.uid == 0 {
            None
        } else {
            users
                .lookup(&UserKey::DatabaseId(case.uid))
                .await
                .expect("user lookup")
        };

        let after = match (&case.after_raw, case.after_id) {
            (Some(raw), _) => Some(raw.clone()),
            (None, Some(id)) => Some(marshal_external_service_id(id)),
            (None, None) => None,
        };
        let repo = match (&case.repo_raw, case.repo_id) {
            (Some(raw), _) => Some(raw.clone()),
            (None, Some(id)) => Some(marshal_repository_id(id)),
            (None, None) => None,
        };
        let args = ExternalServicesArgs {
            first: case.first,
            after,
            repo,
        };

        let envelope = match external_services(
            &args,
            &store,
            Some(&users),
            Some(key.clone()),
            "",
            actor,
            current_user.as_ref(),
        )
        .await
        {
            ExternalServicesConnectionLookup::Found(data) => Envelope {
                error: false,
                nodes: data.nodes.into_iter().map(Node::from).collect(),
                total_count: data.total_count,
                page_info: PageInfo {
                    has_next_page: data.has_next_page,
                    end_cursor: data.end_cursor,
                },
            },
            ExternalServicesConnectionLookup::Error(message) => {
                eprintln!("CASE {} error: {message}", case.name);
                Envelope::error()
            }
        };

        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
