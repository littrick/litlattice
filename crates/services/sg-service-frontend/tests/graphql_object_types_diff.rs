//! M9.19a differential of the GraphQL object types implemented on top of the
//! gitserver/zoekt/database seams: `KeyValuePair`, `ExternalLink`,
//! `RepoMetadata` (+ keys/values connections), `GitRef` connections
//! (`gitRefs`/`branches`/`tags`), `RepositoryContributor` connections,
//! `RepositoryTextSearchIndex` (+ refs/status), `Hunk` (blame),
//! `Webhook.webhookLogs` and `Repository.metadata`/`keyValuePairs`.
//!
//! The Go oracle (`zz_m9_19a_object_types_oracle_test.go`) drives the real
//! `schemaResolver` against the same scratch database fixtures. Both sides use
//! the same deterministic fake gitserver and zoekt data, injected through the
//! `GitObjectSource` / `ZoektIndexSource` seams here and through a fake gRPC
//! gitserver + fake zoekt webserver there.
//!
//! `Query.webhookLogs` / `Webhook.webhookLogs` read the real `webhook_logs`
//! table; the fixture rows are stored in plaintext.
//!
//! Env:
//!
//! * `SG_M9_19A_OBJECT_TYPES_DIFF=1`
//! * `M9_19A_CORPUS` — JSON `[{name, query, variables?, uid?}]`
//! * `M9_19A_OUT` — output directory (one `<name>.json` per case)
//! * `M9_19A_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::PgPool;

use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_git::{BlameHunk, FileStat, GitObject, GitObjectType, GitRef, RefType, Signature};
use sg_graphql::exec::Actor;
use sg_graphql::schema::assemble_oss;
use sg_keyring::Keyring;
use sg_service_frontend::external_services::PgExternalServiceConnectionStore;
use sg_service_frontend::git_graphql::{ContributorCountData, GitObjectSource, GitSourceError};
use sg_service_frontend::graphql_api::{GraphqlDeps, GraphqlExecRequest, SchemaGraphqlDeps};
use sg_service_frontend::graphql_inventory_d::{
    InventoryDDeps, TextSearchIndexBranch, TextSearchIndexEntry, ZoektIndexSource,
};
use sg_service_frontend::webhook_logs::PgWebhookLogConnectionStore;

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
}

const ALPHA_HEAD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BETA_MAIN: &str = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1";
const BETA_DEV: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";
const BETA_TAG: &str = "b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3";
const GAMMA_MAIN: &str = "7777777777777777777777777777777777777777";
const GAMMA_RELEASE_TARGET: &str = "8888888888888888888888888888888888888888";
const GAMMA_RELEASE_INDEXED: &str = "9999999999999999999999999999999999999999";
const HUNK_COMMIT_1: &str = "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1";
const HUNK_COMMIT_2: &str = "c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2";

fn ts(seconds: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos: 0 })
}

/// Deterministic `GitObjectSource` mirroring the Go oracle's injected
/// `gitserver.MockClient` + fake gRPC gitserver (ListRefs/ContributorCounts).
struct M9Git;

impl M9Git {
    fn alpha_hunks(range: Option<(i32, i32)>) -> Vec<BlameHunk> {
        let hunks = vec![
            BlameHunk {
                start_line: 1,
                end_line: 2,
                start_byte: 0,
                end_byte: 20,
                commit_id: HUNK_COMMIT_1.to_owned(),
                previous_commit: None,
                author: Signature {
                    name: "Ann Author".to_owned(),
                    email: "ann@example.com".to_owned(),
                    date: ts(1_577_934_245),
                },
                message: "first hunk".to_owned(),
                filename: "a.go".to_owned(),
            },
            BlameHunk {
                start_line: 3,
                end_line: 3,
                start_byte: 20,
                end_byte: 40,
                commit_id: HUNK_COMMIT_2.to_owned(),
                previous_commit: None,
                author: Signature {
                    name: "Bob Blamer".to_owned(),
                    email: "bob@example.com".to_owned(),
                    date: ts(1_580_702_706),
                },
                message: "second hunk".to_owned(),
                filename: "a.go".to_owned(),
            },
        ];
        match range {
            None => hunks,
            Some((start, end)) => hunks
                .into_iter()
                .filter(|hunk| hunk.end_line as i32 >= start && hunk.start_line as i32 <= end)
                .collect(),
        }
    }
}

#[async_trait]
impl GitObjectSource for M9Git {
    async fn resolve_revision(
        &self,
        _repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<String, GitSourceError> {
        match repo_name {
            "github.com/m9d/alpha" => {
                if rev == "HEAD" || rev == ALPHA_HEAD {
                    Ok(ALPHA_HEAD.to_owned())
                } else {
                    Err(GitSourceError::revision_not_found(format!(
                        "revision not found: {rev}"
                    )))
                }
            }
            "github.com/m9d/gamma" => match rev {
                "refs/heads/main" => Ok(GAMMA_MAIN.to_owned()),
                "refs/heads/release" => Ok(GAMMA_RELEASE_TARGET.to_owned()),
                GAMMA_MAIN => Ok(GAMMA_MAIN.to_owned()),
                GAMMA_RELEASE_TARGET => Ok(GAMMA_RELEASE_TARGET.to_owned()),
                GAMMA_RELEASE_INDEXED => Ok(GAMMA_RELEASE_INDEXED.to_owned()),
                _ => Err(GitSourceError::revision_not_found(format!(
                    "revision not found: {rev}"
                ))),
            },
            _ => {
                if rev.len() == 40 && rev.chars().all(|c| c.is_ascii_hexdigit()) {
                    Ok(rev.to_owned())
                } else {
                    Err(GitSourceError::revision_not_found(format!(
                        "revision not found: {rev}"
                    )))
                }
            }
        }
    }

    async fn get_commit(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        oid: &str,
    ) -> Result<sg_git::CommitDetails, GitSourceError> {
        Err(GitSourceError::revision_not_found(format!(
            "commit not found: {oid}"
        )))
    }

    async fn get_object(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        revspec: &str,
    ) -> Result<GitObject, GitSourceError> {
        let oid = match revspec {
            "refs/heads/main" => GAMMA_MAIN,
            "refs/heads/release" => GAMMA_RELEASE_TARGET,
            other => other,
        };
        Ok(GitObject {
            oid: oid.to_owned(),
            object_type: GitObjectType::Commit,
        })
    }

    async fn stat(
        &self,
        _repo_id: i32,
        repo_name: &str,
        _oid: &str,
        path: &str,
    ) -> Result<FileStat, GitSourceError> {
        if repo_name == "github.com/m9d/alpha" && matches!(path, "a.go" | "boom.go") {
            return Ok(FileStat {
                name: path.to_owned(),
                size: 42,
                mode: 0o100644,
                blob_oid: "d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0".to_owned(),
                submodule: None,
            });
        }
        Err(GitSourceError::not_exist(format!(
            "stat: {path}: not found"
        )))
    }

    async fn list_refs(
        &self,
        _repo_id: i32,
        repo_name: &str,
        heads_only: bool,
        tags_only: bool,
    ) -> Result<Vec<GitRef>, GitSourceError> {
        if repo_name == "github.com/m9d/zeta" {
            return Err(GitSourceError::new("gitserver list refs failed"));
        }
        if repo_name != "github.com/m9d/beta" {
            return Ok(Vec::new());
        }
        let refs = vec![
            GitRef {
                name: "refs/heads/main".to_owned(),
                short_name: "main".to_owned(),
                target_commit: BETA_MAIN.to_owned(),
                ref_oid: BETA_MAIN.to_owned(),
                created_at: ts(1_577_934_245),
                ref_type: RefType::Branch,
                is_head: true,
            },
            GitRef {
                name: "refs/heads/dev".to_owned(),
                short_name: "dev".to_owned(),
                target_commit: BETA_DEV.to_owned(),
                ref_oid: BETA_DEV.to_owned(),
                created_at: ts(1_577_934_245),
                ref_type: RefType::Branch,
                is_head: false,
            },
            GitRef {
                name: "refs/tags/v1.0.0".to_owned(),
                short_name: "v1.0.0".to_owned(),
                target_commit: BETA_TAG.to_owned(),
                ref_oid: BETA_TAG.to_owned(),
                created_at: ts(1_577_934_245),
                ref_type: RefType::Tag,
                is_head: false,
            },
        ];
        Ok(refs
            .into_iter()
            // `Repository.GitRefs` passes `headsOnly`/`tagsOnly` from the
            // GraphQL `type` argument; when it is absent both flags are true
            // and the baseline applies no ref-type filter (repository_git_refs.go).
            .filter(|git_ref| {
                if heads_only && tags_only {
                    return true;
                }
                let is_branch = git_ref.ref_type == RefType::Branch;
                let is_tag = git_ref.ref_type == RefType::Tag;
                (!heads_only || is_branch) && (!tags_only || is_tag)
            })
            .collect())
    }

    async fn default_branch(
        &self,
        _repo_id: i32,
        repo_name: &str,
    ) -> Result<Option<String>, GitSourceError> {
        match repo_name {
            "github.com/m9d/eps" => Err(GitSourceError::new("gitserver default branch failed")),
            "github.com/m9d/eta" => Ok(None),
            "github.com/m9d/alpha"
            | "github.com/m9d/beta"
            | "github.com/m9d/gamma"
            | "github.com/m9d/zeta" => Ok(Some("refs/heads/main".to_owned())),
            _ => Ok(None),
        }
    }

    async fn contributor_count(
        &self,
        _repo_id: i32,
        repo_name: &str,
        _revision_range: Option<&str>,
        _after_date: Option<&str>,
        _path: Option<&str>,
    ) -> Result<Vec<ContributorCountData>, GitSourceError> {
        if repo_name == "github.com/m9d/zeta" {
            return Err(GitSourceError::new("gitserver contributor count failed"));
        }
        if repo_name != "github.com/m9d/beta" {
            return Ok(Vec::new());
        }
        Ok(vec![
            ContributorCountData {
                name: "Alice Dev".to_owned(),
                email: "alice@example.com".to_owned(),
                count: 7,
            },
            ContributorCountData {
                name: "Bob Dev".to_owned(),
                email: "bob@example.com".to_owned(),
                count: 3,
            },
        ])
    }

    async fn blame(
        &self,
        _repo_id: i32,
        repo_name: &str,
        _oid: &str,
        path: &str,
        start_line: Option<i32>,
        end_line: Option<i32>,
        _ignore_whitespace: bool,
    ) -> Result<Vec<BlameHunk>, GitSourceError> {
        if repo_name != "github.com/m9d/alpha" {
            return Err(GitSourceError::new("no blame for this repository"));
        }
        if path == "boom.go" {
            return Err(GitSourceError::new("gitserver blame failed"));
        }
        if path != "a.go" {
            return Err(GitSourceError::new("no blame for this path"));
        }
        let range = match (start_line, end_line) {
            (Some(start), Some(end)) => Some((start, end)),
            _ => None,
        };
        Ok(Self::alpha_hunks(range))
    }
}

/// Deterministic `ZoektIndexSource` mirroring the Go oracle's fake zoekt
/// webserver: only gamma is indexed, with a `HEAD` branch (the default branch)
/// and a `release` branch whose indexed commit is newer than its current one.
struct M9Zoekt;

#[async_trait]
impl ZoektIndexSource for M9Zoekt {
    async fn repo_entry(&self, repo_id: i32) -> Result<Option<TextSearchIndexEntry>, String> {
        if repo_id != 9903 {
            return Ok(None);
        }
        Ok(Some(TextSearchIndexEntry {
            index_time: time::macros::datetime!(2021-05-06 07:08:09 UTC),
            content_bytes: 12345,
            documents: 7,
            index_bytes: 2345,
            shards: 1,
            new_lines: 111,
            default_branch_new_lines: 100,
            other_branches_new_lines: 11,
            branches: vec![
                TextSearchIndexBranch {
                    name: "HEAD".to_owned(),
                    version: GAMMA_MAIN.to_owned(),
                },
                TextSearchIndexBranch {
                    name: "release".to_owned(),
                    version: GAMMA_RELEASE_INDEXED.to_owned(),
                },
            ],
        }))
    }

    async fn indexserver_host(&self, _repo_name: &str) -> Option<String> {
        None
    }

    async fn skipped_count(&self, _repo_id: i32, _branch: &str) -> Result<i64, String> {
        Ok(0)
    }

    async fn canonical_ref(&self, _repo_name: &str, name: &str) -> String {
        // `refByName` probes `refs/heads/<name>` then `refs/tags/<name>`; the
        // fake gitserver only resolves `refs/heads/main`/`refs/heads/release`.
        if name.starts_with("refs/") {
            return name.to_owned();
        }
        if matches!(name, "main" | "release") {
            return format!("refs/heads/{name}");
        }
        name.to_owned()
    }

    async fn ref_target_oid(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        ref_name: &str,
    ) -> Result<String, String> {
        match ref_name {
            "refs/heads/main" => Ok(GAMMA_MAIN.to_owned()),
            "refs/heads/release" => Ok(GAMMA_RELEASE_TARGET.to_owned()),
            _ => Err("unknown ref".to_owned()),
        }
    }
}

#[tokio::test]
async fn graphql_object_types_harness() {
    if std::env::var("SG_M9_19A_OBJECT_TYPES_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9_19A_CORPUS").expect("M9_19A_CORPUS");
    let out = std::env::var("M9_19A_OUT").expect("M9_19A_OUT");
    let dsn = std::env::var("M9_19A_DSN").expect("M9_19A_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = Arc::new(assemble_oss().expect("oss schema"));
    let pool = PgPool::connect(&dsn).await.expect("connect");

    let live = Arc::new(LiveConfig::new(Unified {
        site: SiteConfiguration::parse("{}").expect("site config"),
        service_connections: ServiceConnections::default(),
    }));
    let deps = SchemaGraphqlDeps::with_session(Arc::clone(&schema), pool.clone())
        .with_git_objects(Arc::new(M9Git))
        .with_inventory_d_deps(InventoryDDeps {
            zoekt: Arc::new(M9Zoekt),
        })
        .with_webhook_logs(
            Arc::new(PgWebhookLogConnectionStore::new(pool.clone())),
            Arc::new(PgExternalServiceConnectionStore::new(pool)),
            Arc::new(Keyring::default()),
            live,
        );

    for case in cases {
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
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

        let data = response
            .data
            .as_ref()
            .map(|data| serde_json::from_str::<Value>(data).expect("data json"))
            .unwrap_or(Value::Null);
        if std::env::var("M9_19A_RAW").as_deref() == Ok("1") {
            eprintln!("RAWERR {}: {:?}", case.name, response.errors);
        }
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
