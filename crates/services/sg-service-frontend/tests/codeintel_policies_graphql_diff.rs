//! M9.28c differential of the Code Intelligence configuration-policy GraphQL
//! surface against the Go baseline
//! (`internal/codeintel/policies/transport/graphql` + the real policies store).
//!
//! Both sides read the scratch database seeded by
//! `run_codeintel_policies_graphql_diff.sh` and consume
//! `data/codeintel_policies/corpus.json`. The Go oracle and this harness call
//! the production resolvers directly (not GraphQL strings) and serialize the
//! same documents. Git refs/commits are served by a fake `GitObjectSource`
//! mirroring the oracle's mock gitserver fixtures.

#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::postgres::PgPoolOptions;

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use sg_git::RefType;
use sg_service_frontend::codeintel_policies_graphql::{
    create_configuration_policy, code_intelligence_configuration_policies,
    delete_configuration_policy, node_field,
    preview_git_object_filter, preview_repository_filter, update_configuration_policy,
    CodeIntelPoliciesHandle,
};
use sg_service_frontend::codeintel_policies_graphql_store::PgCodeIntelPoliciesStore;
use sg_service_frontend::git_graphql::{GitObjectSource, GitSourceError};

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct ArgumentsFixture {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    policy: Option<String>,
    #[serde(default)]
    repository: Option<String>,
    #[serde(default, rename = "repositoryPatterns")]
    repository_patterns: Option<Vec<String>>,
    #[serde(default)]
    patterns: Option<Vec<String>>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default, rename = "type")]
    policy_type: Option<String>,
    #[serde(default)]
    pattern: Option<String>,
    #[serde(default, rename = "retentionEnabled")]
    retention_enabled: Option<bool>,
    #[serde(default, rename = "retentionDurationHours")]
    retention_duration_hours: Option<i64>,
    #[serde(default, rename = "retainIntermediateCommits")]
    retain_intermediate_commits: Option<bool>,
    #[serde(default, rename = "indexingEnabled")]
    indexing_enabled: Option<bool>,
    #[serde(default, rename = "syntacticIndexingEnabled")]
    syntactic_indexing_enabled: Option<bool>,
    #[serde(default, rename = "indexCommitMaxAgeHours")]
    index_commit_max_age_hours: Option<i64>,
    #[serde(default, rename = "indexIntermediateCommits")]
    index_intermediate_commits: Option<bool>,
    #[serde(default, rename = "embeddingsEnabled")]
    embeddings_enabled: Option<bool>,
    #[serde(default)]
    first: Option<i64>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    protected: Option<bool>,
    #[serde(default, rename = "forDataRetention")]
    for_data_retention: Option<bool>,
    #[serde(default, rename = "forPreciseIndexing")]
    for_precise_indexing: Option<bool>,
    #[serde(default, rename = "forSyntacticIndexing")]
    for_syntactic_indexing: Option<bool>,
    #[serde(default, rename = "forEmbeddings")]
    for_embeddings: Option<bool>,
    #[serde(default, rename = "countObjectsYoungerThanHours")]
    count_objects_younger_than_hours: Option<i64>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    op: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    repo: Option<i32>,
    #[serde(default)]
    args: ArgumentsFixture,
}

fn arguments_from_fixture(fixture: &ArgumentsFixture, include_id: bool) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = Vec::new();
    let mut push = |name: &str, value: InputValue| entries.push((name.to_owned(), value));
    if include_id {
        if let Some(id) = &fixture.id {
            push("id", InputValue::Str(id.clone()));
        }
    }
    if let Some(value) = &fixture.policy {
        push("policy", InputValue::Str(value.clone()));
    }
    if let Some(value) = &fixture.repository {
        push("repository", InputValue::Str(value.clone()));
    }
    if let Some(values) = &fixture.repository_patterns {
        push(
            "repositoryPatterns",
            InputValue::List(values.iter().cloned().map(InputValue::Str).collect()),
        );
    }
    if let Some(values) = &fixture.patterns {
        push(
            "patterns",
            InputValue::List(values.iter().cloned().map(InputValue::Str).collect()),
        );
    }
    if let Some(value) = &fixture.name {
        push("name", InputValue::Str(value.clone()));
    }
    if let Some(value) = &fixture.policy_type {
        push("type", InputValue::Str(value.clone()));
    }
    if let Some(value) = &fixture.pattern {
        push("pattern", InputValue::Str(value.clone()));
    }
    for (name, value) in [
        ("retentionEnabled", fixture.retention_enabled),
        ("retainIntermediateCommits", fixture.retain_intermediate_commits),
        ("indexingEnabled", fixture.indexing_enabled),
        ("syntacticIndexingEnabled", fixture.syntactic_indexing_enabled),
        ("indexIntermediateCommits", fixture.index_intermediate_commits),
        ("embeddingsEnabled", fixture.embeddings_enabled),
        ("protected", fixture.protected),
        ("forDataRetention", fixture.for_data_retention),
        ("forPreciseIndexing", fixture.for_precise_indexing),
        ("forSyntacticIndexing", fixture.for_syntactic_indexing),
        ("forEmbeddings", fixture.for_embeddings),
    ] {
        if let Some(value) = value {
            push(name, InputValue::Bool(value));
        }
    }
    for (name, value) in [
        ("retentionDurationHours", fixture.retention_duration_hours),
        ("indexCommitMaxAgeHours", fixture.index_commit_max_age_hours),
        ("first", fixture.first),
        ("countObjectsYoungerThanHours", fixture.count_objects_younger_than_hours),
    ] {
        if let Some(value) = value {
            push(name, InputValue::Int(value));
        }
    }
    if let Some(value) = &fixture.after {
        push("after", InputValue::Str(value.clone()));
    }
    if let Some(value) = &fixture.query {
        push("query", InputValue::Str(value.clone()));
    }
    Arguments::from_entries(entries)
}

fn fields_for(type_name: &str) -> Option<&'static [&'static str]> {
    match type_name {
        "CodeIntelligenceConfigurationPolicyConnection" => {
            Some(&["nodes", "totalCount", "pageInfo"])
        }
        "CodeIntelligenceConfigurationPolicy" => Some(&[
            "id",
            "name",
            "repository",
            "repositoryPatterns",
            "type",
            "pattern",
            "protected",
            "retentionEnabled",
            "retentionDurationHours",
            "retainIntermediateCommits",
            "indexingEnabled",
            "syntacticIndexingEnabled",
            "indexCommitMaxAgeHours",
            "indexIntermediateCommits",
            "embeddingsEnabled",
        ]),
        "CodeIntelRepository" => Some(&["name"]),
        "RepositoryFilterPreview" => {
            Some(&["nodes", "totalCount", "totalMatches", "matchesAllRepos", "limit"])
        }
        "PageInfo" => Some(&["hasNextPage"]),
        "GitObjectFilterPreview" => {
            Some(&["nodes", "totalCount", "totalCountYoungerThanThreshold"])
        }
        "CodeIntelGitObject" => Some(&["name", "rev"]),
        "EmptyResponse" => Some(&["alwaysNil"]),
        _ => None,
    }
}

fn json_to_serde(value: &Json) -> Value {
    match value {
        Json::Null => Value::Null,
        Json::Bool(value) => Value::Bool(*value),
        Json::Int(value) => Value::from(*value),
        Json::UInt(value) => Value::from(*value),
        Json::Float(value) => Value::from(*value),
        Json::Str(value) => Value::String(value.clone()),
        Json::Raw(value) => serde_json::from_str(value).unwrap_or(Value::Null),
        Json::Arr(items) => Value::Array(items.iter().map(json_to_serde).collect()),
        Json::Obj(entries) => Value::Object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), json_to_serde(value)))
                .collect(),
        ),
    }
}

fn render_field(value: FieldValue) -> Result<Value, String> {
    match value {
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Json(value) => Ok(json_to_serde(&value)),
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_field(item)?);
            }
            Ok(Value::Array(rendered))
        }
        FieldValue::Object(object) => render_object(object.as_ref()),
    }
}

fn render_object(object: &dyn ObjectValue) -> Result<Value, String> {
    let type_name = object.type_name();
    let fields = fields_for(type_name)
        .ok_or_else(|| format!("codeintel policies diff: unhandled type {type_name}"))?;
    let arguments = Arguments::default();
    let mut map = Map::new();
    for field in fields {
        let value = object
            .field(field, &arguments, Actor::Internal)
            .map_err(|error| error.message)?
            .ok_or_else(|| format!("codeintel policies diff: unresolved {type_name}.{field}"))?;
        map.insert((*field).to_owned(), render_field(value)?);
    }
    Ok(Value::Object(map))
}

fn error_document(error: String) -> Value {
    let mut map = Map::new();
    map.insert("error".to_owned(), Value::String(error));
    Value::Object(map)
}

//
// Fake gitserver data mirroring the Go oracle's mock.
//

#[derive(Clone)]
struct FakeGit;

fn timestamp(seconds: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp {
        seconds,
        nanos: 0,
    })
}

/// 2024-01-01T12:00:00Z
const CREATED: i64 = 1_704_110_400;

#[async_trait::async_trait]
impl GitObjectSource for FakeGit {
    async fn resolve_revision(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        rev: &str,
    ) -> Result<String, GitSourceError> {
        Ok(rev.to_owned())
    }

    async fn get_commit(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<sg_git::CommitDetails, GitSourceError> {
        if repo_name != "r2" || oid != "HEAD" {
            return Err(GitSourceError {
                message: format!("revision not found: {oid}"),
                revision_not_found: true,
                not_exist: false,
            });
        }
        Ok(sg_git::CommitDetails {
            oid: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
            parents: Vec::new(),
            author: sg_git::CommitSignature {
                name: "a".to_owned(),
                email: "a@example.com".to_owned(),
                date: timestamp(CREATED + 96 * 3600),
            },
            committer: Some(sg_git::CommitSignature {
                name: "c".to_owned(),
                email: "c@example.com".to_owned(),
                date: timestamp(CREATED + 96 * 3600),
            }),
            message: "m".to_owned(),
        })
    }

    async fn get_object(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _revspec: &str,
    ) -> Result<sg_git::GitObject, GitSourceError> {
        Err(GitSourceError::new("unused"))
    }

    async fn list_refs(
        &self,
        _repo_id: i32,
        repo_name: &str,
        _heads_only: bool,
        _tags_only: bool,
    ) -> Result<Vec<sg_git::GitRef>, GitSourceError> {
        if repo_name != "r2" {
            return Ok(Vec::new());
        }
        Ok(vec![
            sg_git::GitRef {
                name: "refs/heads/main".to_owned(),
                short_name: "main".to_owned(),
                target_commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ref_oid: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                created_at: timestamp(CREATED),
                ref_type: RefType::Branch,
                is_head: true,
            },
            sg_git::GitRef {
                name: "refs/heads/release/1".to_owned(),
                short_name: "release/1".to_owned(),
                target_commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                ref_oid: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                created_at: timestamp(CREATED + 24 * 3600),
                ref_type: RefType::Branch,
                is_head: false,
            },
            sg_git::GitRef {
                name: "refs/tags/v1.0.0".to_owned(),
                short_name: "v1.0.0".to_owned(),
                target_commit: "cccccccccccccccccccccccccccccccccccccccc".to_owned(),
                ref_oid: "cccccccccccccccccccccccccccccccccccccccc".to_owned(),
                created_at: timestamp(CREATED + 48 * 3600),
                ref_type: RefType::Tag,
                is_head: false,
            },
            sg_git::GitRef {
                name: "refs/tags/v2.0.0".to_owned(),
                short_name: "v2.0.0".to_owned(),
                target_commit: "dddddddddddddddddddddddddddddddddddddddd".to_owned(),
                ref_oid: "dddddddddddddddddddddddddddddddddddddddd".to_owned(),
                created_at: timestamp(CREATED + 72 * 3600),
                ref_type: RefType::Tag,
                is_head: false,
            },
        ])
    }
}

async fn snapshot(pool: &sqlx::PgPool) -> Value {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT id, name, type, pattern, protected, retention_enabled, \
         retention_duration_hours, indexing_enabled, syntactic_indexing_enabled, \
         embeddings_enabled FROM lsif_configuration_policies \
         WHERE id BETWEEN 9500 AND 9599 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("snapshot");
    Value::Array(
        rows.iter()
            .map(|row| {
                let mut map = Map::new();
                map.insert("id".to_owned(), Value::from(row.get::<i32, _>("id")));
                map.insert("name".to_owned(), Value::from(row.get::<Option<String>, _>("name")));
                map.insert("type".to_owned(), Value::from(row.get::<String, _>("type")));
                map.insert("pattern".to_owned(), Value::from(row.get::<String, _>("pattern")));
                map.insert("protected".to_owned(), Value::from(row.get::<bool, _>("protected")));
                map.insert(
                    "retentionEnabled".to_owned(),
                    Value::from(row.get::<bool, _>("retention_enabled")),
                );
                map.insert(
                    "retentionDurationHours".to_owned(),
                    Value::from(row.get::<Option<i32>, _>("retention_duration_hours")),
                );
                map.insert(
                    "indexingEnabled".to_owned(),
                    Value::from(row.get::<bool, _>("indexing_enabled")),
                );
                map.insert(
                    "syntacticIndexingEnabled".to_owned(),
                    Value::from(row.get::<bool, _>("syntactic_indexing_enabled")),
                );
                map.insert(
                    "embeddingsEnabled".to_owned(),
                    Value::from(row.get::<bool, _>("embeddings_enabled")),
                );
                Value::Object(map)
            })
            .collect(),
    )
}

fn with_policies(mut document: Value, policies: Value) -> Value {
    if let Value::Object(map) = &mut document {
        map.insert("policies".to_owned(), policies);
    }
    document
}

#[tokio::test(flavor = "multi_thread")]
async fn codeintel_policies_graphql_harness() {
    if std::env::var("SG_M928C_CODEINTEL_POLICIES_GQL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M928C_CORPUS").expect("M928C_CORPUS");
    let out = std::env::var("M928C_OUT").expect("M928C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");
    let store = Arc::new(PgCodeIntelPoliciesStore::new(pool.clone(), Arc::new(FakeGit)));
    let handle: CodeIntelPoliciesHandle = store;

    for case in &corpus.cases {
        let actor = Actor::Internal;
        let document = match case.op.as_str() {
            "connection" => {
                let arguments = arguments_from_fixture(&case.args, false);
                match code_intelligence_configuration_policies(&handle, &arguments, actor) {
                    Ok(value) => render_field(value).expect("render connection"),
                    Err(error) => error_document(error.message),
                }
            }
            "node" => {
                let id = case.id.as_deref().unwrap_or_default();
                match node_field(&handle, id, actor) {
                    Ok(Some(FieldValue::Null)) => {
                        let mut map = Map::new();
                        map.insert("node".to_owned(), Value::Null);
                        Value::Object(map)
                    }
                    Ok(Some(value)) => {
                        let mut map = Map::new();
                        map.insert("node".to_owned(), render_field(value).expect("render node"));
                        Value::Object(map)
                    }
                    Ok(None) => {
                        let mut map = Map::new();
                        map.insert("node".to_owned(), Value::Null);
                        Value::Object(map)
                    }
                    Err(error) => error_document(error.message),
                }
            }
            "create" => {
                let arguments = arguments_from_fixture(&case.args, false);
                let mut document = match create_configuration_policy(&handle, &arguments, actor) {
                    Ok(value) => {
                        let mut map = Map::new();
                        map.insert("result".to_owned(), Value::String("ok".to_owned()));
                        map.insert("policy".to_owned(), render_field(value).expect("render policy"));
                        Value::Object(map)
                    }
                    Err(error) => error_document(error.message),
                };
                document = with_policies(document, snapshot(&pool).await);
                document
            }
            "update" => {
                let arguments = arguments_from_fixture(&case.args, true);
                let result = update_configuration_policy(&handle, &arguments, actor);
                let mut document = match result {
                    Ok(()) => {
                        let mut map = Map::new();
                        map.insert("result".to_owned(), Value::String("ok".to_owned()));
                        Value::Object(map)
                    }
                    Err(error) => error_document(error.message),
                };
                document = with_policies(document, snapshot(&pool).await);
                document
            }
            "delete" => {
                let arguments = arguments_from_fixture(&case.args, false);
                let result = delete_configuration_policy(&handle, &arguments, actor);
                let mut document = match result {
                    Ok(()) => {
                        let mut map = Map::new();
                        map.insert("result".to_owned(), Value::String("ok".to_owned()));
                        Value::Object(map)
                    }
                    Err(error) => error_document(error.message),
                };
                document = with_policies(document, snapshot(&pool).await);
                document
            }
            "preview_repo" => {
                let arguments = arguments_from_fixture(&case.args, false);
                match preview_repository_filter(&handle, &arguments) {
                    Ok(value) => render_field(value).expect("render preview"),
                    Err(error) => error_document(error.message),
                }
            }
            "preview_git" => {
                let arguments = arguments_from_fixture(&case.args, false);
                match preview_git_object_filter(&handle, case.repo.unwrap_or_default(), &arguments) {
                    Ok(value) => render_field(value).expect("render git preview"),
                    Err(error) => error_document(error.message),
                }
            }
            other => panic!("unknown op {other}"),
        };
        let body = serde_json::to_string(&document).expect("serialize");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body)
            .expect("write");
        println!("RUST {} op={} len={}", case.name, case.op, body.len());
    }
}
