//! M9.8gj differential of `Mutation.createFeatureFlag` / `updateFeatureFlag` /
//! `deleteFeatureFlag` (`cmd/frontend/graphqlbackend/feature_flags.go:236-288`,
//! `internal/database/feature_flags.go:70-176`) against graph-gophers + the
//! real resolver, backed by the same scratch database rows as the Go oracle.
//!
//! Each case records the `feature_flags` row(s) for the flag under test before
//! and after the mutation, so soft-delete renaming, column changes and
//! "timestamps changed" are observable. Error messages are masked.
//!
//! Env:
//!
//! * `SG_M9GJ_DIFF=1`
//! * `M9GJ_CORPUS` — JSON `[{name, query, variables?, uid?, flag}]`
//! * `M9GJ_OUT` — output directory (one `<name>.json` per case)
//! * `M9GJ_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::feature_flag_mutations::prefetch_feature_flag_mutations;
use sg_service_frontend::feature_flag_store::PgFeatureFlagStore;
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::org_store::PgOrgStore;
use sg_service_frontend::settings_mutation::MutationRoot;

const SDL: &str = "
schema { query: Query mutation: Mutation }

type Query { _noop: String }

type Mutation {
  createFeatureFlag(name: String!, value: Boolean, rolloutBasisPoints: Int): FeatureFlag!
  updateFeatureFlag(name: String!, value: Boolean, rolloutBasisPoints: Int): FeatureFlag!
  deleteFeatureFlag(name: String!): EmptyResponse!
}

union FeatureFlag = FeatureFlagBoolean | FeatureFlagRollout

type FeatureFlagBoolean {
  name: String!
  value: Boolean!
  overrides: [FeatureFlagOverride!]!
  createdAt: DateTime!
  updatedAt: DateTime!
}

type FeatureFlagRollout {
  name: String!
  rolloutBasisPoints: Int!
  overrides: [FeatureFlagOverride!]!
  createdAt: DateTime!
  updatedAt: DateTime!
}

type FeatureFlagOverride {
  id: ID!
  value: Boolean!
}

type EmptyResponse {
  alwaysNil: String
}

scalar DateTime
";

/// Field order matches the Go oracle's `m9gjEnvelope` struct.
#[derive(Serialize)]
struct Envelope {
    response: Value,
    db: DbState,
}

#[derive(Serialize)]
struct DbState {
    before: Vec<RowState>,
    after: Vec<RowState>,
    created_at_unchanged: Option<bool>,
    updated_at_unchanged: Option<bool>,
    created_eq_updated: Option<bool>,
}

#[derive(Serialize)]
struct RowState {
    name: String,
    bool_value: Option<bool>,
    rollout: Option<i32>,
    deleted: bool,
}

#[derive(Debug, Clone)]
struct RawRow {
    name: String,
    bool_value: Option<bool>,
    rollout: Option<i32>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    deleted: bool,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    variables: Map<String, Value>,
    #[serde(default)]
    uid: i32,
    flag: String,
}

/// `-DELETED-<digits>` → `-DELETED-rand` (the baseline's random suffix).
fn normalize_name(name: &str) -> String {
    if let Some(position) = name.rfind("-DELETED-") {
        let suffix = &name[position + "-DELETED-".len()..];
        if !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit()) {
            return format!("{}-DELETED-rand", &name[..position]);
        }
    }
    name.to_owned()
}

fn row_less(a: &RawRow, b: &RawRow) -> std::cmp::Ordering {
    a.name
        .cmp(&b.name)
        .then_with(|| a.created_at.cmp(&b.created_at))
        .then_with(|| a.updated_at.cmp(&b.updated_at))
        .then_with(|| a.bool_value.cmp(&b.bool_value))
        .then_with(|| a.rollout.cmp(&b.rollout))
        .then_with(|| a.deleted.cmp(&b.deleted))
}

type RawRowColumns = (
    String,
    Option<bool>,
    Option<i32>,
    OffsetDateTime,
    OffsetDateTime,
    bool,
);

async fn snapshot(pool: &PgPool, flag: &str) -> Vec<RawRow> {
    let rows: Vec<RawRowColumns> = sqlx::query_as(
        r#"
SELECT flag_name, bool_value, rollout, created_at, updated_at, deleted_at IS NOT NULL
FROM feature_flags
WHERE flag_name = $1 OR flag_name LIKE $1 || '-DELETED-%'
ORDER BY created_at, flag_name
"#,
    )
    .bind(flag)
    .fetch_all(pool)
    .await
    .expect("feature_flags snapshot");
    let mut rows: Vec<RawRow> = rows
        .into_iter()
        .map(
            |(name, bool_value, rollout, created_at, updated_at, deleted)| RawRow {
                name: normalize_name(&name),
                bool_value,
                rollout,
                created_at,
                updated_at,
                deleted,
            },
        )
        .collect();
    rows.sort_by(row_less);
    rows
}

fn target_row<'a>(after: &'a [RawRow], flag: &str) -> Option<&'a RawRow> {
    if let Some(exact) = after.iter().find(|row| row.name == flag) {
        return Some(exact);
    }
    let deleted_name = format!("{flag}-DELETED-rand");
    after.iter().rfind(|row| row.name == deleted_name)
}

fn db_state(before: &[RawRow], after: &[RawRow], flag: &str) -> DbState {
    let before_rows = before.iter().map(row_state).collect();
    let after_rows = after.iter().map(row_state).collect();
    let pre = before.iter().find(|row| row.name == flag);
    let post = target_row(after, flag);
    let (created_at_unchanged, updated_at_unchanged) = match (pre, post) {
        (Some(pre), Some(post)) => (
            Some(pre.created_at == post.created_at),
            Some(pre.updated_at == post.updated_at),
        ),
        _ => (None, None),
    };
    DbState {
        before: before_rows,
        after: after_rows,
        created_at_unchanged,
        updated_at_unchanged,
        created_eq_updated: post.map(|post| post.created_at == post.updated_at),
    }
}

fn row_state(row: &RawRow) -> RowState {
    RowState {
        name: row.name.clone(),
        bool_value: row.bool_value,
        rollout: row.rollout,
        deleted: row.deleted,
    }
}

fn mask_response(response: &str) -> Value {
    let mut value: Value = serde_json::from_str(response).expect("response json");
    if let Some(errors) = value.get_mut("errors").and_then(Value::as_array_mut) {
        for error in errors {
            *error = json!({"message": "error"});
        }
    }
    mask_timestamps(&mut value);
    value
}

/// `gqlutil.DateTime` is a second-precision RFC3339 UTC string; the absolute
/// value depends on the run's wall clock, so response timestamps are masked
/// (the DB state records whether they changed).
fn is_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 20
        && bytes.iter().enumerate().all(|(i, byte)| match i {
            4 | 7 => *byte == b'-',
            10 => *byte == b'T',
            13 | 16 => *byte == b':',
            19 => *byte == b'Z',
            _ => byte.is_ascii_digit(),
        })
}

fn mask_timestamps(value: &mut Value) {
    match value {
        Value::String(text) if is_timestamp(text) => *text = "TIMESTAMP".to_owned(),
        Value::Array(items) => {
            for item in items {
                mask_timestamps(item);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                mask_timestamps(item);
            }
        }
        _ => {}
    }
}

async fn site_admin(pool: &PgPool, uid: i32) -> bool {
    if uid == 0 {
        return false;
    }
    sqlx::query_scalar::<_, bool>("SELECT site_admin FROM users WHERE id = $1")
        .bind(uid)
        .fetch_optional(pool)
        .await
        .expect("site_admin query")
        .unwrap_or(false)
}

#[tokio::test]
async fn feature_flag_mutations_harness() {
    if std::env::var("SG_M9GJ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M9GJ_CORPUS").expect("M9GJ_CORPUS");
    let out = std::env::var("M9GJ_OUT").expect("M9GJ_OUT");
    let dsn = std::env::var("M9GJ_DSN").expect("M9GJ_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let flags = PgFeatureFlagStore::new(pool.clone());
    let namespaces = PgOrgStore::new(pool.clone());

    for case in cases {
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let admin = site_admin(&pool, case.uid).await;
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &case.variables,
            actor,
        };
        let fields = root_fields(&schema, &request);

        let before = snapshot(&pool, &case.flag).await;
        let outcomes =
            prefetch_feature_flag_mutations(&flags, &namespaces, &fields, actor, admin).await;
        let mut mutation = MutationRoot::default();
        for (field, outcome) in outcomes {
            mutation = mutation.with_feature_flag_mutation(field, outcome);
        }
        let roots = FrontendRootResolver::new().with_mutation(mutation);
        let response = execute_with(&schema, request, Some(&roots));
        let after = snapshot(&pool, &case.flag).await;

        let envelope = Envelope {
            response: mask_response(&response.to_json()),
            db: db_state(&before, &after, &case.flag),
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
