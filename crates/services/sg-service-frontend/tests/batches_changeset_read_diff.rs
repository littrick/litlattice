//! M9.26c differential of the Batch Changes changeset deep-read GraphQL
//! surface (`Changeset.events` / `diff` / `commitVerification` /
//! `batchChanges`; `BatchChange.changesetCountsOverTime` / `changesetsStats` /
//! `diffStat` / `bulkOperations`; `Query.globalChangesetsStats` /
//! `Query.batchChangesCodeHosts`) against the Go baseline
//! (`cmd/frontend/internal/batches/resolvers` + graph-gophers + the real
//! batches Postgres store + an injectable gitserver client).
//!
//! Both sides read the same scratch database (seeded by
//! `run_batches_changeset_read_diff.sh` immediately before each side runs) and
//! execute the same query strings from
//! `data/batches_changeset_read/corpus.json`. The Rust side assembles the
//! vendored `batches.graphql` on top of the main OSS schema and resolves
//! through the production `FrontendRootResolver`; `Changeset.diff` uses a
//! deterministic [`ChangesetDiffSource`] that parses the same patch bytes the
//! Go oracle's mocked `git diff` returns.
//!
//! Env:
//!
//! * `SG_M926C_CHANGESET_READ_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M926C_CORPUS` — corpus JSON path
//! * `M926C_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Map;
use sqlx::postgres::PgPoolOptions;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{
    assemble, BATCHES_SCHEMA, GITHUB_APPS_SCHEMA, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA,
    VIEWER_SCHEMA,
};
use sg_service_frontend::batches_changeset_read::{
    ChangesetDiffSource, PreviewComparisonObject,
};
use sg_service_frontend::batches_graphql::BatchesHandle;
use sg_service_frontend::batches_graphql_store::PgBatchesGraphqlStore;
use sg_service_frontend::git_commit_extra::ComparisonData;
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, OrgData, OrganizationLookup, RepoKey, RepoLookup, RepositoryData,
    UserData, UserKey, UserLookup, UserObject,
};
use sg_service_frontend::search_stream::convert::MinimalRepo;
use sg_service_gitserver::diff::parse_multi_file_diff;

/// Same bytes as `m926cComparisonDiff` in the Go oracle.
const COMPARISON_DIFF: &str = "diff --git README.md README.md
--- README.md
+++ README.md
@@ -1,2 +1,3 @@
 line one
 line two
+line three
diff --git docs/guide.md docs/guide.md
--- docs/guide.md
+++ docs/guide.md
@@ -3,2 +3,2 @@ section
-old line
+new line
 context
";

struct FixtureDiffSource;

impl ChangesetDiffSource for FixtureDiffSource {
    fn comparison(
        &self,
        _repo: &MinimalRepo,
        base_revspec: &str,
        head_revspec: &str,
    ) -> Result<ComparisonData, String> {
        let files = parse_multi_file_diff(COMPARISON_DIFF.as_bytes()).map_err(|e| e.to_string())?;
        Ok(ComparisonData {
            base_revspec: base_revspec.to_owned(),
            head_revspec: head_revspec.to_owned(),
            range_type: "...".to_owned(),
            base_oid: Some(base_revspec.to_owned()),
            head_oid: head_revspec.to_owned(),
            file_diffs: Some(Ok(files)),
            commits: None,
        })
    }

    fn preview(
        &self,
        repo: &MinimalRepo,
        base_rev: &str,
        patch: &[u8],
    ) -> Result<PreviewComparisonObject, String> {
        let files = parse_multi_file_diff(patch).map_err(|e| e.to_string())?;
        Ok(PreviewComparisonObject {
            repo: repo.clone(),
            commit_oid: base_rev.to_owned(),
            file_diffs: files,
        })
    }
}

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    fixtures: Fixtures,
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct Fixtures {
    #[serde(default)]
    users: Vec<UserFixture>,
    #[serde(default)]
    orgs: Vec<OrgFixture>,
    #[serde(default)]
    org_members: Vec<(i32, i32)>,
    #[serde(default)]
    repositories: Vec<RepoFixture>,
}

#[derive(Deserialize)]
struct UserFixture {
    id: i32,
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    avatar_url: String,
    #[serde(default = "default_time")]
    created_at: String,
    #[serde(default = "default_time")]
    updated_at: String,
    #[serde(default)]
    site_admin: bool,
    #[serde(default)]
    tos_accepted: bool,
    #[serde(default)]
    primary_email: String,
}

#[derive(Deserialize)]
struct OrgFixture {
    id: i32,
    name: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default = "default_time")]
    created_at: String,
    #[serde(default)]
    viewer_can_administer: bool,
    #[serde(default)]
    viewer_is_member: bool,
}

#[derive(Deserialize)]
struct RepoFixture {
    id: i32,
    name: String,
    uri: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    fork: bool,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    private: bool,
    #[serde(default)]
    stars: i32,
    #[serde(default)]
    external_id: String,
    #[serde(default)]
    external_service_type: String,
    #[serde(default)]
    external_service_id: String,
}

fn default_time() -> String {
    "2024-01-01T00:00:00Z".to_owned()
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
    #[serde(default = "default_true")]
    batch_changes_enabled: bool,
    #[serde(default)]
    batch_changes_restricted_to_admins: bool,
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("rfc3339 timestamp")
}

fn user_data(fixture: &UserFixture) -> UserData {
    UserData {
        id: fixture.id,
        username: fixture.username.clone(),
        display_name: fixture.display_name.clone(),
        avatar_url: fixture.avatar_url.clone(),
        created_at: parse_time(&fixture.created_at),
        updated_at: parse_time(&fixture.updated_at),
        site_admin: fixture.site_admin,
        tos_accepted: fixture.tos_accepted,
        primary_email: fixture.primary_email.clone(),
    }
}

fn org_data(fixture: &OrgFixture) -> OrgData {
    OrgData {
        id: fixture.id,
        name: fixture.name.clone(),
        display_name: fixture.display_name.clone(),
        created_at: parse_time(&fixture.created_at),
        viewer_can_administer: fixture.viewer_can_administer,
        viewer_is_member: fixture.viewer_is_member,
    }
}

fn repo_data(fixture: &RepoFixture) -> RepositoryData {
    RepositoryData {
        id: fixture.id,
        name: fixture.name.clone(),
        uri: fixture.uri.clone(),
        description: fixture.description.clone(),
        fork: fixture.fork,
        archived: fixture.archived,
        private: fixture.private,
        stars: fixture.stars,
        external_id: fixture.external_id.clone(),
        external_service_type: fixture.external_service_type.clone(),
        external_service_id: fixture.external_service_id.clone(),
    }
}

fn normalized(response: &sg_graphql::exec::ExecResponse) -> String {
    let mut envelope: BTreeMap<&str, serde_json::Value> = BTreeMap::new();
    if !response.errors.is_empty() {
        envelope.insert(
            "errors",
            serde_json::Value::Array(
                response
                    .errors
                    .iter()
                    .map(|error| serde_json::json!({ "message": error.message }))
                    .collect(),
            ),
        );
    }
    if let Some(data) = response.data_json() {
        envelope.insert("data", serde_json::from_str(&data).unwrap());
    }
    serde_json::to_string(&envelope).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_changeset_read_harness() {
    if std::env::var("SG_M926C_CHANGESET_READ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M926C_CORPUS").expect("M926C_CORPUS");
    let out = std::env::var("M926C_OUT").expect("M926C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    // Match `NewSchemaWithBatchChangesResolver`'s document order: the three
    // OSS documents, the batches document, then the GitHub Apps document.
    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        BATCHES_SCHEMA,
        GITHUB_APPS_SCHEMA,
    ])
    .expect("schema");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    let mut users: HashMap<UserKey, UserLookup> = HashMap::new();
    for fixture in &corpus.fixtures.users {
        users.insert(
            UserKey::Username(fixture.username.clone()),
            UserLookup::Found(user_data(fixture)),
        );
    }
    let mut organizations: HashMap<String, OrganizationLookup> = HashMap::new();
    for fixture in &corpus.fixtures.orgs {
        organizations.insert(
            fixture.name.clone(),
            OrganizationLookup::Found(org_data(fixture)),
        );
    }
    let mut repositories: HashMap<RepoKey, RepoLookup> = HashMap::new();
    for fixture in &corpus.fixtures.repositories {
        repositories.insert(
            RepoKey::Name(fixture.name.clone()),
            RepoLookup::Found(repo_data(fixture)),
        );
    }

    for case in &corpus.cases {
        let (actor, viewer) = match case.viewer.as_str() {
            "anonymous" | "" => (Actor::Anonymous, None),
            _ => {
                let fixture = corpus
                    .fixtures
                    .users
                    .iter()
                    .find(|user| user.id == case.uid)
                    .expect("viewer fixture");
                let data = user_data(fixture);
                let object = UserObject::from_data_as_viewer(data.clone(), data.site_admin);
                (Actor::User { uid: case.uid }, Some(object))
            }
        };
        let store = PgBatchesGraphqlStore::new(pool.clone())
            .with_batch_changes_enabled(case.batch_changes_enabled)
            .with_batch_changes_restricted_to_admins(case.batch_changes_restricted_to_admins)
            .with_changeset_diff_source(Arc::new(FixtureDiffSource));
        let batches: BatchesHandle = Arc::new(store);
        let roots = FrontendRootResolver::with_prefetched_organizations(
            viewer,
            repositories.clone(),
            users.clone(),
            HashMap::new(),
            organizations.clone(),
            HashMap::new(),
            corpus
                .fixtures
                .users
                .iter()
                .find(|user| user.id == case.uid)
                .is_some_and(|user| user.site_admin),
        )
        .with_batches(Some(batches));
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));
        let body = normalized(&response);
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }

    let _ = &corpus.fixtures.org_members;
}
