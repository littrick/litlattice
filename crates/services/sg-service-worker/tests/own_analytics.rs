#![forbid(unsafe_code)]

//! M9.42d differential harness (Rust side): `analyticsIndexer.indexRepo`
//! (`internal/own/background/analytics.go`) over a scratch copy of the
//! frontend schema.
//!
//! Mirrors `internal/own/background/zz_m942d_own_analytics_oracle_test.go`;
//! outputs are compared line for line by
//! `test_ws/p9-go/run_own_analytics_diff.sh`. Both sides seed identical
//! fixtures and print canonical `oa ` lines. Timestamps are never printed.

use async_trait::async_trait;
use sg_service_worker::jobs::own_repo_indexing::{
    self, AnalyticsDirEntry, AnalyticsProvider,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const REPO_ID: i32 = 700;

fn print_line(line: impl Into<String>) {
    println!("oa {}", line.into());
}

fn esc(value: &str) -> String {
    value.replace('\n', "\\n")
}

async fn connect() -> Option<PgPool> {
    let dsn = std::env::var("SG_OWN_TEST_DSN").ok()?;
    Some(
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .expect("connect to SG_OWN_TEST_DSN"),
    )
}

async fn truncate(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE repo_paths, ownership_path_stats, assigned_owners, \
             assigned_teams, codeowners, repo \
         RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate fixtures");
}

async fn seed_repo(pool: &PgPool, repo_id: i32, name: &str) {
    sqlx::query("INSERT INTO repo (id, name, private) VALUES ($1, $2, FALSE)")
        .bind(repo_id)
        .bind(name)
        .execute(pool)
        .await
        .expect("insert repo");
}

async fn repo_path_id(pool: &PgPool, repo_id: i32, path: &str) -> i32 {
    sqlx::query_scalar(
        "INSERT INTO repo_paths (repo_id, absolute_path) VALUES ($1, $2) \
         ON CONFLICT (repo_id, absolute_path) \
         DO UPDATE SET absolute_path = EXCLUDED.absolute_path \
         RETURNING id",
    )
    .bind(repo_id)
    .bind(path)
    .fetch_one(pool)
    .await
    .expect("insert repo path")
}

async fn assign_owner(pool: &PgPool, repo_id: i32, path: &str) {
    sqlx::query(
        "INSERT INTO users (id, username) VALUES (1, 'owner1'), (2, 'assigner2') \
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("seed users");
    let path_id = repo_path_id(pool, repo_id, path).await;
    sqlx::query(
        "INSERT INTO assigned_owners (owner_user_id, file_path_id, who_assigned_user_id) \
         VALUES (1, $1, 2)",
    )
    .bind(path_id)
    .execute(pool)
    .await
    .expect("insert assigned owner");
}

async fn assign_team(pool: &PgPool, repo_id: i32, path: &str) {
    sqlx::query(
        "INSERT INTO teams (id, name) VALUES (1, 'team1') ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("seed teams");
    let path_id = repo_path_id(pool, repo_id, path).await;
    sqlx::query(
        "INSERT INTO assigned_teams (owner_team_id, file_path_id, who_assigned_team_id) \
         VALUES (1, $1, 2)",
    )
    .bind(path_id)
    .execute(pool)
    .await
    .expect("insert assigned team");
}

async fn seed_ingested_codeowners(pool: &PgPool, repo_id: i32, pattern: &str, owner: &str) {
    let contents = format!("{pattern} @{owner}");
    sqlx::query(
        "INSERT INTO codeowners (contents, contents_proto, repo_id) VALUES ($1, $2, $3)",
    )
    .bind(contents)
    .bind(Vec::<u8>::new())
    .bind(repo_id)
    .execute(pool)
    .await
    .expect("insert ingested codeowners");
}

#[derive(Clone, Default)]
struct Fixture {
    files: Vec<String>,
    dirs: Vec<String>,
    codeowners: Vec<(String, String)>,
    resolve_err: Option<String>,
    dir_err: Option<String>,
}

impl Fixture {
    fn codeowners(&self, path: &str) -> Option<&str> {
        self.codeowners
            .iter()
            .find(|(candidate, _)| candidate == path)
            .map(|(_, contents)| contents.as_str())
    }
}

struct FakeProvider {
    fixture: Fixture,
}

#[async_trait]
impl AnalyticsProvider for FakeProvider {
    async fn resolve_revision(&self, _repo: &str, _rev: &str) -> Result<String, String> {
        match &self.fixture.resolve_err {
            Some(err) => Err(err.clone()),
            None => Ok("c0ffee".to_owned()),
        }
    }

    async fn read_dir_recursive(
        &self,
        _repo: &str,
        _commit: &str,
    ) -> Result<Vec<AnalyticsDirEntry>, String> {
        if let Some(err) = &self.fixture.dir_err {
            return Err(err.clone());
        }
        let mut entries: Vec<AnalyticsDirEntry> = self
            .fixture
            .files
            .iter()
            .map(|name| AnalyticsDirEntry {
                name: name.clone(),
                is_dir: false,
            })
            .collect();
        entries.extend(self.fixture.dirs.iter().map(|name| AnalyticsDirEntry {
            name: name.clone(),
            is_dir: true,
        }));
        Ok(entries)
    }

    async fn read_file_optional(
        &self,
        _repo: &str,
        _commit: &str,
        path: &str,
    ) -> Result<Option<String>, String> {
        Ok(self.fixture.codeowners(path).map(str::to_owned))
    }
}

async fn index(pool: &PgPool, fixture: Fixture, repo_id: i32, sub_repo: bool) -> Result<(), String> {
    let provider = FakeProvider { fixture };
    own_repo_indexing::index_analytics(pool, &provider, repo_id, sub_repo).await
}

fn null_int(value: Option<i32>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "none".to_owned(),
    }
}

async fn query_counts(pool: &PgPool, repo_id: i32) -> (Option<i32>, Option<i32>, Option<i32>, Option<i32>) {
    let row = sqlx::query(
        "SELECT p.tree_files_count, \
                s.tree_codeowned_files_count, \
                s.tree_assigned_ownership_files_count, \
                s.tree_any_ownership_files_count \
         FROM repo_paths AS p \
         LEFT JOIN ownership_path_stats AS s ON s.file_path_id = p.id \
         WHERE p.repo_id = $1 AND p.absolute_path = ''",
    )
    .bind(repo_id)
    .fetch_optional(pool)
    .await
    .expect("query counts");
    let Some(row) = row else {
        return (None, None, None, None);
    };
    (
        row.try_get("tree_files_count").expect("tree files count"),
        row.try_get("tree_codeowned_files_count").expect("codeowned"),
        row.try_get("tree_assigned_ownership_files_count").expect("assigned"),
        row.try_get("tree_any_ownership_files_count").expect("any"),
    )
}

async fn print_result(pool: &PgPool, label: &str, repo_id: i32, result: Result<(), String>) {
    let (tree, codeowned, assigned, any) = query_counts(pool, repo_id).await;
    let (error, non_retryable) = match &result {
        Ok(()) => ("-".to_owned(), "false"),
        Err(error) => (
            esc(error),
            if own_repo_indexing::is_non_retryable(error) {
                "true"
            } else {
                "false"
            },
        ),
    };
    print_line(format!(
        "case={label} err={error} non-retryable={non_retryable} tree={} codeowned={} assigned={} any={}",
        null_int(tree),
        null_int(codeowned),
        null_int(assigned),
        null_int(any)
    ));
}

#[derive(Clone, Copy)]
enum Seed {
    AssignedOwner(&'static str),
    AssignedTeam(&'static str),
    IngestedCodeowners,
    PerforceRepo,
}

async fn apply_seed(pool: &PgPool, seed: Option<Seed>) {
    match seed {
        None => {}
        Some(Seed::AssignedOwner(path)) => assign_owner(pool, REPO_ID, path).await,
        Some(Seed::AssignedTeam(path)) => assign_team(pool, REPO_ID, path).await,
        Some(Seed::IngestedCodeowners) => {
            seed_ingested_codeowners(pool, REPO_ID, "*", "ingested").await;
        }
        Some(Seed::PerforceRepo) => {
            sqlx::query(
                "UPDATE repo SET private = TRUE, external_service_type = 'perforce' WHERE id = $1",
            )
            .bind(REPO_ID)
            .execute(pool)
            .await
            .expect("mark perforce repo");
        }
    }
}

async fn run_case(
    pool: &PgPool,
    label: &str,
    fixture: Fixture,
    seed: Option<Seed>,
    sub_repo: bool,
) {
    truncate(pool).await;
    seed_repo(pool, REPO_ID, "analytics-repo").await;
    apply_seed(pool, seed).await;
    let result = index(pool, fixture, REPO_ID, sub_repo).await;
    print_result(pool, label, REPO_ID, result).await;
}

fn base_fixture() -> Fixture {
    Fixture {
        files: vec![
            "a.go".to_owned(),
            "README.md".to_owned(),
            "src/b.go".to_owned(),
            "src/deep/c.go".to_owned(),
            "docs/guide.md".to_owned(),
        ],
        dirs: vec!["src".to_owned(), "docs".to_owned()],
        ..Fixture::default()
    }
}


#[tokio::test]
async fn own_analytics_harness() {
    let Some(pool) = connect().await else {
        eprintln!("SG_OWN_TEST_DSN is not set; skipping the own analytics harness");
        return;
    };

    // 1. No CODEOWNERS anywhere.
    run_case(&pool, "no-codeowners", base_fixture(),
        None, false).await;

    // 2. `* @alice`.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "* @alice\n".to_owned()));
    run_case(&pool, "root-wildcard", fixture,
        None, false).await;

    // 3. `*.go`.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "*.go @alice\n".to_owned()));
    run_case(&pool, "extension-glob", fixture,
        None, false).await;

    // 4. `/src/`.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "/src/ @bob\n".to_owned()));
    run_case(&pool, "anchored-dir", fixture,
        None, false).await;

    // 5. `src/**`.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "src/** @carol\n".to_owned()));
    run_case(&pool, "double-star", fixture,
        None, false).await;

    // 6. Last matching rule wins.
    let mut fixture = base_fixture();
    fixture.codeowners.push((
        "CODEOWNERS".to_owned(),
        "*.go @a\n/src/b.go\n".to_owned(),
    ));
    run_case(&pool, "last-rule-wins", fixture,
        None, false).await;

    // 7. Sections, comments, non-recursive `src/*.go`.
    let mut fixture = base_fixture();
    fixture.codeowners.push((
        "CODEOWNERS".to_owned(),
        "# leading comment\n[documentation]\ndocs/ @docs\nsrc/*.go @gophers  # trailing\n"
            .to_owned(),
    ));
    run_case(&pool, "section-comments", fixture,
        None, false).await;

    // 8. Assigned owner on `src`.
    run_case(
        &pool,
        "assigned-owner",
        base_fixture(),
        Some(Seed::AssignedOwner("src")),
        false,
    )
    .await;

    // 9. Assigned team on the root.
    run_case(
        &pool,
        "assigned-team-root",
        base_fixture(),
        Some(Seed::AssignedTeam("")),
        false,
    )
    .await;

    // 10. CODEOWNERS + assigned ownership.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "*.go @a\n".to_owned()));
    run_case(
        &pool,
        "both",
        fixture,
        Some(Seed::AssignedOwner("docs")),
        false,
    )
    .await;

    // 11. Sub-repo-permissions repo is skipped. The Rust check reads the
    // repo's private/perforce flags, so mark the repo accordingly.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "* @alice\n".to_owned()));
    run_case(
        &pool,
        "subrepo-skip",
        fixture,
        Some(Seed::PerforceRepo),
        true,
    )
    .await;

    // 12. Missing HEAD is non-retryable.
    let mut fixture = base_fixture();
    fixture.resolve_err = Some("revision not found".to_owned());
    run_case(&pool, "resolve-head-error", fixture,
        None, false).await;

    // 13. ReadDir failure is retryable.
    let mut fixture = base_fixture();
    fixture.dir_err = Some("boom".to_owned());
    run_case(&pool, "ls-tree-error", fixture,
        None, false).await;

    // 14. CODEOWNERS location priority.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "* @a\n".to_owned()));
    fixture.codeowners.push((
        ".github/test.CODEOWNERS".to_owned(),
        "a.go @b\n".to_owned(),
    ));
    run_case(&pool, "codeowners-priority", fixture,
        None, false).await;

    // 15. Ingested CODEOWNERS wins over git files.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "*.go @git\n".to_owned()));
    run_case(
        &pool,
        "ingested-codeowners",
        fixture,
        Some(Seed::IngestedCodeowners),
        false,
    )
    .await;

    // 16. Repeated runs are idempotent.
    let mut fixture = base_fixture();
    fixture
        .codeowners
        .push(("CODEOWNERS".to_owned(), "*.go @a\n".to_owned()));
    truncate(&pool).await;
    seed_repo(&pool, REPO_ID, "analytics-repo").await;
    let result = index(&pool, fixture.clone(), REPO_ID, false).await;
    print_result(&pool, "idempotent-run1", REPO_ID, result).await;
    let result = index(&pool, fixture, REPO_ID, false).await;
    print_result(&pool, "idempotent-run2", REPO_ID, result).await;

    // 17. Missing repository.
    truncate(&pool).await;
    let result = index(&pool, base_fixture(), 799, false).await;
    print_result(&pool, "missing-repo", 799, result).await;
}
