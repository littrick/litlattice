#![forbid(unsafe_code)]

//! M9.43c differential harness (Rust side):
//! `recentViewsIndexer.handle` (`internal/own/background/recent_views.go`) and
//! `RecentViewSignalStore.BuildAggregateFromEvents` (`internal/database/
//! recent_view_signal.go`) over a scratch copy of the frontend schema.
//!
//! Mirrors `internal/own/background/zz_m943c_own_recent_views_oracle_test.go`;
//! both sides seed identical fixtures and print canonical `rv ` lines compared
//! by `test_ws/p9-go/run_own_recent_views_diff.sh`. Timestamps and row IDs are
//! never printed; rows are ordered by repository/path.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use sg_service_worker::jobs::own_repo_indexing::{self, SubRepoEnabledChecker};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const REPO_A: i32 = 100;
const REPO_A_NAME: &str = "github.com/acme/alpha";
const REPO_B: i32 = 101;
const REPO_B_NAME: &str = "github.com/acme/beta";

fn print_line(line: impl Into<String>) {
    println!("rv {}", line.into());
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

/// `repository name -> sub-repo-enabled`; a listed error repo fails the
/// checker call.
#[derive(Default, Clone)]
struct FakeChecker {
    enabled: bool,
    enabled_for: HashMap<String, bool>,
    error_repos: HashSet<String>,
}

impl FakeChecker {
    fn enabled_for(repos: &[&str]) -> Self {
        Self {
            enabled: true,
            enabled_for: repos.iter().map(|repo| ((*repo).to_owned(), true)).collect(),
            error_repos: HashSet::new(),
        }
    }

    fn erroring(repo: &str) -> Self {
        Self {
            enabled: true,
            enabled_for: HashMap::new(),
            error_repos: [repo.to_owned()].into_iter().collect(),
        }
    }
}

#[async_trait]
impl SubRepoEnabledChecker for FakeChecker {
    fn enabled(&self) -> bool {
        self.enabled
    }

    async fn enabled_for_repo(&self, repo: &str) -> Result<bool, String> {
        if self.error_repos.contains(repo) {
            return Err("boom".to_owned());
        }
        Ok(*self.enabled_for.get(repo).unwrap_or(&false))
    }
}

async fn reset(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE own_aggregate_recent_view, event_logs_scrape_state_own, \
             event_logs, repo_paths, own_signal_configurations, users, repo \
         RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate fixtures");
}

/// Users 1/2, repos A/B, and the `recent-views` configuration.
async fn seed_base(pool: &PgPool, excluded: &[&str]) {
    sqlx::query("INSERT INTO users (id, username) VALUES (1, 'u1'), (2, 'u2')")
        .execute(pool)
        .await
        .expect("seed users");
    sqlx::query("INSERT INTO repo (id, name, private) VALUES ($1, $2, FALSE), ($3, $4, FALSE)")
        .bind(REPO_A)
        .bind(REPO_A_NAME)
        .bind(REPO_B)
        .bind(REPO_B_NAME)
        .execute(pool)
        .await
        .expect("seed repos");
    let excluded: Vec<String> = excluded.iter().map(|pattern| (*pattern).to_owned()).collect();
    sqlx::query(
        "INSERT INTO own_signal_configurations \
             (id, name, description, excluded_repo_patterns, enabled) \
         VALUES (2, 'recent-views', '', $1, TRUE)",
    )
    .bind(excluded)
    .execute(pool)
    .await
    .expect("seed recent-views configuration");
}

async fn seed_bookmark(pool: &PgPool, bookmark: i32) {
    sqlx::query(
        "INSERT INTO event_logs_scrape_state_own (bookmark_id, job_type) VALUES ($1, 2)",
    )
    .bind(bookmark)
    .execute(pool)
    .await
    .expect("seed bookmark");
}

async fn seed_event(pool: &PgPool, name: &str, user_id: i32, public_argument: &str) {
    sqlx::query(
        "INSERT INTO event_logs \
             (name, url, user_id, anonymous_user_id, source, argument, version, \
              timestamp, public_argument) \
         VALUES ($1, 'http://example.com', $2, '', 'test', '{}'::jsonb, 'v1', now(), $3::jsonb)",
    )
    .bind(name)
    .bind(user_id)
    .bind(public_argument)
    .execute(pool)
    .await
    .expect("seed event");
}

async fn seed_view(pool: &PgPool, user_id: i32, repo: &str, path: &str) {
    seed_event(
        pool,
        "ViewBlob",
        user_id,
        &format!("{{\"repoName\":\"{repo}\",\"filePath\":\"{path}\"}}"),
    )
    .await;
}

async fn print_state(pool: &PgPool, label: &str, result: &Result<(), String>) {
    let error = if result.is_ok() { "none" } else { "error" };
    let bookmark: Option<i32> = sqlx::query_scalar(
        "SELECT bookmark_id FROM event_logs_scrape_state_own WHERE job_type = 2 ORDER BY id LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .expect("query bookmark");
    let bookmark = bookmark
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned());

    let rows = sqlx::query(
        "SELECT v.viewer_id, p.repo_id, p.absolute_path, v.views_count \
         FROM own_aggregate_recent_view v \
         JOIN repo_paths p ON p.id = v.viewed_file_path_id \
         ORDER BY v.viewer_id, p.repo_id, p.absolute_path",
    )
    .fetch_all(pool)
    .await
    .expect("query views");
    let views: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{}:{}:{}:{}",
                row.get::<i32, _>("viewer_id"),
                row.get::<i32, _>("repo_id"),
                row.get::<String, _>("absolute_path"),
                row.get::<i32, _>("views_count")
            )
        })
        .collect();

    let paths = sqlx::query(
        "SELECT p.repo_id, p.absolute_path, pp.absolute_path AS parent_path \
         FROM repo_paths p \
         LEFT JOIN repo_paths pp ON pp.id = p.parent_id \
         ORDER BY p.repo_id, p.absolute_path",
    )
    .fetch_all(pool)
    .await
    .expect("query paths");
    let paths: Vec<String> = paths
        .iter()
        .map(|row| {
            format!(
                "{}:{}<-{}",
                row.get::<i32, _>("repo_id"),
                row.get::<String, _>("absolute_path"),
                row.get::<Option<String>, _>("parent_path")
                    .unwrap_or_else(|| "null".to_owned())
            )
        })
        .collect();

    print_line(format!(
        "case={label} err={error} bookmark={bookmark} views=[{}] paths=[{}]",
        views.join(","),
        paths.join(",")
    ));
}

async fn invoke(
    pool: &PgPool,
    checker: &dyn SubRepoEnabledChecker,
) -> Result<(), String> {
    own_repo_indexing::index_recent_views(pool, checker).await
}

#[tokio::test]
async fn own_recent_views_harness() {
    let Some(pool) = connect().await else {
        eprintln!("SG_OWN_TEST_DSN is not set; skipping the own recent-views harness");
        return;
    };
    let disabled = FakeChecker::default();

    // 1. Basic aggregation with ancestor counts.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "src/a.go").await;
    seed_view(&pool, 1, REPO_A_NAME, "src/b.go").await;
    seed_view(&pool, 1, REPO_A_NAME, "README.md").await;
    print_state(&pool, "basic", &invoke(&pool, &disabled).await).await;

    // 2. One path viewed repeatedly by two users.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "x.go").await;
    seed_view(&pool, 1, REPO_A_NAME, "x.go").await;
    seed_view(&pool, 2, REPO_A_NAME, "x.go").await;
    print_state(&pool, "same-path-twice", &invoke(&pool, &disabled).await).await;

    // 3. Two repositories.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    seed_view(&pool, 2, REPO_B_NAME, "deep/b.go").await;
    print_state(&pool, "multi-repo", &invoke(&pool, &disabled).await).await;

    // 4. Non-ViewBlob events never reach the indexer.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_event(&pool, "SearchResultsQueried", 1, "{}").await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    seed_event(&pool, "codeintel", 1, "{}").await;
    print_state(&pool, "non-viewblob", &invoke(&pool, &disabled).await).await;

    // 5. A wrong-typed PublicArgument is skipped; the bookmark index bug
    //    (`events[numberOfEvents-1]` over the unfiltered list) is visible.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_event(&pool, "ViewBlob", 1, "{\"repoName\":5,\"filePath\":\"bad.go\"}").await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    seed_view(&pool, 1, REPO_A_NAME, "b.go").await;
    print_state(&pool, "malformed-json", &invoke(&pool, &disabled).await).await;

    // 6. Missing repository: counted but never written, no error.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, "github.com/acme/missing", "a.go").await;
    print_state(&pool, "missing-repo", &invoke(&pool, &disabled).await).await;

    // 7. Excluded repository pattern.
    reset(&pool).await;
    seed_base(&pool, &["github.com/acme/%"]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    print_state(&pool, "excluded-pattern", &invoke(&pool, &disabled).await).await;

    // 8. Sub-repo-enabled repository is skipped; bookmark does not advance.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    let checker = FakeChecker::enabled_for(&[REPO_A_NAME]);
    print_state(&pool, "subrepo-skip", &invoke(&pool, &checker).await).await;

    // 9. Mixed sub-repo status; the bookmark lags when the first event is
    //    dropped.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    seed_view(&pool, 1, REPO_B_NAME, "b.go").await;
    let checker = FakeChecker::enabled_for(&[REPO_A_NAME]);
    print_state(&pool, "subrepo-mixed", &invoke(&pool, &checker).await).await;

    // 10. Global checker disabled: everything is processed.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    let checker = FakeChecker {
        enabled: false,
        ..FakeChecker::default()
    };
    print_state(&pool, "checker-disabled", &invoke(&pool, &checker).await).await;

    // 11. Checker error: the event is dropped and not surfaced.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    let checker = FakeChecker::erroring(REPO_A_NAME);
    print_state(&pool, "subrepo-error", &invoke(&pool, &checker).await).await;

    // 12. Bookmark progression: a second run only folds in new events.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    let _ = invoke(&pool, &disabled).await;
    seed_view(&pool, 1, REPO_A_NAME, "b.go").await;
    print_state(&pool, "bookmark-progression", &invoke(&pool, &disabled).await).await;

    // 13. Repeated runs over the same events are not counted twice.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    let _ = invoke(&pool, &disabled).await;
    print_state(&pool, "idempotent-run2", &invoke(&pool, &disabled).await).await;

    // 14. No bookmark row: it is generated from MAX(id) and history is not
    //     backfilled.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    seed_view(&pool, 1, REPO_A_NAME, "b.go").await;
    print_state(&pool, "bookmark-generation", &invoke(&pool, &disabled).await).await;

    // 15. No bookmark row and no events: the baseline's NOT NULL insert fails.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    print_state(&pool, "no-events-no-bookmark", &invoke(&pool, &disabled).await).await;

    // 16. Blocked repository errors the aggregate build.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    sqlx::query("UPDATE repo SET blocked = '{\"reason\":\"dmca\"}'::jsonb WHERE id = $1")
        .bind(REPO_A)
        .execute(&pool)
        .await
        .expect("block repo");
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "a.go").await;
    print_state(&pool, "blocked-repo", &invoke(&pool, &disabled).await).await;

    // 17. Empty file path is the repository root.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_view(&pool, 1, REPO_A_NAME, "").await;
    print_state(&pool, "empty-filepath", &invoke(&pool, &disabled).await).await;

    // 18. JSON `null` PublicArgument decodes to zero values.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    seed_event(&pool, "ViewBlob", 1, "null").await;
    print_state(&pool, "null-json", &invoke(&pool, &disabled).await).await;

    // 19. More than 5000 distinct paths: the aggregate builder feeds
    //     `insert_paths` 5000 leaves (plus their ancestors), so the
    //     5000-row bulk-insert limit splits the augmented rows into two
    //     batches. Print a compact summary instead of 5002 rows.
    reset(&pool).await;
    seed_base(&pool, &[]).await;
    seed_bookmark(&pool, 0).await;
    sqlx::query(
        "INSERT INTO event_logs \
             (name, url, user_id, anonymous_user_id, source, argument, version, \
              timestamp, public_argument) \
         SELECT 'ViewBlob', 'http://example.com', 1, '', 'test', '{}'::jsonb, 'v1', now(), \
             jsonb_build_object('repoName', $1::text, 'filePath', 'src/file' || g || '.go') \
         FROM generate_series(1, 5000) AS g",
    )
    .bind(REPO_A_NAME)
    .execute(&pool)
    .await
    .expect("seed 5000 view events");
    let error = invoke(&pool, &disabled).await;
    let error_str = if error.is_err() { "error" } else { "none" };
    let row = sqlx::query(
        "SELECT count(*), COALESCE(sum(views_count), 0), COALESCE(max(views_count), 0) \
         FROM own_aggregate_recent_view",
    )
    .fetch_one(&pool)
    .await
    .expect("query over-batch aggregates");
    let rows: i64 = row.try_get(0).expect("count");
    let total: i64 = row.try_get(1).expect("sum");
    let max: i32 = row.try_get(2).expect("max");
    print_line(format!(
        "case=over-batch rows={rows} total={total} max={max} err={error_str}"
    ));
}
