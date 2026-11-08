#![forbid(unsafe_code)]

//! Differential harness for the `perforce-changelist-mapper` job (M9.8ae).
//!
//! Mirror of the Go harness
//! `cmd/worker/internal/perforce/mapper_harness_test.go`: same scratch
//! database, same fixtures, same canonical `pm ` lines. Drives the real
//! [`perforce_changelist_mapper`] against the scratch schema-only copy of the
//! frontend database; only the gitserver is a fake.
//!
//! Gated by `SG_PERFORCE_MAPPER_HARNESS_ON=1`; the connection comes from
//! `SG_PM_TEST_DSN`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use sg_config::{LiveConfig, SiteConfiguration, Unified};
use sg_service_worker::jobs::perforce_changelist_mapper::{
    perforce_changelist_mapper, MappingCommit, PerforceGitserver,
};

struct FakeGitserver {
    heads: HashMap<String, String>,
    commits: HashMap<String, Vec<MappingCommit>>,
    calls: Mutex<Vec<String>>,
}

impl FakeGitserver {
    fn record(&self, line: String) {
        self.calls.lock().expect("calls lock").push(line);
    }

    fn call_count(&self) -> usize {
        self.calls.lock().expect("calls lock").len()
    }

    fn take_calls(&self) -> Vec<String> {
        std::mem::take(&mut *self.calls.lock().expect("calls lock"))
    }
}

#[async_trait]
impl PerforceGitserver for FakeGitserver {
    async fn default_branch(&self, repo: &str) -> Result<(String, String), String> {
        self.record(format!("pm call repo={repo} kind=default_branch"));
        match self.heads.get(repo) {
            Some(head) => Ok(("refs/heads/main".to_owned(), head.clone())),
            None => Err(format!("no head for {repo}")),
        }
    }

    async fn commits(
        &self,
        repo: &str,
        ranges: &[String],
        n: usize,
        skip: usize,
    ) -> Result<Vec<MappingCommit>, String> {
        self.record(format!(
            "pm call repo={repo} kind=commits ranges=[{}] n={n} skip={skip}",
            ranges.join(" ")
        ));
        let all = self.commits.get(repo).map(Vec::as_slice).unwrap_or(&[]);
        let start = skip.min(all.len());
        let end = (start + n).min(all.len());
        Ok(all[start..end].to_vec())
    }
}

fn sha(n: usize) -> String {
    format!("{n:040x}")
}

fn trailer(repo_id: usize, changelist: usize) -> String {
    format!(
        "subject {changelist}\n\n[git-p4: depot-paths = \"//depot/{repo_id}/\": change = {changelist}]"
    )
}

fn commit(n: usize, message: String) -> MappingCommit {
    MappingCommit {
        id: sha(n),
        message,
    }
}

async fn cleanup(pool: &PgPool) {
    for statement in [
        "DELETE FROM repo_commits_changelists WHERE repo_id BETWEEN 7100 AND 7130",
        "DELETE FROM search_context_repos WHERE repo_id BETWEEN 7100 AND 7130",
        "DELETE FROM search_contexts WHERE id = 7801",
        "DELETE FROM codeintel_path_ranks WHERE graph_key LIKE 'pm-%'",
        "DELETE FROM codeintel_ranking_progress WHERE graph_key LIKE 'pm-%'",
        "DELETE FROM gitserver_repos WHERE repo_id BETWEEN 7100 AND 7130",
        "DELETE FROM repo WHERE id BETWEEN 7100 AND 7130",
    ] {
        sqlx::query(statement)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("cleanup {statement}: {err}"));
    }
}

async fn insert_repo(
    pool: &PgPool,
    id: i32,
    service_type: &str,
    updated_at: &str,
    deleted: bool,
    blocked: bool,
) {
    let deleted_at = deleted.then_some("2024-01-01T00:00:00Z");
    let blocked = blocked.then_some(r#"{"reason":"blocked"}"#);
    sqlx::query(
        r#"
INSERT INTO repo (id, name, uri, external_service_type, external_service_id, external_id, updated_at, created_at, deleted_at, blocked)
VALUES ($1, $2, $2, $3, 'svc', $4, $5::timestamptz, '2000-01-01T00:00:00Z'::timestamptz, $6::timestamptz, $7::jsonb)"#,
    )
    .bind(id)
    .bind(format!("perforce/repo-{id}"))
    .bind(service_type)
    .bind(format!("ext-{id}"))
    .bind(updated_at)
    .bind(deleted_at)
    .bind(blocked)
    .execute(pool)
    .await
    .unwrap_or_else(|err| panic!("insert repo {id}: {err}"));
}

async fn insert_mapping(pool: &PgPool, repo_id: i32, n: usize, changelist: i64) {
    sqlx::query(
        r#"
INSERT INTO repo_commits_changelists (repo_id, commit_sha, perforce_changelist_id)
VALUES ($1, decode($2, 'hex'), $3)"#,
    )
    .bind(repo_id)
    .bind(sha(n))
    .bind(changelist)
    .execute(pool)
    .await
    .unwrap_or_else(|err| panic!("insert mapping {repo_id}/{changelist}: {err}"));
}

#[tokio::test]
async fn perforce_changelist_mapper_harness() {
    if std::env::var("SG_PERFORCE_MAPPER_HARNESS_ON").as_deref() != Ok("1") {
        return;
    }
    let dsn = std::env::var("SG_PM_TEST_DSN").expect("SG_PM_TEST_DSN is required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .expect("connect scratch database");

    cleanup(&pool).await;

    const FUTURE: &str = "2035-01-01T00:00:00Z";
    const OLD: &str = "2000-01-01T00:00:00Z";

    // 7101-7105 qualify through repo.updated_at. 7106 qualifies only through
    // gitserver_repos.last_changed, 7107 through search_contexts.updated_at and
    // 7108 through the codeintel ranking progress timestamp. 7109 has no recent
    // change and must be skipped. 7110/7111 are deleted/blocked and 7112 is not
    // a Perforce repository.
    for id in [7101, 7102, 7103, 7104, 7105] {
        insert_repo(&pool, id, "perforce", FUTURE, false, false).await;
    }
    for id in [7106, 7107, 7108, 7109] {
        insert_repo(&pool, id, "perforce", OLD, false, false).await;
    }
    insert_repo(&pool, 7110, "perforce", FUTURE, true, false).await;
    insert_repo(&pool, 7111, "perforce", FUTURE, false, true).await;
    insert_repo(&pool, 7112, "github", FUTURE, false, false).await;

    insert_mapping(&pool, 7102, 710200, 50).await;
    insert_mapping(&pool, 7103, 9103, 60).await;
    insert_mapping(&pool, 7106, 9106, 70).await;
    insert_mapping(&pool, 7107, 710700, 80).await;
    insert_mapping(&pool, 7108, 9108, 90).await;

    // `repo` inserts create the gitserver_repos row through
    // trigger_gitserver_repo_insert; only the timestamp needs changing.
    sqlx::query("UPDATE gitserver_repos SET last_changed = $1::timestamptz WHERE repo_id = 7106")
        .bind(FUTURE)
        .execute(&pool)
        .await
        .expect("update gitserver_repos");
    sqlx::query(
        "INSERT INTO search_contexts (id, name, description, public, updated_at) VALUES (7801, 'pm-context', 'pm', false, $1::timestamptz)",
    )
    .bind(FUTURE)
    .execute(&pool)
    .await
    .expect("insert search_contexts");
    sqlx::query(
        "INSERT INTO search_context_repos (search_context_id, repo_id, revision) VALUES (7801, 7107, 'HEAD')",
    )
    .execute(&pool)
    .await
    .expect("insert search_context_repos");
    sqlx::query(
        "INSERT INTO codeintel_path_ranks (repository_id, payload, graph_key) VALUES (7108, '{}', 'pm-graph')",
    )
    .execute(&pool)
    .await
    .expect("insert codeintel_path_ranks");
    sqlx::query(
        "INSERT INTO codeintel_ranking_progress (graph_key, mappers_started_at, reducer_completed_at, max_export_id) VALUES ('pm-graph', '2000-01-01T00:00:00Z'::timestamptz, '9999-01-01T00:00:00Z'::timestamptz, 0)",
    )
    .execute(&pool)
    .await
    .expect("insert codeintel_ranking_progress");

    let mut commits: HashMap<String, Vec<MappingCommit>> = HashMap::new();
    commits.insert(
        "perforce/repo-7101".to_owned(),
        vec![
            commit(710100, trailer(7101, 101)),
            commit(710101, trailer(7101, 102)),
        ],
    );
    commits.insert(
        "perforce/repo-7102".to_owned(),
        vec![commit(710201, trailer(7102, 60))],
    );
    commits.insert(
        "perforce/repo-7104".to_owned(),
        vec![
            commit(710400, trailer(7104, 400)),
            commit(710401, "commit without trailer".to_owned()),
        ],
    );
    commits.insert(
        "perforce/repo-7105".to_owned(),
        (0..51)
            .map(|i| commit(7150000 + i, trailer(7105, 100 + i)))
            .collect(),
    );
    commits.insert(
        "perforce/repo-7107".to_owned(),
        vec![commit(710701, trailer(7107, 85))],
    );

    let mut heads = HashMap::new();
    for repo_id in [7102, 7103, 7106, 7107, 7108] {
        heads.insert(format!("perforce/repo-{repo_id}"), sha(repo_id + 2000));
    }

    let gitserver = Arc::new(FakeGitserver {
        heads,
        commits,
        calls: Mutex::new(Vec::new()),
    });

    let gate_line = |label: &str, result: &Result<(), String>, calls: usize| {
        let message = result.as_ref().err().map(String::as_str).unwrap_or("-");
        format!("pm gate={label} err={message} calls={calls}")
    };

    // Gate disabled: no database or gitserver work at all.
    let disabled = LiveConfig::new(Unified::default());
    let result = perforce_changelist_mapper(&pool, gitserver.as_ref(), &disabled, 2).await;
    println!("{}", gate_line("disabled", &result, gitserver.call_count()));

    // Gate enabled: the full batch walk.
    let site = SiteConfiguration::parse(
        r#"{"experimentalFeatures":{"perforceChangelistMapping":"enabled"}}"#,
    )
    .expect("site configuration");
    let enabled = LiveConfig::new(Unified {
        site,
        ..Unified::default()
    });
    let _ = gitserver.take_calls();
    let result = perforce_changelist_mapper(&pool, gitserver.as_ref(), &enabled, 2).await;
    println!("{}", gate_line("enabled", &result, gitserver.call_count()));
    for call in gitserver.take_calls() {
        println!("{call}");
    }

    let rows = sqlx::query(
        r#"
SELECT repo_id, encode(commit_sha, 'hex') AS sha, perforce_changelist_id
FROM repo_commits_changelists
WHERE repo_id BETWEEN 7100 AND 7130
ORDER BY repo_id, perforce_changelist_id"#,
    )
    .fetch_all(&pool)
    .await
    .expect("select mappings");
    for row in rows {
        let repo_id: i32 = row.get("repo_id");
        let sha: String = row.get("sha");
        let changelist: i32 = row.get("perforce_changelist_id");
        println!("pm row repo={repo_id} sha={sha} cl={changelist}");
    }

    cleanup(&pool).await;
}
