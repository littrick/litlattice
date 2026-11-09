#![forbid(unsafe_code)]

//! M9.8dv differential harness (Rust side): the ranking frontend service layer.
//!
//! `test_ws/p9-go/run_codeintel_ranking_service_diff.sh` applies
//! `codeintel_ranking_service_fixtures.sql` immediately before each side, runs
//! the Go harness (`internal/codeintel/ranking`, `TestRankingServiceHarness`)
//! and this test, and diffs the canonical `rks ` JSON lines. Floats are printed
//! as `%.9f`-style strings and times as Unix seconds, so neither side depends
//! on float formatting or wall clocks.

use serde_json::{json, Map, Value};
use sg_codeintel_ranking::{
    derivative_graph_key, get_document_ranks, get_repo_rank, next_job_starts_at, NextJobStart,
};
use sg_config::SiteConfiguration;
use sqlx::postgres::PgPool;

fn print(value: Value) {
    println!("rks {}", serde_json::to_string(&value).expect("serialize"));
}

fn fixed(value: f64) -> String {
    format!("{value:.9}")
}

fn site(json: &str) -> SiteConfiguration {
    SiteConfiguration::parse(json).expect("valid site config")
}

fn repo_rank_config() -> SiteConfiguration {
    site(
        r#"{"experimentalFeatures": {"ranking": {"repoScores": {
            "github.com": 500,
            "github.com/foo": 300,
            "github.com/foo/bar": 100,
            "github.com/bar": 200
        }}}}"#,
    )
}

fn cron_config(expression: Option<&str>) -> SiteConfiguration {
    match expression {
        Some(expression) => site(
            &json!({
                "codeIntelRanking.documentReferenceCountsCronExpression": expression,
            })
            .to_string(),
        ),
        None => site("{}"),
    }
}

async fn document_ranks_case(pool: &PgPool, repo: &str) {
    let ranks = get_document_ranks(pool, repo)
        .await
        .expect("document ranks");
    let paths: Map<String, Value> = ranks
        .paths
        .iter()
        .map(|(path, rank)| (path.clone(), json!(fixed(*rank))))
        .collect();
    print(json!({
        "case": "document_ranks",
        "repo": repo,
        "mean": fixed(ranks.mean_rank),
        "paths": paths,
    }));
}

async fn next_case(pool: &PgPool, name: &str, expression: Option<&str>) {
    match next_job_starts_at(pool, &cron_config(expression)).await {
        Err(err) => print(json!({
            "case": "next_job_starts_at",
            "scenario": name,
            "kind": "error",
            "error": err.to_string(),
        })),
        Ok(NextJobStart::NoGraphKey) => print(json!({
            "case": "next_job_starts_at",
            "scenario": name,
            "kind": "none",
        })),
        Ok(NextJobStart::Never) => print(json!({
            "case": "next_job_starts_at",
            "scenario": name,
            "kind": "never",
        })),
        Ok(NextJobStart::At(at)) => print(json!({
            "case": "next_job_starts_at",
            "scenario": name,
            "kind": "at",
            "at": at.unix_timestamp(),
        })),
    }
}

#[tokio::test]
async fn ranking_service_harness() {
    if std::env::var("SG_CODEINTEL_RANKING_SERVICE_DIFF").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = std::env::var("SG_CODEINTEL_RANKING_SERVICE_TEST_DSN") else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect database");

    // -- repository ranks ---------------------------------------------------
    let repo_rank = repo_rank_config();
    for repo in ["github.com/foo/bar", "github.com/bar", "example.com/x"] {
        let rank = get_repo_rank(&pool, &repo_rank, repo)
            .await
            .expect("repo rank");
        print(json!({
            "case": "repo_rank",
            "repo": repo,
            "rank": [fixed(rank[0]), fixed(rank[1])],
        }));
    }

    // -- document ranks -----------------------------------------------------
    document_ranks_case(&pool, "github.com/foo/bar").await;
    document_ranks_case(&pool, "github.com/bar").await;
    document_ranks_case(&pool, "github.com/deleted").await;
    document_ranks_case(&pool, "github.com/blocked").await;
    document_ranks_case(&pool, "github.com/noranks").await;

    sqlx::query("UPDATE codeintel_ranking_progress SET reducer_completed_at = NULL")
        .execute(&pool)
        .await
        .expect("clear progress");
    document_ranks_case(&pool, "github.com/foo/bar").await;
    sqlx::query(
        "UPDATE codeintel_ranking_progress SET reducer_completed_at = '2024-05-02T00:00:00Z'",
    )
    .execute(&pool)
    .await
    .expect("restore progress");

    // -- next job starts at -------------------------------------------------
    next_case(&pool, "weekly", None).await;
    next_case(&pool, "custom", Some("0 12 * * MON")).await;
    next_case(&pool, "invalid", Some("not a cron")).await;
    next_case(&pool, "past_year", Some("* * * * * 1980")).await;

    // -- service-level derivative graph key ---------------------------------
    let (key, ok) = derivative_graph_key(&pool, &cron_config(None))
        .await
        .expect("derivative graph key");
    print(json!({
        "case": "derivative_graph_key",
        "scenario": "present",
        "key": key,
        "ok": ok,
    }));

    sqlx::query("DELETE FROM codeintel_ranking_graph_keys")
        .execute(&pool)
        .await
        .expect("delete graph keys");
    let (key, ok) = derivative_graph_key(&pool, &cron_config(None))
        .await
        .expect("derivative graph key");
    print(json!({
        "case": "derivative_graph_key",
        "scenario": "absent",
        "key": key,
        "ok": ok,
    }));
    next_case(&pool, "no_graph_key", None).await;
}
