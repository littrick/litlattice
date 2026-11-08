#![forbid(unsafe_code)]

//! M9.8c differential harness (Rust side): the repo statistics compactor and
//! resetter store operations.
//!
//! Expects `test_ws/p9-go/repostatistics_fixtures.sql` to have been applied to
//! `SG_QTEST_DSN` immediately before the run. `SG_REPOSTATS_MODE` is `compact`
//! or `recreate`. The Go counterpart is
//! `cmd/worker/internal/repostatistics/harness_test.go`.

use sg_db::repo_statistics::{
    compact_gitserver_repos_statistics, compact_repo_statistics, delete_and_recreate_statistics,
    get_gitserver_repos_statistics, get_repo_statistics,
};

#[tokio::test]
async fn repo_statistics_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let Ok(mode) = std::env::var("SG_REPOSTATS_MODE") else {
        return;
    };

    let pool = sg_db::connect(&dsn, 1).await.expect("connect database");
    match mode.as_str() {
        "compact" => {
            compact_repo_statistics(&pool)
                .await
                .expect("compact repo statistics");
            compact_gitserver_repos_statistics(&pool)
                .await
                .expect("compact gitserver repos statistics");
        }
        "recreate" => {
            delete_and_recreate_statistics(&pool)
                .await
                .expect("delete and recreate statistics");
        }
        other => panic!("unknown SG_REPOSTATS_MODE {other:?}"),
    }

    let repo = get_repo_statistics(&pool)
        .await
        .expect("get repo statistics");
    println!(
        "repo_statistics total={} soft_deleted={} not_cloned={} cloning={} cloned={} failed_fetch={} corrupted={}",
        repo.total,
        repo.soft_deleted,
        repo.not_cloned,
        repo.cloning,
        repo.cloned,
        repo.failed_fetch,
        repo.corrupted
    );

    let mut gitserver = get_gitserver_repos_statistics(&pool)
        .await
        .expect("get gitserver repos statistics");
    gitserver.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));
    for row in gitserver {
        // `%q` in the Go harness quotes the shard id with Go's escaping; shard
        // ids are plain identifiers here, so `{:?}` matches byte for byte.
        println!(
            "gitserver_stats shard={:?} total={} not_cloned={} cloning={} cloned={} failed_fetch={} corrupted={}",
            row.shard_id,
            row.total,
            row.not_cloned,
            row.cloning,
            row.cloned,
            row.failed_fetch,
            row.corrupted
        );
    }
}
