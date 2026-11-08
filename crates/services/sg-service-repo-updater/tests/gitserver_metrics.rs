#![forbid(unsafe_code)]

//! Live check that repo-updater's gitserver client is wired like the
//! baseline: enqueuing one repository through the public gRPC API must
//! increment `src_gitserver_addr_for_repo_invoked` and record the
//! `src_gitserver_repositoryservice_client_*` RED families under
//! `op="FetchRepository", scope="repoupdater.scheduler"`.
//!
//! Gated on:
//! - `SG_REPO_UPDATER_GRPC` (host:port of a running repo-updater),
//! - `SG_REPO_UPDATER_METRICS` (base URL serving `/metrics`, defaults to the
//!   same host on the debug port via `SG_REPO_UPDATER_METRICS_PORT`),
//! - `SG_TEST_DATABASE_DSN` (to pick a repository the process knows).

use sg_proto::repoupdater as pb;

fn metric_value(body: &str, family: &str) -> Option<f64> {
    body.lines()
        .find_map(|line| line.strip_prefix(family)?.split_whitespace().next())
        .and_then(|value| value.parse().ok())
}

fn has_series(body: &str, selector: &str) -> bool {
    body.lines().any(|line| line.starts_with(selector))
}

#[tokio::test]
async fn enqueue_records_gitserver_client_metrics() {
    let Ok(grpc_addr) = std::env::var("SG_REPO_UPDATER_GRPC") else {
        eprintln!("SG_REPO_UPDATER_GRPC unset; skipping");
        return;
    };
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
        return;
    };
    let metrics_base = std::env::var("SG_REPO_UPDATER_METRICS")
        .unwrap_or_else(|_| format!("http://{grpc_addr}/metrics"));

    let pool = sg_db::connect(&dsn, 2).await.expect("connect");
    // A repo the running process can resolve through its own store. Prefer
    // one gitserver already cloned, so the success path (duration sample +
    // count) is exercised when available.
    let repo: String = sqlx::query_scalar(
        "SELECT repo.name FROM repo \
         LEFT JOIN gitserver_repos ON gitserver_repos.repo_id = repo.id \
         WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL \
         ORDER BY (gitserver_repos.last_fetched IS NULL), repo.id LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("at least one repo");

    let http = sg_http::HttpClient::new();
    let before = metric_value(
        &String::from_utf8(http.get(&metrics_base).await.expect("scrape")).expect("utf8"),
        "src_gitserver_addr_for_repo_invoked",
    )
    .unwrap_or(0.0);

    let mut client = pb::repo_updater_service_client::RepoUpdaterServiceClient::connect(format!(
        "http://{grpc_addr}"
    ))
    .await
    .expect("connect to repo-updater");
    let response = client
        .enqueue_repo_update(pb::EnqueueRepoUpdateRequest { repo: repo.clone() })
        .await
        .expect("enqueue repo update")
        .into_inner();
    assert_eq!(response.name, repo);

    // The scheduler's update loop picks the high-priority entry up promptly;
    // the RPC itself is recorded only after it finishes.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let body = String::from_utf8(http.get(&metrics_base).await.expect("scrape")).expect("utf8");
        let after = metric_value(&body, "src_gitserver_addr_for_repo_invoked").unwrap_or(0.0);
        let recorded = has_series(
            &body,
            "src_gitserver_repositoryservice_client_total{op=\"FetchRepository\",scope=\"repoupdater.scheduler\"}",
        ) || has_series(
            &body,
            "src_gitserver_repositoryservice_client_errors_total{op=\"FetchRepository\",scope=\"repoupdater.scheduler\"}",
        );
        if after > before && recorded {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "gitserver client metrics did not move: addr {before} -> {after}, recorded={recorded}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
