//! M9.8eh differential harness: prints the same `sc` / `dsn` / `rr` lines as
//! `cmd/frontend/internal/cli/zz_service_connections_harness_test.go`.
//!
//! The scenario comes from `SG_SC_HARNESS_SCENARIO` and is driven by
//! `test_ws/p9-go/run_service_connections_diff.sh`; the DSN and replica
//! tables are environment-independent and printed for every run.

use sg_service_frontend::service_connections::{
    current_username, new_postgres_dsn, replica_addrs, service_connections,
};

fn getenv(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn list(values: &[String]) -> String {
    format!("[{}]", values.join(" "))
}

fn dsn_lines(scenario: &str) {
    let user = current_username();
    println!(
        "dsn {scenario} frontend={} frontend-alice={} codeintel={}",
        new_postgres_dsn("frontend", &user, &getenv),
        new_postgres_dsn("frontend", "alice", &getenv),
        new_postgres_dsn("codeintel", &user, &getenv),
    );
}

fn replica_lines() {
    let cases = [
        (
            "kubernetes-3-gitserver",
            "kubernetes",
            "3",
            "gitserver",
            "3178",
        ),
        ("helm-2-searcher", "helm", "2", "searcher", "3181"),
        ("kustomize-2-symbols", "kustomize", "2", "symbols", "3184"),
        (
            "compose-2-searcher",
            "docker-compose",
            "2",
            "searcher",
            "3181",
        ),
        ("dev-2-gitserver", "dev", "2", "gitserver", "3178"),
        ("k3s-1-gitserver", "k3s", "1", "gitserver", "3178"),
        (
            "kubernetes-x-gitserver",
            "kubernetes",
            "x",
            "gitserver",
            "3178",
        ),
        (
            "kubernetes-0-searcher",
            "kubernetes",
            "0",
            "searcher",
            "3181",
        ),
        (
            "kubernetes--1-searcher",
            "kubernetes",
            "-1",
            "searcher",
            "3181",
        ),
        (
            "kubernetes-plus2-searcher",
            "kubernetes",
            "+2",
            "searcher",
            "3181",
        ),
        (
            "kubernetes-2-embeddings",
            "kubernetes",
            "2",
            "embeddings",
            "9991",
        ),
    ];
    for (name, deploy_type, count, service, port) in cases {
        let (out, err) = match replica_addrs(deploy_type, count, service, port) {
            Ok(out) => (out, "nil".to_string()),
            Err(err) => (String::new(), err),
        };
        println!("rr {name} out={out} err={err}");
    }
}

#[test]
fn service_connections_harness() {
    let scenario = std::env::var("SG_SC_HARNESS_SCENARIO").unwrap_or_default();

    if scenario.starts_with("dsn-") {
        dsn_lines(&scenario);
        return;
    }
    if scenario == "replica-table" {
        replica_lines();
        return;
    }

    match service_connections() {
        Ok(connections) => println!(
            "sc {scenario} pg={} ci={} cs={} git={} searchers={} symbols={} embeddings={} zoekts={} ttl={}",
            connections.postgres_dsn,
            connections.code_intel_postgres_dsn,
            connections.code_insights_dsn,
            list(&connections.git_servers),
            list(&connections.searchers),
            list(&connections.symbols),
            list(&connections.embeddings),
            list(&connections.zoekts),
            connections.zoekt_list_ttl.as_nanos(),
        ),
        Err(err) => println!("sc {scenario} err={err}"),
    }
}
