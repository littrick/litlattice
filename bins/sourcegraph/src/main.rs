#![forbid(unsafe_code)]

//! Rust `sourcegraph` process, drop-in for `cmd/sourcegraph`.
//!
//! The Go binary runs the frontend plus gitserver, repo-updater, searcher and
//! worker in one process through `svcmain`. `sg_core::run` provides the same
//! configure/validate/lock/start lifecycle and graceful signal shutdown.

use sg_core::Service;

/// The combined service set in `cmd/sourcegraph/main.go` order: frontend,
/// then gitserver, repo-updater, searcher and worker (`otherServices`).
fn services() -> Vec<Box<dyn Service>> {
    vec![
        Box::new(sg_service_frontend::FrontendService::new()),
        Box::new(sg_service_gitserver::GitserverService::new()),
        Box::new(sg_service_repo_updater::RepoUpdaterService::new()),
        Box::new(sg_service_searcher::SearcherService::new()),
        Box::new(sg_service_worker::WorkerService::new()),
    ]
}

#[tokio::main]
async fn main() {
    // Baseline `cmd/sourcegraph/main.go` calls `sanitycheck.Pass()` first.
    sg_core::sanity_check();

    sg_log::init("sourcegraph");

    let services = services();

    let names: Vec<&str> = services.iter().map(|service| service.name()).collect();
    if let Err(error) = sg_entry::combined::validate_order(&names) {
        eprintln!("{error}");
        std::process::exit(1);
    }

    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The constructed binary must match the Go `cmd/sourcegraph` Procfile
    /// order that [`sg_entry::combined::validate_order`] pins. The e2e smoke
    /// (`test_ws/p9-go/run_server_e2e_smoke.sh`) exercises the same binary
    /// against Postgres/Redis.
    #[test]
    fn constructs_the_baseline_combined_order() {
        let names: Vec<&'static str> = services().iter().map(|service| service.name()).collect();
        assert_eq!(names, sg_entry::combined::COMBINED_SERVICES);
        assert!(sg_entry::combined::validate_order(&names).is_ok());
    }
}
