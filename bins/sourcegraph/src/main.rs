#![forbid(unsafe_code)]

//! Rust `sourcegraph` process, drop-in for `cmd/sourcegraph`.
//!
//! The Go binary runs the frontend plus gitserver, repo-updater, searcher and
//! worker in one process through `svcmain`. `sg_core::run` provides the same
//! configure/validate/lock/start lifecycle and graceful signal shutdown.

use sg_core::Service;

#[tokio::main]
async fn main() {
    // Baseline `cmd/sourcegraph/main.go` calls `sanitycheck.Pass()` first.
    sg_core::sanity_check();

    sg_log::init("sourcegraph");

    let services: Vec<Box<dyn Service>> = vec![
        Box::new(sg_service_frontend::FrontendService::new()),
        Box::new(sg_service_gitserver::GitserverService::new()),
        Box::new(sg_service_repo_updater::RepoUpdaterService::new()),
        Box::new(sg_service_searcher::SearcherService::new()),
        Box::new(sg_service_worker::WorkerService::new()),
    ];

    let names: Vec<&str> = services.iter().map(|service| service.name()).collect();
    if let Err(error) = sg_entry::combined::validate_order(&names) {
        eprintln!("{error}");
        std::process::exit(1);
    }

    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
