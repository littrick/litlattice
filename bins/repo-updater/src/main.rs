#![forbid(unsafe_code)]

//! Rust `repo-updater` process, drop-in for `cmd/repo-updater`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    sg_log::init("repo-updater");

    // Rust-only rollout knob: listen on a side port while the Go binary keeps
    // :3182 during gray rollout. Not part of the baseline environment
    // registry, so `--help` output stays identical.
    let service = match std::env::var("SG_REPO_UPDATER_ADDR") {
        Ok(addr) if !addr.is_empty() => {
            sg_service_repo_updater::RepoUpdaterService::with_addr(addr)
        }
        _ => sg_service_repo_updater::RepoUpdaterService::new(),
    };

    let services: Vec<Box<dyn Service>> = vec![Box::new(service)];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
