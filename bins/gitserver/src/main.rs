#![forbid(unsafe_code)]

//! Rust `gitserver` process, drop-in for `cmd/gitserver`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    sg_log::init("gitserver");

    // Rust-only rollout knob: listen on a side port while the Go binary keeps
    // :3178 during gray rollout. Not part of the baseline environment
    // registry, so `--help` output stays identical.
    let service = match std::env::var("SG_GITSERVER_ADDR") {
        Ok(addr) if !addr.is_empty() => sg_service_gitserver::GitserverService::with_addr(addr),
        _ => sg_service_gitserver::GitserverService::new(),
    };

    let services: Vec<Box<dyn Service>> = vec![Box::new(service)];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
