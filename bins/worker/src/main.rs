#![forbid(unsafe_code)]

//! Rust `worker` process, drop-in for `cmd/worker`.
//!
//! Baseline `cmd/worker/main.go` calls `sanitycheck.Pass()` before
//! `svcmain.SingleServiceMain(shared.Service)`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    sg_core::sanity_check();

    sg_log::init("worker");

    // Rust-only rollout knob: lets the pilot listen on a side port while the
    // Go binary keeps `:3189`. Not part of the baseline environment registry,
    // so `--help` output stays identical.
    let service = match std::env::var("SG_WORKER_ADDR") {
        Ok(addr) if !addr.is_empty() => sg_service_worker::WorkerService::with_addr(addr),
        _ => sg_service_worker::WorkerService::new(),
    };

    let services: Vec<Box<dyn Service>> = vec![Box::new(service)];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
