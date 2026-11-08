#![forbid(unsafe_code)]

//! Rust `precise-code-intel-worker` process, drop-in for
//! `cmd/precise-code-intel-worker`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    // Baseline `cmd/precise-code-intel-worker/main.go` calls
    // `sanitycheck.Pass()` before `svcmain.SingleServiceMain`.
    sg_core::sanity_check();

    sg_log::init("precise-code-intel-worker");

    // Rust-only rollout knob: lets the pilot listen on a side port while the
    // Go binary keeps `:3188`. Not part of the baseline environment registry,
    // so `--help` output stays identical.
    let service = match std::env::var("SG_PRECISE_WORKER_ADDR") {
        Ok(addr) if !addr.is_empty() => {
            sg_service_precise_worker::PreciseWorkerService::with_addr(addr)
        }
        _ => sg_service_precise_worker::PreciseWorkerService::new(),
    };

    let services: Vec<Box<dyn Service>> = vec![Box::new(service)];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
