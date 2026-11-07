#![forbid(unsafe_code)]

//! Rust `searcher` process, drop-in for `cmd/searcher`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    // Baseline `cmd/searcher/main.go` calls `sanitycheck.Pass()` before
    // `svcmain.SingleServiceMain`.
    sg_core::sanity_check();

    sg_log::init("searcher");

    // Rust-only rollout knob: lets the pilot listen on a side port while the
    // Go binary keeps `:3181` during gray rollout. Not part of the baseline
    // environment registry, so `--help` output stays identical.
    let service = match std::env::var("SG_SEARCHER_ADDR") {
        Ok(addr) if !addr.is_empty() => sg_service_searcher::SearcherService::with_addr(addr),
        _ => sg_service_searcher::SearcherService::new(),
    };

    let services: Vec<Box<dyn Service>> = vec![Box::new(service)];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
