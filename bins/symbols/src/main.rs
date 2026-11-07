#![forbid(unsafe_code)]

//! Rust `symbols` process, drop-in for `cmd/symbols`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    // Baseline `cmd/symbols/main.go` calls `sanitycheck.Pass()` before
    // `svcmain.SingleServiceMain`.
    sg_core::sanity_check();

    sg_log::init("symbols");

    // Rust-only rollout knob: lets the pilot listen on a side port while the
    // Go binary keeps `:3184` during gray rollout (step 8.5). Not part of the
    // baseline environment registry, so `--help` output stays identical.
    let service = match std::env::var("SG_SYMBOLS_ADDR") {
        Ok(addr) if !addr.is_empty() => sg_service_symbols::SymbolsService::with_addr(addr),
        _ => sg_service_symbols::SymbolsService::new(),
    };

    let services: Vec<Box<dyn Service>> = vec![Box::new(service)];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
