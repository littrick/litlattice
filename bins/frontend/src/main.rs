#![forbid(unsafe_code)]

//! Rust `frontend` process, drop-in for `cmd/frontend`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    // Baseline `cmd/frontend/main.go` calls `assets.Init()` then
    // `sanitycheck.Pass()` before `shared.FrontendMain`. The asset manifest is
    // not ported yet.
    sg_core::sanity_check();

    sg_log::init("frontend");

    let services: Vec<Box<dyn Service>> =
        vec![Box::new(sg_service_frontend::FrontendService::new())];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
