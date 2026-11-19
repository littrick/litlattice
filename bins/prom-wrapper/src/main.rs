#![forbid(unsafe_code)]

//! Rust `prom-wrapper`, drop-in for
//! `docker-images/prometheus/cmd/prom-wrapper`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    sg_core::sanity_check();
    sg_log::init("prom-wrapper");

    let services: Vec<Box<dyn Service>> =
        vec![Box::new(sg_service_prom_wrapper::PromWrapperService::new())];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
