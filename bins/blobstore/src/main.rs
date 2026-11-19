#![forbid(unsafe_code)]

//! Rust `blobstore` process, drop-in for `cmd/blobstore`.

use sg_core::Service;

#[tokio::main]
async fn main() {
    sg_core::sanity_check();
    sg_log::init("blobstore");

    let services: Vec<Box<dyn Service>> =
        vec![Box::new(sg_service_blobstore::BlobstoreService::new())];
    let exit_code = sg_core::run(services).await;
    std::process::exit(exit_code);
}
