#![forbid(unsafe_code)]

//! Rust `server` process, drop-in for `cmd/server`.
//!
//! `cmd/server` is the single-container entry point: it derives environment
//! defaults, writes the nginx/redis configuration, assembles the Procfile and
//! supervises the child processes with goreman semantics. The startup sequence
//! lives in [`sg_entry::server`].

#[tokio::main]
async fn main() {
    // Baseline `cmd/server/main.go`: `assets.Init(); sanitycheck.Pass()`.
    // `sg_ui::init()` prefers a real `/assets-dist` and otherwise installs the
    // compiled-in tree.
    sg_ui::init();
    sg_core::sanity_check();

    sg_log::init("server");

    let exit_code = sg_entry::server::run_from_env().await;
    std::process::exit(exit_code);
}
