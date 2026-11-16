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
    // `sg_ui::init()` installs the `/assets-dist` DirProvider, exactly like Go;
    // a missing manifest surfaces as the UI shell's 500 rather than silently
    // falling back to the embedded dummy manifest. Set `SG_UI_EMBEDDED_ASSETS=1`
    // to opt into the compiled-in tree for source checkouts without a build.
    sg_ui::init();
    sg_core::sanity_check();

    sg_log::init("server");

    let exit_code = sg_entry::server::run_from_env().await;
    std::process::exit(exit_code);
}
