#![forbid(unsafe_code)]

//! `cmd/sourcegraph` assembly: one program, five services, baseline order.
//!
//! The baseline is `FrontendMain(otherServices...)` where `otherServices` is
//! `[gitserver, repo-updater, searcher, worker]`; `svcmain` configures and
//! validates the frontend first, then the rest, and starts them concurrently.
//! The Rust `sg_core::run` performs the same lifecycle, so this module only
//! pins the order the binary must construct.

/// Service order of `cmd/sourcegraph/main.go`.
pub const COMBINED_SERVICES: [&str; 5] = [
    "frontend",
    "gitserver",
    "repo-updater",
    "searcher",
    "worker",
];

/// Verify a constructed service vector matches [`COMBINED_SERVICES`].
pub fn validate_order(names: &[&str]) -> Result<(), String> {
    if names == COMBINED_SERVICES {
        return Ok(());
    }
    Err(format!(
        "unexpected sourcegraph service order: got {names:?}, want {COMBINED_SERVICES:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_is_the_baseline_order() {
        assert!(validate_order(&COMBINED_SERVICES).is_ok());
        assert!(validate_order(&["frontend", "worker", "gitserver"]).is_err());
    }
}
