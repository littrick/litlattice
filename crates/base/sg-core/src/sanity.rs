#![forbid(unsafe_code)]

/// Baseline `sanitycheck.Pass`: when `SANITY_CHECK` is exactly `"true"`, print
/// the fixed line and exit 0. Called by the binary entry point before any
/// service configuration runs.
pub fn sanity_check() {
    if std::env::var("SANITY_CHECK").as_deref() == Ok("true") {
        println!("Sanity check passed, exiting without error");
        std::process::exit(0);
    }
}
