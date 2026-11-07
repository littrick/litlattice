#![forbid(unsafe_code)]

//! `sg-service-precise-worker`: skeleton service crate for `precise-code-intel-worker`.

pub const SERVICE_NAME: &str = "precise-code-intel-worker";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}
