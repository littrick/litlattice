#![forbid(unsafe_code)]

//! `sg-service-worker`: skeleton service crate for `worker`.

pub const SERVICE_NAME: &str = "worker";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}
