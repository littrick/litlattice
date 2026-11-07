#![forbid(unsafe_code)]

//! `sg-service-gitserver`: skeleton service crate for `gitserver`.

pub const SERVICE_NAME: &str = "gitserver";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}
