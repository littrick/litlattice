#![forbid(unsafe_code)]

//! `sg-service-symbols`: skeleton service crate for `symbols`.

pub const SERVICE_NAME: &str = "symbols";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}
