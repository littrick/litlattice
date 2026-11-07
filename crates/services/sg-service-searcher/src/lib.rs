#![forbid(unsafe_code)]

//! `sg-service-searcher`: skeleton service crate for `searcher`.

pub const SERVICE_NAME: &str = "searcher";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}
