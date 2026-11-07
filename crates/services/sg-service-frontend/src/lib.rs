#![forbid(unsafe_code)]

//! `sg-service-frontend`: skeleton service crate for `frontend`.

pub const SERVICE_NAME: &str = "frontend";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}
