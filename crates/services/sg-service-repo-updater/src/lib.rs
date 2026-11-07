#![forbid(unsafe_code)]

//! `sg-service-repo-updater`: skeleton service crate for `repo-updater`.

pub const SERVICE_NAME: &str = "repo-updater";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}
