#![forbid(unsafe_code)]

//! `internal/debugserver` address resolution.
//!
//! Go reads `SRC_PROF_HTTP` with the default `:6060`; when the variable is set
//! to the empty string (as `/server` does for the whole Procfile), it looks the
//! binary name up in the `SRC_PROF_SERVICES` JSON list exported by
//! `cmd/server`. Reproduce that fallback so per-service metrics ports match the
//! bundled Prometheus target list.

use serde::Deserialize;

const DEFAULT_PROF_HTTP_ADDR: &str = ":6060";

#[derive(Debug, Deserialize)]
struct ProfService {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Host")]
    host: String,
}

/// Resolve the debug/metrics listen address for `service`.
///
/// `registered_value` is the value returned by
/// `env.get("SRC_PROF_HTTP", ":6060", ...)`:
/// - non-empty: use it verbatim;
/// - empty and `SRC_PROF_SERVICES` contains the service: use that host;
/// - empty and no entry: `None` (debug server disabled).
pub fn prof_http_addr(service: &str, registered_value: &str) -> Option<String> {
    if !registered_value.is_empty() {
        return Some(registered_value.to_owned());
    }
    lookup_service(service)
}

/// The `SRC_PROF_SERVICES` entry for `service`, if any.
pub fn lookup_service(service: &str) -> Option<String> {
    let raw = std::env::var("SRC_PROF_SERVICES").ok()?;
    let services: Vec<ProfService> = serde_json::from_str(&raw).ok()?;
    services
        .into_iter()
        .find(|candidate| candidate.name == service)
        .map(|candidate| candidate.host)
}

/// The Go `env.Get` default, exposed for call sites/tests.
pub const fn default_prof_http_addr() -> &'static str {
    DEFAULT_PROF_HTTP_ADDR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_empty_value_wins() {
        assert_eq!(
            prof_http_addr("frontend", ":1234"),
            Some(":1234".to_owned())
        );
    }

    #[test]
    fn empty_value_falls_back_to_services_list() {
        // Tests run in the same process; use the real env carefully.
        let previous = std::env::var("SRC_PROF_SERVICES").ok();
        std::env::set_var(
            "SRC_PROF_SERVICES",
            r#"[{"Name":"frontend","Host":"127.0.0.1:6063"}]"#,
        );
        assert_eq!(
            prof_http_addr("frontend", ""),
            Some("127.0.0.1:6063".to_owned())
        );
        assert_eq!(prof_http_addr("worker", ""), None);
        match previous {
            Some(value) => std::env::set_var("SRC_PROF_SERVICES", value),
            None => std::env::remove_var("SRC_PROF_SERVICES"),
        }
    }
}
