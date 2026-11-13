#![forbid(unsafe_code)]

//! `internal/extsvc/perforce`: the type and the gitserver boundary the
//! repo-updater Perforce source needs.
//!
//! Perforce has no independent HTTP API: the baseline source talks to the
//! gitserver (`IsPerforcePathCloneable`, `CheckPerforceCredentials`). The Rust
//! gitserver does not expose those RPCs yet, and this slice does not add them
//! (see the M9.13c overview). [`PerforceGitserver`] is the injectable client
//! boundary; the default source is constructed without one and reports an
//! explicit not-ported error.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// `extsvc.TypePerforce` (stored in `repo.external_service_type`).
pub const TYPE_PERFORCE: &str = "perforce";

/// `perforce.Depot`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Depot {
    /// The path of the Perforce depot.
    #[serde(default)]
    pub depot: String,
}

/// `perforce.Changelist`. The Go type has no JSON tags, so the metadata blob
/// uses the capitalised field names; `CreationDate` is a `time.Time` in the
/// baseline and is carried here as its RFC3339 rendering.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Changelist {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "CreationDate", default)]
    pub creation_date: String,
    #[serde(rename = "State", default)]
    pub state: String,
    #[serde(rename = "Author", default)]
    pub author: String,
    #[serde(rename = "Title", default)]
    pub title: String,
    #[serde(rename = "Message", default)]
    pub message: String,
}

/// `protocol.PerforceConnectionDetails`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PerforceConnectionDetails {
    pub p4_port: String,
    pub p4_user: String,
    pub p4_passwd: String,
}

/// `repos.composePerforceCloneURL`: `url.URL{Scheme:"perforce", Host:host,
/// Path:depot}.String()`. Go prefixes a `/` when the host is non-empty and the
/// path does not already start with one, and percent-encodes the host and path
/// (`net/url`'s `encodeHost`/`encodePath`).
pub fn compose_perforce_clone_url(host: &str, depot: &str) -> String {
    let path = if !depot.is_empty() && !depot.starts_with('/') {
        format!("/{depot}")
    } else {
        depot.to_owned()
    };
    format!(
        "perforce://{}{}",
        escape(host, Mode::Host),
        escape(&path, Mode::Path)
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Host,
    Path,
}

/// `net/url.escape` with Go's uppercase hex digits.
fn escape(value: &str, mode: Mode) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        if should_escape(byte, mode) {
            out.push('%');
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 0xf)] as char);
        } else {
            // Every non-ASCII byte escapes, so the raw bytes here are ASCII.
            out.push(byte as char);
        }
    }
    out
}

/// `net/url.shouldEscape` for `encodeHost`/`encodePath`.
fn should_escape(byte: u8, mode: Mode) -> bool {
    if byte.is_ascii_alphanumeric() {
        return false;
    }
    if mode == Mode::Host {
        match byte {
            b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'='
            | b':' => return false,
            b'[' | b']' => return true,
            _ => {}
        }
    }
    match byte {
        // URL marks and path-segment-friendly reserved characters.
        b'-' | b'_' | b'.' | b'~' => false,
        b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'?' | b'@' => {
            // `encodePath` only escapes `?`; `encodeHost` escapes all of these
            // except the ones handled above.
            mode == Mode::Path && byte == b'?'
        }
        _ => true,
    }
}

/// The gitserver RPC boundary the Perforce source needs. Implementations are
/// responsible for the actual `p4` interaction; the source only composes the
/// requests. No production implementation exists because the Rust gitserver
/// has no Perforce RPCs (not ported in this slice).
#[async_trait]
pub trait PerforceGitserver: Send + Sync {
    /// `gitserver.Client.IsPerforcePathCloneable`.
    async fn is_perforce_path_cloneable(
        &self,
        connection: &PerforceConnectionDetails,
        depot: &str,
    ) -> Result<(), String>;

    /// `gitserver.Client.CheckPerforceCredentials`.
    async fn check_perforce_credentials(
        &self,
        connection: &PerforceConnectionDetails,
    ) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_url_matches_go_url_string() {
        assert_eq!(
            compose_perforce_clone_url("ssl:111.222.333.444:1666", "//Sourcegraph/"),
            "perforce://ssl:111.222.333.444:1666//Sourcegraph/"
        );
        assert_eq!(
            compose_perforce_clone_url("host:1666", "Sourcegraph/"),
            "perforce://host:1666/Sourcegraph/"
        );
        assert_eq!(
            compose_perforce_clone_url("host:1666", ""),
            "perforce://host:1666"
        );
        // `URL.String` percent-encodes the path like Go's `encodePath`.
        assert_eq!(
            compose_perforce_clone_url("host:1666", "//Depot With Space/Sub"),
            "perforce://host:1666//Depot%20With%20Space/Sub"
        );
        assert_eq!(
            compose_perforce_clone_url("host:1666", "//A?B#C/"),
            "perforce://host:1666//A%3FB%23C/"
        );
        assert_eq!(
            compose_perforce_clone_url("host:1666", "//café/"),
            "perforce://host:1666//caf%C3%A9/"
        );
    }

    #[test]
    fn depot_serialises_with_the_depot_tag() {
        let value = serde_json::to_value(Depot {
            depot: "//Sourcegraph/".to_owned(),
        })
        .unwrap();
        assert_eq!(value, serde_json::json!({"depot": "//Sourcegraph/"}));
    }
}
