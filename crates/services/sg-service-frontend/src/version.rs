#![forbid(unsafe_code)]

//! Port of `internal/version.Version`.
//!
//! The version string is baked into the binary at build time (Go `ldflags`).
//! The Rust build has no equivalent injector, so the deployment-provided
//! `/version.txt` is the only override: when the file exists its **raw bytes**
//! become the version (no trimming — Go assigns `string(versionFromFile)`),
//! otherwise the dev fallback `0.0.0+dev` is used.

use std::fs;

use bytes::Bytes;

/// `version.devVersion`.
pub const DEV_VERSION: &str = "0.0.0+dev";

/// Path read by the baseline `init()`.
pub const VERSION_FILE: &str = "/version.txt";

/// `version.Version()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version(Bytes);

impl Version {
    /// The dev fallback (`version.devVersion`).
    pub fn dev() -> Self {
        Self(Bytes::from_static(DEV_VERSION.as_bytes()))
    }

    /// Read `/version.txt`, falling back to [`Version::dev`] exactly like the
    /// baseline `init()` (any read error keeps the dev version).
    pub fn load() -> Self {
        match fs::read(VERSION_FILE) {
            Ok(contents) => Self(Bytes::from(contents)),
            Err(_) => Self::dev(),
        }
    }

    /// Test hook: an explicit version string.
    pub fn from_bytes(contents: impl Into<Bytes>) -> Self {
        Self(contents.into())
    }

    /// `Version()`: the bytes written by `/healthz` and `/__version`.
    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::dev()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_version_matches_the_baseline() {
        assert_eq!(Version::dev().as_bytes(), b"0.0.0+dev");
        assert_eq!(Version::default(), Version::dev());
    }

    #[test]
    fn explicit_bytes_are_kept_verbatim() {
        // The baseline does not trim `/version.txt`; a trailing newline is
        // part of the version string.
        let version = Version::from_bytes("1.2.3\n");
        assert_eq!(version.as_bytes(), b"1.2.3\n");
    }
}
