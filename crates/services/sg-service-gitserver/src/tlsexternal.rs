//! Port of `executil.getTlsExternalDoNotInvoke`: the git TLS settings derived
//! from `experimentalFeatures.tlsExternal`.
//!
//! The baseline caches the result with `conf.Cached` and recomputes when the
//! site configuration changes. The Rust config client has no watcher yet, so
//! the settings are computed once at startup (the CA bundle is written once and
//! lives for the process lifetime, like the baseline).

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use sg_config::{SiteConfiguration, TlsExternal};

/// `executil.tlsConfig`.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// `GIT_SSL_NO_VERIFY`: `tlsExternal.insecureSkipVerify`.
    pub ssl_no_verify: bool,
    /// `GIT_SSL_CAINFO`: a process-lifetime PEM bundle when
    /// `tlsExternal.certificates` is non-empty.
    pub ssl_ca_info: Option<PathBuf>,
}

impl TlsConfig {
    /// `getTlsExternalDoNotInvoke`.
    pub fn from_site(site: &SiteConfiguration) -> Self {
        let tls = site.tls_external();
        if tls.certificates.is_empty() {
            return Self {
                ssl_no_verify: tls.insecure_skip_verify,
                ssl_ca_info: None,
            };
        }
        Self::from_parts(&tls, crate::cacert::system())
    }

    /// The pure part of `getTlsExternalDoNotInvoke`, with the system roots
    /// injected so tests do not depend on the host's certificate store.
    fn from_parts(tls: &TlsExternal, system_roots: io::Result<Vec<Vec<u8>>>) -> Self {
        let mut bundle: Vec<u8> = Vec::new();
        for certificate in &tls.certificates {
            bundle.extend_from_slice(certificate.as_bytes());
            bundle.push(b'\n');
        }

        // git ignores the system certificates once `SSLCAInfo` is set, so the
        // configured certificates are merged with the system store.
        match system_roots {
            Ok(roots) => {
                if roots.is_empty() {
                    tracing::warn!(
                        "no system certificates found for inclusion in SSLCAInfo. Git will now \
                         fail to speak to TLS services not specified in your TlsExternal site \
                         configuration."
                    );
                }
                for root in roots {
                    bundle.extend_from_slice(&root);
                    bundle.push(b'\n');
                }
            }
            Err(err) => tracing::error!(
                error = %err,
                "failed to load system certificates for inclusion in SSLCAInfo. Git will now \
                 fail to speak to TLS services not specified in your TlsExternal site \
                 configuration."
            ),
        }

        let ssl_ca_info = match write_temp_file(&bundle) {
            Ok(path) => Some(path),
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to create file holding tls.external.certificates for git"
                );
                None
            }
        };
        Self {
            ssl_no_verify: tls.insecure_skip_verify,
            ssl_ca_info,
        }
    }
}

/// `writeTempFile("gitserver*.crt", data)`: not cleaned up because it lives
/// for the process lifetime.
fn write_temp_file(data: &[u8]) -> io::Result<PathBuf> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir();
    for _ in 0..100 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!(
            "gitserver{}-{nanos}-{counter}.crt",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => {
                use std::io::Write as _;
                if let Err(err) = file.write_all(data) {
                    let _ = std::fs::remove_file(&path);
                    return Err(err);
                }
                return Ok(path);
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "failed to create the tls.external certificate file",
    ))
}

/// Read a CA bundle back, for tests.
#[cfg(test)]
fn read_bundle(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).expect("read bundle")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tls(certificates: &[&str], insecure: bool) -> TlsExternal {
        TlsExternal {
            insecure_skip_verify: insecure,
            certificates: certificates.iter().map(|cert| (*cert).to_owned()).collect(),
        }
    }

    #[test]
    fn no_certificates_means_no_ca_bundle() {
        // `getTlsExternalDoNotInvoke` returns before touching the system
        // store when no certificates are configured.
        let site = SiteConfiguration::parse(
            r#"{"experimentalFeatures":{"tlsExternal":{"insecureSkipVerify":true}}}"#,
        )
        .expect("parse");
        let config = TlsConfig::from_site(&site);
        assert!(config.ssl_no_verify);
        assert_eq!(config.ssl_ca_info, None);
    }

    #[test]
    fn merges_configured_and_system_certificates() {
        let config = TlsConfig::from_parts(
            &tls(&["CONFIGURED"], false),
            Ok(vec![b"SYSTEM-A".to_vec(), b"SYSTEM-B".to_vec()]),
        );
        assert!(!config.ssl_no_verify);
        let path = config.ssl_ca_info.expect("bundle");
        assert_eq!(read_bundle(&path), "CONFIGURED\nSYSTEM-A\nSYSTEM-B\n");
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn keeps_the_bundle_when_system_roots_fail() {
        let config = TlsConfig::from_parts(
            &tls(&["ONLY"], false),
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "nope")),
        );
        let path = config.ssl_ca_info.expect("bundle");
        assert_eq!(read_bundle(&path), "ONLY\n");
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn reads_the_site_configuration() {
        let site = SiteConfiguration::parse(
            r#"{"experimentalFeatures":{"tlsExternal":{"insecureSkipVerify":true,"certificates":["A"]}}}"#,
        )
        .expect("parse");
        let config = TlsConfig::from_site(&site);
        assert!(config.ssl_no_verify);
        assert!(config.ssl_ca_info.is_some());
        std::fs::remove_file(config.ssl_ca_info.expect("bundle")).ok();
    }
}
