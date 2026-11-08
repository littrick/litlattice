//! Port of `cmd/gitserver/internal/cacert`, a subset of the standard
//! library's `x509` root loading that exposes the raw PEM files on Linux.
//!
//! Only used to merge the system certificates into the
//! `tls.external.certificates` CA bundle: git ignores the system store once
//! `GIT_SSL_CAINFO` is set.

use std::io;
use std::path::Path;

/// Possible certificate files; the first readable one wins.
const CERT_FILES: &[&str] = &[
    "/etc/ssl/certs/ca-certificates.crt", // Debian/Ubuntu/Gentoo etc.
    "/etc/pki/tls/certs/ca-bundle.crt",   // Fedora/RHEL 6
    "/etc/ssl/ca-bundle.pem",             // OpenSUSE
    "/etc/pki/tls/cacert.pem",            // OpenELEC
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
    "/etc/ssl/cert.pem",                  // Alpine Linux
];

/// Possible directories with certificate files; all are read.
const CERT_DIRECTORIES: &[&str] = &[
    "/etc/ssl/certs",               // SLES10/SLES11
    "/etc/pki/tls/certs",           // Fedora/RHEL
    "/system/etc/security/cacerts", // Android
];

/// `cacert.System`: PEM-encoded system certificates. Every entry is the raw
/// content of one file, like `AppendCertsFromPEM`.
pub fn system() -> io::Result<Vec<Vec<u8>>> {
    let cert_file = std::env::var("SSL_CERT_FILE")
        .ok()
        .filter(|value| !value.is_empty());
    let cert_dir = std::env::var("SSL_CERT_DIR")
        .ok()
        .filter(|value| !value.is_empty());
    load_system_roots(cert_file.as_deref(), cert_dir.as_deref())
}

/// `loadSystemRoots` (Linux); the environment overrides are parameters so the
/// behavior can be tested without touching the process environment.
fn load_system_roots(cert_file: Option<&str>, cert_dir: Option<&str>) -> io::Result<Vec<Vec<u8>>> {
    let mut roots = Vec::new();
    let mut first_err: Option<io::Error> = None;

    let files: Vec<&str> = match cert_file {
        Some(file) => vec![file],
        None => CERT_FILES.to_vec(),
    };
    for file in files {
        match std::fs::read(file) {
            Ok(data) => {
                roots.push(data);
                break;
            }
            Err(err) => {
                if first_err.is_none() && err.kind() != io::ErrorKind::NotFound {
                    first_err = Some(err);
                }
            }
        }
    }

    let dirs: Vec<String> = match cert_dir {
        // OpenSSL and BoringSSL both use ":" as the separator.
        Some(dirs) => dirs.split(':').map(str::to_owned).collect(),
        None => CERT_DIRECTORIES
            .iter()
            .map(|dir| (*dir).to_owned())
            .collect(),
    };
    for directory in dirs {
        let dir = Path::new(&directory);
        let entries = match read_unique_directory_entries(dir) {
            Ok(entries) => entries,
            Err(err) => {
                if first_err.is_none() && err.kind() != io::ErrorKind::NotFound {
                    first_err = Some(err);
                }
                continue;
            }
        };
        for name in entries {
            if let Ok(data) = std::fs::read(dir.join(&name)) {
                roots.push(data);
            }
        }
    }

    if !roots.is_empty() {
        return Ok(roots);
    }
    match first_err {
        Some(err) => Err(err),
        None => Ok(roots),
    }
}

/// `readUniqueDirectoryEntries`: `os.ReadDir` order (sorted by name), omitting
/// symlinks that point within the directory itself.
fn read_unique_directory_entries(dir: &Path) -> io::Result<Vec<String>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let is_symlink = entry
            .file_type()
            .map(|file_type| file_type.is_symlink())
            .unwrap_or(false);
        if is_symlink && is_same_dir_symlink(&name, dir) {
            continue;
        }
        entries.push(name.to_string_lossy().into_owned());
    }
    entries.sort();
    Ok(entries)
}

/// `isSameDirSymlink`: a symlink whose target does not contain a slash.
fn is_same_dir_symlink(name: &std::ffi::OsStr, dir: &Path) -> bool {
    match std::fs::read_link(dir.join(name)) {
        Ok(target) => !target.to_string_lossy().contains('/'),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_first_readable_certificate_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = tmp.path().join("first.pem");
        std::fs::write(&first, b"first").expect("write");
        let roots = load_system_roots(Some(first.to_str().expect("utf8")), Some("")).expect("load");
        assert_eq!(roots, vec![b"first".to_vec()]);
    }

    #[test]
    fn appends_every_directory_entry_in_name_order() {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::write(tmp.path().join("b.pem"), b"b").expect("write");
        std::fs::write(tmp.path().join("a.pem"), b"a").expect("write");
        // Same-directory symlinks are skipped, like the baseline.
        std::os::unix::fs::symlink("a.pem", tmp.path().join("c.pem")).expect("symlink");
        let roots = load_system_roots(
            Some("/nonexistent/file.pem"),
            Some(tmp.path().to_str().expect("utf8")),
        )
        .expect("load");
        assert_eq!(roots, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn reports_the_first_non_missing_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // Reading a directory as a file fails with a non-`NotFound` error,
        // whatever the process user is.
        let err = load_system_roots(Some(tmp.path().to_str().expect("utf8")), Some(""))
            .expect_err("directory is not a certificate file");
        assert_ne!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn missing_paths_are_not_an_error() {
        let roots = load_system_roots(Some("/nonexistent/a.pem"), Some("/nonexistent/dir"))
            .expect("missing paths");
        assert!(roots.is_empty());
    }
}
