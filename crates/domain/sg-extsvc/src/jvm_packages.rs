#![forbid(unsafe_code)]

//! `internal/repos/jvm_packages.go` + `internal/conf/reposource/jvm_packages.go`:
//! Maven dependency parsing for the JVM package source.
//!
//! # Not-ported boundary
//!
//! The baseline's `jvmPackagesSource.Get` resolves a dependency through
//! `coursier.Exists`, but that import is **commented out** in `jvm_packages.go`
//! (importing it pulled coursier's side effects into the frontend and
//! repo-updater binaries). Repo enumeration never reaches that code, so no
//! coursier port is needed and this crate deliberately adds no heavy JVM
//! dependency. The ledger is [`NOT_PORTED`].
//!
//! Everything the source does use — parsing `group:artifact[:version]`,
//! `jdk` handling, repo names and metadata — lives in
//! [`crate::reposource_packages`].

pub use crate::reposource_packages::{
    maven_parse_versioned_package, parse_package_from_name, parse_package_from_repo_name,
    parse_versioned_package, MavenModule, PackageInfo, PackageKind,
};

/// Functions from the baseline that are intentionally not ported.
pub const NOT_PORTED: &[&str] = &[
    // `jvmPackagesSource.Get` calls `coursier.Exists`; the import and the
    // function are commented out in the baseline (jvm_packages.go).
    "jvm_packages.go: jvmPackagesSource.Get (coursier.Exists) — commented out in the baseline",
    // `internal/extsvc/jvmpackages/coursier`: fetching sources via coursier.
    "internal/extsvc/jvmpackages/coursier: not ported, adds a heavy JVM dependency",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jdk_and_maven_repo_names() {
        let jdk = parse_package_from_name(PackageKind::Jvm, "jdk").unwrap();
        assert_eq!(jdk.repo_name, "jdk");
        assert_eq!(jdk.syntax, "jdk:jdk");

        let maven =
            parse_package_from_name(PackageKind::Jvm, "com.google.guava:guava:30.0-jre").unwrap();
        assert_eq!(maven.repo_name, "maven/com.google.guava/guava");
        assert_eq!(maven.version, "30.0-jre");
    }

    #[test]
    fn not_ported_ledger_names_the_coursier_boundary() {
        assert!(NOT_PORTED.iter().any(|entry| entry.contains("coursier")));
    }
}
