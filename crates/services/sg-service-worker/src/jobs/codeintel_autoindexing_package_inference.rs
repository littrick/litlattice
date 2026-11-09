#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/inference/infer.go` — map a
//! versioned package to the repository and revision that should be indexed.
//!
//! [`infer_repository_and_revision`] ports `InferRepositoryAndRevision`: the
//! six recognizers are tried in order and the first match wins.
//!
//! * go (`gomod`): only `https://github.com/…` names; the repository is the
//!   first three slash-separated segments of the name with the `https://`
//!   prefix stripped, and a `v?[\d.]+-<commit>` version contributes only the
//!   commit.
//! * JVM (`semanticdb`): the name is the repository, the revision is
//!   `v` + version.
//! * npm (`npm`): `(@scope/)?name` is parsed with the reposource rules and
//!   the repository is `npm/scope/name` (or `npm/name`), revision
//!   `v` + version.
//! * Rust (`rust-analyzer`): `name(@version)?` → `crates/name`,
//!   revision `v` + version.
//! * Python (`python`): `name(==version)?` → `python/name`, revision is the
//!   version unchanged.
//! * Ruby (`scip-ruby`): `name(@version)?` → `rubygems/name`, revision is the
//!   version unchanged.
//!
//! The `reposource` parsing helpers used by the last four recognizers are
//! ported here as small string functions (the npm name validation mirrors the
//! package-name regular expressions without a regex dependency).
//!
//! [`DefaultPackageRepoInference`] adapts this to the enqueuer's
//! [`PackageRepoInference`] seam.
//!
//! Erratum: `inferGoRepositoryAndRevision` slices `Split(name, "/")[:3]`, so
//! a `https://github.com/…` name with fewer than three segments panics in the
//! baseline. The port treats such names as "not matched" (the remaining
//! recognizers reject the `gomod` scheme, so the result is no mapping).

use super::codeintel_autoindexing_enqueuer::{PackageRepoInference, VersionedPackageRepo};

/// `GitHubScheme`.
pub const GITHUB_SCHEME: &str = "https://";

pub const GOMOD_SCHEME: &str = "gomod";
/// `dependencies.JVMPackagesScheme`.
pub const JVM_PACKAGES_SCHEME: &str = "semanticdb";
/// `dependencies.NpmPackagesScheme`.
pub const NPM_PACKAGES_SCHEME: &str = "npm";
/// `dependencies.RustPackagesScheme`.
pub const RUST_PACKAGES_SCHEME: &str = "rust-analyzer";
/// `dependencies.PythonPackagesScheme`.
pub const PYTHON_PACKAGES_SCHEME: &str = "python";
/// `dependencies.RubyPackagesScheme`.
pub const RUBY_PACKAGES_SCHEME: &str = "scip-ruby";

/// `InferRepositoryAndRevision`.
pub fn infer_repository_and_revision(package: &VersionedPackageRepo) -> Option<(String, String)> {
    infer_go(package)
        .or_else(|| infer_jvm(package))
        .or_else(|| infer_npm(package))
        .or_else(|| infer_rust(package))
        .or_else(|| infer_python(package))
        .or_else(|| infer_ruby(package))
}

/// `inferGoRepositoryAndRevision`.
fn infer_go(package: &VersionedPackageRepo) -> Option<(String, String)> {
    if package.scheme != GOMOD_SCHEME {
        return None;
    }
    let github_prefix = format!("{GITHUB_SCHEME}github.com/");
    if !package.name.starts_with(&github_prefix) {
        return None;
    }
    // `strings.Split(name[len("https://"):], "/")[:3]`: only the "https://"
    // prefix is stripped, so "github.com" remains the first segment. At
    // least three segments are required (see the module erratum).
    let rest = &package.name[GITHUB_SCHEME.len()..];
    let mut parts = rest.split('/');
    let repo = format!("{}/{}/{}", parts.next()?, parts.next()?, parts.next()?);

    let version = go_version_commit(&package.version)
        .map(str::to_owned)
        .unwrap_or_else(|| package.version.clone());
    Some((repo, version))
}

/// `goVersionPattern = ^v?[\d\.]+-([a-f0-9]+)`; returns the captured commit.
fn go_version_commit(version: &str) -> Option<&str> {
    let rest = version.strip_prefix('v').unwrap_or(version);
    let bytes = rest.as_bytes();

    let mut digits = 0;
    while digits < bytes.len() && (bytes[digits].is_ascii_digit() || bytes[digits] == b'.') {
        digits += 1;
    }
    if digits == 0 || digits >= bytes.len() || bytes[digits] != b'-' {
        return None;
    }

    let commit = &rest[digits + 1..];
    let commit_bytes = commit.as_bytes();
    let mut hex = 0;
    while hex < commit_bytes.len()
        && (commit_bytes[hex].is_ascii_digit() || (b'a'..=b'f').contains(&commit_bytes[hex]))
    {
        hex += 1;
    }
    if hex == 0 {
        return None;
    }
    Some(&commit[..hex])
}

/// `inferJVMRepositoryAndRevision`.
fn infer_jvm(package: &VersionedPackageRepo) -> Option<(String, String)> {
    if package.scheme != JVM_PACKAGES_SCHEME {
        return None;
    }
    Some((package.name.clone(), format!("v{}", package.version)))
}

/// `inferNpmRepositoryAndRevision`.
fn infer_npm(package: &VersionedPackageRepo) -> Option<(String, String)> {
    if package.scheme != NPM_PACKAGES_SCHEME {
        return None;
    }
    let name = parse_npm_package_from_package_syntax(&package.name)?;
    Some((name.repo_name(), format!("v{}", package.version)))
}

/// `inferRustRepositoryAndRevision`.
fn infer_rust(package: &VersionedPackageRepo) -> Option<(String, String)> {
    if package.scheme != RUST_PACKAGES_SCHEME {
        return None;
    }
    let name = package_name_before(&package.name, "@");
    Some((format!("crates/{name}"), format!("v{}", package.version)))
}

/// `inferPythonRepositoryAndRevision`.
fn infer_python(package: &VersionedPackageRepo) -> Option<(String, String)> {
    if package.scheme != PYTHON_PACKAGES_SCHEME {
        return None;
    }
    let name = package_name_before(&package.name, "==");
    Some((format!("python/{name}"), package.version.clone()))
}

/// `inferRubyRepositoryAndRevision`.
fn infer_ruby(package: &VersionedPackageRepo) -> Option<(String, String)> {
    if package.scheme != RUBY_PACKAGES_SCHEME {
        return None;
    }
    let name = package_name_before(&package.name, "@");
    Some((format!("rubygems/{name}"), package.version.clone()))
}

/// `ParseRustVersionedPackage` / `ParseRubyVersionedPackage` /
/// `ParseVersionedPackage`: split at the last separator and trim both sides;
/// no separator means the whole string is the name.
fn package_name_before(dependency: &str, separator: &str) -> String {
    match dependency.rfind(separator) {
        Some(index) => dependency[..index].trim().to_owned(),
        None => dependency.to_owned(),
    }
}

/// An `NpmPackageName` parsed from `(@scope/)?name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NpmPackageName {
    pub scope: Option<String>,
    pub name: String,
}

impl NpmPackageName {
    /// `NpmPackageName.RepoName`.
    pub fn repo_name(&self) -> String {
        match &self.scope {
            Some(scope) => format!("npm/{scope}/{}", self.name),
            None => format!("npm/{}", self.name),
        }
    }
}

/// `ParseNpmPackageFromPackageSyntax` → `ParseNpmVersionedPackage(name + "@0")`.
pub(crate) fn parse_npm_package_from_package_syntax(package_name: &str) -> Option<NpmPackageName> {
    let dependency = format!("{package_name}@0");
    // `scopedPackageNameRegex` requires a final `@version`; a name containing
    // `@` therefore cannot leave the version segment empty or misplaced.
    let (prefix, version) = dependency.rsplit_once('@')?;
    if !valid_dotted_word(version) {
        return None;
    }

    match prefix.strip_prefix('@') {
        Some(scoped) => {
            let (scope, name) = scoped.split_once('/')?;
            if !valid_scope(scope) || !valid_dotted_word(name) {
                return None;
            }
            Some(NpmPackageName {
                scope: Some(scope.to_owned()),
                name: name.to_owned(),
            })
        }
        None => {
            if !valid_dotted_word(prefix) {
                return None;
            }
            Some(NpmPackageName {
                scope: None,
                name: prefix.to_owned(),
            })
        }
    }
}

/// `[\w\-]+(\.[\w\-]+)*` (Go's `\w` is ASCII word characters).
fn valid_dotted_word(value: &str) -> bool {
    !value.is_empty() && value.split('.').all(valid_word_segment)
}

fn valid_word_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// `NpmScopeRegexString = [\w\-\.]+` (empty is not allowed here).
fn valid_scope(scope: &str) -> bool {
    !scope.is_empty()
        && scope
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// The production [`PackageRepoInference`]: `inference.InferRepositoryAndRevision`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultPackageRepoInference;

impl PackageRepoInference for DefaultPackageRepoInference {
    fn infer_repository_and_revision(
        &self,
        package: &VersionedPackageRepo,
    ) -> Option<(String, String)> {
        infer_repository_and_revision(package)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(scheme: &str, name: &str, version: &str) -> VersionedPackageRepo {
        VersionedPackageRepo {
            scheme: scheme.to_owned(),
            name: name.to_owned(),
            version: version.to_owned(),
        }
    }

    fn infer(scheme: &str, name: &str, version: &str) -> Option<(String, String)> {
        infer_repository_and_revision(&package(scheme, name, version))
    }

    #[test]
    fn go_repositories_keep_the_commit_part() {
        assert_eq!(
            infer(
                "gomod",
                "https://github.com/sourcegraph/sourcegraph",
                "v0.0.0-de0123456789"
            ),
            Some((
                "github.com/sourcegraph/sourcegraph".to_owned(),
                "de0123456789".to_owned()
            ))
        );
        assert_eq!(
            infer(
                "gomod",
                "https://github.com/aws/aws-sdk-go-v2/credentials",
                "v0.1.0"
            ),
            Some((
                "github.com/aws/aws-sdk-go-v2".to_owned(),
                "v0.1.0".to_owned()
            ))
        );
        assert_eq!(infer("gomod", "https://gitlab.com/a/b", "v1.0.0"), None);
    }

    #[test]
    fn jvm_uses_the_name_as_repository() {
        assert_eq!(
            infer("semanticdb", "com.example:lib", "1.2.3"),
            Some(("com.example:lib".to_owned(), "v1.2.3".to_owned()))
        );
    }

    #[test]
    fn npm_scopes_become_repository_prefixes() {
        assert_eq!(
            infer("npm", "mypackage", "1.0.0"),
            Some(("npm/mypackage".to_owned(), "v1.0.0".to_owned()))
        );
        assert_eq!(
            infer("npm", "@myscope/mypackage", "1.0.0"),
            Some(("npm/myscope/mypackage".to_owned(), "v1.0.0".to_owned()))
        );
        assert_eq!(infer("npm", "bad name", "1.0.0"), None);
        assert_eq!(infer("npm", "@scope/", "1.0.0"), None);
    }

    #[test]
    fn rust_python_and_ruby_prefixes() {
        assert_eq!(
            infer("rust-analyzer", "serde@1.0", "1.0.100"),
            Some(("crates/serde".to_owned(), "v1.0.100".to_owned()))
        );
        assert_eq!(
            infer("python", "requests==2.0", "2.0"),
            Some(("python/requests".to_owned(), "2.0".to_owned()))
        );
        assert_eq!(
            infer("scip-ruby", "rake@13.0", "13.0"),
            Some(("rubygems/rake".to_owned(), "13.0".to_owned()))
        );
    }

    #[test]
    fn unknown_schemes_are_not_mapped() {
        assert_eq!(infer("nonsense", "x", "1"), None);
    }

    #[test]
    fn go_version_without_commit_is_kept() {
        assert_eq!(go_version_commit("v2.3.2"), None);
        assert_eq!(go_version_commit("v2.3.2-ABC"), None);
        assert_eq!(go_version_commit("v2.3.2-ab"), Some("ab"));
        assert_eq!(go_version_commit("...-ab"), Some("ab"));
    }

    #[test]
    fn short_go_names_do_not_panic() {
        assert_eq!(infer("gomod", "https://github.com/a", "v1"), None);
        assert_eq!(infer("gomod", "https://github.com", "v1"), None);
    }
}
