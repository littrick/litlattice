#![forbid(unsafe_code)]

//! `internal/conf/reposource/{npm_packages,python_packages,ruby_packages,
//! rust_packages,go_modules,jvm_packages}.go`: package names, repo names and
//! the metadata blob stored on a package repository.
//!
//! Only the pieces the repo-updater package sources use are ported: the
//! `ParseVersionedPackageFromConfiguration` / `ParsePackageFromName` /
//! `ParsePackageFromRepoName` trio, `PackageSyntax`, `RepoName` and
//! `Description`. npm scope/name validation and the Go module checks keep the
//! baseline's error strings byte-for-byte.

use serde_json::{json, Value};

use crate::go_modules;

/// Which package ecosystem a [`crate::packages::PackagesSource`] serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageKind {
    Npm,
    Python,
    Ruby,
    Rust,
    Go,
    Jvm,
}

impl PackageKind {
    /// `dependencies.*PackagesScheme`, also the `Package.Scheme()` value.
    pub fn scheme(self) -> &'static str {
        match self {
            Self::Npm => "npm",
            Self::Python => "python",
            Self::Ruby => "scip-ruby",
            Self::Rust => "rust-analyzer",
            Self::Go => "go",
            Self::Jvm => "semanticdb",
        }
    }

    /// `extsvc.KindToType(kind)`, stored in `repo.external_service_type`.
    pub fn service_type(self) -> &'static str {
        match self {
            Self::Npm => "npmPackages",
            Self::Python => "pythonPackages",
            Self::Ruby => "rubyPackages",
            Self::Rust => "rustPackages",
            Self::Go => "goModules",
            Self::Jvm => "jvmPackages",
        }
    }
}

/// The `reposource.Package` surface the source maps into a `types.Repo`.
#[derive(Debug, Clone, PartialEq)]
pub struct PackageInfo {
    pub scheme: &'static str,
    pub syntax: String,
    pub repo_name: String,
    pub description: String,
    pub version: String,
    pub metadata: Value,
}

impl PackageInfo {
    fn new(kind: PackageKind, syntax: String, repo_name: String, metadata: Value) -> Self {
        Self {
            scheme: kind.scheme(),
            syntax,
            repo_name,
            description: String::new(),
            version: String::new(),
            metadata,
        }
    }
}

/// `reposource.Package.PackageSyntax` for the config dependency.
pub fn parse_versioned_package(kind: PackageKind, dependency: &str) -> Result<PackageInfo, String> {
    match kind {
        PackageKind::Npm => npm_parse_versioned(dependency),
        PackageKind::Python => Ok(python_parse(dependency)),
        PackageKind::Ruby => Ok(ruby_parse(dependency)),
        PackageKind::Rust => Ok(rust_parse(dependency)),
        PackageKind::Go => go_parse(dependency),
        PackageKind::Jvm => maven_parse(dependency),
    }
}

/// `packagesSource.ParsePackageFromName`.
pub fn parse_package_from_name(kind: PackageKind, name: &str) -> Result<PackageInfo, String> {
    match kind {
        PackageKind::Npm => npm_parse_from_name(name),
        PackageKind::Python => Ok(python_parse(name)),
        PackageKind::Ruby => Ok(ruby_parse(name)),
        PackageKind::Rust => Ok(rust_parse(name)),
        PackageKind::Go => go_parse(name),
        PackageKind::Jvm => maven_parse_from_name(name),
    }
}

/// `packagesSource.ParsePackageFromRepoName`.
pub fn parse_package_from_repo_name(
    kind: PackageKind,
    repo_name: &str,
) -> Result<PackageInfo, String> {
    match kind {
        PackageKind::Npm => npm_parse_from_repo_name(repo_name),
        PackageKind::Python => {
            let Some(dependency) = repo_name.strip_prefix("python/") else {
                return Err(
                    "invalid python dependency repo name, missing python/ prefix".to_owned(),
                );
            };
            Ok(python_parse(dependency))
        }
        PackageKind::Ruby => {
            const PREFIX: &str = "rubygems/";
            let Some(dependency) = repo_name.strip_prefix(PREFIX) else {
                return Err(format!(
                    "invalid Ruby dependency repo name, missing {PREFIX} prefix '{repo_name}'"
                ));
            };
            Ok(ruby_parse(dependency))
        }
        PackageKind::Rust => {
            let Some(dependency) = repo_name.strip_prefix("crates/") else {
                return Err(format!(
                    "invalid Rust dependency repo name, missing crates/ prefix '{repo_name}'"
                ));
            };
            Ok(rust_parse(dependency))
        }
        PackageKind::Go => {
            let Some(dependency) = repo_name.strip_prefix("go/") else {
                return Err("invalid go dependency repo name, missing go/ prefix".to_owned());
            };
            go_parse(dependency)
        }
        PackageKind::Jvm => maven_parse_from_repo_name(repo_name),
    }
}

// ---------------------------------------------------------------------------
// npm — `internal/conf/reposource/npm_packages.go`
// ---------------------------------------------------------------------------

fn npm_scope_regex() -> regex::Regex {
    // `(?P<scope>[\w\-\.]+)` with Go's ASCII `\w`.
    regex::Regex::new(r"^(?P<scope>[0-9A-Za-z_.\-]+)$").expect("npm scope regex")
}

fn npm_name_regex() -> regex::Regex {
    regex::Regex::new(r"^(?P<name>[0-9A-Za-z_\-]+(\.[0-9A-Za-z_\-]+)*)$").expect("npm name regex")
}

fn npm_versioned_regex() -> regex::Regex {
    regex::Regex::new(
        r"^(@(?P<scope>[0-9A-Za-z_.\-]+)/)?(?P<name>[0-9A-Za-z_\-]+(\.[0-9A-Za-z_\-]+)*)@(?P<version>[0-9A-Za-z_\-]+(\.[0-9A-Za-z_\-]+)*)$",
    )
    .expect("npm versioned regex")
}

fn npm_without_version_regex() -> regex::Regex {
    regex::Regex::new(
        r"^(@(?P<scope>[0-9A-Za-z_.\-]+)/)?(?P<name>[0-9A-Za-z_\-]+(\.[0-9A-Za-z_\-]+)*)",
    )
    .expect("npm name-only regex")
}

fn npm_url_regex() -> regex::Regex {
    regex::Regex::new(
        r"^npm/((?P<scope>[0-9A-Za-z_.\-]+)/)?(?P<name>[0-9A-Za-z_\-]+(\.[0-9A-Za-z_\-]+)*)$",
    )
    .expect("npm repo url regex")
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NpmPackageName {
    pub scope: String,
    pub name: String,
}

impl NpmPackageName {
    pub fn new(scope: &str, name: &str) -> Result<Self, String> {
        if !scope.is_empty() && !npm_scope_regex().is_match(scope) {
            return Err(format!(
                "illegal scope {scope} (allowed characters: 0-9, a-z, A-Z, _, -)"
            ));
        }
        if !npm_name_regex().is_match(name) {
            return Err(format!(
                "illegal package name {name} (allowed characters: 0-9, a-z, A-Z, _, -)"
            ));
        }
        Ok(Self {
            scope: scope.to_owned(),
            name: name.to_owned(),
        })
    }

    /// `NpmPackageName.PackageSyntax`.
    pub fn package_syntax(&self) -> String {
        if self.scope.is_empty() {
            self.name.clone()
        } else {
            format!("@{}/{}", self.scope, self.name)
        }
    }

    /// `NpmPackageName.RepoName`.
    pub fn repo_name(&self) -> String {
        if self.scope.is_empty() {
            format!("npm/{}", self.name)
        } else {
            format!("npm/{}/{}", self.scope, self.name)
        }
    }

    fn metadata(&self) -> Value {
        json!({"Package": {"Scope": self.scope, "Name": self.name}})
    }
}

/// `ParseNpmVersionedPackage`.
pub fn npm_parse_versioned_package(dependency: &str) -> Result<(NpmPackageName, String), String> {
    let regex = npm_versioned_regex();
    let Some(caps) = regex.captures(dependency) else {
        return Err(format!(
            "expected dependency in (@scope/)?name@version format but found {dependency}"
        ));
    };
    let package = NpmPackageName {
        scope: caps
            .name("scope")
            .map(|value| value.as_str().to_owned())
            .unwrap_or_default(),
        name: caps
            .name("name")
            .map(|value| value.as_str().to_owned())
            .expect("name group"),
    };
    let version = caps
        .name("version")
        .map(|value| value.as_str().to_owned())
        .expect("version group");
    Ok((package, version))
}

/// `ParseNpmPackageNameWithoutVersion`.
pub fn npm_parse_package_name_without_version(input: &str) -> Result<NpmPackageName, String> {
    let regex = npm_without_version_regex();
    let Some(caps) = regex.captures(input) else {
        return Err(format!(
            "expected dependency in (@scope/)?name format but found {input}"
        ));
    };
    Ok(NpmPackageName {
        scope: caps
            .name("scope")
            .map(|value| value.as_str().to_owned())
            .unwrap_or_default(),
        name: caps
            .name("name")
            .map(|value| value.as_str().to_owned())
            .expect("name group"),
    })
}

/// `ParseNpmPackageFromRepoURL`.
pub fn npm_parse_package_from_repo_url(repo_name: &str) -> Result<NpmPackageName, String> {
    let regex = npm_url_regex();
    let Some(caps) = regex.captures(repo_name) else {
        return Err(format!(
            "expected path in npm/(scope/)?name format but found {repo_name}"
        ));
    };
    Ok(NpmPackageName {
        scope: caps
            .name("scope")
            .map(|value| value.as_str().to_owned())
            .unwrap_or_default(),
        name: caps
            .name("name")
            .map(|value| value.as_str().to_owned())
            .expect("name group"),
    })
}

/// `ParseNpmPackageFromPackageSyntax`.
fn npm_parse_package_from_package_syntax(syntax: &str) -> Result<NpmPackageName, String> {
    Ok(npm_parse_versioned_package(&format!("{syntax}@0"))?.0)
}

fn npm_parse_versioned(dependency: &str) -> Result<PackageInfo, String> {
    let (package, version) = npm_parse_versioned_package(dependency)?;
    Ok(npm_info(package, version))
}

fn npm_parse_from_name(name: &str) -> Result<PackageInfo, String> {
    // `ParsePackageFromRepoName("npm/" + strings.TrimPrefix(name, "@"))`.
    npm_parse_from_repo_name(&format!("npm/{}", name.strip_prefix('@').unwrap_or(name)))
}

fn npm_parse_from_repo_name(repo_name: &str) -> Result<PackageInfo, String> {
    let package = npm_parse_package_from_repo_url(repo_name)?;
    // `NpmVersionedPackage{NpmPackageName: pkg}` (empty version).
    Ok(npm_info(package, String::new()))
}

fn npm_info(package: NpmPackageName, version: String) -> PackageInfo {
    let mut info = PackageInfo::new(
        PackageKind::Npm,
        package.package_syntax(),
        package.repo_name(),
        package.metadata(),
    );
    info.version = version;
    info
}

/// `ParseNpmPackageFromPackageSyntax` exposed for the npm client.
pub fn npm_package_from_syntax(syntax: &str) -> Result<NpmPackageName, String> {
    npm_parse_package_from_package_syntax(syntax)
}

// ---------------------------------------------------------------------------
// python — `internal/conf/reposource/python_packages.go`
// ---------------------------------------------------------------------------

fn python_parse(dependency: &str) -> PackageInfo {
    let (name, version) = match dependency.rfind("==") {
        None => (dependency.to_owned(), String::new()),
        Some(index) => (
            dependency[..index].trim().to_owned(),
            dependency[index + 2..].trim().to_owned(),
        ),
    };
    let mut info = PackageInfo::new(
        PackageKind::Python,
        name.clone(),
        format!("python/{name}"),
        json!({}),
    );
    info.version = version;
    info
}

// ---------------------------------------------------------------------------
// ruby — `internal/conf/reposource/ruby_packages.go`
// ---------------------------------------------------------------------------

fn ruby_parse(dependency: &str) -> PackageInfo {
    let (name, version) = match dependency.rfind('@') {
        None => (dependency.to_owned(), String::new()),
        Some(index) => (
            dependency[..index].trim().to_owned(),
            dependency[index + 1..].trim().to_owned(),
        ),
    };
    let mut info = PackageInfo::new(
        PackageKind::Ruby,
        name.clone(),
        format!("rubygems/{name}"),
        json!({}),
    );
    info.version = version;
    info
}

// ---------------------------------------------------------------------------
// rust — `internal/conf/reposource/rust_packages.go`
// ---------------------------------------------------------------------------

fn rust_parse(dependency: &str) -> PackageInfo {
    let (name, version) = match dependency.rfind('@') {
        None => (dependency.to_owned(), String::new()),
        Some(index) => (
            dependency[..index].trim().to_owned(),
            dependency[index + 1..].trim().to_owned(),
        ),
    };
    let mut info = PackageInfo::new(
        PackageKind::Rust,
        name.clone(),
        format!("crates/{name}"),
        json!({}),
    );
    info.version = version;
    info
}

// ---------------------------------------------------------------------------
// go — `internal/conf/reposource/go_modules.go`
// ---------------------------------------------------------------------------

/// `ParseGoVersionedPackage`.
pub fn go_parse_versioned_package(dependency: &str) -> Result<go_modules::ModuleVersion, String> {
    let (path, version) = match dependency.rfind('@') {
        None => (dependency.to_owned(), String::new()),
        Some(index) => (
            dependency[..index].to_owned(),
            dependency[index + 1..].to_owned(),
        ),
    };
    if version.is_empty() {
        go_modules::check_path(&path)?;
    } else {
        go_modules::check(&path, &version)?;
    }
    Ok(go_modules::ModuleVersion { path, version })
}

fn go_parse(dependency: &str) -> Result<PackageInfo, String> {
    let module = go_parse_versioned_package(dependency)?;
    let mut info = PackageInfo::new(
        PackageKind::Go,
        module.path.clone(),
        format!("go/{}", module.path),
        json!({}),
    );
    info.version = module.version;
    Ok(info)
}

// ---------------------------------------------------------------------------
// jvm — `internal/conf/reposource/jvm_packages.go`
// ---------------------------------------------------------------------------

/// `reposource.MavenModule`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MavenModule {
    pub group_id: String,
    pub artifact_id: String,
}

impl MavenModule {
    fn is_jdk(&self) -> bool {
        *self == jdk_module()
    }

    fn package_syntax(&self) -> String {
        format!("{}:{}", self.group_id, self.artifact_id)
    }

    fn repo_name(&self) -> String {
        if self.is_jdk() {
            "jdk".to_owned()
        } else {
            format!("maven/{}/{}", self.group_id, self.artifact_id)
        }
    }

    fn metadata(&self) -> Value {
        json!({"Module": {"GroupID": self.group_id, "ArtifactID": self.artifact_id}})
    }
}

fn jdk_module() -> MavenModule {
    MavenModule {
        group_id: "jdk".to_owned(),
        artifact_id: "jdk".to_owned(),
    }
}

/// `ParseMavenVersionedPackage`.
pub fn maven_parse_versioned_package(
    dependency: &str,
) -> Result<(MavenModule, String), String> {
    let parts: Vec<&str> = dependency.split(':').collect();
    let (mut module, version) = match parts.len() {
        3 => (
            MavenModule {
                group_id: parts[0].to_owned(),
                artifact_id: parts[1].to_owned(),
            },
            parts[2].to_owned(),
        ),
        2 => (
            MavenModule {
                group_id: parts[0].to_owned(),
                artifact_id: parts[1].to_owned(),
            },
            String::new(),
        ),
        _ => {
            return Err(format!(
                "dependency {} must contain at least one colon ':' character",
                go_quote(dependency)
            ));
        }
    };
    module.group_id = std::mem::take(&mut module.group_id);
    Ok((module, version))
}

fn maven_info(module: MavenModule, version: String) -> PackageInfo {
    let mut info = PackageInfo::new(
        PackageKind::Jvm,
        module.package_syntax(),
        module.repo_name(),
        module.metadata(),
    );
    info.version = version;
    info
}

fn maven_parse(dependency: &str) -> Result<PackageInfo, String> {
    let (module, version) = maven_parse_versioned_package(dependency)?;
    Ok(maven_info(module, version))
}

fn maven_parse_from_name(name: &str) -> Result<PackageInfo, String> {
    if name == "jdk" {
        return Ok(maven_info(jdk_module(), String::new()));
    }
    maven_parse(name)
}

fn maven_parse_from_repo_name(repo_name: &str) -> Result<PackageInfo, String> {
    let name = repo_name.strip_prefix("maven/").unwrap_or(repo_name);
    let name = name.replace('/', ":");
    maven_parse_from_name(&name)
}

/// `fmt.Sprintf("%q", s)` (Go string quoting) used by the Maven error.
pub fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn npm_names_and_versions() {
        let (package, version) = npm_parse_versioned_package("@scope/pkg@1.2.3").unwrap();
        assert_eq!(package.scope, "scope");
        assert_eq!(package.name, "pkg");
        assert_eq!(version, "1.2.3");
        assert_eq!(package.repo_name(), "npm/scope/pkg");
        assert_eq!(package.package_syntax(), "@scope/pkg");

        let info = parse_package_from_name(PackageKind::Npm, "react").unwrap();
        assert_eq!(info.repo_name, "npm/react");
        assert_eq!(info.syntax, "react");
        assert_eq!(
            info.metadata,
            json!({"Package":{"Scope":"","Name":"react"}})
        );

        let err = npm_parse_versioned_package("bad").unwrap_err();
        assert_eq!(
            err,
            "expected dependency in (@scope/)?name@version format but found bad"
        );
    }

    #[test]
    fn ecosystem_repo_names() {
        assert_eq!(
            parse_package_from_name(PackageKind::Python, "requests==2.1")
                .unwrap()
                .repo_name,
            "python/requests"
        );
        assert_eq!(
            parse_package_from_name(PackageKind::Ruby, "rails@7.0")
                .unwrap()
                .repo_name,
            "rubygems/rails"
        );
        assert_eq!(
            parse_package_from_name(PackageKind::Rust, "serde@1.0")
                .unwrap()
                .repo_name,
            "crates/serde"
        );
        assert_eq!(
            parse_package_from_name(PackageKind::Jvm, "com.google.guava:guava:30.0-jre")
                .unwrap()
                .repo_name,
            "maven/com.google.guava/guava"
        );
        assert_eq!(
            parse_package_from_name(PackageKind::Jvm, "jdk")
                .unwrap()
                .repo_name,
            "jdk"
        );
        assert_eq!(
            parse_package_from_name(PackageKind::Jvm, "jdk")
                .unwrap()
                .metadata,
            json!({"Module":{"GroupID":"jdk","ArtifactID":"jdk"}})
        );
    }

    #[test]
    fn go_repo_names_and_errors() {
        let info = parse_package_from_name(PackageKind::Go, "github.com/gorilla/mux@v1.8.0").unwrap();
        assert_eq!(info.repo_name, "go/github.com/gorilla/mux");
        assert_eq!(info.version, "v1.8.0");

        let err = parse_package_from_name(PackageKind::Go, "nodot/mod").unwrap_err();
        assert_eq!(
            err,
            "malformed module path \"nodot/mod\": missing dot in first path element"
        );
    }

    #[test]
    fn repo_name_prefix_errors_match() {
        assert_eq!(
            parse_package_from_repo_name(PackageKind::Python, "foo/bar").unwrap_err(),
            "invalid python dependency repo name, missing python/ prefix"
        );
        assert_eq!(
            parse_package_from_repo_name(PackageKind::Ruby, "foo/bar").unwrap_err(),
            "invalid Ruby dependency repo name, missing rubygems/ prefix 'foo/bar'"
        );
        assert_eq!(
            parse_package_from_repo_name(PackageKind::Rust, "foo/bar").unwrap_err(),
            "invalid Rust dependency repo name, missing crates/ prefix 'foo/bar'"
        );
        assert_eq!(
            parse_package_from_repo_name(PackageKind::Go, "foo/bar").unwrap_err(),
            "invalid go dependency repo name, missing go/ prefix"
        );
    }
}
