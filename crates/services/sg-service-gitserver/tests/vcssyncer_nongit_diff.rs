//! Differential harness for the non-git (package-manager) VCSSyncer
//! (`cmd/gitserver/internal/vcssyncer/packages_syncer.go`, ported in
//! `sg_service_gitserver::vcssyncer::PackageSyncer`).
//!
//! The untracked Go oracle
//! (`cmd/gitserver/internal/vcssyncer/zz_m9_16b_nongit_oracle_test.go`) drives
//! `vcsPackagesSyncer.fetchVersions` against a real temporary bare repository
//! with a fake package source. The Rust side replays the same `versions_runs`
//! with the same fake download contents and `ProcessGitRunner`, then both sides
//! record tags, branches and each tag's `file.txt` content.
//!
//! The package-manager classes covered are npm, Go modules, Python, Rust and
//! Ruby; JVM/coursier is intentionally not ported. Everything is offline: the
//! download is in-memory and no network is used.
//!
//! Inert unless `M916B_CORPUS` and `M916B_OUT` are set.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use sg_service_gitserver::vcssyncer::{
    EmptyPackageVersionLister, PackageGitRunner, PackageKind, PackageSource, PackageSyncer,
    ProcessGitRunner, VersionedPackage,
};

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    kind: String,
    package: String,
    versions_runs: Vec<Vec<String>>,
}

#[derive(Debug)]
struct FakeSource {
    kind: PackageKind,
}

#[async_trait]
impl PackageSource for FakeSource {
    async fn download(&self, dir: &Path, dependency: &VersionedPackage) -> Result<(), String> {
        fs::write(dir.join("file.txt"), dependency.version.as_bytes())
            .map_err(|err| err.to_string())
    }

    fn parse_versioned_package_from_name_and_version(
        &self,
        name: &str,
        version: &str,
    ) -> Result<VersionedPackage, String> {
        VersionedPackage::from_name_and_version(self.kind, name, version)
    }

    fn parse_versioned_package_from_configuration(
        &self,
        dependency: &str,
    ) -> Result<VersionedPackage, String> {
        VersionedPackage::from_configuration(self.kind, dependency)
    }

    fn parse_package_from_repo_name(&self, repo_name: &str) -> Result<String, String> {
        Ok(repo_name.to_owned())
    }
}

fn kind_from_str(kind: &str) -> Option<PackageKind> {
    Some(match kind {
        "npm" => PackageKind::Npm,
        "go" => PackageKind::Go,
        "python" => PackageKind::Python,
        "rust" => PackageKind::Rust,
        "ruby" => PackageKind::Ruby,
        _ => return None,
    })
}

fn placeholder(kind: PackageKind) -> VersionedPackage {
    let (name, version) = if kind == PackageKind::Go {
        ("sourcegraph.com/placeholder", "v0.0.0")
    } else {
        ("placeholder", "1.0.0")
    };
    VersionedPackage::from_name_and_version(kind, name, version)
        .expect("valid placeholder package")
}

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn lines(output: &str) -> Vec<String> {
    output
        .split('\n')
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect()
}

#[tokio::test]
async fn vcssyncer_nongit_diff() {
    let (Ok(corpus_path), Ok(out)) = (
        std::env::var("M916B_CORPUS"),
        std::env::var("M916B_OUT"),
    ) else {
        return;
    };
    let root = tempfile::tempdir().expect("temp root");
    // Keep synthetic git output independent of the host's git config.
    let git_config = root.path().join("gitconfig");
    fs::write(&git_config, b"").expect("write git config");
    std::env::set_var("GIT_CONFIG_GLOBAL", &git_config);
    std::env::set_var("GIT_CONFIG_NOSYSTEM", "1");

    let raw = fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    for case in &cases {
        let kind = kind_from_str(&case.kind).expect("known package kind");
        let dir = root.path().join(format!("{}-bare", case.name));
        fs::create_dir_all(&dir).expect("create bare dir");
        git(&dir, &["init", "--bare", "."]);

        let runner: Arc<dyn PackageGitRunner> = Arc::new(ProcessGitRunner);
        let mut syncer = PackageSyncer::new(
            kind,
            placeholder(kind),
            Vec::new(),
            Arc::new(FakeSource { kind }),
            Arc::new(EmptyPackageVersionLister),
            runner,
        );
        syncer.temp_root = Some(root.path().to_path_buf());

        for versions in &case.versions_runs {
            syncer
                .fetch_versions(&dir, &case.package, versions.clone())
                .await
                .unwrap_or_else(|err| panic!("{}: {err}", case.name));
        }

        let mut tags = lines(&git(
            &dir,
            &["for-each-ref", "--format=%(refname:short)", "refs/tags"],
        ));
        let mut branches = lines(&git(
            &dir,
            &["for-each-ref", "--format=%(refname:short)", "refs/heads"],
        ));
        tags.sort();
        branches.sort();
        let mut contents = BTreeMap::new();
        for tag in &tags {
            let shown = git(&dir, &["show", &format!("{tag}:file.txt")]);
            contents.insert(tag.clone(), shown.trim().to_owned());
        }

        let envelope = json!({
            "tags": tags,
            "branches": branches,
            "contents": contents,
        });
        fs::write(
            Path::new(&out)
                .join("rust")
                .join(format!("{}.json", case.name)),
            serde_json::to_vec(&envelope).expect("serialize envelope"),
        )
        .expect("write rust envelope");
        println!("RUST {}", case.name);
    }
}
