//! Read-path git commands: `archive`, `read_file`, `changed_files`.
//!
//! The baseline shells out to `git` with `cwd = $GIT_DIR` and `GIT_DIR` set in
//! the environment (`common.GitDir.Set`); we do the same so that no discovery
//! happens when a repository is broken.

use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::process::Stdio;

use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

use crate::repo::clean_path_bytes;

/// Go `streamio.WriteBufferSize`: the largest slice handed to a stream send.
pub const STREAM_CHUNK: usize = 128 * 1024;

/// Go `chunk.maxMessageSize`.
pub const CHUNK_MESSAGE_SIZE: usize = 1024 * 1024;

/// Faults that the gRPC layer maps to `NotFound`.
#[derive(Debug)]
pub enum GitError {
    RevisionNotFound(String),
    FileNotFound,
    Message(String),
}

impl GitError {
    pub fn message(self) -> String {
        match self {
            Self::RevisionNotFound(spec) => format!("revision not found: {spec}"),
            Self::FileNotFound => "file not found".to_owned(),
            Self::Message(message) => message,
        }
    }
}

/// One repository, addressed by its git directory.
#[derive(Debug, Clone)]
pub struct GitRepo {
    pub git_dir: PathBuf,
}

/// Go `os.ModeDir`.
pub const MODE_DIR: u32 = 1 << 31;
/// Go `os.ModeSymlink`.
pub const MODE_SYMLINK: u32 = 1 << 27;
/// Go `gitdomain.ModeSubmodule` = `0o160000 | os.ModeDevice`.
pub const MODE_SUBMODULE: u32 = 0o160000 | (1 << 26);
/// `gitdomain.ModeSymlink`, the git type bit for symlink blobs.
const GIT_MODE_SYMLINK: u32 = 0o20000;

/// A submodule's `.gitmodules` metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submodule {
    pub url: String,
    pub path: Vec<u8>,
    pub commit_sha: String,
}

/// One `ls-tree` entry, already mapped to the values `proto.FileInfo` carries.
///
/// `name` is the full path relative to the repository root (the baseline does
/// not strip the directory), including for recursive listings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: Vec<u8>,
    pub size: i64,
    pub mode: u32,
    /// Set for trees and blobs; empty for submodules.
    pub blob_oid: Option<String>,
    pub submodule: Option<Submodule>,
}

/// `for-each-ref` field order used by `ListRefs`.
const FOR_EACH_REF_FORMAT: &str = "%(objecttype)%00%(HEAD)%00%(refname)%00%(refname:short)%00\
     %(objectname)%00%(*objectname)%00%(creatordate:unix)";

/// `logFormatWithoutRefs`: record separator, then ten NUL-separated fields.
const LOG_FORMAT_WITHOUT_REFS: &str = "--format=format:%x1e%H%x00%aN%x00%aE%x00%at%x00%cN%x00\
     %cE%x00%ct%x00%B%x00%P%x00";

/// Go `time.Time{}` as a proto timestamp (year 1).
pub const ZERO_TIMESTAMP_SECONDS: i64 = -62_135_596_800;

/// `proto.GitRef.RefType` without the proto dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefType {
    Unspecified,
    Branch,
    Tag,
}

/// One ref, already mapped to `proto.GitRef`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRef {
    pub ref_name: Vec<u8>,
    pub short_ref_name: Vec<u8>,
    pub target_commit: String,
    pub ref_oid: String,
    /// Unix seconds; `ZERO_TIMESTAMP_SECONDS` for the Go zero time.
    pub created_at_seconds: i64,
    pub ref_type: RefType,
    pub is_head: bool,
}

/// A commit signature (`proto.GitSignature`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub name: Vec<u8>,
    pub email: Vec<u8>,
    pub date_seconds: i64,
}

/// A commit plus the files it modified (`proto.GitCommit`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommit {
    pub oid: String,
    pub author: Signature,
    pub committer: Signature,
    pub message: Vec<u8>,
    pub parents: Vec<String>,
    pub modified_files: Vec<Vec<u8>>,
}

/// `git.CommitLogOrder` without the proto dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogOrder {
    Default,
    CommitDate,
    TopoDate,
}

/// `git.CommitLogOpts` without the proto dependency.
#[derive(Debug, Clone)]
pub struct CommitLogOptions {
    pub ranges: Vec<Vec<u8>>,
    pub all_refs: bool,
    /// The gRPC handler passes `GetAfter().AsTime()` even when the timestamp is
    /// unset; a nil `Timestamppb` yields the Unix epoch (not Go's zero time), so
    /// both filters are always emitted.
    pub after_seconds: i64,
    pub before_seconds: i64,
    pub max_commits: u32,
    pub skip: u32,
    pub follow_only_first_parent: bool,
    pub include_modified_files: bool,
    pub order: CommitLogOrder,
    pub message_query: Vec<u8>,
    pub author_query: Vec<u8>,
    pub path: Vec<u8>,
    pub follow_path_renames: bool,
}

/// `git.GitDiffComparisonType` without the proto dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawDiffComparison {
    OnlyInHead,
    Intersection,
}

impl GitRepo {
    pub fn new(git_dir: impl Into<PathBuf>) -> Self {
        Self {
            git_dir: git_dir.into(),
        }
    }

    fn command(&self, args: &[OsString]) -> Command {
        let mut command = Command::new("git");
        command
            .args(args)
            .current_dir(&self.git_dir)
            // Baseline `GitDir.Set`: avoid git discovering a parent repo.
            .env("GIT_DIR", &self.git_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
    }

    async fn output(&self, args: &[OsString]) -> Result<std::process::Output, GitError> {
        self.command(args)
            .output()
            .await
            .map_err(|err| GitError::Message(format!("failed to run git: {err}")))
    }

    /// `git cat-file -t -- <spec>`; exit 128 with the baseline's stderr
    /// markers means the revision does not exist.
    async fn object_type(&self, spec: &str) -> Result<String, GitError> {
        let output = self
            .output(&["cat-file".into(), "-t".into(), "--".into(), spec.into()])
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.code() == Some(128)
                && (stderr.contains("Not a valid object name")
                    || stderr.contains("git cat-file: could not get object info"))
            {
                return Err(GitError::RevisionNotFound(spec.to_owned()));
            }
            return Err(GitError::Message(format!(
                "git cat-file -t {spec}: {}",
                stderr.trim_end()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    /// `git rev-parse <spec> --`, returning the first output line.
    async fn rev_parse(&self, spec: &str) -> Result<String, GitError> {
        let output = self
            .output(&["rev-parse".into(), spec.into(), "--".into()])
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.code() == Some(128)
                && (stderr.contains("bad revision")
                    || stderr.contains("unknown revision")
                    || stderr.contains("expected commit type"))
            {
                return Err(GitError::RevisionNotFound(spec.to_owned()));
            }
            return Err(GitError::Message(format!(
                "git rev-parse {spec}: {}",
                stderr.trim_end()
            )));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.lines().next().unwrap_or_default().trim().to_owned())
    }

    /// Baseline `ResolveRevision`: empty means `HEAD`; anything else is
    /// resolved through `<spec>^0` so that non-commit objects are rejected.
    pub async fn resolve_revision(&self, spec: &str) -> Result<String, GitError> {
        let spec = if spec.is_empty() { "HEAD" } else { spec };
        let spec = if spec == "HEAD" {
            spec.to_owned()
        } else {
            format!("{spec}^0")
        };
        if spec.starts_with('-') {
            return Err(GitError::Message(format!(
                "invalid git revision spec {spec:?} (begins with '-')"
            )));
        }
        self.rev_parse(&spec).await
    }

    /// Baseline `GetObject`: resolve the name, then report its object type.
    /// Returns the raw OID bytes (the proto field is `bytes`) and the type.
    pub async fn get_object(&self, name: &str) -> Result<(Vec<u8>, String), GitError> {
        if name.starts_with('-') {
            return Err(GitError::Message(format!(
                "invalid git revision spec {name:?} (begins with '-')"
            )));
        }
        let sha = self.rev_parse(name).await?;
        let oid = decode_hex(&sha)
            .ok_or_else(|| GitError::Message(format!("failed to decode OID {sha:?}")))?;
        let object_type = self.object_type(&sha).await?;
        Ok((oid, object_type))
    }

    /// Baseline `SymbolicRefHead`. `short` asks git for the abbreviated ref.
    pub async fn symbolic_ref_head(&self, short: bool) -> Result<String, GitError> {
        let mut args: Vec<OsString> = vec!["symbolic-ref".into(), "HEAD".into()];
        if short {
            args.push("--short".into());
        }
        let output = self.output(&args).await?;
        if !output.status.success() {
            return Err(GitError::Message(format!(
                "git symbolic-ref HEAD: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    /// Baseline `RevParseHead`, including the "HEAD does not point to
    /// anything" case of an empty repository.
    pub async fn rev_parse_head(&self) -> Result<String, GitError> {
        let sha = self.rev_parse("HEAD").await?;
        if sha == "HEAD" {
            return Err(GitError::RevisionNotFound("HEAD".to_owned()));
        }
        Ok(sha)
    }

    /// Baseline `MergeBase`: exit 1 with empty stderr means "no merge base"
    /// and is reported as an empty SHA rather than an error.
    pub async fn merge_base(&self, base: &str, head: &str) -> Result<String, GitError> {
        let output = self
            .output(&["merge-base".into(), "--".into(), base.into(), head.into()])
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.code() == Some(1) && stderr.is_empty() {
                return Ok(String::new());
            }
            if output.status.code() == Some(128)
                && stderr.contains("fatal: Not a valid object name")
            {
                let spec = if stderr.contains(base) { base } else { head };
                return Err(GitError::RevisionNotFound(spec.to_owned()));
            }
            return Err(GitError::Message(format!(
                "git merge-base: {}",
                stderr.trim_end()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    /// Launch `git archive` and return the child; the caller pumps stdout.
    pub async fn archive(
        &self,
        format: &str,
        treeish: &str,
        paths: &[Vec<u8>],
    ) -> Result<Child, GitError> {
        if treeish.starts_with('-') {
            return Err(GitError::Message(format!(
                "invalid git revision spec {treeish:?} (begins with '-')"
            )));
        }
        self.object_type(treeish).await?;

        let mut args: Vec<OsString> = vec![
            "archive".into(),
            "--worktree-attributes".into(),
            format!("--format={format}").into(),
        ];
        if format == "zip" {
            // Baseline stores zip entries uncompressed (`-0`).
            args.push("-0".into());
        }
        args.push(treeish.into());
        args.push("--".into());
        for path in paths {
            let mut spec = OsString::from(":(literal)");
            spec.push(OsStr::from_bytes(path));
            args.push(spec);
        }

        self.command(&args)
            .spawn()
            .map_err(|err| GitError::Message(format!("failed to run git archive: {err}")))
    }

    /// `git ls-tree <commit> -- <path>` then `git cat-file -p <blob>`.
    ///
    /// A gitlink (submodule, mode 160000) reads as an empty file, like the
    /// baseline.
    pub async fn read_file(&self, commit: &str, path: &[u8]) -> Result<Option<Child>, GitError> {
        let output = self
            .output(&[
                "ls-tree".into(),
                commit.into(),
                "--".into(),
                OsStr::from_bytes(path).to_owned(),
            ])
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.code() == Some(128)
                && (stderr.contains("not a tree object")
                    || stderr.contains("Not a valid object name"))
            {
                return Err(GitError::RevisionNotFound(commit.to_owned()));
            }
            return Err(GitError::Message(format!(
                "git ls-tree {commit}: {}",
                stderr.trim_end()
            )));
        }
        let stdout = output.stdout;
        let stdout = trim_ascii(&stdout);
        if stdout.is_empty() {
            return Err(GitError::FileNotFound);
        }
        // format: 100644 blob <oid>\t<path>
        let mut fields = stdout.splitn(3, |byte| *byte == b' ');
        let _mode = fields.next();
        let _kind = fields.next();
        let oid = fields
            .next()
            .map(|rest| rest.split(|byte| *byte == b'\t').next().unwrap_or(rest))
            .ok_or_else(|| GitError::Message("unexpected output while parsing blob OID".into()))?;
        if stdout.starts_with(b"160000") {
            // Submodule: the baseline returns an empty reader.
            return Ok(None);
        }

        let child = self
            .command(&[
                "cat-file".into(),
                "-p".into(),
                OsStr::from_bytes(oid).to_owned(),
            ])
            .spawn()
            .map_err(|err| GitError::Message(format!("failed to run git cat-file: {err}")))?;
        Ok(Some(child))
    }

    /// `git diff-tree -r --root --format=format: --no-prefix --name-status
    /// --no-renames -z [base] head`, parsed into (status, path) pairs.
    pub async fn changed_files(
        &self,
        base: Option<&[u8]>,
        head: &[u8],
    ) -> Result<Vec<(u8, Vec<u8>)>, GitError> {
        let mut args: Vec<OsString> = vec![
            "diff-tree".into(),
            "-r".into(),
            "--root".into(),
            "--format=format:".into(),
            "--no-prefix".into(),
            "--name-status".into(),
            "--no-renames".into(),
            "-z".into(),
        ];
        if let Some(base) = base.filter(|base| !base.is_empty()) {
            // The baseline wraps base failures with `errors.Wrapf`, which
            // keeps `RevisionNotFoundError` in the chain, so both base and
            // head resolution failures surface as `NotFound`.
            let oid = self.rev_parse(&String::from_utf8_lossy(base)).await?;
            args.push(oid.into());
        }
        let head_spec = String::from_utf8_lossy(head).into_owned();
        let head_oid = self.rev_parse(&head_spec).await?;
        args.push(head_oid.into());

        let output = self.output(&args).await?;
        if !output.status.success() {
            return Err(GitError::Message(format!(
                "git diff-tree: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            )));
        }
        parse_name_status(&output.stdout)
    }

    /// Baseline `Stat`: `git ls-tree --long -z` of a single path, with the
    /// root tree resolved through `<commit>^{tree}` because `ls-tree` does not
    /// list the root itself.
    pub async fn stat(&self, commit: &str, path: &[u8]) -> Result<TreeEntry, GitError> {
        check_spec_arg_safety(commit)?;
        let path = clean_path_bytes(&rel_bytes(path));
        if path == b"." {
            let sha = match self.rev_parse(&format!("{commit}^{{tree}}")).await {
                Ok(sha) => sha,
                // Baseline maps a missing revision at the root to
                // `os.ErrNotExist`, i.e. "file not found".
                Err(GitError::RevisionNotFound(_)) => return Err(GitError::FileNotFound),
                Err(err) => return Err(err),
            };
            return Ok(TreeEntry {
                name: Vec::new(),
                size: 0,
                mode: MODE_DIR,
                blob_oid: Some(sha),
                submodule: None,
            });
        }

        let entries = self.ls_tree(commit, Some(&path), false).await?;
        entries.into_iter().next().ok_or(GitError::FileNotFound)
    }

    /// Baseline `ReadDir`: list the entries under `path`, adding a trailing
    /// slash so `ls-tree` lists the directory's contents rather than the
    /// directory entry itself.
    pub async fn read_dir(
        &self,
        commit: &str,
        path: &[u8],
        recursive: bool,
    ) -> Result<Vec<TreeEntry>, GitError> {
        check_spec_arg_safety(commit)?;
        let path = if path.is_empty() {
            Vec::new()
        } else {
            let mut cleaned = clean_path_bytes(&rel_bytes(path));
            cleaned.push(b'/');
            cleaned
        };
        let entries = self.ls_tree(commit, Some(&path), recursive).await?;
        // An empty root tree is valid; an empty subdirectory means the path
        // does not exist.
        if entries.is_empty() && clean_path_bytes(&path) != b"." {
            return Err(GitError::FileNotFound);
        }
        Ok(entries)
    }

    /// `git ls-tree --long --full-name -z <commit> [-r -t] [-- :(literal)<path>]`
    ///
    /// `:(literal)` prevents glob expansion, like the baseline.
    async fn ls_tree(
        &self,
        commit: &str,
        path: Option<&[u8]>,
        recursive: bool,
    ) -> Result<Vec<TreeEntry>, GitError> {
        let mut args: Vec<OsString> = vec![
            "ls-tree".into(),
            "--long".into(),
            "--full-name".into(),
            "-z".into(),
            commit.into(),
        ];
        if recursive {
            // `-t` keeps tree entries when recursing.
            args.push("-r".into());
            args.push("-t".into());
        }
        if let Some(path) = path.filter(|path| !path.is_empty()) {
            let mut spec = b":(literal)".to_vec();
            spec.extend_from_slice(path);
            args.push("--".into());
            args.push(OsStr::from_bytes(&spec).to_owned());
        }

        let output = self.output(&args).await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("exists on disk, but not in") {
                return Err(GitError::FileNotFound);
            }
            if output.status.code() == Some(128)
                && (stderr.contains("fatal: not a tree object")
                    || stderr.contains("fatal: Not a valid object name"))
            {
                return Err(GitError::RevisionNotFound(commit.to_owned()));
            }
            return Err(GitError::Message(format!(
                "git ls-tree {commit}: {}",
                stderr.trim_end()
            )));
        }

        let raws = parse_ls_tree(&output.stdout)?;
        let modules = if raws.iter().any(|entry| entry.kind == "commit") {
            self.git_modules_config(commit).await?
        } else {
            GitModules::default()
        };
        Ok(raws
            .into_iter()
            .map(|raw| raw.into_tree_entry(&modules))
            .collect())
    }

    /// Baseline `gitModulesConfig`: `.gitmodules` at `commit`, empty when the
    /// file does not exist.
    async fn git_modules_config(&self, commit: &str) -> Result<GitModules, GitError> {
        let child = match self.read_file(commit, b".gitmodules").await {
            Ok(Some(child)) => child,
            // Absent file (or a submodule at that path) means no overrides.
            Ok(None) | Err(GitError::FileNotFound) => return Ok(GitModules::default()),
            Err(err) => return Err(err),
        };
        let output = child
            .wait_with_output()
            .await
            .map_err(|err| format!("failed to run git cat-file: {err}"))
            .map_err(GitError::Message)?;
        if !output.status.success() {
            return Err(GitError::Message(format!(
                "error reading .gitmodules: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            )));
        }
        Ok(parse_gitmodules(&output.stdout))
    }

    /// Baseline `ListRefs`: `for-each-ref` with the same sort keys, format and
    /// filters. Output order is git's and is preserved.
    pub async fn list_refs(
        &self,
        heads_only: bool,
        tags_only: bool,
        points_at_commit: &[String],
        contains_sha: Option<&str>,
    ) -> Result<Vec<GitRef>, GitError> {
        let mut args: Vec<OsString> = vec![
            "for-each-ref".into(),
            "--sort".into(),
            "-refname".into(),
            "--sort".into(),
            "-creatordate".into(),
            "--sort".into(),
            "-HEAD".into(),
            "--format".into(),
            FOR_EACH_REF_FORMAT.into(),
        ];
        for commit in points_at_commit {
            args.push(format!("--points-at={commit}").into());
        }
        if let Some(contains) = contains_sha.filter(|contains| !contains.is_empty()) {
            args.push(format!("--contains={contains}").into());
        }
        // The `--` separator is added once, before the first pattern.
        let mut added_separator = false;
        if heads_only {
            added_separator = true;
            args.push("--".into());
            args.push("refs/heads/".into());
        }
        if tags_only {
            if !added_separator {
                args.push("--".into());
            }
            args.push("refs/tags/".into());
        }

        let output = self.output(&args).await?;
        if !output.status.success() {
            return Err(GitError::Message(format!(
                "git for-each-ref: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            )));
        }
        parse_refs(&output.stdout)
    }

    /// Baseline `revParse` used by `GetCommit`: resolve to an absolute revision
    /// or report `RevisionNotFound`.
    async fn rev_parse_commit(&self, spec: &str) -> Result<String, GitError> {
        let sha = self.rev_parse(spec).await?;
        if !is_absolute_revision(&sha) {
            if sha == "HEAD" {
                // Empty repository: `rev-parse` echoes the unresolved name.
                return Err(GitError::RevisionNotFound(spec.to_owned()));
            }
            return Err(GitError::Message(format!(
                "bad commit {sha:?} for revision {spec:?}"
            )));
        }
        Ok(sha)
    }

    /// Baseline `GetCommit`: `git log -n 1` with the NUL-separated format.
    pub async fn get_commit(
        &self,
        commit: &str,
        include_modified_files: bool,
    ) -> Result<GitCommit, GitError> {
        check_spec_arg_safety(commit)?;
        let commit_id = self.rev_parse_commit(commit).await?;

        let mut args: Vec<OsString> = vec![
            "log".into(),
            LOG_FORMAT_WITHOUT_REFS.into(),
            "-n".into(),
            "1".into(),
        ];
        if include_modified_files {
            args.push("--name-only".into());
        }
        args.push(commit_id.into());

        let output = self.output(&args).await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // `rev-parse` accepted the object but `log` cannot walk it (for
            // example a tree ID): the baseline reports this as a missing
            // revision.
            if output.status.code() == Some(128)
                && (stderr.contains("fatal: bad object") || stderr.contains("fatal: bad revision"))
            {
                return Err(GitError::RevisionNotFound(commit.to_owned()));
            }
            return Err(GitError::Message(format!(
                "git log {commit}: {}",
                stderr.trim_end()
            )));
        }
        parse_commit_log(&output.stdout)
    }

    /// Baseline `RevAtTime`: resolve the spec, then walk first parents for the
    /// newest commit whose committer time is before `before_seconds`. An empty
    /// result means no such ancestor exists.
    pub async fn rev_at_time(&self, spec: &str, before_seconds: i64) -> Result<String, GitError> {
        let sha = self.resolve_revision(spec).await?;
        let output = self
            .output(&[
                "log".into(),
                "--format=format:%H".into(),
                "--first-parent".into(),
                format!("--before={before_seconds}").into(),
                "--max-count=1".into(),
                sha.into(),
            ])
            .await?;
        if !output.status.success() {
            return Err(GitError::Message(format!(
                "git log: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    /// Baseline `RawDiff`: resolve both specs, linearize `BASE...HEAD` through
    /// `merge-base`, then stream `git diff-tree --patch`.
    pub async fn raw_diff(
        &self,
        base: &str,
        head: &str,
        comparison: RawDiffComparison,
        inter_hunk_context: u32,
        context_lines: u32,
        paths: &[Vec<u8>],
    ) -> Result<Child, GitError> {
        let mut base_oid = self.resolve_revision(base).await?;
        let head_oid = self.resolve_revision(head).await?;
        // Go re-checks the resolved OIDs before handing them to git.
        check_spec_arg_safety(&base_oid)?;
        check_spec_arg_safety(&head_oid)?;
        if comparison == RawDiffComparison::Intersection {
            base_oid = self.merge_base(&base_oid, &head_oid).await?;
        }

        let mut args: Vec<OsString> = vec![
            // The baseline uses `diff-tree` so that arbitrary on-disk files can
            // never be diffed.
            "diff-tree".into(),
            "--patch".into(),
            "--find-renames".into(),
            "--full-index".into(),
            format!("--inter-hunk-context={inter_hunk_context}").into(),
            format!("--unified={context_lines}").into(),
            "--no-prefix".into(),
            base_oid.into(),
            head_oid.into(),
            "--".into(),
        ];
        for path in paths {
            args.push(OsStr::from_bytes(path).to_owned());
        }
        self.command(&args)
            .spawn()
            .map_err(|err| GitError::Message(format!("failed to run git diff-tree: {err}")))
    }

    /// Baseline `CommitLog`: spawn `git log` with the shared NUL format; the
    /// caller parses records with [`pump_commit_log`].
    pub async fn commit_log(&self, options: &CommitLogOptions) -> Result<Child, GitError> {
        for range in &options.ranges {
            if range.first() == Some(&b'-') {
                return Err(GitError::Message(format!(
                    "invalid git revision spec {:?} (begins with '-')",
                    String::from_utf8_lossy(range)
                )));
            }
        }

        let mut args: Vec<OsString> = vec!["log".into(), LOG_FORMAT_WITHOUT_REFS.into()];
        if options.max_commits != 0 {
            args.push("-n".into());
            args.push(options.max_commits.to_string().into());
        }
        if options.skip != 0 {
            args.push(format!("--skip={}", options.skip).into());
        }
        if !options.author_query.is_empty() {
            args.push("--fixed-strings".into());
            let mut arg = OsString::from("--author=");
            arg.push(OsStr::from_bytes(&options.author_query));
            args.push(arg);
        }
        // See `CommitLogOptions`: unset timestamps arrive as the Unix epoch.
        args.push(format!("--after={}", rfc3339_utc(options.after_seconds)).into());
        args.push(format!("--before={}", rfc3339_utc(options.before_seconds)).into());
        match options.order {
            CommitLogOrder::CommitDate => args.push("--date-order".into()),
            CommitLogOrder::TopoDate => args.push("--topo-order".into()),
            CommitLogOrder::Default => {}
        }
        if !options.message_query.is_empty() {
            args.push("--fixed-strings".into());
            args.push("--regexp-ignore-case".into());
            let mut arg = OsString::from("--grep=");
            arg.push(OsStr::from_bytes(&options.message_query));
            args.push(arg);
        }
        if options.follow_only_first_parent {
            args.push("--first-parent".into());
        }
        if options.all_refs {
            args.push("--all".into());
        }
        if options.include_modified_files {
            args.push("--name-only".into());
        }
        if options.follow_path_renames {
            args.push("--follow".into());
        }
        for range in &options.ranges {
            args.push(OsStr::from_bytes(range).to_owned());
        }
        args.push("--".into());
        if !options.path.is_empty() {
            args.push(OsStr::from_bytes(&options.path).to_owned());
        }

        self.command(&args)
            .spawn()
            .map_err(|err| GitError::Message(format!("failed to run git log: {err}")))
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).ok())
        .collect()
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

/// NUL-separated `status\0path\0` pairs. `status` is the first byte (`A`, `M`,
/// `D`, `T`); the baseline rejects anything else.
fn parse_name_status(output: &[u8]) -> Result<Vec<(u8, Vec<u8>)>, GitError> {
    let mut tokens = output.split(|byte| *byte == 0);
    let mut files = Vec::new();
    loop {
        let status = match tokens.next() {
            Some(status) if !status.is_empty() => status,
            _ => break,
        };
        // A trailing NUL yields an empty final token; the baseline scanner
        // reports a missing path instead.
        let path = tokens
            .next()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| {
                GitError::Message("uneven pairs while parsing git diff output".into())
            })?;
        let code = status[0];
        if !matches!(code, b'A' | b'M' | b'D' | b'T') {
            return Err(GitError::Message(format!(
                "encountered unexpected file status {:?} for file {:?}",
                String::from_utf8_lossy(status),
                String::from_utf8_lossy(path)
            )));
        }
        files.push((code, path.to_vec()));
    }
    Ok(files)
}

/// Baseline `checkSpecArgSafety`: reject revisions that git would treat as an
/// option.
fn check_spec_arg_safety(spec: &str) -> Result<(), GitError> {
    if spec.starts_with('-') {
        return Err(GitError::Message(format!(
            "invalid git revision spec {spec:?} (begins with '-')"
        )));
    }
    Ok(())
}

/// Baseline `rel`: make an absolute slash path relative to the repository
/// root. A bare "/" becomes ".".
fn rel_bytes(path: &[u8]) -> Vec<u8> {
    if path == b"/" {
        return b".".to_vec();
    }
    path.strip_prefix(b"/").unwrap_or(path).to_vec()
}

/// One parsed `ls-tree --long -z` line before file-mode mapping.
struct RawTreeEntry {
    name: Vec<u8>,
    mode: i32,
    kind: String,
    sha: String,
    size: i64,
}

impl RawTreeEntry {
    fn into_tree_entry(self, modules: &GitModules) -> TreeEntry {
        let mut mode = self.mode as u32;
        let mut blob_oid = Some(self.sha.clone());
        let mut submodule = None;
        match self.kind.as_str() {
            "blob" => {
                if mode & GIT_MODE_SYMLINK != 0 {
                    mode = MODE_SYMLINK;
                } else {
                    mode |= 0o644;
                }
            }
            "commit" => {
                mode = MODE_SUBMODULE;
                blob_oid = None;
                submodule = Some(Submodule {
                    url: modules.option(&self.name, "url"),
                    path: modules.option(&self.name, "path").into_bytes(),
                    commit_sha: self.sha.clone(),
                });
            }
            "tree" => mode |= MODE_DIR,
            _ => {}
        }
        TreeEntry {
            name: self.name,
            size: self.size,
            mode,
            blob_oid,
            submodule,
        }
    }
}

/// Baseline `IsAbsoluteRevision`: exactly 40 hex characters.
fn is_absolute_revision(sha: &str) -> bool {
    sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Parse `git ls-tree --long -z` output. Each NUL-terminated record looks like
/// `<mode> <type> <sha> <size>\t<path>`; `-` means "no size" (tree or
/// submodule).
fn parse_ls_tree(output: &[u8]) -> Result<Vec<RawTreeEntry>, GitError> {
    let mut entries = Vec::new();
    for line in output.split(|byte| *byte == 0) {
        if line.is_empty() {
            continue;
        }
        let invalid_output =
            || GitError::Message(format!("invalid `git ls-tree` output: {line:?}"));
        let tab = line
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or_else(invalid_output)?;
        let info: Vec<&[u8]> = line[..tab].splitn(4, |byte| *byte == b' ').collect();
        if info.len() != 4 {
            return Err(invalid_output());
        }
        let name = line[tab + 1..].to_vec();
        let mode_text = std::str::from_utf8(info[0]).unwrap_or_default();
        let mode = i32::from_str_radix(mode_text, 8).map_err(|_| {
            GitError::Message(format!("invalid `git ls-tree` mode output: {mode_text:?}"))
        })?;
        let kind = String::from_utf8_lossy(info[1]).into_owned();
        let sha = String::from_utf8_lossy(info[2]).into_owned();
        if !is_absolute_revision(&sha) {
            return Err(GitError::Message(format!(
                "invalid `git ls-tree` SHA output: {sha:?}"
            )));
        }
        let size_text = String::from_utf8_lossy(info[3]).trim().to_owned();
        let size = if size_text == "-" {
            0
        } else {
            let size = size_text.parse::<i64>().map_err(|err| {
                GitError::Message(format!(
                    "invalid `git ls-tree` size output: {size_text:?} (error: {err})"
                ))
            })?;
            if size < 0 {
                return Err(GitError::Message(format!(
                    "invalid `git ls-tree` size output: {size_text:?} (error: negative)"
                )));
            }
            size
        };
        entries.push(RawTreeEntry {
            name,
            mode,
            kind,
            sha,
            size,
        });
    }
    Ok(entries)
}

/// The subset of `.gitmodules` needed to annotate submodule entries.
#[derive(Debug, Default)]
struct GitModules {
    /// subsection name (the submodule path) -> lowercased option key -> value
    sections: std::collections::HashMap<Vec<u8>, std::collections::HashMap<String, String>>,
}

impl GitModules {
    fn option(&self, name: &[u8], key: &str) -> String {
        self.sections
            .get(name)
            .and_then(|section| section.get(key))
            .cloned()
            .unwrap_or_default()
    }
}

/// Minimal git-config reader for `.gitmodules`: section headers, `key = value`
/// pairs, quoted values and `#`/`;` comments.
fn parse_gitmodules(text: &[u8]) -> GitModules {
    let mut modules = GitModules::default();
    let mut current: Option<Vec<u8>> = None;
    for raw_line in text.split(|byte| *byte == b'\n') {
        let line = trim_ascii(raw_line);
        if line.is_empty() || line[0] == b'#' || line[0] == b';' {
            continue;
        }
        if line[0] == b'[' {
            current = parse_submodule_header(line);
            if let Some(name) = &current {
                modules.sections.entry(name.clone()).or_default();
            }
            continue;
        }
        let Some(name) = &current else { continue };
        let Some(eq) = line.iter().position(|byte| *byte == b'=') else {
            continue;
        };
        let key = String::from_utf8_lossy(trim_ascii(&line[..eq]))
            .trim()
            .to_lowercase();
        let value = parse_config_value(&line[eq + 1..]);
        // Later assignments win, like git config.
        modules
            .sections
            .entry(name.clone())
            .or_default()
            .insert(key, value);
    }
    modules
}

/// `[submodule "name"]` (or the legacy `[submodule.name]`) -> `name`; other
/// sections are ignored.
fn parse_submodule_header(line: &[u8]) -> Option<Vec<u8>> {
    let inner = line.strip_prefix(b"[")?.strip_suffix(b"]")?;
    let inner = trim_ascii(inner);
    let end = inner
        .iter()
        .position(|byte| *byte == b'.' || byte.is_ascii_whitespace())
        .unwrap_or(inner.len());
    let section = String::from_utf8_lossy(&inner[..end]).into_owned();
    if !section.eq_ignore_ascii_case("submodule") {
        return None;
    }
    let rest = trim_ascii(&inner[end..]);
    let rest = match rest.strip_prefix(b".") {
        Some(rest) => trim_ascii(rest),
        None => rest,
    };
    let rest = if rest.first() == Some(&b'"') && rest.last() == Some(&b'"') && rest.len() >= 2 {
        &rest[1..rest.len() - 1]
    } else {
        rest
    };
    Some(rest.to_vec())
}

/// Parse a git-config value: optional quotes with backslash escapes, and a
/// trailing comment when `#`/`;` follows whitespace.
fn parse_config_value(raw: &[u8]) -> String {
    let raw = trim_ascii(raw);
    let mut value = Vec::new();
    let mut quoted = false;
    let mut index = 0;
    while index < raw.len() {
        let byte = raw[index];
        match byte {
            b'"' => {
                quoted = !quoted;
                index += 1;
            }
            b'\\' if quoted && index + 1 < raw.len() => {
                value.push(match raw[index + 1] {
                    b'n' => b'\n',
                    b't' => b'\t',
                    b'b' => 0x08,
                    other => other,
                });
                index += 2;
            }
            b'#' | b';'
                if !quoted
                    && (value.is_empty() || matches!(value.last(), Some(&b' ') | Some(&b'\t'))) =>
            {
                break;
            }
            _ => {
                value.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(trim_ascii(&value)).into_owned()
}

/// Baseline `refTypeForString`: annotated tags are `tag`; commits are tags when
/// the ref lives under `refs/tags/` (lightweight tags) and branches otherwise.
fn ref_type_for(object_type: &str, ref_name: &[u8]) -> RefType {
    match object_type {
        "tag" => RefType::Tag,
        "commit" => {
            if ref_name.starts_with(b"refs/tags/") {
                RefType::Tag
            } else {
                RefType::Branch
            }
        }
        _ => RefType::Unspecified,
    }
}

/// Parse newline-separated `for-each-ref` records of seven NUL-separated
/// fields: objecttype, HEAD marker, refname, short refname, objectname,
/// dereferenced objectname, creator date.
fn parse_refs(output: &[u8]) -> Result<Vec<GitRef>, GitError> {
    let mut refs = Vec::new();
    for line in output.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&[u8]> = line.split(|byte| *byte == 0).collect();
        if parts.len() != 7 {
            return Err(GitError::Message(format!(
                "unexpected output from git for-each-ref {:?}",
                String::from_utf8_lossy(line)
            )));
        }
        let object_type = String::from_utf8_lossy(parts[0]).into_owned();
        // Only tags dereference to a target object; for everything else the
        // target commit equals the ref's own object ID.
        let target_commit = if object_type == "tag" {
            String::from_utf8_lossy(parts[5]).into_owned()
        } else {
            String::from_utf8_lossy(parts[4]).into_owned()
        };
        let created_at_seconds = if parts[6].is_empty() {
            ZERO_TIMESTAMP_SECONDS
        } else {
            let text = String::from_utf8_lossy(parts[6]);
            text.parse::<i64>().map_err(|_| {
                GitError::Message(format!(
                    "unexpected output from git for-each-ref (bad date format) {:?}",
                    String::from_utf8_lossy(line)
                ))
            })?
        };
        refs.push(GitRef {
            ref_name: parts[2].to_vec(),
            short_ref_name: parts[3].to_vec(),
            target_commit,
            ref_oid: String::from_utf8_lossy(parts[4]).into_owned(),
            created_at_seconds,
            ref_type: ref_type_for(&object_type, parts[2]),
            is_head: parts[1] == b"*",
        });
    }
    Ok(refs)
}

/// Parse the single commit emitted by `LOG_FORMAT_WITHOUT_REFS`.
fn parse_commit_log(output: &[u8]) -> Result<GitCommit, GitError> {
    let output = output.strip_prefix(b"\x1e").unwrap_or(output);
    let parts: Vec<&[u8]> = output.split(|byte| *byte == 0).collect();
    if parts.len() != 10 {
        return Err(GitError::Message(format!(
            "internal error: expected 10 parts, got {}",
            parts.len()
        )));
    }
    // `git log` separates commits with a newline, which lands at the start of
    // the first field for every commit but the first.
    let oid = String::from_utf8_lossy(parts[0].strip_prefix(b"\n").unwrap_or(parts[0]))
        .trim()
        .to_owned();
    let parse_time = |bytes: &[u8], what: &str| -> Result<i64, GitError> {
        String::from_utf8_lossy(bytes)
            .parse::<i64>()
            .map_err(|err| GitError::Message(format!("parsing git commit {what} time: {err}")))
    };
    let parents = if parts[8].is_empty() {
        Vec::new()
    } else {
        parts[8]
            .split(|byte| *byte == b' ')
            .map(|parent| String::from_utf8_lossy(parent).into_owned())
            .collect()
    };
    let modified_files = {
        let trimmed = trim_ascii(parts[9]);
        if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split(|byte| *byte == b'\n')
                .map(<[u8]>::to_vec)
                .collect()
        }
    };
    let message = parts[7].strip_suffix(b"\n").unwrap_or(parts[7]);
    Ok(GitCommit {
        oid,
        author: Signature {
            name: parts[1].to_vec(),
            email: parts[2].to_vec(),
            date_seconds: parse_time(parts[3], "author")?,
        },
        committer: Signature {
            name: parts[4].to_vec(),
            email: parts[5].to_vec(),
            date_seconds: parse_time(parts[6], "committer")?,
        },
        message: message.to_vec(),
        parents,
        modified_files,
    })
}

/// Format Unix seconds as Go's `time.RFC3339` in UTC (seconds precision).
fn rfc3339_utc(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        day_seconds / 3600,
        (day_seconds % 3600) / 60,
        day_seconds % 60
    )
}

/// Howard Hinnant's `civil_from_days`: convert days since 1970-01-01 to a
/// proleptic Gregorian date.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { y + 1 } else { y }, month, day)
}

/// Stream the `\x1e`-separated records of `CommitLog`'s NUL format.
///
/// `send` resolves to false once the client went away; the child is killed
/// then. Baseline error mapping: exit 128 with a bad-revision marker becomes
/// `RevisionNotFound`; an empty repository ends the stream cleanly.
pub async fn pump_commit_log<F, Fut>(
    mut child: Child,
    spec: String,
    mut send: F,
) -> Result<(), GitError>
where
    F: FnMut(GitCommit) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| GitError::Message("git process has no stdout".to_owned()))?;
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; STREAM_CHUNK];
    let mut wrote_any = false;
    loop {
        let read = match stdout.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => read,
            Err(err) => {
                let _ = child.start_kill();
                return Err(GitError::Message(format!(
                    "failed to read git output: {err}"
                )));
            }
        };
        wrote_any = true;
        buffer.extend_from_slice(&chunk[..read]);
        // The leading `\x1e` opens a record; the next one closes it.
        while buffer.len() > 1 {
            let Some(offset) = buffer[1..].iter().position(|byte| *byte == 0x1e) else {
                break;
            };
            // Keep the next record's leading separator: it opens the next
            // token. `drain(..=end)` would swallow the first byte of the next
            // commit ID.
            let end = offset + 1;
            let record = buffer[1..end].to_vec();
            buffer.drain(..end);
            let commit = parse_commit_log(&record)?;
            if !send(commit).await {
                let _ = child.start_kill();
                return Ok(());
            }
        }
    }
    if buffer.len() > 1 {
        let record = buffer[1..].to_vec();
        let commit = parse_commit_log(&record)?;
        if !send(commit).await {
            let _ = child.start_kill();
            return Ok(());
        }
    }

    let status = child
        .wait()
        .await
        .map_err(|err| GitError::Message(format!("failed to wait for git: {err}")))?;
    if status.success() {
        return Ok(());
    }
    let mut stderr = String::new();
    if let Some(mut err) = child.stderr.take() {
        let _ = err.read_to_string(&mut stderr).await;
    }
    if status.code() == Some(128) {
        // A repository without commits is an empty result, not an error.
        if stderr.contains("fatal: bad revision 'HEAD'")
            || (stderr.contains("fatal: your current branch")
                && stderr.contains("does not have any commits yet"))
        {
            return Ok(());
        }
        let markers = [
            "not a tree object",
            "fatal: bad object",
            "fatal: Invalid revision range",
            "fatal: bad revision",
        ];
        if markers.iter().any(|marker| stderr.contains(marker))
            || (stderr.contains("fatal: ambiguous argument")
                && stderr.contains("unknown revision or path not in the working tree."))
        {
            return Err(GitError::RevisionNotFound(spec));
        }
    }
    if !wrote_any || !stderr.is_empty() {
        return Err(GitError::Message(format!(
            "git exited with {}: {}",
            status
                .code()
                .map_or("signal".to_owned(), |code| code.to_string()),
            stderr.trim_end()
        )));
    }
    Ok(())
}

/// Read a child's stdout into `send`, chunked at `STREAM_CHUNK`.
///
/// `send` resolves to false once the client went away; the child is killed
/// then. Errors from the child are returned only when no data was produced.
pub async fn pump_stdout<F, Fut>(mut child: Child, mut send: F) -> Result<(), String>
where
    F: FnMut(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| "git process has no stdout".to_owned())?;
    let mut buffer = vec![0u8; STREAM_CHUNK];
    let mut wrote_any = false;
    loop {
        let read = match stdout.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => read,
            Err(err) => {
                let _ = child.start_kill();
                return Err(format!("failed to read git output: {err}"));
            }
        };
        wrote_any = true;
        if !send(buffer[..read].to_vec()).await {
            let _ = child.start_kill();
            return Ok(());
        }
    }
    let status = child
        .wait()
        .await
        .map_err(|err| format!("failed to wait for git: {err}"))?;
    if !status.success() {
        let mut stderr = String::new();
        if let Some(mut err) = child.stderr.take() {
            let _ = err.read_to_string(&mut stderr).await;
        }
        if !wrote_any || !stderr.is_empty() {
            return Err(format!(
                "git exited with {}: {}",
                status
                    .code()
                    .map_or("signal".to_owned(), |code| code.to_string()),
                stderr.trim_end()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nul_separated_name_status() {
        let files = parse_name_status(b"A\0a.txt\0M\0b.txt\0D\0c.txt\0").expect("parse");
        assert_eq!(
            files,
            vec![
                (b'A', b"a.txt".to_vec()),
                (b'M', b"b.txt".to_vec()),
                (b'D', b"c.txt".to_vec()),
            ]
        );
    }

    #[test]
    fn rejects_unknown_statuses_and_uneven_pairs() {
        assert!(parse_name_status(b"R\0a.txt\0").is_err());
        assert!(parse_name_status(b"A\0").is_err());
    }

    #[test]
    fn trims_ascii_whitespace() {
        assert_eq!(trim_ascii(b"  abc\n"), b"abc");
        assert_eq!(trim_ascii(b"   "), b"");
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn git(dir: &std::path::Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }

    fn fixture_repo(root: &std::path::Path) -> (std::path::PathBuf, String) {
        let dir = root.join("repo.git");
        std::fs::create_dir_all(&dir).expect("mkdir");
        git(&dir, &["init", "--bare", "-q"]);
        // Populate through a temporary worktree so commits have a tree.
        let work = root.join("work");
        git(root, &["init", "-q", work.to_str().expect("utf8")]);
        std::fs::write(work.join("a.txt"), b"hello\n").expect("write");
        git(&work, &["add", "a.txt"]);
        git(&work, &["commit", "-q", "-m", "first"]);
        let head = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&work)
            .output()
            .expect("rev-parse");
        let head = String::from_utf8(head.stdout)
            .expect("utf8")
            .trim()
            .to_owned();
        git(
            &work,
            &[
                "push",
                "-q",
                dir.to_str().expect("utf8"),
                "HEAD:refs/heads/main",
            ],
        );
        // gitserver sets HEAD after cloning; a bare `git init` leaves it on
        // the default branch.
        git(&dir, &["symbolic-ref", "HEAD", "refs/heads/main"]);
        (dir, head)
    }

    #[tokio::test]
    async fn reads_files_and_lists_initial_commits() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (dir, head) = fixture_repo(tmp.path());
        let repo = GitRepo::new(dir);

        let mut child = repo
            .read_file(&head, b"a.txt")
            .await
            .expect("read_file")
            .expect("blob");
        let mut data = Vec::new();
        child
            .stdout
            .take()
            .expect("stdout")
            .read_to_end(&mut data)
            .await
            .expect("read");
        assert_eq!(data, b"hello\n");

        let files = repo
            .changed_files(None, head.as_bytes())
            .await
            .expect("diff");
        assert_eq!(files, vec![(b'A', b"a.txt".to_vec())]);

        // Base resolution failures are `RevisionNotFound` like head failures.
        assert!(matches!(
            repo.changed_files(Some(b"deadbeef"), head.as_bytes()).await,
            Err(GitError::RevisionNotFound(_))
        ));

        // Unknown revision and missing file are distinguishable.
        assert!(matches!(
            repo.read_file("deadbeef", b"a.txt").await,
            Err(GitError::RevisionNotFound(_))
        ));
        assert!(matches!(
            repo.read_file(&head, b"missing.txt").await,
            Err(GitError::FileNotFound)
        ));
    }

    #[tokio::test]
    async fn resolves_revisions_heads_and_merge_bases() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (dir, head) = fixture_repo(tmp.path());
        let repo = GitRepo::new(dir);

        // Empty spec defaults to HEAD; branches go through `^0`.
        assert_eq!(repo.resolve_revision("").await.expect("HEAD"), head);
        assert_eq!(repo.resolve_revision("main").await.expect("main"), head);
        assert!(matches!(
            repo.resolve_revision("deadbeef").await,
            Err(GitError::RevisionNotFound(_))
        ));

        let (oid, object_type) = repo.get_object("HEAD").await.expect("object");
        assert_eq!(object_type, "commit");
        assert_eq!(oid.len(), 20);
        assert_eq!(hex(&oid), head);

        assert_eq!(
            repo.symbolic_ref_head(false).await.expect("ref"),
            "refs/heads/main"
        );
        assert_eq!(repo.symbolic_ref_head(true).await.expect("short"), "main");
        assert_eq!(repo.rev_parse_head().await.expect("head"), head);

        assert_eq!(
            repo.merge_base(&head, &head).await.expect("merge-base"),
            head
        );
        assert!(matches!(
            repo.merge_base("deadbeef", &head).await,
            Err(GitError::RevisionNotFound(_))
        ));
    }

    #[test]
    fn maps_ls_tree_modes_like_the_baseline() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let output = format!(
            "040000 tree {sha}       -\tdir\0\
             100644 blob {sha}       5\ta.txt\0\
             120000 blob {sha}       5\tlink\0\
             160000 commit {sha}       -\tvendor/lib\0"
        );
        let modules = parse_gitmodules(
            b"[submodule \"vendor/lib\"]\n\turl = https://example.com/lib.git\n\tpath = vendor/lib\n",
        );
        let entries: Vec<TreeEntry> = parse_ls_tree(output.as_bytes())
            .expect("parse")
            .into_iter()
            .map(|raw| raw.into_tree_entry(&modules))
            .collect();

        assert_eq!(entries[0].mode, 0o40000 | MODE_DIR);
        assert_eq!(entries[0].blob_oid.as_deref(), Some(sha));
        assert_eq!(entries[0].size, 0);

        assert_eq!(entries[1].mode, 0o100644);
        assert_eq!(entries[1].size, 5);
        assert_eq!(entries[1].name, b"a.txt");

        assert_eq!(entries[2].mode, MODE_SYMLINK);

        assert_eq!(entries[3].mode, MODE_SUBMODULE);
        assert_eq!(entries[3].blob_oid, None);
        let submodule = entries[3].submodule.as_ref().expect("submodule");
        assert_eq!(submodule.url, "https://example.com/lib.git");
        assert_eq!(submodule.path, b"vendor/lib");
        assert_eq!(submodule.commit_sha, sha);
    }

    #[test]
    fn parses_gitmodules_sections() {
        let modules = parse_gitmodules(
            b"# comment\n[core]\n\tbare = true\n\
              [submodule \"a\"]\nurl = \"https://example.com/a.git\" ; comment\n\
              [Submodule \"b\"]\nURL = https://example.com/b.git#frag\n",
        );
        assert_eq!(modules.option(b"a", "url"), "https://example.com/a.git");
        assert_eq!(
            modules.option(b"b", "url"),
            "https://example.com/b.git#frag"
        );
        assert_eq!(modules.option(b"a", "path"), "");
    }

    #[tokio::test]
    async fn stats_and_lists_tree_entries() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (dir, head, gitlink) = tree_fixture_repo(tmp.path());
        let repo = GitRepo::new(dir);

        // The root is not listed by `ls-tree`; Stat resolves `<commit>^{tree}`.
        let root = repo.stat(&head, b"").await.expect("root stat");
        assert!(root.name.is_empty());
        assert_eq!(root.mode, MODE_DIR);
        assert_eq!(root.size, 0);
        assert_eq!(root.blob_oid.as_deref().map(str::len), Some(40));
        assert_eq!(repo.stat(&head, b"/").await.expect("slash").mode, MODE_DIR);

        let file = repo.stat(&head, b"a.txt").await.expect("file stat");
        assert_eq!(file.name, b"a.txt");
        assert_eq!(file.size, 6);
        assert_eq!(file.mode, 0o100644);

        let dir_entry = repo.stat(&head, b"dir").await.expect("dir stat");
        assert_eq!(dir_entry.mode, 0o40000 | MODE_DIR);

        let link = repo.stat(&head, b"link").await.expect("symlink stat");
        assert_eq!(link.mode, MODE_SYMLINK);

        let submodule = repo.stat(&head, b"vendor/lib").await.expect("submodule");
        assert_eq!(submodule.mode, MODE_SUBMODULE);
        let submodule = submodule.submodule.expect("submodule metadata");
        assert_eq!(submodule.url, "https://example.com/lib.git");
        assert_eq!(submodule.path, b"vendor/lib");
        assert_eq!(submodule.commit_sha, gitlink);

        assert!(matches!(
            repo.stat(&head, b"missing").await,
            Err(GitError::FileNotFound)
        ));
        assert!(matches!(
            repo.stat("deadbeef", b"a.txt").await,
            Err(GitError::RevisionNotFound(_))
        ));
        // A missing revision at the root reads as a missing file.
        assert!(matches!(
            repo.stat("deadbeef", b"").await,
            Err(GitError::FileNotFound)
        ));

        let root_entries = repo.read_dir(&head, b"", false).await.expect("root");
        assert!(root_entries.iter().any(|entry| entry.name == b"dir"));
        assert!(root_entries
            .iter()
            .any(|entry| entry.name == b".gitmodules"));

        let dir_entries = repo.read_dir(&head, b"dir", false).await.expect("dir");
        assert_eq!(dir_entries.len(), 1);
        assert_eq!(dir_entries[0].name, b"dir/b.txt");

        let all = repo.read_dir(&head, b"", true).await.expect("recursive");
        assert!(all.iter().any(|entry| entry.name == b"dir/b.txt"));
        assert!(all
            .iter()
            .any(|entry| entry.name == b"vendor" && entry.mode == 0o40000 | MODE_DIR));

        assert_eq!(
            repo.read_dir(&head, b"/", false)
                .await
                .expect("slash root")
                .len(),
            root_entries.len()
        );
        assert!(matches!(
            repo.read_dir(&head, b"nope", false).await,
            Err(GitError::FileNotFound)
        ));
        assert!(matches!(
            repo.read_dir("deadbeef", b"", false).await,
            Err(GitError::RevisionNotFound(_))
        ));
    }

    /// Bare repo with a subdirectory, a symlink, a `.gitmodules` and a gitlink.
    fn tree_fixture_repo(root: &std::path::Path) -> (std::path::PathBuf, String, String) {
        let dir = root.join("tree.git");
        std::fs::create_dir_all(&dir).expect("mkdir");
        git(&dir, &["init", "--bare", "-q"]);
        let work = root.join("tree-work");
        git(root, &["init", "-q", work.to_str().expect("utf8")]);
        std::fs::write(work.join("a.txt"), b"hello\n").expect("write");
        std::fs::create_dir_all(work.join("dir")).expect("mkdir");
        std::fs::write(work.join("dir/b.txt"), b"world\n").expect("write");
        std::fs::write(
            work.join(".gitmodules"),
            b"[submodule \"vendor/lib\"]\n\turl = https://example.com/lib.git\n\tpath = vendor/lib\n",
        )
        .expect("write");
        std::os::unix::fs::symlink("a.txt", work.join("link")).expect("symlink");
        git(&work, &["add", "-A"]);
        git(&work, &["commit", "-q", "-m", "first"]);
        let head = rev_parse(&work, "HEAD");
        // A gitlink can point at any commit; reuse the first one.
        let cacheinfo = format!("160000,{head},vendor/lib");
        git(&work, &["update-index", "--add", "--cacheinfo", &cacheinfo]);
        git(&work, &["commit", "-q", "-m", "submodule"]);
        let commit = rev_parse(&work, "HEAD");
        git(
            &work,
            &[
                "push",
                "-q",
                dir.to_str().expect("utf8"),
                "HEAD:refs/heads/main",
            ],
        );
        git(&dir, &["symbolic-ref", "HEAD", "refs/heads/main"]);
        (dir, commit, head)
    }

    fn rev_parse(dir: &std::path::Path, spec: &str) -> String {
        let output = std::process::Command::new("git")
            .args(["rev-parse", spec])
            .current_dir(dir)
            .output()
            .expect("rev-parse");
        assert!(output.status.success(), "git rev-parse {spec} failed");
        String::from_utf8(output.stdout)
            .expect("utf8")
            .trim()
            .to_owned()
    }

    #[tokio::test]
    async fn archives_the_requested_paths() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (dir, head) = fixture_repo(tmp.path());
        let repo = GitRepo::new(dir);

        let mut child = repo
            .archive("tar", &head, &[b"a.txt".to_vec()])
            .await
            .expect("archive");
        let mut data = Vec::new();
        child
            .stdout
            .take()
            .expect("stdout")
            .read_to_end(&mut data)
            .await
            .expect("read");
        assert!(child.wait().await.expect("wait").success());
        assert!(data.len() > 512);
        assert!(data.windows(5).any(|window| window == b"a.txt"));

        assert!(matches!(
            repo.archive("tar", "deadbeef", &[]).await,
            Err(GitError::RevisionNotFound(_))
        ));
    }

    #[test]
    fn parses_annotated_tag_refs() {
        let output: Vec<u8> = [
            &b"tag\0*\0refs/tags/v1\0v1\0tagoid\0commitoid\0"[..],
            &b"1709216000\n"[..],
            &b"commit\0 \0refs/heads/main\0main\0commitoid\0\0\n"[..],
        ]
        .concat();
        let refs = parse_refs(&output).expect("parse refs");
        assert_eq!(refs.len(), 2);

        // Annotated tags dereference `*objectname` for the target commit and
        // keep the tag object ID as `ref_oid`.
        assert_eq!(refs[0].ref_name, b"refs/tags/v1");
        assert_eq!(refs[0].target_commit, "commitoid");
        assert_eq!(refs[0].ref_oid, "tagoid");
        assert_eq!(refs[0].ref_type, RefType::Tag);
        assert_eq!(refs[0].created_at_seconds, 1_709_216_000);
        assert!(refs[0].is_head);

        // A commit under refs/tags is still a tag. Empty dates map to Go's
        // zero time instead of failing.
        assert_eq!(refs[1].ref_type, RefType::Branch);
        assert_eq!(refs[1].target_commit, "commitoid");
        assert_eq!(refs[1].created_at_seconds, ZERO_TIMESTAMP_SECONDS);
        assert!(!refs[1].is_head);
    }

    #[tokio::test]
    async fn lists_refs_and_reads_commits() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (dir, head) = fixture_repo(tmp.path());
        let repo = GitRepo::new(dir);

        let refs = repo
            .list_refs(false, false, &[], None)
            .await
            .expect("list refs");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].ref_name, b"refs/heads/main");
        assert_eq!(refs[0].short_ref_name, b"main");
        assert_eq!(refs[0].target_commit, head);
        assert_eq!(refs[0].ref_oid, head);
        assert_eq!(refs[0].ref_type, RefType::Branch);
        assert!(refs[0].is_head);
        assert!(refs[0].created_at_seconds > 0);

        assert_eq!(
            repo.list_refs(true, false, &[], None)
                .await
                .expect("heads only")
                .len(),
            1
        );
        assert!(repo
            .list_refs(false, true, &[], None)
            .await
            .expect("tags only")
            .is_empty());
        assert_eq!(
            repo.list_refs(false, false, std::slice::from_ref(&head), None)
                .await
                .expect("points at")
                .len(),
            1
        );
        assert!(repo
            .list_refs(
                false,
                false,
                &["deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_owned()],
                None,
            )
            .await
            .expect("no ref points at")
            .is_empty());

        let commit = repo.get_commit(&head, false).await.expect("commit");
        assert_eq!(commit.oid, head);
        assert_eq!(commit.message, b"first");
        assert!(commit.parents.is_empty());
        assert!(commit.modified_files.is_empty());
        assert_eq!(commit.author.name, b"test");
        assert_eq!(commit.author.email, b"test@example.com");

        let commit = repo.get_commit(&head, true).await.expect("commit files");
        assert_eq!(commit.modified_files, vec![b"a.txt".to_vec()]);

        assert!(matches!(
            repo.get_commit("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", false)
                .await,
            Err(GitError::RevisionNotFound(_))
        ));
    }

    #[test]
    fn formats_rfc3339_like_go() {
        // Go formats `time.Unix(sec, 0).UTC()` with `time.RFC3339`.
        assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_utc(1_706_832_000), "2024-02-02T00:00:00Z");
        assert_eq!(rfc3339_utc(1_709_251_200), "2024-03-01T00:00:00Z");
        // Leap day and a negative epoch (1969).
        assert_eq!(rfc3339_utc(1_709_164_800), "2024-02-29T00:00:00Z");
        assert_eq!(rfc3339_utc(-86_400), "1969-12-31T00:00:00Z");
        assert_eq!(rfc3339_utc(-1), "1969-12-31T23:59:59Z");
    }

    fn now_seconds() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64
    }

    #[tokio::test]
    async fn resolves_revisions_at_time() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (dir, head) = fixture_repo(tmp.path());
        let repo = GitRepo::new(dir);

        let date = repo
            .get_commit(&head, false)
            .await
            .expect("commit")
            .committer
            .date_seconds;
        assert_eq!(
            repo.rev_at_time("main", date + 1).await.expect("after"),
            head
        );
        // Strictly before the root commit: no ancestor matches.
        assert_eq!(
            repo.rev_at_time("main", date - 1).await.expect("before"),
            ""
        );
        assert!(matches!(
            repo.rev_at_time("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", date + 1)
                .await,
            Err(GitError::RevisionNotFound(_))
        ));
    }

    #[tokio::test]
    async fn streams_commit_log_records() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (dir, head) = fixture_repo(tmp.path());
        let repo = GitRepo::new(dir);
        let before = now_seconds() + 86_400;
        let options = |ranges: Vec<Vec<u8>>| CommitLogOptions {
            ranges,
            all_refs: false,
            after_seconds: 0,
            before_seconds: before,
            max_commits: 0,
            skip: 0,
            follow_only_first_parent: false,
            include_modified_files: true,
            order: CommitLogOrder::Default,
            message_query: Vec::new(),
            author_query: Vec::new(),
            path: Vec::new(),
            follow_path_renames: false,
        };

        let child = repo
            .commit_log(&options(vec![b"main".to_vec()]))
            .await
            .expect("commit log");
        let mut commits = Vec::new();
        pump_commit_log(child, "main".to_owned(), |commit| {
            commits.push(commit);
            async { true }
        })
        .await
        .expect("pump");
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].oid, head);
        assert_eq!(commits[0].message, b"first");
        assert_eq!(commits[0].modified_files, vec![b"a.txt".to_vec()]);

        let child = repo
            .commit_log(&options(vec![
                b"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_vec()
            ]))
            .await
            .expect("commit log");
        assert!(matches!(
            pump_commit_log(child, "deadbeef".to_owned(), |_commit| async { true }).await,
            Err(GitError::RevisionNotFound(_))
        ));
    }
}
