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
            .output(&[
                "cat-file".into(),
                "-t".into(),
                "--".into(),
                spec.into(),
            ])
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
            let oid = self
                .rev_parse(&String::from_utf8_lossy(base))
                .await?;
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
                status.code().map_or("signal".to_owned(), |code| code.to_string()),
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
        let head = String::from_utf8(head.stdout).expect("utf8").trim().to_owned();
        git(
            &work,
            &["push", "-q", dir.to_str().expect("utf8"), "HEAD:refs/heads/main"],
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

        let files = repo.changed_files(None, head.as_bytes()).await.expect("diff");
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

        assert_eq!(repo.merge_base(&head, &head).await.expect("merge-base"), head);
        assert!(matches!(
            repo.merge_base("deadbeef", &head).await,
            Err(GitError::RevisionNotFound(_))
        ));
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
}
