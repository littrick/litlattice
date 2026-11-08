//! Git-level clone/fetch primitives from
//! `cmd/gitserver/internal/vcssyncer/git.go` and the helpers it uses.
//!
//! The baseline does not run `git clone`: it initializes a bare repository and
//! fetches the configured refspecs into it, then points `HEAD` at the remote's
//! default branch. Only the git syncer is covered here; the HTTP/SSH remote
//! URL lookup, custom fetch commands (site configuration), TLS settings, and
//! credential redaction still come from the site/DB layer that is not ported.
//! Remote URLs with inline credentials are therefore passed to git as-is; the
//! baseline moves them into a credential helper instead.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::OnceLock;

use regex::Regex;
use tokio::process::Command;

use crate::git::GitError;

/// The refspecs `runFetchCommand` uses when `SRC_GITSERVER_REFSPECS` is unset.
const DEFAULT_REFSPECS: [&str; 7] = [
    "+refs/heads/*:refs/heads/*",
    "+refs/tags/*:refs/tags/*",
    // GitHub pull requests
    "+refs/pull/*:refs/pull/*",
    // GitLab merge requests
    "+refs/merge-requests/*:refs/merge-requests/*",
    // Bitbucket pull requests
    "+refs/pull-requests/*:refs/pull-requests/*",
    // Gerrit changesets
    "+refs/changes/*:refs/changes/*",
    // Possibly deprecated refs for the sourcegraph zap experiment.
    "+refs/sourcegraph/*:refs/sourcegraph/*",
];

/// Port of `EnsureHEAD`: make sure `HEAD` exists and is non-empty, otherwise
/// point it at `refs/heads/master`. The file is written mode 0600 and has no
/// trailing newline, exactly like `os.WriteFile` in the baseline.
pub fn ensure_head(git_dir: &Path) -> std::io::Result<()> {
    let path = git_dir.join("HEAD");
    match std::fs::metadata(&path) {
        Ok(metadata) if metadata.len() > 0 => Ok(()),
        Ok(_) => write_head(&path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => write_head(&path),
        Err(err) => Err(err),
    }
}

fn write_head(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(b"ref: refs/heads/master")
}

/// Port of `MakeBareRepo`: `git init --bare .` in `git_dir`.
pub async fn make_bare_repo(git_dir: &Path) -> Result<(), GitError> {
    let output = Command::new("git")
        .args(["init", "--bare", "."])
        .current_dir(git_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|err| GitError::Message(format!("failed to create bare repo: {err}")))?;
    if !output.status.success() {
        return Err(GitError::Message(format!(
            "failed to create bare repo: {}",
            String::from_utf8_lossy(&output.stdout)
        )));
    }
    Ok(())
}

fn refspec_overrides() -> &'static [String] {
    static OVERRIDES: OnceLock<Vec<String>> = OnceLock::new();
    OVERRIDES.get_or_init(|| {
        std::env::var("SRC_GITSERVER_REFSPECS")
            .map(|value| value.split_whitespace().map(str::to_owned).collect())
            .unwrap_or_default()
    })
}

/// `SRC_GITSERVER_REFSPECS` when set, otherwise the baseline's default list.
pub fn fetch_refspecs() -> Vec<String> {
    let overrides = refspec_overrides();
    if overrides.is_empty() {
        DEFAULT_REFSPECS.iter().map(|s| s.to_string()).collect()
    } else {
        overrides.to_vec()
    }
}

/// Port of `executil.ConfigureRemoteGitCommand`'s environment, minus the TLS
/// and URL-credential handling that needs site configuration. The baseline
/// also injects `-c credential.helper=` and `-c protocol.version=2` before
/// the subcommand; callers build those explicitly.
fn apply_remote_env(command: &mut Command, _remote_url: &str) {
    // Disable password prompts; the command is non-interactive.
    command.env("GIT_ASKPASS", "true");
    // Suppress SSH host-key prompts and bound the connect time.
    command.env(
        "GIT_SSH_COMMAND",
        "ssh -o BatchMode=yes -o ConnectTimeout=30",
    );
    // Keep the `git/` prefix: GitHub breaks protocol v2 negotiation without it.
    command.env("GIT_HTTP_USER_AGENT", "git/Sourcegraph-Bot");
}

/// One `git fetch` execution: exit status plus the merged output (stdout then
/// stderr; the baseline interleaves both through a synchronized writer, which
/// only matters for progress display).
pub struct FetchOutcome {
    pub exit_status: i32,
    pub output: Vec<u8>,
}

/// Port of `runFetchCommand` for the default command shape: `git -c
/// credential.helper= -c protocol.version=2 fetch --progress --prune <url>
/// <refspecs...>` with `GIT_DIR` set and `GIT_LFS_SKIP_SMUDGE=1` (see issue
/// #7322: skip LFS content in repositories with Git LFS configured).
pub async fn fetch_remote(git_dir: &Path, remote_url: &str) -> Result<FetchOutcome, GitError> {
    fetch_remote_with(git_dir, remote_url, &mut |_| {}).await
}

/// [`fetch_remote`] while reporting progress lines, like the baseline
/// `readFetchProgress`: every `\r`/`\n`-terminated, whitespace-trimmed,
/// non-empty line of the command's output is passed to `on_progress`.
///
/// git writes its `--progress` output to stderr; stdout is only read into the
/// merged output. Both are drained concurrently so a full pipe cannot stall
/// the child.
pub async fn fetch_remote_with(
    git_dir: &Path,
    remote_url: &str,
    on_progress: &mut (dyn FnMut(&str) + Send),
) -> Result<FetchOutcome, GitError> {
    let mut command = Command::new("git");
    command
        .arg("-c")
        .arg("credential.helper=")
        // Use protocol v2 for everything except `ls-remote`, like the baseline.
        .arg("-c")
        .arg("protocol.version=2")
        .arg("fetch")
        .arg("--progress")
        .arg("--prune")
        .arg(remote_url);
    for refspec in fetch_refspecs() {
        command.arg(refspec);
    }
    command
        .current_dir(git_dir)
        .env("GIT_DIR", git_dir)
        .env("GIT_LFS_SKIP_SMUDGE", "1");
    apply_remote_env(&mut command, remote_url);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|err| GitError::Message(format!("failed to start command: {err}")))?;
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");

    let read_stdout = async {
        let mut buffer = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stdout, &mut buffer)
            .await
            .map(|_| buffer)
    };
    let read_stderr = async {
        let mut buffer = Vec::new();
        let mut scanner = LineScanner::default();
        let mut chunk = [0u8; 4096];
        loop {
            let read = tokio::io::AsyncReadExt::read(&mut stderr, &mut chunk).await?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            scanner.push(&chunk[..read], on_progress);
        }
        scanner.finish(on_progress);
        Ok::<Vec<u8>, std::io::Error>(buffer)
    };

    let (stdout_buffer, stderr_buffer) = tokio::join!(read_stdout, read_stderr);
    let stdout_buffer = stdout_buffer
        .map_err(|err| GitError::Message(format!("failed to read fetch output: {err}")))?;
    let stderr_buffer = stderr_buffer
        .map_err(|err| GitError::Message(format!("failed to read fetch output: {err}")))?;

    let status = child
        .wait()
        .await
        .map_err(|err| GitError::Message(format!("failed to wait for git: {err}")))?;

    // Keep the baseline's ordering: read stdout in full first, then stderr.
    let mut merged = stdout_buffer;
    merged.extend_from_slice(&stderr_buffer);
    Ok(FetchOutcome {
        exit_status: status.code().unwrap_or(-1),
        output: merged,
    })
}

/// Splits a byte stream on `\r` and `\n`, emitting whitespace-trimmed,
/// non-empty tokens (`bufio.Scanner` with the baseline's `scanCRLF`).
#[derive(Default)]
struct LineScanner {
    buffer: Vec<u8>,
}

impl LineScanner {
    fn push(&mut self, data: &[u8], on_line: &mut (dyn FnMut(&str) + Send)) {
        self.buffer.extend_from_slice(data);
        let mut consumed = 0;
        for (index, byte) in self.buffer.iter().enumerate() {
            if *byte == b'\r' || *byte == b'\n' {
                let token = String::from_utf8_lossy(&self.buffer[consumed..index]);
                let token = token.trim();
                if !token.is_empty() {
                    on_line(token);
                }
                consumed = index + 1;
            }
        }
        if consumed > 0 {
            self.buffer.drain(..consumed);
        }
    }

    /// The final, unterminated line at EOF.
    fn finish(&mut self, on_line: &mut (dyn FnMut(&str) + Send)) {
        let token = String::from_utf8_lossy(&self.buffer);
        let token = token.trim();
        if !token.is_empty() {
            on_line(token);
        }
        self.buffer.clear();
    }
}

/// Port of `setHEAD`: pick the remote's default branch, falling back to
/// `master`, then to the first local branch, and point `HEAD` at it.
pub async fn set_head(git_dir: &Path, remote_url: &str) -> Result<(), GitError> {
    // `EnsureHEAD` failures are logged, not fatal, like the baseline.
    if let Err(err) = ensure_head(git_dir) {
        tracing::error!(
            error = %err,
            dir = %git_dir.display(),
            "failed to ensure HEAD exists"
        );
    }

    // Fall back to git's default branch name if `git remote show` fails.
    let mut head_branch = "master".to_owned();

    // `git remote show` in the baseline runs with `dir.Set` applied before
    // `ConfigureRemoteGitCommand`, so it inherits neither the parent
    // environment nor (for local remotes) needs it. Mirror that: only
    // `GIT_DIR` plus the remote settings are exported.
    let output = run_bare(git_dir, &["remote", "show", remote_url], true).await?;
    if !output.status.success() {
        return Err(GitError::Message(format!(
            "failed to fetch remote info: {}",
            String::from_utf8_lossy(&output.stderr).trim_end()
        )));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    if let Some(matched) = head_branch_pattern().captures(&stdout) {
        if let Some(branch) = matched.get(1) {
            let branch = branch.as_str();
            if branch != "(unknown)" && !branch.is_empty() {
                head_branch = branch.to_owned();
            }
        }
    }

    // If the branch pointed to by HEAD does not exist, pick the first branch.
    let check = run_bare(git_dir, &["rev-parse", &head_branch, "--"], false).await?;
    if !check.status.success() {
        let branches = run_bare(git_dir, &["branch"], false).await?;
        if !branches.status.success() {
            return Err(GitError::Message(format!(
                "failed to list branches: {}",
                String::from_utf8_lossy(&branches.stderr).trim_end()
            )));
        }
        let stdout = String::from_utf8_lossy(&branches.stdout);
        let first = stdout.lines().next().unwrap_or_default();
        let branch = first
            .strip_prefix("* ")
            .or_else(|| first.strip_prefix("  "))
            .unwrap_or(first);
        if !branch.is_empty() {
            head_branch = branch.to_owned();
        }
    }

    let target = format!("refs/heads/{head_branch}");
    let output = run_bare(git_dir, &["symbolic-ref", "HEAD", &target], false).await?;
    if !output.status.success() {
        return Err(GitError::Message(format!(
            "Failed to set HEAD: {}",
            String::from_utf8_lossy(&output.stderr).trim_end()
        )));
    }
    Ok(())
}

fn head_branch_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    // Baseline `headBranchPattern = lazyregexp.New("HEAD branch: (.+?)\\n")`.
    PATTERN.get_or_init(|| Regex::new(r"HEAD branch: (.+?)\n").expect("valid regex"))
}

/// `git` in `git_dir` with `GIT_DIR` set and a cleared environment, matching
/// `common.GitDir.Set` followed by an optional `ConfigureRemoteGitCommand`
/// (which sees a non-nil `cmd.Env` and therefore does not inherit the
/// parent process environment).
async fn run_bare(git_dir: &Path, args: &[&str], remote: bool) -> Result<Output, GitError> {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(git_dir)
        .env_clear()
        .env("GIT_DIR", git_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if remote {
        apply_remote_env(&mut command, "");
    }
    command
        .output()
        .await
        .map_err(|err| GitError::Message(format!("failed to run git: {err}")))
}

/// Port of `gitRepoSyncer.Clone`: `git init --bare`, fetch all refspecs from
/// the remote into the temporary directory, then set `HEAD`. Progress lines
/// are appended to `progress`; write failures are ignored (`tryWrite`).
pub async fn clone_repo(
    git_dir: &Path,
    remote_url: &str,
    progress: &mut Vec<u8>,
) -> Result<(), GitError> {
    clone_repo_with(git_dir, remote_url, progress, &mut |_| {}).await
}

/// [`clone_repo`] reporting every syncer message and fetch progress line, the
/// way `readFetchProgress` turns the progress pipe into lock statuses.
pub async fn clone_repo_with(
    git_dir: &Path,
    remote_url: &str,
    progress: &mut Vec<u8>,
    on_progress: &mut (dyn FnMut(&str) + Send),
) -> Result<(), GitError> {
    std::fs::create_dir_all(git_dir)
        .map_err(|err| GitError::Message(format!("clone failed to create tmp dir: {err}")))?;

    emit(progress, on_progress, "Creating bare repo\n");
    make_bare_repo(git_dir).await?;
    emit(
        progress,
        on_progress,
        &format!("Created bare repo at {}\n", git_dir.display()),
    );

    emit(progress, on_progress, "Fetching remote contents\n");
    let outcome = fetch_remote_with(git_dir, remote_url, on_progress).await?;
    progress.extend_from_slice(&outcome.output);
    if outcome.exit_status != 0 {
        return Err(GitError::Message(format!(
            "failed to fetch: exit status {}",
            outcome.exit_status
        )));
    }
    emit(progress, on_progress, "Fetched remote contents\n");

    emit(progress, on_progress, "Setting local HEAD to remote HEAD\n");
    set_head(git_dir, remote_url).await?;
    emit(
        progress,
        on_progress,
        "Finished setting local HEAD to remote HEAD\n",
    );

    Ok(())
}

fn emit(progress: &mut Vec<u8>, on_progress: &mut (dyn FnMut(&str) + Send), message: &str) {
    let _ = progress.write_all(message.as_bytes());
    on_progress(message.trim_end());
}

/// Port of `gitRepoSyncer.Fetch`: fetch into an existing repository, then
/// point `HEAD` at the remote's default branch. Progress is reported the same
/// way as during a clone.
pub async fn fetch_repo_with(
    git_dir: &Path,
    remote_url: &str,
    progress: &mut Vec<u8>,
    on_progress: &mut (dyn FnMut(&str) + Send),
) -> Result<(), GitError> {
    emit(progress, on_progress, "Fetching remote contents\n");
    let outcome = fetch_remote_with(git_dir, remote_url, on_progress).await?;
    progress.extend_from_slice(&outcome.output);
    if outcome.exit_status != 0 {
        return Err(GitError::Message(format!(
            "exit code: {}, failed to fetch from remote",
            outcome.exit_status
        )));
    }
    emit(progress, on_progress, "Fetched remote contents\n");

    emit(progress, on_progress, "Setting local HEAD to remote HEAD\n");
    set_head(git_dir, remote_url).await?;
    emit(
        progress,
        on_progress,
        "Finished setting local HEAD to remote HEAD\n",
    );
    Ok(())
}

/// The temp path a clone is staged in before being renamed into the repo
/// tree: `<tmp_dir>/.git`, like `filepath.Join(tmpDir, ".git")`.
pub fn clone_tmp_path(tmp_dir: &Path) -> PathBuf {
    tmp_dir.join(".git")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn git_output(dir: &Path, args: &[&str]) -> std::process::Output {
        std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_DIR", dir)
            .output()
            .expect("run git")
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }

    fn source_repo(root: &Path) -> PathBuf {
        let origin = root.join("origin.git");
        std::fs::create_dir_all(&origin).expect("mkdir");
        git(&origin, &["init", "--bare", "-q", "--initial-branch=trunk"]);
        let work = root.join("work");
        git(
            root,
            &[
                "init",
                "-q",
                "--initial-branch=trunk",
                work.to_str().unwrap(),
            ],
        );
        std::fs::write(work.join("a.txt"), b"hello\n").expect("write");
        git(&work, &["add", "a.txt"]);
        git(
            &work,
            &[
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "-q",
                "-m",
                "first",
            ],
        );
        git(
            &work,
            &[
                "push",
                "-q",
                origin.to_str().unwrap(),
                "HEAD:refs/heads/trunk",
            ],
        );
        origin
    }

    #[test]
    fn ensures_head_like_the_baseline() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        // Missing HEAD: created with the baseline content and mode 0600.
        ensure_head(dir).expect("create");
        let head = dir.join("HEAD");
        assert_eq!(
            std::fs::read(&head).expect("read"),
            b"ref: refs/heads/master"
        );
        assert_eq!(
            std::fs::metadata(&head).expect("stat").permissions().mode() & 0o777,
            0o600
        );
        // Non-empty HEAD is left alone.
        std::fs::write(&head, b"ref: refs/heads/other\n").expect("write");
        ensure_head(dir).expect("keep");
        assert_eq!(
            std::fs::read(&head).expect("read"),
            b"ref: refs/heads/other\n"
        );
        // Empty HEAD is rewritten.
        std::fs::write(&head, b"").expect("write");
        ensure_head(dir).expect("rewrite");
        assert_eq!(
            std::fs::read(&head).expect("read"),
            b"ref: refs/heads/master"
        );
    }

    #[test]
    fn scans_progress_lines_like_the_baseline() {
        let mut lines = Vec::new();
        let mut on_line = |line: &str| lines.push(line.to_owned());
        let mut scanner = LineScanner::default();
        scanner.push(
            b"Receiving objects: 1%\rReceiving objects: 2%",
            &mut on_line,
        );
        scanner.push(b"\n  \r\nWriting objects: 100%\npartial", &mut on_line);
        scanner.finish(&mut on_line);
        assert_eq!(
            lines,
            vec![
                "Receiving objects: 1%",
                "Receiving objects: 2%",
                "Writing objects: 100%",
                "partial",
            ]
        );
    }

    #[test]
    fn builds_the_default_fetch_refspecs() {
        assert_eq!(
            fetch_refspecs(),
            vec![
                "+refs/heads/*:refs/heads/*",
                "+refs/tags/*:refs/tags/*",
                "+refs/pull/*:refs/pull/*",
                "+refs/merge-requests/*:refs/merge-requests/*",
                "+refs/pull-requests/*:refs/pull-requests/*",
                "+refs/changes/*:refs/changes/*",
                "+refs/sourcegraph/*:refs/sourcegraph/*",
            ]
        );
    }

    #[tokio::test]
    async fn clones_from_a_local_remote() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let origin = source_repo(tmp.path());
        let tmp_dir = tmp.path().join("clone-1");
        let git_dir = clone_tmp_path(&tmp_dir);
        let mut progress = Vec::new();

        clone_repo(&git_dir, origin.to_str().expect("utf8"), &mut progress)
            .await
            .expect("clone");

        let progress = String::from_utf8(progress).expect("utf8");
        for expected in [
            "Creating bare repo\n",
            "Fetching remote contents\n",
            "Fetched remote contents\n",
            "Setting local HEAD to remote HEAD\n",
            "Finished setting local HEAD to remote HEAD\n",
        ] {
            assert!(
                progress.contains(expected),
                "missing {expected:?} in {progress:?}"
            );
        }

        // The remote branch was fetched, and HEAD points at it.
        let rev = git_output(&git_dir, &["rev-parse", "--verify", "refs/heads/trunk"]);
        assert!(rev.status.success(), "trunk not fetched");
        let head = git_output(&git_dir, &["symbolic-ref", "HEAD"]);
        assert!(head.status.success());
        assert_eq!(
            String::from_utf8_lossy(&head.stdout).trim(),
            "refs/heads/trunk"
        );
    }

    #[tokio::test]
    async fn reports_a_fetch_failure() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let git_dir = tmp.path().join("clone-2").join(".git");
        let mut progress = Vec::new();
        let err = clone_repo(&git_dir, "/does/not/exist", &mut progress)
            .await
            .expect_err("missing remote");
        match err {
            GitError::Message(message) => {
                assert!(
                    message.starts_with("failed to fetch: exit status "),
                    "unexpected message: {message}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
        // The bare repo and its progress output were still produced.
        assert!(git_dir.join("HEAD").exists());
        assert!(String::from_utf8_lossy(&progress).contains("Creating bare repo\n"));
    }
}
