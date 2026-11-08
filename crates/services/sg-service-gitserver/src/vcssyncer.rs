//! Git-level clone/fetch primitives from
//! `cmd/gitserver/internal/vcssyncer/git.go` and the helpers it uses.
//!
//! The baseline does not run `git clone`: it initializes a bare repository and
//! fetches the configured refspecs into it, then points `HEAD` at the remote's
//! default branch. Only the git syncer is covered here; the site-config remote
//! URL lookup (`cloneurl`) and custom fetch commands (site configuration) are
//! not ported yet. URL parsing, credential handling, and output redaction do
//! follow the baseline: credentials move from the command line into
//! `GIT_SG_*` env vars, and progress output is redacted before it is written.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::OnceLock;

use regex::Regex;
use tokio::process::Command;

use crate::git::GitError;
use crate::vcsurl::{UrlRedactor, VcsUrl};

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

/// Port of `executil.ConfigureRemoteGitCommand` (minus the site-config TLS
/// settings, which are not ported yet).
///
/// The baseline prepends `-c` options to `git`'s arguments and appends
/// environment variables. When the remote URL carries a password and is not
/// SSH, the URL argument loses its userinfo and an extra credential helper
/// reads `GIT_SG_USERNAME`/`GIT_SG_PASSWORD` instead, so the secret does not
/// appear in `ps` output.
fn configure_remote_command(
    args: &mut Vec<String>,
    env: &mut Vec<(String, String)>,
    remote: &VcsUrl,
) {
    // Disable password prompts; the command is non-interactive.
    env.push(("GIT_ASKPASS".to_owned(), "true".to_owned()));
    // Suppress SSH host-key prompts and bound the connect time.
    env.push((
        "GIT_SSH_COMMAND".to_owned(),
        "ssh -o BatchMode=yes -o ConnectTimeout=30".to_owned(),
    ));
    // Keep the `git/` prefix: GitHub breaks protocol v2 negotiation without it.
    env.push((
        "GIT_HTTP_USER_AGENT".to_owned(),
        "git/Sourcegraph-Bot".to_owned(),
    ));

    // Unset credential helpers because the command is non-interactive. Even
    // when a second helper is added below, this is required so the original
    // helper is not used as well.
    let mut extra_args = vec!["-c".to_owned(), "credential.helper=".to_owned()];

    if let Some(password) = remote.password().filter(|_| !remote.is_ssh()) {
        // The baseline compares `arg == remoteURL.String()`; a URL copied
        // verbatim from the remote source therefore matches.
        let url_string = remote.to_string();
        let mut has_creds = false;
        for arg in args.iter_mut() {
            if *arg == url_string {
                *arg = remote.without_user();
                has_creds = true;
            }
        }
        if has_creds {
            extra_args.push("-c".to_owned());
            extra_args.push(
                "credential.helper=!f() { echo \"username=$GIT_SG_USERNAME\\npassword=$GIT_SG_PASSWORD\"; }; f"
                    .to_owned(),
            );
            env.push(("GIT_SG_USERNAME".to_owned(), remote.username().to_owned()));
            env.push(("GIT_SG_PASSWORD".to_owned(), password.to_owned()));
        }
    }

    // Protocol v2 for every command except `ls-remote`, where it hurts.
    if args.len() > 1 && args[1] != "ls-remote" {
        extra_args.push("-c".to_owned());
        extra_args.push("protocol.version=2".to_owned());
    }

    if let Some((program, rest)) = args.split_first_mut() {
        let mut configured = vec![std::mem::take(program)];
        configured.append(&mut extra_args);
        configured.extend(rest.iter().cloned());
        *args = configured;
    }
}

/// Port of `runFetchCommand` for the default command shape:
/// `git -c credential.helper= [-c credential.helper=…] -c protocol.version=2
/// fetch --progress --prune <url> <refspecs...>` with `GIT_DIR` set and
/// `GIT_LFS_SKIP_SMUDGE=1` (see issue #7322: skip LFS content in repositories
/// with Git LFS configured).
pub async fn fetch_remote(git_dir: &Path, remote: &VcsUrl) -> Result<i32, GitError> {
    fetch_remote_with(git_dir, remote, &mut |_| {}).await
}

/// [`fetch_remote`] while reporting the command's output line by line.
///
/// Both stdout and stderr are split with `scanLinesWithCRLF` (the token keeps
/// its `\r`/`\n` delimiter) and passed to `on_progress`, matching
/// `executil.RunCommandWriteOutput`. Every token is redacted first, so the
/// remote's credentials never reach `SetLastOutput` or the lock status. The
/// baseline interleaves the two streams through a synchronized writer; here
/// they are drained concurrently and tokens may arrive in either order, which
/// only affects display.
pub async fn fetch_remote_with(
    git_dir: &Path,
    remote: &VcsUrl,
    on_progress: &mut (dyn FnMut(&[u8]) + Send),
) -> Result<i32, GitError> {
    let mut args = vec![
        "git".to_owned(),
        "fetch".to_owned(),
        "--progress".to_owned(),
        "--prune".to_owned(),
        remote.to_string(),
    ];
    for refspec in fetch_refspecs() {
        args.push(refspec);
    }
    let mut env = Vec::new();
    configure_remote_command(&mut args, &mut env, remote);

    let mut command = Command::new(&args[0]);
    command
        .args(&args[1..])
        .current_dir(git_dir)
        .env("GIT_DIR", git_dir)
        .env("GIT_LFS_SKIP_SMUDGE", "1");
    for (key, value) in &env {
        command.env(key, value);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|err| GitError::Message(format!("failed to start command: {err}")))?;
    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");

    // The baseline redacts each line before writing it out.
    let redactor = UrlRedactor::new(remote);
    let mut redact = |token: &[u8]| {
        let text = String::from_utf8_lossy(token);
        on_progress(redactor.redact(&text).as_bytes());
    };

    // The baseline serializes stdout and stderr through a synchronized writer;
    // a mutex around the callback gives the same guarantee (it is only held
    // for the synchronous call, never across an await).
    let on_progress = std::sync::Mutex::new(&mut redact as &mut (dyn FnMut(&[u8]) + Send));
    let read_stdout = scan_stream(&mut stdout, &on_progress);
    let read_stderr = scan_stream(&mut stderr, &on_progress);
    let (stdout_result, stderr_result) = tokio::join!(read_stdout, read_stderr);
    stdout_result
        .map_err(|err| GitError::Message(format!("failed to read fetch output: {err}")))?;
    stderr_result
        .map_err(|err| GitError::Message(format!("failed to read fetch output: {err}")))?;

    let status = child
        .wait()
        .await
        .map_err(|err| GitError::Message(format!("failed to wait for git: {err}")))?;
    Ok(status.code().unwrap_or(-1))
}

/// Progress callback shared by the stdout and stderr readers.
type SharedProgress<'a> = std::sync::Mutex<&'a mut (dyn FnMut(&[u8]) + Send)>;

/// Drain one child pipe through a [`LineScanner`]. The callback is shared
/// with the other pipe through a mutex (see the call site).
async fn scan_stream(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
    on_line: &SharedProgress<'_>,
) -> std::io::Result<()> {
    let mut scanner = LineScanner::default();
    let mut chunk = [0u8; 4096];
    loop {
        let read = tokio::io::AsyncReadExt::read(stream, &mut chunk).await?;
        if read == 0 {
            break;
        }
        let mut guard = on_line.lock().unwrap_or_else(|err| err.into_inner());
        scanner.push(&chunk[..read], &mut *guard);
        drop(guard);
    }
    let mut guard = on_line.lock().unwrap_or_else(|err| err.into_inner());
    scanner.finish(&mut *guard);
    Ok(())
}

/// Splits a byte stream on `\r` and `\n` (`scanLinesWithCRLF`): every token
/// keeps its delimiter. Unlike `readFetchProgress`, no trimming or filtering
/// happens here; consumers that display the line do that themselves.
#[derive(Default)]
struct LineScanner {
    buffer: Vec<u8>,
}

impl LineScanner {
    fn push(&mut self, data: &[u8], on_line: &mut (dyn FnMut(&[u8]) + Send)) {
        self.buffer.extend_from_slice(data);
        let mut consumed = 0;
        for index in 0..self.buffer.len() {
            if self.buffer[index] == b'\r' || self.buffer[index] == b'\n' {
                on_line(&self.buffer[consumed..=index]);
                consumed = index + 1;
            }
        }
        if consumed > 0 {
            self.buffer.drain(..consumed);
        }
    }

    /// The final, unterminated line at EOF.
    fn finish(&mut self, on_line: &mut (dyn FnMut(&[u8]) + Send)) {
        if !self.buffer.is_empty() {
            on_line(&self.buffer);
        }
        self.buffer.clear();
    }
}

/// Port of `setHEAD`: pick the remote's default branch, falling back to
/// `master`, then to the first local branch, and point `HEAD` at it.
pub async fn set_head(git_dir: &Path, remote: &VcsUrl) -> Result<(), GitError> {
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
    let remote_string = remote.to_string();
    let output = run_bare(git_dir, &["remote", "show", &remote_string], Some(remote)).await?;
    if !output.status.success() {
        let redactor = UrlRedactor::new(remote);
        return Err(GitError::Message(format!(
            "failed to fetch remote info: {}",
            redactor.redact(String::from_utf8_lossy(&output.stderr).trim_end())
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
    let check = run_bare(git_dir, &["rev-parse", &head_branch, "--"], None).await?;
    if !check.status.success() {
        let branches = run_bare(git_dir, &["branch"], None).await?;
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
    let output = run_bare(git_dir, &["symbolic-ref", "HEAD", &target], None).await?;
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
/// `common.GitDir.Set` followed by an optional `ConfigureRemoteGitCommand`.
/// The environment is cleared for determinism; only the variables the
/// baseline would set for a remote command are added back.
async fn run_bare(
    git_dir: &Path,
    args: &[&str],
    remote: Option<&VcsUrl>,
) -> Result<Output, GitError> {
    let mut configured_args: Vec<String> = std::iter::once("git".to_owned())
        .chain(args.iter().map(|arg| (*arg).to_owned()))
        .collect();
    let mut env = Vec::new();
    if let Some(remote) = remote {
        configure_remote_command(&mut configured_args, &mut env, remote);
    }

    let mut command = Command::new(&configured_args[0]);
    command
        .args(&configured_args[1..])
        .current_dir(git_dir)
        .env_clear()
        .env("GIT_DIR", git_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in &env {
        command.env(key, value);
    }
    command
        .output()
        .await
        .map_err(|err| GitError::Message(format!("failed to run git: {err}")))
}

/// Port of `gitRepoSyncer.Clone`: `git init --bare`, fetch all refspecs from
/// the remote into the temporary directory, then set `HEAD`. Every syncer
/// message and git output line is passed to `on_progress` (`tryWrite`).
pub async fn clone_repo(
    git_dir: &Path,
    remote: &VcsUrl,
    on_progress: &mut (dyn FnMut(&[u8]) + Send),
) -> Result<(), GitError> {
    clone_repo_with(git_dir, remote, on_progress).await
}

/// [`clone_repo`]: syncer messages and fetch output are reported the way
/// `readFetchProgress` turns the progress pipe into lock statuses.
pub async fn clone_repo_with(
    git_dir: &Path,
    remote: &VcsUrl,
    on_progress: &mut (dyn FnMut(&[u8]) + Send),
) -> Result<(), GitError> {
    std::fs::create_dir_all(git_dir)
        .map_err(|err| GitError::Message(format!("clone failed to create tmp dir: {err}")))?;

    emit(on_progress, "Creating bare repo\n");
    make_bare_repo(git_dir).await?;
    emit(
        on_progress,
        &format!("Created bare repo at {}\n", git_dir.display()),
    );

    emit(on_progress, "Fetching remote contents\n");
    let exit_status = fetch_remote_with(git_dir, remote, on_progress).await?;
    if exit_status != 0 {
        return Err(GitError::Message(format!(
            "failed to fetch: exit status {exit_status}"
        )));
    }
    emit(on_progress, "Fetched remote contents\n");

    emit(on_progress, "Setting local HEAD to remote HEAD\n");
    set_head(git_dir, remote).await?;
    emit(on_progress, "Finished setting local HEAD to remote HEAD\n");

    Ok(())
}

fn emit(on_progress: &mut (dyn FnMut(&[u8]) + Send), message: &str) {
    on_progress(message.as_bytes());
}

/// Port of `gitRepoSyncer.Fetch`: fetch into an existing repository, then
/// point `HEAD` at the remote's default branch. Progress is reported the same
/// way as during a clone.
pub async fn fetch_repo_with(
    git_dir: &Path,
    remote: &VcsUrl,
    on_progress: &mut (dyn FnMut(&[u8]) + Send),
) -> Result<(), GitError> {
    emit(on_progress, "Fetching remote contents\n");
    let exit_status = fetch_remote_with(git_dir, remote, on_progress).await?;
    if exit_status != 0 {
        return Err(GitError::Message(format!(
            "exit code: {exit_status}, failed to fetch from remote"
        )));
    }
    emit(on_progress, "Fetched remote contents\n");

    emit(on_progress, "Setting local HEAD to remote HEAD\n");
    set_head(git_dir, remote).await?;
    emit(on_progress, "Finished setting local HEAD to remote HEAD\n");
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
        let mut lines: Vec<Vec<u8>> = Vec::new();
        let mut on_line = |line: &[u8]| lines.push(line.to_vec());
        let mut scanner = LineScanner::default();
        scanner.push(
            b"Receiving objects: 1%\rReceiving objects: 2%",
            &mut on_line,
        );
        scanner.push(b"\n  \r\nWriting objects: 100%\npartial", &mut on_line);
        scanner.finish(&mut on_line);
        // Tokens keep their delimiter (`scanLinesWithCRLF`); the consumer
        // trims them before using them as a lock status.
        assert_eq!(
            lines,
            vec![
                b"Receiving objects: 1%\r".to_vec(),
                b"Receiving objects: 2%\n".to_vec(),
                b"  \r".to_vec(),
                b"\n".to_vec(),
                b"Writing objects: 100%\n".to_vec(),
                b"partial".to_vec(),
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
        let mut on_progress = |line: &[u8]| progress.extend_from_slice(line);

        let remote = VcsUrl::parse(origin.to_str().expect("utf8")).expect("parse");
        clone_repo(&git_dir, &remote, &mut on_progress)
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

    #[test]
    fn configures_remote_commands_like_the_baseline() {
        // Credentials move from the argument list into the environment.
        let remote = VcsUrl::parse("https://user:password@host.xz/org/repo.git").expect("parse");
        let mut args = vec!["git".to_owned(), "fetch".to_owned(), remote.to_string()];
        let mut env = Vec::new();
        configure_remote_command(&mut args, &mut env, &remote);
        assert_eq!(
            args,
            vec![
                "git".to_owned(),
                "-c".to_owned(),
                "credential.helper=".to_owned(),
                "-c".to_owned(),
                "credential.helper=!f() { echo \"username=$GIT_SG_USERNAME\\npassword=$GIT_SG_PASSWORD\"; }; f".to_owned(),
                "-c".to_owned(),
                "protocol.version=2".to_owned(),
                "fetch".to_owned(),
                "https://host.xz/org/repo.git".to_owned(),
            ]
        );
        assert!(env.contains(&("GIT_SG_USERNAME".to_owned(), "user".to_owned())));
        assert!(env.contains(&("GIT_SG_PASSWORD".to_owned(), "password".to_owned())));
        assert!(env.contains(&("GIT_ASKPASS".to_owned(), "true".to_owned())));

        // `ls-remote` does not get protocol v2.
        let mut args = vec!["git".to_owned(), "ls-remote".to_owned(), remote.to_string()];
        let mut env = Vec::new();
        configure_remote_command(&mut args, &mut env, &remote);
        assert!(!args.iter().any(|arg| arg == "protocol.version=2"));

        // SSH URLs keep their credentials out of the helper machinery.
        let ssh = VcsUrl::parse("ssh://user:password@host.xz/org/repo.git").expect("parse");
        let mut args = vec!["git".to_owned(), "fetch".to_owned(), ssh.to_string()];
        let mut env = Vec::new();
        configure_remote_command(&mut args, &mut env, &ssh);
        assert!(args.contains(&ssh.to_string()));
        assert!(!env.iter().any(|(key, _)| key.starts_with("GIT_SG_")));

        // No password: the URL is left alone and no helper is added.
        let plain = VcsUrl::parse("https://host.xz/org/repo.git").expect("parse");
        let mut args = vec!["git".to_owned(), "fetch".to_owned(), plain.to_string()];
        let mut env = Vec::new();
        configure_remote_command(&mut args, &mut env, &plain);
        assert!(args.contains(&plain.to_string()));
        assert!(!env.iter().any(|(key, _)| key.starts_with("GIT_SG_")));
    }

    #[tokio::test]
    async fn redacts_credentials_from_fetch_progress() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let git_dir = tmp.path().join("clone-3").join(".git");
        // A remote URL with credentials that does not exist: git reports the
        // URL in its error output, which must be redacted.
        let remote = VcsUrl::parse("https://user:secret@127.0.0.1:1/org/repo.git").expect("parse");
        let mut progress = Vec::new();
        let mut on_progress = |line: &[u8]| progress.extend_from_slice(line);
        clone_repo(&git_dir, &remote, &mut on_progress)
            .await
            .expect_err("unreachable remote");
        let progress = String::from_utf8_lossy(&progress).into_owned();
        // The credential never appears: it is passed through GIT_SG_PASSWORD
        // and every progress token is redacted before it is written.
        assert!(
            !progress.contains("secret"),
            "credential leaked: {progress}"
        );
        assert!(!progress.contains("user@"), "userinfo leaked: {progress}");
        assert!(progress.contains("https://127.0.0.1:1/org/repo.git"));
    }

    #[tokio::test]
    async fn reports_a_fetch_failure() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let git_dir = tmp.path().join("clone-2").join(".git");
        let mut progress = Vec::new();
        let mut on_progress = |line: &[u8]| progress.extend_from_slice(line);
        let remote = VcsUrl::parse("/does/not/exist").expect("parse");
        let err = clone_repo(&git_dir, &remote, &mut on_progress)
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
