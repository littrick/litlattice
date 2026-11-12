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
use std::sync::{Arc, OnceLock};

use regex::Regex;
use tokio::process::Command;

use crate::customfetch::CustomFetch;
use crate::git::GitError;
use crate::tlsexternal::TlsConfig;
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
        // The operation-level deadline drops this future; the baseline kills
        // the whole process group (see the module notes in the docs).
        .kill_on_drop(true)
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

/// Port of `executil.configureRemoteGitCommand`, including the
/// `experimentalFeatures.tlsExternal` settings.
///
/// The baseline prepends `-c` options to `git`'s arguments and appends
/// environment variables. When the remote URL carries a password and is not
/// SSH, the URL argument loses its userinfo and an extra credential helper
/// reads `GIT_SG_USERNAME`/`GIT_SG_PASSWORD` instead, so the secret does not
/// appear in `ps` output.
pub(crate) fn configure_remote_command(
    args: &mut Vec<String>,
    env: &mut Vec<(String, String)>,
    remote: &VcsUrl,
    tls: &TlsConfig,
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

    // `experimentalFeatures.tlsExternal`.
    if tls.ssl_no_verify {
        env.push(("GIT_SSL_NO_VERIFY".to_owned(), "true".to_owned()));
    }
    if let Some(path) = &tls.ssl_ca_info {
        env.push((
            "GIT_SSL_CAINFO".to_owned(),
            path.to_string_lossy().into_owned(),
        ));
    }

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
pub async fn fetch_remote(
    git_dir: &Path,
    remote: &VcsUrl,
    tls: &TlsConfig,
    custom_fetch: &CustomFetch,
) -> Result<i32, GitError> {
    fetch_remote_with(git_dir, remote, tls, custom_fetch, &mut |_| {}).await
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
///
/// A `customGitFetch` mapping replaces the whole `git fetch` invocation and,
/// like the baseline, skips `ConfigureRemoteGitCommand`: `GIT_DIR` and
/// `GIT_LFS_SKIP_SMUDGE` still apply, the process environment is inherited.
pub async fn fetch_remote_with(
    git_dir: &Path,
    remote: &VcsUrl,
    tls: &TlsConfig,
    custom_fetch: &CustomFetch,
    on_progress: &mut (dyn FnMut(&[u8]) + Send),
) -> Result<i32, GitError> {
    let mut env = Vec::new();
    let args = match custom_fetch.command(remote) {
        Some(command) => command,
        None => {
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
            configure_remote_command(&mut args, &mut env, remote, tls);
            args
        }
    };

    let mut command = Command::new(&args[0]);
    command
        .args(&args[1..])
        .current_dir(git_dir)
        .kill_on_drop(true)
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
pub async fn set_head(git_dir: &Path, remote: &VcsUrl, tls: &TlsConfig) -> Result<(), GitError> {
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
    let output = run_bare(
        git_dir,
        &["remote", "show", &remote_string],
        Some(remote),
        tls,
    )
    .await?;
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
    let check = run_bare(git_dir, &["rev-parse", &head_branch, "--"], None, tls).await?;
    if !check.status.success() {
        let branches = run_bare(git_dir, &["branch"], None, tls).await?;
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
    let output = run_bare(git_dir, &["symbolic-ref", "HEAD", &target], None, tls).await?;
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
    tls: &TlsConfig,
) -> Result<Output, GitError> {
    let mut configured_args: Vec<String> = std::iter::once("git".to_owned())
        .chain(args.iter().map(|arg| (*arg).to_owned()))
        .collect();
    let mut env = Vec::new();
    if let Some(remote) = remote {
        configure_remote_command(&mut configured_args, &mut env, remote, tls);
    }

    let mut command = Command::new(&configured_args[0]);
    command
        .args(&configured_args[1..])
        .current_dir(git_dir)
        .kill_on_drop(true)
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

/// `gitRepoSyncer.IsCloneable`: `git ls-remote <url> HEAD` with the
/// baseline's 30-second deadline and command configuration.
///
/// Failure text matches `errors.Wrap(err, "failed to check remote access: " +
/// httpOutput)`: the combined output comes first, then the exit status. The
/// environment is inherited (unlike `run_bare`), because `ls-remote` relies on
/// `HOME`/`SSH_AUTH_SOCK`/credential helpers.
pub async fn check_cloneable(remote: &VcsUrl, tls: &TlsConfig) -> Result<(), GitError> {
    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    let mut args = vec![
        "git".to_owned(),
        "ls-remote".to_owned(),
        remote.to_string(),
        "HEAD".to_owned(),
    ];
    let mut env = Vec::new();
    configure_remote_command(&mut args, &mut env, remote, tls);

    let mut command = Command::new(&args[0]);
    command
        .args(&args[1..])
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in &env {
        command.env(key, value);
    }

    let output = match tokio::time::timeout(TIMEOUT, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => return Err(GitError::Message(format!("failed to run git: {err}"))),
        // The baseline replaces the command error with `ctx.Err()`.
        Err(_) => return Err(GitError::Message("context deadline exceeded".to_owned())),
    };
    if output.status.success() {
        return Ok(());
    }

    let redactor = UrlRedactor::new(remote);
    let mut combined = output.stdout;
    combined.extend_from_slice(&output.stderr);
    let exit = exit_status(&output.status);
    if combined.is_empty() {
        return Err(GitError::Message(exit));
    }
    Err(GitError::Message(format!(
        "failed to check remote access: {}: {exit}",
        redactor.redact(&String::from_utf8_lossy(&combined))
    )))
}

/// Go `exec.ExitError.Error()`.
pub(crate) fn exit_status(status: &std::process::ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal: {signal}");
        }
    }
    "exit status 1".to_owned()
}

/// Port of `gitRepoSyncer.Clone`: `git init --bare`, fetch all refspecs from
/// the remote into the temporary directory, then set `HEAD`. Every syncer
/// message and git output line is passed to `on_progress` (`tryWrite`).
pub async fn clone_repo(
    git_dir: &Path,
    remote: &VcsUrl,
    tls: &TlsConfig,
    custom_fetch: &CustomFetch,
    on_progress: &mut (dyn FnMut(&[u8]) + Send),
) -> Result<(), GitError> {
    clone_repo_with(git_dir, remote, tls, custom_fetch, on_progress).await
}

/// [`clone_repo`]: syncer messages and fetch output are reported the way
/// `readFetchProgress` turns the progress pipe into lock statuses.
pub async fn clone_repo_with(
    git_dir: &Path,
    remote: &VcsUrl,
    tls: &TlsConfig,
    custom_fetch: &CustomFetch,
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
    let exit_status = fetch_remote_with(git_dir, remote, tls, custom_fetch, on_progress).await?;
    if exit_status != 0 {
        return Err(GitError::Message(format!(
            "failed to fetch: exit status {exit_status}"
        )));
    }
    emit(on_progress, "Fetched remote contents\n");

    emit(on_progress, "Setting local HEAD to remote HEAD\n");
    set_head(git_dir, remote, tls).await?;
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
    tls: &TlsConfig,
    custom_fetch: &CustomFetch,
    on_progress: &mut (dyn FnMut(&[u8]) + Send),
) -> Result<(), GitError> {
    emit(on_progress, "Fetching remote contents\n");
    let exit_status = fetch_remote_with(git_dir, remote, tls, custom_fetch, on_progress).await?;
    if exit_status != 0 {
        return Err(GitError::Message(format!(
            "exit code: {exit_status}, failed to fetch from remote"
        )));
    }
    emit(on_progress, "Fetched remote contents\n");

    emit(on_progress, "Setting local HEAD to remote HEAD\n");
    set_head(git_dir, remote, tls).await?;
    emit(on_progress, "Finished setting local HEAD to remote HEAD\n");
    Ok(())
}

/// The temp path a clone is staged in before being renamed into the repo
/// tree: `<tmp_dir>/.git`, like `filepath.Join(tmpDir, ".git")`.
pub fn clone_tmp_path(tmp_dir: &Path) -> PathBuf {
    tmp_dir.join(".git")
}

// ---------------------------------------------------------------------------
// Non-git syncer: `cmd/gitserver/internal/vcssyncer/packages_syncer.go` and
// the package-manager sources. JVM/coursier is intentionally not ported.
// ---------------------------------------------------------------------------

/// `stableGitCommitDate`: every synthetic commit uses the same timestamp.
pub const STABLE_GIT_COMMIT_DATE: &str = "2000-01-01T00:00:00Z";

/// The package ecosystem a [`PackageSyncer`] serves.
pub use sg_extsvc::reposource_packages::PackageKind;

/// `reposource.VersionedPackage` for a package name and version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedPackage {
    pub kind: PackageKind,
    pub package: String,
    pub version: String,
}

impl VersionedPackage {
    /// `packagesSource.ParseVersionedPackageFromNameAndVersion`.
    pub fn from_name_and_version(
        kind: PackageKind,
        name: &str,
        version: &str,
    ) -> Result<Self, String> {
        let dependency = match kind {
            PackageKind::Python => format!("{name}=={version}"),
            _ => format!("{name}@{version}"),
        };
        Self::from_configuration(kind, &dependency)
    }

    /// `packagesSource.ParseVersionedPackageFromConfiguration`.
    pub fn from_configuration(kind: PackageKind, dependency: &str) -> Result<Self, String> {
        let info = sg_extsvc::reposource_packages::parse_versioned_package(kind, dependency)?;
        Ok(Self {
            kind,
            package: info.syntax,
            version: info.version,
        })
    }

    /// `VersionedPackageSyntax`: the syntax a package manager accepts.
    pub fn versioned_package_syntax(&self) -> String {
        if self.version.is_empty() {
            return self.package.clone();
        }
        match self.kind {
            PackageKind::Python => format!("{}=={}", self.package, self.version),
            _ => format!("{}@{}", self.package, self.version),
        }
    }

    /// `GitTagFromVersion`.
    pub fn git_tag(&self) -> String {
        match self.kind {
            PackageKind::Go => self.version.clone(),
            _ => format!("v{}", self.version.trim_start_matches('v')),
        }
    }

    /// `VersionedPackage.Less`: descending order, so the latest version sorts
    /// first. Different packages sort by package name descending.
    pub fn less(&self, other: &Self) -> bool {
        if self.package == other.package {
            return version_greater_than(&self.version, &other.version);
        }
        self.package > other.package
    }
}

/// `reposource.versionGreaterThan` (internal/conf/reposource/package_version.go).
pub fn version_greater_than(version1: &str, version2: &str) -> bool {
    let a = version1.as_bytes();
    let b = version2.as_bytes();
    let mut index = 0;
    let end = a.len().min(b.len());
    while index < end {
        if a[index].is_ascii_digit() && b[index].is_ascii_digit() {
            let int1 = version_parse_int(index, a);
            let int2 = version_parse_int(index, b);
            if int1 == int2 {
                index = version_next_non_digit_offset(index, a);
            } else {
                return int1 > int2;
            }
        } else if a[index] == b[index] {
            index += 1;
        } else {
            return a[index] > b[index];
        }
    }
    a.len() < b.len()
}

fn version_parse_int(index: usize, version: &[u8]) -> i64 {
    let end = version_next_non_digit_offset(index, version);
    std::str::from_utf8(&version[index..end])
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn version_next_non_digit_offset(index: usize, version: &[u8]) -> usize {
    let mut offset = index;
    while offset < version.len() && version[offset].is_ascii_digit() {
        offset += 1;
    }
    offset
}

/// `packagesSource`: download the package and parse names/versions. The HTTP
/// and unpacking details live behind this seam so the syncer is testable
/// offline.
#[async_trait::async_trait]
pub trait PackageSource: Send + Sync {
    async fn download(&self, dir: &Path, dependency: &VersionedPackage) -> Result<(), String>;
    fn parse_versioned_package_from_name_and_version(
        &self,
        name: &str,
        version: &str,
    ) -> Result<VersionedPackage, String>;
    fn parse_versioned_package_from_configuration(
        &self,
        dependency: &str,
    ) -> Result<VersionedPackage, String>;
    fn parse_package_from_repo_name(&self, repo_name: &str) -> Result<String, String>;
}

/// A [`PackageSource`] that uses the shared `reposource` parsers and has no
/// downloader (tests or a future HTTP-backed source supply the download).
#[derive(Debug, Clone, Copy)]
pub struct ParsingPackageSource {
    pub kind: PackageKind,
}

#[async_trait::async_trait]
impl PackageSource for ParsingPackageSource {
    async fn download(&self, _dir: &Path, _dependency: &VersionedPackage) -> Result<(), String> {
        Err("package download is not configured".to_owned())
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
        Ok(sg_extsvc::reposource_packages::parse_package_from_repo_name(
            self.kind, repo_name,
        )?
        .syntax)
    }
}

/// `dependenciesService.ListPackageRepoRefs` for one exact package name.
#[async_trait::async_trait]
pub trait PackageVersionLister: Send + Sync {
    async fn versions(&self, scheme: &str, package: &str) -> Result<Vec<String>, String>;
}

/// A lister with no configured package-repo rows.
#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyPackageVersionLister;

#[async_trait::async_trait]
impl PackageVersionLister for EmptyPackageVersionLister {
    async fn versions(&self, _scheme: &str, _package: &str) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }
}

/// The injectable command runner (`exec.Cmd` + `CombinedOutput` in
/// `runCommandInDirectory`).
#[async_trait::async_trait]
pub trait PackageGitRunner: Send + Sync {
    /// Run `args` (including the program) in `dir`, with the baseline's stable
    /// author/committer environment, returning combined stdout+stderr.
    async fn combined_output(
        &self,
        dir: &Path,
        args: &[String],
        dependency: &VersionedPackage,
    ) -> Result<String, String>;
}

/// The production runner: inherits the environment and appends the baseline's
/// `EMAIL`/`GIT_*` variables.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessGitRunner;

#[async_trait::async_trait]
impl PackageGitRunner for ProcessGitRunner {
    async fn combined_output(
        &self,
        dir: &Path,
        args: &[String],
        dependency: &VersionedPackage,
    ) -> Result<String, String> {
        let Some(program) = args.first() else {
            return Err("empty command".to_owned());
        };
        let git_name = format!("{} authors", dependency.versioned_package_syntax());
        let git_email = "code-intel@sourcegraph.com";
        let mut command = Command::new(program);
        command
            .args(&args[1..])
            .current_dir(dir)
            .kill_on_drop(true)
            .env("EMAIL", git_email)
            .env("GIT_AUTHOR_NAME", &git_name)
            .env("GIT_AUTHOR_EMAIL", git_email)
            .env("GIT_AUTHOR_DATE", STABLE_GIT_COMMIT_DATE)
            .env("GIT_COMMITTER_NAME", &git_name)
            .env("GIT_COMMITTER_EMAIL", git_email)
            .env("GIT_COMMITTER_DATE", STABLE_GIT_COMMIT_DATE)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = command
            .output()
            .await
            .map_err(|err| format!("command {args:?} failed with output : {err}"))?;
        let combined = String::from_utf8_lossy(&output.stdout).into_owned()
            + &String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            return Err(format!("command {args:?} failed with output {combined}"));
        }
        Ok(combined)
    }
}

/// `vcsPackagesSyncer`: one synthetic git tag per requested package version.
pub struct PackageSyncer {
    pub kind: PackageKind,
    pub placeholder: VersionedPackage,
    pub config_deps: Vec<String>,
    pub source: Arc<dyn PackageSource>,
    pub version_lister: Arc<dyn PackageVersionLister>,
    pub git: Arc<dyn PackageGitRunner>,
    /// Where per-version work directories are created (`fs.TempDir`).
    pub temp_root: Option<PathBuf>,
}

impl PackageSyncer {
    pub fn new(
        kind: PackageKind,
        placeholder: VersionedPackage,
        config_deps: Vec<String>,
        source: Arc<dyn PackageSource>,
        version_lister: Arc<dyn PackageVersionLister>,
        git: Arc<dyn PackageGitRunner>,
    ) -> Self {
        Self {
            kind,
            placeholder,
            config_deps,
            source,
            version_lister,
            git,
            temp_root: None,
        }
    }

    /// `VCSSyncer.Type`: `npm_packages`, `go_modules`, ...
    pub fn type_name(&self) -> &'static str {
        match self.kind {
            PackageKind::Npm => "npm_packages",
            PackageKind::Go => "go_modules",
            PackageKind::Python => "python_packages",
            PackageKind::Rust => "rust_packages",
            PackageKind::Ruby => "ruby_packages",
            PackageKind::Jvm => "jvm_packages",
        }
    }

    /// `vcsPackagesSyncer.IsCloneable` always succeeds.
    pub fn is_cloneable(&self) -> Result<(), String> {
        Ok(())
    }

    /// `versions`: configured dependencies first, then the database rows.
    pub async fn versions(&self, package_name: &str) -> Result<Vec<String>, String> {
        let mut combined = Vec::new();
        for dependency in &self.config_deps {
            match self
                .source
                .parse_versioned_package_from_configuration(dependency)
            {
                Ok(dep) => {
                    if dep.package == package_name {
                        combined.push(dep.version);
                    }
                }
                // The baseline logs and skips malformed config dependencies.
                Err(_) => continue,
            }
        }
        let listed = self
            .version_lister
            .versions(self.kind.scheme(), package_name)
            .await
            .map_err(|err| format!("failed to list dependencies from db: {err}"))?;
        combined.extend(listed);
        Ok(combined)
    }

    /// `vcsPackagesSyncer.Fetch` after the remote URL was resolved: parse the
    /// package from the repo name, read its versions, then sync them.
    pub async fn fetch(
        &self,
        dir: &Path,
        repo_name: &str,
        on_progress: &mut (dyn FnMut(&[u8]) + Send),
    ) -> Result<(), String> {
        emit(on_progress, "Fetching package revisions\n");
        let package = self.source.parse_package_from_repo_name(repo_name)?;
        let versions = self.versions(&package).await?;
        self.fetch_versions(dir, &package, versions).await
    }

    /// `fetchVersions`: parse, sort descending, skip existing tags, push the
    /// rest, point `latest` at the newest success and prune stale tags.
    pub async fn fetch_versions(
        &self,
        dir: &Path,
        package_name: &str,
        versions: Vec<String>,
    ) -> Result<(), String> {
        let mut errors = Vec::new();
        let mut cloneable = Vec::with_capacity(versions.len());
        for version in versions {
            match self
                .source
                .parse_versioned_package_from_name_and_version(package_name, &version)
            {
                Ok(dependency) => cloneable.push(dependency),
                Err(err) => errors.push(err),
            }
        }
        if !errors.is_empty() {
            return Err(errors.join("\n"));
        }

        cloneable.sort_by(|a, b| {
            if a.less(b) {
                std::cmp::Ordering::Less
            } else if b.less(a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });

        let output = self
            .git
            .combined_output(dir, &["git".to_owned(), "tag".to_owned()], &self.placeholder)
            .await?;
        let tags: std::collections::HashSet<&str> = output
            .split('\n')
            .filter(|line| !line.is_empty())
            .collect();

        let mut cloned = Vec::new();
        for dependency in &cloneable {
            if tags.contains(dependency.git_tag().as_str()) {
                cloned.push(dependency.clone());
                continue;
            }
            match self.git_push_dependency_tag(dir, dependency).await {
                Ok(()) => cloned.push(dependency.clone()),
                Err(err) => errors.push(format!("error pushing dependency {dependency:?}: {err}")),
            }
        }

        if let Some(latest) = cloned.first() {
            self.git
                .combined_output(
                    dir,
                    &[
                        "git".to_owned(),
                        "branch".to_owned(),
                        "--force".to_owned(),
                        "latest".to_owned(),
                        latest.git_tag(),
                    ],
                    latest,
                )
                .await?;
        }

        if !errors.is_empty() {
            return Err(errors.join("\n"));
        }

        let dependency_tags: std::collections::HashSet<String> =
            cloneable.iter().map(VersionedPackage::git_tag).collect();
        for tag in tags {
            if !dependency_tags.contains(tag) {
                // Deleting a stale tag is best-effort; log and continue.
                let _ = self
                    .git
                    .combined_output(
                        dir,
                        &[
                            "git".to_owned(),
                            "tag".to_owned(),
                            "-d".to_owned(),
                            tag.to_owned(),
                        ],
                        &self.placeholder,
                    )
                    .await;
            }
        }

        if cloneable.is_empty() {
            let _ = self
                .git
                .combined_output(
                    dir,
                    &[
                        "git".to_owned(),
                        "branch".to_owned(),
                        "--force".to_owned(),
                        "-D".to_owned(),
                        "latest".to_owned(),
                    ],
                    &self.placeholder,
                )
                .await;
        }

        Ok(())
    }

    /// `gitPushDependencyTag`: download into a temp directory, commit it on a
    /// synthetic history and force-push the tag into `bare_git_directory`.
    pub async fn git_push_dependency_tag(
        &self,
        bare_git_directory: &Path,
        dependency: &VersionedPackage,
    ) -> Result<(), String> {
        let temp_root = self
            .temp_root
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        let work_dir = tempfile::Builder::new()
            .prefix(self.type_name())
            .tempdir_in(temp_root)
            .map_err(|err| format!("failed to create temp dir: {err}"))?;
        let work_dir_path = work_dir.path();

        self.source.download(work_dir_path, dependency).await?;

        for args in [
            vec!["git".to_owned(), "init".to_owned()],
            vec!["git".to_owned(), "add".to_owned(), ".".to_owned()],
        ] {
            self.git
                .combined_output(work_dir_path, &args, dependency)
                .await?;
        }

        // Use --no-verify for security reasons (baseline PR #23399).
        self.git
            .combined_output(
                work_dir_path,
                &[
                    "git".to_owned(),
                    "commit".to_owned(),
                    "--no-verify".to_owned(),
                    "-m".to_owned(),
                    dependency.versioned_package_syntax(),
                    "--date".to_owned(),
                    STABLE_GIT_COMMIT_DATE.to_owned(),
                ],
                dependency,
            )
            .await?;

        self.git
            .combined_output(
                work_dir_path,
                &[
                    "git".to_owned(),
                    "tag".to_owned(),
                    "-m".to_owned(),
                    dependency.versioned_package_syntax(),
                    dependency.git_tag(),
                ],
                dependency,
            )
            .await?;

        self.git
            .combined_output(
                work_dir_path,
                &[
                    "git".to_owned(),
                    "remote".to_owned(),
                    "add".to_owned(),
                    "origin".to_owned(),
                    bare_git_directory.to_string_lossy().into_owned(),
                ],
                dependency,
            )
            .await?;

        self.git
            .combined_output(
                work_dir_path,
                &[
                    "git".to_owned(),
                    "push".to_owned(),
                    "--no-verify".to_owned(),
                    "--force".to_owned(),
                    "origin".to_owned(),
                    "--tags".to_owned(),
                ],
                dependency,
            )
            .await?;

        Ok(())
    }
}

/// `isPotentiallyMaliciousFilepathInArchive`.
pub fn is_potentially_malicious_filepath_in_archive(path: &str, destination: &str) -> bool {
    if path.ends_with('/') || path.starts_with('/') {
        return true;
    }
    if path.split(std::path::MAIN_SEPARATOR).any(|part| part == ".git") {
        return true;
    }
    let cleaned = path_clean(&Path::new(destination).join(path));
    !cleaned.starts_with(Path::new(destination))
}

/// Lexical `path.Join`/`path.Clean` equivalent.
fn path_clean(path: &Path) -> PathBuf {
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                cleaned.pop();
            }
            other => cleaned.push(other.as_os_str()),
        }
    }
    cleaned
}

/// `stripSingleOutermostDirectory`.
pub fn strip_single_outermost_directory(dir: &Path) -> Result<(), String> {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|err| err.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;
    if entries.len() != 1 || !entries[0].file_type().map_err(|err| err.to_string())?.is_dir() {
        return Ok(());
    }
    let outermost = entries[0].file_name();
    let tmp = PathBuf::from(format!("{}.tmp", dir.display()));
    std::fs::rename(dir, &tmp).map_err(|err| err.to_string())?;
    std::fs::rename(tmp.join(&outermost), dir).map_err(|err| err.to_string())?;
    Ok(())
}

const ARCHIVE_SIZE_LIMIT: u64 = 15 * 1024 * 1024;

/// `unpack.Opts{SkipInvalid: true, SkipDuplicates: true}` plus the package
/// filter: skip entries at/over 15 MiB and potentially malicious paths.
pub fn unpack_tgz(contents: &[u8], destination: &Path) -> Result<(), String> {
    // `unpack.Tgz` treats a non-gzip stream as a plain tar archive.
    if contents.len() >= 2 && contents[0] == 0x1f && contents[1] == 0x8b {
        let decoder = flate2::read::GzDecoder::new(contents);
        unpack_tar(decoder, destination)
    } else {
        unpack_tar(contents, destination)
    }
}

/// `unpack.Tar` with the package filter, `SkipInvalid` and `SkipDuplicates`.
pub fn unpack_tar(reader: impl std::io::Read, destination: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(destination).map_err(|err| err.to_string())?;
    let mut archive = tar::Archive::new(reader);
    for entry in archive.entries().map_err(|err| err.to_string())? {
        let mut entry = entry.map_err(|err| err.to_string())?;
        let size = entry.header().size().map_err(|err| err.to_string())? as u64;
        let name = String::from_utf8_lossy(&entry.path_bytes()).into_owned();
        if size >= ARCHIVE_SIZE_LIMIT
            || is_potentially_malicious_filepath_in_archive(&name, &destination.to_string_lossy())
        {
            continue;
        }
        let Some(target) = sanitize_archive_path(&name, destination) else {
            continue;
        };
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            let mode = entry.header().mode().unwrap_or(0o755) | 0o700;
            if std::fs::create_dir_all(&target).is_ok() {
                let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode));
            }
            continue;
        }
        if let Some(parent) = target.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match entry_type {
            tar::EntryType::Regular | tar::EntryType::Continuous | tar::EntryType::GNUSparse => {
                let mode = entry.header().mode().unwrap_or(0o644) | 0o600;
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&target)
                {
                    Ok(mut file) => {
                        std::io::copy(&mut entry, &mut file).map_err(|err| err.to_string())?;
                        let _ =
                            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode));
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(err) => return Err(err.to_string()),
                }
            }
            tar::EntryType::Symlink => {
                if let Ok(Some(link)) = entry.link_name() {
                    let _ = std::os::unix::fs::symlink(link, &target);
                }
            }
            _ => continue,
        }
    }
    Ok(())
}

/// `unpack.Zip` with the package filter.
pub fn unpack_zip(contents: &[u8], destination: &Path) -> Result<(), String> {
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(destination).map_err(|err| err.to_string())?;
    let cursor = std::io::Cursor::new(contents);
    let mut archive = zip::ZipArchive::new(cursor).map_err(|err| err.to_string())?;
    for index in 0..archive.len() {
        let mut file = archive.by_index(index).map_err(|err| err.to_string())?;
        let name = file.name().to_owned();
        if file.size() >= ARCHIVE_SIZE_LIMIT
            || is_potentially_malicious_filepath_in_archive(&name, &destination.to_string_lossy())
        {
            continue;
        }
        let Some(target) = sanitize_archive_path(&name, destination) else {
            continue;
        };
        if file.is_dir() {
            let mode = file.unix_mode().unwrap_or(0o755) | 0o700;
            if std::fs::create_dir_all(&target).is_ok() {
                let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode));
            }
            continue;
        }
        if let Some(parent) = target.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mode = file.unix_mode().unwrap_or(0o644) | 0o600;
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
        {
            Ok(mut out) => {
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).map_err(|err| err.to_string())?;
                std::io::Write::write_all(&mut out, &buffer).map_err(|err| err.to_string())?;
                let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(mode));
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(())
}

/// `sanitizeTarPath`/`sanitizeZipPath`: reject paths that escape `dir`.
fn sanitize_archive_path(name: &str, dir: &Path) -> Option<PathBuf> {
    let path = Path::new(name);
    if path.is_absolute() {
        return None;
    }
    let target = path_clean(&dir.join(path));
    if target.starts_with(dir) {
        Some(target)
    } else {
        None
    }
}

/// `decompressTgz` (npm): unpack then strip the tarball's package directory.
pub fn decompress_tgz(contents: &[u8], destination: &Path) -> Result<(), String> {
    unpack_tgz(contents, destination)?;
    strip_single_outermost_directory(destination)
}

/// `unpackRustPackage`.
pub fn unpack_rust_package(contents: &[u8], work_dir: &Path) -> Result<(), String> {
    unpack_tgz(contents, work_dir)?;
    strip_single_outermost_directory(work_dir)
}

/// `unpackPythonPackage`: dispatch on the package URL's file extension.
pub fn unpack_python_package(
    contents: &[u8],
    package_url: &str,
    work_dir: &Path,
) -> Result<(), String> {
    // `path.Base(u.Path)` without pulling in the URL crate.
    let path_part = package_url
        .split(['?', '#'])
        .next()
        .unwrap_or(package_url)
        .trim_end_matches('/');
    let filename = path_part.rsplit('/').next().unwrap_or_default().to_owned();
    if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
        unpack_tgz(contents, work_dir)?;
    } else if filename.ends_with(".whl") || filename.ends_with(".zip") {
        unpack_zip(contents, work_dir)?;
    } else if filename.ends_with(".tar") {
        unpack_tar(contents, work_dir)?;
    } else {
        return Err(format!("unsupported python package type {filename:?}"));
    }
    strip_single_outermost_directory(work_dir)
}

/// `unpackRubyPackage`: the outer tar holds `data.tar.gz` and `metadata.gz`.
pub fn unpack_ruby_package(contents: &[u8], work_dir: &Path) -> Result<(), String> {
    let mut data_tar_gz: Option<Vec<u8>> = None;
    let mut metadata_gz: Option<Vec<u8>> = None;
    let mut archive = tar::Archive::new(contents);
    for entry in archive.entries().map_err(|err| err.to_string())? {
        let mut entry = entry.map_err(|err| err.to_string())?;
        let name = String::from_utf8_lossy(&entry.path_bytes()).into_owned();
        if name != "data.tar.gz" && name != "metadata.gz" {
            continue;
        }
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut bytes).map_err(|err| err.to_string())?;
        if name == "data.tar.gz" {
            data_tar_gz = Some(bytes);
        } else {
            metadata_gz = Some(bytes);
        }
    }
    let data = data_tar_gz.ok_or_else(|| "failed to read data archive file \"data.tar.gz\"".to_owned())?;
    unpack_tgz(&data, work_dir)?;
    strip_single_outermost_directory(work_dir)?;

    let metadata = metadata_gz.ok_or_else(|| "failed to read metadata.gz".to_owned())?;
    let mut metadata_reader = flate2::read::GzDecoder::new(metadata.as_slice());
    let mut metadata_bytes = Vec::new();
    std::io::Read::read_to_end(&mut metadata_reader, &mut metadata_bytes)
        .map_err(|err| err.to_string())?;
    std::fs::write(work_dir.join("rubygems-metadata.yml"), metadata_bytes)
        .map_err(|err| err.to_string())
}

/// `perforce.DecomposePerforceRemoteURL`.
pub fn decompose_perforce_remote_url(remote: &VcsUrl) -> Result<(String, String, String, String), String> {
    if remote.scheme() != "perforce" {
        return Err("scheme is not \"perforce\"".to_owned());
    }
    Ok((
        remote.username().to_owned(),
        remote.password().unwrap_or_default().to_owned(),
        remote.host().to_owned(),
        remote.path().to_owned(),
    ))
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
        clone_repo(
            &git_dir,
            &remote,
            &TlsConfig::default(),
            &CustomFetch::default(),
            &mut on_progress,
        )
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
    async fn runs_a_custom_fetch_command() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let origin = source_repo(tmp.path());
        let git_dir = tmp.path().join("clone-custom").join(".git");
        let marker = tmp.path().join("custom-fetch-ran");
        let script = tmp.path().join("custom-fetch.sh");
        // The mapping replaces `git fetch` entirely; the baseline does not run
        // `ConfigureRemoteGitCommand` for it, so `GIT_ASKPASS` is not set.
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nset -e\nif [ -n \"${{GIT_ASKPASS:-}}\" ]; then echo leaked >&2; exit 1; fi\n\
                 touch {}\nexec git fetch --prune \"$1\" '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*'\n",
                marker.display()
            ),
        )
        .expect("write script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).expect("chmod");

        let remote = VcsUrl::parse(origin.to_str().expect("utf8")).expect("parse");
        let mappings = vec![sg_config::CustomFetchMapping {
            domain_path: origin.to_str().expect("utf8").to_owned(),
            fetch: vec![
                script.to_str().expect("utf8").to_owned(),
                remote.to_string(),
            ],
        }];
        let custom = CustomFetch::build(Some(&mappings), true);
        let mut progress = Vec::new();
        let mut on_progress = |line: &[u8]| progress.extend_from_slice(line);
        clone_repo(
            &git_dir,
            &remote,
            &TlsConfig::default(),
            &custom,
            &mut on_progress,
        )
        .await
        .expect("clone");

        assert!(marker.exists(), "the custom command did not run");
        let head = git_output(&git_dir, &["symbolic-ref", "HEAD"]);
        assert!(head.status.success());
        assert_eq!(
            String::from_utf8_lossy(&head.stdout).trim(),
            "refs/heads/trunk"
        );
        let progress = String::from_utf8_lossy(&progress);
        assert!(progress.contains("Fetching remote contents\n"));
    }

    #[test]
    fn configures_remote_commands_like_the_baseline() {
        // Credentials move from the argument list into the environment.
        let remote = VcsUrl::parse("https://user:password@host.xz/org/repo.git").expect("parse");
        let mut args = vec!["git".to_owned(), "fetch".to_owned(), remote.to_string()];
        let mut env = Vec::new();
        configure_remote_command(&mut args, &mut env, &remote, &TlsConfig::default());
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
        configure_remote_command(&mut args, &mut env, &remote, &TlsConfig::default());
        assert!(!args.iter().any(|arg| arg == "protocol.version=2"));

        // SSH URLs keep their credentials out of the helper machinery.
        let ssh = VcsUrl::parse("ssh://user:password@host.xz/org/repo.git").expect("parse");
        let mut args = vec!["git".to_owned(), "fetch".to_owned(), ssh.to_string()];
        let mut env = Vec::new();
        configure_remote_command(&mut args, &mut env, &ssh, &TlsConfig::default());
        assert!(args.contains(&ssh.to_string()));
        assert!(!env.iter().any(|(key, _)| key.starts_with("GIT_SG_")));

        // No password: the URL is left alone and no helper is added.
        let plain = VcsUrl::parse("https://host.xz/org/repo.git").expect("parse");
        let mut args = vec!["git".to_owned(), "fetch".to_owned(), plain.to_string()];
        let mut env = Vec::new();
        configure_remote_command(&mut args, &mut env, &plain, &TlsConfig::default());
        assert!(args.contains(&plain.to_string()));
        assert!(!env.iter().any(|(key, _)| key.starts_with("GIT_SG_")));

        // `tls.external` settings become git environment variables.
        let tls = TlsConfig {
            ssl_no_verify: true,
            ssl_ca_info: Some(std::path::PathBuf::from("/tmp/ca.pem")),
        };
        let mut args = vec!["git".to_owned(), "fetch".to_owned(), plain.to_string()];
        let mut env = Vec::new();
        configure_remote_command(&mut args, &mut env, &plain, &tls);
        assert!(env.contains(&("GIT_SSL_NO_VERIFY".to_owned(), "true".to_owned())));
        assert!(env.contains(&("GIT_SSL_CAINFO".to_owned(), "/tmp/ca.pem".to_owned())));
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
        clone_repo(
            &git_dir,
            &remote,
            &TlsConfig::default(),
            &CustomFetch::default(),
            &mut on_progress,
        )
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
        let err = clone_repo(
            &git_dir,
            &remote,
            &TlsConfig::default(),
            &CustomFetch::default(),
            &mut on_progress,
        )
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

    // -----------------------------------------------------------------------
    // Package syncers (`packages_syncer.go`)
    // -----------------------------------------------------------------------

    #[test]
    fn compares_versions_like_reposource() {
        assert!(version_greater_than("11.0x", "11.0a"));
        // The baseline returns `len(version1) < len(version2)` after exhausting
        // the common prefix, so the shorter version compares greater.
        assert!(!version_greater_than("11.0a", "11.0"));
        assert!(version_greater_than("11.0", "11.0a"));
        assert!(version_greater_than("11.0", "8.0"));
        assert!(version_greater_than("1.10", "1.9"));
        assert!(!version_greater_than("1.9", "1.10"));
        assert!(!version_greater_than("1.0", "1.0"));
        // Prefix ordering: the shorter version is "greater" here, matching the
        // baseline's `len(version1) < len(version2)` return.
        assert!(!version_greater_than("1.0.1", "1.0"));
        assert!(version_greater_than("1.0", "1.0.1"));
    }

    #[test]
    fn builds_git_tags_per_ecosystem() {
        let cases = [
            (PackageKind::Npm, "pkg", "1.2.3", "v1.2.3"),
            (PackageKind::Go, "example.com/m", "v1.2.3", "v1.2.3"),
            (PackageKind::Python, "requests", "v2.0", "v2.0"),
            (PackageKind::Rust, "serde", "1.0", "v1.0"),
            (PackageKind::Ruby, "rails", "7", "v7"),
        ];
        for (kind, package, version, tag) in cases {
            let dependency = VersionedPackage {
                kind,
                package: package.to_owned(),
                version: version.to_owned(),
            };
            assert_eq!(dependency.git_tag(), tag, "{kind:?}");
        }
    }

    #[test]
    fn parses_versioned_packages_and_repo_names() {
        let npm = VersionedPackage::from_configuration(PackageKind::Npm, "@scope/pkg@1.2.3")
            .expect("npm");
        assert_eq!(npm.package, "@scope/pkg");
        assert_eq!(npm.version, "1.2.3");
        assert_eq!(npm.versioned_package_syntax(), "@scope/pkg@1.2.3");
        assert_eq!(npm.git_tag(), "v1.2.3");

        let go = VersionedPackage::from_configuration(PackageKind::Go, "example.com/m@v1.2.3")
            .expect("go");
        assert_eq!(go.versioned_package_syntax(), "example.com/m@v1.2.3");

        let python =
            VersionedPackage::from_configuration(PackageKind::Python, "requests==2.0").expect("py");
        assert_eq!(python.versioned_package_syntax(), "requests==2.0");

        let rust =
            VersionedPackage::from_configuration(PackageKind::Rust, "serde@1.0").expect("rust");
        assert_eq!(rust.versioned_package_syntax(), "serde@1.0");

        let ruby =
            VersionedPackage::from_configuration(PackageKind::Ruby, "rails@7").expect("ruby");
        assert_eq!(ruby.package, "rails");

        let source = ParsingPackageSource {
            kind: PackageKind::Npm,
        };
        assert_eq!(
            source
                .parse_package_from_repo_name("npm/scope/pkg")
                .expect("repo name"),
            "@scope/pkg"
        );
    }

    struct FakeGit {
        calls: std::sync::Mutex<Vec<(PathBuf, Vec<String>)>>,
        tags: std::sync::Mutex<String>,
    }

    #[async_trait::async_trait]
    impl PackageGitRunner for FakeGit {
        async fn combined_output(
            &self,
            dir: &Path,
            args: &[String],
            _dependency: &VersionedPackage,
        ) -> Result<String, String> {
            self.calls
                .lock()
                .unwrap()
                .push((dir.to_path_buf(), args.to_vec()));
            if args.len() == 2 && args[0] == "git" && args[1] == "tag" {
                return Ok(self.tags.lock().unwrap().clone());
            }
            Ok(String::new())
        }
    }

    struct FakeSource {
        downloads: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl PackageSource for FakeSource {
        async fn download(
            &self,
            dir: &Path,
            dependency: &VersionedPackage,
        ) -> Result<(), String> {
            self.downloads
                .lock()
                .unwrap()
                .push(dependency.versioned_package_syntax());
            std::fs::write(dir.join("file.txt"), b"contents").map_err(|err| err.to_string())
        }
        fn parse_versioned_package_from_name_and_version(
            &self,
            name: &str,
            version: &str,
        ) -> Result<VersionedPackage, String> {
            VersionedPackage::from_name_and_version(PackageKind::Npm, name, version)
        }
        fn parse_versioned_package_from_configuration(
            &self,
            dependency: &str,
        ) -> Result<VersionedPackage, String> {
            VersionedPackage::from_configuration(PackageKind::Npm, dependency)
        }
        fn parse_package_from_repo_name(&self, repo_name: &str) -> Result<String, String> {
            Ok(repo_name.trim_start_matches("npm/").to_owned())
        }
    }

    #[tokio::test]
    async fn fetches_versions_in_descending_order_and_sets_latest() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let git = Arc::new(FakeGit {
            calls: std::sync::Mutex::new(Vec::new()),
            tags: std::sync::Mutex::new(String::new()),
        });
        let source = Arc::new(FakeSource {
            downloads: std::sync::Mutex::new(Vec::new()),
        });
        let mut syncer = PackageSyncer::new(
            PackageKind::Npm,
            VersionedPackage::from_configuration(PackageKind::Npm, "@sourcegraph/placeholder@1.0.0")
                .unwrap(),
            Vec::new(),
            source.clone(),
            Arc::new(EmptyPackageVersionLister),
            git.clone(),
        );
        syncer.temp_root = Some(tmp.path().to_path_buf());

        let dir = tmp.path().join("bare");
        std::fs::create_dir_all(&dir).unwrap();
        syncer
            .fetch_versions(
                &dir,
                "pkg",
                vec!["1.2.3".to_owned(), "1.10.0".to_owned(), "2.0.0".to_owned()],
            )
            .await
            .expect("fetch versions");

        // 2.0.0 > 1.10.0 > 1.2.3: the newest is tagged last and wins `latest`.
        assert_eq!(
            *source.downloads.lock().unwrap(),
            vec!["pkg@2.0.0", "pkg@1.10.0", "pkg@1.2.3"]
        );
        let calls = git.calls.lock().unwrap();
        let latest = calls
            .iter()
            .find(|(_, args)| args.starts_with(&["git".to_owned(), "branch".to_owned()]))
            .expect("branch call");
        assert_eq!(
            latest.1,
            vec!["git", "branch", "--force", "latest", "v2.0.0"]
        );
        let push = calls
            .iter()
            .find(|(_, args)| args.starts_with(&["git".to_owned(), "push".to_owned()]))
            .expect("push call");
        assert!(push.1.contains(&"--tags".to_owned()));
        // Release the call log before the second run, which locks it again.
        drop(calls);

        // A second run sees the tags and does not download again.
        *git.tags.lock().unwrap() = "v1.2.3\nv1.10.0\nv2.0.0\n".to_owned();
        syncer
            .fetch_versions(
                &dir,
                "pkg",
                vec!["2.0.0".to_owned(), "1.10.0".to_owned(), "1.2.3".to_owned()],
            )
            .await
            .expect("second fetch");
        assert_eq!(source.downloads.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn fetch_reports_package_progress() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let git = Arc::new(FakeGit {
            calls: std::sync::Mutex::new(Vec::new()),
            tags: std::sync::Mutex::new(String::new()),
        });
        let source = Arc::new(FakeSource {
            downloads: std::sync::Mutex::new(Vec::new()),
        });
        let mut syncer = PackageSyncer::new(
            PackageKind::Npm,
            VersionedPackage::from_configuration(PackageKind::Npm, "@sourcegraph/placeholder@1.0.0")
                .unwrap(),
            vec!["pkg@1.0.0".to_owned(), "other@2.0.0".to_owned()],
            source,
            Arc::new(EmptyPackageVersionLister),
            git,
        );
        syncer.temp_root = Some(tmp.path().to_path_buf());
        let dir = tmp.path().join("bare");
        std::fs::create_dir_all(&dir).unwrap();
        let mut progress = Vec::new();
        let mut on_progress = |line: &[u8]| progress.extend_from_slice(line);
        syncer
            .fetch(&dir, "npm/pkg", &mut on_progress)
            .await
            .expect("fetch");
        assert_eq!(
            String::from_utf8_lossy(&progress),
            "Fetching package revisions\n"
        );
    }

    #[test]
    fn unpacks_archives_without_escaping_the_destination() {
        let tmp = tempfile::tempdir().expect("tempdir");

        // Build a tgz with one file plus a traversal attempt. The tar builder
        // rejects `..` paths, so write the raw header name for the malicious
        // entry (the reader/unpacker must still filter it).
        let raw_header = |name: &[u8], size: u64| {
            let mut header = tar::Header::new_gnu();
            header.as_mut_bytes()[..name.len()].copy_from_slice(name);
            header.set_size(size);
            header.set_mode(0o644);
            header.set_cksum();
            header
        };
        let mut tar_bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_bytes);
            builder
                .append(&raw_header(b"pkg/file.txt", 5), &b"hello"[..])
                .unwrap();
            builder
                .append(&raw_header(b"../escape.txt", 3), &b"bad"[..])
                .unwrap();
            builder.finish().unwrap();
        }
        let mut gz = Vec::new();
        {
            use std::io::Write;
            let mut encoder =
                flate2::write::GzEncoder::new(&mut gz, flate2::Compression::default());
            encoder.write_all(&tar_bytes).unwrap();
            encoder.finish().unwrap();
        }
        let out = tmp.path().join("out");
        decompress_tgz(&gz, &out).expect("unpack");
        // The single outermost `pkg/` directory is stripped.
        assert_eq!(std::fs::read(out.join("file.txt")).unwrap(), b"hello");
        assert!(!tmp.path().join("escape.txt").exists());

        assert!(is_potentially_malicious_filepath_in_archive(
            ".git/config",
            &out.to_string_lossy()
        ));
        assert!(is_potentially_malicious_filepath_in_archive(
            "/abs",
            &out.to_string_lossy()
        ));
        assert!(is_potentially_malicious_filepath_in_archive(
            "dir/",
            &out.to_string_lossy()
        ));
        assert!(!is_potentially_malicious_filepath_in_archive(
            "dir/file",
            &out.to_string_lossy()
        ));
    }

    #[test]
    fn decomposes_a_perforce_remote_url() {
        let remote =
            VcsUrl::parse("perforce://admin:password@ssl:10.0.0.1:1666//Sourcegraph/")
                .expect("parse");
        assert_eq!(
            decompose_perforce_remote_url(&remote).expect("decompose"),
            (
                "admin".to_owned(),
                "password".to_owned(),
                "ssl:10.0.0.1:1666".to_owned(),
                "//Sourcegraph/".to_owned()
            )
        );
        let https = VcsUrl::parse("https://example.com").unwrap();
        assert!(decompose_perforce_remote_url(&https).is_err());
    }
}
