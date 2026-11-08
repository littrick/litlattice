//! Port of `Server.CreateCommitFromPatch` (`cmd/gitserver/internal/patch.go`):
//! apply a patch against a base commit in a temporary non-bare repository,
//! commit it with the requested author/committer, copy the new objects back
//! into the target repository, optionally push, and create the target ref.
//!
//! Differences from the baseline (tracked in `docs/modules/02_gitserver.md`):
//! - The patch is buffered by the gRPC handler instead of being streamed into
//!   `git apply`'s stdin; the resulting error behavior is preserved: a failing
//!   `git apply` wins, and the reader error is only reported when the child
//!   exits successfully, exactly like `os/exec.Cmd.Wait`'s `skipStdinCopyError`
//!   handling.
//! - The Perforce shelving branch and SSH private-key pushes are not ported;
//!   they report an explicit error instead of silently doing the wrong thing.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sg_config::LiveConfig;
use sg_proto::gitserver as gpb;
use tokio::io::AsyncWriteExt;

use crate::fs;
use crate::repo::repo_dir;
use crate::tlsexternal::TlsConfig;
use crate::update::RemoteUrlSource;
use crate::vcssyncer;
use crate::vcsurl::{UrlRedactor, VcsUrl};

/// `protocol.CreateCommitFromPatchRequest` after `FromProto`.
pub struct CreateCommitFromPatchRequest {
    pub repo: String,
    pub base_commit: String,
    pub target_ref: String,
    pub commit_info: CommitInfo,
    pub push: Option<PushConfig>,
    pub push_ref: Option<String>,
    pub patch_filenames_no_prefix: bool,
}

/// `protocol.PatchCommitInfo`.
#[derive(Default)]
pub struct CommitInfo {
    pub messages: Vec<String>,
    pub author_name: String,
    pub author_email: String,
    pub committer_name: String,
    pub committer_email: String,
    pub date: Option<prost_types::Timestamp>,
}

/// `protocol.PushConfig`.
pub struct PushConfig {
    pub remote_url: String,
    pub private_key: String,
    pub passphrase: String,
}

impl CreateCommitFromPatchRequest {
    /// `CreateCommitFromPatchRequest.FromProto` (with the zero values of the
    /// getters for an absent `commit_info`).
    pub fn from_metadata(metadata: gpb::create_commit_from_patch_binary_request::Metadata) -> Self {
        let commit_info = metadata
            .commit_info
            .map_or_else(CommitInfo::default, |info| CommitInfo {
                messages: info.messages,
                author_name: info.author_name,
                author_email: info.author_email,
                committer_name: info.committer_name,
                committer_email: info.committer_email,
                date: info.date,
            });
        Self {
            repo: metadata.repo,
            base_commit: metadata.base_commit,
            target_ref: metadata.target_ref,
            commit_info,
            push: metadata.push.map(|push| PushConfig {
                remote_url: push.remote_url,
                private_key: push.private_key,
                passphrase: push.passphrase,
            }),
            push_ref: metadata.push_ref,
            patch_filenames_no_prefix: metadata.patch_filenames_no_prefix,
        }
    }
}

/// `protocol.CreateCommitFromPatchError`.
pub struct CreateCommitFromPatchError {
    pub repository_name: String,
    pub internal_error: String,
    pub command: String,
    pub combined_output: String,
}

impl CreateCommitFromPatchError {
    pub fn internal_error(&self) -> &str {
        &self.internal_error
    }
}

/// `protocol.CreateCommitFromPatchResponse`.
#[derive(Default)]
pub struct CreateCommitFromPatchResponse {
    pub rev: String,
    pub error: Option<CreateCommitFromPatchError>,
    pub changelist_id: String,
}

pub struct PatchService {
    pub repos_dir: Arc<PathBuf>,
    /// `getRemoteURLFunc`.
    remote_urls: Arc<dyn RemoteUrlSource>,
    live: Arc<LiveConfig>,
    /// `executil.tlsExternal` for `ConfigureRemoteGitCommand`, keyed by
    /// configuration generation so the CA bundle is written once per revision.
    tls_cache: Mutex<Option<(u64, TlsConfig)>>,
}

impl PatchService {
    pub fn new(
        repos_dir: PathBuf,
        remote_urls: Arc<dyn RemoteUrlSource>,
        live: Arc<LiveConfig>,
    ) -> Self {
        Self {
            repos_dir: Arc::new(repos_dir),
            remote_urls,
            live,
            tls_cache: Mutex::new(None),
        }
    }

    fn tls_config(&self) -> TlsConfig {
        let generation = self.live.generation();
        let mut cache = self.tls_cache.lock().unwrap_or_else(|err| err.into_inner());
        if let Some((cached_generation, tls)) = cache.as_ref() {
            if *cached_generation == generation {
                return tls.clone();
            }
        }
        let tls = TlsConfig::from_site(&self.live.current().site);
        *cache = Some((generation, tls.clone()));
        tls
    }

    /// `Server.CreateCommitFromPatch`.
    ///
    /// `patch_stream_error` is what the baseline's patch reader would return
    /// after `patch` (the bytes received before the error). The handler passes
    /// `Some` when the stream sent a non-patch message; the apply step then
    /// reports that error only if `git apply` itself succeeded, matching
    /// `os/exec.Cmd.Wait` (the exit error wins; the stdin copy error is
    /// returned only when the process otherwise exited normally).
    pub async fn create_commit_from_patch(
        &self,
        req: &CreateCommitFromPatchRequest,
        patch: &[u8],
        patch_stream_error: Option<String>,
    ) -> CreateCommitFromPatchResponse {
        let repo = req.repo.as_str();
        let mut response = CreateCommitFromPatchResponse::default();

        // `s.getRemoteURL(ctx, req.Repo)` or `vcs.ParseURL(req.Push.RemoteURL)`.
        let remote =
            if let Some(push) = req.push.as_ref().filter(|push| !push.remote_url.is_empty()) {
                match VcsUrl::parse(&push.remote_url) {
                    Ok(remote) => remote,
                    Err(err) => {
                        set_error(
                            &mut response,
                            repo,
                            None,
                            String::new(),
                            "",
                            format!("repoRemoteURL: {}", err.message()),
                        );
                        return response;
                    }
                }
            } else {
                // `s.getRemoteURL` → `GetRemoteURLFunc` (the raw lookup; no
                // `getting remote URL for %q` wrapper here, that belongs to the
                // `vcssyncer.RemoteURLSource` closure used by clones).
                match self.remote_urls.remote_url_func(repo).await {
                    Ok(raw) => match VcsUrl::parse(&raw) {
                        Ok(remote) => remote,
                        // `errors.Wrap(err, "repoRemoteURL")`.
                        Err(err) => {
                            set_error(
                                &mut response,
                                repo,
                                None,
                                String::new(),
                                "",
                                format!("repoRemoteURL: {}", err.message()),
                            );
                            return response;
                        }
                    },
                    // `s.getRemoteURL` wraps the source error with
                    // `GetRemoteURLFunc`; `patch.go` then wraps that with
                    // `repoRemoteURL`.
                    Err(err) => {
                        set_error(
                            &mut response,
                            repo,
                            None,
                            String::new(),
                            "",
                            format!("repoRemoteURL: GetRemoteURLFunc: {}", err.message()),
                        );
                        return response;
                    }
                }
            };

        // Gerrit pushes to a magic ref; otherwise a push updates `TargetRef`
        // with the `refs/heads/` prefix.
        let mut target_ref = req.target_ref.clone();
        if let Some(push_ref) = req
            .push_ref
            .as_deref()
            .filter(|push_ref| !push_ref.is_empty())
        {
            target_ref = push_ref.to_owned();
        }
        if req.push.is_some() && req.push_ref.is_none() {
            target_ref = ensure_ref_prefix(&target_ref);
        }

        let redactor = UrlRedactor::new(&remote);
        let tls = self.tls_config();

        // `s.fs.RepoDir(repo)`: unlike the baseline (which always returns a
        // path), a name that escapes the repos dir is rejected here.
        let Some(repo_git_dir) = repo_dir(&self.repos_dir, repo) else {
            set_error(
                &mut response,
                repo,
                Some(&redactor),
                String::new(),
                "",
                "gitserver: repository path escapes the repos dir".to_owned(),
            );
            return response;
        };

        let tmp_repo_dir = match fs::temp_dir(&self.repos_dir, "patch-repo-") {
            Ok(dir) => dir,
            Err(err) => {
                set_error(
                    &mut response,
                    repo,
                    Some(&redactor),
                    String::new(),
                    "",
                    format!("gitserver: make tmp repo: {err}"),
                );
                return response;
            }
        };

        let outcome = self
            .stage_and_commit(
                req,
                patch,
                patch_stream_error,
                &target_ref,
                repo,
                &remote,
                &tls,
                &redactor,
                &tmp_repo_dir,
                &repo_git_dir,
                &mut response,
            )
            .await;
        // `defer cleanUpTmpRepo`.
        if let Err(err) = std::fs::remove_dir_all(&tmp_repo_dir) {
            tracing::warn!(path = %tmp_repo_dir.display(), error = %err, "unable to clean up tmp repo");
        }
        let _ = outcome;
        response
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_and_commit(
        &self,
        req: &CreateCommitFromPatchRequest,
        patch: &[u8],
        patch_stream_error: Option<String>,
        target_ref: &str,
        repo: &str,
        remote: &VcsUrl,
        tls: &TlsConfig,
        redactor: &UrlRedactor,
        tmp_repo_dir: &Path,
        repo_git_dir: &Path,
        response: &mut CreateCommitFromPatchResponse,
    ) -> Result<(), ()> {
        let tmp_git_env = vec![format!("GIT_DIR={}", tmp_repo_dir.join(".git").display())];
        let tmp_objects_dir = tmp_repo_dir.join(".git").join("objects");
        let repo_objects_dir = repo_git_dir.join("objects");
        let alt_objects_env = vec![format!(
            "GIT_ALTERNATE_OBJECT_DIRECTORIES={}",
            repo_objects_dir.display()
        )];
        let git_dir_env = vec![format!("GIT_DIR={}", repo_git_dir.display())];

        let exec = Exec { redactor };
        let base_env: Vec<String> = tmp_git_env
            .iter()
            .chain(&alt_objects_env)
            .cloned()
            .collect();

        // 1. `git init` in the temporary directory.
        if let Err(failure) = exec
            .run(
                &["init"],
                tmp_repo_dir,
                &tmp_git_env,
                Stdin::None,
                false,
                remote,
                tls,
                "init tmp repo",
            )
            .await
        {
            record_failure(response, repo, redactor, failure);
            return Err(());
        }

        // 2. Base the staging repo on the requested commit, borrowing the
        // original repository's objects.
        if let Err(failure) = exec
            .run(
                &["reset", "-q", &req.base_commit],
                tmp_repo_dir,
                &base_env,
                Stdin::None,
                false,
                remote,
                tls,
                "basing staging on base rev",
            )
            .await
        {
            record_failure(response, repo, redactor, failure);
            return Err(());
        }

        // 3. Apply the patch to the index.
        let mut apply_args = vec!["apply", "--cached"];
        if req.patch_filenames_no_prefix {
            apply_args.push("-p0");
        }
        let apply = exec
            .run(
                &apply_args,
                tmp_repo_dir,
                &base_env,
                Stdin::Bytes(patch),
                false,
                remote,
                tls,
                "applying patch",
            )
            .await;
        match apply {
            // `os/exec.Cmd.Wait` only reports the stdin copy error when the
            // process "otherwise exited normally"; a failing `git apply` wins.
            Err(failure) => {
                record_failure(response, repo, redactor, failure);
                return Err(());
            }
            Ok(finished) => {
                if let Some(stream_error) = patch_stream_error {
                    response.error = Some(redacted_error(
                        repo,
                        redactor,
                        finished.command,
                        &String::from_utf8_lossy(&finished.output),
                        format!("gitserver: applying patch: {stream_error}"),
                    ));
                    return Err(());
                }
            }
        }

        // 4. Commit, with the baseline's defaults for missing fields.
        let mut messages = req.commit_info.messages.clone();
        if messages.is_empty() {
            messages = vec!["<Sourcegraph> Creating commit from patch".to_owned()];
        }
        let author_name = non_empty_or(&req.commit_info.author_name, "Sourcegraph");
        let author_email = non_empty_or(&req.commit_info.author_email, "support@sourcegraph.com");
        let committer_name = non_empty_or(&req.commit_info.committer_name, &author_name);
        let committer_email = non_empty_or(&req.commit_info.committer_email, &author_email);
        let date = go_time_string(req.commit_info.date.as_ref());

        let mut commit_env = base_env.clone();
        commit_env.extend([
            format!("GIT_COMMITTER_NAME={committer_name}"),
            format!("GIT_COMMITTER_EMAIL={committer_email}"),
            format!("GIT_AUTHOR_NAME={author_name}"),
            format!("GIT_AUTHOR_EMAIL={author_email}"),
            format!("GIT_COMMITTER_DATE={date}"),
            format!("GIT_AUTHOR_DATE={date}"),
        ]);
        // Commit messages can be arbitrary strings, so they are fed through
        // stdin; multiple messages are joined with a blank line, matching the
        // previous `-m` argument behavior.
        let commit_message = messages.join("\n\n");
        if let Err(failure) = exec
            .run(
                &["commit", "-F", "-"],
                tmp_repo_dir,
                &commit_env,
                Stdin::Bytes(commit_message.as_bytes()),
                false,
                remote,
                tls,
                "committing patch",
            )
            .await
        {
            record_failure(response, repo, redactor, failure);
            return Err(());
        }

        // 5. `git rev-parse HEAD` — `cmd.Output()`, so only stdout is
        // reported and no allowlist check happens.
        let output = match exec
            .output(&["rev-parse", "HEAD"], tmp_repo_dir, &base_env)
            .await
        {
            Ok(finished) => finished.output,
            Err(failure) => {
                record_failure(response, repo, redactor, failure);
                return Err(());
            }
        };
        let commit_hash = String::from_utf8_lossy(&output).trim().to_owned();

        // 6. Move the new objects into the target repository.
        if let Err(err) = move_objects(&tmp_objects_dir, &repo_objects_dir) {
            response.error = Some(redacted_error(
                repo,
                redactor,
                String::new(),
                "",
                format!("copying git objects: {err}"),
            ));
            return Err(());
        }

        // 7. Optional push.
        if let Some(push) = &req.push {
            if remote.scheme() == "perforce" {
                response.error = Some(redacted_error(
                    repo,
                    redactor,
                    String::new(),
                    "",
                    "gitserver: shelving a Perforce changelist is not implemented by the Rust gitserver yet"
                        .to_owned(),
                ));
                return Err(());
            }
            if remote.is_ssh() && !push.private_key.is_empty() && !push.passphrase.is_empty() {
                response.error = Some(redacted_error(
                    repo,
                    redactor,
                    String::new(),
                    "",
                    "gitserver: error creating ssh-agent: SSH private-key pushes are not implemented by the Rust gitserver yet"
                        .to_owned(),
                ));
                return Err(());
            }

            if let Err(failure) = exec
                .run(
                    &[
                        "push",
                        "--force",
                        &remote.to_string(),
                        &format!("{commit_hash}:{target_ref}"),
                    ],
                    repo_git_dir,
                    &git_dir_env,
                    Stdin::None,
                    true,
                    remote,
                    tls,
                    "pushing ref",
                )
                .await
            {
                record_failure(response, repo, redactor, failure);
                return Err(());
            }
        }

        response.rev = format!("refs/{}", target_ref.trim_start_matches("refs/"));

        // 8. Update the target ref unless the commit was pushed elsewhere.
        if req.push_ref.is_none() {
            if let Err(failure) = exec
                .run(
                    &["update-ref", "--", target_ref, &commit_hash],
                    repo_git_dir,
                    &git_dir_env,
                    Stdin::None,
                    false,
                    remote,
                    tls,
                    "creating ref",
                )
                .await
            {
                // The baseline logs this failure and still returns the
                // response, whose error makes the RPC fail.
                record_failure(response, repo, redactor, failure);
                return Err(());
            }
        }

        Ok(())
    }
}

/// Why a `git` invocation failed.
enum RunFailure {
    /// `gitcli.IsAllowedGitCmd` rejected the command. The baseline's `run`
    /// returns an error without calling `SetError`, and its caller returns the
    /// response, so the RPC succeeds with an empty `rev`.
    NotAllowed,
    Failed {
        command: String,
        combined: Vec<u8>,
        message: String,
    },
}

/// The successful outcome of a `git` invocation (`run` and `cmd.Output`).
struct Finished {
    /// The command line after `ConfigureRemoteGitCommand`, used for errors.
    command: String,
    /// Combined stdout+stderr (`run`) or stdout only (`cmd.Output`).
    output: Vec<u8>,
}

enum Stdin<'a> {
    None,
    Bytes(&'a [u8]),
}

struct Exec<'a> {
    redactor: &'a UrlRedactor,
}

impl Exec<'_> {
    /// `run`: execute `git`, capturing combined output.
    #[allow(clippy::too_many_arguments)]
    async fn run(
        &self,
        args: &[&str],
        dir: &Path,
        env: &[String],
        stdin: Stdin<'_>,
        is_remote: bool,
        remote: &VcsUrl,
        tls: &TlsConfig,
        reason: &str,
    ) -> Result<Finished, RunFailure> {
        let mut args: Vec<String> = std::iter::once("git".to_owned())
            .chain(args.iter().map(|arg| (*arg).to_owned()))
            .collect();
        if !is_allowed_git_cmd(&args[1..]) {
            return Err(RunFailure::NotAllowed);
        }

        let mut env: Vec<(String, String)> = env
            .iter()
            .map(|assignment| match assignment.split_once('=') {
                Some((key, value)) => (key.to_owned(), value.to_owned()),
                None => (assignment.clone(), String::new()),
            })
            .collect();
        if is_remote {
            vcssyncer::configure_remote_command(&mut args, &mut env, remote, tls);
        }
        let command = args.join(" ");

        let started = Instant::now();
        let mut command_process = tokio::process::Command::new(&args[0]);
        command_process
            .args(&args[1..])
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(match stdin {
                Stdin::None => Stdio::null(),
                Stdin::Bytes(_) => Stdio::piped(),
            });
        for (key, value) in &env {
            command_process.env(key, value);
        }

        let output = match stdin {
            Stdin::None => match command_process.output().await {
                Ok(output) => output,
                Err(err) => return Err(spawn_failure(command, reason, err)),
            },
            Stdin::Bytes(data) => {
                let mut child = match command_process.spawn() {
                    Ok(child) => child,
                    Err(err) => return Err(spawn_failure(command, reason, err)),
                };
                let write_result = match child.stdin.take() {
                    Some(mut child_stdin) => child_stdin.write_all(data).await,
                    None => Ok(()),
                };
                let output = match child.wait_with_output().await {
                    Ok(output) => output,
                    Err(err) => return Err(spawn_failure(command, reason, err)),
                };
                // `CombinedOutput` surfaces a stdin write failure; `io.Copy`
                // returning an error makes `Wait` return it.
                if output.status.success() {
                    if let Err(err) = write_result {
                        return Err(RunFailure::Failed {
                            command,
                            combined: Vec::new(),
                            message: format!("gitserver: {reason}: {err}"),
                        });
                    }
                }
                output
            }
        };

        let mut combined = output.stdout;
        combined.extend_from_slice(&output.stderr);
        tracing::info!(
            command = %self.redactor.redact(&command),
            duration = ?started.elapsed(),
            output = %String::from_utf8_lossy(&combined),
            "command ran"
        );

        if output.status.success() {
            return Ok(Finished {
                command,
                output: combined,
            });
        }
        Err(RunFailure::Failed {
            command,
            combined,
            message: format!(
                "gitserver: {reason}: {}",
                vcssyncer::exit_status(&output.status)
            ),
        })
    }

    /// The `cmd.Output()` variant used by `git rev-parse`: only stdout is
    /// reported, stderr is discarded, and there is no allowlist check.
    async fn output(
        &self,
        args: &[&str],
        dir: &Path,
        env: &[String],
    ) -> Result<Finished, RunFailure> {
        let command = std::iter::once("git")
            .chain(args.iter().copied())
            .collect::<Vec<_>>()
            .join(" ");

        let mut command_process = tokio::process::Command::new("git");
        command_process
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null());
        for assignment in env {
            if let Some((key, value)) = assignment.split_once('=') {
                command_process.env(key, value);
            }
        }
        let output = match command_process.output().await {
            Ok(output) => output,
            Err(err) => {
                return Err(RunFailure::Failed {
                    command,
                    combined: Vec::new(),
                    message: format!("gitserver: retrieving new commit id: {err}"),
                })
            }
        };
        if output.status.success() {
            return Ok(Finished {
                command,
                output: output.stdout,
            });
        }
        Err(RunFailure::Failed {
            command,
            combined: output.stdout,
            message: format!(
                "gitserver: retrieving new commit id: {}",
                vcssyncer::exit_status(&output.status)
            ),
        })
    }
}

fn spawn_failure(command: String, reason: &str, err: std::io::Error) -> RunFailure {
    RunFailure::Failed {
        command,
        combined: Vec::new(),
        message: format!("gitserver: {reason}: {err}"),
    }
}

/// `SetError` for a failed command; `NotAllowed` leaves the response alone.
fn record_failure(
    response: &mut CreateCommitFromPatchResponse,
    repo: &str,
    redactor: &UrlRedactor,
    failure: RunFailure,
) {
    if let RunFailure::Failed {
        command,
        combined,
        message,
    } = failure
    {
        response.error = Some(redacted_error(
            repo,
            redactor,
            command,
            &String::from_utf8_lossy(&combined),
            message,
        ));
    }
}

/// `CreateCommitFromPatchResponse.SetError`, redacting the URL.
fn redacted_error(
    repo: &str,
    redactor: &UrlRedactor,
    command: String,
    combined_output: &str,
    internal_error: String,
) -> CreateCommitFromPatchError {
    CreateCommitFromPatchError {
        repository_name: repo.to_owned(),
        internal_error: redactor.redact(&internal_error),
        command: redactor.redact(&command),
        combined_output: redactor.redact(combined_output),
    }
}

/// `SetError` before a redactor exists (remote URL resolution failures).
fn set_error(
    response: &mut CreateCommitFromPatchResponse,
    repo: &str,
    redactor: Option<&UrlRedactor>,
    command: String,
    combined_output: &str,
    internal_error: String,
) {
    response.error = Some(match redactor {
        Some(redactor) => redacted_error(repo, redactor, command, combined_output, internal_error),
        None => CreateCommitFromPatchError {
            repository_name: repo.to_owned(),
            internal_error,
            command,
            combined_output: combined_output.to_owned(),
        },
    });
}

fn non_empty_or(value: &str, default: &str) -> String {
    if value.is_empty() {
        default.to_owned()
    } else {
        value.to_owned()
    }
}

/// `ensureRefPrefix`.
fn ensure_ref_prefix(reference: &str) -> String {
    format!("refs/heads/{}", reference.trim_start_matches("refs/heads/"))
}

/// `filepath.Walk(tmpObjectsDir, ...)`: move every non-directory entry into the
/// target objects directory. Existing destination files have the same content
/// (content addressable), so a rename failure means the object is already
/// there.
fn move_objects(src: &Path, dst: &Path) -> std::io::Result<()> {
    let mut entries = Vec::new();
    collect_files(src, src, &mut entries)?;
    for (path, relative) in entries {
        let destination = dst.join(relative);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::rename(&path, &destination) {
            Ok(()) => {}
            // Go ignores `os.IsExist`; POSIX rename overwrites, so a failure
            // here usually means something else, but keep the relaxation.
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn collect_files(
    root: &Path,
    dir: &Path,
    out: &mut Vec<(PathBuf, PathBuf)>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_files(root, &path, out)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map(Path::to_path_buf)
                .unwrap_or_else(|_| path.clone());
            out.push((path, relative));
        }
    }
    Ok(())
}

/// `gitcli.IsAllowedGitCmd` for the commands this module runs. The user
/// controlled values are positional (base commit, refs, URL) and are rejected
/// when they would be parsed as flags.
fn is_allowed_git_cmd(args: &[String]) -> bool {
    let Some((command, rest)) = args.split_first() else {
        return false;
    };
    let allowed: &[&str] = match command.as_str() {
        "init" | "update-ref" => &[],
        "reset" => &["-q"],
        // `-F` is special-cased below (only `-F -` may read stdin).
        "commit" => &["-m"],
        "push" => &["--force"],
        "apply" => &["--cached", "-p0"],
        _ => return false,
    };

    let mut check_file_input = false;
    for arg in rest {
        if arg == "--" {
            break;
        }
        if check_file_input {
            if arg == "-" {
                check_file_input = false;
                continue;
            }
            return false;
        }
        if !arg.starts_with('-') {
            continue;
        }
        if (command == "log" || command == "show")
            && (arg.starts_with("-S") || arg.starts_with("-G"))
        {
            continue;
        }
        if command == "blame" && arg.starts_with("-L") {
            continue;
        }
        // `strconv.Atoi(arg[1:])`: numeric flags are allowed.
        if arg[1..].parse::<i64>().is_ok() {
            continue;
        }
        if command == "commit" {
            if arg == "--file=-" {
                continue;
            }
            if arg == "-F" {
                check_file_input = true;
                continue;
            }
        }
        let split_arg = arg.split('=').next().unwrap_or(arg);
        if split_arg == "--" || allowed.contains(&split_arg) {
            continue;
        }
        return false;
    }
    true
}

/// Go `fmt.Sprintf("%v", time.Time)`: `2006-01-02 15:04:05[.fffffffff] -0700 MST`.
/// `timestamppb`'s `AsTime()` yields UTC, and a nil timestamp is the Unix
/// epoch.
fn go_time_string(timestamp: Option<&prost_types::Timestamp>) -> String {
    let (seconds, nanos) = match timestamp {
        Some(timestamp) => (timestamp.seconds, timestamp.nanos.clamp(0, 999_999_999)),
        None => (0, 0),
    };
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    let fraction = if nanos == 0 {
        String::new()
    } else {
        let mut fraction = format!(".{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        fraction
    };
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}{fraction} +0000 UTC")
}

/// Howard Hinnant's `civil_from_days` (days since 1970-01-01).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_go_time_strings() {
        assert_eq!(go_time_string(None), "1970-01-01 00:00:00 +0000 UTC");
        let timestamp = prost_types::Timestamp {
            seconds: 1_789_548_132,
            nanos: 123_456_789,
        };
        assert_eq!(
            go_time_string(Some(&timestamp)),
            "2026-09-16 08:42:12.123456789 +0000 UTC"
        );
        let timestamp = prost_types::Timestamp {
            seconds: 1_789_548_132,
            nanos: 0,
        };
        assert_eq!(
            go_time_string(Some(&timestamp)),
            "2026-09-16 08:42:12 +0000 UTC"
        );
        let timestamp = prost_types::Timestamp {
            seconds: 1_789_548_132,
            nanos: 500_000_000,
        };
        assert_eq!(
            go_time_string(Some(&timestamp)),
            "2026-09-16 08:42:12.5 +0000 UTC"
        );
    }

    #[test]
    fn allowlist_matches_the_baseline_rules() {
        let argv = |args: &[&str]| args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
        assert!(is_allowed_git_cmd(&argv(&["init"])));
        assert!(is_allowed_git_cmd(&argv(&["reset", "-q", "HEAD"])));
        assert!(is_allowed_git_cmd(&argv(&["apply", "--cached", "-p0"])));
        assert!(is_allowed_git_cmd(&argv(&["commit", "-F", "-"])));
        assert!(is_allowed_git_cmd(&argv(&[
            "update-ref",
            "--",
            "refs/heads/x",
            "abc"
        ])));
        assert!(is_allowed_git_cmd(&argv(&[
            "push", "--force", "url", "a:b"
        ])));
        // A user-controlled revision that looks like a flag is rejected.
        assert!(!is_allowed_git_cmd(&argv(&["reset", "-q", "--hard"])));
        assert!(!is_allowed_git_cmd(&argv(&["commit", "-F", "/etc/passwd"])));
        assert!(!is_allowed_git_cmd(&argv(&["unknown-command"])));
    }
}
