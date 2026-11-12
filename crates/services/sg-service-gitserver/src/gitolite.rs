//! Port of `internal/extsvc/gitolite` and
//! `cmd/gitserver/internal/list_gitolite.go`: list the repositories of a
//! Gitolite server by running `ssh <host> info` and parsing its output.
//!
//! The client must run where a Gitolite-authorized SSH key is available
//! (gitserver). Non-zero exits are reported like the baseline's
//! `maybeUnauthorized`, which keeps the `exec.ExitError` text (`exit status
//! N`) and only tags permission failures as unauthorized — the tag has no
//! representation over gRPC, so only the text remains.

use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;

use crate::security;

/// `gitolite.Repo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitoliteRepo {
    /// Repository name as returned by `ssh git@GITOLITE_HOST info`.
    pub name: String,
    /// Clone URL.
    pub url: String,
}

/// `gitoliteRepoLister`: the injectable boundary the gRPC handler calls.
/// Production is [`GitoliteFetcher`]; tests inject fixed repository lists.
#[async_trait]
pub trait GitoliteRepoLister: Send + Sync {
    async fn list_repos(&self, host: &str) -> Result<Vec<GitoliteRepo>, String>;
}

/// The observable result of `exec.CommandContext(...).Output()`: stdout and,
/// when the command failed, `err.Error()`.
#[derive(Debug, Clone, Default)]
pub struct SshInfoOutput {
    /// Command stdout, including any partial output captured before a failure.
    pub stdout: Vec<u8>,
    /// `err.Error()` when the command could not start or exited non-zero.
    pub error: Option<String>,
}

/// Injectable command seam for `ssh <host> info`, so the gitolite path can be
/// exercised offline. Production is [`TokioSshInfoCommand`].
#[async_trait]
pub trait SshInfoCommand: Send + Sync {
    async fn run(&self, host: &str) -> SshInfoOutput;
}

/// Production runner: `exec.CommandContext(ctx, "ssh", host, "info")`.
pub struct TokioSshInfoCommand;

#[async_trait]
impl SshInfoCommand for TokioSshInfoCommand {
    async fn run(&self, host: &str) -> SshInfoOutput {
        let output = tokio::process::Command::new("ssh")
            .arg(host)
            .arg("info")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await;
        match output {
            Ok(output) => SshInfoOutput {
                stdout: output.stdout,
                error: (!output.status.success()).then(|| exit_status(&output.status)),
            },
            Err(err) => SshInfoOutput {
                stdout: Vec::new(),
                error: Some(err.to_string()),
            },
        }
    }
}

/// `gitoliteFetcher`: validate the host, then delegate to the injected
/// `gitoliteRepoLister` client. Production wraps [`SshGitoliteLister`].
pub struct GitoliteFetcher {
    client: Arc<dyn GitoliteRepoLister>,
}

impl GitoliteFetcher {
    pub fn new(client: Arc<dyn GitoliteRepoLister>) -> Self {
        Self { client }
    }

    /// The production fetcher (`gitoliteFetcher{client: gitoliteClient{}}`),
    /// with the real `ssh` command.
    pub fn with_default_command() -> Self {
        Self::new(Arc::new(SshGitoliteLister::new(Arc::new(
            TokioSshInfoCommand,
        ))))
    }
}

#[async_trait]
impl GitoliteRepoLister for GitoliteFetcher {
    async fn list_repos(&self, host: &str) -> Result<Vec<GitoliteRepo>, String> {
        // 🚨 SECURITY: a non-empty host must pass hostname validation.
        if !host.is_empty() && !security::validate_remote_addr(host).await {
            return Err("invalid gitolite host".to_owned());
        }
        self.client.list_repos(host).await
    }
}

/// `gitoliteClient`: run `ssh <host> info` through the injected command seam
/// and decode its output.
pub struct SshGitoliteLister {
    command: Arc<dyn SshInfoCommand>,
}

impl SshGitoliteLister {
    pub fn new(command: Arc<dyn SshInfoCommand>) -> Self {
        Self { command }
    }
}

#[async_trait]
impl GitoliteRepoLister for SshGitoliteLister {
    async fn list_repos(&self, host: &str) -> Result<Vec<GitoliteRepo>, String> {
        let output = self.command.run(host).await;
        if let Some(err) = output.error {
            // `gitolite.Client.ListRepos` logs the stdout as `out` and returns
            // `maybeUnauthorized(err)`. The unauthorized tag has no gRPC
            // representation (the handler only uses `err.Error()`).
            tracing::error!(
                error = %err,
                out = %String::from_utf8_lossy(&output.stdout),
                "listing gitolite failed"
            );
            return Err(err);
        }

        Ok(decode_repos(host, &String::from_utf8_lossy(&output.stdout)))
    }
}

/// Convenience wrapper around the production fetcher.
pub async fn list_repos(host: &str) -> Result<Vec<GitoliteRepo>, String> {
    GitoliteFetcher::with_default_command()
        .list_repos(host)
        .await
}

/// Go `exec.ExitError.Error()`.
fn exit_status(status: &std::process::ExitStatus) -> String {
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

/// `gitolite.decodeRepos`: every `R` line contributes a repository whose name
/// is the line's last field. The URL is `host + ":" + name` for SCP-style
/// hosts and an `ssh://` URL with the name as its path for URL hosts; hosts
/// `net/url` cannot parse are skipped entirely, as are URL hosts with a scheme
/// other than `ssh` (they keep an empty URL but are still listed).
pub fn decode_repos(host: &str, gitolite_info: &str) -> Vec<GitoliteRepo> {
    let class = classify_host(host);
    let mut repos = Vec::new();
    for line in gitolite_info.split('\n') {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 || fields[0] != "R" {
            continue;
        }
        let name = fields[fields.len() - 1];
        let url = match &class {
            // `url.Parse` failed: the baseline skips the whole line.
            HostClass::Invalid => continue,
            HostClass::Path => format!("{host}:{name}"),
            HostClass::Ssh => ssh_url_with_path(host, name),
            HostClass::OtherScheme => String::new(),
        };
        repos.push(GitoliteRepo {
            name: name.to_owned(),
            url,
        });
    }
    repos
}

enum HostClass {
    /// No scheme and no colon before the first slash: `url.Parse` returns a
    /// path.
    Path,
    Ssh,
    /// A parseable non-ssh scheme; the baseline leaves `Repo.URL` empty.
    OtherScheme,
    /// `url.Parse` returns an error (first path segment contains a colon).
    Invalid,
}

fn classify_host(host: &str) -> HostClass {
    if let Some(scheme) = scheme_prefix(host) {
        return if scheme.eq_ignore_ascii_case("ssh") {
            HostClass::Ssh
        } else {
            HostClass::OtherScheme
        };
    }
    // No scheme: `url.Parse` rejects a relative URL whose first path segment
    // contains a colon ("first path segment in URL cannot contain colon").
    let first_segment = host.split('/').next().unwrap_or_default();
    if first_segment.contains(':') {
        HostClass::Invalid
    } else {
        HostClass::Path
    }
}

/// Go `net/url.getScheme`: a scheme is a leading letter followed by letters,
/// digits, `+`, `-` or `.`, terminated by `:`. Anything else (including a
/// leading digit or an invalid character such as `@`) means there is no
/// scheme.
fn scheme_prefix(host: &str) -> Option<&str> {
    for (index, byte) in host.bytes().enumerate() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' if index > 0 => {}
            b':' => return (index > 0).then(|| &host[..index]),
            _ => return None,
        }
    }
    None
}

/// `URL.String()` after `u.Path = name` for an `ssh://` host: `url.Parse`
/// lowercases the scheme; keep the userinfo/port/query/fragment and replace
/// the path with the repository name. `URL.String` percent-encodes the path
/// (`encodePath`: everything but unreserved characters, `$&+,/:;=@`, and `/`).
fn ssh_url_with_path(host: &str, name: &str) -> String {
    let (scheme, rest) = host.split_once("://").unwrap_or(("", host));
    let cut = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..cut];
    let tail = &rest[cut..];
    let suffix = tail
        .find(['?', '#'])
        .map(|index| &tail[index..])
        .unwrap_or("");
    let path = if !name.is_empty() && !name.starts_with('/') {
        format!("/{name}")
    } else {
        name.to_owned()
    };
    format!(
        "{}://{}{}{}",
        scheme.to_ascii_lowercase(),
        authority,
        escape_path(&path),
        suffix
    )
}

/// `net/url`'s `encodePath` escaping (`escape(s, encodePath)`).
fn escape_path(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        let escape = if byte.is_ascii_alphanumeric() {
            false
        } else {
            match byte {
                b'-' | b'_' | b'.' | b'~' => false,
                b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@' => false,
                // `encodePath` escapes `?` and everything else.
                _ => true,
            }
        };
        if escape {
            out.push('%');
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 0xf)] as char);
        } else {
            out.push(byte as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const INFO: &str = "hello admin, this is git@gitolite-799486b5db-ghrxg running gitolite3 v3.6.6-0-g908f8c6 on git 2.7.4\n\n\t R W    gitolite-admin\n\t R W    repowith@sign\n\t R W    testing\n";

    #[test]
    fn decodes_scp_style_hosts() {
        assert_eq!(
            decode_repos("git@gitolite.example.com", INFO),
            vec![
                GitoliteRepo {
                    name: "gitolite-admin".to_owned(),
                    url: "git@gitolite.example.com:gitolite-admin".to_owned(),
                },
                GitoliteRepo {
                    name: "repowith@sign".to_owned(),
                    url: "git@gitolite.example.com:repowith@sign".to_owned(),
                },
                GitoliteRepo {
                    name: "testing".to_owned(),
                    url: "git@gitolite.example.com:testing".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn decodes_url_hosts() {
        assert_eq!(
            decode_repos("ssh://git@gitolite.example.com:2222/", INFO),
            vec![
                GitoliteRepo {
                    name: "gitolite-admin".to_owned(),
                    url: "ssh://git@gitolite.example.com:2222/gitolite-admin".to_owned(),
                },
                GitoliteRepo {
                    name: "repowith@sign".to_owned(),
                    url: "ssh://git@gitolite.example.com:2222/repowith@sign".to_owned(),
                },
                GitoliteRepo {
                    name: "testing".to_owned(),
                    url: "ssh://git@gitolite.example.com:2222/testing".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn escapes_the_url_path_like_go() {
        assert_eq!(
            decode_repos("ssh://git@gitolite.example.com:2222/", "R W repo?x"),
            vec![GitoliteRepo {
                name: "repo?x".to_owned(),
                url: "ssh://git@gitolite.example.com:2222/repo%3Fx".to_owned(),
            }]
        );
        assert_eq!(
            decode_repos(
                "ssh://git@gitolite.example.com:2222/",
                "R W repo#y\nR W pct%zz\nR W /leading"
            ),
            vec![
                GitoliteRepo {
                    name: "repo#y".to_owned(),
                    url: "ssh://git@gitolite.example.com:2222/repo%23y".to_owned(),
                },
                GitoliteRepo {
                    name: "pct%zz".to_owned(),
                    url: "ssh://git@gitolite.example.com:2222/pct%25zz".to_owned(),
                },
                GitoliteRepo {
                    name: "/leading".to_owned(),
                    url: "ssh://git@gitolite.example.com:2222/leading".to_owned(),
                },
            ]
        );
        // SCP-style hosts concatenate the name without escaping.
        assert_eq!(
            decode_repos("git@host", "R W repo?x"),
            vec![GitoliteRepo {
                name: "repo?x".to_owned(),
                url: "git@host:repo?x".to_owned(),
            }]
        );
    }

    #[test]
    fn lowercases_the_url_scheme() {
        assert_eq!(
            decode_repos("SSH://git@gitolite.example.com:2222/", "R W repo"),
            vec![GitoliteRepo {
                name: "repo".to_owned(),
                url: "ssh://git@gitolite.example.com:2222/repo".to_owned(),
            }]
        );
    }

    #[test]
    fn handles_empty_and_malformed_output() {
        assert!(decode_repos("git@gitolite.example.com", "").is_empty());
        assert!(decode_repos("git@gitolite.example.com", "hello\n  R\n  W name\n").is_empty());
        // An scp-style host with a port cannot be parsed by `net/url`.
        assert!(decode_repos("git@gitolite.example.com:2222", INFO).is_empty());
        // A leading digit is not a valid scheme, so the first path segment
        // contains a colon and `net/url` rejects the whole host.
        assert!(decode_repos("127.0.0.1:22", INFO).is_empty());
        // An alphabetic prefix is parsed as a scheme; it is not `ssh`, so the
        // repositories are listed with empty clone URLs.
        assert_eq!(
            decode_repos("git.example.com:2222", INFO),
            vec![
                GitoliteRepo {
                    name: "gitolite-admin".to_owned(),
                    url: String::new(),
                },
                GitoliteRepo {
                    name: "repowith@sign".to_owned(),
                    url: String::new(),
                },
                GitoliteRepo {
                    name: "testing".to_owned(),
                    url: String::new(),
                },
            ]
        );
    }

    struct FakeCommand {
        output: SshInfoOutput,
    }

    #[async_trait]
    impl SshInfoCommand for FakeCommand {
        async fn run(&self, _host: &str) -> SshInfoOutput {
            self.output.clone()
        }
    }

    fn lister(output: SshInfoOutput) -> SshGitoliteLister {
        SshGitoliteLister::new(Arc::new(FakeCommand { output }))
    }

    #[tokio::test]
    async fn fetch_decodes_injected_output() {
        let lister = lister(SshInfoOutput {
            stdout: INFO.as_bytes().to_vec(),
            error: None,
        });
        // An empty host skips `ValidateRemoteAddr` without a DNS lookup.
        let repos = lister.list_repos("").await.expect("list");
        assert_eq!(repos.len(), 3);
        assert_eq!(repos[0].name, "gitolite-admin");
    }

    #[tokio::test]
    async fn fetch_rejects_invalid_hosts_before_running_the_command() {
        struct PanicLister;
        #[async_trait]
        impl GitoliteRepoLister for PanicLister {
            async fn list_repos(&self, _host: &str) -> Result<Vec<GitoliteRepo>, String> {
                panic!("invalid hosts must not reach the client")
            }
        }
        let fetcher = GitoliteFetcher::new(Arc::new(PanicLister));
        // Fails validation before any DNS lookup: the username is not
        // alphanumeric and the port is not an integer.
        assert_eq!(
            fetcher.list_repos("git-user@127.0.0.1").await.unwrap_err(),
            "invalid gitolite host"
        );
        assert_eq!(
            fetcher.list_repos("127.0.0.1:ssh").await.unwrap_err(),
            "invalid gitolite host"
        );
    }

    #[tokio::test]
    async fn fetch_delegates_valid_hosts_to_the_client() {
        struct FixedLister;
        #[async_trait]
        impl GitoliteRepoLister for FixedLister {
            async fn list_repos(&self, host: &str) -> Result<Vec<GitoliteRepo>, String> {
                Ok(vec![GitoliteRepo {
                    name: "repo".to_owned(),
                    url: format!("{host}:repo"),
                }])
            }
        }
        let repos = GitoliteFetcher::new(Arc::new(FixedLister))
            .list_repos("127.0.0.1")
            .await
            .expect("list");
        assert_eq!(repos[0].url, "127.0.0.1:repo");
    }

    #[tokio::test]
    async fn fetch_reports_and_preserves_command_errors() {
        // `maybeUnauthorized` only tags permission errors; the RPC handler
        // reports the unchanged text.
        let lister = lister(SshInfoOutput {
            stdout: b"partial".to_vec(),
            error: Some("exit status 255".to_owned()),
        });
        assert_eq!(
            lister.list_repos("127.0.0.1").await.unwrap_err(),
            "exit status 255"
        );
    }
}
