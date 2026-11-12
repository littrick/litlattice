#![forbid(unsafe_code)]

//! `internal/extsvc/gitolite` + the gitserver boundary the repo-updater
//! source consumes.
//!
//! The baseline source calls `gitserver.Client.ListGitoliteRepos`, which runs
//! `ssh <host> info` inside gitserver and parses the `R <perms> <name>` lines.
//! This port keeps the parsing and the `ExternalRepoSpec`/clone-URL helpers and
//! exposes [`GitoliteRepoLister`] as the injectable boundary. Production
//! repo-updater wires a gRPC client ([`GitoliteRepoLister`] backed by
//! `GitserverService.ListGitolite`); [`SshGitoliteCommand`] remains as the
//! offline fallback that runs the command in-process, and tests inject fixed
//! repository lists without touching the network.

use std::sync::Arc;

use async_trait::async_trait;

/// `extsvc.TypeGitolite`.
pub const TYPE_GITOLITE: &str = "gitolite";

/// `gitolite.Repo`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Repo {
    /// `Name`: the repository name as returned by `ssh git@HOST info`.
    #[serde(rename = "Name", default)]
    pub name: String,
    /// `URL`: the clone URL.
    #[serde(rename = "URL", default)]
    pub url: String,
}

/// `gitolite.ServiceID`.
pub fn service_id(host: &str) -> String {
    host.to_owned()
}

/// `gitolite.ExternalRepoSpec`.
pub fn external_repo_spec(repo: &Repo, service_id: &str) -> crate::ExternalRepoSpec {
    crate::ExternalRepoSpec {
        id: repo.name.clone(),
        service_type: TYPE_GITOLITE.to_owned(),
        service_id: service_id.to_owned(),
    }
}

/// `gitolite.CloneURL`.
pub fn clone_url(spec: &crate::ExternalRepoSpec) -> String {
    if spec.service_type != TYPE_GITOLITE {
        return String::new();
    }
    format!("{}:{}", spec.service_id, spec.id)
}

/// `gitserver.Client.ListGitoliteRepos`: the boundary the source consumes.
/// Production is a gRPC client; tests and the offline differential inject
/// fixed repository lists.
#[async_trait]
pub trait GitoliteRepoLister: Send + Sync {
    async fn list_repos(&self, host: &str) -> Result<Vec<Repo>, String>;
}

/// Offline fallback: run `ssh <host> info` in this process and parse it. The
/// baseline runs the same command inside gitserver; production repo-updater
/// reaches gitserver over gRPC instead (see the module docs).
pub struct SshGitoliteCommand;

#[async_trait]
impl GitoliteRepoLister for SshGitoliteCommand {
    async fn list_repos(&self, host: &str) -> Result<Vec<Repo>, String> {
        let output = tokio::process::Command::new("ssh")
            .arg(host)
            .arg("info")
            .output()
            .await
            .map_err(|err| err.to_string())?;
        if !output.status.success() {
            // `maybeUnauthorized` keeps the `exec.ExitError` text; the stderr
            // is only logged, so the error text stays `exit status N`.
            return Err(exit_status(&output.status));
        }
        Ok(decode_repos(host, &String::from_utf8_lossy(&output.stdout)))
    }
}

/// Convenience alias for the source constructor.
pub type CommandHandle = Arc<dyn GitoliteRepoLister>;

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
pub fn decode_repos(host: &str, gitolite_info: &str) -> Vec<Repo> {
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
        repos.push(Repo {
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
    let first_segment = host.split('/').next().unwrap_or_default();
    if first_segment.contains(':') {
        HostClass::Invalid
    } else {
        HostClass::Path
    }
}

/// Go `net/url.getScheme`.
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
/// lowercases the scheme; keep userinfo/port/query/fragment and replace the
/// path with the repository name. `URL.String` percent-encodes the path
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

/// `gitolite.maybeUnauthorized`: marks errors mentioning "permission denied".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitoliteError {
    pub message: String,
    pub unauthorized: bool,
}

impl GitoliteError {
    pub fn new(message: impl Into<String>) -> Self {
        let message = message.into();
        let unauthorized = message.contains("permission denied");
        Self {
            message,
            unauthorized,
        }
    }
}

impl std::fmt::Display for GitoliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GitoliteError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_scp_style_host() {
        let repos = decode_repos("git@gitolite.example.com", "R   W   repo1\nR   W   org/repo2\n");
        assert_eq!(
            repos,
            vec![
                Repo {
                    name: "repo1".to_owned(),
                    url: "git@gitolite.example.com:repo1".to_owned(),
                },
                Repo {
                    name: "org/repo2".to_owned(),
                    url: "git@gitolite.example.com:org/repo2".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn decode_ssh_url_host() {
        let repos = decode_repos("ssh://git@gitolite.example.com:2222/", "R W repo");
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].name, "repo");
        assert_eq!(repos[0].url, "ssh://git@gitolite.example.com:2222/repo");
    }

    #[test]
    fn decode_escapes_ssh_url_paths_like_go() {
        let repos = decode_repos("ssh://git@host:2222/", "R W repo?x\nR W repo#y");
        assert_eq!(
            repos,
            vec![
                Repo {
                    name: "repo?x".to_owned(),
                    url: "ssh://git@host:2222/repo%3Fx".to_owned(),
                },
                Repo {
                    name: "repo#y".to_owned(),
                    url: "ssh://git@host:2222/repo%23y".to_owned(),
                },
            ]
        );
        // SCP-style hosts concatenate the name without escaping.
        assert_eq!(
            decode_repos("git@host", "R W repo?x"),
            vec![Repo {
                name: "repo?x".to_owned(),
                url: "git@host:repo?x".to_owned(),
            }]
        );
    }

    #[test]
    fn decode_ignores_other_lines_and_uses_last_field() {
        let repos = decode_repos(
            "git@host",
            "@R_ @W_ repo name with space\n\nR W extra\nX W ignore\n",
        );
        // First line starts with "@R_", not "R"; the second is "R W extra".
        assert_eq!(
            repos,
            vec![Repo {
                name: "extra".to_owned(),
                url: "git@host:extra".to_owned(),
            }]
        );
    }

    #[test]
    fn clone_url_only_for_gitolite() {
        let spec = crate::ExternalRepoSpec {
            id: "repo".to_owned(),
            service_type: TYPE_GITOLITE.to_owned(),
            service_id: "git@host".to_owned(),
        };
        assert_eq!(clone_url(&spec), "git@host:repo");
        let other = crate::ExternalRepoSpec {
            service_type: "github".to_owned(),
            ..spec
        };
        assert_eq!(clone_url(&other), "");
    }

    #[test]
    fn unauthorized_marker() {
        assert!(GitoliteError::new("ssh: permission denied (publickey)").unauthorized);
        assert!(!GitoliteError::new("connection refused").unauthorized);
    }
}
