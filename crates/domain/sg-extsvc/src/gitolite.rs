#![forbid(unsafe_code)]

//! `internal/extsvc/gitolite`: repository discovery for a Gitolite host.
//!
//! The baseline `Client.ListRepos` runs `ssh <host> info` from gitserver and
//! parses the `R <perms> <name>` lines. This port keeps the parsing and the
//! `ExternalRepoSpec`/clone-URL helpers, and moves the `ssh` invocation behind
//! the injectable [`GitoliteCommand`] seam. The production [`SshGitoliteCommand`]
//! runs the same command from the repo-updater process (the baseline runs it
//! from gitserver); tests inject fixed output and never touch the network.

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

/// The injectable `ssh <host> info` seam.
#[async_trait]
pub trait GitoliteCommand: Send + Sync {
    /// Runs `ssh <host> info` and returns stdout.
    async fn list_repos(&self, host: &str) -> Result<String, String>;
}

/// The production command runner: `ssh <host> info` (baseline
/// `gitolite.Client.ListRepos`).
pub struct SshGitoliteCommand;

#[async_trait]
impl GitoliteCommand for SshGitoliteCommand {
    async fn list_repos(&self, host: &str) -> Result<String, String> {
        let output = tokio::process::Command::new("ssh")
            .arg(host)
            .arg("info")
            .output()
            .await
            .map_err(|err| err.to_string())?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return Err(format!("ssh {host} info: {stderr}"));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

/// Convenience alias for the source constructor.
pub type CommandHandle = Arc<dyn GitoliteCommand>;

/// `gitolite.decodeRepos`.
pub fn decode_repos(host: &str, gitolite_info: &str) -> Vec<Repo> {
    let mut repos = Vec::new();
    for line in gitolite_info.split('\n') {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 || fields[0] != "R" {
            continue;
        }
        let name = fields[fields.len() - 1].to_owned();
        let mut repo = Repo {
            name: name.clone(),
            url: String::new(),
        };

        // `url.Parse(host)`: an absolute URL only when it carries a scheme.
        // Go parses scheme-less strings as a path (Scheme == ""), which the
        // baseline treats as SCP-style and renders as `host:name`.
        let parsed = url::Url::parse(host);
        match parsed {
            Ok(parsed) => {
                if parsed.scheme().is_empty() {
                    repo.url = format!("{host}:{name}");
                } else if parsed.scheme() == "ssh" {
                    let mut parsed = parsed;
                    // `u.Path = name` (no leading slash; `URL.String` adds it).
                    parsed.set_path(&name);
                    repo.url = parsed.to_string();
                }
            }
            Err(_) if !host.contains("://") => {
                // Scheme-less host: Go's `url.Parse` succeeds with Scheme "".
                repo.url = format!("{host}:{name}");
            }
            Err(_) => continue,
        }

        repos.push(repo);
    }
    repos
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
