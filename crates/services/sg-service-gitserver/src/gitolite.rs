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

use crate::security;

/// `gitolite.Repo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitoliteRepo {
    /// Repository name as returned by `ssh git@GITOLITE_HOST info`.
    pub name: String,
    /// Clone URL.
    pub url: String,
}

/// `gitoliteFetcher.listRepos`: validate the host, then ask Gitolite for its
/// repository list.
pub async fn list_repos(host: &str) -> Result<Vec<GitoliteRepo>, String> {
    // 🚨 SECURITY: a non-empty host must pass hostname validation.
    if !host.is_empty() && !security::validate_remote_addr(host).await {
        return Err("invalid gitolite host".to_owned());
    }

    let output = tokio::process::Command::new("ssh")
        .arg(host)
        .arg("info")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|err| format!("{err}"))?;

    if !output.status.success() {
        // `exec.ExitError.Error()` is `exit status N` (or `signal: ...`); the
        // command's stderr is only logged by the baseline, not returned.
        return Err(exit_status(&output.status));
    }

    Ok(decode_repos(host, &String::from_utf8_lossy(&output.stdout)))
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
fn decode_repos(host: &str, gitolite_info: &str) -> Vec<GitoliteRepo> {
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

/// `URL.String()` after `u.Path = name` for an `ssh://` host: keep the
/// scheme, userinfo and port, and use the repository name as the path.
fn ssh_url_with_path(host: &str, name: &str) -> String {
    let scheme_end = host.find("://").map(|index| index + 3).unwrap_or(0);
    let authority_end = host[scheme_end..]
        .find(['/', '?', '#'])
        .map(|index| scheme_end + index)
        .unwrap_or(host.len());
    format!("{}/{name}", &host[..authority_end])
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
}
