#![forbid(unsafe_code)]

//! `internal/conf/reposource/other.go` + `common.go`: clone-URL-to-repo-name
//! mapping for the "other" code host.

use url::Url;

/// `reposource.DefaultRepositoryPathPattern`.
pub const DEFAULT_REPOSITORY_PATH_PATTERN: &str = "{base}/{repo}";

/// The parsed subset of a git clone URL used for name derivation. Go's
/// `parseCloneURL` supports both regular URLs and the SCP-style
/// `git@host:path` syntax; the latter is represented without a scheme.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCloneUrl {
    pub host: String,
    pub path: String,
}

fn is_non_scp_url(value: &str) -> bool {
    // `^(git\+)?(https?|ssh|rsync|file|git)://`
    let rest = value.strip_prefix("git+").unwrap_or(value);
    [
        "https://", "http://", "ssh://", "rsync://", "file://", "git://",
    ]
    .iter()
    .any(|prefix| rest.starts_with(prefix))
}

/// `parseCloneURL`.
pub fn parse_clone_url(clone_url: &str) -> Result<ParsedCloneUrl, String> {
    if is_non_scp_url(clone_url) {
        let url = Url::parse(clone_url).map_err(|err| format!("Error parsing cloneURL: {err}"))?;
        return Ok(ParsedCloneUrl {
            host: url.host_str().unwrap_or_default().to_owned(),
            path: url.path().to_owned(),
        });
    }

    // Support SCP-style syntax: replace only the first colon with a slash,
    // parse under a fake scheme, then strip the leading slash from the path.
    let replaced = clone_url.replacen(':', "/", 1);
    let url = Url::parse(&format!("fake://{replaced}"))
        .map_err(|err| format!("Error parsing cloneURL: {err}"))?;
    Ok(ParsedCloneUrl {
        host: url.host_str().unwrap_or_default().to_owned(),
        path: url
            .path()
            .strip_prefix('/')
            .unwrap_or(url.path())
            .to_owned(),
    })
}

/// `hostname`: the host without a port, with a leading `www.` removed.
fn hostname(host: &str) -> &str {
    host.strip_prefix("www.").unwrap_or(host)
}

/// The parsed base URL, normalized like `extsvc.NormalizeBaseURL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedBaseUrl {
    /// `url.Host`: hostname plus port, lowercased.
    pub host: String,
    /// `url.Hostname()`: no port, used for the host match.
    pub hostname: String,
    pub path: String,
}

/// `parseCloneURL` + `extsvc.NormalizeBaseURL` for the connection's `url`.
pub fn parse_base_url(base_url: &str) -> Result<ParsedBaseUrl, String> {
    let url = Url::parse(base_url).map_err(|err| format!("Error parsing baseURL: {err}"))?;
    let hostname = url.host_str().unwrap_or_default().to_owned();
    let mut host = hostname.clone();
    if let Some(port) = url.port() {
        host = format!("{host}:{port}");
    }
    // `NormalizeBaseURL` lowercases the whole `Host` (hostname and port) and
    // appends a trailing slash to the path.
    let host = host.to_lowercase();
    let mut path = url.path().to_owned();
    if !path.ends_with('/') {
        path.push('/');
    }
    Ok(ParsedBaseUrl {
        host,
        hostname,
        path,
    })
}

/// `cloneURLToRepoName`: returns `Ok("")` when the clone URL does not belong
/// to the configured base URL (Go's `(name, nil)` with `match == false`).
pub fn clone_url_to_repo_name(
    clone_url: &str,
    base_url: &str,
    repository_path_pattern: &str,
) -> Result<String, String> {
    let clone = parse_clone_url(clone_url)?;
    let base = if base_url.is_empty() {
        None
    } else {
        Some(parse_base_url(base_url)?)
    };
    let Some(base) = base else {
        return Ok(String::new());
    };
    if hostname(&base.hostname) != hostname(&clone.host) {
        return Ok(String::new());
    }

    // For SCP-style clone URLs the path may not start with a slash.
    let mut standardized_path = clone.path.clone();
    if base.path.starts_with('/') && !standardized_path.starts_with('/') {
        standardized_path.insert(0, '/');
    }
    let mut base_prefix = base.path.clone();
    if !base_prefix.ends_with('/') {
        base_prefix.push('/');
    }
    if standardized_path != base.path && !standardized_path.starts_with(&base_prefix) {
        return Ok(String::new());
    }
    let relative_repo_path = standardized_path
        .strip_prefix(&base_prefix)
        .unwrap_or(&standardized_path)
        .to_owned();

    Ok(other_repo_name(
        repository_path_pattern,
        &base_url_string(&base),
        &relative_repo_path,
    ))
}

/// `url.URL{Host, Path}.String()` with no scheme: `//host/path`.
fn base_url_string(base: &ParsedBaseUrl) -> String {
    format!("//{}{}", base.host, base.path)
}

/// `otherRepoNameReplacer`: `":" -> "-"`, `"@" -> "-"`, `"//" -> ""`.
fn replace_other_repo_name(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b':' | b'@' => {
                out.push('-');
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
            }
            _ => {
                let ch = value[index..].chars().next().expect("valid boundary");
                out.push(ch);
                index += ch.len_utf8();
            }
        }
    }
    out
}

/// `OtherRepoName`: substitutes `{base}` and `{repo}` in one pass.
pub fn other_repo_name(
    repository_path_pattern: &str,
    base: &str,
    relative_repo_path: &str,
) -> String {
    let pattern = if repository_path_pattern.is_empty() {
        DEFAULT_REPOSITORY_PATH_PATTERN
    } else {
        repository_path_pattern
    };
    let base = replace_other_repo_name(base.trim_end_matches('/'));
    let repo = relative_repo_path
        .strip_prefix('/')
        .unwrap_or(relative_repo_path);
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    let repo = repo.strip_suffix('/').unwrap_or(repo);
    let repo = replace_other_repo_name(repo);

    let mut out = String::with_capacity(pattern.len());
    let bytes = pattern.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if pattern[index..].starts_with("{base}") {
            out.push_str(&base);
            index += "{base}".len();
        } else if pattern[index..].starts_with("{repo}") {
            out.push_str(&repo);
            index += "{repo}".len();
        } else {
            let ch = pattern[index..].chars().next().expect("valid boundary");
            out.push(ch);
            index += ch.len_utf8();
        }
    }
    out
}

/// `reposource.Other.CloneURLToRepoName` and `CloneURLToRepoURI`.
#[derive(Debug, Clone)]
pub struct OtherRepoSource {
    pub url: String,
    pub repository_path_pattern: String,
}

impl OtherRepoSource {
    pub fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<String, String> {
        clone_url_to_repo_name(clone_url, &self.url, &self.repository_path_pattern)
    }

    /// The URI always uses the default pattern, like the baseline.
    pub fn clone_url_to_repo_uri(&self, clone_url: &str) -> Result<String, String> {
        clone_url_to_repo_name(clone_url, &self.url, DEFAULT_REPOSITORY_PATH_PATTERN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Case {
        url: &'static str,
        pattern: &'static str,
        clone_url: &'static str,
        want: &'static str,
    }

    const CASES: &[Case] = &[
        Case {
            url: "https://github.com",
            pattern: "{base}/{repo}",
            clone_url: "https://github.com/gorilla/mux",
            want: "github.com/gorilla/mux",
        },
        Case {
            url: "https://github.com",
            pattern: "{base}/{repo}",
            clone_url: "https://github.com/gorilla/mux.git",
            want: "github.com/gorilla/mux",
        },
        Case {
            url: "https://github.com",
            pattern: "{base}/{repo}",
            clone_url: "https://asdf.com/gorilla/mux.git",
            want: "",
        },
        Case {
            url: "https://github.com/?access_token=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            pattern: "{base}/{repo}",
            clone_url: "https://github.com/gorilla/mux",
            want: "github.com/gorilla/mux",
        },
        Case {
            url: "ssh://thaddeus@gerrit.com:12345",
            pattern: "{base}/{repo}",
            clone_url: "ssh://thaddeus@gerrit.com:12345/repos/repo1",
            want: "gerrit.com-12345/repos/repo1",
        },
        Case {
            url: "ssh://thaddeus@gerrit.com:12345",
            pattern: "prettyhost/{repo}",
            clone_url: "ssh://thaddeus@gerrit.com:12345/repos/repo1",
            want: "prettyhost/repos/repo1",
        },
        Case {
            url: "ssh://thaddeus@gerrit.com:12345/repos",
            pattern: "{repo}",
            clone_url: "ssh://thaddeus@gerrit.com:12345/repos/repo1",
            want: "repo1",
        },
        Case {
            url: "ssh://thaddeus@gerrit.com:12345/repos",
            pattern: "{repo}",
            clone_url: "ssh://thaddeus@asdf.com/repos/repo1",
            want: "",
        },
        Case {
            url: "ssh://thaddeus@gerrit.com:12345/repos",
            pattern: "{repo}",
            clone_url: "ssh://thaddeus@gerrit.com:12345/asdf/repo1",
            want: "",
        },
    ];

    #[test]
    fn matches_the_baseline_name_derivation() {
        for case in CASES {
            let source = OtherRepoSource {
                url: case.url.to_owned(),
                repository_path_pattern: case.pattern.to_owned(),
            };
            assert_eq!(
                source.clone_url_to_repo_name(case.clone_url).expect("name"),
                case.want,
                "url={} clone_url={}",
                case.url,
                case.clone_url
            );
        }
    }

    #[test]
    fn scp_style_clone_urls_are_parsed() {
        let parsed = parse_clone_url("git@codehost.com:a/b").expect("parse");
        assert_eq!(parsed.host, "codehost.com");
        assert_eq!(parsed.path, "a/b");
        let parsed = parse_clone_url("git@codehost.com:/a/b").expect("parse");
        assert_eq!(parsed.host, "codehost.com");
        assert_eq!(parsed.path, "/a/b");
    }

    #[test]
    fn empty_pattern_defaults_to_base_slash_repo() {
        assert_eq!(
            other_repo_name("", "//github.com/", "o/r"),
            "github.com/o/r"
        );
    }
}
