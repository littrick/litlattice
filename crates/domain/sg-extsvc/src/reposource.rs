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
    /// `url.URL.Scheme`; the empty string for SCP-style clone URLs
    /// (`parseCloneURL` clears it after parsing under the fake scheme).
    pub scheme: String,
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
            scheme: url.scheme().to_owned(),
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
        scheme: String::new(),
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
    // `NormalizeBaseURL` lowercases `url.Host`, and `Hostname()` is derived
    // from it, so the hostname used for matching is lowercase too.
    let hostname = url.host_str().unwrap_or_default().to_lowercase();
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

/// `parseURLs`: parse the clone URL and the configured base URL, and report
/// whether their (www-stripped) hostnames match. `base_url == ""` disables
/// the match, like the baseline's nil `parsedBaseURL`.
pub fn parse_urls(
    clone_url: &str,
    base_url: &str,
) -> Result<(ParsedCloneUrl, Option<ParsedBaseUrl>, bool), String> {
    let base = if base_url.is_empty() {
        None
    } else {
        Some(parse_base_url(base_url)?)
    };
    let clone = parse_clone_url(clone_url)?;
    let equal_hosts = base
        .as_ref()
        .is_some_and(|base| hostname(&base.hostname) == hostname(&clone.host));
    Ok((clone, base, equal_hosts))
}

/// `strings.NewReplacer(pairs...).Replace(input)`: a single left-to-right pass
/// that tries the pairs in order at each byte position and never re-scans
/// inserted text.
pub fn replace_pattern(input: &str, pairs: &[(&str, &str)]) -> String {
    let mut out = String::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        let mut replaced = false;
        for (from, to) in pairs {
            if !from.is_empty() && input[index..].starts_with(from) {
                out.push_str(to);
                index += from.len();
                replaced = true;
                break;
            }
        }
        if !replaced {
            let ch = input[index..].chars().next().expect("valid boundary");
            out.push(ch);
            index += ch.len_utf8();
        }
    }
    out
}

/// `strings.TrimPrefix(strings.TrimSuffix(path, ".git"), "/")`.
pub fn trim_git_suffix_and_slash(path: &str) -> &str {
    let path = path.strip_suffix(".git").unwrap_or(path);
    path.strip_prefix('/').unwrap_or(path)
}

/// `reposource.GitHubRepoName`.
pub fn github_repo_name(
    repository_path_pattern: &str,
    host: &str,
    name_with_owner: &str,
) -> String {
    let pattern = if repository_path_pattern.is_empty() {
        "{host}/{nameWithOwner}"
    } else {
        repository_path_pattern
    };
    replace_pattern(
        pattern,
        &[("{host}", host), ("{nameWithOwner}", name_with_owner)],
    )
}

/// `reposource.NameTransformation` (regex replacement only).
#[derive(Debug, Clone)]
pub struct NameTransformation {
    pub regex: regex::Regex,
    pub replacement: String,
}

impl NameTransformation {
    /// `NewNameTransformation`: compiles the regex.
    pub fn new(regex: &str, replacement: &str) -> Result<Self, String> {
        let compiled =
            regex::Regex::new(regex).map_err(|err| format!("regexp.Compile {regex:?}: {err}"))?;
        Ok(Self {
            regex: compiled,
            replacement: replacement.to_owned(),
        })
    }

    /// `NameTransformation.Transform`.
    pub fn transform(&self, value: &str) -> String {
        self.regex
            .replace_all(value, self.replacement.as_str())
            .into_owned()
    }
}

/// `CompileGitLabNameTransformations`.
pub fn compile_name_transformations(
    transformations: &[(String, String)],
) -> Result<Vec<NameTransformation>, String> {
    transformations
        .iter()
        .map(|(regex, replacement)| NameTransformation::new(regex, replacement))
        .collect()
}

/// `reposource.GitLabRepoName`.
pub fn gitlab_repo_name(
    repository_path_pattern: &str,
    host: &str,
    path_with_namespace: &str,
    transformations: &[NameTransformation],
) -> String {
    let pattern = if repository_path_pattern.is_empty() {
        "{host}/{pathWithNamespace}"
    } else {
        repository_path_pattern
    };
    let name = replace_pattern(
        pattern,
        &[
            ("{host}", host),
            ("{pathWithNamespace}", path_with_namespace),
        ],
    );
    transformations.iter().fold(name, |value, transformation| {
        transformation.transform(&value)
    })
}

/// `reposource.BitbucketCloudRepoName`.
pub fn bitbucket_cloud_repo_name(
    repository_path_pattern: &str,
    host: &str,
    name_with_owner: &str,
) -> String {
    let pattern = if repository_path_pattern.is_empty() {
        "{host}/{nameWithOwner}"
    } else {
        repository_path_pattern
    };
    replace_pattern(
        pattern,
        &[("{host}", host), ("{nameWithOwner}", name_with_owner)],
    )
}

/// `reposource.BitbucketServerRepoName`.
pub fn bitbucket_server_repo_name(
    repository_path_pattern: &str,
    host: &str,
    project_key: &str,
    repo_slug: &str,
) -> String {
    let pattern = if repository_path_pattern.is_empty() {
        "{host}/{projectKey}/{repositorySlug}"
    } else {
        repository_path_pattern
    };
    replace_pattern(
        pattern,
        &[
            ("{host}", host),
            ("{projectKey}", project_key),
            ("{repositorySlug}", repo_slug),
        ],
    )
}

/// `reposource.GerritRepoName`.
pub fn gerrit_repo_name(repository_path_pattern: &str, host: &str, name: &str) -> String {
    let pattern = if repository_path_pattern.is_empty() {
        "{host}/{name}"
    } else {
        repository_path_pattern
    };
    replace_pattern(pattern, &[("{host}", host), ("{name}", name)])
}

/// `reposource.AWSRepoName`.
pub fn aws_repo_name(repository_path_pattern: &str, name: &str) -> String {
    let pattern = if repository_path_pattern.is_empty() {
        "{name}"
    } else {
        repository_path_pattern
    };
    replace_pattern(pattern, &[("{name}", name)])
}

/// `reposource.GitoliteRepoName`: `@` is not allowed in repo names and is
/// rewritten to `-`.
pub fn gitolite_repo_name(prefix: &str, gitolite_name: &str) -> String {
    let name = gitolite_name.replace('@', "-");
    format!("{prefix}{name}")
}

/// `reposource.GitHub.CloneURLToRepoName`.
#[derive(Debug, Clone)]
pub struct GitHubRepoSource {
    pub url: String,
    pub repository_path_pattern: String,
}

impl GitHubRepoSource {
    pub fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<String, String> {
        let (clone, base, matched) = parse_urls(clone_url, &self.url)?;
        let Some(base) = base else {
            return Ok(String::new());
        };
        if !matched {
            return Ok(String::new());
        }
        Ok(github_repo_name(
            &self.repository_path_pattern,
            &base.hostname,
            trim_git_suffix_and_slash(&clone.path),
        ))
    }
}

/// `reposource.GitLab.CloneURLToRepoName`.
#[derive(Debug, Clone)]
pub struct GitLabRepoSource {
    pub url: String,
    pub repository_path_pattern: String,
    /// Raw `nameTransformations` (`regex`, `replacement`), compiled only when
    /// the hostname matches, like the baseline.
    pub name_transformations: Vec<(String, String)>,
}

impl GitLabRepoSource {
    pub fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<String, String> {
        let (clone, base, matched) = parse_urls(clone_url, &self.url)?;
        let Some(base) = base else {
            return Ok(String::new());
        };
        if !matched {
            return Ok(String::new());
        }
        let transformations = compile_name_transformations(&self.name_transformations)?;
        Ok(gitlab_repo_name(
            &self.repository_path_pattern,
            &base.hostname,
            trim_git_suffix_and_slash(&clone.path),
            &transformations,
        ))
    }
}

/// `reposource.BitbucketCloud.CloneURLToRepoName`.
#[derive(Debug, Clone)]
pub struct BitbucketCloudRepoSource {
    pub url: String,
    pub repository_path_pattern: String,
}

impl BitbucketCloudRepoSource {
    pub fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<String, String> {
        let (clone, base, matched) = parse_urls(clone_url, &self.url)?;
        let Some(base) = base else {
            return Ok(String::new());
        };
        if !matched {
            return Ok(String::new());
        }
        Ok(bitbucket_cloud_repo_name(
            &self.repository_path_pattern,
            &base.hostname,
            trim_git_suffix_and_slash(&clone.path),
        ))
    }
}

/// `reposource.BitbucketServer.CloneURLToRepoName`.
#[derive(Debug, Clone)]
pub struct BitbucketServerRepoSource {
    pub url: String,
    pub repository_path_pattern: String,
}

impl BitbucketServerRepoSource {
    pub fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<String, String> {
        let (clone, base, matched) = parse_urls(clone_url, &self.url)?;
        let Some(base) = base else {
            return Ok(String::new());
        };
        if !matched {
            return Ok(String::new());
        }

        let trimmed = clone.path.strip_suffix(".git").unwrap_or(&clone.path);
        let project_and_repo = if clone.scheme == "ssh" {
            trimmed.strip_prefix('/').unwrap_or(trimmed).to_owned()
        } else if clone.scheme.starts_with("http") {
            trimmed.strip_prefix("/scm/").unwrap_or(trimmed).to_owned()
        } else {
            String::new()
        };
        let Some(index) = project_and_repo.find('/') else {
            return Ok(String::new());
        };
        if project_and_repo.len() - 1 == index {
            return Ok(String::new());
        }
        Ok(bitbucket_server_repo_name(
            &self.repository_path_pattern,
            &base.hostname,
            &project_and_repo[..index],
            &project_and_repo[index + 1..],
        ))
    }
}

/// `reposource.Gerrit.CloneURLToRepoName`.
#[derive(Debug, Clone)]
pub struct GerritRepoSource {
    pub url: String,
    pub repository_path_pattern: String,
}

impl GerritRepoSource {
    pub fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<String, String> {
        let (clone, base, matched) = parse_urls(clone_url, &self.url)?;
        let Some(base) = base else {
            return Ok(String::new());
        };
        if !matched {
            return Ok(String::new());
        }
        let name = clone.path.strip_suffix(".git").unwrap_or(&clone.path);
        let name = name.strip_prefix('/').unwrap_or(name);
        // `/a/` means "authenticated clone".
        let name = name.strip_prefix("a/").unwrap_or(name);
        Ok(gerrit_repo_name(
            &self.repository_path_pattern,
            &base.hostname,
            name,
        ))
    }
}

/// `reposource.AWS.CloneURLToRepoName`.
#[derive(Debug, Clone)]
pub struct AwsCodeCommitRepoSource {
    pub repository_path_pattern: String,
}

impl AwsCodeCommitRepoSource {
    pub fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<String, String> {
        let (clone, _, _) = parse_urls(clone_url, "")?;
        if !clone.host.ends_with(".amazonaws.com") {
            return Ok(String::new());
        }
        let name = clone.path.strip_suffix(".git").unwrap_or(&clone.path);
        let name = name.strip_prefix("/v1/repos/").unwrap_or(name);
        Ok(aws_repo_name(&self.repository_path_pattern, name))
    }
}

/// `reposource.Gitolite.CloneURLToRepoName`.
#[derive(Debug, Clone)]
pub struct GitoliteRepoSource {
    pub host: String,
    pub prefix: String,
}

impl GitoliteRepoSource {
    pub fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<String, String> {
        let clone = parse_clone_url(clone_url)?;
        let host = parse_clone_url(&self.host)?;
        // `Gitolite.CloneURLToRepoName` compares `Hostname()` directly (no
        // `www.` stripping, unlike `parseURLs`).
        if host.host != clone.host {
            return Ok(String::new());
        }
        Ok(gitolite_repo_name(
            &self.prefix,
            trim_git_suffix_and_slash(&clone.path),
        ))
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

    #[test]
    fn code_host_mappings_match_the_baseline() {
        let github = GitHubRepoSource {
            url: "https://GitHub.example.com".to_owned(),
            repository_path_pattern: String::new(),
        };
        assert_eq!(
            github
                .clone_url_to_repo_name("https://github.example.com/o/r.git")
                .expect("github"),
            "github.example.com/o/r"
        );
        assert_eq!(
            github
                .clone_url_to_repo_name("https://other.example.com/o/r")
                .expect("github"),
            ""
        );

        let gitlab = GitLabRepoSource {
            url: "https://gitlab.example.com".to_owned(),
            repository_path_pattern: String::new(),
            name_transformations: vec![(
                r"^gitlab\.example\.com/org/(.*)$".to_owned(),
                "gitlab.example.com/transformed/$1".to_owned(),
            )],
        };
        assert_eq!(
            gitlab
                .clone_url_to_repo_name("git@gitlab.example.com:org/sub/repo.git")
                .expect("gitlab"),
            "gitlab.example.com/transformed/sub/repo"
        );

        let bitbucket_server = BitbucketServerRepoSource {
            url: "https://bbs.example.com".to_owned(),
            repository_path_pattern: String::new(),
        };
        assert_eq!(
            bitbucket_server
                .clone_url_to_repo_name("https://bbs.example.com/scm/PROJ/repo.git")
                .expect("bbs"),
            "bbs.example.com/PROJ/repo"
        );
        assert_eq!(
            bitbucket_server
                .clone_url_to_repo_name("ssh://git@bbs.example.com:7999/PROJ/repo.git")
                .expect("bbs ssh"),
            "bbs.example.com/PROJ/repo"
        );

        let bitbucket_cloud = BitbucketCloudRepoSource {
            url: "https://bitbucket.org".to_owned(),
            repository_path_pattern: String::new(),
        };
        assert_eq!(
            bitbucket_cloud
                .clone_url_to_repo_name("https://bitbucket.org/o/r.git")
                .expect("bbc"),
            "bitbucket.org/o/r"
        );

        let gerrit = GerritRepoSource {
            url: "https://gerrit.example.com".to_owned(),
            repository_path_pattern: String::new(),
        };
        assert_eq!(
            gerrit
                .clone_url_to_repo_name("ssh://gerrit.example.com:29418/a/proj/repo")
                .expect("gerrit"),
            "gerrit.example.com/proj/repo"
        );

        let aws = AwsCodeCommitRepoSource {
            repository_path_pattern: String::new(),
        };
        assert_eq!(
            aws.clone_url_to_repo_name(
                "https://git-codecommit.us-east-1.amazonaws.com/v1/repos/myrepo"
            )
            .expect("aws"),
            "myrepo"
        );
        assert_eq!(
            aws.clone_url_to_repo_name("https://github.com/o/r")
                .expect("aws miss"),
            ""
        );

        let gitolite = GitoliteRepoSource {
            host: "git@gitolite.example.com".to_owned(),
            prefix: "gitolite.example.com/".to_owned(),
        };
        assert_eq!(
            gitolite
                .clone_url_to_repo_name("ssh://git@gitolite.example.com/team/r@x.git")
                .expect("gitolite"),
            "gitolite.example.com/team/r-x"
        );
    }
}
