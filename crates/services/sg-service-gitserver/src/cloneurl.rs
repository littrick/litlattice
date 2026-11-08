//! Port of `cmd/gitserver/internal/cloneurl`: compute a repository's Git
//! remote URL from its external service configuration and stored metadata.
//!
//! The Go implementation unmarshals the config into a `schema.*Connection`
//! struct and type-asserts `repo.Metadata`. The Rust port keeps both as JSON
//! and reads the fields by name (case-insensitively, like `encoding/json`).
//! The one observable difference is that a metadata value of the wrong code
//! host cannot be detected: Go would report `unknown repo.Metadata type %T`,
//! while this port produces the clone URL from whatever fields exist.

use serde_json::Value;
use sg_config::parse_jsonc;
use sg_db::repos::Repo;
use sg_encryption::{maybe_decrypt, Key};

/// `cloneurl.ForEncryptableConfig`: decrypt the stored config, parse it as
/// JSONC, and select the clone URL for `repo`.
///
/// Errors carry the baseline's wrappers (`loading service configuration: …`).
pub async fn for_encryptable_config(
    kind: &str,
    config: &str,
    encryption_key_id: &str,
    key: Option<&dyn Key>,
    repo: &Repo,
) -> Result<String, String> {
    let parsed = parse_encryptable_config(kind, config, encryption_key_id, key).await?;
    clone_url(kind, &parsed, repo)
}

/// `extsvc.ParseEncryptableConfig`: `getConfigPrototype` (kind validation) →
/// `EncryptableConfig.Decrypt` → `jsonc.Unmarshal`.
async fn parse_encryptable_config(
    kind: &str,
    config: &str,
    encryption_key_id: &str,
    key: Option<&dyn Key>,
) -> Result<Value, String> {
    if !known_kind(kind) {
        return Err(format!(
            "loading service configuration: unknown external service kind {kind:?}"
        ));
    }
    let raw = maybe_decrypt(key, config, encryption_key_id)
        .await
        .map_err(|err| format!("loading service configuration: {err}"))?;
    parse_jsonc(&raw).map_err(|err| format!("loading service configuration: {err}"))
}

/// `extsvc.VariantValueOf` matches kinds and service types case-insensitively.
fn known_kind(kind: &str) -> bool {
    matches!(
        kind.to_ascii_uppercase().as_str(),
        "AWSCODECOMMIT"
            | "AZUREDEVOPS"
            | "BITBUCKETCLOUD"
            | "BITBUCKETSERVER"
            | "GERRIT"
            | "GITHUB"
            | "GITLAB"
            | "GITOLITE"
            | "GOMODULES"
            | "JVMPACKAGES"
            | "NPMPACKAGES"
            | "OTHER"
            | "PAGURE"
            | "PERFORCE"
            | "PHABRICATOR"
            | "PYTHONPACKAGES"
            | "RUBYPACKAGES"
            | "RUSTPACKAGES"
    )
}

/// The `cloneURL` switch. Unknown kinds were rejected earlier by
/// [`parse_encryptable_config`].
fn clone_url(kind: &str, config: &Value, repo: &Repo) -> Result<String, String> {
    let kind = kind.to_ascii_uppercase();
    let metadata = &repo.metadata;
    match kind.as_str() {
        "AWSCODECOMMIT" => aws_code_clone_url(metadata, config),
        "AZUREDEVOPS" => azure_devops_clone_url(metadata, config),
        "BITBUCKETSERVER" => bitbucket_server_clone_url(metadata, config),
        "BITBUCKETCLOUD" => bitbucket_cloud_clone_url(metadata, config),
        "GERRIT" => gerrit_clone_url(metadata, config),
        "GITHUB" => github_clone_url(metadata, config),
        "GITLAB" => gitlab_clone_url(metadata, config),
        "GITOLITE" => Ok(text(metadata, "URL")),
        "PERFORCE" => Ok(perforce_clone_url(metadata, config)),
        "PHABRICATOR" => phabricator_clone_url(metadata),
        "PAGURE" => Ok(text(metadata, "full_url")),
        "OTHER" => Ok(other_clone_url(repo, metadata)),
        // The baseline returns the repository name for these kinds without
        // looking at the metadata.
        "GOMODULES" | "PYTHONPACKAGES" | "RUSTPACKAGES" | "RUBYPACKAGES" => Ok(repo.name.clone()),
        "JVMPACKAGES" => Ok(maven_clone_url(metadata)),
        "NPMPACKAGES" => Ok(npm_clone_url(metadata)),
        _ => Err(format!(
            "unknown external service kind {kind:?} for repo {}",
            repo.id
        )),
    }
}

fn aws_code_clone_url(repo: &Value, config: &Value) -> Result<String, String> {
    let http = text(repo, "HTTPCloneURL");
    let credentials = obj(config, "gitCredentials");
    let username = credentials.map(|c| text(c, "username")).unwrap_or_default();
    let password = credentials.map(|c| text(c, "password")).unwrap_or_default();
    let mut url =
        ParsedUrl::parse(&http).map_err(|err| format!("failed to parse HTTP clone URL: {err}"))?;
    url.set_userinfo(&username, Some(&password));
    Ok(url.to_string())
}

fn azure_devops_clone_url(repo: &Value, config: &Value) -> Result<String, String> {
    if text(config, "gitURLType") == "ssh" {
        return Ok(text(repo, "SSHURL"));
    }
    let mut url = ParsedUrl::parse(&text(repo, "RemoteURL"))
        .map_err(|err| format!("failed to parse remote URL: {err}"))?;
    url.set_userinfo(&text(config, "username"), Some(&text(config, "token")));
    Ok(url.to_string())
}

fn bitbucket_server_clone_url(repo: &Value, config: &Value) -> Result<String, String> {
    let links = obj(repo, "links")
        .and_then(|links| links.get("clone"))
        .and_then(Value::as_array);
    let mut clone_url = String::new();
    if let Some(links) = links {
        for link in links {
            let name = text(link, "name");
            let href = text(link, "href");
            if name == "ssh" && text(config, "gitURLType") == "ssh" {
                clone_url = href;
                break;
            }
            if name == "http" {
                // Prefer the personal access token over the password.
                let token = text(config, "token");
                let password = if token.is_empty() {
                    text(config, "password")
                } else {
                    token
                };
                clone_url = href.clone();
                let username = text(config, "username");
                if !username.is_empty() {
                    if let Ok(mut url) = ParsedUrl::parse(&href) {
                        if password.is_empty() {
                            url.set_userinfo(&username, None);
                        } else {
                            url.set_userinfo(&username, Some(&password));
                        }
                        clone_url = url.to_string();
                    }
                }
                // No break: fall back to http when ssh is missing.
            }
        }
    }
    if clone_url.is_empty() {
        return Err("no clone URL found in repo links".to_owned());
    }
    Ok(clone_url)
}

fn bitbucket_cloud_clone_url(repo: &Value, config: &Value) -> Result<String, String> {
    let url_host = text(config, "url");
    if text(config, "gitURLType") == "ssh" {
        return Ok(format!("git@{url_host}:{}.git", text(repo, "full_name")));
    }

    let https_url = obj(repo, "links")
        .and_then(|links| links.get("clone"))
        .and_then(Value::as_array)
        .and_then(|links| {
            links
                .iter()
                .find(|link| text(link, "name") == "https")
                .map(|link| text(link, "href"))
        })
        .ok_or_else(|| "no HTTPS clone URL found".to_owned())?;

    let mut url = ParsedUrl::parse(&https_url)
        .map_err(|err| format!("failed to parse HTTPS clone URL: {err}"))?;
    let access_token = text(config, "accessToken");
    if !access_token.is_empty() {
        url.set_userinfo("x-token-auth", Some(&access_token));
    } else {
        url.set_userinfo(
            &text(config, "username"),
            Some(&text(config, "appPassword")),
        );
    }
    Ok(url.to_string())
}

fn gerrit_clone_url(project: &Value, config: &Value) -> Result<String, String> {
    if text(config, "gitURLType") == "ssh" {
        return Ok(text(project, "ssh_url_to_repo"));
    }

    // TODO(backcompat, Sourcegraph 5.5): projects without an HTTP URL are
    // cloned with the configured Gerrit URL and the project's ID.
    if text(project, "http_url_to_repo").is_empty() {
        let mut url = ParsedUrl::parse(&text(config, "url"))
            .map_err(|err| format!("failed to parse Gerrit URL: {err}"))?;
        url.set_userinfo(&text(config, "username"), Some(&text(config, "password")));
        // Gerrit encodes slashes in IDs; the `a` prefix selects the
        // authenticated endpoint.
        let id = text(project, "ID").replace("%2F", "/");
        url.set_path(&format!("a/{}", clean_path(&id)));
        return Ok(url.to_string());
    }

    let mut url = ParsedUrl::parse(&text(project, "http_url_to_repo"))
        .map_err(|err| format!("failed to parse Gerrit project remote URL: {err}"))?;
    url.set_userinfo(&text(config, "username"), Some(&text(config, "password")));
    Ok(url.to_string())
}

fn github_clone_url(repo: &Value, config: &Value) -> Result<String, String> {
    if text(config, "gitURLType") == "ssh" {
        let base = ParsedUrl::parse(&text(config, "url")).map_err(|err| err.to_string())?;
        // `extsvc.NormalizeBaseURL` lowercases the host and returns its
        // hostname (without brackets or port).
        let hostname = base.hostname().to_ascii_lowercase();
        return Ok(format!(
            "git@{hostname}:{}.git",
            text(repo, "NameWithOwner")
        ));
    }

    let repo_url = text(repo, "URL");
    if repo_url.is_empty() {
        return Err("empty repo.URL".to_owned());
    }
    let has_app_details = config
        .get("gitHubAppDetails")
        .map(|details| !details.is_null())
        .unwrap_or(false);
    if text(config, "token").is_empty() && !has_app_details {
        return Ok(repo_url);
    }
    if has_app_details {
        // `ghauth.FromConnection` (GitHub App installation tokens) is not
        // ported; the token path below covers the common configuration.
        return Err(
            "GitHub App authentication is not supported by the Rust gitserver yet".to_owned(),
        );
    }
    let mut url =
        ParsedUrl::parse(&repo_url).map_err(|err| format!("failed to parse repo.URL: {err}"))?;
    // `ghauth.FromConnection` returns an `OAuthBearerToken` for a personal
    // access token; `SetURLUser` sets `oauth2:<token>`.
    url.set_userinfo("oauth2", Some(&text(config, "token")));
    Ok(url.to_string())
}

fn gitlab_clone_url(project: &Value, config: &Value) -> Result<String, String> {
    if text(config, "gitURLType") == "ssh" {
        return Ok(text(project, "ssh_url_to_repo"));
    }
    let http = text(project, "http_url_to_repo");
    let token = text(config, "token");
    if token.is_empty() {
        return Ok(http);
    }
    let mut url = ParsedUrl::parse(&http)
        .map_err(|err| format!("failed to parse repo.HTTPURLToRepo: {err}"))?;
    let username = if text(config, "token.type") == "oauth" {
        "oauth2"
    } else {
        "git"
    };
    url.set_userinfo(username, Some(&token));
    Ok(url.to_string())
}

/// `perforceCloneURL`: `perforce://user:pass@port/depot`.
fn perforce_clone_url(depot: &Value, config: &Value) -> String {
    let host = text(config, "p4.port");
    let path = text(depot, "depot");
    let userinfo = format!(
        "{}:{}",
        encode_userinfo(&text(config, "p4.user")),
        encode_userinfo(&text(config, "p4.passwd"))
    );
    if path.is_empty() {
        format!("perforce://{userinfo}@{host}")
    } else if path.starts_with('/') {
        format!("perforce://{userinfo}@{host}{}", encode_path(&path))
    } else {
        format!("perforce://{userinfo}@{host}/{}", encode_path(&path))
    }
}

fn phabricator_clone_url(repo: &Value) -> Result<String, String> {
    let mut external: Vec<String> = Vec::new();
    let mut builtin: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    if let Some(uris) = repo.get("URIs").and_then(Value::as_array) {
        for uri in uris {
            if flag(uri, "Disabled") || text(uri, "Normalized").is_empty() {
                continue;
            }
            let identifier = text(uri, "BuiltinIdentifier");
            if !identifier.is_empty() {
                builtin.insert(
                    format!("{}+{}", text(uri, "BuiltinProtocol"), identifier),
                    (text(uri, "Effective"), text(uri, "Normalized")),
                );
            } else {
                external.push(text(uri, "Normalized"));
            }
        }
    }

    let mut name = external.first().cloned().unwrap_or_default();
    let mut clone_url = String::new();
    for (protocol, identifier) in [
        ("https", "shortname"),
        ("https", "callsign"),
        ("https", "id"),
        ("ssh", "shortname"),
        ("ssh", "callsign"),
        ("ssh", "id"),
    ] {
        if let Some((effective, normalized)) = builtin.get(&format!("{protocol}+{identifier}")) {
            clone_url = effective.clone();
            if name.is_empty() {
                name = normalized.clone();
            }
        }
    }
    if clone_url.is_empty() {
        return Err("no clone URL found".to_owned());
    }
    Ok(clone_url)
}

/// `otherCloneURL`: the source's service ID joined with the metadata's
/// relative path.
fn other_clone_url(repo: &Repo, metadata: &Value) -> String {
    format!(
        "{}{}",
        repo.external_service_id.as_deref().unwrap_or_default(),
        text(metadata, "RelativePath")
    )
}

/// `MavenModule.CloneURL`: `url.URL{Path: RepoName()}.String()`; the JDK
/// module maps to `jdk`.
fn maven_clone_url(metadata: &Value) -> String {
    let Some(module) = obj(metadata, "Module") else {
        return String::new();
    };
    let group = text(module, "GroupID");
    let artifact = text(module, "ArtifactID");
    if group == "jdk" && artifact == "jdk" {
        return "jdk".to_owned();
    }
    encode_path(&format!("maven/{group}/{artifact}"))
}

/// `NpmPackageName.RepoName` as used by `CloneURL`.
fn npm_clone_url(metadata: &Value) -> String {
    let Some(package) = obj(metadata, "Package") else {
        return String::new();
    };
    let scope = text(package, "Scope");
    let name = text(package, "Name");
    if scope.is_empty() {
        format!("npm/{name}")
    } else {
        format!("npm/{scope}/{name}")
    }
}

// --- JSON helpers: Go's encoding/json matches keys case-insensitively. -----

fn field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    let object = value.as_object()?;
    object.get(key).or_else(|| {
        object
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
            .map(|(_, value)| value)
    })
}

fn text(value: &Value, key: &str) -> String {
    field(value, key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn flag(value: &Value, key: &str) -> bool {
    field(value, key).and_then(Value::as_bool).unwrap_or(false)
}

fn obj<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    field(value, key).filter(|value| value.is_object())
}

// --- URL assembly ----------------------------------------------------------
//
// The baseline parses clone URLs with Go's `net/url`, replaces the userinfo,
// and re-renders the URL. Most fields round-trip unchanged; this implements
// the subset clone URLs need: scheme, authority (userinfo/host), path, query
// and fragment, plus Go's userinfo escaping.

struct ParsedUrl {
    scheme: String,
    /// Authority without any userinfo.
    host: String,
    userinfo: Option<String>,
    /// Path starting with `/` when the URL is hierarchical (empty otherwise).
    path: String,
    query: String,
    fragment: String,
}

impl ParsedUrl {
    fn parse(raw: &str) -> Result<Self, String> {
        if let Some(err) = invalid_url_error(raw) {
            return Err(format!("parse {raw:?}: {err}"));
        }
        let Some(scheme_end) = raw.find("://") else {
            // `url.Parse` treats this as a path-only URL; setting userinfo
            // still renders as `//user@path`.
            return Ok(Self {
                scheme: String::new(),
                host: String::new(),
                userinfo: None,
                path: raw.to_owned(),
                query: String::new(),
                fragment: String::new(),
            });
        };
        let scheme = raw[..scheme_end].to_owned();
        let rest = &raw[scheme_end + 3..];
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        let mut remainder = &rest[authority_end..];

        let (mut userinfo, host) = match authority.rfind('@') {
            Some(index) => (
                Some(authority[..index].to_owned()),
                authority[index + 1..].to_owned(),
            ),
            None => (None, authority.to_owned()),
        };
        // No userinfo in the URL to begin with.
        if !host.contains(':') && userinfo.is_none() {
            userinfo = None;
        }

        let (before_fragment, fragment) = match remainder.find('#') {
            Some(index) => (&remainder[..index], remainder[index + 1..].to_owned()),
            None => (remainder, String::new()),
        };
        remainder = before_fragment;
        let (path, query) = match remainder.find('?') {
            Some(index) => (
                remainder[..index].to_owned(),
                remainder[index + 1..].to_owned(),
            ),
            None => (remainder.to_owned(), String::new()),
        };

        Ok(Self {
            scheme,
            host,
            userinfo,
            path,
            query,
            fragment,
        })
    }

    fn set_userinfo(&mut self, username: &str, password: Option<&str>) {
        let mut userinfo = encode_userinfo(username);
        if let Some(password) = password {
            userinfo.push(':');
            userinfo.push_str(&encode_userinfo(password));
        }
        self.userinfo = Some(userinfo);
    }

    /// `u.Path = path`: replaced with Go's `EscapedPath` semantics (a path
    /// that does not start with `/` gains one when a host is present).
    fn set_path(&mut self, path: &str) {
        self.path = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
    }

    /// `URL.Hostname()`: the host without port and brackets, lowercased by
    /// callers that want `NormalizeBaseURL` semantics.
    fn hostname(&self) -> String {
        let host = &self.host;
        let host = host.strip_prefix('[').unwrap_or(host);
        let host = match host.find(']') {
            Some(index) => &host[..index],
            None => host.rsplit_once(':').map(|(host, _)| host).unwrap_or(host),
        };
        host.to_owned()
    }
}

impl std::fmt::Display for ParsedUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.scheme.is_empty() && self.host.is_empty() && self.userinfo.is_none() {
            f.write_str(&encode_path(&self.path))?;
        } else {
            if !self.scheme.is_empty() {
                write!(f, "{}:", self.scheme)?;
            }
            f.write_str("//")?;
            if let Some(userinfo) = &self.userinfo {
                f.write_str(userinfo)?;
                f.write_str("@")?;
            }
            f.write_str(&self.host)?;
            if !self.path.is_empty() {
                if self.path.starts_with('/') || self.host.is_empty() {
                    f.write_str(&encode_path(&self.path))?;
                } else {
                    f.write_str("/")?;
                    f.write_str(&encode_path(&self.path))?;
                }
            }
        }
        if !self.query.is_empty() {
            write!(f, "?{}", self.query)?;
        }
        if !self.fragment.is_empty() {
            write!(f, "#{}", self.fragment)?;
        }
        Ok(())
    }
}

/// `url.Parse` rejects control characters and malformed percent escapes.
fn invalid_url_error(raw: &str) -> Option<String> {
    if raw.chars().any(|c| c.is_control()) {
        return Some("net/url: invalid control character in URL".to_owned());
    }
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let ok = bytes.get(index + 1).is_some_and(|b| b.is_ascii_hexdigit())
                && bytes.get(index + 2).is_some_and(|b| b.is_ascii_hexdigit());
            if !ok {
                return Some("invalid URL escape".to_owned());
            }
        }
        index += 1;
    }
    None
}

/// `url.escape(s, encodeUserPassword)`: only `@`, `/`, `?`, `:` (plus
/// non-unreserved characters) are escaped; sub-delims stay literal.
fn encode_userinfo(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b'$' | b'&' | b'+' | b',' | b';' | b'=' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `url.escape(s, encodePath)`: everything except unreserved characters and
/// `$ & + , / : ; = @` is escaped.
fn encode_path(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `path.Join("a", id)`: collapse duplicate and trailing slashes.
fn clean_path(path: &str) -> String {
    path.split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(kind_suffix: &str, metadata: Value) -> Repo {
        Repo {
            id: 1,
            name: format!("example.com/{kind_suffix}"),
            external_service_type: None,
            external_service_id: Some("https://example.com/".to_owned()),
            metadata,
            uri: None,
            blocked: None,
            sources: Vec::new(),
        }
    }

    async fn clone_url_for(kind: &str, config: Value, metadata: Value) -> Result<String, String> {
        let repo = repo("repo", metadata);
        for_encryptable_config(kind, &config.to_string(), "", None, &repo).await
    }

    #[tokio::test]
    async fn other_joins_the_service_id_and_relative_path() {
        let url = clone_url_for(
            "OTHER",
            serde_json::json!({"url": "https://example.com", "repos": []}),
            serde_json::json!({"RelativePath": "owner/repo"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://example.com/owner/repo");
    }

    #[tokio::test]
    async fn gitolite_and_pagure_return_the_metadata_url() {
        let url = clone_url_for(
            "GITOLITE",
            serde_json::json!({"prefix": "gitolite.example.com", "host": "git@host"}),
            serde_json::json!({"URL": "ssh://git@host/repo.git"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "ssh://git@host/repo.git");

        let url = clone_url_for(
            "PAGURE",
            serde_json::json!({"url": "https://pagure.io"}),
            serde_json::json!({"full_url": "https://pagure.io/repo.git"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://pagure.io/repo.git");
    }

    #[tokio::test]
    async fn github_inserts_the_token_as_oauth2_userinfo() {
        // `OAuthBearerToken.SetURLUser` sets `oauth2:<token>`.
        let url = clone_url_for(
            "GITHUB",
            serde_json::json!({"url": "https://github.com", "token": "tok"}),
            serde_json::json!({"NameWithOwner": "o/r", "URL": "https://github.com/o/r"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://oauth2:tok@github.com/o/r");
    }

    #[tokio::test]
    async fn github_without_a_token_returns_the_web_url() {
        let url = clone_url_for(
            "github",
            serde_json::json!({"url": "https://github.com"}),
            serde_json::json!({"NameWithOwner": "o/r", "URL": "https://github.com/o/r"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://github.com/o/r");
    }

    #[tokio::test]
    async fn github_ssh_uses_the_normalized_hostname() {
        let url = clone_url_for(
            "GITHUB",
            serde_json::json!({"url": "https://GHE.Example.com:8443/", "gitURLType": "ssh"}),
            serde_json::json!({"NameWithOwner": "o/r"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "git@ghe.example.com:o/r.git");
    }

    #[tokio::test]
    async fn gitlab_inserts_the_token_with_the_right_username() {
        let url = clone_url_for(
            "GITLAB",
            serde_json::json!({"url": "https://gitlab.com", "token": "tok"}),
            serde_json::json!({"http_url_to_repo": "https://gitlab.com/g/p.git"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://git:tok@gitlab.com/g/p.git");

        let url = clone_url_for(
            "GITLAB",
            serde_json::json!({"url": "https://gitlab.com", "token": "tok", "token.type": "oauth"}),
            serde_json::json!({"http_url_to_repo": "https://gitlab.com/g/p.git"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://oauth2:tok@gitlab.com/g/p.git");
    }

    #[tokio::test]
    async fn bitbucket_server_prefers_ssh_then_http_with_token() {
        let metadata = serde_json::json!({
            "links": {"clone": [
                {"name": "http", "href": "https://bb.example.com/scm/p/r.git"},
                {"name": "ssh", "href": "ssh://git@bb.example.com/p/r.git"},
            ]},
        });
        let url = clone_url_for(
            "BITBUCKETSERVER",
            serde_json::json!({"url": "https://bb.example.com", "gitURLType": "ssh"}),
            metadata.clone(),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "ssh://git@bb.example.com/p/r.git");

        let url = clone_url_for(
            "BITBUCKETSERVER",
            serde_json::json!({
                "url": "https://bb.example.com",
                "username": "user",
                "password": "pw",
                "token": "tok",
            }),
            metadata,
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://user:tok@bb.example.com/scm/p/r.git");
    }

    #[tokio::test]
    async fn bitbucket_cloud_uses_app_password_or_access_token() {
        let metadata = serde_json::json!({
            "full_name": "team/repo",
            "links": {"clone": [
                {"name": "https", "href": "https://bitbucket.org/team/repo.git"},
            ]},
        });
        let url = clone_url_for(
            "BITBUCKETCLOUD",
            serde_json::json!({"url": "https://bitbucket.org", "username": "u", "appPassword": "p"}),
            metadata.clone(),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://u:p@bitbucket.org/team/repo.git");

        let url = clone_url_for(
            "BITBUCKETCLOUD",
            serde_json::json!({"url": "https://bitbucket.org", "accessToken": "tok"}),
            metadata,
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://x-token-auth:tok@bitbucket.org/team/repo.git");
    }

    #[tokio::test]
    async fn gerrit_uses_the_http_url_or_falls_back_to_the_project_id() {
        let url = clone_url_for(
            "GERRIT",
            serde_json::json!({"url": "https://gerrit.example.com", "username": "u", "password": "p"}),
            serde_json::json!({"http_url_to_repo": "https://gerrit.example.com/p.git"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://u:p@gerrit.example.com/p.git");

        let url = clone_url_for(
            "GERRIT",
            serde_json::json!({"url": "https://gerrit.example.com", "username": "u", "password": "p"}),
            serde_json::json!({"ID": "foo%2Fbar"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://u:p@gerrit.example.com/a/foo/bar");
    }

    #[tokio::test]
    async fn azure_devops_and_aws_codecommit_insert_credentials() {
        let url = clone_url_for(
            "AZUREDEVOPS",
            serde_json::json!({"url": "https://dev.azure.com", "username": "u", "token": "t"}),
            serde_json::json!({"remoteURL": "https://dev.azure.com/org/proj/_git/repo"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://u:t@dev.azure.com/org/proj/_git/repo");

        let url = clone_url_for(
            "AWSCODECOMMIT",
            serde_json::json!({"gitCredentials": {"username": "u", "password": "p"}}),
            serde_json::json!({"HTTPCloneURL": "https://git-codecommit.us-east-1.amazonaws.com/v1/repos/r"}),
        )
        .await
        .expect("clone url");
        assert_eq!(
            url,
            "https://u:p@git-codecommit.us-east-1.amazonaws.com/v1/repos/r"
        );
    }

    #[tokio::test]
    async fn perforce_and_phabricator_compose_urls() {
        let url = clone_url_for(
            "PERFORCE",
            serde_json::json!({"p4.user": "admin", "p4.passwd": "pw", "p4.port": "ssl:host:1666"}),
            serde_json::json!({"depot": "//Sourcegraph/"}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "perforce://admin:pw@ssl:host:1666//Sourcegraph/");

        let url = clone_url_for(
            "PHABRICATOR",
            serde_json::json!({"url": "https://phab.example.com"}),
            serde_json::json!({"URIs": [
                // An external URI only provides the display name.
                {"Disabled": false, "Normalized": "gitolite.example.com/r", "Effective": "git@gitolite.example.com:r"},
                {"Disabled": false, "Normalized": "phab.example.com/source/r", "Effective": "https://phab.example.com/source/r", "BuiltinProtocol": "https", "BuiltinIdentifier": "shortname"},
            ]}),
        )
        .await
        .expect("clone url");
        assert_eq!(url, "https://phab.example.com/source/r");

        // Only external URIs have no clone URL at all.
        let err = clone_url_for(
            "PHABRICATOR",
            serde_json::json!({}),
            serde_json::json!({"URIs": [
                {"Disabled": false, "Normalized": "gitolite.example.com/r", "Effective": "git@gitolite.example.com:r"},
            ]}),
        )
        .await
        .expect_err("no clone url");
        assert_eq!(err, "no clone URL found");
    }

    #[tokio::test]
    async fn package_kinds_use_the_repo_name_or_package_metadata() {
        let go_repo = repo("pkg", serde_json::json!({}));
        assert_eq!(
            for_encryptable_config("GOMODULES", "{}", "", None, &go_repo)
                .await
                .expect("go"),
            "example.com/pkg"
        );
        assert_eq!(
            for_encryptable_config(
                "NPMPACKAGES",
                "{}",
                "",
                None,
                &repo(
                    "pkg",
                    serde_json::json!({"Package": {"Scope": "s", "Name": "n"}})
                ),
            )
            .await
            .expect("npm"),
            "npm/s/n"
        );
        assert_eq!(
            for_encryptable_config(
                "JVMPACKAGES",
                "{}",
                "",
                None,
                &repo(
                    "pkg",
                    serde_json::json!({"Module": {"GroupID": "g", "ArtifactID": "a"}})
                ),
            )
            .await
            .expect("jvm"),
            "maven/g/a"
        );
    }

    #[tokio::test]
    async fn rejects_unknown_kinds_and_missing_clone_urls() {
        let repo = repo("repo", serde_json::json!({}));
        let err = for_encryptable_config("MYSTERY", "{}", "", None, &repo)
            .await
            .expect_err("unknown kind");
        assert_eq!(
            err,
            "loading service configuration: unknown external service kind \"MYSTERY\""
        );

        let err = clone_url_for(
            "GITHUB",
            serde_json::json!({"url": "https://github.com"}),
            serde_json::json!({}),
        )
        .await
        .expect_err("empty repo url");
        assert_eq!(err, "empty repo.URL");

        let err = clone_url_for(
            "BITBUCKETSERVER",
            serde_json::json!({"url": "https://bb.example.com"}),
            serde_json::json!({"links": {"clone": []}}),
        )
        .await
        .expect_err("no links");
        assert_eq!(err, "no clone URL found in repo links");
    }
}
