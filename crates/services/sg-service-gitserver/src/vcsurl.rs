//! Port of `internal/vcs.URL` (`ParseURL` + `String` + `IsSSH`) and
//! `cmd/gitserver/internal/urlredactor`.
//!
//! The baseline parses every remote URL once and then:
//!
//! - passes `URL.String()` to git (rsync-style URLs keep their `user@host:path`
//!   shape instead of gaining a leading `/`),
//! - derives credential redaction from the URL's userinfo so progress output
//!   and error strings never leak tokens,
//! - moves URL credentials out of the command line into `GIT_SG_*` environment
//!   variables (see `configure_remote_command` in `vcssyncer`).
//!
//! The Go implementation accepts `Host` values that a strict URL parser
//! rejects (e.g. `perforce://admin:pw@ssl:10.0.0.1:1666//Depot`), so the
//! parser here is hand-written and mirrors Go's four fallbacks in order:
//! `file://` → scheme → SCP syntax → local path. For stdlib- and SCP-format
//! URLs `String()` returns the original text, which is what Go reserializes
//! for well-formed inputs.

use crate::git::GitError;

const SCHEMES: &[&str] = &[
    "ssh", "git", "git+ssh", "http", "https", "ftp", "ftps", "rsync", "file", "perforce",
    // Not an official git transport; GitLab instances sometimes report it.
    "iap",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlFormat {
    /// `URL.String()` from the standard library.
    Stdlib,
    /// SCP syntax (`git@host:path`), reserialized without a leading `/`.
    Rsync,
    /// A bare local path, rendered as `file://<path>`.
    Local,
}

/// Port of `vcs.URL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VcsUrl {
    raw: String,
    format: UrlFormat,
    scheme: String,
    /// Decoded username from the userinfo section.
    username: Option<String>,
    /// `Some` when the userinfo contained a `:`; the value may be empty
    /// (`Password()` returns `ok == true` in that case).
    password: Option<String>,
    /// Host, including a `:port` suffix when present.
    host: String,
    /// Escape-preserving path component.
    path: String,
    query: Option<String>,
}

impl VcsUrl {
    /// Port of `vcs.ParseURL`.
    pub fn parse(rawurl: &str) -> Result<Self, GitError> {
        if let Some(url) = parse_file(rawurl) {
            return Ok(url);
        }
        if let Some(url) = parse_scheme(rawurl) {
            return Ok(url);
        }
        if let Some(url) = parse_scp(rawurl) {
            return Ok(url);
        }
        Ok(parse_local(rawurl))
    }

    /// `URL.IsSSH`: SCP-format URLs have an empty scheme.
    pub fn is_ssh(&self) -> bool {
        self.scheme.is_empty() || self.scheme == "ssh"
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// `URL.User.Username()`; empty when there is no userinfo.
    pub fn username(&self) -> &str {
        self.username.as_deref().unwrap_or("")
    }

    /// `URL.User.Password()`.
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    /// The URL with its userinfo section removed, as produced by clearing
    /// `URL.User` before `String()`.
    pub fn without_user(&self) -> String {
        match self.format {
            UrlFormat::Rsync => match self.username.as_deref() {
                Some(username) => {
                    let prefix = format!("{username}@");
                    // Prefer the decoded form, falling back to the raw prefix.
                    if let Some(rest) = self.raw.strip_prefix(&prefix) {
                        rest.to_owned()
                    } else {
                        strip_userinfo(&self.raw)
                    }
                }
                None => self.raw.clone(),
            },
            UrlFormat::Stdlib => strip_userinfo(&self.raw),
            UrlFormat::Local => self.to_string(),
        }
    }
}

impl std::fmt::Display for VcsUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.format {
            // ParseURL keeps the input text; reserializing a well-formed URL
            // yields the same string.
            UrlFormat::Stdlib | UrlFormat::Rsync => f.write_str(&self.raw),
            UrlFormat::Local => write!(f, "file://{}", self.raw),
        }
    }
}

fn parse_file(rawurl: &str) -> Option<VcsUrl> {
    let path = rawurl.strip_prefix("file://")?;
    Some(VcsUrl {
        raw: rawurl.to_owned(),
        format: UrlFormat::Stdlib,
        scheme: "file".to_owned(),
        username: None,
        password: None,
        host: String::new(),
        path: path.to_owned(),
        query: None,
    })
}

fn parse_scheme(rawurl: &str) -> Option<VcsUrl> {
    let colon = rawurl.find(':')?;
    let scheme = &rawurl[..colon];
    if scheme.is_empty()
        || !scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        || !scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        || !SCHEMES.contains(&scheme)
    {
        return None;
    }

    let mut rest = &rawurl[colon + 1..];
    let mut username = None;
    let mut password = None;
    let mut host = String::new();
    if let Some(after) = rest.strip_prefix("//") {
        let authority_end = after.find(['/', '?', '#']).unwrap_or(after.len());
        let authority = &after[..authority_end];
        rest = &after[authority_end..];

        let (userinfo, hostpart) = match authority.rfind('@') {
            Some(at) => (Some(&authority[..at]), &authority[at + 1..]),
            None => (None, authority),
        };
        host = hostpart.to_owned();
        if let Some(userinfo) = userinfo {
            let (user, pass) = match userinfo.find(':') {
                Some(colon) => (Some(&userinfo[..colon]), Some(&userinfo[colon + 1..])),
                None => (Some(userinfo), None),
            };
            username = user.map(percent_decode);
            password = pass.map(percent_decode);
        }
    }

    let (path, query) = split_path_query(rest);
    Some(VcsUrl {
        raw: rawurl.to_owned(),
        format: UrlFormat::Stdlib,
        scheme: scheme.to_owned(),
        username,
        password,
        host,
        path,
        query,
    })
}

fn parse_scp(rawurl: &str) -> Option<VcsUrl> {
    // The baseline regexp is
    // `^([a-zA-Z0-9-._~]+@)?([a-zA-Z0-9._-]+):([a-zA-Z0-9\@./._-]+)(?:\?|$)(.*)$`.
    // The user/host segments cannot contain `:`, so the first one separates
    // them; the path may contain `@`, `.`, `_`, `-` and `/`.
    let colon = rawurl.find(':')?;
    let (user_host, rest) = rawurl.split_at(colon);
    let rest = &rest[1..];

    let (username, host) = match user_host.rfind('@') {
        Some(at) => {
            let user = &user_host[..at];
            if user.is_empty() || !is_username(user) {
                return None;
            }
            (Some(user.to_owned()), &user_host[at + 1..])
        }
        None => (None, user_host),
    };
    if host.is_empty() || !host.chars().all(is_host_char) {
        return None;
    }

    let (path, query) = match rest.find(['?', '#']) {
        Some(index) => (&rest[..index], Some(rest[index + 1..].to_owned())),
        None => (rest, None),
    };
    if path.is_empty() || !path.chars().all(is_scp_path_char) {
        return None;
    }

    Some(VcsUrl {
        raw: rawurl.to_owned(),
        format: UrlFormat::Rsync,
        scheme: String::new(),
        username,
        password: None,
        host: host.to_owned(),
        path: path.to_owned(),
        query,
    })
}

fn parse_local(rawurl: &str) -> VcsUrl {
    VcsUrl {
        raw: rawurl.to_owned(),
        format: UrlFormat::Local,
        scheme: "file".to_owned(),
        username: None,
        password: None,
        host: String::new(),
        path: rawurl.to_owned(),
        query: None,
    }
}

fn is_username(value: &str) -> bool {
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~'))
}

fn is_host_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}

fn is_scp_path_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '@' | '.' | '/' | '_' | '-')
}

/// Split `<path>[?query][#fragment]` into the path and raw query.
fn split_path_query(rest: &str) -> (String, Option<String>) {
    let end = rest.find(['?', '#']).unwrap_or(rest.len());
    let path = rest[..end].to_owned();
    let query = rest[end..]
        .strip_prefix('?')
        .and_then(|q| q.split('#').next())
        .map(str::to_owned);
    (path, query)
}

/// Remove `userinfo@` from a `scheme://...` URL, leaving the rest untouched.
fn strip_userinfo(raw: &str) -> String {
    let Some(scheme_end) = raw.find("://") else {
        return raw.to_owned();
    };
    let authority_start = scheme_end + 3;
    let authority_end = raw[authority_start..]
        .find(['/', '?', '#'])
        .map(|index| authority_start + index)
        .unwrap_or(raw.len());
    let authority = &raw[authority_start..authority_end];
    match authority.rfind('@') {
        Some(at) => {
            let mut out = String::with_capacity(raw.len());
            // Drop the whole userinfo section (authority start through `@`).
            out.push_str(&raw[..authority_start]);
            out.push_str(&raw[authority_start + at + 1..]);
            out
        }
        None => raw.to_owned(),
    }
}

/// Lenient percent-decoding (Go's userinfo unescaping).
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                out.push(high * 16 + low);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Port of `urlredactor.URLRedactor`.
#[derive(Debug, Clone)]
pub struct UrlRedactor {
    sensitive: Vec<String>,
}

impl UrlRedactor {
    /// `urlredactor.New`: redact the password (and, when there is no password,
    /// the username) plus the full URL. Both are replaced with `<redacted>`.
    pub fn new(url: &VcsUrl) -> Self {
        let mut sensitive = Vec::new();
        let password = url.password().unwrap_or("");
        let username = url.username();
        if !password.is_empty() && !username.is_empty() {
            // Only block the password when both are set; the username is not
            // considered sensitive next to it.
            sensitive.push(password.to_owned());
        } else {
            if !password.is_empty() {
                sensitive.push(password.to_owned());
            }
            if !username.is_empty() {
                sensitive.push(username.to_owned());
            }
        }
        sensitive.push(url.to_string());
        Self { sensitive }
    }

    /// `URLRedactor.Redact`.
    pub fn redact(&self, message: &str) -> String {
        let mut message = message.to_owned();
        for sensitive in &self.sensitive {
            message = message.replace(sensitive.as_str(), "<redacted>");
        }
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(input: &str) -> VcsUrl {
        VcsUrl::parse(input).expect("parse")
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn parses_the_baseline_test_table() {
        // Cases from `internal/vcs/url_test.go` (format, scheme, user, host,
        // path, serialized form).
        let cases: &[(&str, UrlFormat, &str, Option<&str>, &str, &str, &str)] = &[
            (
                "user@host.xz:path/to/repo.git/",
                UrlFormat::Rsync,
                "",
                Some("user"),
                "host.xz",
                "path/to/repo.git/",
                "user@host.xz:path/to/repo.git/",
            ),
            (
                "host.xz:path/to/repo.git/",
                UrlFormat::Rsync,
                "",
                None,
                "host.xz",
                "path/to/repo.git/",
                "host.xz:path/to/repo.git/",
            ),
            (
                "host.xz:/path/to/repo.git/",
                UrlFormat::Rsync,
                "",
                None,
                "host.xz",
                "/path/to/repo.git/",
                "host.xz:/path/to/repo.git/",
            ),
            (
                "git@host.xz:organization/repo.git?ref=test",
                UrlFormat::Rsync,
                "",
                Some("git"),
                "host.xz",
                "organization/repo.git",
                "git@host.xz:organization/repo.git?ref=test",
            ),
            (
                "git://host.xz:1234/path/to/repo.git/",
                UrlFormat::Stdlib,
                "git",
                None,
                "host.xz:1234",
                "/path/to/repo.git/",
                "git://host.xz:1234/path/to/repo.git/",
            ),
            (
                "ssh://user@host.xz:1234/path/to/repo.git/",
                UrlFormat::Stdlib,
                "ssh",
                Some("user"),
                "host.xz:1234",
                "/path/to/repo.git/",
                "ssh://user@host.xz:1234/path/to/repo.git/",
            ),
            (
                "https://user:password@host.xz/organization/repo.git?ref=feature/test",
                UrlFormat::Stdlib,
                "https",
                Some("user"),
                "host.xz",
                "/organization/repo.git",
                "https://user:password@host.xz/organization/repo.git?ref=feature/test",
            ),
            (
                "perforce://admin:pa$$word@ssl:192.168.1.100:1666//Sourcegraph/",
                UrlFormat::Stdlib,
                "perforce",
                Some("admin"),
                "ssl:192.168.1.100:1666",
                "//Sourcegraph/",
                "perforce://admin:pa$$word@ssl:192.168.1.100:1666//Sourcegraph/",
            ),
            (
                "iap://admin:pa$$word@gitlab.sgdev.org/test/repo.git",
                UrlFormat::Stdlib,
                "iap",
                Some("admin"),
                "gitlab.sgdev.org",
                "/test/repo.git",
                "iap://admin:pa$$word@gitlab.sgdev.org/test/repo.git",
            ),
            (
                "/path/to/repo.git/",
                UrlFormat::Local,
                "file",
                None,
                "",
                "/path/to/repo.git/",
                "file:///path/to/repo.git/",
            ),
            (
                "file:///path/to/repo.git/",
                UrlFormat::Stdlib,
                "file",
                None,
                "",
                "/path/to/repo.git/",
                "file:///path/to/repo.git/",
            ),
        ];

        for (input, format, scheme, user, host, path, serialized) in cases {
            let url = parsed(input);
            assert_eq!(&url.format, format, "{input}");
            assert_eq!(url.scheme(), *scheme, "{input}");
            let got_user = (!url.username().is_empty()).then(|| url.username());
            assert_eq!(got_user, *user, "{input}");
            assert_eq!(url.host(), *host, "{input}");
            assert_eq!(url.path(), *path, "{input}");
            assert_eq!(url.to_string(), *serialized, "{input}");
        }
    }

    #[test]
    fn parses_passwords_and_percent_escapes() {
        let url = parsed("https://user:p%40ss@host.xz/repo.git");
        assert_eq!(url.username(), "user");
        assert_eq!(url.password(), Some("p@ss"));
        // No password segment: `Password()` is not ok.
        let url = parsed("https://token@host.xz/repo.git");
        assert_eq!(url.password(), None);
        assert_eq!(url.username(), "token");
        // Empty password segment still counts as a password.
        let url = parsed("https://user:@host.xz/repo.git");
        assert_eq!(url.password(), Some(""));
    }

    #[test]
    fn identifies_ssh_urls() {
        assert!(parsed("git@host.xz:org/repo.git").is_ssh());
        assert!(parsed("ssh://host.xz/org/repo.git").is_ssh());
        assert!(!parsed("https://host.xz/org/repo.git").is_ssh());
        assert!(!parsed("/path/to/repo.git").is_ssh());
    }

    #[test]
    fn removes_userinfo_like_clearing_url_user() {
        let url = parsed("https://user:password@host.xz/org/repo.git");
        assert_eq!(url.without_user(), "https://host.xz/org/repo.git");
        let url = parsed("https://host.xz/org/repo.git");
        assert_eq!(url.without_user(), "https://host.xz/org/repo.git");
        let url = parsed("git@host.xz:org/repo.git");
        assert_eq!(url.without_user(), "host.xz:org/repo.git");
        // `@` in the path is not userinfo.
        let url = parsed("host.xz:path/to/repo@domain.git/");
        assert_eq!(url.without_user(), "host.xz:path/to/repo@domain.git/");
    }

    #[test]
    fn redacts_credentials_like_the_baseline() {
        let url = parsed("https://user:password@host.xz/org/repo.git");
        let redactor = UrlRedactor::new(&url);
        // The username is kept when a password is present, exactly like
        // `urlredactor_test.go`; the whole-URL rule only fires when nothing
        // in the URL was replaced first.
        assert_eq!(
            redactor.redact("fatal: https://user:password@host.xz/org/repo.git not found"),
            "fatal: https://user:<redacted>@host.xz/org/repo.git not found"
        );
        assert_eq!(redactor.redact("password leaked"), "<redacted> leaked");

        // With only a username, the username is sensitive.
        let url = parsed("https://token@host.xz/org/repo.git");
        let redactor = UrlRedactor::new(&url);
        assert_eq!(redactor.redact("token rejected"), "<redacted> rejected");

        // Without userinfo only the full URL is sensitive.
        let url = parsed("https://host.xz/org/repo.git");
        let redactor = UrlRedactor::new(&url);
        assert_eq!(
            redactor.redact("fatal: https://host.xz/org/repo.git"),
            "fatal: <redacted>"
        );
        assert_eq!(redactor.redact("nothing here"), "nothing here");
    }
}
