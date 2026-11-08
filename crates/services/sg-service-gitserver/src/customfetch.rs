//! Port of `cmd/gitserver/internal/vcssyncer/customfetch.go`: site-config
//! mappings that replace `git fetch` for matching remotes.
//!
//! The baseline computes the mappings with `conf.Cached` and enables them only
//! when `ENABLE_CUSTOM_GIT_FETCH` is not the literal `"false"`. The Rust
//! config client has no watcher yet, so the mappings are a startup snapshot.

use std::collections::BTreeMap;

use sg_config::CustomFetchMapping;

use crate::vcsurl::VcsUrl;

/// `customGitFetch()`: `domainPath` → command words.
#[derive(Debug, Clone, Default)]
pub struct CustomFetch {
    mappings: BTreeMap<String, Vec<String>>,
}

impl CustomFetch {
    /// `buildCustomFetchMappings`. `mappings` is `None` when
    /// `experimentalFeatures.customGitFetch` is absent or `null` (Go's
    /// `c == nil`); an empty list still counts as configured for the warning.
    pub fn build(mappings: Option<&[CustomFetchMapping]>, enabled: bool) -> Self {
        if !enabled {
            if mappings.is_some() {
                tracing::warn!(
                    "a CustomGitFetchMapping is configured but ENABLE_CUSTOM_GIT_FETCH is not set"
                );
            }
            return Self::default();
        }
        let Some(mappings) = mappings else {
            return Self::default();
        };
        Self {
            mappings: mappings
                .iter()
                .map(|mapping| (mapping.domain_path.clone(), mapping.fetch.clone()))
                .collect(),
        }
    }

    /// `customFetchCmd`: the command replacing `git fetch` for `remote`, if a
    /// mapping matches. An empty `fetch` string yields no command.
    pub fn command(&self, remote: &VcsUrl) -> Option<Vec<String>> {
        if self.mappings.is_empty() {
            return None;
        }
        // `path.Join(remoteURL.Host, remoteURL.Path)`.
        let domain_path = join_path(remote.host(), &remote.decoded_path());
        match self.mappings.get(&domain_path) {
            Some(command) if !command.is_empty() => Some(command.clone()),
            _ => None,
        }
    }
}

/// Go `path.Join(host, path)`: join with `/` and clean.
fn join_path(host: &str, path: &str) -> String {
    let mut joined = String::with_capacity(host.len() + path.len() + 1);
    for part in [host, path] {
        if !joined.is_empty() || !part.is_empty() {
            if !joined.is_empty() {
                joined.push('/');
            }
            joined.push_str(part);
        }
    }
    if joined.is_empty() {
        return String::new();
    }
    clean_path(&joined)
}

/// Go `path.Clean` for slash-separated paths.
///
/// A literal port, including the `lazybuf` trick the standard library uses:
/// while the output is still a prefix of the input, byte comparisons read the
/// input directly, and backtracking checks the byte *after* the last written
/// position. Copying the algorithm avoids subtle differences around `..`.
fn clean_path(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let s = path.as_bytes();
    let rooted = s[0] == b'/';
    let n = s.len();
    let mut out = LazyBuf::new(s);
    let mut r = 0;
    let mut dotdot = 0;
    if rooted {
        out.append(b'/');
        r = 1;
        dotdot = 1;
    }
    while r < n {
        if s[r] == b'/' {
            // empty path element
            r += 1;
        } else if s[r] == b'.' && (r + 1 == n || s[r + 1] == b'/') {
            // . element
            r += 1;
        } else if s[r] == b'.' && s.get(r + 1) == Some(&b'.') && (r + 2 == n || s[r + 2] == b'/') {
            // .. element: remove to last /
            r += 2;
            if out.w > dotdot {
                // can backtrack
                let mut w = out.w - 1;
                while w > dotdot && out.index(w) != b'/' {
                    w -= 1;
                }
                out.w = w;
            } else if !rooted {
                // cannot backtrack, but not rooted, so append .. element.
                if out.w > 0 {
                    out.append(b'/');
                }
                out.append(b'.');
                out.append(b'.');
                dotdot = out.w;
            }
        } else {
            // real path element; add slash if needed
            if (rooted && out.w != 1) || (!rooted && out.w != 0) {
                out.append(b'/');
            }
            while r < n && s[r] != b'/' {
                out.append(s[r]);
                r += 1;
            }
        }
    }
    if out.w == 0 {
        ".".to_owned()
    } else {
        out.into_string()
    }
}

/// Go `path`'s `lazybuf`: a copy-on-write output buffer.
struct LazyBuf<'a> {
    s: &'a [u8],
    buf: Option<Vec<u8>>,
    w: usize,
}

impl<'a> LazyBuf<'a> {
    fn new(s: &'a [u8]) -> Self {
        Self { s, buf: None, w: 0 }
    }

    fn index(&self, i: usize) -> u8 {
        match &self.buf {
            Some(buf) => buf[i],
            None => self.s[i],
        }
    }

    fn append(&mut self, c: u8) {
        if self.buf.is_none() {
            if self.w < self.s.len() && self.s[self.w] == c {
                self.w += 1;
                return;
            }
            let mut buf = vec![0; self.s.len()];
            buf[..self.w].copy_from_slice(&self.s[..self.w]);
            self.buf = Some(buf);
        }
        let buf = self.buf.as_mut().expect("buf was just created");
        buf[self.w] = c;
        self.w += 1;
    }

    fn into_string(self) -> String {
        let bytes = match &self.buf {
            Some(buf) => &buf[..self.w],
            None => &self.s[..self.w],
        };
        String::from_utf8_lossy(bytes).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping(domain_path: &str, fetch: &str) -> CustomFetchMapping {
        CustomFetchMapping {
            domain_path: domain_path.to_owned(),
            fetch: fetch.split_whitespace().map(str::to_owned).collect(),
        }
    }

    fn mappings() -> Vec<CustomFetchMapping> {
        vec![
            mapping("github.com/foo/normal/one", "echo normal one"),
            mapping("github.com/foo/normal/two", "echo normal two"),
            mapping("github.com/foo/faulty", ""),
            mapping("github.com/foo/absolute", "/foo/bar/git fetch things"),
        ]
    }

    fn remote(url: &str) -> VcsUrl {
        VcsUrl::parse(url).expect("parse")
    }

    #[test]
    fn empty_configuration_has_no_command() {
        let custom = CustomFetch::build(None, false);
        assert_eq!(
            custom.command(&remote("git@github.com:sourcegraph/sourcegraph.git")),
            None
        );
    }

    #[test]
    fn disabled_mappings_have_no_command() {
        // Copied from `TestDisabledCustomGitFetch`: `ENABLE_CUSTOM_GIT_FETCH`
        // is unset, so even a matching mapping is ignored.
        let mappings = vec![mapping("github.com/foo/normal/one", "echo normal one")];
        let custom = CustomFetch::build(Some(&mappings), false);
        assert_eq!(
            custom.command(&remote(
                "https://8cd1419f4d5c1e0527f2893c9422f1a2a435116d@github.com/foo/normal/one"
            )),
            None
        );
    }

    #[test]
    fn selects_the_configured_command() {
        // Copied from `TestCustomGitFetch`.
        let mappings = mappings();
        let custom = CustomFetch::build(Some(&mappings), true);
        let cases = [
            (
                "https://8cd1419f4d5c1e0527f2893c9422f1a2a435116d@github.com/foo/normal/one",
                Some(vec![
                    "echo".to_owned(),
                    "normal".to_owned(),
                    "one".to_owned(),
                ]),
            ),
            (
                "https://8cd1419f4d5c1e0527f2893c9422f1a2a435116d@github.com/foo/normal/two",
                Some(vec![
                    "echo".to_owned(),
                    "normal".to_owned(),
                    "two".to_owned(),
                ]),
            ),
            (
                "https://8cd1419f4d5c1e0527f2893c9422f1a2a435116d@github.com/foo/faulty",
                None,
            ),
            (
                "https://8cd1419f4d5c1e0527f2893c9422f1a2a435116dgit@github.com/bar/notthere",
                None,
            ),
            (
                "https://8cd1419f4d5c1e0527f2893c9422f1a2a435116d@github.com/foo/absolute",
                Some(vec![
                    "/foo/bar/git".to_owned(),
                    "fetch".to_owned(),
                    "things".to_owned(),
                ]),
            ),
        ];
        for (url, want) in cases {
            assert_eq!(custom.command(&remote(url)), want, "URL {url}");
        }
    }

    #[test]
    fn joins_like_go_path_join() {
        // `path.Join` cleaning rules.
        assert_eq!(
            join_path("github.com", "/foo/bar.git"),
            "github.com/foo/bar.git"
        );
        assert_eq!(
            join_path("github.com", "foo/bar.git"),
            "github.com/foo/bar.git"
        );
        assert_eq!(join_path("", "/tmp/repo.git"), "/tmp/repo.git");
        assert_eq!(join_path("", ""), "");
        assert_eq!(join_path("host", "/"), "host");
        assert_eq!(join_path("host", "/a//b/./c/"), "host/a/b/c");
        assert_eq!(join_path("host", "/a/../b"), "host/b");
        // Golden value from Go `path.Join("host", "/../b")`.
        assert_eq!(join_path("host", "/../b"), "b");
    }

    #[test]
    fn local_remotes_match_by_path() {
        let mappings = vec![mapping("/tmp/example/repo.git", "custom fetch")];
        let custom = CustomFetch::build(Some(&mappings), true);
        assert_eq!(
            custom.command(&remote("/tmp/example/repo.git")),
            Some(vec!["custom".to_owned(), "fetch".to_owned()])
        );
    }
}
