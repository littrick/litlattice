//! Port of `internal/gitserver/connection/addrs.go`: which gitserver shard
//! owns a repository.
//!
//! Both the gitserver process (which shard am I?) and the repo-updater
//! process (which shard should this fetch go to?) compute the same function,
//! so it lives here instead of in a service crate.

use std::collections::BTreeMap;

use sg_config::Unified;

/// `connection.GitserverAddresses`.
#[derive(Debug, Clone, Default)]
pub struct GitserverAddresses {
    /// `cfg.ServiceConnections().GitServers`.
    pub addresses: Vec<String>,
    /// `experimentalFeatures.gitServerPinnedRepos`.
    pub pinned_servers: BTreeMap<String, String>,
}

impl GitserverAddresses {
    /// `connection.NewGitserverAddresses(conf.Get())`: the shard list from
    /// the service connections and the pins from the site config. Callers
    /// invoke this per request/run so both follow a live configuration.
    pub fn from_unified(unified: &Unified) -> Self {
        Self {
            addresses: unified.service_connections.git_servers.clone(),
            pinned_servers: unified.site.git_server_pinned_repos(),
        }
    }

    /// `AddrForRepo`: pinned repos win; otherwise the normalized name is
    /// hashed (MD5, big-endian first 8 bytes) modulo the shard count.
    ///
    /// Returns `None` when no gitservers are configured; the baseline would
    /// panic on the modulo by zero, which a drop-in server must not do.
    pub fn addr_for_repo(&self, repo_name: &str) -> Option<&str> {
        // `api.UndeletedRepoName`: a soft-deleted repo keeps its gitserver.
        let name = undeleted_repo_name(repo_name);
        if let Some(pinned) = self.pinned_servers.get(&name) {
            return Some(pinned.as_str());
        }
        let normalized = normalize_repo(&name);
        let index = addr_index(&normalized, self.addresses.len())?;
        self.addresses.get(index).map(String::as_str)
    }
}

/// `addrForKey`'s `binary.BigEndian.Uint64(md5.Sum(key)[:]) % len(addrs)`.
pub(crate) fn addr_index(key: &str, len: usize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let sum = md5::compute(key.as_bytes());
    let mut first = [0u8; 8];
    first.copy_from_slice(&sum.0[..8]);
    Some((u64::from_be_bytes(first) % len as u64) as usize)
}

/// `hostnameMatch`: `addr` equals `shard_id`, or extends it at a `.`/`:`
/// boundary. The baseline assumes `shard_id` is a prefix of a shard address
/// (`gitserver-0` matches `gitserver-0:3178`).
pub fn hostname_match(shard_id: &str, addr: &str) -> bool {
    if !addr.starts_with(shard_id) {
        return false;
    }
    if addr == shard_id {
        return true;
    }
    matches!(addr.as_bytes().get(shard_id.len()), Some(b'.' | b':'))
}

/// Port of Go `path.Clean` for slash-separated paths (only the rooted case
/// is needed here; repo names are rooted immediately).
fn clean_path(path: &str) -> String {
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => match parts.last() {
                Some(last) if *last != ".." => {
                    parts.pop();
                }
                _ => {
                    if !rooted {
                        parts.push(part);
                    }
                }
            },
            other => parts.push(other),
        }
    }
    let mut out = String::new();
    if rooted {
        out.push('/');
    }
    out.push_str(&parts.join("/"));
    if out.is_empty() {
        out.push('.');
    }
    out
}

/// Port of `protocol.NormalizeRepo`: clean the path, lowercase the host and
/// lowercase the whole path for GitHub (which is case-insensitive).
pub(crate) fn normalize_repo(input: &str) -> String {
    let cleaned = clean_path(&format!("/{input}"));
    let cleaned = cleaned.trim_start_matches('/');
    match cleaned.find('/') {
        None => cleaned.to_owned(),
        Some(slash) => {
            let host = cleaned[..slash].to_lowercase();
            let repo_path = &cleaned[slash..];
            if host == "github.com" {
                format!("{host}{}", repo_path.to_lowercase())
            } else {
                format!("{host}{repo_path}")
            }
        }
    }
}

/// Port of `api.UndeletedRepoName`: soft-deleted repos are renamed to
/// `DELETED-<major>.<minor>-<original>`; strip that marker once.
pub(crate) fn undeleted_repo_name(name: &str) -> String {
    let Some(rest) = name.strip_prefix("DELETED-") else {
        return name.to_owned();
    };
    let major = rest.bytes().take_while(u8::is_ascii_digit).count();
    let Some(rest) = rest[major..].strip_prefix('.') else {
        return name.to_owned();
    };
    let minor = rest.bytes().take_while(u8::is_ascii_digit).count();
    let Some(rest) = rest[minor..].strip_prefix('-') else {
        return name.to_owned();
    };
    if major == 0 || minor == 0 {
        return name.to_owned();
    }
    rest.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_hostnames_on_a_boundary() {
        assert!(hostname_match("gitserver-0", "gitserver-0"));
        assert!(hostname_match("gitserver-0", "gitserver-0:3178"));
        assert!(hostname_match("gitserver-0", "gitserver-0.example.com"));
        assert!(!hostname_match("gitserver-0", "gitserver-01:3178"));
        assert!(!hostname_match("gitserver-0", "other:3178"));
        assert!(!hostname_match("gitserver-0", ""));
    }

    #[test]
    fn pins_win_over_hashing() {
        let addrs = GitserverAddresses {
            addresses: vec!["a:1".to_owned(), "b:1".to_owned()],
            pinned_servers: [("github.com/o/r".to_owned(), "b:1".to_owned())]
                .into_iter()
                .collect(),
        };
        assert_eq!(addrs.addr_for_repo("github.com/o/r"), Some("b:1"));
        // The baseline looks pins up after `UndeletedRepoName`.
        assert_eq!(
            addrs.addr_for_repo("DELETED-2.3-github.com/o/r"),
            Some("b:1")
        );
    }

    /// Golden values from `TestAddrForRepo` in `connection/addrs_test.go`.
    #[test]
    fn hashes_like_the_baseline() {
        let addrs = GitserverAddresses {
            addresses: vec![
                "gitserver-1".to_owned(),
                "gitserver-2".to_owned(),
                "gitserver-3".to_owned(),
            ],
            pinned_servers: [("repo2".to_owned(), "gitserver-1".to_owned())]
                .into_iter()
                .collect(),
        };
        for (repo, want) in [
            ("repo1", "gitserver-3"),
            ("DELETED-123123.123123-repo1", "gitserver-3"),
            ("DELETED-123123.123123-repo2", "gitserver-1"),
            ("github.com/sourcegraph/sourcegraph", "gitserver-2"),
            // GitHub paths are lowercased before hashing.
            ("github.com/sourcegraph/Sourcegraph", "gitserver-2"),
            ("repo2", "gitserver-1"),
        ] {
            assert_eq!(addrs.addr_for_repo(repo), Some(want), "{repo}");
        }
    }

    #[test]
    fn normalizes_like_the_baseline() {
        assert_eq!(normalize_repo("foo/bar"), "foo/bar");
        assert_eq!(normalize_repo("/foo/bar/"), "foo/bar");
        assert_eq!(normalize_repo("foo//bar/./baz"), "foo/bar/baz");
        assert_eq!(normalize_repo("foo/../bar"), "bar");
        assert_eq!(normalize_repo("../bar"), "bar");
        assert_eq!(
            normalize_repo("GitHub.com/Owner/Repo"),
            "github.com/owner/repo"
        );
        assert_eq!(
            normalize_repo("gitlab.com/Owner/Repo"),
            "gitlab.com/Owner/Repo"
        );
        assert_eq!(normalize_repo("github.com/../foo/bar"), "foo/bar");
    }

    #[test]
    fn strips_delete_markers_like_the_baseline() {
        assert_eq!(undeleted_repo_name("DELETED-1.2-foo/bar"), "foo/bar");
        assert_eq!(undeleted_repo_name("DELETED-1.2-"), "");
        for unchanged in [
            "foo/bar",
            "deleted-1.2-foo",
            "DELETED-x-foo",
            "DELETED-1-foo",
            "DELETED-1.2foo",
            "foo/DELETED-1.2-bar",
        ] {
            assert_eq!(undeleted_repo_name(unchanged), unchanged);
        }
    }

    #[test]
    fn handles_no_configured_shards() {
        let addrs = GitserverAddresses::default();
        assert_eq!(addrs.addr_for_repo("example.com/o/r"), None);
        assert_eq!(addr_index("x", 0), None);
    }
}
