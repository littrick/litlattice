//! Port of `internal/gitserver/connection` address computation
//! (`NewGitserverAddresses`, `AddrForRepo`, `hostnameMatch`).
//!
//! gitserver only needs the child side: which shard owns a repository, and
//! whether this process is that shard.

use std::collections::BTreeMap;

/// `connection.GitserverAddresses`.
#[derive(Debug, Clone, Default)]
pub struct GitserverAddresses {
    /// `cfg.ServiceConnections().GitServers`.
    pub addresses: Vec<String>,
    /// `experimentalFeatures.gitServerPinnedRepos`.
    pub pinned_servers: BTreeMap<String, String>,
}

impl GitserverAddresses {
    /// `AddrForRepo`: pinned repos win; otherwise the normalized name is
    /// hashed (MD5, big-endian first 8 bytes) modulo the shard count.
    ///
    /// Returns `None` when no gitservers are configured; the baseline would
    /// panic on the modulo by zero, which a drop-in server must not do.
    pub fn addr_for_repo(&self, repo_name: &str) -> Option<&str> {
        // `api.UndeletedRepoName`: a soft-deleted repo keeps its gitserver.
        let name = crate::repo::undeleted_repo_name(repo_name);
        if let Some(pinned) = self.pinned_servers.get(&name) {
            return Some(pinned.as_str());
        }
        let normalized = crate::repo::normalize_repo(&name);
        let index = addr_index(&normalized, self.addresses.len())?;
        self.addresses.get(index).map(String::as_str)
    }
}

/// `addrForKey`'s `binary.BigEndian.Uint64(md5.Sum(key)[:]) % len(addrs)`.
fn addr_index(key: &str, len: usize) -> Option<usize> {
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
    fn handles_no_configured_shards() {
        let addrs = GitserverAddresses::default();
        assert_eq!(addrs.addr_for_repo("example.com/o/r"), None);
        assert_eq!(addr_index("x", 0), None);
    }
}
