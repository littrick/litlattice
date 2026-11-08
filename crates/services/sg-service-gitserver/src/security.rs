//! Port of `internal/security.ValidateRemoteAddr`, used by gitolite host
//! validation (`cmd/gitserver/internal/list_gitolite.go`).
//!
//! The baseline accepts an IP address or a hostname that resolves, with an
//! optional integer port and an optional alphanumeric username:
//! `[user@]host[:port]`. The hostname is resolved with `net.LookupHost`; here
//! that is `tokio::net::lookup_host`, which also performs a DNS lookup for
//! hostnames. An IP literal never reaches the resolver.

use std::net::IpAddr;

/// `net.SplitHostPort` reduced to the cases [`validate_remote_addr`] cares
/// about: `Some((host, port))` when the address carries a port, `None`
/// otherwise. An unbracketed IPv6 address has no port (Go returns an error).
fn split_host_port(addr: &str) -> Option<(&str, &str)> {
    if let Some(rest) = addr.strip_prefix('[') {
        let (host, tail) = rest.split_once(']')?;
        let port = tail.strip_prefix(':')?;
        return Some((host, port));
    }
    let (host, port) = addr.rsplit_once(':')?;
    if host.contains(':') {
        // Unbracketed IPv6: Go's SplitHostPort reports "too many colons".
        return None;
    }
    Some((host, port))
}

/// `security.ValidateRemoteAddr`.
pub async fn validate_remote_addr(addr: &str) -> bool {
    let mut raddr = addr;

    if let Some((host, port)) = split_host_port(raddr) {
        // `strconv.Atoi(port)`: any signed integer is accepted.
        if port.parse::<i64>().is_err() {
            return false;
        }
        raddr = host;
    }

    // `userRegex = "^[a-zA-Z0-9]+$"`; more than one `@` is invalid.
    let fragments: Vec<&str> = raddr.split('@').collect();
    if fragments.len() > 2 {
        return false;
    }
    if fragments.len() == 2 {
        let user = fragments[0];
        if user.is_empty() || !user.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return false;
        }
        raddr = fragments[1];
    }

    if raddr.parse::<IpAddr>().is_ok() {
        return true;
    }

    // `net.LookupHost`: the baseline treats any resolution failure as invalid.
    tokio::net::lookup_host((raddr, 0)).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn validates_ips_and_usernames_like_the_baseline() {
        assert!(validate_remote_addr("127.0.0.1").await);
        assert!(validate_remote_addr("127.0.0.1:22").await);
        assert!(validate_remote_addr("git@127.0.0.1").await);
        assert!(validate_remote_addr("git@127.0.0.1:2222").await);
        assert!(validate_remote_addr("[::1]:22").await);

        // Port must be an integer.
        assert!(!validate_remote_addr("127.0.0.1:ssh").await);
        // More than one `@`.
        assert!(!validate_remote_addr("a@b@127.0.0.1").await);
        // Username must be alphanumeric.
        assert!(!validate_remote_addr("git-user@127.0.0.1").await);
        // Unresolvable host.
        assert!(!validate_remote_addr("no-such-host.invalid").await);
        // Empty string does not resolve.
        assert!(!validate_remote_addr("").await);
    }
}
