#![forbid(unsafe_code)]

//! `internal/webhooks/outbound.CheckURL` and the subset of
//! `internal/hostmatcher` it uses.
//!
//! The baseline rejects webhook destinations that match the denylist built in
//! `CheckURL`: the `loopback` builtin network plus the metadata address
//! `169.254.169.254`. `outbound.SetTestDenyList` (used by the baseline's own
//! tests) drops the `loopback` rule; [`DenyList::test_denylist`] reproduces
//! that list so the differential can talk to a local HTTP server.
//!
//! Go's denylist is an allow-list with negative polarity: `CheckURL` matches
//! the IPv4/IPv6 addresses against a [`HostMatchList`] and rejects the URL if
//! any address matches. Only the pieces reachable from `CheckURL` are ported:
//! host patterns (`filepath.Match` is approximated by `*`/`?` globbing),
//! the `external`/`private`/`loopback` builtins and IP matching. CIDR entries
//! cannot occur here because the list is built internally.

use std::net::IpAddr;

/// `hostmatcher.HostMatchList`'s builtin networks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Builtin {
    /// `external`: a valid non-private unicast address.
    External,
    /// `private`: RFC 1918 / RFC 4193.
    Private,
    /// `loopback`: 127.0.0.0/8 and ::1.
    Loopback,
}

/// One denylist entry: a builtin network or a host pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DenyRule {
    Builtin(Builtin),
    Pattern(String),
}

/// The list `CheckURL` builds. Production uses [`DenyList::default_denylist`];
/// the test-visible variant mirrors `outbound.SetTestDenyList`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenyList {
    rules: Vec<DenyRule>,
}

impl DenyList {
    /// `defaultDenylist` with `SetTestDenyList` not applied.
    pub fn default_denylist() -> Self {
        Self {
            rules: vec![
                DenyRule::Builtin(Builtin::Loopback),
                DenyRule::Pattern("169.254.169.254".to_owned()),
            ],
        }
    }

    /// `outbound.SetTestDenyList`: the loopback builtin is removed.
    pub fn test_denylist() -> Self {
        Self {
            rules: vec![DenyRule::Pattern("169.254.169.254".to_owned())],
        }
    }

    fn matches_ip(&self, ip: IpAddr) -> bool {
        let host = ip.to_string();
        for rule in &self.rules {
            match rule {
                DenyRule::Pattern(pattern) => {
                    if pattern == "*" || glob_matches(pattern, &host) {
                        return true;
                    }
                }
                DenyRule::Builtin(builtin) => {
                    let matched = match builtin {
                        Builtin::External => is_global_unicast(ip) && !is_private(ip),
                        Builtin::Private => is_private(ip),
                        Builtin::Loopback => ip.is_loopback(),
                    };
                    if matched {
                        return true;
                    }
                }
            }
        }
        false
    }
}

/// `errIllegalAddr`.
pub const ERR_ILLEGAL_ADDR: &str = "Address must not be private, link-local or loopback";

/// `outbound.CheckURL`.
pub async fn check_url(dest: &str, deny: &DenyList) -> Result<(), String> {
    let parsed = match parse_go_url(dest) {
        Ok(parsed) => parsed,
        Err(()) => return Err(format!("Could not parse provided URL {dest}")),
    };

    if !parsed.scheme.starts_with("http") || parsed.host.is_empty() {
        return Err(format!("Unsupported URL provided {dest}"));
    }

    // `net.ParseIP(u.Hostname())`.
    if let Ok(ip) = parsed.hostname.parse::<IpAddr>() {
        if deny.matches_ip(ip) {
            return Err(ERR_ILLEGAL_ADDR.to_owned());
        }
        return Ok(());
    }

    // `defaultResolver.LookupHost` followed by a per-address check.
    let host = parsed.hostname.clone();
    let addrs = tokio::task::spawn_blocking(move || {
        use std::net::ToSocketAddrs;
        format!("{host}:0")
            .to_socket_addrs()
            .map(|iter| iter.map(|addr| addr.ip()).collect::<Vec<_>>())
    })
    .await
    .map_err(|_| "Could not resolve hostname".to_owned())?;
    let addrs = match addrs {
        Ok(addrs) if !addrs.is_empty() => addrs,
        _ => return Err("Could not resolve hostname".to_owned()),
    };
    for addr in addrs {
        if deny.matches_ip(addr) {
            return Err(ERR_ILLEGAL_ADDR.to_owned());
        }
    }

    Ok(())
}

/// The subset of `url.URL` `CheckURL` reads.
#[derive(Debug, Default, PartialEq, Eq)]
struct ParsedUrl {
    scheme: String,
    host: String,
    hostname: String,
}

/// A Go-compatible approximation of `url.Parse` for the fields used above.
///
/// Go returns a populated `url.URL` even for `"http://"` and `"not a url"`;
/// the Rust `url` crate rejects both. The Go error conditions that matter here
/// are a leading `://` (no scheme) and control characters.
fn parse_go_url(raw: &str) -> Result<ParsedUrl, ()> {
    if raw.chars().any(|c| c.is_control()) {
        return Err(());
    }
    // `getScheme`: a colon with no scheme in front is `missing protocol
    // scheme`, which `url.Parse` reports as an error.
    if raw.starts_with(':') {
        return Err(());
    }

    let (scheme, rest) = match split_scheme(raw) {
        Some((scheme, rest)) => (scheme, rest),
        None => (String::new(), raw),
    };

    if scheme.is_empty() && rest.starts_with("//") {
        // `getScheme` reports `missing protocol scheme`.
        return Err(());
    }

    let (host, hostname) = match rest.strip_prefix("//") {
        Some(authority) => {
            let end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
            let authority = &authority[..end];
            // Userinfo: take everything after the last '@'.
            let host = authority.rsplit('@').next().unwrap_or(authority);
            (host.to_owned(), hostname_of(host))
        }
        None => (String::new(), String::new()),
    };

    Ok(ParsedUrl {
        scheme,
        host,
        hostname,
    })
}

/// `getScheme`: split `scheme:` when the prefix is a valid scheme and a colon
/// follows it.
fn split_scheme(raw: &str) -> Option<(String, &str)> {
    let colon = raw.find(':')?;
    let candidate = &raw[..colon];
    if candidate.is_empty() {
        return None;
    }
    let mut chars = candidate.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')) {
        return None;
    }
    Some((candidate.to_ascii_lowercase(), &raw[colon + 1..]))
}

/// `url.URL.Hostname`: strip the port and the IPv6 brackets.
fn hostname_of(host: &str) -> String {
    if let Some(rest) = host.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return rest[..end].to_owned();
        }
        return host.to_owned();
    }
    match host.rfind(':') {
        Some(index) if !host[index + 1..].is_empty() => host[..index].to_owned(),
        Some(_) => host[..host.len() - 1].to_owned(),
        None => host.to_owned(),
    }
}

/// `filepath.Match` for host patterns: `*` matches any run of characters and
/// `?` matches one character. Character classes are not used by the denylist
/// and fall back to literal comparison.
fn glob_matches(pattern: &str, value: &str) -> bool {
    fn inner(pattern: &[char], value: &[char]) -> bool {
        match pattern.split_first() {
            None => value.is_empty(),
            Some(('*', rest)) => {
                if inner(rest, value) {
                    return true;
                }
                match value.split_first() {
                    Some((_, tail)) => inner(pattern, tail),
                    None => false,
                }
            }
            Some(('?', rest)) => match value.split_first() {
                Some((_, tail)) => inner(rest, tail),
                None => false,
            },
            Some((expected, rest)) => match value.split_first() {
                Some((actual, tail)) if actual == expected => inner(rest, tail),
                _ => false,
            },
        }
    }

    let pattern: Vec<char> = pattern.chars().collect();
    let value: Vec<char> = value.chars().collect();
    inner(&pattern, &value)
}

/// `net.IP.IsPrivate`: RFC 1918 and RFC 4193.
fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private(),
        IpAddr::V6(v6) => (v6.segments()[0] & 0xfe00) == 0xfc00,
    }
}

/// `net.IP.IsGlobalUnicast`.
fn is_global_unicast(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            !v4.is_unspecified()
                && !v4.is_loopback()
                && !v4.is_multicast()
                && !is_v4_link_local(v4)
                && octets != [255, 255, 255, 255]
        }
        IpAddr::V6(v6) => {
            !v6.is_unspecified()
                && !v6.is_loopback()
                && !v6.is_multicast()
                && (v6.segments()[0] & 0xffc0) != 0xfe80
        }
    }
}

/// `net.IP.IsLinkLocalUnicast` for IPv4 (169.254.0.0/16).
fn is_v4_link_local(v4: std::net::Ipv4Addr) -> bool {
    let octets = v4.octets();
    octets[0] == 169 && octets[1] == 254
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_go_shaped_urls() {
        assert_eq!(
            parse_go_url("http://").expect("parse"),
            ParsedUrl {
                scheme: "http".to_owned(),
                host: String::new(),
                hostname: String::new(),
            }
        );
        assert_eq!(
            parse_go_url("http://[::1]:8080/x").expect("parse"),
            ParsedUrl {
                scheme: "http".to_owned(),
                host: "[::1]:8080".to_owned(),
                hostname: "::1".to_owned(),
            }
        );
        assert_eq!(
            parse_go_url("http://user:pass@1.2.3.4:80/x").expect("parse"),
            ParsedUrl {
                scheme: "http".to_owned(),
                host: "1.2.3.4:80".to_owned(),
                hostname: "1.2.3.4".to_owned(),
            }
        );
        assert!(parse_go_url("://bad").is_err());
    }

    #[tokio::test]
    async fn checks_numeric_addresses() {
        let deny = DenyList::default_denylist();
        assert!(check_url("http://1.2.3.4/hook", &deny).await.is_ok());
        assert_eq!(
            check_url("http://127.0.0.1/hook", &deny).await,
            Err(ERR_ILLEGAL_ADDR.to_owned())
        );
        assert_eq!(
            check_url("http://169.254.169.254/x", &deny).await,
            Err(ERR_ILLEGAL_ADDR.to_owned())
        );
        assert!(check_url("http://10.0.0.1/x", &deny).await.is_ok());
        assert_eq!(
            check_url("ftp://1.2.3.4/x", &deny).await,
            Err("Unsupported URL provided ftp://1.2.3.4/x".to_owned())
        );
        assert!(
            check_url("http://127.0.0.1/hook", &DenyList::test_denylist())
                .await
                .is_ok()
        );
    }
}
