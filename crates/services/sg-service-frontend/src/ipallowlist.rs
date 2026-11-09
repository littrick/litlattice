#![forbid(unsafe_code)]

//! Port of `cmd/frontend/internal/auth/ipallowlist`.
//!
//! The baseline `Middleware.Handle` is the outermost layer of
//! `newExternalHTTPHandler`'s chain: when the site configuration enables
//! `auth.allowedIpAddress` it rejects every request whose client or user IP is
//! not on the allowlist with a 403 HTML page. Disabled (the default) the
//! middleware is a pass-through.
//!
//! Configuration comes from the raw site configuration plus four
//! `AUTH_ALLOWED_IP_ADDRESS_*` environment variables that Cloud sets to
//! override it. The baseline reads them with `os.Getenv` on every config
//! computation, which here means [`AllowlistEnv::from_env`] at service
//! configuration time.

use std::net::IpAddr;

use bytes::Bytes;
use http::{header, HeaderMap, Request, Response, StatusCode};
use http_body_util::Full;
use sg_config::SiteConfiguration;
use sg_http::RemoteAddr;
use tonic::body::BoxBody;

/// `AUTH_ALLOWED_IP_ADDRESS_STABLE_USER_IP_ALLOWLIST`.
pub const ENV_STABLE_USER_IP_ALLOWLIST: &str = "AUTH_ALLOWED_IP_ADDRESS_STABLE_USER_IP_ALLOWLIST";
/// `AUTH_ALLOWED_IP_ADDRESS_STABLE_CLIENT_IP_ALLOWLIST`.
pub const ENV_STABLE_CLIENT_IP_ALLOWLIST: &str =
    "AUTH_ALLOWED_IP_ADDRESS_STABLE_CLIENT_IP_ALLOWLIST";
/// `AUTH_ALLOWED_IP_ADDRESS_STABLE_TRUSTED_CLIENT_IP_ALLOWLIST`.
pub const ENV_STABLE_TRUSTED_CLIENT_IP_ALLOWLIST: &str =
    "AUTH_ALLOWED_IP_ADDRESS_STABLE_TRUSTED_CLIENT_IP_ALLOWLIST";
/// `AUTH_ALLOWED_IP_ADDRESS_USER_IP_REQUEST_HEADERS_OVERRIDE`.
pub const ENV_USER_IP_REQUEST_HEADERS_OVERRIDE: &str =
    "AUTH_ALLOWED_IP_ADDRESS_USER_IP_REQUEST_HEADERS_OVERRIDE";

/// `defaultTrustedClientIpAllowlist`: liveness probes must always connect.
const DEFAULT_TRUSTED_CLIENT_IPS: &[&str] = &["127.0.0.1"];

/// The environment overrides, already split and trimmed like
/// `getEnvCommaSeparated`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllowlistEnv {
    pub stable_user_ip_allowlist: Vec<String>,
    pub stable_client_ip_allowlist: Vec<String>,
    pub stable_trusted_client_ip_allowlist: Vec<String>,
    pub user_ip_request_headers_override: Vec<String>,
}

impl AllowlistEnv {
    /// Read the four variables from the process environment.
    pub fn from_env() -> Self {
        Self {
            stable_user_ip_allowlist: env_comma_separated(ENV_STABLE_USER_IP_ALLOWLIST),
            stable_client_ip_allowlist: env_comma_separated(ENV_STABLE_CLIENT_IP_ALLOWLIST),
            stable_trusted_client_ip_allowlist: env_comma_separated(
                ENV_STABLE_TRUSTED_CLIENT_IP_ALLOWLIST,
            ),
            user_ip_request_headers_override: env_comma_separated(
                ENV_USER_IP_REQUEST_HEADERS_OVERRIDE,
            ),
        }
    }
}

/// `getEnvCommaSeparated`: trim each element and drop the empty ones.
fn env_comma_separated(name: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

/// A CIDR range (`net.IPNet`).
///
/// The network address keeps the Go distinction between 4-byte and 16-byte
/// forms: `IPNet.Contains` first converts the candidate to 4 bytes when
/// possible and then requires equal lengths, so `::ffff:1.2.3.0/120` does
/// **not** contain `1.2.3.4` (nor does `1.2.3.0/24` contain
/// `::ffff:1.2.3.4`? — it does, because the candidate is converted first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNet {
    addr: IpAddr,
    prefix: u8,
}

impl IpNet {
    /// `net.ParseCIDR` for the subset this middleware accepts.
    pub fn parse(value: &str) -> Option<Self> {
        let (ip, prefix) = value.split_once('/')?;
        let addr = ip.parse::<IpAddr>().ok()?;
        let prefix = prefix.parse::<u8>().ok()?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        if prefix > max {
            return None;
        }
        Some(Self { addr, prefix })
    }

    /// `(*net.IPNet).Contains`.
    pub fn contains(&self, candidate: IpAddr) -> bool {
        let candidate = normalize(candidate);
        match (self.addr, candidate) {
            (IpAddr::V4(network), IpAddr::V4(candidate)) => {
                let mask = v4_mask(self.prefix);
                u32::from(network) & mask == u32::from(candidate) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(candidate)) => {
                let mask = v6_mask(self.prefix);
                u128::from(network) & mask == u128::from(candidate) & mask
            }
            _ => false,
        }
    }

    /// `(*net.IPNet).String`: the masked network plus the prefix.
    pub fn network_string(&self) -> String {
        match self.addr {
            IpAddr::V4(network) => {
                let masked = std::net::Ipv4Addr::from(u32::from(network) & v4_mask(self.prefix));
                format!("{masked}/{}", self.prefix)
            }
            IpAddr::V6(network) => {
                let masked = std::net::Ipv6Addr::from(u128::from(network) & v6_mask(self.prefix));
                format!("{masked}/{}", self.prefix)
            }
        }
    }
}

fn v4_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix))
    }
}

fn v6_mask(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - u32::from(prefix))
    }
}

/// Go's `net.IP.To4` view: IPv4-mapped IPv6 addresses behave like IPv4.
fn normalize(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        IpAddr::V4(_) => addr,
    }
}

/// The computed `checker.config`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IpAllowlistConfig {
    pub authorized_user_ips: Vec<IpAddr>,
    pub authorized_user_ranges: Vec<IpNet>,
    pub authorized_client_ips: Vec<IpAddr>,
    pub authorized_client_ranges: Vec<IpNet>,
    pub trusted_client_ips: Vec<IpAddr>,
    pub trusted_client_ranges: Vec<IpNet>,
    pub user_headers: Vec<String>,
    pub error_message_template: Option<String>,
    /// `config.err`: the baseline logs it on every request but still applies
    /// the rest of the configuration.
    pub error: Option<String>,
}

impl IpAllowlistConfig {
    /// `checker.isAuthorized`.
    pub fn is_authorized<B>(&self, req: &Request<B>) -> Result<(), String> {
        let (client_ip, user_ip) = get_ip(req, &self.user_headers)?;

        // Trusted clients bypass the user IP check.
        if !is_empty(&self.trusted_client_ips, &self.trusted_client_ranges)
            && contains_ip(
                client_ip,
                &self.trusted_client_ips,
                &self.trusted_client_ranges,
            )
        {
            return Ok(());
        }

        // Empty implies all IPs are allowed.
        if !is_empty(&self.authorized_client_ips, &self.authorized_client_ranges)
            && !contains_ip(
                client_ip,
                &self.authorized_client_ips,
                &self.authorized_client_ranges,
            )
        {
            return Err(render_unauthorized_error(
                user_ip,
                self.error_message_template.as_deref(),
            ));
        }

        if !is_empty(&self.authorized_user_ips, &self.authorized_user_ranges)
            && !contains_ip(
                user_ip,
                &self.authorized_user_ips,
                &self.authorized_user_ranges,
            )
        {
            return Err(render_unauthorized_error(
                user_ip,
                self.error_message_template.as_deref(),
            ));
        }

        Ok(())
    }
}

/// `checker.getConfig` + `buildConfigFromSiteConfig`: `None` when the
/// allowlist is disabled or `auth.allowedIpAddress` is absent.
pub fn compile(site: &SiteConfiguration, env: &AllowlistEnv) -> Option<IpAllowlistConfig> {
    let raw = site.raw().get("auth.allowedIpAddress")?;
    if !raw
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }

    let mut errors: Vec<String> = Vec::new();

    let (user_ips, user_ranges) = parse_ips(&concat(
        strings(raw, "userIpAddress"),
        &env.stable_user_ip_allowlist,
    ))
    .into_parts_checked(&mut errors);

    let (client_ips, client_ranges) = parse_ips(&concat(
        strings(raw, "clientIpAddress"),
        &env.stable_client_ip_allowlist,
    ))
    .into_parts_checked(&mut errors);

    let (mut trusted_client_ips, mut trusted_client_ranges) = parse_ips(&concat(
        strings(raw, "trustedClientIpAddress"),
        &env.stable_trusted_client_ip_allowlist,
    ))
    .into_parts_checked(&mut errors);

    // 🚨 SECURITY: localhost always bypasses the check so liveness probes
    // keep working.
    let (default_trusted_ips, default_trusted_ranges) = parse_ips(
        &DEFAULT_TRUSTED_CLIENT_IPS
            .iter()
            .map(|ip| ip.to_string())
            .collect::<Vec<_>>(),
    )
    .into_parts_checked(&mut errors);
    trusted_client_ips.extend(default_trusted_ips);
    trusted_client_ranges.extend(default_trusted_ranges);

    let mut error_message_template = None;
    let template = raw
        .get("errorMessageTemplate")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if !template.is_empty() {
        match validate_template(template) {
            Ok(()) => error_message_template = Some(template.to_owned()),
            Err(err) => errors.push(err),
        }
    }

    let mut user_headers = strings(raw, "userIpRequestHeaders");
    if !env.user_ip_request_headers_override.is_empty() {
        user_headers = env.user_ip_request_headers_override.clone();
    }

    Some(IpAllowlistConfig {
        authorized_user_ips: user_ips,
        authorized_user_ranges: user_ranges,
        authorized_client_ips: client_ips,
        authorized_client_ranges: client_ranges,
        trusted_client_ips,
        trusted_client_ranges,
        user_headers,
        error_message_template,
        error: multi_error(errors),
    })
}

fn strings(raw: &serde_json::Value, key: &str) -> Vec<String> {
    raw.get(key)
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn concat(mut left: Vec<String>, right: &[String]) -> Vec<String> {
    left.extend_from_slice(right);
    left
}

struct ParsedIps {
    ips: Vec<IpAddr>,
    ranges: Vec<IpNet>,
    errors: Vec<String>,
}

impl ParsedIps {
    /// Collect the `config.err` entries and return the usable half.
    fn into_parts_checked(self, errors: &mut Vec<String>) -> (Vec<IpAddr>, Vec<IpNet>) {
        errors.extend(self.errors);
        (self.ips, self.ranges)
    }
}

/// `parseIPs`: addresses first, then CIDR ranges, accumulating the baseline's
/// per-entry error text.
fn parse_ips(addresses: &[String]) -> ParsedIps {
    let mut ips = Vec::new();
    let mut ranges = Vec::new();
    let mut errors = Vec::new();
    for addr in addresses {
        if let Ok(ip) = addr.parse::<IpAddr>() {
            ips.push(normalize(ip));
            continue;
        }
        match IpNet::parse(addr) {
            Some(range) => ranges.push(range),
            None => errors.push(format!(
                "invalid ip addr: {}: invalid CIDR address: {addr}",
                go_quote(addr)
            )),
        }
    }
    ParsedIps {
        ips,
        ranges,
        errors,
    }
}

fn multi_error(errors: Vec<String>) -> Option<String> {
    match errors.len() {
        0 => None,
        1 => Some(errors[0].clone()),
        count => Some(format!(
            "{count} errors occurred:\n\t* {}",
            errors.join("\n\t* ")
        )),
    }
}

/// `getIP`: returns `(client IP, user IP)`.
fn get_ip<B>(req: &Request<B>, user_headers: &[String]) -> Result<(IpAddr, IpAddr), String> {
    let remote_addr = req
        .extensions()
        .get::<RemoteAddr>()
        .map(|addr| addr.0.as_str())
        .unwrap_or("");

    let host = split_host_port(remote_addr)
        .map_err(|err| format!("get user ip: parse remote addr: address {remote_addr}: {err}"))?;
    let client_ip = host
        .parse::<IpAddr>()
        .map(normalize)
        .map_err(|_| format!("get user ip: invalid remote addr: {}", go_quote(host)))?;

    let user_ip_raw = user_ip_from_headers(req.headers(), user_headers);
    if user_ip_raw.is_empty() {
        return Ok((client_ip, client_ip));
    }

    // The baseline formats the *nil* `net.IP` here, so the message always
    // contains `"<nil>"` (net.IP implements Stringer) regardless of the
    // header value.
    let user_ip = user_ip_raw
        .parse::<IpAddr>()
        .map(normalize)
        .map_err(|_| "get user ip: invalid user ip: \"<nil>\"".to_owned())?;
    Ok((client_ip, user_ip))
}

/// The precedence loop in `getIP`: first non-empty left-most comma element
/// wins. Empty means "fall back to the client IP".
fn user_ip_from_headers(headers: &HeaderMap, user_headers: &[String]) -> String {
    for name in user_headers {
        let Ok(name) = http::HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) else {
            continue;
        };
        let mut parts = value.split(',');
        if let Some(first) = parts.next() {
            if !first.is_empty() {
                return first.trim().to_owned();
            }
        }
    }
    String::new()
}

/// `net.SplitHostPort` for the forms `RemoteAddr` takes.
fn split_host_port(address: &str) -> Result<&str, &'static str> {
    if let Some(rest) = address.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err("missing ']' in address");
        };
        let host = &rest[..end];
        match rest[end + 1..].strip_prefix(':') {
            Some(_) => Ok(host),
            None => Err("missing port in address"),
        }
    } else {
        match address.rsplit_once(':') {
            Some((host, _port)) => {
                if host.contains(':') {
                    Err("too many colons in address")
                } else {
                    Ok(host)
                }
            }
            None => Err("missing port in address"),
        }
    }
}

/// `containsIP`.
fn contains_ip(addr: IpAddr, ips: &[IpAddr], ranges: &[IpNet]) -> bool {
    let addr = normalize(addr);
    ips.contains(&addr) || ranges.iter().any(|range| range.contains(addr))
}

fn is_empty(ips: &[IpAddr], ranges: &[IpNet]) -> bool {
    ips.is_empty() && ranges.is_empty()
}

/// `renderUnAuthorizedErrorMessage`.
fn render_unauthorized_error(user_ip: IpAddr, template: Option<&str>) -> String {
    let message = format!(
        "You are not allowed to access this Sourcegraph instance: {}",
        go_quote(&user_ip.to_string())
    );
    match template {
        None => message,
        Some(template) => {
            render_template(template, &message, &user_ip.to_string()).unwrap_or(message)
        }
    }
}

/// The schema documents exactly two variables: `{{.Error}}` and `{{.UserIP}}`.
/// Go `text/template` supports far more; the harness only exercises these.
fn validate_template(template: &str) -> Result<(), String> {
    render_template(template, "", "").map(|_| ())
}

fn render_template(template: &str, error: &str, user_ip: &str) -> Result<String, String> {
    let mut output = String::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            return Err("template: :1: unclosed action".to_owned());
        };
        let mut action = &after[..end];
        let trim_left = action.starts_with('-');
        if trim_left {
            action = &action[1..];
            let trimmed = output.trim_end();
            output.truncate(trimmed.len());
        }
        let trim_right = action.ends_with('-');
        if trim_right {
            action = &action[..action.len() - 1];
        }
        match action.trim() {
            ".Error" => output.push_str(error),
            ".UserIP" => output.push_str(user_ip),
            other => {
                return Err(format!(
                "template: :1: unsupported action {other:?} (only .Error and .UserIP are ported)"
            ))
            }
        }
        rest = &after[end + 2..];
        if trim_right {
            rest = rest.trim_start();
        }
    }
    output.push_str(rest);
    Ok(output)
}

/// `goImportMetaTag`-independent `%q` formatting for the fields that appear
/// in allowlist errors (ASCII in practice).
fn go_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        match ch {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\t' => quoted.push_str("\\t"),
            '\r' => quoted.push_str("\\r"),
            ch if (ch as u32) < 0x20 || ch as u32 == 0x7f => {
                quoted.push_str(&format!("\\x{:02x}", ch as u32));
            }
            ch => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

/// `ipallowlist.Middleware.Handle`'s rejection: 403 and the `error.html`
/// page rendered with `StatusCode=403`, `StatusText=Forbidden`.
pub fn forbidden_response(error: &str) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(error_page(
        error,
    )))));
    *response.status_mut() = StatusCode::FORBIDDEN;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
}

/// The baseline template, rendered for the only status the middleware uses.
fn error_page(error: &str) -> String {
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n    <head>\n        <meta charset=\"utf-8\" />\n        <title>403 Forbidden - Sourcegraph</title>\n    </head>\n    <body>\n        <div class=\"error\">\n            <h1>\n                <span class=\"status-code\">403</span>\n                <span class=\"status-text\">Forbidden</span>\n            </h1>\n            \n            <pre class=\"error-text\">{error}</pre>\n            \n            <hr />\n            <p>\n                Sorry, there's been a problem. Please <a href=\"mailto:support@sourcegraph.com\">contact us</a> and\n                include the error message above.</strong>\n            </p>\n        </div>\n    </body>\n</html>\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowlist(site: &str, env: AllowlistEnv) -> IpAllowlistConfig {
        let site = SiteConfiguration::parse(site).expect("site config");
        compile(&site, &env).expect("enabled")
    }

    fn request(remote_addr: &str) -> Request<()> {
        let mut request = Request::builder().uri("/").body(()).expect("request");
        request
            .extensions_mut()
            .insert(RemoteAddr(remote_addr.to_owned()));
        request
    }

    #[test]
    fn disabled_by_default() {
        let site = SiteConfiguration::parse("{}").expect("site config");
        assert_eq!(compile(&site, &AllowlistEnv::default()), None);
        let site =
            SiteConfiguration::parse(r#"{"auth.allowedIpAddress":{"enabled":false}}"#).unwrap();
        assert_eq!(compile(&site, &AllowlistEnv::default()), None);
    }

    #[test]
    fn client_allowlist_round_trip() {
        let config = allowlist(
            r#"{"auth.allowedIpAddress":{"enabled":true,"clientIpAddress":["10.0.0.0/8","1.2.3.4"]}}"#,
            AllowlistEnv::default(),
        );
        assert!(config.is_authorized(&request("10.1.2.3:80")).is_ok());
        assert!(config.is_authorized(&request("1.2.3.4:80")).is_ok());
        let denied = config
            .is_authorized(&request("192.168.0.1:80"))
            .unwrap_err();
        assert_eq!(
            denied,
            "You are not allowed to access this Sourcegraph instance: \"192.168.0.1\""
        );
    }

    #[test]
    fn localhost_is_a_trusted_client() {
        let config = allowlist(
            r#"{"auth.allowedIpAddress":{"enabled":true,"userIpAddress":["10.0.0.0/8"]}}"#,
            AllowlistEnv::default(),
        );
        assert!(config.is_authorized(&request("127.0.0.1:9999")).is_ok());
    }

    #[test]
    fn user_headers_use_the_left_most_value() {
        let config = allowlist(
            r#"{"auth.allowedIpAddress":{"enabled":true,"userIpAddress":["10.0.0.1"],"userIpRequestHeaders":["X-Forwarded-For"]}}"#,
            AllowlistEnv::default(),
        );
        let request = Request::builder()
            .uri("/")
            .header("X-Forwarded-For", "10.0.0.1, 192.168.0.1")
            .body(())
            .map(|mut request| {
                request
                    .extensions_mut()
                    .insert(RemoteAddr("192.168.0.9:1".to_owned()));
                request
            })
            .unwrap();
        assert!(config.is_authorized(&request).is_ok());
    }

    #[test]
    fn env_overrides_merge_and_replace() {
        let env = AllowlistEnv {
            stable_client_ip_allowlist: vec!["10.0.0.0/8".to_owned()],
            user_ip_request_headers_override: vec!["CF-Connecting-IP".to_owned()],
            ..AllowlistEnv::default()
        };
        let config = allowlist(
            r#"{"auth.allowedIpAddress":{"enabled":true,"clientIpAddress":["192.168.0.0/16"],"userIpRequestHeaders":["X-Forwarded-For"]}}"#,
            env,
        );
        assert_eq!(config.user_headers, vec!["CF-Connecting-IP"]);
        assert!(config.is_authorized(&request("10.9.9.9:1")).is_ok());
    }

    #[test]
    fn invalid_entries_are_reported_but_usable_entries_still_apply() {
        let config = allowlist(
            r#"{"auth.allowedIpAddress":{"enabled":true,"userIpAddress":["invalid-ip","10.0.0.1"]}}"#,
            AllowlistEnv::default(),
        );
        assert_eq!(
            config.error.as_deref(),
            Some("invalid ip addr: \"invalid-ip\": invalid CIDR address: invalid-ip")
        );
        assert!(config.is_authorized(&request("1.2.3.4:1")).is_err());
    }

    #[test]
    fn remote_addr_errors_match_the_baseline() {
        let config = allowlist(
            r#"{"auth.allowedIpAddress":{"enabled":true,"userIpAddress":["10.0.0.1"]}}"#,
            AllowlistEnv::default(),
        );
        assert_eq!(
            config.is_authorized(&request("1.2.3.4")).unwrap_err(),
            "get user ip: parse remote addr: address 1.2.3.4: missing port in address"
        );
        assert_eq!(
            config.is_authorized(&request("bad:1")).unwrap_err(),
            "get user ip: invalid remote addr: \"bad\""
        );
    }

    #[test]
    fn custom_template_uses_documented_variables() {
        let config = allowlist(
            r#"{"auth.allowedIpAddress":{"enabled":true,"userIpAddress":["10.0.0.1"],"errorMessageTemplate":"Blocked {{.UserIP}}: {{.Error}}"}}"#,
            AllowlistEnv::default(),
        );
        let message = config.is_authorized(&request("1.2.3.4:1")).unwrap_err();
        assert_eq!(
            message,
            "Blocked 1.2.3.4: You are not allowed to access this Sourcegraph instance: \"1.2.3.4\""
        );
    }

    #[test]
    fn forbidden_page_matches_the_baseline_template() {
        let page = error_page("blocked");
        assert!(page.starts_with("<!DOCTYPE html>\n<html lang=\"en\">"));
        assert!(page.contains("<pre class=\"error-text\">blocked</pre>"));
        assert!(page.ends_with("</html>\n"));
    }
}
