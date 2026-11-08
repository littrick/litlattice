#![forbid(unsafe_code)]

//! `internal/extsvc` rate-limit extraction: `GetLimitFromConfig`,
//! `GetDefaultRateLimit`, `limitOrInf` and the `Variant` table they switch on.
//!
//! The caller decrypts `external_services.config` first (`svc.Config.Decrypt`)
//! and hands the plaintext JSON here, mirroring
//! `ParseEncryptableConfig` + `GetLimitFromConfig` in the baseline. `rate.Limit`
//! is a float64 in requests per *second*; `rate.Inf` is
//! [`f64::INFINITY`].

/// `extsvc.Variant`: the external-service variants that have a schema
/// prototype (`variantValuesMap`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Variant {
    AwsCodeCommit,
    AzureDevOps,
    BitbucketCloud,
    BitbucketServer,
    Gerrit,
    GitHub,
    GitLab,
    Gitolite,
    GoPackages,
    JvmPackages,
    NpmPackages,
    Other,
    Pagure,
    Perforce,
    Phabricator,
    PythonPackages,
    RubyPackages,
    RustPackages,
}

impl Variant {
    /// `Variant.AsKind`: the value stored in `external_services.kind`.
    pub fn kind(self) -> &'static str {
        match self {
            Self::AwsCodeCommit => "AWSCODECOMMIT",
            Self::AzureDevOps => "AZUREDEVOPS",
            Self::BitbucketCloud => "BITBUCKETCLOUD",
            Self::BitbucketServer => "BITBUCKETSERVER",
            Self::Gerrit => "GERRIT",
            Self::GitHub => "GITHUB",
            Self::GitLab => "GITLAB",
            Self::Gitolite => "GITOLITE",
            Self::GoPackages => "GOMODULES",
            Self::JvmPackages => "JVMPACKAGES",
            Self::NpmPackages => "NPMPACKAGES",
            Self::Other => "OTHER",
            Self::Pagure => "PAGURE",
            Self::Perforce => "PERFORCE",
            Self::Phabricator => "PHABRICATOR",
            Self::PythonPackages => "PYTHONPACKAGES",
            Self::RubyPackages => "RUBYPACKAGES",
            Self::RustPackages => "RUSTPACKAGES",
        }
    }

    /// `Variant.AsType`: the value stored in `repo.external_service_type`.
    pub fn type_name(self) -> &'static str {
        match self {
            Self::AwsCodeCommit => "awscodecommit",
            Self::AzureDevOps => "azuredevops",
            Self::BitbucketCloud => "bitbucketCloud",
            Self::BitbucketServer => "bitbucketServer",
            Self::Gerrit => "gerrit",
            Self::GitHub => "github",
            Self::GitLab => "gitlab",
            Self::Gitolite => "gitolite",
            Self::GoPackages => "goModules",
            Self::JvmPackages => "jvmPackages",
            Self::NpmPackages => "npmPackages",
            Self::Other => "other",
            Self::Pagure => "pagure",
            Self::Perforce => "perforce",
            Self::Phabricator => "phabricator",
            Self::PythonPackages => "pythonPackages",
            Self::RubyPackages => "rubyPackages",
            Self::RustPackages => "rustPackages",
        }
    }

    /// `extsvc.VariantValueOf`: case-insensitive match against either the kind
    /// or the type spelling; `None` is the baseline's "no Variant found".
    pub fn from_kind(input: &str) -> Option<Self> {
        ALL_VARIANTS.iter().copied().find(|variant| {
            variant.kind().eq_ignore_ascii_case(input)
                || variant.type_name().eq_ignore_ascii_case(input)
        })
    }
}

/// Every variant in `variantValuesMap`; no two share a kind or type spelling,
/// so the lookup order is irrelevant.
pub const ALL_VARIANTS: [Variant; 18] = [
    Variant::AwsCodeCommit,
    Variant::AzureDevOps,
    Variant::BitbucketCloud,
    Variant::BitbucketServer,
    Variant::Gerrit,
    Variant::GitHub,
    Variant::GitLab,
    Variant::Gitolite,
    Variant::GoPackages,
    Variant::JvmPackages,
    Variant::NpmPackages,
    Variant::Other,
    Variant::Pagure,
    Variant::Perforce,
    Variant::Phabricator,
    Variant::PythonPackages,
    Variant::RubyPackages,
    Variant::RustPackages,
];

/// `extsvc.ErrRateLimitUnsupported`: rate limiting is not implemented for the
/// code host. `syncServices` skips these services silently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrRateLimitUnsupported {
    pub codehost_kind: String,
}

impl std::fmt::Display for ErrRateLimitUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "internal rate limiting not supported for {}",
            self.codehost_kind
        )
    }
}

impl std::error::Error for ErrRateLimitUnsupported {}

/// `limitOrInf`: an enabled limit is "requests per hour" divided down to
/// requests per second; a disabled one is unlimited.
pub fn limit_or_inf(enabled: bool, per_hour: f64) -> f64 {
    if enabled {
        per_hour / 3600.0
    } else {
        f64::INFINITY
    }
}

/// `GetDefaultRateLimit`: the requests-per-second limit per code host, in the
/// baseline's own order (the default is `rate.Inf`).
pub fn get_default_rate_limit(variant: Variant) -> f64 {
    match variant {
        // Use an infinite rate limiter. GitHub has an external rate limiter we
        // obey.
        Variant::GitHub => f64::INFINITY,
        Variant::GitLab => f64::INFINITY,
        // 8/s is the default limit we enforce.
        Variant::BitbucketServer => 8.0,
        Variant::BitbucketCloud => 2.0,
        Variant::Perforce => 5000.0 / 3600.0,
        Variant::JvmPackages => 2.0,
        Variant::Pagure => 8.0,
        Variant::NpmPackages => 6000.0 / 3600.0,
        // Unlike the GitHub or GitLab APIs, the public npm registry (i.e.
        // https://proxy.golang.org) doesn't document an enforced req/s rate
        // limit AND we do a lot more individual requests in comparison since
        // they don't offer enough batch APIs.
        Variant::GoPackages => 57600.0 / 3600.0,
        // Unlike the GitHub or GitLab APIs, the pypi.org doesn't document an
        // enforced req/s rate limit.
        Variant::PythonPackages => 57600.0 / 3600.0,
        // The crates.io CDN has no rate limits.
        Variant::RustPackages => 100.0,
        // The rubygems.org API allows 10 rps.
        Variant::RubyPackages => 10.0,
        Variant::AzureDevOps => f64::INFINITY,
        _ => f64::INFINITY,
    }
}

/// Where the per-connection rate-limit block lives in the parsed config.
#[derive(Debug, Clone, Copy)]
enum RateLimitPath {
    /// `config.rateLimit`.
    RateLimit,
    /// `config.maven.rateLimit` (`JVMPackagesConnection.Maven.RateLimit`).
    MavenRateLimit,
}

/// `GetLimitFromConfig`: the configured limit if the service defines one,
/// else the code-host default. Returns `(limit, is_default)`.
///
/// The baseline switches on the *parsed prototype type*, which is selected by
/// the kind through `getConfigPrototype`; the two are equivalent, and the set
/// of kinds with a case here is exactly the set the baseline handles.
pub fn get_limit_from_config(
    config: &serde_json::Value,
    variant: Variant,
) -> Result<(f64, bool), ErrRateLimitUnsupported> {
    let path = match variant {
        Variant::GitLab
        | Variant::GitHub
        | Variant::BitbucketServer
        | Variant::BitbucketCloud
        | Variant::Pagure
        | Variant::NpmPackages
        | Variant::GoPackages
        | Variant::PythonPackages
        | Variant::RustPackages
        | Variant::RubyPackages
        | Variant::AzureDevOps => RateLimitPath::RateLimit,
        Variant::JvmPackages => RateLimitPath::MavenRateLimit,
        other => {
            return Err(ErrRateLimitUnsupported {
                codehost_kind: other.kind().to_owned(),
            })
        }
    };

    let default_limit = get_default_rate_limit(variant);
    let rate_limit = match path {
        RateLimitPath::RateLimit => config.get("rateLimit"),
        RateLimitPath::MavenRateLimit => {
            config.get("maven").and_then(|maven| maven.get("rateLimit"))
        }
    };
    let Some(rate_limit) = rate_limit.and_then(serde_json::Value::as_object) else {
        // Not defined: the code host default applies.
        return Ok((default_limit, true));
    };

    // Defined: `enabled` / `requestsPerHour` are the struct's zero values when
    // absent, like `json.Unmarshal` into `schema.*RateLimit`.
    let enabled = rate_limit
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let per_hour = rate_limit
        .get("requestsPerHour")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);
    Ok((limit_or_inf(enabled, per_hour), false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn variants_match_kind_and_type_case_insensitively() {
        assert_eq!(Variant::from_kind("GITHUB"), Some(Variant::GitHub));
        assert_eq!(Variant::from_kind("github"), Some(Variant::GitHub));
        assert_eq!(Variant::from_kind("GitHub"), Some(Variant::GitHub));
        assert_eq!(
            Variant::from_kind("bitbucketServer"),
            Some(Variant::BitbucketServer)
        );
        assert_eq!(Variant::from_kind("GOMODULES"), Some(Variant::GoPackages));
        assert_eq!(Variant::from_kind("goModules"), Some(Variant::GoPackages));
        assert_eq!(Variant::from_kind("FOO"), None);
    }

    #[test]
    fn default_limits_match_the_baseline_table() {
        assert_eq!(get_default_rate_limit(Variant::GitHub), f64::INFINITY);
        assert_eq!(get_default_rate_limit(Variant::GitLab), f64::INFINITY);
        assert_eq!(get_default_rate_limit(Variant::BitbucketServer), 8.0);
        assert_eq!(get_default_rate_limit(Variant::BitbucketCloud), 2.0);
        assert_eq!(get_default_rate_limit(Variant::JvmPackages), 2.0);
        assert_eq!(get_default_rate_limit(Variant::Pagure), 8.0);
        assert_eq!(get_default_rate_limit(Variant::RustPackages), 100.0);
        assert_eq!(get_default_rate_limit(Variant::RubyPackages), 10.0);
        assert_eq!(get_default_rate_limit(Variant::AzureDevOps), f64::INFINITY);
        assert_eq!(get_default_rate_limit(Variant::Perforce), 5000.0 / 3600.0);
        assert_eq!(get_default_rate_limit(Variant::GoPackages), 16.0);
    }

    #[test]
    fn absent_rate_limit_uses_the_default() {
        let config = json!({"url": "https://github.com"});
        assert_eq!(
            get_limit_from_config(&config, Variant::GitHub),
            Ok((f64::INFINITY, true))
        );
        assert_eq!(
            get_limit_from_config(&config, Variant::BitbucketServer),
            Ok((8.0, true))
        );
        // `rateLimit: null` leaves the pointer nil, i.e. the default.
        let config = json!({"rateLimit": null});
        assert_eq!(
            get_limit_from_config(&config, Variant::GitLab),
            Ok((f64::INFINITY, true))
        );
    }

    #[test]
    fn configured_rate_limit_is_divided_down_to_per_second() {
        let config = json!({"rateLimit": {"enabled": true, "requestsPerHour": 150.0}});
        assert_eq!(
            get_limit_from_config(&config, Variant::GitHub),
            Ok((150.0 / 3600.0, false))
        );
        // Disabled means unlimited regardless of requestsPerHour.
        let config = json!({"rateLimit": {"enabled": false, "requestsPerHour": 150.0}});
        assert_eq!(
            get_limit_from_config(&config, Variant::GitLab),
            Ok((f64::INFINITY, false))
        );
        // The struct's zero values when the fields are absent: a defined but
        // disabled rate limit (`enabled` defaults to false) is unlimited.
        let config = json!({"rateLimit": {}});
        assert_eq!(
            get_limit_from_config(&config, Variant::BitbucketCloud),
            Ok((f64::INFINITY, false))
        );
    }

    #[test]
    fn jvm_packages_read_the_maven_rate_limit() {
        let config = json!({"maven": {"rateLimit": {"enabled": true, "requestsPerHour": 7200}}});
        assert_eq!(
            get_limit_from_config(&config, Variant::JvmPackages),
            Ok((2.0, false))
        );
        let config = json!({"maven": {}});
        assert_eq!(
            get_limit_from_config(&config, Variant::JvmPackages),
            Ok((2.0, true))
        );
    }

    #[test]
    fn kinds_without_a_case_are_unsupported() {
        for variant in [
            Variant::AwsCodeCommit,
            Variant::Gerrit,
            Variant::Gitolite,
            Variant::Other,
            Variant::Perforce,
            Variant::Phabricator,
        ] {
            let err = get_limit_from_config(&json!({}), variant).expect_err("unsupported");
            assert_eq!(
                err.to_string(),
                format!(
                    "internal rate limiting not supported for {}",
                    variant.kind()
                )
            );
        }
    }

    #[test]
    fn per_hour_to_per_second_conversion_truncates_like_go() {
        // `int32(limit * 3600)` in `syncServices`; float64 round-trips here.
        for (per_hour, expected) in [(150.0, 150), (500.0, 500), (999.0, 999), (0.5, 0)] {
            let limit = limit_or_inf(true, per_hour);
            assert_eq!((limit * 3600.0) as i32, expected, "per_hour={per_hour}");
        }
    }
}
