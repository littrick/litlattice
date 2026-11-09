#![forbid(unsafe_code)]

//! Port of `internal/conf/confdefaults/confdefaults.go` and
//! `internal/conf/deploy/deploytype.go`'s selection logic
//! (`internal/conf/computed.go:41-53`).
//!
//! The baseline database seeds the deployment's default site configuration
//! when it has none, so `conf.Get()` is never empty in a fresh instance. The
//! port has no configuration service yet; without `SITE_CONFIG_FILE` it uses
//! the same default document, which is what keeps
//! [`crate::auth::forbidden`] from rejecting every request on a fresh
//! instance (the default has a `builtin` provider).
//!
//! `SITE_CONFIG_FILE` replaces this default wholesale, matching
//! `WriteWithOverride` (the override is not merged with the default).

use sg_config::SiteConfiguration;

/// `DEPLOY_TYPE` is read with `os.Getenv` in the baseline, not `env.Get`.
pub const DEPLOY_TYPE_VAR: &str = "DEPLOY_TYPE";

/// `deploy.Type()`'s default when `DEPLOY_TYPE` is unset.
pub const DEFAULT_DEPLOY_TYPE: &str = "kubernetes";

/// `confdefaults.DevAndTesting.Site`.
const DEV_AND_TESTING: &str = r#"{
	"auth.providers": [
		{
			"type": "builtin",
			"allowSignup": true
		}
	],
}"#;

/// `confdefaults.DockerContainer.Site`.
const DOCKER_CONTAINER: &str = r#"{
	// The externally accessible URL for Sourcegraph (i.e., what you type into your browser)
	// This is required to be configured for Sourcegraph to work correctly.
	// "externalURL": "https://sourcegraph.example.com",

	"auth.providers": [
		{
			"type": "builtin",
			"allowSignup": true
		}
	]
}"#;

/// `confdefaults.KubernetesOrDockerComposeOrPureDocker.Site`.
const KUBERNETES_OR_DOCKER_COMPOSE_OR_PURE_DOCKER: &str = r#"{
	// The externally accessible URL for Sourcegraph (i.e., what you type into your browser)
	// This is required to be configured for Sourcegraph to work correctly.
	// "externalURL": "https://sourcegraph.example.com",

	// The authentication provider to use for identifying and signing in users.
	// Only one entry is supported.
	//
	// The builtin auth provider with signup disallowed (shown below) means that
	// after the initial site admin signs in, all other users must be invited.
	//
	// Other providers are documented at https://sourcegraph.com/docs/admin/auth.
	"auth.providers": [
		{
			"type": "builtin",
			"allowSignup": false
		}
	],
}"#;

/// `deploy.Type()` without its process-wide mock: an empty value means
/// `kubernetes`.
pub fn resolve_deploy_type(value: &str) -> &str {
    if value.is_empty() {
        DEFAULT_DEPLOY_TYPE
    } else {
        value
    }
}

/// `defaultConfigForDeployment`: the raw site configuration document for a
/// deployment type. An unknown type is fatal in the baseline
/// (`stdlog.Fatalf`); the port returns the same message as a startup error.
pub fn default_site_config_json(deploy_type: &str) -> Result<&'static str, String> {
    match deploy_type {
        "dev" => Ok(DEV_AND_TESTING),
        "docker-container" => Ok(DOCKER_CONTAINER),
        // `IsDeployTypeKubernetes` includes these aliases.
        "kubernetes" | "k8s" | "cluster" | "helm" | "kustomize" | "k3s" | "appliance"
        | "docker-compose" | "pure-docker" => Ok(KUBERNETES_OR_DOCKER_COMPOSE_OR_PURE_DOCKER),
        other => Err(format!(
            "The 'DEPLOY_TYPE' environment variable is invalid. Expected one of: \
             \"kubernetes\", \"docker-compose\", \"pure-docker\", \"docker-container\", \
             \"dev\", \"helm\". Got: {other:?}"
        )),
    }
}

/// `defaultConfigForDeployment` resolved and parsed.
pub fn default_site_config(value: &str) -> Result<SiteConfiguration, String> {
    let json = default_site_config_json(resolve_deploy_type(value))?;
    SiteConfiguration::parse(json).map_err(|err| err.to_string())
}

/// `confdefaults.DevAndTesting.Site` after `conf.RedactAndHashSecrets`
/// (`internal/conf/validate.go:371-465`).
const DEV_AND_TESTING_REDACTED: &str = r#"{
  "auth.providers": [
    {
      "allowSignup": true,
      "type": "builtin"
    }
  ],
}"#;

/// `confdefaults.DockerContainer.Site` after `RedactAndHashSecrets`.
const DOCKER_CONTAINER_REDACTED: &str = r#"{
  // The externally accessible URL for Sourcegraph (i.e., what you type into your browser)
  // This is required to be configured for Sourcegraph to work correctly.
  // "externalURL": "https://sourcegraph.example.com",
  "auth.providers": [
    {
      "allowSignup": true,
      "type": "builtin"
    }
  ]
}"#;

/// `confdefaults.KubernetesOrDockerComposeOrPureDocker.Site` after
/// `RedactAndHashSecrets`. The `builtin` provider's `allowSignup: false` is
/// omitted from the redacted copy (`allowSignup` has `omitempty`), and
/// `jsonc.Format` keeps the comments, drops blank lines and indents with two
/// spaces.
const KUBERNETES_OR_DOCKER_COMPOSE_OR_PURE_DOCKER_REDACTED: &str = r#"{
  // The externally accessible URL for Sourcegraph (i.e., what you type into your browser)
  // This is required to be configured for Sourcegraph to work correctly.
  // "externalURL": "https://sourcegraph.example.com",
  // The authentication provider to use for identifying and signing in users.
  // Only one entry is supported.
  //
  // The builtin auth provider with signup disallowed (shown below) means that
  // after the initial site admin signs in, all other users must be invited.
  //
  // Other providers are documented at https://sourcegraph.com/docs/admin/auth.
  "auth.providers": [
    {
      "type": "builtin"
    }
  ],
}"#;

/// `RedactAndHashSecrets(Default).Site` for the deployment's default
/// document.
///
/// The baseline redacts with `jsonc`, which is not ported: the default
/// documents are fixed constants, so their redacted forms are stored as
/// byte-for-byte snapshots (verified against the Go implementation by
/// `run_conf_db_source_diff.sh`). Arbitrary user-written configurations are
/// still not accepted (the DB write path is unported).
pub fn default_site_config_redacted_json(deploy_type: &str) -> Result<&'static str, String> {
    match resolve_deploy_type(deploy_type) {
        "dev" => Ok(DEV_AND_TESTING_REDACTED),
        "docker-container" => Ok(DOCKER_CONTAINER_REDACTED),
        // Same aliases as `default_site_config_json`.
        "kubernetes" | "k8s" | "cluster" | "helm" | "kustomize" | "k3s" | "appliance"
        | "docker-compose" | "pure-docker" => {
            Ok(KUBERNETES_OR_DOCKER_COMPOSE_OR_PURE_DOCKER_REDACTED)
        }
        other => Err(format!(
            "The 'DEPLOY_TYPE' environment variable is invalid. Expected one of: \
             \"kubernetes\", \"docker-compose\", \"pure-docker\", \"docker-container\", \
             \"dev\", \"helm\". Got: {other:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn providers(site: &SiteConfiguration) -> &[serde_json::Value] {
        site.raw()
            .get("auth.providers")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .expect("auth.providers")
    }

    #[test]
    fn unset_and_kubernetes_use_the_kubernetes_default() {
        for value in [
            "",
            "kubernetes",
            "k8s",
            "cluster",
            "helm",
            "kustomize",
            "k3s",
            "appliance",
        ] {
            let site = default_site_config(value).expect("default");
            assert_eq!(providers(&site).len(), 1, "{value}");
            assert_eq!(
                providers(&site)[0]
                    .get("allowSignup")
                    .and_then(serde_json::Value::as_bool),
                Some(false),
                "{value}"
            );
        }
    }

    #[test]
    fn docker_compose_and_pure_docker_share_the_kubernetes_default() {
        for value in ["docker-compose", "pure-docker"] {
            let site = default_site_config(value).expect("default");
            assert_eq!(providers(&site).len(), 1, "{value}");
        }
    }

    #[test]
    fn dev_and_docker_container_allow_signup() {
        for value in ["dev", "docker-container"] {
            let site = default_site_config(value).expect("default");
            assert_eq!(
                providers(&site)[0]
                    .get("allowSignup")
                    .and_then(serde_json::Value::as_bool),
                Some(true),
                "{value}"
            );
        }
    }

    #[test]
    fn invalid_deploy_type_matches_the_baseline_message() {
        let err = default_site_config("bogus").expect_err("invalid");
        assert_eq!(
            err,
            "The 'DEPLOY_TYPE' environment variable is invalid. Expected one of: \
             \"kubernetes\", \"docker-compose\", \"pure-docker\", \"docker-container\", \
             \"dev\", \"helm\". Got: \"bogus\""
        );
        let err = default_site_config_redacted_json("bogus").expect_err("invalid");
        assert!(err.ends_with("Got: \"bogus\""), "{err}");
    }

    #[test]
    fn redacted_snapshots_are_valid_jsonc_and_share_the_raw_shape() {
        for value in ["dev", "docker-container", "kubernetes", "docker-compose"] {
            let raw = default_site_config_json(value).expect("raw");
            let redacted = default_site_config_redacted_json(value).expect("redacted");
            for document in [raw, redacted] {
                // JSONC with comments: `SiteConfiguration::parse` strips them.
                let site = SiteConfiguration::parse(document).expect("parse");
                assert_eq!(providers(&site).len(), 1, "{value}");
            }
            assert!(!redacted.contains("allowSignup\": false"), "{value}");
        }
    }
}
