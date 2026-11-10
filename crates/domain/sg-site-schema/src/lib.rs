#![forbid(unsafe_code)]

//! Site configuration JSON Schema validation + `conf.MustValidateDefaults`.
//!
//! Baseline: `internal/conf/validate.go` — the site branch of
//! `conf.Validate` (`internal/jsonc.Parse` then
//! `doValidate(siteJSON, schema.SiteSchemaJSON)`, `:168-185`) and
//! `MustValidateDefaults`/`mustValidate` (`:556-574`). The embedded
//! `assets/site.schema.json` is a verbatim copy of
//! `schema/site.schema.json` (`schema/stringdata.go:98-101`).
//!
//! The miniature gojsonschema clone is shared with `sg-settings-schema`
//! (M9.8fs); see `validator.rs` there for the error-message and scoring
//! contract. External service validation and `validateCustomRaw` are out of
//! scope (see `docs/overviews/m9_8gp_site_schema_validation_overview.md`).

use std::fmt;
use std::sync::OnceLock;

use sg_settings_schema::validator::Schema;

/// `schema.SiteSchemaJSON` (`schema/site.schema.json`).
pub const SITE_SCHEMA_JSON: &str = include_str!("../assets/site.schema.json");

/// `internal/conf/confdefaults/confdefaults.go` — the documents
/// `MustValidateDefaults` validates, in baseline order.
pub mod confdefaults {
    /// `confdefaults.DevAndTesting.Site`.
    pub const DEV_AND_TESTING_SITE: &str = r#"{
	"auth.providers": [
		{
			"type": "builtin",
			"allowSignup": true
		}
	],
}"#;

    /// `confdefaults.DockerContainer.Site`.
    pub const DOCKER_CONTAINER_SITE: &str = r#"{
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
    pub const KUBERNETES_OR_DOCKER_COMPOSE_OR_PURE_DOCKER_SITE: &str = r#"{
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

    /// Names and documents in `MustValidateDefaults` order.
    pub const DEFAULT_CONFIGS: &[(&str, &str)] = &[
        ("DevAndTesting", DEV_AND_TESTING_SITE),
        ("DockerContainer", DOCKER_CONTAINER_SITE),
        (
            "KubernetesOrDockerComposeOrPureDocker",
            KUBERNETES_OR_DOCKER_COMPOSE_OR_PURE_DOCKER_SITE,
        ),
    ];
}

/// Error returned by [`validate_site`]; `Display` is the baseline
/// `internal/jsonc.Parse` error text (`failed to parse JSON: [...]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// Site branch of `conf.Validate`: JSONC parse + `site.schema.json`
/// gojsonschema problems.
pub fn validate_site(input: &str) -> Result<Vec<String>, Error> {
    site_schema().validate_jsonc(input).map_err(Error)
}

/// Equivalent of one `mustValidate(name, cfg)` call from
/// `MustValidateDefaults`: `Ok(())` when the document passes, otherwise the
/// exact panic text the baseline would produce.
pub fn validate_default(name: &str, document: &str) -> Result<(), String> {
    match validate_site(document) {
        Err(error) => Err(format!("Error with {name:?}: {error}")),
        Ok(problems) if !problems.is_empty() => Err(format!(
            "conf: problems with default configuration for {name:?}:\n  {}",
            problems.join("\n  ")
        )),
        Ok(_) => Ok(()),
    }
}

/// `conf.MustValidateDefaults`: validate all three default deployment
/// documents in baseline order and return the first failure. The baseline
/// panics; the port returns the panic text as a startup error.
pub fn must_validate_defaults() -> Result<(), String> {
    for (name, document) in confdefaults::DEFAULT_CONFIGS {
        validate_default(name, document)?;
    }
    Ok(())
}

fn site_schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Schema::parse(SITE_SCHEMA_JSON).expect("embedded site schema is valid JSON")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_documents_use_the_root_special_case() {
        for input in ["", "// nothing", "[]", "\"x\"", "null", "3"] {
            assert_eq!(
                validate_site(input).expect("parsed"),
                vec!["must be a JSON object (use {} for empty)".to_owned()],
                "{input:?}"
            );
        }
    }

    #[test]
    fn accepts_typical_and_unknown_fields() {
        let typical = r#"{
            "externalURL": "https://sourcegraph.example.com",
            "auth.providers": [{"type": "builtin", "allowSignup": true}],
            "email.address": "dev@example.com",
            "email.smtp": {
                "host": "smtp.example.com",
                "port": 465,
                "authentication": "PLAIN"
            },
            "corsOrigin": "https://git.example.com",
            "update.channel": "release",
        }"#;
        assert!(validate_site(typical).expect("parsed").is_empty());
        assert!(validate_site("{\"totally.unknown\": {\"x\": 1}}")
            .expect("parsed")
            .is_empty());
    }

    #[test]
    fn reports_parse_errors() {
        assert_eq!(
            validate_site("{\"externalURL\": tru}"),
            Err(Error(
                "failed to parse JSON: [InvalidSymbol ValueExpected]".to_owned()
            ))
        );
    }

    #[test]
    fn reports_types_enums_and_required() {
        assert_eq!(
            validate_site("{\"externalURL\": 123}").expect("parsed"),
            vec!["externalURL: Invalid type. Expected: string, given: integer".to_owned()]
        );
        assert_eq!(
            validate_site("{\"externalURL\": null}").expect("parsed"),
            vec!["externalURL: Invalid type. Expected: string, given: null".to_owned()]
        );
        assert_eq!(
            validate_site("{\"email.smtp\": {}}").expect("parsed"),
            vec![
                "email.smtp: host is required".to_owned(),
                "email.smtp: port is required".to_owned(),
                "email.smtp: authentication is required".to_owned(),
            ]
        );
        assert_eq!(
            validate_site("{\"update.channel\": \"bogus\"}").expect("parsed"),
            vec![
                "update.channel: update.channel must be one of the following: \
                 \"release\", \"none\""
                    .to_owned()
            ]
        );
    }

    #[test]
    fn reports_patterns_bounds_and_formats() {
        assert_eq!(
            validate_site("{\"corsOrigin\": \"not a url\"}").expect("parsed"),
            vec!["corsOrigin: Does not match pattern \
                 '^((https?:\\/\\/[\\w-\\.]+)( https?:\\/\\/[\\w-\\.]+)*)|\\*$'"
                .to_owned()]
        );
        assert_eq!(
            validate_site("{\"outboundRequestLogLimit\": -1}").expect("parsed"),
            vec!["outboundRequestLogLimit: Must be greater than or equal to 0".to_owned()]
        );
        assert_eq!(
            validate_site("{\"outboundRequestLogLimit\": 501}").expect("parsed"),
            vec!["outboundRequestLogLimit: Must be less than or equal to 500".to_owned()]
        );
        assert_eq!(
            validate_site("{\"email.address\": \"not-an-email\"}").expect("parsed"),
            vec!["email.address: Does not match format 'email'".to_owned()]
        );
        assert_eq!(
            validate_site("{\"notifications\": [{\"key\": \"\", \"message\": \"m\"}]}")
                .expect("parsed"),
            vec![
                "notifications.0.key: String length must be greater than or equal to 1".to_owned()
            ]
        );
        assert_eq!(
            validate_site("{\"cody.contextFilters\": {\"include\": []}}").expect("parsed"),
            vec!["cody.contextFilters.include: Array must have at least 1 items".to_owned()]
        );
        assert_eq!(
            validate_site("{\"cody.contextFilters\": {}}").expect("parsed"),
            vec!["cody.contextFilters: Must have at least 1 properties".to_owned()]
        );
    }

    #[test]
    fn reports_one_of_not_any_of_and_dependencies() {
        assert_eq!(
            validate_site("{\"auth.providers\": [{\"type\": \"bogus\"}]}").expect("parsed"),
            vec![
                "auth.providers.0: Must validate one and only one schema (oneOf)".to_owned(),
                "auth.providers.0.type: auth.providers.0.type does not match: \"builtin\""
                    .to_owned(),
                "auth.providers.0.type: auth.providers.0.type must be one of the \
                 following: \"azureDevOps\", \"bitbucketcloud\", \"bitbucketserver\", \
                 \"builtin\", \"gerrit\", \"github\", \"gitlab\", \"http-header\", \
                 \"openidconnect\", \"saml\""
                    .to_owned(),
            ]
        );
        assert_eq!(
            validate_site("{\"completions\": {\"chatModel\": \"claude-2\"}}").expect("parsed"),
            vec!["completions.chatModel: Must not validate the schema (not)".to_owned()]
        );
        assert_eq!(
            validate_site(
                "{\"experimentalFeatures\": {\"search.index.revisions\": \
                 [{\"revisions\": [\"r\"]}]}}"
            )
            .expect("parsed"),
            vec![
                "experimentalFeatures.search.index.revisions.0: Must validate at least \
                 one schema (anyOf)"
                    .to_owned(),
                "experimentalFeatures.search.index.revisions.0: name is required".to_owned(),
            ]
        );
        assert_eq!(
            validate_site(
                "{\"auth.providers\": [{\"type\": \"saml\", \
                 \"serviceProviderCertificate\": \"cert\"}]}"
            )
            .expect("parsed"),
            vec![
                "auth.providers.0: Must validate one and only one schema (oneOf)".to_owned(),
                "auth.providers.0: Has a dependency on serviceProviderPrivateKey".to_owned(),
                "auth.providers.0.serviceProviderCertificate: Does not match pattern \
                 '^(-----BEGIN CERTIFICATE-----\n|\\$)'"
                    .to_owned(),
            ]
        );
    }

    #[test]
    fn default_documents_pass() {
        assert_eq!(must_validate_defaults(), Ok(()));
        for (name, document) in confdefaults::DEFAULT_CONFIGS {
            assert_eq!(validate_default(name, document), Ok(()));
        }
    }

    #[test]
    fn default_failure_matches_baseline_panic_text() {
        assert_eq!(
            validate_default("Synthetic", "[]"),
            Err(
                "conf: problems with default configuration for \"Synthetic\":\n  \
                 must be a JSON object (use {} for empty)"
                    .to_owned()
            )
        );
        assert_eq!(
            validate_default("Synthetic", "{\"externalURL\": tru}"),
            Err(
                "Error with \"Synthetic\": failed to parse JSON: [InvalidSymbol ValueExpected]"
                    .to_owned()
            )
        );
    }
}
