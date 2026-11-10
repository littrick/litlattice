#![forbid(unsafe_code)]

//! `internal/conf.ValidateSettings` + `internal/jsonc.Parse`
//! (`internal/conf/validate.go:484-554`, `internal/jsonc/jsonc.go:16-39`).
//!
//! The embedded `assets/settings.schema.json` is a verbatim copy of
//! `schema/settings.schema.json` (`go:embed` in `schema/stringdata.go`).
//! Validation reimplements the subset of
//! `github.com/xeipuuv/gojsonschema v1.2.0` used by that schema, keeping
//! gojsonschema's messages, field paths and error order.
//!
//! [`validator`] is shared with the M9.8gp `sg-site-schema` crate, which
//! reuses the same miniature validator for `schema/site.schema.json`.

mod decimal;
pub mod validator;

use std::sync::OnceLock;

use validator::Schema;

/// `schema.SettingsSchemaJSON` (`schema/settings.schema.json`).
pub const SETTINGS_SCHEMA_JSON: &str = include_str!("../assets/settings.schema.json");

/// `conf.ValidateSettings`: JSONC syntax + settings JSON Schema problems.
pub fn validate_settings(jsonc_input: &str) -> Vec<String> {
    match settings_schema().validate_jsonc(jsonc_input) {
        Ok(problems) => problems,
        Err(error) => vec![error],
    }
}

fn settings_schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Schema::parse(SETTINGS_SCHEMA_JSON).expect("embedded settings schema is valid JSON")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_or_commented_document_is_not_an_object() {
        assert_eq!(
            validate_settings(""),
            vec!["must be a JSON object (use {} for empty)".to_owned()]
        );
        assert_eq!(
            validate_settings("// only a comment"),
            vec!["must be a JSON object (use {} for empty)".to_owned()]
        );
        assert_eq!(
            validate_settings("[1]"),
            vec!["must be a JSON object (use {} for empty)".to_owned()]
        );
    }

    #[test]
    fn accepts_valid_settings() {
        assert!(validate_settings("{\"search.contextLines\": 2}").is_empty());
        assert!(validate_settings("{\"a\": 1,}").is_empty());
        assert!(validate_settings("{\"unknown.key\": {\"nested\": true}}").is_empty());
    }

    #[test]
    fn reports_parse_errors_with_code_names() {
        assert_eq!(
            validate_settings("{\"a\": tru}"),
            vec!["failed to parse JSON: [InvalidSymbol ValueExpected]".to_owned()]
        );
        assert_eq!(
            validate_settings("{\"a\": \"x"),
            vec![
                "failed to parse JSON: [ParseErrorUnexpectedEndOfString CloseBraceExpected]"
                    .to_owned()
            ]
        );
    }

    #[test]
    fn reports_schema_problems() {
        assert_eq!(
            validate_settings("{\"search.defaultMode\": \"bogus\"}"),
            vec!["search.defaultMode: Does not match pattern 'precise|smart'".to_owned()]
        );
        assert_eq!(
            validate_settings("{\"search.contextLines\": -1}"),
            vec!["search.contextLines: Must be greater than or equal to 0".to_owned()]
        );
        assert_eq!(
            validate_settings("{\"search.contextLines\": \"x\"}"),
            vec!["search.contextLines: Invalid type. Expected: integer, given: string".to_owned()]
        );
        assert_eq!(
            validate_settings("{\"notices\": [{\"message\": \"hi\"}]}"),
            vec!["notices.0: location is required".to_owned()]
        );
        assert_eq!(
            validate_settings(
                "{\"notices\": [{\"location\": \"top\", \"message\": \"hi\", \"x\": 1}]}"
            ),
            vec!["notices.0: Additional property x is not allowed".to_owned()]
        );
        assert_eq!(
            validate_settings("{\"quicklinks\": [{\"name\": \"x\", \"url\": \"ftp://x\"}]}"),
            vec!["quicklinks.0.url: Does not match pattern '^(https?://|/)'".to_owned()]
        );
        assert_eq!(
            validate_settings("{\"notices\": [{\"message\": \"hi\", \"location\": \"there\"}]}"),
            vec!["notices.0.location: notices.0.location must be one of the \
                 following: \"top\", \"home\""
                .to_owned()]
        );
        assert_eq!(
            validate_settings("{\"history.defaultPageSize\": 101}"),
            vec!["history.defaultPageSize: Must be less than or equal to 100".to_owned()]
        );
    }
}
