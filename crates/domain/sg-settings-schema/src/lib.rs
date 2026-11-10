#![forbid(unsafe_code)]

//! `internal/conf.ValidateSettings` + `internal/jsonc.Parse`
//! (`internal/conf/validate.go:484-554`, `internal/jsonc/jsonc.go:16-39`).
//!
//! The embedded `assets/settings.schema.json` is a verbatim copy of
//! `schema/settings.schema.json` (`go:embed` in `schema/stringdata.go`).
//! Validation reimplements the subset of
//! `github.com/xeipuuv/gojsonschema v1.2.0` used by that schema, keeping
//! gojsonschema's messages, field paths and error order.

mod decimal;

use std::collections::BTreeSet;
use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;

use sg_jsonx::{parse, JsonValue, ParseOptions};

use decimal::Decimal;

/// `schema.SettingsSchemaJSON` (`schema/settings.schema.json`).
pub const SETTINGS_SCHEMA_JSON: &str = include_str!("../assets/settings.schema.json");

/// `conf.ValidateSettings`: JSONC syntax + settings JSON Schema problems.
pub fn validate_settings(jsonc_input: &str) -> Vec<String> {
    let (value, errors) = parse(
        jsonc_input,
        ParseOptions {
            comments: true,
            trailing_commas: true,
        },
    );
    if !errors.is_empty() {
        // `internal/jsonc.Parse`: `failed to parse JSON: %v` over
        // `[]jsonx.ParseError`, rendered as `[Code1 Code2 ...]`.
        let names: Vec<&str> = errors.iter().map(|error| error.code_name()).collect();
        return vec![format!("failed to parse JSON: [{}]", names.join(" "))];
    }

    // `jsonc.Parse` turns a missing root value into `null`.
    let value = value.unwrap_or(JsonValue::Null);
    let schema = settings_schema();
    let mut issues = Vec::new();
    let mut path = Vec::new();
    validate_node(schema, schema, &value, &mut path, &mut issues);

    issues
        .into_iter()
        .filter_map(|issue| {
            if IGNORE_LEGACY_KUBERNETES_FIELDS.contains(&issue.field.as_str()) {
                return None;
            }
            if issue.root_invalid_type
                && issue
                    .description
                    .starts_with("Invalid type. Expected: object, given: ")
            {
                return Some("must be a JSON object (use {} for empty)".to_owned());
            }
            Some(format!("{}: {}", issue.field, issue.description))
        })
        .collect()
}

/// `ignoreLegacyKubernetesFields` (`internal/conf/validate.go:23-45`).
const IGNORE_LEGACY_KUBERNETES_FIELDS: &[&str] = &[
    "alertmanagerConfig",
    "alertmanagerURL",
    "authProxyIP",
    "authProxyPassword",
    "deploymentOverrides",
    "gitoliteIP",
    "gitserverCount",
    "gitserverDiskSize",
    "gitserverSSH",
    "httpNodePort",
    "httpsNodePort",
    "indexedSearchDiskSize",
    "langGo",
    "langJava",
    "langJavaScript",
    "langPHP",
    "langPython",
    "langSwift",
    "langTypeScript",
    "namespace",
    "nodeSSDPath",
    "phabricatorIP",
    "prometheus",
    "pyPIIP",
    "rbac",
    "storageClass",
    "useAlertManager",
];

fn settings_schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        serde_json::from_str(SETTINGS_SCHEMA_JSON).expect("embedded settings schema is valid JSON")
    })
}

struct Issue {
    /// `gojsonschema` `Field()`: `"(root)"` or the dotted path.
    field: String,
    description: String,
    root_invalid_type: bool,
}

fn field_name(path: &[String]) -> String {
    if path.is_empty() {
        "(root)".to_owned()
    } else {
        path.join(".")
    }
}

fn validate_node(
    root_schema: &Value,
    schema: &Value,
    node: &JsonValue,
    path: &mut Vec<String>,
    issues: &mut Vec<Issue>,
) {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        if let Some(resolved) = resolve_ref(root_schema, reference) {
            validate_node(root_schema, resolved, node, path, issues);
        }
        return;
    }

    let field = field_name(path);

    if let Some(expected) = schema.get("type").and_then(Value::as_str) {
        let given = node_type(node);
        let matches = expected == given || (expected == "number" && given == "integer");
        if !matches {
            issues.push(Issue {
                description: format!("Invalid type. Expected: {expected}, given: {given}"),
                root_invalid_type: path.is_empty() && expected == "object",
                field,
            });
            return;
        }
    }

    match node {
        JsonValue::Object(properties) => {
            validate_object(root_schema, schema, properties, path, issues);
        }
        JsonValue::Array(items) => {
            if let Some(item_schema) = schema.get("items").filter(|items| items.is_object()) {
                for (index, item) in items.iter().enumerate() {
                    path.push(index.to_string());
                    validate_node(root_schema, item_schema, item, path, issues);
                    path.pop();
                }
            }
        }
        JsonValue::String(value) => {
            validate_enum(schema, value, &field, issues);
            if let Some(pattern) = schema.get("pattern").and_then(Value::as_str) {
                let regex = Regex::new(pattern).expect("schema pattern compiles");
                if !regex.is_match(value) {
                    issues.push(Issue {
                        field: field.clone(),
                        description: format!("Does not match pattern '{pattern}'"),
                        root_invalid_type: false,
                    });
                }
            }
        }
        JsonValue::Number(lexeme) => {
            validate_number(schema, lexeme, &field, issues);
        }
        JsonValue::Null | JsonValue::Bool(_) => {}
    }
}

fn validate_object(
    root_schema: &Value,
    schema: &Value,
    properties: &std::collections::BTreeMap<String, JsonValue>,
    path: &mut Vec<String>,
    issues: &mut Vec<Issue>,
) {
    let field = field_name(path);

    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for required_property in required {
            let Some(name) = required_property.as_str() else {
                continue;
            };
            if !properties.contains_key(name) {
                issues.push(Issue {
                    field: field.clone(),
                    description: format!("{name} is required"),
                    root_invalid_type: false,
                });
            }
        }
    }

    let declared: BTreeSet<&str> = schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| properties.keys().map(String::as_str).collect())
        .unwrap_or_default();

    match schema.get("additionalProperties") {
        Some(Value::Bool(false)) => {
            for name in properties.keys() {
                if !declared.contains(name.as_str()) {
                    issues.push(Issue {
                        field: field.clone(),
                        description: format!("Additional property {name} is not allowed"),
                        root_invalid_type: false,
                    });
                }
            }
        }
        Some(additional) if additional.is_object() => {
            for (name, value) in properties {
                path.push(name.clone());
                validate_node(root_schema, additional, value, path, issues);
                path.pop();
            }
        }
        _ => {}
    }

    // Go iterates the schema property map (`propertiesChildren`), whose
    // order is unspecified; we use the schema's sorted key order.
    if let Some(declared_properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, property_schema) in declared_properties {
            if let Some(value) = properties.get(name) {
                path.push(name.clone());
                validate_node(root_schema, property_schema, value, path, issues);
                path.pop();
            }
        }
    }
}

fn validate_number(schema: &Value, lexeme: &str, field: &str, issues: &mut Vec<Issue>) {
    let Some(number) = Decimal::parse(lexeme) else {
        return;
    };
    if let Some(maximum) = schema.get("maximum") {
        if compare_schema_number(&number, maximum) == std::cmp::Ordering::Greater {
            issues.push(Issue {
                field: field.to_owned(),
                description: format!("Must be less than or equal to {}", schema_number(maximum)),
                root_invalid_type: false,
            });
        }
    }
    if let Some(minimum) = schema.get("minimum") {
        if compare_schema_number(&number, minimum) == std::cmp::Ordering::Less {
            issues.push(Issue {
                field: field.to_owned(),
                description: format!(
                    "Must be greater than or equal to {}",
                    schema_number(minimum)
                ),
                root_invalid_type: false,
            });
        }
    }
}

fn validate_enum(schema: &Value, value: &str, field: &str, issues: &mut Vec<Issue>) {
    let Some(allowed) = schema.get("enum").and_then(Value::as_array) else {
        return;
    };
    if !allowed.iter().any(|entry| entry.as_str() == Some(value)) {
        // gojsonschema stores enum entries JSON-encoded, so strings keep
        // their quotes in the message.
        let names: Vec<String> = allowed
            .iter()
            .map(|entry| match entry {
                Value::String(name) => serde_json::to_string(name).unwrap_or_default(),
                other => other.to_string(),
            })
            .collect();
        issues.push(Issue {
            field: field.to_owned(),
            // gojsonschema's Enum template: `{{.field}} must be one of ...`.
            description: format!("{field} must be one of the following: {}", names.join(", ")),
            root_invalid_type: false,
        });
    }
}

fn node_type(node: &JsonValue) -> &'static str {
    match node {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(lexeme) => match Decimal::parse(lexeme) {
            Some(number) if number.is_integer() => "integer",
            _ => "number",
        },
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn resolve_ref<'a>(root_schema: &'a Value, reference: &str) -> Option<&'a Value> {
    let name = reference.strip_prefix("#/definitions/")?;
    root_schema.get("definitions")?.get(name)
}

/// gojsonschema stores schema numbers as `big.Rat`; `serde_json` keeps the
/// original lexeme, so compare against exactly that.
fn schema_number(value: &Value) -> String {
    match value {
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

fn compare_schema_number(number: &Decimal, schema_value: &Value) -> std::cmp::Ordering {
    let Some(other) = Decimal::parse(&schema_number(schema_value)) else {
        return std::cmp::Ordering::Equal;
    };
    number.cmp(&other)
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
