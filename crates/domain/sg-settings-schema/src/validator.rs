//! Miniature JSON Schema validator cloned from the subset of
//! `github.com/xeipuuv/gojsonschema v1.2.0` used by the embedded site and
//! settings schemas. It keeps gojsonschema's field paths, descriptions,
//! error order and `oneOf`/`anyOf` "closest matching" scoring.
//!
//! Started as the M9.8fs settings-only subset and extended for M9.8gp with
//! `type` arrays, `const`, length/item/property counts, `format`,
//! `anyOf`/`oneOf`/`not`, `dependencies` and generic JSON-Pointer `$ref`.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use regex::Regex;
use serde_json::Value;

use sg_jsonx::{parse, JsonValue, ParseOptions};

use crate::decimal::Decimal;

/// `ignoreLegacyKubernetesFields` (`internal/conf/validate.go:23-45`).
/// `doValidate` drops any error whose `Field()` is exactly one of these.
pub const IGNORE_LEGACY_KUBERNETES_FIELDS: &[&str] = &[
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

/// A parsed schema document plus the `validate_jsonc` entry point.
pub struct Schema {
    root: Value,
}

impl Schema {
    pub fn parse(json: &str) -> Result<Self, serde_json::Error> {
        Ok(Self {
            root: serde_json::from_str(json)?,
        })
    }

    /// `internal/jsonc.Parse` + `doValidate`: JSONC errors are returned as
    /// the single baseline `failed to parse JSON: [...]` message.
    pub fn validate_jsonc(&self, input: &str) -> Result<Vec<String>, String> {
        let (value, errors) = parse(
            input,
            ParseOptions {
                comments: true,
                trailing_commas: true,
            },
        );
        if !errors.is_empty() {
            let names: Vec<&str> = errors.iter().map(|error| error.code_name()).collect();
            return Err(format!("failed to parse JSON: [{}]", names.join(" ")));
        }

        // `jsonc.Parse` turns a missing root value into `null`.
        let value = value.unwrap_or(JsonValue::Null);
        Ok(self.validate_value(&value))
    }

    pub fn validate_value(&self, value: &JsonValue) -> Vec<String> {
        let mut validation = Validation::default();
        let mut path = Vec::new();
        validate_recursive(&self.root, &self.root, value, &mut path, &mut validation);
        format_issues(validation.issues)
    }
}

/// gojsonschema's error accumulator. `score` mirrors `Result.score` and is
/// only used to pick the closest matching `oneOf`/`anyOf` branch.
#[derive(Default)]
struct Validation {
    issues: Vec<Issue>,
    score: i32,
}

impl Validation {
    fn fail(&mut self, field: String, description: String, root_invalid_type: bool) {
        self.issues.push(Issue {
            field,
            description,
            root_invalid_type,
        });
        // `addInternalError` subtracts two; the enclosing validation adds one.
        self.score -= 2;
    }

    fn merge(&mut self, other: Validation) {
        self.issues.extend(other.issues);
        self.score += other.score;
    }
}

struct Issue {
    /// `gojsonschema` `Field()`: `"(root)"` or the dotted path.
    field: String,
    description: String,
    root_invalid_type: bool,
}

fn format_issues(issues: Vec<Issue>) -> Vec<String> {
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

fn field_name(path: &[String]) -> String {
    if path.is_empty() {
        "(root)".to_owned()
    } else {
        path.join(".")
    }
}

fn validate_recursive(
    root_schema: &Value,
    schema: &Value,
    node: &JsonValue,
    path: &mut Vec<String>,
    validation: &mut Validation,
) {
    if let Value::Bool(pass) = schema {
        if !pass {
            validation.fail(
                field_name(path),
                "False schema does not allow this value".into(),
                false,
            );
        }
        return;
    }

    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        if let Some(resolved) = resolve_ref(root_schema, reference) {
            // `$ref` delegates and returns before the enclosing +1 score.
            validate_recursive(root_schema, resolved, node, path, validation);
        }
        return;
    }

    let field = field_name(path);

    if let Some(expected) = schema_types(schema) {
        if !type_matches(&expected, node) {
            validation.fail(
                field,
                format!(
                    "Invalid type. Expected: {}, given: {}",
                    type_list_name(&expected),
                    node_type(node)
                ),
                path.is_empty() && type_list_name(&expected) == "object",
            );
            return;
        }
    }

    validate_schema(root_schema, schema, node, path, validation);

    match node {
        JsonValue::Object(properties) => {
            validate_object(root_schema, schema, properties, path, validation);
            validate_common(schema, node, path, validation);
            // Go iterates `propertiesChildren` (a map); we use the schema's
            // sorted key order.
            if let Some(declared_properties) = schema.get("properties").and_then(Value::as_object) {
                for (name, property_schema) in declared_properties {
                    if let Some(value) = properties.get(name) {
                        path.push(name.clone());
                        validate_recursive(root_schema, property_schema, value, path, validation);
                        path.pop();
                    }
                }
            }
        }
        JsonValue::Array(items) => {
            validate_array(root_schema, schema, items, path, validation);
            validate_common(schema, node, path, validation);
        }
        JsonValue::String(value) => {
            validate_common(schema, node, path, validation);
            validate_string(schema, value, path, validation);
        }
        JsonValue::Number(lexeme) => {
            validate_number(schema, lexeme, path, validation);
            validate_common(schema, node, path, validation);
        }
        // `validateCommon` is the only other validator invoked for booleans
        // and nulls.
        JsonValue::Null | JsonValue::Bool(_) => {
            validate_common(schema, node, path, validation);
        }
    }

    validation.score += 1;
}

/// `validateSchema` (`validation.go:307-395`): combinators run before the
/// type-specific validators, in gojsonschema's order.
fn validate_schema(
    root_schema: &Value,
    schema: &Value,
    node: &JsonValue,
    path: &mut Vec<String>,
    validation: &mut Validation,
) {
    if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array) {
        let mut validated = false;
        let mut best: Option<Validation> = None;
        for branch in any_of {
            let mut branch_validation = Validation::default();
            validate_recursive(root_schema, branch, node, path, &mut branch_validation);
            if branch_validation.issues.is_empty() {
                validated = true;
                break;
            }
            if best
                .as_ref()
                .is_none_or(|current| branch_validation.score > current.score)
            {
                best = Some(branch_validation);
            }
        }
        if !validated {
            validation.fail(
                field_name(path),
                "Must validate at least one schema (anyOf)".to_owned(),
                false,
            );
            if let Some(best) = best {
                validation.merge(best);
            }
        }
    }

    if let Some(one_of) = schema.get("oneOf").and_then(Value::as_array) {
        let mut validated = 0usize;
        let mut best: Option<Validation> = None;
        for branch in one_of {
            let mut branch_validation = Validation::default();
            validate_recursive(root_schema, branch, node, path, &mut branch_validation);
            if branch_validation.issues.is_empty() {
                validated += 1;
            } else if validated == 0
                && best
                    .as_ref()
                    .is_none_or(|current| branch_validation.score > current.score)
            {
                best = Some(branch_validation);
            }
        }
        if validated != 1 {
            validation.fail(
                field_name(path),
                "Must validate one and only one schema (oneOf)".to_owned(),
                false,
            );
            if validated == 0 {
                if let Some(best) = best {
                    validation.merge(best);
                }
            }
        }
    }

    if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
        let mut validated = 0usize;
        for branch in all_of {
            let mut branch_validation = Validation::default();
            validate_recursive(root_schema, branch, node, path, &mut branch_validation);
            if branch_validation.issues.is_empty() {
                validated += 1;
            }
            validation.merge(branch_validation);
        }
        if validated != all_of.len() {
            validation.fail(
                field_name(path),
                "Must validate all the schemas (allOf)".to_owned(),
                false,
            );
        }
    }

    if let Some(not_schema) = schema.get("not") {
        let mut branch_validation = Validation::default();
        validate_recursive(root_schema, not_schema, node, path, &mut branch_validation);
        if branch_validation.issues.is_empty() {
            validation.fail(
                field_name(path),
                "Must not validate the schema (not)".to_owned(),
                false,
            );
        }
    }

    if let (JsonValue::Object(properties), Some(dependencies)) =
        (node, schema.get("dependencies").and_then(Value::as_object))
    {
        for (property, dependency) in dependencies {
            if !properties.contains_key(property) {
                continue;
            }
            match dependency {
                Value::Array(required) => {
                    for required_property in required {
                        let Some(required_property) = required_property.as_str() else {
                            continue;
                        };
                        if !properties.contains_key(required_property) {
                            validation.fail(
                                field_name(path),
                                format!("Has a dependency on {required_property}"),
                                false,
                            );
                        }
                    }
                }
                Value::Object(_) => {
                    validate_recursive(root_schema, dependency, node, path, validation);
                }
                _ => {}
            }
        }
    }

    validation.score += 1;
}

fn validate_object(
    root_schema: &Value,
    schema: &Value,
    properties: &std::collections::BTreeMap<String, JsonValue>,
    path: &mut Vec<String>,
    validation: &mut Validation,
) {
    let field = field_name(path);

    if let Some(minimum) = schema.get("minProperties").and_then(Value::as_u64) {
        if properties.len() < usize::try_from(minimum).unwrap_or(usize::MAX) {
            validation.fail(
                field.clone(),
                format!("Must have at least {minimum} properties"),
                false,
            );
        }
    }
    if let Some(maximum) = schema.get("maxProperties").and_then(Value::as_u64) {
        if properties.len() > usize::try_from(maximum).unwrap_or(usize::MAX) {
            validation.fail(
                field.clone(),
                format!("Must have at most {maximum} properties"),
                false,
            );
        }
    }

    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for required_property in required {
            let Some(name) = required_property.as_str() else {
                continue;
            };
            if !properties.contains_key(name) {
                validation.fail(field.clone(), format!("{name} is required"), false);
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
                    validation.fail(
                        field.clone(),
                        format!("Additional property {name} is not allowed"),
                        false,
                    );
                }
            }
        }
        Some(additional @ Value::Object(_)) => {
            for (name, value) in properties {
                if !declared.contains(name.as_str()) {
                    path.push(name.clone());
                    validate_recursive(root_schema, additional, value, path, validation);
                    path.pop();
                }
            }
        }
        _ => {}
    }

    validation.score += 1;
}

fn validate_array(
    root_schema: &Value,
    schema: &Value,
    items: &[JsonValue],
    path: &mut Vec<String>,
    validation: &mut Validation,
) {
    if let Some(item_schema) = schema.get("items") {
        if item_schema.is_object() {
            for (index, item) in items.iter().enumerate() {
                path.push(index.to_string());
                let mut item_validation = Validation::default();
                validate_recursive(root_schema, item_schema, item, path, &mut item_validation);
                path.pop();
                validation.merge(item_validation);
            }
        }
    }

    if let Some(minimum) = schema.get("minItems").and_then(Value::as_u64) {
        if items.len() < usize::try_from(minimum).unwrap_or(usize::MAX) {
            validation.fail(
                field_name(path),
                format!("Array must have at least {minimum} items"),
                false,
            );
        }
    }
    if let Some(maximum) = schema.get("maxItems").and_then(Value::as_u64) {
        if items.len() > usize::try_from(maximum).unwrap_or(usize::MAX) {
            validation.fail(
                field_name(path),
                format!("Array must have at most {maximum} items"),
                false,
            );
        }
    }

    validation.score += 1;
}

fn validate_string(schema: &Value, value: &str, path: &[String], validation: &mut Validation) {
    let field = field_name(path);
    let length = value.chars().count();

    if let Some(minimum) = schema.get("minLength").and_then(Value::as_u64) {
        if length < usize::try_from(minimum).unwrap_or(usize::MAX) {
            validation.fail(
                field.clone(),
                format!("String length must be greater than or equal to {minimum}"),
                false,
            );
        }
    }
    if let Some(maximum) = schema.get("maxLength").and_then(Value::as_u64) {
        if length > usize::try_from(maximum).unwrap_or(usize::MAX) {
            validation.fail(
                field.clone(),
                format!("String length must be less than or equal to {maximum}"),
                false,
            );
        }
    }
    if let Some(pattern) = schema.get("pattern").and_then(Value::as_str) {
        let regex = go_regex(pattern);
        if !regex.is_match(value) {
            validation.fail(
                field.clone(),
                format!("Does not match pattern '{pattern}'"),
                false,
            );
        }
    }
    if let Some(format) = schema.get("format").and_then(Value::as_str) {
        if !format_matches(format, value) {
            validation.fail(field, format!("Does not match format '{format}'"), false);
        }
    }

    validation.score += 1;
}

fn validate_number(schema: &Value, lexeme: &str, path: &[String], validation: &mut Validation) {
    let Some(number) = Decimal::parse(lexeme) else {
        return;
    };
    let field = field_name(path);
    if let Some(maximum) = schema.get("maximum") {
        if compare_schema_number(&number, maximum) == Ordering::Greater {
            validation.fail(
                field.clone(),
                format!("Must be less than or equal to {}", schema_number(maximum)),
                false,
            );
        }
    }
    if let Some(minimum) = schema.get("minimum") {
        if compare_schema_number(&number, minimum) == Ordering::Less {
            validation.fail(
                field,
                format!(
                    "Must be greater than or equal to {}",
                    schema_number(minimum)
                ),
                false,
            );
        }
    }

    validation.score += 1;
}

fn validate_common(schema: &Value, node: &JsonValue, path: &[String], validation: &mut Validation) {
    let field = field_name(path);

    if let Some(constant) = schema.get("const") {
        let allowed = schema_value_json(constant);
        if canonical_json(node) != allowed {
            validation.fail(
                field.clone(),
                format!("{field} does not match: {allowed}"),
                false,
            );
        }
    }

    if let Some(allowed) = schema.get("enum").and_then(Value::as_array) {
        let actual = canonical_json(node);
        if !allowed
            .iter()
            .any(|entry| schema_value_json(entry) == actual)
        {
            let names: Vec<String> = allowed.iter().map(schema_value_json).collect();
            validation.fail(
                field.clone(),
                format!("{field} must be one of the following: {}", names.join(", ")),
                false,
            );
        }
    }

    validation.score += 1;
}

fn schema_types(schema: &Value) -> Option<Vec<String>> {
    match schema.get("type") {
        Some(Value::String(name)) => Some(vec![name.clone()]),
        Some(Value::Array(names)) => {
            let names: Vec<String> = names
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect();
            if names.is_empty() {
                None
            } else {
                Some(names)
            }
        }
        _ => None,
    }
}

fn type_list_name(types: &[String]) -> String {
    if types.len() == 1 {
        types[0].clone()
    } else {
        format!("[{}]", types.join(","))
    }
}

fn type_matches(types: &[String], node: &JsonValue) -> bool {
    let contains = |name: &str| types.iter().any(|entry| entry == name);
    match node {
        JsonValue::Null => contains("null"),
        JsonValue::Bool(_) => contains("boolean"),
        JsonValue::String(_) => contains("string"),
        JsonValue::Array(_) => contains("array"),
        JsonValue::Object(_) => contains("object"),
        JsonValue::Number(lexeme) => {
            let is_integer = Decimal::parse(lexeme).is_some_and(|number| number.is_integer());
            contains("number") || (is_integer && contains("integer"))
        }
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
    let pointer = reference.strip_prefix('#')?;
    if pointer.is_empty() {
        return Some(root_schema);
    }
    let mut current = root_schema;
    for token in pointer.strip_prefix('/')?.split('/') {
        let token = token.replace("~1", "/").replace("~0", "~");
        current = match current {
            Value::Object(map) => map.get(&token)?,
            Value::Array(items) => items.get(token.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// gojsonschema stores enum/const entries JSON-encoded (`marshalWithoutNumber`).
fn schema_value_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
}

fn canonical_json(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_owned(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(lexeme) => lexeme.clone(),
        JsonValue::String(value) => serde_json::to_string(value).unwrap_or_default(),
        JsonValue::Array(items) => {
            let entries: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", entries.join(","))
        }
        JsonValue::Object(properties) => {
            let entries: Vec<String> = properties
                .iter()
                .map(|(name, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(name).unwrap_or_default(),
                        canonical_json(value)
                    )
                })
                .collect();
            format!("{{{}}}", entries.join(","))
        }
    }
}

/// gojsonschema stores schema numbers as `big.Rat`; `serde_json` keeps the
/// original lexeme, so compare against exactly that.
fn schema_number(value: &Value) -> String {
    match value {
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

fn compare_schema_number(number: &Decimal, schema_value: &Value) -> Ordering {
    let Some(other) = Decimal::parse(&schema_number(schema_value)) else {
        return Ordering::Equal;
    };
    number.cmp(&other)
}

/// Compiles a schema `pattern`. Go's RE2 accepts `[\w-\.]` where Rust's
/// regex prefers the hyphen last; normalize that one construct on failure.
/// The error message always uses the original pattern text.
fn go_regex(pattern: &str) -> Regex {
    match Regex::new(pattern) {
        Ok(regex) => regex,
        Err(_) => {
            let normalized = pattern.replace(r"[\w-\.]", r"[\w.-]");
            Regex::new(&normalized).expect("schema pattern compiles")
        }
    }
}

/// gojsonschema's default format checkers for the formats used by the
/// embedded schemas. `email` and `uri` are approximations of Go's
/// `net/mail.ParseAddress` and `net/url.Parse` (documented in the M9.8gp
/// overview); unknown formats always pass, like gojsonschema.
fn format_matches(format: &str, value: &str) -> bool {
    match format {
        "email" | "idn-email" => email_is_valid(value),
        "uri" | "iri" => uri_is_valid(value),
        "regex" => value.is_empty() || Regex::new(value).is_ok(),
        _ => true,
    }
}

fn email_is_valid(value: &str) -> bool {
    // Accept both `local@domain` and `Display Name <local@domain>`.
    let address = match (value.rfind('<'), value.rfind('>')) {
        (Some(start), Some(end)) if end > start => &value[start + 1..end],
        (Some(_), _) | (_, Some(_)) => return false,
        _ => value.trim(),
    };
    let Some((local, domain)) = address.rsplit_once('@') else {
        return false;
    };
    if local.is_empty() || domain.is_empty() {
        return false;
    }
    if domain.contains('@') || domain.contains(char::is_whitespace) {
        return false;
    }
    if domain.starts_with('.') || domain.ends_with('.') || domain.contains("..") {
        return false;
    }
    if local.contains(char::is_whitespace) && !(local.starts_with('"') && local.ends_with('"')) {
        return false;
    }
    true
}

fn uri_is_valid(value: &str) -> bool {
    !value.contains('\\') && url::Url::parse(value).is_ok_and(|parsed| !parsed.scheme().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        Schema::parse(
            r#"{
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "name": {"type": "string"},
                    "kind": {"type": "string", "enum": ["a", "b"]},
                    "count": {"type": "integer", "minimum": 0, "maximum": 10},
                    "one": {"oneOf": [
                        {"type": "object", "required": ["x"], "properties": {"x": {"const": "one"}}},
                        {"type": "object", "required": ["y"], "properties": {"y": {"const": "two"}}}
                    ]},
                    "not": {"not": {"enum": ["bad"]}},
                    "list": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}},
                    "dep": {"type": "object", "dependencies": {"a": ["b"]}}
                }
            }"#,
        )
        .expect("test schema")
    }

    #[test]
    fn accepts_valid_documents() {
        assert!(schema()
            .validate_jsonc("{\"name\": \"x\",}")
            .expect("parse")
            .is_empty());
        assert!(schema()
            .validate_jsonc("{\"one\": {\"x\": \"one\"}}")
            .expect("parse")
            .is_empty());
        assert!(schema()
            .validate_jsonc("{\"list\": [\"x\"], \"dep\": {\"a\": 1, \"b\": 2}}")
            .expect("parse")
            .is_empty());
    }

    #[test]
    fn reports_parse_errors() {
        assert_eq!(
            schema().validate_jsonc("{\"name\": tru}"),
            Err("failed to parse JSON: [InvalidSymbol ValueExpected]".to_owned())
        );
    }

    #[test]
    fn reports_common_keywords() {
        let problems = schema()
            .validate_jsonc("{\"count\": 11, \"kind\": \"c\"}")
            .expect("parse");
        assert_eq!(
            problems,
            vec![
                "count: Must be less than or equal to 10".to_owned(),
                "kind: kind must be one of the following: \"a\", \"b\"".to_owned(),
            ]
        );
        assert_eq!(
            schema()
                .validate_jsonc("{\"not\": \"bad\"}")
                .expect("parse"),
            vec!["not: Must not validate the schema (not)".to_owned()]
        );
        assert_eq!(
            schema().validate_jsonc("{\"list\": []}").expect("parse"),
            vec!["list: Array must have at least 1 items".to_owned()]
        );
        assert_eq!(
            schema()
                .validate_jsonc("{\"dep\": {\"a\": 1}}")
                .expect("parse"),
            vec!["dep: Has a dependency on b".to_owned()]
        );
    }

    #[test]
    fn one_of_reports_closest_branch() {
        assert_eq!(
            schema()
                .validate_jsonc("{\"one\": {\"x\": \"two\"}}")
                .expect("parse"),
            vec![
                "one: Must validate one and only one schema (oneOf)".to_owned(),
                "one.x: one.x does not match: \"one\"".to_owned(),
            ]
        );
    }

    #[test]
    fn checks_formats() {
        assert!(format_matches("email", "dev@example.com"));
        assert!(!format_matches("email", "not-an-email"));
        assert!(format_matches("uri", "https://example.com"));
        assert!(!format_matches("uri", "not a uri"));
        assert!(format_matches("regex", "^a+$"));
        assert!(!format_matches("regex", "["));
    }
}
