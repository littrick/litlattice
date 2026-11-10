//! `internal/jsonc` + `internal/settings`'s merge of `schema.Settings`,
//! backing `settingsCascade.final` / `viewerSettings`
//! (`internal/settings/settings.go:137-290`, `internal/jsonc/jsonc.go:12-35`,
//! `schema/schema.go:2816-3050`).
//!
//! The Go baseline parses each subject's JSONC contents into the typed
//! `schema.Settings`, merges the typed values with `reflect`, and then
//! serializes the result with `encoding/json`. `Settings.MarshalJSON` first
//! marshals the struct (declaration order, `omitempty`) and then round-trips
//! it through `map[string]any`, so the final string is the canonical
//! recursively-key-sorted JSON with Go's string escaping and `float64`
//! number formatting. This module reproduces that pipeline over JSON values:
//!
//! * [`parse_jsonc`] — comments/trailing-comma tolerant parse (empty document
//!   is `null`);
//! * [`SettingsValue`] — the typed/normalized form (known fields keep the Go
//!   zero-value rules, unknown keys become `Additional`);
//! * [`merge_settings`] / [`final_settings`] — the `mergeLeft` cascade;
//! * [`marshal_settings`] / [`marshal_go`] — Go-compatible JSON output.

use std::collections::BTreeMap;

use serde_json::Value;

/// `defaultSettings()` (`internal/settings/settings.go:17-21`): the default
/// subject's synthetic contents, equivalent to `{"experimentalFeatures": {}}`.
pub const DEFAULT_SETTINGS_CONTENTS: &str = r#"{"experimentalFeatures": {}}"#;

/// The scalar type of a pointer field (`*bool`, `*int`, ...). Pointer fields
/// are non-zero whenever they are present, even when they hold the type's
/// zero value (`false`, `0`, `""`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Prim {
    Bool,
    Int,
    Float,
    Str,
}

/// Nested struct types without an `Additional` map: unknown keys are dropped
/// by `encoding/json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StructKind {
    Notice,
    StyleOverrides,
    QuickLink,
    SearchScope,
    OpenInEditor,
}

/// The merge behavior of a field (`settingsFieldMergeDepths` plus the
/// `reflect.Kind` switch in `mergeLeft`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Non-pointer scalar with `omitempty`.
    Bool,
    Int,
    Float,
    Str,
    /// Pointer scalar: presence is what counts.
    Ptr(Prim),
    /// Pointer to a struct with depth 0 (`openInEditor`).
    PtrStruct(StructKind),
    /// `*SettingsExperimentalFeatures` at depth 1.
    Experiment,
    /// `map[string]string` at depth 0.
    MapStr,
    /// `[]string` at depth 1 (`motd`).
    StrSlice,
    /// `[]*T` at depth 1 (`notices`, `search.scopes`).
    StructSlice(StructKind),
    /// `[]*T` at depth 0 (`quicklinks`).
    StructSliceClobber(StructKind),
}

/// `schema.Settings` fields (`schema/schema.go:2816-2854`).
const SETTINGS_FIELDS: &[(&str, Kind)] = &[
    ("alerts.hideObservabilitySiteAlerts", Kind::Ptr(Prim::Bool)),
    ("alerts.showMajorMinorUpdates", Kind::Bool),
    ("alerts.showPatchUpdates", Kind::Bool),
    ("basicCodeIntel.globalSearchesEnabled", Kind::Bool),
    ("basicCodeIntel.includeArchives", Kind::Bool),
    ("basicCodeIntel.includeForks", Kind::Bool),
    ("basicCodeIntel.indexOnly", Kind::Bool),
    ("basicCodeIntel.unindexedSearchTimeout", Kind::Float),
    ("codeIntel.disableRangeQueries", Kind::Bool),
    ("codeIntel.disableSearchBased", Kind::Bool),
    ("codeIntel.mixPreciseAndSearchBasedReferences", Kind::Bool),
    ("codeIntel.traceExtension", Kind::Bool),
    ("experimentalFeatures", Kind::Experiment),
    ("fileSidebarVisibleByDefault", Kind::Bool),
    ("history.defaultPageSize", Kind::Int),
    ("history.preferAbsoluteTimestamps", Kind::Bool),
    ("insights.aggregations.extendedTimeout", Kind::Int),
    ("motd", Kind::StrSlice),
    ("notices", Kind::StructSlice(StructKind::Notice)),
    ("openInEditor", Kind::PtrStruct(StructKind::OpenInEditor)),
    ("orgs.allMembersBatchChangesAdmin", Kind::Ptr(Prim::Bool)),
    ("perforce.codeHostToSwarmMap", Kind::MapStr),
    (
        "quicklinks",
        Kind::StructSliceClobber(StructKind::QuickLink),
    ),
    ("search.contextLines", Kind::Ptr(Prim::Int)),
    ("search.defaultCaseSensitive", Kind::Bool),
    ("search.defaultMode", Kind::Str),
    ("search.defaultPatternType", Kind::Str),
    ("search.displayLimit", Kind::Ptr(Prim::Int)),
    ("search.hideSuggestions", Kind::Ptr(Prim::Bool)),
    ("search.includeArchived", Kind::Ptr(Prim::Bool)),
    ("search.includeForks", Kind::Ptr(Prim::Bool)),
    ("search.scopes", Kind::StructSlice(StructKind::SearchScope)),
];

/// `schema.SettingsExperimentalFeatures` fields (`schema/schema.go:2965-3022`).
const EXPERIMENTAL_FIELDS: &[(&str, Kind)] = &[
    ("batchChangesExecution", Kind::Ptr(Prim::Bool)),
    ("clientSearchResultRanking", Kind::Ptr(Prim::Str)),
    ("codeInsightsCompute", Kind::Ptr(Prim::Bool)),
    ("codeInsightsRepoUI", Kind::Ptr(Prim::Str)),
    ("codeMonitoringWebHooks", Kind::Ptr(Prim::Bool)),
    ("enableLazyBlobSyntaxHighlighting", Kind::Ptr(Prim::Bool)),
    (
        "enableLazyFileResultSyntaxHighlighting",
        Kind::Ptr(Prim::Bool),
    ),
    ("enableSearchFilePrefetch", Kind::Ptr(Prim::Bool)),
    ("enableSidebarFilePrefetch", Kind::Ptr(Prim::Bool)),
    ("fuzzyFinder", Kind::Ptr(Prim::Bool)),
    ("fuzzyFinderActions", Kind::Ptr(Prim::Bool)),
    ("fuzzyFinderAll", Kind::Ptr(Prim::Bool)),
    (
        "fuzzyFinderCaseInsensitiveFileCountThreshold",
        Kind::Ptr(Prim::Float),
    ),
    ("fuzzyFinderNavbar", Kind::Ptr(Prim::Bool)),
    ("fuzzyFinderRepositories", Kind::Ptr(Prim::Bool)),
    ("fuzzyFinderSymbols", Kind::Ptr(Prim::Bool)),
    ("goCodeCheckerTemplates", Kind::Ptr(Prim::Bool)),
    ("keywordSearch", Kind::Bool),
    ("newSearchNavigationUI", Kind::Ptr(Prim::Bool)),
    ("newSearchResultFiltersPanel", Kind::Ptr(Prim::Bool)),
    ("newSearchResultsUI", Kind::Ptr(Prim::Bool)),
    ("proactiveSearchResultsAggregations", Kind::Ptr(Prim::Bool)),
    ("searchContextsQuery", Kind::Ptr(Prim::Bool)),
    ("searchQueryInput", Kind::Ptr(Prim::Str)),
    ("searchResultsAggregations", Kind::Ptr(Prim::Bool)),
    ("showCodeMonitoringLogs", Kind::Ptr(Prim::Bool)),
    ("symbolKindTags", Kind::Bool),
];

fn kind_for(name: &str) -> Option<Kind> {
    SETTINGS_FIELDS
        .iter()
        .find(|(field, _)| *field == name)
        .map(|(_, kind)| *kind)
}

fn experimental_kind_for(name: &str) -> Option<Kind> {
    EXPERIMENTAL_FIELDS
        .iter()
        .find(|(field, _)| *field == name)
        .map(|(_, kind)| *kind)
}

/// A parsed `schema.Settings`: the known (non-zero) fields plus the
/// `Additional` map of unknown keys (`json:"-"`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SettingsValue {
    fields: BTreeMap<String, Value>,
    additional: BTreeMap<String, Value>,
}

/// `jsonc.Parse` + `json.Unmarshal`: converts JSONC to a JSON value with
/// comments and trailing commas removed. An empty document is `null`.
pub fn parse_jsonc(text: &str) -> Result<Value, String> {
    let stripped = strip_jsonc(text)?;
    if stripped.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&stripped).map_err(|error| format!("failed to parse JSON: {error}"))
}

/// String-aware removal of `//`/`/* */` comments and trailing commas
/// (`jsonx.Parse` with `Comments`/`TrailingCommas`).
fn strip_jsonc(text: &str) -> Result<String, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' {
            out.push(c);
            i += 1;
            while i < chars.len() {
                let ch = chars[i];
                out.push(ch);
                i += 1;
                if ch == '\\' {
                    if i < chars.len() {
                        out.push(chars[i]);
                        i += 1;
                    }
                } else if ch == '"' {
                    break;
                }
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'/') {
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            out.push(' ');
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            loop {
                if i + 1 >= chars.len() {
                    return Err("failed to parse JSON: unterminated comment".to_owned());
                }
                if chars[i] == '*' && chars[i + 1] == '/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            out.push(' ');
        } else if c == ',' {
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if chars.get(j) == Some(&'}') || chars.get(j) == Some(&']') {
                i += 1;
            } else {
                out.push(c);
                i += 1;
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
    Ok(out)
}

/// `json.Unmarshal(contents, &schema.Settings)`: normalizes a parsed JSON
/// document to the typed settings shape. A `null` document (or a `null`
/// field) is the zero value.
pub fn normalize_settings(value: &Value) -> Result<SettingsValue, String> {
    let object = match value {
        Value::Null => return Ok(SettingsValue::default()),
        Value::Object(object) => object,
        _ => {
            return Err(
                "json: cannot unmarshal value into Go value of type schema.Settings".to_owned(),
            )
        }
    };
    let mut settings = SettingsValue::default();
    for (name, raw) in object {
        match kind_for(name) {
            Some(kind) => {
                if let Some(normalized) = normalize_field(kind, raw)? {
                    settings.fields.insert(name.clone(), normalized);
                }
            }
            None => {
                settings.additional.insert(name.clone(), raw.clone());
            }
        }
    }
    Ok(settings)
}

fn normalize_field(kind: Kind, raw: &Value) -> Result<Option<Value>, String> {
    if raw.is_null() {
        return Ok(None);
    }
    let mismatch = |what: &str| {
        Err(format!(
            "json: cannot unmarshal {what} into Go struct field Settings of type schema"
        ))
    };
    match kind {
        Kind::Bool => match raw {
            Value::Bool(true) => Ok(Some(raw.clone())),
            Value::Bool(false) => Ok(None),
            _ => mismatch("value"),
        },
        Kind::Int => match raw {
            Value::Number(number) => {
                let value = number
                    .as_i64()
                    .or_else(|| number.as_u64().and_then(|v| i64::try_from(v).ok()))
                    .ok_or_else(|| {
                        "json: cannot unmarshal number into Go struct field of type int".to_owned()
                    })?;
                if value == 0 {
                    Ok(None)
                } else {
                    Ok(Some(raw.clone()))
                }
            }
            _ => mismatch("value"),
        },
        Kind::Float => match raw {
            Value::Number(number) => {
                let value = number.as_f64().ok_or_else(|| {
                    "json: cannot unmarshal number into Go struct field of type float64".to_owned()
                })?;
                if value == 0.0 {
                    Ok(None)
                } else {
                    Ok(Some(raw.clone()))
                }
            }
            _ => mismatch("value"),
        },
        Kind::Str => match raw {
            Value::String(text) if text.is_empty() => Ok(None),
            Value::String(_) => Ok(Some(raw.clone())),
            _ => mismatch("value"),
        },
        Kind::Ptr(prim) => check_prim(prim, raw),
        Kind::PtrStruct(struct_kind) => match raw {
            Value::Object(object) => Ok(Some(normalize_struct(struct_kind, object))),
            _ => mismatch("value"),
        },
        Kind::Experiment => match raw {
            Value::Object(object) => Ok(Some(normalize_experimental(object))),
            _ => mismatch("value"),
        },
        Kind::MapStr => match raw {
            Value::Object(object) => {
                for (key, value) in object {
                    if !value.is_string() {
                        return Err(format!(
                            "json: cannot unmarshal value into Go struct field of type string (map key {key:?})"
                        ));
                    }
                }
                if object.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(raw.clone()))
                }
            }
            _ => mismatch("value"),
        },
        Kind::StrSlice => {
            match raw {
                Value::Array(items) => {
                    let mut normalized = Vec::with_capacity(items.len());
                    for item in items {
                        match item {
                            // `null` into a `string` leaves the zero value.
                            Value::Null => normalized.push(Value::String(String::new())),
                            Value::String(_) => normalized.push(item.clone()),
                            _ => return Err(
                                "json: cannot unmarshal value into Go struct field of type string"
                                    .to_owned(),
                            ),
                        }
                    }
                    if normalized.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(Value::Array(normalized)))
                    }
                }
                _ => mismatch("value"),
            }
        }
        Kind::StructSlice(struct_kind) | Kind::StructSliceClobber(struct_kind) => match raw {
            Value::Array(items) => {
                let mut normalized = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Value::Null => normalized.push(Value::Null),
                        Value::Object(object) => {
                            normalized.push(normalize_struct(struct_kind, object))
                        }
                        _ => return mismatch("value"),
                    }
                }
                if normalized.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(Value::Array(normalized)))
                }
            }
            _ => mismatch("value"),
        },
    }
}

fn check_prim(prim: Prim, raw: &Value) -> Result<Option<Value>, String> {
    let ok = match prim {
        Prim::Bool => raw.is_boolean(),
        Prim::Int => raw
            .as_i64()
            .or_else(|| raw.as_u64().and_then(|v| i64::try_from(v).ok()))
            .is_some(),
        Prim::Float => raw.as_f64().is_some() && raw.is_number(),
        Prim::Str => raw.is_string(),
    };
    if ok {
        Ok(Some(raw.clone()))
    } else {
        Err("json: cannot unmarshal value into Go pointer field".to_owned())
    }
}

fn normalize_struct(kind: StructKind, object: &serde_json::Map<String, Value>) -> Value {
    let mut out = serde_json::Map::new();
    match kind {
        StructKind::Notice => {
            copy_bool(object, "dismissible", &mut out);
            copy_required_string(object, "location", &mut out);
            copy_required_string(object, "message", &mut out);
            if let Some(Value::Object(style)) = object.get("styleOverrides") {
                out.insert(
                    "styleOverrides".to_owned(),
                    normalize_struct(StructKind::StyleOverrides, style),
                );
            }
            copy_optional_string(object, "variant", &mut out);
        }
        StructKind::StyleOverrides => {
            copy_optional_string(object, "backgroundColor", &mut out);
            copy_bool(object, "textCentered", &mut out);
            copy_optional_string(object, "textColor", &mut out);
        }
        StructKind::QuickLink => {
            copy_optional_string(object, "description", &mut out);
            copy_required_string(object, "name", &mut out);
            copy_required_string(object, "url", &mut out);
        }
        StructKind::SearchScope => {
            copy_required_string(object, "name", &mut out);
            copy_required_string(object, "value", &mut out);
        }
        StructKind::OpenInEditor => {
            copy_optional_string(object, "custom.urlPattern", &mut out);
            copy_string_slice(object, "editorIds", &mut out);
            copy_optional_string(object, "jetbrains.forceApi", &mut out);
            copy_optional_string(object, "projectPaths.default", &mut out);
            copy_optional_string(object, "projectPaths.linux", &mut out);
            copy_optional_string(object, "projectPaths.mac", &mut out);
            copy_optional_string(object, "projectPaths.windows", &mut out);
            copy_string_map(object, "replacements", &mut out);
            copy_bool(object, "vscode.isProjectPathUNCPath", &mut out);
            copy_optional_string(object, "vscode.remoteHostForSSH", &mut out);
            copy_bool(object, "vscode.useInsiders", &mut out);
            copy_bool(object, "vscode.useSSH", &mut out);
        }
    }
    Value::Object(out)
}

fn normalize_experimental(object: &serde_json::Map<String, Value>) -> Value {
    let mut out = serde_json::Map::new();
    for (name, raw) in object {
        match experimental_kind_for(name) {
            Some(kind) => {
                if let Ok(Some(normalized)) = normalize_field(kind, raw) {
                    out.insert(name.clone(), normalized);
                }
            }
            None => {
                out.insert(name.clone(), raw.clone());
            }
        }
    }
    Value::Object(out)
}

fn copy_bool(
    object: &serde_json::Map<String, Value>,
    key: &str,
    out: &mut serde_json::Map<String, Value>,
) {
    if let Some(Value::Bool(true)) = object.get(key) {
        out.insert(key.to_owned(), Value::Bool(true));
    }
}

fn copy_optional_string(
    object: &serde_json::Map<String, Value>,
    key: &str,
    out: &mut serde_json::Map<String, Value>,
) {
    if let Some(Value::String(text)) = object.get(key) {
        if !text.is_empty() {
            out.insert(key.to_owned(), Value::String(text.clone()));
        }
    }
}

fn copy_required_string(
    object: &serde_json::Map<String, Value>,
    key: &str,
    out: &mut serde_json::Map<String, Value>,
) {
    let text = match object.get(key) {
        Some(Value::String(text)) => text.clone(),
        _ => String::new(),
    };
    out.insert(key.to_owned(), Value::String(text));
}

fn copy_string_slice(
    object: &serde_json::Map<String, Value>,
    key: &str,
    out: &mut serde_json::Map<String, Value>,
) {
    if let Some(Value::Array(items)) = object.get(key) {
        if !items.is_empty() {
            out.insert(key.to_owned(), Value::Array(items.clone()));
        }
    }
}

fn copy_string_map(
    object: &serde_json::Map<String, Value>,
    key: &str,
    out: &mut serde_json::Map<String, Value>,
) {
    if let Some(Value::Object(map)) = object.get(key) {
        if !map.is_empty() {
            out.insert(key.to_owned(), Value::Object(map.clone()));
        }
    }
}

/// `mergeSettings` (`internal/settings/settings.go:202-268`): merges the
/// lower-priority `left` with the higher-priority `right`.
pub fn merge_settings(
    left: Option<SettingsValue>,
    right: Option<SettingsValue>,
) -> Option<SettingsValue> {
    match (left, right) {
        (None, None) => None,
        (None, Some(right)) => Some(right),
        (Some(left), None) => Some(left),
        (Some(left), Some(right)) => {
            let mut fields = BTreeMap::new();
            for (name, kind) in SETTINGS_FIELDS {
                let (Some(left_value), Some(right_value)) =
                    (left.fields.get(*name), right.fields.get(*name))
                else {
                    if let Some(value) = right.fields.get(*name).or_else(|| left.fields.get(*name))
                    {
                        fields.insert((*name).to_owned(), value.clone());
                    }
                    continue;
                };
                let merged = match kind {
                    Kind::StrSlice | Kind::StructSlice(_) => {
                        let mut items = as_array(left_value).to_vec();
                        items.extend(as_array(right_value).iter().cloned());
                        Value::Array(items)
                    }
                    Kind::Experiment => merge_experimental(left_value, right_value),
                    // Every other kind is depth 0: the non-zero `right` wins.
                    _ => right_value.clone(),
                };
                fields.insert((*name).to_owned(), merged);
            }
            let additional = if right.additional.is_empty() {
                left.additional
            } else {
                right.additional
            };
            Some(SettingsValue { fields, additional })
        }
    }
}

fn as_array(value: &Value) -> &[Value] {
    match value {
        Value::Array(items) => items,
        _ => &[],
    }
}

/// `*SettingsExperimentalFeatures` at depth 1: its fields are depth 0, and
/// its `Additional` map clobbers as a whole.
fn merge_experimental(left: &Value, right: &Value) -> Value {
    let left_object = match left {
        Value::Object(object) => object,
        _ => return right.clone(),
    };
    let right_object = match right {
        Value::Object(object) => object,
        _ => return right.clone(),
    };
    let mut out = serde_json::Map::new();
    for (name, _) in EXPERIMENTAL_FIELDS {
        if let Some(value) = right_object.get(*name).or_else(|| left_object.get(*name)) {
            out.insert((*name).to_owned(), value.clone());
        }
    }
    let right_additional: serde_json::Map<String, Value> = right_object
        .iter()
        .filter(|(name, _)| experimental_kind_for(name).is_none())
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let additional = if right_additional.is_empty() {
        left_object
            .iter()
            .filter(|(name, _)| experimental_kind_for(name).is_none())
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<serde_json::Map<String, Value>>()
    } else {
        right_additional
    };
    out.extend(additional);
    Value::Object(out)
}

/// `settings.Final`: parses each subject's contents in priority order and
/// merges them. `None` is a subject without settings (`GetLatest` returned
/// nil); the default subject passes [`DEFAULT_SETTINGS_CONTENTS`].
pub fn final_settings(subjects: &[Option<&str>]) -> Result<String, String> {
    let mut merged: Option<SettingsValue> = None;
    for contents in subjects {
        let right = match contents {
            None => None,
            Some(text) => Some(normalize_settings(&parse_jsonc(text)?)?),
        };
        merged = merge_settings(merged, right);
    }
    Ok(marshal_settings(&merged))
}

/// `json.Marshal(*schema.Settings)` after the `Settings.MarshalJSON`
/// map round-trip (recursively sorted keys).
pub fn marshal_settings(settings: &Option<SettingsValue>) -> String {
    match settings {
        None => "null".to_owned(),
        Some(settings) => {
            let mut object = settings.additional.clone();
            for (name, value) in &settings.fields {
                object.insert(name.clone(), value.clone());
            }
            marshal_go(&Value::Object(object.into_iter().collect()))
        }
    }
}

/// `encoding/json`-compatible serialization: recursively sorted object keys,
/// HTML-escaped strings and Go `float64` formatting.
pub fn marshal_go(value: &Value) -> String {
    let mut out = String::new();
    write_go(value, &mut out);
    out
}

fn write_go(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => out.push_str(&go_float(number.as_f64().unwrap_or(0.0))),
        Value::String(text) => write_go_string(text, out),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_go(item, out);
            }
            out.push(']');
        }
        Value::Object(object) => {
            out.push('{');
            for (index, (key, item)) in object.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_go_string(key, out);
                out.push(':');
                write_go(item, out);
            }
            out.push('}');
        }
    }
}

fn write_go_string(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// `encoding/json`'s `floatEncoder` (`strconv.AppendFloat` with format `f`
/// unless `abs < 1e-6 || abs >= 1e21`): shortest round-tripping digits, and
/// the `e-0X` → `e-X` cleanup for negative exponents.
fn go_float(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0".to_owned()
        } else {
            "0".to_owned()
        };
    }
    let abs = value.abs();
    if abs < 1e-6 || abs >= 1e21 {
        let formatted = format!("{value:e}");
        let (mantissa, exponent) = formatted
            .split_once('e')
            .expect("LowerExp always contains an exponent");
        let exponent: i32 = exponent.parse().expect("numeric exponent");
        if exponent < 0 {
            format!("{mantissa}e{exponent}")
        } else {
            format!("{mantissa}e+{exponent:02}")
        }
    } else {
        format!("{value}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jsonc_with_comments_and_trailing_commas() {
        let value = parse_jsonc(
            r#"{
                // line comment
                "a": 1, /* block */ "b": [1, 2,],
            }"#,
        )
        .expect("parse");
        assert_eq!(value, serde_json::json!({"a": 1, "b": [1, 2]}));
    }

    #[test]
    fn default_subject_is_only_experimental_features() {
        assert_eq!(
            final_settings(&[Some(DEFAULT_SETTINGS_CONTENTS)]).expect("merge"),
            r#"{"experimentalFeatures":{}}"#
        );
    }

    #[test]
    fn merges_scalars_slices_and_experimental_features() {
        let merged = final_settings(&[
            Some(DEFAULT_SETTINGS_CONTENTS),
            Some(r#"{"motd": ["global"], "search.defaultMode": "literal", "experimentalFeatures": {"fuzzyFinder": true}}"#),
            Some(r#"{"motd": ["org"], "search.defaultMode": "", "experimentalFeatures": {"fuzzyFinder": false, "newSearchResultsUI": true}}"#),
        ])
        .expect("merge");
        assert_eq!(
            merged,
            r#"{"experimentalFeatures":{"fuzzyFinder":false,"newSearchResultsUI":true},"motd":["global","org"],"search.defaultMode":"literal"}"#
        );
    }

    #[test]
    fn additional_clobbers_as_a_map() {
        let merged =
            final_settings(&[Some(r#"{"lower": 1}"#), Some(r#"{"higher": 2}"#)]).expect("merge");
        assert_eq!(merged, r#"{"higher":2}"#);
    }

    #[test]
    fn escapes_html_and_formats_numbers_like_go() {
        assert_eq!(
            marshal_go(&serde_json::json!({"s": "<a>&", "n": 1e6, "m": 1e-7})),
            "{\"m\":1e-7,\"n\":1000000,\"s\":\"\\u003ca\\u003e\\u0026\"}"
        );
    }
}
