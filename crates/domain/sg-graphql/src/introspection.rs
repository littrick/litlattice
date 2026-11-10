#![forbid(unsafe_code)]

//! Relay introspection JSON (`(*graphql.Schema).ToJSON`).
//!
//! graph-gophers executes a fixed introspection query
//! (`graphql-go@v1.5.0/introspection.go:29-112`) and re-indents the resulting
//! `json.RawMessage` with `json.MarshalIndent(data, "", "\t")`. The raw message
//! keeps the query's selection order, so the JSON keys are **not** sorted:
//!
//! - `__Schema`: `queryType, mutationType, subscriptionType, types, directives`
//! - `__Type`: `kind, name, description, fields, inputFields, interfaces,
//!   enumValues, possibleTypes`
//! - `__Field`: `name, description, args, type, isDeprecated, deprecationReason`
//! - `__InputValue`: `name, description, type, defaultValue`
//! - `__EnumValue`: `name, description, isDeprecated, deprecationReason`
//! - `__Directive`: `name, description, locations, args`
//! - type references: `kind, name, ofType`
//!
//! [`to_json`] reproduces those bytes exactly, including Go's HTML escaping
//! (`<`, `>`, `&` and U+2028/U+2029 are written as `\uXXXX`).

use crate::json::Json;
use crate::schema::{DirectiveDef, DirectiveUse, FieldDef, InputValueDef, Schema, TypeKind};

/// The Relay introspection JSON for `schema`, byte-identical to
/// `(*graphql.Schema).ToJSON()`.
pub fn to_json(schema: &Schema) -> String {
    let root = Json::Obj(vec![("__schema".to_owned(), schema_obj(schema))]);
    root.to_indented_string()
}

fn schema_obj(schema: &Schema) -> Json {
    Json::obj(vec![
        ("queryType", root_type(schema.query_type.as_deref())),
        ("mutationType", root_type(schema.mutation_type.as_deref())),
        (
            "subscriptionType",
            root_type(schema.subscription_type.as_deref()),
        ),
        (
            "types",
            Json::Arr(
                schema
                    .types
                    .values()
                    .map(|def| type_obj(schema, def))
                    .collect(),
            ),
        ),
        (
            "directives",
            Json::Arr(
                schema
                    .directives
                    .values()
                    .map(|def| directive_obj(schema, def))
                    .collect(),
            ),
        ),
    ])
}

fn root_type(name: Option<&str>) -> Json {
    match name {
        Some(name) => Json::obj(vec![("name", Json::Str(name.to_owned()))]),
        None => Json::Null,
    }
}

fn type_obj(schema: &Schema, def: &crate::schema::TypeDef) -> Json {
    let fields = match def.kind {
        TypeKind::Object | TypeKind::Interface => Json::Arr(
            def.fields
                .iter()
                .map(|field| field_obj(schema, field))
                .collect(),
        ),
        _ => Json::Null,
    };
    let input_fields = match def.kind {
        TypeKind::InputObject => Json::Arr(
            def.input_fields
                .iter()
                .map(|value| input_value_json(schema, value))
                .collect(),
        ),
        _ => Json::Null,
    };
    // Baseline quirk: `interfaces` is non-null only for object type
    // definitions, even when an interface implements another interface.
    let interfaces = match def.kind {
        TypeKind::Object => Json::Arr(
            def.interfaces
                .iter()
                .map(|name| named_ref(schema, name))
                .collect(),
        ),
        _ => Json::Null,
    };
    let enum_values = match def.kind {
        TypeKind::Enum => Json::Arr(
            def.enum_values
                .iter()
                .map(|value| {
                    let (is_deprecated, reason) = deprecation(&value.directives);
                    Json::obj(vec![
                        ("name", Json::Str(value.name.clone())),
                        ("description", Json::opt_str(value.description.as_deref())),
                        ("isDeprecated", Json::Bool(is_deprecated)),
                        ("deprecationReason", Json::opt_str(reason.as_deref())),
                    ])
                })
                .collect(),
        ),
        _ => Json::Null,
    };
    let possible_types = match def.kind {
        TypeKind::Interface => Json::Arr(
            schema
                .possible_types(&def.name)
                .iter()
                .map(|implementer| named_ref(schema, &implementer.name))
                .collect(),
        ),
        TypeKind::Union => Json::Arr(
            def.union_members
                .iter()
                .map(|member| named_ref(schema, member))
                .collect(),
        ),
        _ => Json::Null,
    };
    Json::obj(vec![
        ("kind", Json::Str(def.kind.introspection_str().to_owned())),
        ("name", Json::Str(def.name.clone())),
        ("description", Json::opt_str(def.description.as_deref())),
        ("fields", fields),
        ("inputFields", input_fields),
        ("interfaces", interfaces),
        ("enumValues", enum_values),
        ("possibleTypes", possible_types),
    ])
}

fn field_obj(schema: &Schema, field: &FieldDef) -> Json {
    let (is_deprecated, reason) = deprecation(&field.directives);
    Json::obj(vec![
        ("name", Json::Str(field.name.clone())),
        ("description", Json::opt_str(field.description.as_deref())),
        (
            "args",
            Json::Arr(
                field
                    .args
                    .iter()
                    .map(|value| input_value_json(schema, value))
                    .collect(),
            ),
        ),
        ("type", type_ref(schema, &field.field_type)),
        ("isDeprecated", Json::Bool(is_deprecated)),
        ("deprecationReason", Json::opt_str(reason.as_deref())),
    ])
}

fn directive_obj(schema: &Schema, def: &DirectiveDef) -> Json {
    Json::obj(vec![
        ("name", Json::Str(def.name.clone())),
        ("description", Json::opt_str(def.description.as_deref())),
        (
            "locations",
            Json::Arr(
                def.locations
                    .iter()
                    .map(|location| Json::Str(location.clone()))
                    .collect(),
            ),
        ),
        (
            "args",
            Json::Arr(
                def.args
                    .iter()
                    .map(|value| input_value_json(schema, value))
                    .collect(),
            ),
        ),
    ])
}

/// `__InputValue` with its resolved type reference.
fn input_value_json(schema: &Schema, value: &InputValueDef) -> Json {
    Json::obj(vec![
        ("name", Json::Str(value.name.clone())),
        ("description", Json::opt_str(value.description.as_deref())),
        ("type", type_ref(schema, &value.value_type)),
        (
            "defaultValue",
            match &value.default_value {
                Some(default) => Json::Str(default.render()),
                None => Json::Null,
            },
        ),
    ])
}

fn deprecation(uses: &[DirectiveUse]) -> (bool, Option<String>) {
    match uses.iter().find(|use_| use_.name == "deprecated") {
        Some(use_) => (true, use_.deprecation_reason()),
        None => (false, None),
    }
}

/// `TypeRef` for a rendered type (`[String!]!`).
fn type_ref(schema: &Schema, rendered: &str) -> Json {
    if let Some(inner) = rendered.strip_suffix('!') {
        return Json::obj(vec![
            ("kind", Json::Str("NON_NULL".to_owned())),
            ("name", Json::Null),
            ("ofType", type_ref(schema, inner)),
        ]);
    }
    if let Some(inner) = rendered.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        return Json::obj(vec![
            ("kind", Json::Str("LIST".to_owned())),
            ("name", Json::Null),
            ("ofType", type_ref(schema, inner)),
        ]);
    }
    named_ref(schema, rendered)
}

fn named_ref(schema: &Schema, name: &str) -> Json {
    let kind = schema
        .types
        .get(name)
        .map(|def| def.kind.introspection_str())
        .unwrap_or("SCALAR");
    Json::obj(vec![
        ("kind", Json::Str(kind.to_owned())),
        ("name", Json::Str(name.to_owned())),
        ("ofType", Json::Null),
    ])
}

#[cfg(test)]
mod tests {
    use crate::schema::{assemble_oss, assemble_sdl};

    fn value(json: &str) -> serde_json::Value {
        serde_json::from_str(json).expect("valid JSON")
    }

    #[test]
    fn to_json_is_deterministic_and_covers_the_meta_types() {
        let schema = assemble_oss().unwrap();
        let json = schema.to_json();
        assert_eq!(json, schema.to_json());
        let parsed = value(&json);
        let schema_value = &parsed["__schema"];
        assert_eq!(schema_value["queryType"]["name"], "Query");
        assert_eq!(schema_value["mutationType"]["name"], "Mutation");
        assert!(schema_value["subscriptionType"].is_null());
        let types = schema_value["types"].as_array().unwrap();
        // 330 user types + 5 built-in scalars + 7 __* introspection types +
        // _Service (graph-gophers meta schema), sorted by name.
        assert_eq!(types.len(), 343);
        let names: Vec<&str> = types.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(names.contains(&"_Service"));
        assert!(names.contains(&"__Type"));
        let directives = schema_value["directives"].as_array().unwrap();
        let directive_names: Vec<&str> = directives
            .iter()
            .map(|d| d["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            directive_names,
            vec!["deprecated", "include", "skip", "specifiedBy"]
        );
        assert_eq!(
            directives[0]["args"][0]["defaultValue"],
            "\"No longer supported\""
        );
    }

    #[test]
    fn field_selection_and_wrapper_refs_follow_the_baseline() {
        let schema = assemble_sdl("type Query { x: [String!]! }").unwrap();
        let parsed = value(&schema.to_json());
        let query = parsed["__schema"]["types"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == "Query")
            .unwrap();
        let field_type = &query["fields"][0]["type"];
        assert_eq!(field_type["kind"], "NON_NULL");
        assert!(field_type["name"].is_null());
        assert_eq!(field_type["ofType"]["kind"], "LIST");
        assert_eq!(field_type["ofType"]["ofType"]["kind"], "NON_NULL");
        assert_eq!(field_type["ofType"]["ofType"]["ofType"]["kind"], "SCALAR");
        assert_eq!(field_type["ofType"]["ofType"]["ofType"]["name"], "String");
        // Non-applicable selections are null, and object interfaces are arrays.
        assert!(query["inputFields"].is_null());
        assert!(query["enumValues"].is_null());
        assert_eq!(query["interfaces"], serde_json::json!([]));
        assert!(query["possibleTypes"].is_null());
    }

    #[test]
    fn descriptions_are_go_escaped_and_empty_descriptions_are_null() {
        let schema = assemble_sdl(
            r#"type Query { x: String }
"a <b> & \"q\""
scalar Marked
""
scalar Empty"#,
        )
        .unwrap();
        let json = schema.to_json();
        assert!(
            json.contains("a \\u003cb\\u003e \\u0026 \\\"q\\\""),
            "{json}"
        );
        let parsed = value(&json);
        let types = parsed["__schema"]["types"].as_array().unwrap();
        let empty = types.iter().find(|t| t["name"] == "Empty").unwrap();
        assert!(empty["description"].is_null());
        let marked = types.iter().find(|t| t["name"] == "Marked").unwrap();
        assert_eq!(marked["description"], "a <b> & \"q\"");
    }

    #[test]
    fn deprecated_fields_and_enum_values_are_flagged() {
        let schema = assemble_sdl(
            "type Query { old: String @deprecated }\nenum E { A @deprecated(reason: \"gone\") B }",
        )
        .unwrap();
        let parsed = value(&schema.to_json());
        let types = parsed["__schema"]["types"].as_array().unwrap();
        let query = types.iter().find(|t| t["name"] == "Query").unwrap();
        assert_eq!(query["fields"][0]["isDeprecated"], true);
        assert_eq!(
            query["fields"][0]["deprecationReason"],
            "No longer supported"
        );
        let enum_type = types.iter().find(|t| t["name"] == "E").unwrap();
        assert_eq!(enum_type["enumValues"][0]["isDeprecated"], true);
        assert_eq!(enum_type["enumValues"][0]["deprecationReason"], "gone");
        assert_eq!(enum_type["enumValues"][1]["isDeprecated"], false);
    }

    #[test]
    fn possible_types_follow_object_definition_order() {
        let schema = assemble_sdl(
            "interface Node { id: ID! }\ntype B implements Node { id: ID! }\ntype A implements Node { id: ID! }\ntype Query { b: B }",
        )
        .unwrap();
        let parsed = value(&schema.to_json());
        let node = parsed["__schema"]["types"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == "Node")
            .unwrap();
        let names: Vec<&str> = node["possibleTypes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        // Definition order, not alphabetical.
        assert_eq!(names, vec!["B", "A"]);
        // Interfaces of an interface type stay null (baseline quirk).
        assert!(node["interfaces"].is_null());
    }
}
