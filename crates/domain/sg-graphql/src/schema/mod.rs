#![forbid(unsafe_code)]

//! GraphQL schema assembly and validation.
//!
//! Port of the schema-shape half of `graphqlbackend.NewSchema`
//! (`cmd/frontend/graphqlbackend/graphqlbackend.go` and `schema.go`) for an
//! OSS build with no enterprise `OptionalResolver`s: the OSS schema is
//! `schema.graphql` + `outbound_webhooks.graphql` + `viewer.graphql`, joined
//! with `"\n"` exactly like `NewSchema` does.
//!
//! The acceptance rules mirror graph-gophers `graphql-go` v1.5.0
//! (`internal/schema`), which is what the Go frontend calls through
//! `graphql.ParseSchema`:
//!
//! - base type/scalar/directive definitions silently replace earlier ones
//!   (the baseline parser assigns into a map without checking);
//! - `extend` targets an existing type of the same kind, and extension fields,
//!   interfaces, union members, enum values and input fields must not collide
//!   with what the base definition (or a *previous* extension) already
//!   declared;
//! - `schema { ... }` entries must resolve; without one, `Query`, `Mutation`
//!   and `Subscription` are inferred by name;
//! - every field/argument/input-field type reference must resolve to a defined
//!   type or a built-in scalar;
//! - object/interface types implementing an interface must provide all of its
//!   fields (by name);
//! - union members must be object types.
//!
//! Directives are parsed but not otherwise validated: the only directive the
//! OSS SDL uses is `@deprecated`, which is built in. Execution, introspection
//! (`__*` types) and resolvers are not ported here.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use graphql_parser::schema as gql;

pub mod sdl;

/// The three OSS schema documents, in `NewSchema` order.
pub use sdl::{MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA};

/// Built-in scalars provided by the GraphQL runtime (graph-gophers preloads
/// them into the type map before parsing user SDL).
pub const BUILTIN_SCALARS: [&str; 5] = ["String", "Int", "Float", "Boolean", "ID"];

pub fn is_builtin_scalar(name: &str) -> bool {
    BUILTIN_SCALARS.contains(&name)
}

/// Error produced while assembling or validating a schema. The message text
/// is intentionally not part of any wire contract (the baseline aborts
/// startup with `fmt.Errorf` text); the differential harness only compares
/// acceptance and the type inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaError {
    message: String,
}

impl SchemaError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SchemaError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TypeKind {
    Scalar,
    Object,
    Interface,
    Union,
    Enum,
    InputObject,
}

impl TypeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TypeKind::Scalar => "scalar",
            TypeKind::Object => "object",
            TypeKind::Interface => "interface",
            TypeKind::Union => "union",
            TypeKind::Enum => "enum",
            TypeKind::InputObject => "input_object",
        }
    }
}

/// A field argument or an input-object field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputValueDef {
    pub name: String,
    /// Rendered GraphQL type, e.g. `[String!]!`.
    pub value_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDef {
    pub name: String,
    /// Rendered GraphQL type, e.g. `[String!]!`.
    pub field_type: String,
    pub args: Vec<InputValueDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDef {
    pub name: String,
    pub kind: TypeKind,
    /// Object/interface fields.
    pub fields: Vec<FieldDef>,
    /// Input-object fields.
    pub input_fields: Vec<InputValueDef>,
    /// Interfaces implemented by an object/interface.
    pub interfaces: Vec<String>,
    /// Union members.
    pub union_members: Vec<String>,
    /// Enum values.
    pub enum_values: Vec<String>,
}

/// The assembled schema: the type map plus root operation types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    pub types: BTreeMap<String, TypeDef>,
    pub query_type: Option<String>,
    pub mutation_type: Option<String>,
    pub subscription_type: Option<String>,
    /// Directive definitions seen in the SDL (built-ins are not recorded).
    pub directives: BTreeSet<String>,
    /// Whether the SDL contained an explicit `schema { ... }` definition.
    pub has_schema_definition: bool,
}

impl Schema {
    /// Number of non-built-in, non-introspection types.
    pub fn type_count(&self) -> usize {
        self.types
            .keys()
            .filter(|name| !is_builtin_scalar(name) && !name.starts_with("__"))
            .count()
    }

    /// Stable line-oriented summary of the schema, sorted, used to diff the
    /// port against the Go baseline. See `tests/graphql_schema_harness.rs`.
    pub fn canonical_inventory(&self) -> Vec<String> {
        let opt = |value: &Option<String>| value.clone().unwrap_or_else(|| "-".to_owned());
        let mut lines = vec![format!(
            "r query={} mutation={} subscription={}",
            opt(&self.query_type),
            opt(&self.mutation_type),
            opt(&self.subscription_type)
        )];
        for (name, def) in &self.types {
            if is_builtin_scalar(name) || name.starts_with("__") {
                continue;
            }
            lines.push(format!("t {} {}", def.kind.as_str(), name));
            match def.kind {
                TypeKind::Object | TypeKind::Interface => {
                    for field in &def.fields {
                        lines.push(format!(
                            "f {name}.{} {} {}",
                            field.name,
                            field.field_type,
                            render_args(&field.args)
                        ));
                    }
                }
                TypeKind::InputObject => {
                    for field in &def.input_fields {
                        lines.push(format!("f {name}.{} {} -", field.name, field.value_type));
                    }
                }
                TypeKind::Union => {
                    if !def.union_members.is_empty() {
                        lines.push(format!("u {name} {}", join_sorted(&def.union_members)));
                    }
                }
                TypeKind::Enum => {
                    if !def.enum_values.is_empty() {
                        lines.push(format!("e {name} {}", join_sorted(&def.enum_values)));
                    }
                }
                TypeKind::Scalar => {}
            }
            if matches!(def.kind, TypeKind::Object | TypeKind::Interface)
                && !def.interfaces.is_empty()
            {
                lines.push(format!("i {name} {}", join_sorted(&def.interfaces)));
            }
        }
        lines
    }
}

fn render_args(args: &[InputValueDef]) -> String {
    if args.is_empty() {
        return "-".to_owned();
    }
    let mut rendered: Vec<String> = args
        .iter()
        .map(|arg| format!("{}:{}", arg.name, arg.value_type))
        .collect();
    rendered.sort();
    rendered.join(",")
}

fn join_sorted(values: &[String]) -> String {
    // No dedup: the baseline parser keeps repeated members in base
    // definitions, and the oracle inventory prints them as-is.
    let mut sorted: Vec<&str> = values.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.join(",")
}

/// Assemble the OSS schema exactly like `graphqlbackend.NewSchema` does for a
/// build whose enterprise resolvers are all nil.
pub fn assemble_oss() -> Result<Schema, SchemaError> {
    assemble(&[MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, VIEWER_SCHEMA])
}

/// Join the documents with `"\n"` (as `strings.Join(schemas, "\n")` does) and
/// assemble them.
pub fn assemble(sdls: &[&str]) -> Result<Schema, SchemaError> {
    assemble_sdl(&sdls.join("\n"))
}

/// The always-present meta types graph-gophers parses before user SDL
/// (`internal/schema/meta.go`). Only the parts that surface in the schema
/// shape are kept: the built-in scalars and the federation `_Service` object.
/// The `__*` introspection types are supplied by the executor, not by schema
/// assembly, and are therefore not modelled here.
const META_SDL: &str = "\
scalar Int
scalar Float
scalar String
scalar Boolean
scalar ID

type _Service {
  sdl: String!
}
";

/// Parse and validate a single GraphQL SDL document.
pub fn assemble_sdl(sdl: &str) -> Result<Schema, SchemaError> {
    // graph-gophers parses its meta SDL into the same type map before the
    // user schema, so user definitions may replace meta ones.
    let mut source = String::with_capacity(META_SDL.len() + 1 + sdl.len());
    source.push_str(META_SDL);
    source.push('\n');
    source.push_str(sdl);
    let document = gql::parse_schema::<String>(&source)
        .map_err(|err| SchemaError::new(format!("parse error: {err}")))?;

    let mut schema = Schema::default();
    let mut extensions: Vec<gql::TypeExtension<'_, String>> = Vec::new();

    for definition in &document.definitions {
        match definition {
            gql::Definition::SchemaDefinition(def) => {
                // The baseline parser overwrites `EntryPointNames` entries.
                schema.has_schema_definition = true;
                schema.query_type = def.query.clone();
                schema.mutation_type = def.mutation.clone();
                schema.subscription_type = def.subscription.clone();
            }
            gql::Definition::TypeDefinition(def) => {
                let ty = convert_type_def(def)?;
                // The baseline assigns into the type map without a duplicate
                // check: the last definition wins.
                schema.types.insert(ty.name.clone(), ty);
            }
            gql::Definition::TypeExtension(ext) => extensions.push(ext.clone()),
            gql::Definition::DirectiveDefinition(def) => {
                schema.directives.insert(def.name.clone());
            }
        }
    }

    for extension in &extensions {
        apply_extension(&mut schema.types, extension)?;
    }

    validate(&schema)
}

fn convert_type_def(def: &gql::TypeDefinition<'_, String>) -> Result<TypeDef, SchemaError> {
    let mut ty = TypeDef {
        name: String::new(),
        kind: TypeKind::Scalar,
        fields: Vec::new(),
        input_fields: Vec::new(),
        interfaces: Vec::new(),
        union_members: Vec::new(),
        enum_values: Vec::new(),
    };
    match def {
        gql::TypeDefinition::Scalar(scalar) => {
            ty.name = scalar.name.clone();
            ty.kind = TypeKind::Scalar;
        }
        gql::TypeDefinition::Object(object) => {
            ty.name = object.name.clone();
            ty.kind = TypeKind::Object;
            ty.interfaces = object.implements_interfaces.clone();
            ty.fields = object.fields.iter().map(convert_field).collect();
        }
        gql::TypeDefinition::Interface(interface) => {
            ty.name = interface.name.clone();
            ty.kind = TypeKind::Interface;
            ty.interfaces = interface.implements_interfaces.clone();
            ty.fields = interface.fields.iter().map(convert_field).collect();
        }
        gql::TypeDefinition::Union(union) => {
            ty.name = union.name.clone();
            ty.kind = TypeKind::Union;
            ty.union_members = union.types.clone();
        }
        gql::TypeDefinition::Enum(enum_def) => {
            ty.name = enum_def.name.clone();
            ty.kind = TypeKind::Enum;
            ty.enum_values = enum_def.values.iter().map(|v| v.name.clone()).collect();
        }
        gql::TypeDefinition::InputObject(input) => {
            ty.name = input.name.clone();
            ty.kind = TypeKind::InputObject;
            ty.input_fields = input.fields.iter().map(convert_input_value).collect();
        }
    }
    Ok(ty)
}

fn convert_field(field: &gql::Field<'_, String>) -> FieldDef {
    FieldDef {
        name: field.name.clone(),
        field_type: render_type(&field.field_type),
        args: field.arguments.iter().map(convert_input_value).collect(),
    }
}

fn convert_input_value(value: &gql::InputValue<'_, String>) -> InputValueDef {
    InputValueDef {
        name: value.name.clone(),
        value_type: render_type(&value.value_type),
    }
}

pub fn render_type(ty: &gql::Type<'_, String>) -> String {
    match ty {
        gql::Type::NamedType(name) => name.clone(),
        gql::Type::ListType(inner) => format!("[{}]", render_type(inner)),
        gql::Type::NonNullType(inner) => format!("{}!", render_type(inner)),
    }
}

fn extension_kind(ext: &gql::TypeExtension<'_, String>) -> TypeKind {
    match ext {
        gql::TypeExtension::Scalar(_) => TypeKind::Scalar,
        gql::TypeExtension::Object(_) => TypeKind::Object,
        gql::TypeExtension::Interface(_) => TypeKind::Interface,
        gql::TypeExtension::Union(_) => TypeKind::Union,
        gql::TypeExtension::Enum(_) => TypeKind::Enum,
        gql::TypeExtension::InputObject(_) => TypeKind::InputObject,
    }
}

fn extension_name<'a, 'b>(ext: &'a gql::TypeExtension<'b, String>) -> &'a str {
    match ext {
        gql::TypeExtension::Scalar(e) => &e.name,
        gql::TypeExtension::Object(e) => &e.name,
        gql::TypeExtension::Interface(e) => &e.name,
        gql::TypeExtension::Union(e) => &e.name,
        gql::TypeExtension::Enum(e) => &e.name,
        gql::TypeExtension::InputObject(e) => &e.name,
    }
}

/// Merge an `extend` block. Mirrors `internal/schema.mergeExtensions`:
/// collisions are checked against the pre-extension state only, so a single
/// extension block may repeat itself without an error.
fn apply_extension(
    types: &mut BTreeMap<String, TypeDef>,
    ext: &gql::TypeExtension<'_, String>,
) -> Result<(), SchemaError> {
    let name = extension_name(ext).to_owned();
    let kind = extension_kind(ext);
    let target = types
        .get_mut(&name)
        .ok_or_else(|| SchemaError::new(format!("trying to extend unknown type {name:?}")))?;
    if target.kind != kind {
        return Err(SchemaError::new(format!(
            "trying to extend type {name:?} with type {:?}",
            kind.as_str()
        )));
    }

    match ext {
        gql::TypeExtension::Scalar(_) => {}
        gql::TypeExtension::Object(extension) => {
            check_new_fields(&name, &target.fields, &extension.fields)?;
            check_new_interfaces(&name, &target.interfaces, &extension.implements_interfaces)?;
            target
                .fields
                .extend(extension.fields.iter().map(convert_field));
            target
                .interfaces
                .extend(extension.implements_interfaces.iter().cloned());
        }
        gql::TypeExtension::Interface(extension) => {
            check_new_fields(&name, &target.fields, &extension.fields)?;
            check_new_interfaces(&name, &target.interfaces, &extension.implements_interfaces)?;
            target
                .fields
                .extend(extension.fields.iter().map(convert_field));
            target
                .interfaces
                .extend(extension.implements_interfaces.iter().cloned());
        }
        gql::TypeExtension::Union(extension) => {
            for member in &extension.types {
                if target.union_members.contains(member) {
                    return Err(SchemaError::new(format!(
                        "union type {member:?} already declared in {name:?}"
                    )));
                }
            }
            target.union_members.extend(extension.types.iter().cloned());
        }
        gql::TypeExtension::Enum(extension) => {
            for value in &extension.values {
                if target.enum_values.contains(&value.name) {
                    return Err(SchemaError::new(format!(
                        "enum value {:?} already declared in {name:?}",
                        value.name
                    )));
                }
            }
            target
                .enum_values
                .extend(extension.values.iter().map(|v| v.name.clone()));
        }
        gql::TypeExtension::InputObject(extension) => {
            for field in &extension.fields {
                if target.input_fields.iter().any(|f| f.name == field.name) {
                    return Err(SchemaError::new(format!(
                        "extended field {:?} already exists",
                        field.name
                    )));
                }
            }
            target
                .input_fields
                .extend(extension.fields.iter().map(convert_input_value));
        }
    }
    Ok(())
}

fn check_new_fields(
    name: &str,
    existing: &[FieldDef],
    new_fields: &[gql::Field<'_, String>],
) -> Result<(), SchemaError> {
    for field in new_fields {
        if existing.iter().any(|f| f.name == field.name) {
            return Err(SchemaError::new(format!(
                "extended field {:?} already exists in {name:?}",
                field.name
            )));
        }
    }
    Ok(())
}

fn check_new_interfaces(
    name: &str,
    existing: &[String],
    new_interfaces: &[String],
) -> Result<(), SchemaError> {
    for interface in new_interfaces {
        if existing.contains(interface) {
            return Err(SchemaError::new(format!(
                "interface {interface:?} implemented in the extension is already implemented in {name:?}"
            )));
        }
    }
    Ok(())
}

fn type_exists(schema: &Schema, name: &str) -> bool {
    schema.types.contains_key(name) || is_builtin_scalar(name)
}

fn validate(schema: &Schema) -> Result<Schema, SchemaError> {
    // Root operation types: explicit `schema { ... }` entries must resolve;
    // otherwise the conventional names are inferred (graph-gophers only infers
    // when no schema definition was present).
    let roots = [
        (schema.query_type.clone(), "query"),
        (schema.mutation_type.clone(), "mutation"),
        (schema.subscription_type.clone(), "subscription"),
    ];
    let mut schema = schema.clone();
    schema.query_type = None;
    schema.mutation_type = None;
    schema.subscription_type = None;
    for (root, operation) in roots {
        match root {
            Some(name) => {
                if !type_exists(&schema, &name) {
                    return Err(SchemaError::new(format!(
                        "{operation} root type {name:?} does not exist"
                    )));
                }
                match operation {
                    "query" => schema.query_type = Some(name.clone()),
                    "mutation" => schema.mutation_type = Some(name.clone()),
                    _ => schema.subscription_type = Some(name.clone()),
                }
            }
            None if !schema.has_schema_definition => {
                let conventional = match operation {
                    "query" => "Query",
                    "mutation" => "Mutation",
                    _ => "Subscription",
                };
                if schema.types.contains_key(conventional) {
                    match operation {
                        "query" => schema.query_type = Some(conventional.to_owned()),
                        "mutation" => schema.mutation_type = Some(conventional.to_owned()),
                        _ => schema.subscription_type = Some(conventional.to_owned()),
                    }
                }
            }
            None => {}
        }
    }

    for (name, def) in &schema.types {
        if name.starts_with("__") {
            continue;
        }
        match def.kind {
            TypeKind::Object | TypeKind::Interface => {
                for field in &def.fields {
                    let named = named_type_str(&field.field_type);
                    if !type_exists(&schema, named) {
                        return Err(SchemaError::new(format!(
                            "unknown type {named:?} for field {name}.{}",
                            field.name
                        )));
                    }
                    for arg in &field.args {
                        let arg_type = named_type_str(&arg.value_type);
                        if !type_exists(&schema, arg_type) {
                            return Err(SchemaError::new(format!(
                                "unknown type {arg_type:?} for argument {}.{}",
                                field.name, arg.name
                            )));
                        }
                    }
                }
                for interface in &def.interfaces {
                    let Some(target) = schema.types.get(interface) else {
                        return Err(SchemaError::new(format!(
                            "interface {interface:?} not found for {name:?}"
                        )));
                    };
                    if target.kind != TypeKind::Interface {
                        return Err(SchemaError::new(format!(
                            "type {interface:?} is not an interface"
                        )));
                    }
                    for field in &target.fields {
                        if !def.fields.iter().any(|f| f.name == field.name) {
                            return Err(SchemaError::new(format!(
                                "interface {interface:?} expects field {:?} but {name:?} does not provide it",
                                field.name
                            )));
                        }
                    }
                }
            }
            TypeKind::InputObject => {
                for field in &def.input_fields {
                    let named = named_type_str(&field.value_type);
                    if !type_exists(&schema, named) {
                        return Err(SchemaError::new(format!(
                            "unknown type {named:?} for input field {name}.{}",
                            field.name
                        )));
                    }
                }
            }
            TypeKind::Union => {
                for member in &def.union_members {
                    match schema.types.get(member) {
                        None => {
                            return Err(SchemaError::new(format!(
                                "object type {member:?} not found in union {name:?}"
                            )));
                        }
                        Some(target) if target.kind != TypeKind::Object => {
                            return Err(SchemaError::new(format!(
                                "type {member:?} is not an object (union {name:?})"
                            )));
                        }
                        Some(_) => {}
                    }
                }
            }
            TypeKind::Scalar | TypeKind::Enum => {}
        }
    }
    Ok(schema)
}

/// Extract the named type from a rendered type string (`[Foo!]!` → `Foo`).
fn named_type_str(rendered: &str) -> &str {
    rendered.trim_matches(|c| c == '[' || c == ']' || c == '!')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal valid document with an explicit schema definition.
    fn doc(body: &str) -> String {
        format!("schema {{ query: Query }}\ntype Query {{ hello: String }}\n{body}")
    }

    #[test]
    fn oss_schema_assembles() {
        let schema = assemble_oss().expect("OSS schema must assemble");
        assert!(schema.type_count() > 300, "{}", schema.type_count());
        assert_eq!(schema.query_type.as_deref(), Some("Query"));
        assert_eq!(schema.mutation_type.as_deref(), Some("Mutation"));
        assert_eq!(schema.subscription_type, None);
        assert!(schema.has_schema_definition);

        let query = &schema.types["Query"];
        let query_fields: Vec<&str> = query.fields.iter().map(|f| f.name.as_str()).collect();
        // From outbound_webhooks.graphql.
        assert!(query_fields.contains(&"outboundWebhooks"));
        // From viewer.graphql.
        assert!(query_fields.contains(&"viewer"));
    }

    #[test]
    fn extension_fields_are_merged_in_place() {
        let schema = assemble_oss().unwrap();
        let with_extensions = &schema.types["Query"];
        assert!(with_extensions
            .fields
            .iter()
            .any(|f| f.name == "outboundWebhookEventTypes"));
        assert!(with_extensions
            .fields
            .iter()
            .any(|f| f.name == "viewerCanChangeLibraryItemVisibilityToPublic"));
    }

    #[test]
    fn inventory_is_stable_and_sorted_by_type() {
        let schema = assemble_oss().unwrap();
        let inventory = schema.canonical_inventory();
        assert!(inventory[0].starts_with("r query=Query mutation=Mutation "));
        assert!(inventory.contains(&"t object Query".to_owned()));
        assert!(inventory
            .iter()
            .any(|line| line == "f Query.viewer Viewer! -"));
    }

    #[test]
    fn last_base_definition_wins_like_the_baseline() {
        let schema = assemble_sdl(&format!(
            "{}\ntype Query {{ second: Int }}",
            doc("type Dup { a: String }")
        ))
        .unwrap();
        assert_eq!(schema.types["Dup"].fields[0].name, "a");
        assert_eq!(schema.types["Query"].fields[0].name, "second");
    }

    #[test]
    fn extending_an_unknown_type_is_an_error() {
        let err =
            assemble_sdl(&format!("{}\nextend type Nope {{ x: String }}", doc(""))).unwrap_err();
        assert!(err.message().contains("extend unknown type"), "{err}");
    }

    #[test]
    fn extension_kind_mismatch_is_an_error() {
        let err = assemble_sdl(&format!(
            "{}\nextend interface Query {{ x: String }}",
            doc("")
        ))
        .unwrap_err();
        assert!(err.message().contains("extend type"), "{err}");
    }

    #[test]
    fn extension_field_collision_is_an_error() {
        let err = assemble_sdl(&format!(
            "{}\nextend type Query {{ hello: String }}",
            doc("")
        ))
        .unwrap_err();
        assert!(err.message().contains("already exists"), "{err}");
    }

    #[test]
    fn repeated_members_inside_one_extension_block_are_accepted() {
        // Mirrors graph-gophers: collisions are checked against the
        // pre-extension state only.
        let schema =
            assemble_sdl(&format!("{}\nextend enum E {{ B B }}", doc("enum E { A }"))).unwrap();
        assert_eq!(schema.types["E"].enum_values, vec!["A", "B", "B"]);
    }

    #[test]
    fn unknown_field_type_is_an_error() {
        let err = assemble_sdl(&doc("type Bad { x: Missing }")).unwrap_err();
        assert!(err.message().contains("unknown type"), "{err}");
    }

    #[test]
    fn unknown_argument_type_is_an_error() {
        let err = assemble_sdl(&format!(
            "{}\ntype Query2 {{ x(arg: Missing): String }}",
            doc("")
        ))
        .unwrap_err();
        assert!(err.message().contains("unknown type"), "{err}");
    }

    #[test]
    fn union_members_must_be_objects() {
        let err = assemble_sdl(&format!(
            "{}\nunion U = Nope",
            doc("type Bad { x: String }")
        ))
        .unwrap_err();
        assert!(err.message().contains("not found in union"), "{err}");

        let err = assemble_sdl(&format!("{}\nscalar S\nunion U = S", doc(""))).unwrap_err();
        assert!(err.message().contains("is not an object"), "{err}");
    }

    #[test]
    fn interface_implementers_must_provide_fields() {
        let err = assemble_sdl(&format!(
            "{}\ninterface I {{ x: String }}\ntype T implements I {{ y: String }}",
            doc("")
        ))
        .unwrap_err();
        assert!(err.message().contains("does not provide it"), "{err}");
    }

    #[test]
    fn schema_root_must_resolve() {
        let err = assemble_sdl("schema { query: Missing }\ntype Other { x: String }").unwrap_err();
        assert!(err.message().contains("does not exist"), "{err}");
    }

    #[test]
    fn conventional_roots_are_inferred_without_a_schema_definition() {
        let schema = assemble_sdl("type Query { a: String }\ntype Mutation { b: Int }").unwrap();
        assert_eq!(schema.query_type.as_deref(), Some("Query"));
        assert_eq!(schema.mutation_type.as_deref(), Some("Mutation"));
        assert_eq!(schema.subscription_type, None);
    }

    #[test]
    fn builtin_scalars_resolve_without_definitions() {
        assemble_sdl("type Query { a: [String!]!, b(x: Int = 1): Boolean }").unwrap();
    }

    #[test]
    fn parse_errors_are_reported() {
        assert!(assemble_sdl("type {").is_err());
    }
}
