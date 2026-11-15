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
//!   declared (`extend scalar` is a syntax error in the baseline parser);
//! - `schema { ... }` entries must resolve; without one, `Query`, `Mutation`
//!   and `Subscription` are inferred by name;
//! - every field/argument/input-field type reference must resolve to a defined
//!   type or a built-in scalar;
//! - object/interface types implementing an interface must provide all of its
//!   fields (by name);
//! - union members must be object types;
//! - directive *usages* resolve against the directive definitions and receive
//!   the definitions' default arguments (this is where the default
//!   `@deprecated(reason: "No longer supported")` comes from).
//!
//! The model retains everything introspection needs (descriptions, directive
//! usage, default values) and includes the full graph-gophers meta schema
//! (`internal/schema/meta.go`): the five built-in scalars, the four built-in
//! directives, the `__*` introspection types and `_Service`. Relay JSON is
//! produced by [`crate::introspection`]; query execution and resolvers are not
//! ported yet.

use std::collections::BTreeMap;
use std::fmt;

use graphql_parser::schema as gql;

pub mod sdl;

/// The three OSS schema documents, in `NewSchema` order, plus the two
/// optional M9.13a documents (`code_monitors.graphql` then
/// `saved_searches.graphql`, the order `NewSchema` appends them in).
pub use sdl::{
    AUTHZ_SCHEMA, BATCHES_SCHEMA, CODEINTEL_AUTOINDEXING_SCHEMA,
    CODEINTEL_CODENAV_SCHEMA, CODEINTEL_POLICIES_SCHEMA, CODE_MONITORS_SCHEMA,
    CODY_CONTEXT_SCHEMA, EMBEDDINGS_SCHEMA, GITHUB_APPS_SCHEMA,
    INSIGHTS_AGGREGATIONS_SCHEMA, INSIGHTS_SCHEMA, MAIN_SCHEMA, NOTEBOOKS_SCHEMA,
    OUTBOUND_WEBHOOKS_SCHEMA, OWN_SCHEMA, SAVED_SEARCHES_SCHEMA, VIEWER_SCHEMA,
};

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

    /// The `__TypeKind` name used by introspection (`types.Kind()`).
    pub fn introspection_str(self) -> &'static str {
        match self {
            TypeKind::Scalar => "SCALAR",
            TypeKind::Object => "OBJECT",
            TypeKind::Interface => "INTERFACE",
            TypeKind::Union => "UNION",
            TypeKind::Enum => "ENUM",
            TypeKind::InputObject => "INPUT_OBJECT",
        }
    }
}

/// A GraphQL literal as written in the SDL, retaining enough structure to
/// reproduce graph-gophers' `types.Value.String()` rendering used by
/// introspection's `defaultValue`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueLiteral {
    Null,
    Boolean(bool),
    Enum(String),
    Int(String),
    Float(String),
    String(String),
    List(Vec<ValueLiteral>),
    Object(Vec<(String, ValueLiteral)>),
}

impl ValueLiteral {
    /// `types.Value.String()`: primitives are printed from the source token
    /// text (strings keep their quotes), lists as `[a, b]`, objects as
    /// `{k: v}`.
    pub fn render(&self) -> String {
        match self {
            ValueLiteral::Null => "null".to_owned(),
            ValueLiteral::Boolean(value) => value.to_string(),
            ValueLiteral::Enum(value) | ValueLiteral::Int(value) | ValueLiteral::Float(value) => {
                value.clone()
            }
            ValueLiteral::String(value) => quote_graphql_string(value),
            ValueLiteral::List(values) => {
                let items: Vec<String> = values.iter().map(ValueLiteral::render).collect();
                format!("[{}]", items.join(", "))
            }
            ValueLiteral::Object(fields) => {
                let items: Vec<String> = fields
                    .iter()
                    .map(|(name, value)| format!("{name}: {}", value.render()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }
}

/// Quote a string the way the source token would appear (graph-gophers keeps
/// `scanner`'s token text including quotes).
fn quote_graphql_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// A resolved directive usage: the definition's default arguments have been
/// appended (mirrors `internal/schema.resolveDirectives`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectiveUse {
    pub name: String,
    /// Arguments in source order followed by the definition defaults; the
    /// value is `None` when the definition argument has no default.
    pub args: Vec<(String, Option<ValueLiteral>)>,
}

impl DirectiveUse {
    pub fn argument(&self, name: &str) -> Option<&ValueLiteral> {
        self.args
            .iter()
            .find(|(arg, _)| arg == name)
            .and_then(|(_, value)| value.as_ref())
    }

    /// `reason` for `@deprecated`, defaulting like the built-in definition.
    pub fn deprecation_reason(&self) -> Option<String> {
        if self.name != "deprecated" {
            return None;
        }
        match self.argument("reason") {
            Some(ValueLiteral::String(reason)) => Some(reason.clone()),
            Some(other) => Some(other.render()),
            None => Some("No longer supported".to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectiveDef {
    pub name: String,
    pub description: Option<String>,
    /// Upper-case `__DirectiveLocation` names, in definition order.
    pub locations: Vec<String>,
    pub args: Vec<InputValueDef>,
    pub repeatable: bool,
}

/// A field argument or an input-object field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputValueDef {
    pub name: String,
    pub description: Option<String>,
    /// Rendered GraphQL type, e.g. `[String!]!`.
    pub value_type: String,
    pub default_value: Option<ValueLiteral>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDef {
    pub name: String,
    pub description: Option<String>,
    /// Rendered GraphQL type, e.g. `[String!]!`.
    pub field_type: String,
    pub args: Vec<InputValueDef>,
    pub directives: Vec<DirectiveUse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumValueDef {
    pub name: String,
    pub description: Option<String>,
    pub directives: Vec<DirectiveUse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDef {
    pub name: String,
    pub kind: TypeKind,
    pub description: Option<String>,
    pub directives: Vec<DirectiveUse>,
    /// Object/interface fields.
    pub fields: Vec<FieldDef>,
    /// Input-object fields.
    pub input_fields: Vec<InputValueDef>,
    /// Interfaces implemented by an object/interface.
    pub interfaces: Vec<String>,
    /// Union members.
    pub union_members: Vec<String>,
    /// Enum values.
    pub enum_values: Vec<EnumValueDef>,
}

/// The assembled schema: the type map plus root operation types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    pub types: BTreeMap<String, TypeDef>,
    pub query_type: Option<String>,
    pub mutation_type: Option<String>,
    pub subscription_type: Option<String>,
    /// Directive definitions (built-ins from the meta schema are included).
    pub directives: BTreeMap<String, DirectiveDef>,
    /// Whether the SDL contained an explicit `schema { ... }` definition.
    pub has_schema_definition: bool,
    /// Object definitions in SDL order (including the meta `_Service`), used
    /// to derive interface `possibleTypes` like the baseline's `s.Objects`.
    pub object_order: Vec<String>,
    /// The user SDL passed to `NewSchema` (`strings.Join(schemas, "\n")`),
    /// returned by `_Service.sdl`. The vendored meta schema is not included.
    pub schema_string: String,
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
                    let values: Vec<String> =
                        def.enum_values.iter().map(|v| v.name.clone()).collect();
                    if !values.is_empty() {
                        lines.push(format!("e {name} {}", join_sorted(&values)));
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

    /// The Relay introspection JSON (`(*graphql.Schema).ToJSON`), byte for
    /// byte.
    pub fn to_json(&self) -> String {
        crate::introspection::to_json(self)
    }

    /// Objects implementing `interface`, in definition order (baseline
    /// `InterfaceTypeDefinition.PossibleTypes`).
    pub fn possible_types(&self, interface: &str) -> Vec<&TypeDef> {
        self.object_order
            .iter()
            .filter_map(|name| self.types.get(name))
            .filter(|def| {
                def.kind == TypeKind::Object && def.interfaces.iter().any(|i| i == interface)
            })
            .collect()
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

/// The always-present meta schema graph-gophers parses before user SDL
/// (`internal/schema/meta.go`): the built-in scalars, built-in directives,
/// `__*` introspection types and the federation `_Service` object.
///
/// The baseline parses its meta schema with `useStringDescriptions=false`, so
/// the descriptions come from the `#` comments in `metaSrc`. graphql-parser
/// ignores comments, so the vendored copy turns every comment into a
/// description block string with the identical text.
const META_SDL: &str = include_str!("sdl/meta.graphql");

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

    let mut schema = Schema {
        schema_string: sdl.to_owned(),
        ..Schema::default()
    };
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
                if ty.kind == TypeKind::Object {
                    // `s.Objects` receives every object definition in parse
                    // order; it drives interface `possibleTypes`.
                    schema.object_order.push(ty.name.clone());
                }
                // The baseline assigns into the type map without a duplicate
                // check: the last definition wins.
                schema.types.insert(ty.name.clone(), ty);
            }
            gql::Definition::TypeExtension(ext) => extensions.push(ext.clone()),
            gql::Definition::DirectiveDefinition(def) => {
                let directive = convert_directive_def(def);
                schema.directives.insert(directive.name.clone(), directive);
            }
        }
    }

    for extension in &extensions {
        apply_extension(&mut schema.types, extension)?;
    }

    resolve_all_directives(&mut schema)?;
    validate(schema)
}

fn convert_type_def(def: &gql::TypeDefinition<'_, String>) -> Result<TypeDef, SchemaError> {
    let mut ty = TypeDef {
        name: String::new(),
        kind: TypeKind::Scalar,
        description: None,
        directives: Vec::new(),
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
            ty.description = scalar.description.clone();
            ty.directives = convert_directives(&scalar.directives);
        }
        gql::TypeDefinition::Object(object) => {
            ty.name = object.name.clone();
            ty.kind = TypeKind::Object;
            ty.description = object.description.clone();
            ty.directives = convert_directives(&object.directives);
            ty.interfaces = object.implements_interfaces.clone();
            ty.fields = object.fields.iter().map(convert_field).collect();
        }
        gql::TypeDefinition::Interface(interface) => {
            ty.name = interface.name.clone();
            ty.kind = TypeKind::Interface;
            ty.description = interface.description.clone();
            ty.directives = convert_directives(&interface.directives);
            ty.interfaces = interface.implements_interfaces.clone();
            ty.fields = interface.fields.iter().map(convert_field).collect();
        }
        gql::TypeDefinition::Union(union) => {
            ty.name = union.name.clone();
            ty.kind = TypeKind::Union;
            ty.description = union.description.clone();
            ty.directives = convert_directives(&union.directives);
            ty.union_members = union.types.clone();
        }
        gql::TypeDefinition::Enum(enum_def) => {
            ty.name = enum_def.name.clone();
            ty.kind = TypeKind::Enum;
            ty.description = enum_def.description.clone();
            ty.directives = convert_directives(&enum_def.directives);
            ty.enum_values = enum_def
                .values
                .iter()
                .map(|value| EnumValueDef {
                    name: value.name.clone(),
                    description: value.description.clone(),
                    directives: convert_directives(&value.directives),
                })
                .collect();
        }
        gql::TypeDefinition::InputObject(input) => {
            ty.name = input.name.clone();
            ty.kind = TypeKind::InputObject;
            ty.description = input.description.clone();
            ty.directives = convert_directives(&input.directives);
            ty.input_fields = input.fields.iter().map(convert_input_value).collect();
        }
    }
    Ok(ty)
}

fn convert_directive_def(def: &gql::DirectiveDefinition<'_, String>) -> DirectiveDef {
    DirectiveDef {
        name: def.name.clone(),
        description: def.description.clone(),
        locations: def.locations.iter().map(directive_location_str).collect(),
        args: def.arguments.iter().map(convert_input_value).collect(),
        repeatable: def.repeatable,
    }
}

fn directive_location_str(location: &gql::DirectiveLocation) -> String {
    use gql::DirectiveLocation::*;
    match location {
        Query => "QUERY",
        Mutation => "MUTATION",
        Subscription => "SUBSCRIPTION",
        Field => "FIELD",
        FragmentDefinition => "FRAGMENT_DEFINITION",
        FragmentSpread => "FRAGMENT_SPREAD",
        InlineFragment => "INLINE_FRAGMENT",
        Schema => "SCHEMA",
        Scalar => "SCALAR",
        Object => "OBJECT",
        FieldDefinition => "FIELD_DEFINITION",
        ArgumentDefinition => "ARGUMENT_DEFINITION",
        Interface => "INTERFACE",
        Union => "UNION",
        Enum => "ENUM",
        EnumValue => "ENUM_VALUE",
        InputObject => "INPUT_OBJECT",
        InputFieldDefinition => "INPUT_FIELD_DEFINITION",
        VariableDefinition => "VARIABLE_DEFINITION",
    }
    .to_owned()
}

fn convert_field(field: &gql::Field<'_, String>) -> FieldDef {
    FieldDef {
        name: field.name.clone(),
        description: field.description.clone(),
        field_type: render_type(&field.field_type),
        args: field.arguments.iter().map(convert_input_value).collect(),
        directives: convert_directives(&field.directives),
    }
}

fn convert_input_value(value: &gql::InputValue<'_, String>) -> InputValueDef {
    InputValueDef {
        name: value.name.clone(),
        description: value.description.clone(),
        value_type: render_type(&value.value_type),
        default_value: value.default_value.as_ref().map(convert_value),
    }
}

fn convert_directives(directives: &[gql::Directive<'_, String>]) -> Vec<DirectiveUse> {
    directives
        .iter()
        .map(|directive| DirectiveUse {
            name: directive.name.clone(),
            args: directive
                .arguments
                .iter()
                .map(|(name, value)| (name.clone(), Some(convert_value(value))))
                .collect(),
        })
        .collect()
}

fn convert_value(value: &gql::Value<'_, String>) -> ValueLiteral {
    match value {
        gql::Value::Variable(name) => ValueLiteral::Enum(format!("${name}")),
        gql::Value::Int(number) => ValueLiteral::Int(
            number
                .as_i64()
                .map(|value| value.to_string())
                .unwrap_or_default(),
        ),
        gql::Value::Float(value) => ValueLiteral::Float(value.to_string()),
        gql::Value::String(value) => ValueLiteral::String(value.clone()),
        gql::Value::Boolean(value) => ValueLiteral::Boolean(*value),
        gql::Value::Null => ValueLiteral::Null,
        gql::Value::Enum(name) => ValueLiteral::Enum(name.clone()),
        gql::Value::List(values) => ValueLiteral::List(values.iter().map(convert_value).collect()),
        gql::Value::Object(fields) => ValueLiteral::Object(
            fields
                .iter()
                .map(|(name, value)| (name.clone(), convert_value(value)))
                .collect(),
        ),
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
    // The baseline parser only has parseExtension cases for type/enum/
    // interface/union/input; `extend scalar` is a syntax error there.
    if matches!(ext, gql::TypeExtension::Scalar(_)) {
        return Err(SchemaError::new(
            "parse error: unexpected \"scalar\" after extend",
        ));
    }
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
        gql::TypeExtension::Scalar(_) => unreachable!("rejected above"),
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
                if target.enum_values.iter().any(|v| v.name == value.name) {
                    return Err(SchemaError::new(format!(
                        "enum value {:?} already declared in {name:?}",
                        value.name
                    )));
                }
            }
            target
                .enum_values
                .extend(extension.values.iter().map(|value| EnumValueDef {
                    name: value.name.clone(),
                    description: value.description.clone(),
                    directives: convert_directives(&value.directives),
                }));
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

/// Resolve every directive usage against the (meta + user) directive
/// definitions: validate name/location/argument names, reject repeated
/// non-repeatable directives and append the definitions' default arguments
/// (`internal/schema.resolveDirectives`). Input-object fields and field
/// arguments are not checked, matching the baseline.
fn resolve_all_directives(schema: &mut Schema) -> Result<(), SchemaError> {
    let definitions = &schema.directives;
    for def in schema.types.values_mut() {
        let location = match def.kind {
            TypeKind::Scalar => "SCALAR",
            TypeKind::Object => "OBJECT",
            TypeKind::Interface => "INTERFACE",
            TypeKind::Union => "UNION",
            TypeKind::Enum => "ENUM",
            TypeKind::InputObject => "INPUT_OBJECT",
        };
        resolve_directives(definitions, &mut def.directives, location)?;
        for field in &mut def.fields {
            resolve_directives(definitions, &mut field.directives, "FIELD_DEFINITION")?;
        }
        for value in &mut def.enum_values {
            resolve_directives(definitions, &mut value.directives, "ENUM_VALUE")?;
        }
    }
    Ok(())
}

fn resolve_directives(
    definitions: &BTreeMap<String, DirectiveDef>,
    uses: &mut [DirectiveUse],
    location: &str,
) -> Result<(), SchemaError> {
    // Validate first (immutable), then append the default arguments.
    for (index, directive) in uses.iter().enumerate() {
        let Some(definition) = definitions.get(&directive.name) else {
            return Err(SchemaError::new(format!(
                "directive {:?} not found",
                directive.name
            )));
        };
        if !definition.locations.iter().any(|known| known == location) {
            return Err(SchemaError::new(format!(
                "invalid location {location:?} for directive {:?}",
                directive.name
            )));
        }
        for (arg, _) in &directive.args {
            if !definition.args.iter().any(|known| &known.name == arg) {
                return Err(SchemaError::new(format!(
                    "invalid argument {arg:?} for directive {:?}",
                    directive.name
                )));
            }
        }
        if !definition.repeatable && uses[..index].iter().any(|p| p.name == directive.name) {
            return Err(SchemaError::new(format!(
                "non repeatable directive {:?} can not be repeated",
                directive.name
            )));
        }
    }
    for directive in uses.iter_mut() {
        let Some(definition) = definitions.get(&directive.name) else {
            continue;
        };
        for arg in &definition.args {
            if !directive.args.iter().any(|(name, _)| name == &arg.name) {
                directive
                    .args
                    .push((arg.name.clone(), arg.default_value.clone()));
            }
        }
    }
    Ok(())
}

fn type_exists(schema: &Schema, name: &str) -> bool {
    schema.types.contains_key(name) || is_builtin_scalar(name)
}

fn validate(schema: Schema) -> Result<Schema, SchemaError> {
    // Root operation types: explicit `schema { ... }` entries must resolve;
    // otherwise the conventional names are inferred (graph-gophers only infers
    // when no schema definition was present).
    let roots = [
        (schema.query_type.clone(), "query"),
        (schema.mutation_type.clone(), "mutation"),
        (schema.subscription_type.clone(), "subscription"),
    ];
    let mut schema = schema;
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

    // Directive argument types resolve like every other type reference.
    for directive in schema.directives.values() {
        for arg in &directive.args {
            let named = named_type_str(&arg.value_type);
            if !type_exists(&schema, named) {
                return Err(SchemaError::new(format!(
                    "unknown type {named:?} for directive @{} argument {}",
                    directive.name, arg.name
                )));
            }
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

    fn enum_names(def: &TypeDef) -> Vec<&str> {
        def.enum_values.iter().map(|v| v.name.as_str()).collect()
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

    /// M9.48c: the opt-in own document assembles on top of the OSS schema and
    /// declares `GitBlob implements Ownable` (the SDL-coverage DIFF). The
    /// default `assemble_oss` schema must not contain either.
    #[test]
    fn own_schema_assembles_and_gitblob_implements_ownable() {
        let oss = assemble_oss().expect("OSS schema must assemble");
        assert!(!oss.types.contains_key("Ownable"));
        assert!(!oss.types["GitBlob"]
            .fields
            .iter()
            .any(|field| field.name == "ownership"));

        let schema = assemble(&[
            MAIN_SCHEMA,
            OUTBOUND_WEBHOOKS_SCHEMA,
            VIEWER_SCHEMA,
            OWN_SCHEMA,
        ])
        .expect("own schema must assemble");
        assert_eq!(schema.types["Ownable"].kind, TypeKind::Interface);
        assert!(schema
            .possible_types("Ownable")
            .iter()
            .any(|def| def.name == "GitBlob"));
        assert!(schema.types["GitBlob"]
            .fields
            .iter()
            .any(|field| field.name == "ownership"));
        for root in ["codeownersIngestedFiles", "ownSignalConfigurations", "instanceOwnershipStats"] {
            assert!(schema.types["Query"]
                .fields
                .iter()
                .any(|field| field.name == root));
        }
    }

    #[test]
    fn meta_schema_is_complete() {
        let schema = assemble_oss().unwrap();
        for name in BUILTIN_SCALARS {
            assert_eq!(schema.types[name].kind, TypeKind::Scalar, "{name}");
        }
        assert_eq!(schema.types["_Service"].kind, TypeKind::Object);
        // All __* introspection types come from the meta schema.
        for name in [
            "__Schema",
            "__Type",
            "__TypeKind",
            "__Field",
            "__InputValue",
            "__EnumValue",
            "__Directive",
            "__DirectiveLocation",
        ] {
            assert!(schema.types.contains_key(name), "{name}");
        }
        // Built-in directive definitions, sorted by BTreeMap iteration.
        let names: Vec<&str> = schema.directives.keys().map(String::as_str).collect();
        assert_eq!(names, vec!["deprecated", "include", "skip", "specifiedBy"]);
        assert_eq!(
            schema.directives["deprecated"].locations,
            vec!["FIELD_DEFINITION", "ENUM_VALUE", "ARGUMENT_DEFINITION"]
        );
        // Meta descriptions survive (comments converted to block strings).
        assert_eq!(
            schema.types["String"].description.as_deref(),
            Some(
                "The `String` scalar type represents textual data, represented as UTF-8 \
                 character sequences. The String type is most often used by GraphQL to \
                 represent free-form human-readable text."
            )
        );
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
    fn extending_a_scalar_is_rejected() {
        // graphql-parser already rejects a directive-less scalar extension;
        // the baseline rejects every `extend scalar`.
        assert!(assemble_sdl(&format!("{}\nextend scalar Query", doc(""))).is_err());
        assert!(assemble_sdl(&format!(
            "{}\nextend scalar Query @specifiedBy(url: \"x\")",
            doc("")
        ))
        .is_err());
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
        assert_eq!(enum_names(&schema.types["E"]), vec!["A", "B", "B"]);
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

    #[test]
    fn descriptions_and_deprecations_are_retained() {
        let schema = assemble_sdl(&format!(
            "{}\ntype Query2 {{\n  \"\"\"Old field.\"\"\"\n  old: String @deprecated\n  gone: String @deprecated(reason: \"use old\")\n}}",
            doc("")
        ))
        .unwrap();
        let query2 = &schema.types["Query2"];
        assert_eq!(query2.fields[0].description.as_deref(), Some("Old field."));
        let deprecated = &query2.fields[0].directives[0];
        assert_eq!(deprecated.name, "deprecated");
        assert_eq!(
            deprecated.deprecation_reason().as_deref(),
            Some("No longer supported")
        );
        assert_eq!(
            query2.fields[1].directives[0]
                .deprecation_reason()
                .as_deref(),
            Some("use old")
        );
    }

    #[test]
    fn unknown_directives_are_rejected() {
        let err = assemble_sdl(&format!(
            "{}\ntype Query2 {{ x: String @unknown }}",
            doc("")
        ))
        .unwrap_err();
        assert!(err.message().contains("not found"), "{err}");
    }

    #[test]
    fn cody_context_and_embeddings_documents_assemble_opt_in() {
        // `assemble_oss()` (the default introspection surface) must not grow.
        let oss = assemble_oss().expect("oss schema");
        assert!(!oss.types.contains_key("ChatIntentResponse"));
        assert!(!oss.types.contains_key("RepoEmbeddingJob"));

        // `NewSchema` append order: embeddings.graphql then cody_context.graphql.
        let full = assemble(&[
            MAIN_SCHEMA,
            OUTBOUND_WEBHOOKS_SCHEMA,
            VIEWER_SCHEMA,
            EMBEDDINGS_SCHEMA,
            CODY_CONTEXT_SCHEMA,
        ])
        .expect("opt-in schema");
        for ty in [
            "EmbeddingsSearchResult",
            "EmbeddingsSearchResults",
            "RepoEmbeddingJob",
            "RepoEmbeddingJobsConnection",
            "RepoEmbeddingsStats",
            "ChatIntentResponse",
            "ChatContextResult",
            "CodyContextAlternativesResult",
            "CodyContextList",
            "FileChunkContext",
            "RankingResponse",
            "RankedItem",
            "RetrieverContextItem",
            "URLMentionContextResult",
            "InputContextItem",
            "RankOptions",
        ] {
            assert!(full.types.contains_key(ty), "missing {ty}");
        }
        let query = &full.types["Query"];
        for field in [
            "embeddingsSearch",
            "embeddingsMultiSearch",
            "isContextRequiredForChatQuery",
            "repoEmbeddingJobs",
            "getCodyContext",
            "getCodyContextAlternatives",
            "chatIntent",
            "rankContext",
            "recordContext",
            "chatContext",
            "urlMentionContext",
        ] {
            assert!(
                query.fields.iter().any(|def| def.name == field),
                "missing Query.{field}"
            );
        }
        let repository = &full.types["Repository"];
        assert!(repository
            .fields
            .iter()
            .any(|def| def.name == "embeddingJobs"));
    }

    #[test]
    fn directive_defaults_are_rendered_like_gophers() {
        assert_eq!(
            ValueLiteral::String("No longer supported".to_owned()).render(),
            "\"No longer supported\""
        );
        assert_eq!(ValueLiteral::Int("40".to_owned()).render(), "40");
        assert_eq!(
            ValueLiteral::List(vec![
                ValueLiteral::Int("1".to_owned()),
                ValueLiteral::Enum("A".to_owned())
            ])
            .render(),
            "[1, A]"
        );
    }
}
