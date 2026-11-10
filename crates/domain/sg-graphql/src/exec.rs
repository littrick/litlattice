#![forbid(unsafe_code)]

//! The GraphQL introspection query executor (`graphql.Schema.exec`,
//! `graphql-go@v1.5.0/graphql.go:199-278` → `internal/exec/*`).
//!
//! This slice ports the parts needed to execute introspection queries:
//! operation/fragment/variable parsing, `@skip`/`@include`, alias merging,
//! the `__schema`/`__type`/`__typename`/`_service` root fields with the
//! `introspection` resolver semantics, non-null propagation and the
//! `graphql.Response` JSON shape (selection-ordered `data`, Go escaping).
//!
//! Business field execution is delegated to [`ObjectValue`] resolvers supplied
//! by the caller ([`execute_with`]); the actor passed in [`ExecRequest`] is
//! threaded through to them. Validation implements the structural rules the
//! introspection corpus needs (known fields/arguments, required arguments,
//! scalar leaves, known directives); see the overview for the unported
//! rules. A known (non-introspection) field without a resolver reports that
//! resolvers are not ported.

use std::collections::{BTreeMap, BTreeSet};

use graphql_parser::{query as q, Pos};
use serde_json::Value as JsonValue;

use crate::json::Json;
use crate::schema::ValueLiteral;
use crate::schema::{DirectiveDef, EnumValueDef, FieldDef, InputValueDef, Schema, TypeKind};

type Doc<'a> = q::Document<'a, String>;
type Selection<'a> = q::Selection<'a, String>;
type Field<'a> = q::Field<'a, String>;
type Fragment<'a> = q::FragmentDefinition<'a, String>;
type Operation<'a> = q::OperationDefinition<'a, String>;

/// `errors.Location` in the GraphQL response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    pub line: usize,
    pub column: usize,
}

/// A field name or list index in an error path (`errors.QueryError.Path`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    Field(String),
    Index(usize),
}

/// `errors.QueryError` (only the serialized fields).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecError {
    pub message: String,
    pub locations: Vec<Location>,
    pub path: Vec<PathSegment>,
}

impl ExecError {
    /// An error without locations/path (graph-gophers assigns the path
    /// while unwinding the selection).
    pub fn new(message: impl Into<String>) -> Self {
        Self::message(message)
    }

    fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            locations: Vec::new(),
            path: Vec::new(),
        }
    }

    fn at(message: impl Into<String>, position: Pos) -> Self {
        Self {
            message: message.into(),
            locations: vec![Location {
                line: position.line,
                column: position.column,
            }],
            path: Vec::new(),
        }
    }
}

/// `actor.Actor` as seen by resolvers. The frontend has no session
/// middleware yet, so production requests are always [`Actor::Anonymous`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Actor {
    #[default]
    Anonymous,
    User {
        uid: i32,
    },
}

impl Actor {
    pub fn is_authenticated(self) -> bool {
        matches!(self, Actor::User { .. })
    }

    pub fn uid(self) -> Option<i32> {
        match self {
            Actor::Anonymous => None,
            Actor::User { uid } => Some(uid),
        }
    }
}

/// A resolved GraphQL input value (arguments after variable substitution and
/// schema defaults).
#[derive(Debug, Clone, PartialEq)]
pub enum InputValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Enum(String),
    List(Vec<InputValue>),
    Object(BTreeMap<String, InputValue>),
}

impl InputValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            InputValue::Str(value) | InputValue::Enum(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            InputValue::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            InputValue::Int(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            InputValue::Int(value) => Some(*value as f64),
            InputValue::Float(value) => Some(*value),
            _ => None,
        }
    }
}

/// A field's arguments: schema defaults overridden by the query.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Arguments(BTreeMap<String, InputValue>);

impl Arguments {
    pub fn get(&self, name: &str) -> Option<&InputValue> {
        self.0.get(name)
    }

    pub fn string(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(InputValue::as_str)
    }

    pub fn bool(&self, name: &str) -> Option<bool> {
        self.get(name).and_then(InputValue::as_bool)
    }

    pub fn i64(&self, name: &str) -> Option<i64> {
        self.get(name).and_then(InputValue::as_i64)
    }
}

/// The value an [`ObjectValue`] returns for one selected field.
#[derive(Clone)]
pub enum FieldValue {
    Null,
    Json(Json),
    Object(std::sync::Arc<dyn ObjectValue>),
    List {
        items: Vec<FieldValue>,
        /// `Some(type)` marks non-null elements and names the wrapped type
        /// for `graphql: got nil for non-null` errors; `None` is nullable.
        element_type: Option<&'static str>,
    },
}

/// A business object (e.g. `User`, `Repository`) whose fields are resolved on
/// demand. Selection/nullability semantics stay in the executor.
pub trait ObjectValue: Send + Sync {
    /// The GraphQL type name, which must exist in the schema.
    fn type_name(&self) -> &'static str;

    /// Resolve `field`. `Ok(None)` means this slice has no resolver for the
    /// field; the executor turns that into the "no ported resolver" error.
    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError>;
}

/// Root objects for an operation kind (`Query`, `Mutation`, ...). Returning
/// `None` means that root has no business resolver.
pub trait RootResolver: Send + Sync {
    fn root(&self, root_type: &str) -> Option<&dyn ObjectValue>;
}

/// `graphql.Schema.Exec` arguments (`graphQLQueryParams`).
pub struct ExecRequest<'a> {
    pub query: &'a str,
    pub operation_name: &'a str,
    pub variables: &'a serde_json::Map<String, JsonValue>,
    pub actor: Actor,
}

/// `graphql.Response`: `errors` (omitted when empty), `data` (omitted when
/// absent), `extensions` (never produced here).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecResponse {
    pub errors: Vec<ExecError>,
    data: Option<Json>,
}

impl ExecResponse {
    /// The `data` value as raw JSON (`json.RawMessage`).
    pub fn data_json(&self) -> Option<String> {
        self.data.as_ref().map(Json::to_compact_string)
    }

    /// `json.Marshal(Response)`.
    pub fn to_json(&self) -> String {
        let mut entries: Vec<(&str, Json)> = Vec::new();
        if !self.errors.is_empty() {
            entries.push((
                "errors",
                Json::Arr(self.errors.iter().map(error_json).collect()),
            ));
        }
        if let Some(data) = &self.data {
            entries.push(("data", data.clone()));
        }
        Json::obj(entries).to_compact_string()
    }
}

fn error_json(error: &ExecError) -> Json {
    let mut entries: Vec<(&str, Json)> = vec![("message", Json::Str(error.message.clone()))];
    if !error.locations.is_empty() {
        entries.push((
            "locations",
            Json::Arr(
                error
                    .locations
                    .iter()
                    .map(|location| {
                        Json::obj(vec![
                            ("line", Json::Int(location.line as i64)),
                            ("column", Json::Int(location.column as i64)),
                        ])
                    })
                    .collect(),
            ),
        ));
    }
    if !error.path.is_empty() {
        entries.push((
            "path",
            Json::Arr(
                error
                    .path
                    .iter()
                    .map(|segment| match segment {
                        PathSegment::Field(name) => Json::Str(name.clone()),
                        PathSegment::Index(index) => Json::Int(*index as i64),
                    })
                    .collect(),
            ),
        ));
    }
    Json::obj(entries)
}

/// Execute `request` against `schema` (introspection fields only).
pub fn execute(schema: &Schema, request: ExecRequest<'_>) -> ExecResponse {
    execute_with(schema, request, None)
}

/// Execute `request`, resolving business root fields through `roots`.
pub fn execute_with(
    schema: &Schema,
    request: ExecRequest<'_>,
    roots: Option<&dyn RootResolver>,
) -> ExecResponse {
    let doc: Doc<'_> = match q::parse_query::<String>(request.query) {
        Ok(doc) => doc,
        Err(error) => {
            return ExecResponse {
                errors: vec![ExecError::message(format!("syntax error: {error}"))],
                data: None,
            }
        }
    };

    let mut operations: Vec<&Operation<'_>> = Vec::new();
    let mut fragments: BTreeMap<String, &Fragment<'_>> = BTreeMap::new();
    for definition in &doc.definitions {
        match definition {
            q::Definition::Operation(op) => operations.push(op),
            q::Definition::Fragment(fragment) => {
                fragments.insert(fragment.name.clone(), fragment);
            }
        }
    }

    let (kind, op_name, variable_definitions, selection_set, op_position) =
        match select_operation(&operations, request.operation_name) {
            Ok(selected) => selected,
            Err(error) => {
                return ExecResponse {
                    errors: vec![error],
                    data: None,
                }
            }
        };

    let mut errors = Vec::new();
    validate_selection_set(
        schema,
        root_type_name(schema, kind),
        &selection_set.items,
        &fragments,
        &mut BTreeSet::new(),
        &mut errors,
    );
    if !errors.is_empty() {
        return ExecResponse { errors, data: None };
    }

    if kind == OpKind::Subscription {
        return ExecResponse {
            errors: vec![ExecError::message("graphql-ws protocol header is missing")],
            data: None,
        };
    }
    if kind == OpKind::Mutation && schema.mutation_type.is_none() {
        return ExecResponse {
            errors: vec![ExecError::message("no mutations are offered by the schema")],
            data: None,
        };
    }
    let _ = op_name;
    let _ = op_position;

    let mut variables = BTreeMap::new();
    for (name, value) in request.variables {
        variables.insert(name.clone(), runtime_from_json(value));
    }
    for definition in variable_definitions {
        if !variables.contains_key(&definition.name) {
            if let Some(default) = &definition.default_value {
                variables.insert(definition.name.clone(), eval_value(default, &variables));
            }
        }
    }

    let root_name = root_type_name(schema, kind).to_owned();
    let root_object = roots.and_then(|roots| roots.root(&root_name));
    let mut path = Vec::new();
    let mut errors = Vec::new();
    let data = execute_object(
        schema,
        &root_name,
        Parent::Intro(IntroValue::Root),
        &selection_set.items,
        &fragments,
        &variables,
        root_object,
        request.actor,
        &mut path,
        &mut errors,
    );
    ExecResponse {
        errors,
        data: Some(data),
    }
}

// ---------------------------------------------------------------------------
// Operations and fragments
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Query,
    Mutation,
    Subscription,
}

fn root_type_name(schema: &Schema, kind: OpKind) -> &str {
    match kind {
        OpKind::Query => schema.query_type.as_deref().unwrap_or("Query"),
        OpKind::Mutation => schema.mutation_type.as_deref().unwrap_or("Mutation"),
        OpKind::Subscription => schema
            .subscription_type
            .as_deref()
            .unwrap_or("Subscription"),
    }
}

type SelectedOperation<'a> = (
    OpKind,
    Option<&'a String>,
    &'a [q::VariableDefinition<'a, String>],
    &'a q::SelectionSet<'a, String>,
    Pos,
);

fn select_operation<'a>(
    operations: &[&'a Operation<'a>],
    operation_name: &str,
) -> Result<SelectedOperation<'a>, ExecError> {
    if operations.is_empty() {
        return Err(ExecError::message("no operations in query document"));
    }
    let operation = if operation_name.is_empty() {
        if operations.len() > 1 {
            return Err(ExecError::message(
                "more than one operation in query document and no operation name given",
            ));
        }
        operations[0]
    } else {
        operations
            .iter()
            .find(|op| operation_name_of(op) == Some(operation_name))
            .copied()
            .ok_or_else(|| {
                ExecError::message(format!("no operation with name {operation_name:?}"))
            })?
    };

    Ok(match operation {
        q::OperationDefinition::SelectionSet(selection_set) => {
            (OpKind::Query, None, &[], selection_set, Pos::default())
        }
        q::OperationDefinition::Query(query) => (
            OpKind::Query,
            query.name.as_ref(),
            &query.variable_definitions,
            &query.selection_set,
            query.position,
        ),
        q::OperationDefinition::Mutation(mutation) => (
            OpKind::Mutation,
            mutation.name.as_ref(),
            &mutation.variable_definitions,
            &mutation.selection_set,
            mutation.position,
        ),
        q::OperationDefinition::Subscription(subscription) => (
            OpKind::Subscription,
            subscription.name.as_ref(),
            &subscription.variable_definitions,
            &subscription.selection_set,
            subscription.position,
        ),
    })
}

fn operation_name_of<'a>(operation: &'a Operation<'a>) -> Option<&'a str> {
    match operation {
        q::OperationDefinition::SelectionSet(_) => None,
        q::OperationDefinition::Query(query) => query.name.as_deref(),
        q::OperationDefinition::Mutation(mutation) => mutation.name.as_deref(),
        q::OperationDefinition::Subscription(subscription) => subscription.name.as_deref(),
    }
}

// ---------------------------------------------------------------------------
// Runtime values
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum RtValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Enum(String),
    List(Vec<RtValue>),
    Object(BTreeMap<String, RtValue>),
}

fn runtime_from_json(value: &JsonValue) -> RtValue {
    match value {
        JsonValue::Null => RtValue::Null,
        JsonValue::Bool(value) => RtValue::Bool(*value),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                RtValue::Int(value)
            } else {
                RtValue::Float(number.as_f64().unwrap_or_default())
            }
        }
        JsonValue::String(value) => RtValue::Str(value.clone()),
        JsonValue::Array(values) => RtValue::List(values.iter().map(runtime_from_json).collect()),
        JsonValue::Object(values) => RtValue::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), runtime_from_json(value)))
                .collect(),
        ),
    }
}

fn eval_value(value: &q::Value<String>, variables: &BTreeMap<String, RtValue>) -> RtValue {
    match value {
        q::Value::Variable(name) => variables.get(name).cloned().unwrap_or(RtValue::Null),
        q::Value::Int(number) => number.as_i64().map(RtValue::Int).unwrap_or(RtValue::Null),
        q::Value::Float(value) => RtValue::Float(*value),
        q::Value::String(value) => RtValue::Str(value.clone()),
        q::Value::Boolean(value) => RtValue::Bool(*value),
        q::Value::Null => RtValue::Null,
        q::Value::Enum(value) => RtValue::Enum(value.clone()),
        q::Value::List(values) => RtValue::List(
            values
                .iter()
                .map(|value| eval_value(value, variables))
                .collect(),
        ),
        q::Value::Object(fields) => RtValue::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), eval_value(value, variables)))
                .collect(),
        ),
    }
}

fn argument<'a, 'q>(field: &'a Field<'q>, name: &str) -> Option<&'a q::Value<'q, String>> {
    field
        .arguments
        .iter()
        .find(|(arg_name, _)| arg_name == name)
        .map(|(_, value)| value)
}

fn bool_argument<'q>(field: &Field<'q>, name: &str, variables: &BTreeMap<String, RtValue>) -> bool {
    match argument(field, name).map(|value| eval_value(value, variables)) {
        Some(RtValue::Bool(value)) => value,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Validation skeleton
// ---------------------------------------------------------------------------

struct FieldSpec {
    field_type: String,
    args: Vec<String>,
}

fn field_spec(schema: &Schema, parent_type: &str, name: &str) -> Option<FieldSpec> {
    if name == "__typename" {
        // Valid on every composite type (`validation.go:308-312`); the
        // synthetic field's type is nullable `String`.
        return Some(FieldSpec {
            field_type: "String".to_owned(),
            args: Vec::new(),
        });
    }
    if parent_type == root_schema_query_name(schema) || parent_type == "Query" {
        return match name {
            "__schema" => Some(FieldSpec {
                field_type: "__Schema".to_owned(),
                args: Vec::new(),
            }),
            "__type" => Some(FieldSpec {
                field_type: "__Type".to_owned(),
                args: vec!["name".to_owned()],
            }),
            "_service" => Some(FieldSpec {
                field_type: "_Service".to_owned(),
                args: Vec::new(),
            }),
            _ => schema
                .types
                .get(parent_type)
                .and_then(|def| def.fields.iter().find(|field| field.name == name))
                .map(|field| FieldSpec {
                    field_type: field.field_type.clone(),
                    args: field.args.iter().map(|arg| arg.name.clone()).collect(),
                }),
        };
    }
    schema
        .types
        .get(parent_type)
        .and_then(|def| def.fields.iter().find(|field| field.name == name))
        .map(|field| FieldSpec {
            field_type: field.field_type.clone(),
            args: field.args.iter().map(|arg| arg.name.clone()).collect(),
        })
}

fn root_schema_query_name(schema: &Schema) -> &str {
    schema.query_type.as_deref().unwrap_or("Query")
}

fn required_arguments<'a>(
    schema: &'a Schema,
    parent_type: &str,
    name: &str,
) -> Vec<&'a InputValueDef> {
    if (parent_type == root_schema_query_name(schema) || parent_type == "Query") && name == "__type"
    {
        // Synthetic `name: String!` argument (`validation.go:314-325`);
        // it has no `InputValueDef` in the schema model.
        return Vec::new();
    }
    schema
        .types
        .get(parent_type)
        .and_then(|def| def.fields.iter().find(|field| field.name == name))
        .map(|field| {
            field
                .args
                .iter()
                .filter(|arg| arg.value_type.ends_with('!'))
                .collect()
        })
        .unwrap_or_default()
}

fn validate_selection_set(
    schema: &Schema,
    parent_type: &str,
    selections: &[Selection<'_>],
    fragments: &BTreeMap<String, &Fragment<'_>>,
    visited_fragments: &mut BTreeSet<String>,
    errors: &mut Vec<ExecError>,
) {
    for selection in selections {
        match selection {
            Selection::Field(field) => {
                validate_field(
                    schema,
                    parent_type,
                    field,
                    fragments,
                    visited_fragments,
                    errors,
                );
            }
            Selection::InlineFragment(fragment) => {
                validate_directives(schema, "INLINE_FRAGMENT", &fragment.directives, errors);
                let child_type = match &fragment.type_condition {
                    Some(q::TypeCondition::On(name)) => name.as_str(),
                    None => parent_type,
                };
                validate_selection_set(
                    schema,
                    child_type,
                    &fragment.selection_set.items,
                    fragments,
                    visited_fragments,
                    errors,
                );
            }
            Selection::FragmentSpread(spread) => {
                validate_directives(schema, "FRAGMENT_SPREAD", &spread.directives, errors);
                let Some(fragment) = fragments.get(&spread.fragment_name) else {
                    // The parser's spread position is the fragment name,
                    // matching graph-gophers' `Name.Loc`.
                    errors.push(ExecError::at(
                        format!("Unknown fragment {:?}.", spread.fragment_name),
                        spread.position,
                    ));
                    continue;
                };
                if !visited_fragments.insert(spread.fragment_name.clone()) {
                    continue;
                }
                let q::TypeCondition::On(condition) = &fragment.type_condition;
                validate_selection_set(
                    schema,
                    condition,
                    &fragment.selection_set.items,
                    fragments,
                    visited_fragments,
                    errors,
                );
            }
        }
    }
}

fn validate_field(
    schema: &Schema,
    parent_type: &str,
    field: &Field<'_>,
    fragments: &BTreeMap<String, &Fragment<'_>>,
    visited_fragments: &mut BTreeSet<String>,
    errors: &mut Vec<ExecError>,
) {
    validate_directives(schema, "FIELD", &field.directives, errors);
    let Some(spec) = field_spec(schema, parent_type, &field.name) else {
        // graph-gophers validates against a nil entry point without
        // reporting unknown fields (e.g. subscriptions in a schema that has
        // no subscription root).
        if !schema.types.contains_key(parent_type) {
            return;
        }
        errors.push(ExecError::at(
            format!(
                "Cannot query field {:?} on type {:?}.",
                field.name, parent_type
            ),
            field.position,
        ));
        return;
    };

    for (name, _) in &field.arguments {
        if !spec.args.iter().any(|arg| arg == name) && field.name != "__type" {
            errors.push(ExecError::at(
                format!(
                    "Unknown argument {:?} on field {:?} of type {:?}.",
                    name, field.name, parent_type
                ),
                field.position,
            ));
        }
    }
    if field.name == "__type" && !field.arguments.iter().any(|(name, _)| name == "name") {
        errors.push(ExecError::at(
            "Field \"__type\" argument \"name\" of type \"String!\" is required but not provided.",
            field.position,
        ));
    }
    for argument in required_arguments(schema, parent_type, &field.name) {
        if !field
            .arguments
            .iter()
            .any(|(name, _)| name == &argument.name)
        {
            errors.push(ExecError::at(
                format!(
                    "Field {:?} argument {:?} of type {:?} is required but not provided.",
                    field.name, argument.name, argument.value_type
                ),
                field.position,
            ));
        }
    }

    let composite = has_subfields(schema, &spec.field_type);
    if composite && field.selection_set.items.is_empty() {
        errors.push(ExecError::at(
            format!(
                "Field {:?} of type {:?} must have a selection of subfields. \
                 Did you mean \"{} {{ ... }}\"?",
                field.name, spec.field_type, field.name
            ),
            field.position,
        ));
    }
    if !composite && !field.selection_set.items.is_empty() {
        errors.push(ExecError::at(
            format!(
                "Field {:?} must not have a selection since type {:?} has no subfields.",
                field.name, spec.field_type
            ),
            field.selection_set.span.0,
        ));
    }

    if !field.selection_set.items.is_empty() {
        let child_type = unwrap_named(&spec.field_type);
        validate_selection_set(
            schema,
            child_type,
            &field.selection_set.items,
            fragments,
            visited_fragments,
            errors,
        );
    }
}

fn validate_directives(
    schema: &Schema,
    location: &str,
    directives: &[q::Directive<'_, String>],
    errors: &mut Vec<ExecError>,
) {
    for directive in directives {
        let Some(defined) = schema.directives.get(&directive.name) else {
            errors.push(ExecError::at(
                format!("Unknown directive {:?}.", directive.name),
                directive.position,
            ));
            continue;
        };
        if !defined.locations.iter().any(|allowed| allowed == location) {
            errors.push(ExecError::at(
                format!(
                    "Directive {:?} may not be used on {}.",
                    directive.name, location
                ),
                directive.position,
            ));
        }
    }
}

fn has_subfields(schema: &Schema, rendered: &str) -> bool {
    let name = unwrap_named(rendered);
    schema
        .types
        .get(name)
        .map(|def| {
            matches!(
                def.kind,
                TypeKind::Object | TypeKind::Interface | TypeKind::Union
            )
        })
        .unwrap_or(false)
}

/// Strip `!` and `[...]` wrappers (`unwrapType` in the baseline).
fn unwrap_named(rendered: &str) -> &str {
    let mut current = rendered.trim();
    loop {
        if let Some(inner) = current.strip_suffix('!') {
            current = inner;
            continue;
        }
        if let Some(inner) = current.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            current = inner;
            continue;
        }
        return current;
    }
}

// ---------------------------------------------------------------------------
// Selection flattening
// ---------------------------------------------------------------------------

fn skip_by_directive(field: &Field<'_>, variables: &BTreeMap<String, RtValue>) -> bool {
    if directive_bool_argument(field, "skip", variables).unwrap_or(false) {
        return true;
    }
    if let Some(value) = directive_bool_argument(field, "include", variables) {
        return !value;
    }
    false
}

fn directive_bool_argument(
    field: &Field<'_>,
    name: &str,
    variables: &BTreeMap<String, RtValue>,
) -> Option<bool> {
    let directive = field
        .directives
        .iter()
        .find(|directive| directive.name == name)?;
    let (_, value) = directive
        .arguments
        .iter()
        .find(|(argument, _)| argument == "if")?;
    match eval_value(value, variables) {
        RtValue::Bool(value) => Some(value),
        _ => None,
    }
}

/// Flatten a selection set into fields keyed by alias (`collectFieldsToResolve`
/// in `exec.go:115-156`): first occurrence fixes the output order, later
/// occurrences append their sub-selections.
fn flatten_fields<'a>(
    parent_type: &str,
    selections: &[Selection<'a>],
    fragments: &BTreeMap<String, &Fragment<'a>>,
    variables: &BTreeMap<String, RtValue>,
    out: &mut Vec<Field<'a>>,
) {
    for selection in selections {
        match selection {
            Selection::Field(field) => {
                let alias = field.alias.clone().unwrap_or_else(|| field.name.clone());
                if let Some(existing) = out.iter_mut().find(|existing| {
                    existing
                        .alias
                        .clone()
                        .unwrap_or_else(|| existing.name.clone())
                        == alias
                }) {
                    existing
                        .selection_set
                        .items
                        .extend(field.selection_set.items.iter().cloned());
                } else {
                    out.push(field.clone());
                }
            }
            Selection::InlineFragment(fragment) => {
                let matches = match &fragment.type_condition {
                    Some(q::TypeCondition::On(condition)) => condition == parent_type,
                    None => true,
                };
                if matches {
                    flatten_fields(
                        parent_type,
                        &fragment.selection_set.items,
                        fragments,
                        variables,
                        out,
                    );
                }
            }
            Selection::FragmentSpread(spread) => {
                let matches = fragments
                    .get(&spread.fragment_name)
                    .map(|fragment| {
                        let q::TypeCondition::On(condition) = &fragment.type_condition;
                        condition == parent_type
                    })
                    .unwrap_or(false);
                if matches {
                    let fragment = fragments[&spread.fragment_name];
                    flatten_fields(
                        parent_type,
                        &fragment.selection_set.items,
                        fragments,
                        variables,
                        out,
                    );
                }
            }
        }
    }
    out.retain(|field| !skip_by_directive(field, variables));
}

/// Skip/include is evaluated per field; `retain` above would drop merged
/// fields whose first occurrence was skipped, which matches graph-gophers
/// (each occurrence is skipped independently before merging). The caller
/// re-applies this after flattening.
fn apply_include_filter(fields: &mut Vec<Field<'_>>, variables: &BTreeMap<String, RtValue>) {
    fields.retain(|field| !skip_by_directive(field, variables));
}

// ---------------------------------------------------------------------------
// Introspection values
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntroValue<'a> {
    Root,
    Schema,
    /// A `__Type` value; the string is a rendered type (`[String!]!`).
    Type(&'a str),
    Field(&'a FieldDef),
    InputValue(&'a InputValueDef),
    EnumValue(&'a EnumValueDef),
    Directive(&'a DirectiveDef),
    Service,
}

enum Resolved<'a> {
    Null,
    Json(Json),
    Object(IntroValue<'a>, &'static str),
    Business(std::sync::Arc<dyn ObjectValue>),
    List {
        items: Vec<Resolved<'a>>,
        element_type: Option<&'static str>,
    },
}

/// The object owning the current selection set.
#[derive(Clone)]
enum Parent<'a> {
    Intro(IntroValue<'a>),
    Business(std::sync::Arc<dyn ObjectValue>),
}

#[allow(clippy::too_many_arguments)]
fn execute_object<'s, 'q>(
    schema: &'s Schema,
    parent_type: &str,
    parent: Parent<'s>,
    selections: &[Selection<'q>],
    fragments: &BTreeMap<String, &'q Fragment<'q>>,
    variables: &BTreeMap<String, RtValue>,
    root: Option<&dyn ObjectValue>,
    actor: Actor,
    path: &mut Vec<PathSegment>,
    errors: &mut Vec<ExecError>,
) -> Json {
    let mut fields = Vec::new();
    flatten_fields(parent_type, selections, fragments, variables, &mut fields);
    apply_include_filter(&mut fields, variables);

    let mut entries: Vec<(String, Json)> = Vec::with_capacity(fields.len());
    for field in &fields {
        let alias = field.alias.clone().unwrap_or_else(|| field.name.clone());
        path.push(PathSegment::Field(alias.clone()));

        if field.name == "__typename" {
            entries.push((alias, Json::Str(parent_type.to_owned())));
            path.pop();
            continue;
        }

        let spec = field_spec(schema, parent_type, &field.name);
        let non_null = spec
            .as_ref()
            .map(|spec| spec.field_type.ends_with('!'))
            .unwrap_or(false);

        let resolved = resolve_field(schema, &parent, parent_type, field, variables, root, actor);
        // Only a value that *is* null (`nil` in the baseline) triggers
        // `got nil for non-null`; nulls propagated from a field error or a
        // failed non-null child are silent (`exec.go:78-100,243-262`).
        let resolved_to_null = matches!(&resolved, Ok(Resolved::Null));
        let value = match resolved {
            Ok(resolved) => execute_resolved(
                schema,
                resolved,
                &field.selection_set.items,
                fragments,
                variables,
                root,
                actor,
                path,
                errors,
            ),
            Err(error) => {
                let mut error = error;
                error.path = path.clone();
                errors.push(error);
                Json::Null
            }
        };

        if non_null && value == Json::Null {
            if resolved_to_null {
                let rendered = spec
                    .as_ref()
                    .map(|spec| spec.field_type.as_str())
                    .unwrap_or("");
                let name = rendered.strip_suffix('!').unwrap_or(rendered);
                errors.push(ExecError {
                    message: format!("graphql: got nil for non-null {name:?}"),
                    locations: Vec::new(),
                    path: path.clone(),
                });
            }
            path.pop();
            return Json::Null;
        }
        entries.push((alias, value));
        path.pop();
    }
    Json::Obj(entries)
}

#[allow(clippy::too_many_arguments)]
fn execute_resolved<'s, 'q>(
    schema: &'s Schema,
    resolved: Resolved<'s>,
    selections: &[Selection<'q>],
    fragments: &BTreeMap<String, &'q Fragment<'q>>,
    variables: &BTreeMap<String, RtValue>,
    root: Option<&dyn ObjectValue>,
    actor: Actor,
    path: &mut Vec<PathSegment>,
    errors: &mut Vec<ExecError>,
) -> Json {
    match resolved {
        Resolved::Null => Json::Null,
        Resolved::Json(value) => value,
        Resolved::Object(value, type_name) => execute_object(
            schema,
            type_name,
            Parent::Intro(value),
            selections,
            fragments,
            variables,
            root,
            actor,
            path,
            errors,
        ),
        Resolved::Business(object) => {
            let type_name = object.type_name().to_owned();
            execute_object(
                schema,
                &type_name,
                Parent::Business(object),
                selections,
                fragments,
                variables,
                root,
                actor,
                path,
                errors,
            )
        }
        Resolved::List {
            items,
            element_type,
        } => {
            let mut values = Vec::with_capacity(items.len());
            for (index, item) in items.into_iter().enumerate() {
                path.push(PathSegment::Index(index));
                let resolved_to_null = matches!(&item, Resolved::Null);
                let value = execute_resolved(
                    schema, item, selections, fragments, variables, root, actor, path, errors,
                );
                if element_type.is_some() && value == Json::Null {
                    if resolved_to_null {
                        if let Some(name) = element_type {
                            errors.push(ExecError {
                                message: format!("graphql: got nil for non-null {name:?}"),
                                locations: Vec::new(),
                                path: path.clone(),
                            });
                        }
                    }
                    path.pop();
                    return Json::Null;
                }
                path.pop();
                values.push(value);
            }
            Json::Arr(values)
        }
    }
}

fn resolve_field<'s>(
    schema: &'s Schema,
    parent: &Parent<'s>,
    parent_type: &str,
    field: &Field<'_>,
    variables: &BTreeMap<String, RtValue>,
    root: Option<&dyn ObjectValue>,
    actor: Actor,
) -> Result<Resolved<'s>, ExecError> {
    match parent {
        Parent::Business(object) => {
            let arguments = collect_arguments(schema, parent_type, field, variables);
            match object.field(&field.name, &arguments, actor)? {
                Some(value) => Ok(resolved_from_field_value(value)),
                None => Err(no_ported_resolver(&field.name, parent_type)),
            }
        }
        Parent::Intro(IntroValue::Root) => match field.name.as_str() {
            "__schema" => Ok(Resolved::Object(IntroValue::Schema, "__Schema")),
            "_service" => Ok(Resolved::Object(IntroValue::Service, "_Service")),
            "__type" => {
                let name = match argument(field, "name").map(|value| eval_value(value, variables)) {
                    Some(RtValue::Str(name)) => name,
                    _ => return Ok(Resolved::Null),
                };
                Ok(match schema.types.get(&name) {
                    Some(def) => Resolved::Object(IntroValue::Type(def.name.as_str()), "__Type"),
                    None => Resolved::Null,
                })
            }
            _ => match root {
                Some(root) => {
                    let arguments = collect_arguments(schema, parent_type, field, variables);
                    match root.field(&field.name, &arguments, actor)? {
                        Some(value) => Ok(resolved_from_field_value(value)),
                        None => Err(no_ported_resolver(&field.name, parent_type)),
                    }
                }
                None => Err(no_ported_resolver(&field.name, parent_type)),
            },
        },
        Parent::Intro(parent) => match parent {
            IntroValue::Schema => resolve_schema_field(schema, field, variables),
            IntroValue::Type(rendered) => resolve_type_field(schema, rendered, field, variables),
            IntroValue::Field(def) => resolve_field_def_field(schema, def, field),
            IntroValue::InputValue(def) => resolve_input_value_field(schema, def, field),
            IntroValue::EnumValue(def) => resolve_enum_value_field(def, field),
            IntroValue::Directive(def) => resolve_directive_field(schema, def, field),
            IntroValue::Service => match field.name.as_str() {
                "sdl" => Ok(Resolved::Json(Json::Str(schema.schema_string.clone()))),
                _ => Ok(Resolved::Null),
            },
            IntroValue::Root => unreachable!(),
        },
    }
}

fn no_ported_resolver(field: &str, parent_type: &str) -> ExecError {
    ExecError::message(format!(
        "GraphQL field {field:?} of {parent_type:?} has no ported resolver"
    ))
}

fn resolved_from_field_value(value: FieldValue) -> Resolved<'static> {
    match value {
        FieldValue::Null => Resolved::Null,
        FieldValue::Json(value) => Resolved::Json(value),
        FieldValue::Object(object) => Resolved::Business(object),
        FieldValue::List {
            items,
            element_type,
        } => Resolved::List {
            items: items.into_iter().map(resolved_from_field_value).collect(),
            element_type,
        },
    }
}

/// Schema defaults overridden by the query arguments (`Argument` values are
/// already variable-substituted).
fn collect_arguments(
    schema: &Schema,
    parent_type: &str,
    field: &Field<'_>,
    variables: &BTreeMap<String, RtValue>,
) -> Arguments {
    let mut values = BTreeMap::new();
    if let Some(definition) = schema
        .types
        .get(parent_type)
        .and_then(|def| def.fields.iter().find(|def| def.name == field.name))
    {
        for argument in &definition.args {
            if let Some(default) = &argument.default_value {
                values.insert(argument.name.clone(), input_from_literal(default));
            }
        }
    }
    for (name, value) in &field.arguments {
        values.insert(
            name.clone(),
            input_from_runtime(eval_value(value, variables)),
        );
    }
    Arguments(values)
}

fn input_from_runtime(value: RtValue) -> InputValue {
    match value {
        RtValue::Null => InputValue::Null,
        RtValue::Bool(value) => InputValue::Bool(value),
        RtValue::Int(value) => InputValue::Int(value),
        RtValue::Float(value) => InputValue::Float(value),
        RtValue::Str(value) => InputValue::Str(value),
        RtValue::Enum(value) => InputValue::Enum(value),
        RtValue::List(values) => {
            InputValue::List(values.into_iter().map(input_from_runtime).collect())
        }
        RtValue::Object(values) => InputValue::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, input_from_runtime(value)))
                .collect(),
        ),
    }
}

fn input_from_literal(value: &ValueLiteral) -> InputValue {
    match value {
        ValueLiteral::Null => InputValue::Null,
        ValueLiteral::Boolean(value) => InputValue::Bool(*value),
        ValueLiteral::Enum(value) => InputValue::Enum(value.clone()),
        ValueLiteral::Int(value) => match value.trim().parse() {
            Ok(value) => InputValue::Int(value),
            Err(_) => InputValue::Null,
        },
        ValueLiteral::Float(value) => match value.trim().parse() {
            Ok(value) => InputValue::Float(value),
            Err(_) => InputValue::Null,
        },
        ValueLiteral::String(value) => InputValue::Str(value.clone()),
        ValueLiteral::List(values) => {
            InputValue::List(values.iter().map(input_from_literal).collect())
        }
        ValueLiteral::Object(fields) => InputValue::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), input_from_literal(value)))
                .collect(),
        ),
    }
}

fn resolve_schema_field<'s>(
    schema: &'s Schema,
    field: &Field<'_>,
    _variables: &BTreeMap<String, RtValue>,
) -> Result<Resolved<'s>, ExecError> {
    Ok(match field.name.as_str() {
        "types" => Resolved::List {
            items: schema
                .types
                .values()
                .map(|def| Resolved::Object(IntroValue::Type(def.name.as_str()), "__Type"))
                .collect(),
            element_type: None,
        },
        "queryType" => schema
            .query_type
            .as_deref()
            .and_then(|name| schema.types.get(name))
            .map(|def| Resolved::Object(IntroValue::Type(def.name.as_str()), "__Type"))
            .unwrap_or(Resolved::Null),
        "mutationType" => schema
            .mutation_type
            .as_deref()
            .and_then(|name| schema.types.get(name))
            .map(|def| Resolved::Object(IntroValue::Type(def.name.as_str()), "__Type"))
            .unwrap_or(Resolved::Null),
        "subscriptionType" => schema
            .subscription_type
            .as_deref()
            .and_then(|name| schema.types.get(name))
            .map(|def| Resolved::Object(IntroValue::Type(def.name.as_str()), "__Type"))
            .unwrap_or(Resolved::Null),
        "directives" => Resolved::List {
            items: schema
                .directives
                .values()
                .map(|def| Resolved::Object(IntroValue::Directive(def), "__Directive"))
                .collect(),
            element_type: None,
        },
        _ => Resolved::Null,
    })
}

fn resolve_type_field<'s>(
    schema: &'s Schema,
    rendered: &'s str,
    field: &Field<'_>,
    variables: &BTreeMap<String, RtValue>,
) -> Result<Resolved<'s>, ExecError> {
    let wrapper_kind = if rendered.ends_with('!') {
        Some("NON_NULL")
    } else if rendered.starts_with('[') && rendered.ends_with(']') {
        Some("LIST")
    } else {
        None
    };
    let named = schema.types.get(rendered);

    Ok(match field.name.as_str() {
        "kind" => Resolved::Json(Json::Str(
            wrapper_kind
                .map(str::to_owned)
                .or_else(|| named.map(|def| def.kind.introspection_str().to_owned()))
                .unwrap_or_else(|| "SCALAR".to_owned()),
        )),
        "name" => match (wrapper_kind, named) {
            (None, Some(def)) => Resolved::Json(Json::Str(def.name.clone())),
            _ => Resolved::Null,
        },
        "description" => match (wrapper_kind, named) {
            (None, Some(def)) => Resolved::Json(Json::opt_str(def.description.as_deref())),
            _ => Resolved::Null,
        },
        "fields" => {
            let include_deprecated = bool_argument(field, "includeDeprecated", variables);
            match named.map(|def| (def.kind, &def.fields)) {
                Some((TypeKind::Object | TypeKind::Interface, fields)) => Resolved::List {
                    items: fields
                        .iter()
                        .filter(|field| include_deprecated || !is_deprecated(&field.directives))
                        .map(|field| Resolved::Object(IntroValue::Field(field), "__Field"))
                        .collect(),
                    element_type: None,
                },
                _ => Resolved::Null,
            }
        }
        "interfaces" => match named.map(|def| (def.kind, &def.interfaces)) {
            // The baseline quirk: only objects expose `interfaces`.
            Some((TypeKind::Object, interfaces)) => Resolved::List {
                items: interfaces
                    .iter()
                    .filter_map(|name| schema.types.get(name))
                    .map(|def| Resolved::Object(IntroValue::Type(def.name.as_str()), "__Type"))
                    .collect(),
                element_type: None,
            },
            _ => Resolved::Null,
        },
        "possibleTypes" => match named.map(|def| (def.kind, def)) {
            Some((TypeKind::Interface, def)) => Resolved::List {
                items: schema
                    .possible_types(&def.name)
                    .into_iter()
                    .map(|possible| {
                        Resolved::Object(IntroValue::Type(possible.name.as_str()), "__Type")
                    })
                    .collect(),
                element_type: None,
            },
            Some((TypeKind::Union, def)) => Resolved::List {
                items: def
                    .union_members
                    .iter()
                    .filter_map(|name| schema.types.get(name))
                    .map(|member| {
                        Resolved::Object(IntroValue::Type(member.name.as_str()), "__Type")
                    })
                    .collect(),
                element_type: None,
            },
            _ => Resolved::Null,
        },
        "enumValues" => {
            let include_deprecated = bool_argument(field, "includeDeprecated", variables);
            match named.map(|def| (def.kind, &def.enum_values)) {
                Some((TypeKind::Enum, values)) => Resolved::List {
                    items: values
                        .iter()
                        .filter(|value| include_deprecated || !is_deprecated(&value.directives))
                        .map(|value| Resolved::Object(IntroValue::EnumValue(value), "__EnumValue"))
                        .collect(),
                    element_type: None,
                },
                _ => Resolved::Null,
            }
        }
        "inputFields" => match named.map(|def| (def.kind, &def.input_fields)) {
            Some((TypeKind::InputObject, fields)) => Resolved::List {
                items: fields
                    .iter()
                    .map(|field| Resolved::Object(IntroValue::InputValue(field), "__InputValue"))
                    .collect(),
                element_type: None,
            },
            _ => Resolved::Null,
        },
        "ofType" => {
            let inner = if let Some(inner) = rendered.strip_suffix('!') {
                Some(inner)
            } else {
                rendered.strip_prefix('[').and_then(|s| s.strip_suffix(']'))
            };
            match inner {
                Some(inner) => Resolved::Object(IntroValue::Type(inner), "__Type"),
                None => Resolved::Null,
            }
        }
        "specifiedByURL" => match named {
            Some(def) if def.kind == TypeKind::Scalar => {
                match def.directives.iter().find(|d| d.name == "specifiedBy") {
                    Some(directive) => match directive.argument("url") {
                        Some(ValueLiteral::String(url)) => Resolved::Json(Json::Str(url.clone())),
                        _ => Resolved::Null,
                    },
                    None => Resolved::Null,
                }
            }
            _ => Resolved::Null,
        },
        _ => Resolved::Null,
    })
}

fn resolve_field_def_field<'s>(
    _schema: &'s Schema,
    def: &'s FieldDef,
    field: &Field<'_>,
) -> Result<Resolved<'s>, ExecError> {
    Ok(match field.name.as_str() {
        "name" => Resolved::Json(Json::Str(def.name.clone())),
        "description" => Resolved::Json(Json::opt_str(def.description.as_deref())),
        "args" => Resolved::List {
            items: def
                .args
                .iter()
                .map(|arg| Resolved::Object(IntroValue::InputValue(arg), "__InputValue"))
                .collect(),
            element_type: None,
        },
        "type" => Resolved::Object(IntroValue::Type(def.field_type.as_str()), "__Type"),
        "isDeprecated" => Resolved::Json(Json::Bool(is_deprecated(&def.directives))),
        "deprecationReason" => Resolved::Json(deprecation_reason(&def.directives)),
        _ => Resolved::Null,
    })
}

fn resolve_input_value_field<'s>(
    _schema: &'s Schema,
    def: &'s InputValueDef,
    field: &Field<'_>,
) -> Result<Resolved<'s>, ExecError> {
    Ok(match field.name.as_str() {
        "name" => Resolved::Json(Json::Str(def.name.clone())),
        "description" => Resolved::Json(Json::opt_str(def.description.as_deref())),
        "type" => Resolved::Object(IntroValue::Type(def.value_type.as_str()), "__Type"),
        "defaultValue" => Resolved::Json(match &def.default_value {
            Some(value) => Json::Str(value.render()),
            None => Json::Null,
        }),
        _ => Resolved::Null,
    })
}

fn resolve_enum_value_field<'s>(
    def: &'s EnumValueDef,
    field: &Field<'_>,
) -> Result<Resolved<'s>, ExecError> {
    Ok(match field.name.as_str() {
        "name" => Resolved::Json(Json::Str(def.name.clone())),
        "description" => Resolved::Json(Json::opt_str(def.description.as_deref())),
        "isDeprecated" => Resolved::Json(Json::Bool(is_deprecated(&def.directives))),
        "deprecationReason" => Resolved::Json(deprecation_reason(&def.directives)),
        _ => Resolved::Null,
    })
}

fn resolve_directive_field<'s>(
    _schema: &'s Schema,
    def: &'s DirectiveDef,
    field: &Field<'_>,
) -> Result<Resolved<'s>, ExecError> {
    Ok(match field.name.as_str() {
        "name" => Resolved::Json(Json::Str(def.name.clone())),
        "description" => Resolved::Json(Json::opt_str(def.description.as_deref())),
        "locations" => Resolved::List {
            items: def
                .locations
                .iter()
                .map(|location| Resolved::Json(Json::Str(location.clone())))
                .collect(),
            element_type: None,
        },
        "args" => Resolved::List {
            items: def
                .args
                .iter()
                .map(|arg| Resolved::Object(IntroValue::InputValue(arg), "__InputValue"))
                .collect(),
            element_type: None,
        },
        _ => Resolved::Null,
    })
}

fn is_deprecated(directives: &[crate::schema::DirectiveUse]) -> bool {
    directives
        .iter()
        .any(|directive| directive.name == "deprecated")
}

fn deprecation_reason(directives: &[crate::schema::DirectiveUse]) -> Json {
    match directives
        .iter()
        .find(|directive| directive.name == "deprecated")
        .and_then(|directive| directive.deprecation_reason())
    {
        Some(reason) => Json::Str(reason),
        None => Json::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::assemble_sdl;

    fn run(schema: &Schema, query: &str) -> String {
        let variables = serde_json::Map::new();
        execute(
            schema,
            ExecRequest {
                query,
                operation_name: "",
                variables: &variables,
                actor: Actor::Anonymous,
            },
        )
        .to_json()
    }

    #[test]
    fn executes_schema_types_and_type_lookup() {
        let schema = assemble_sdl("type Query { hello: String! }").unwrap();
        let json = run(&schema, "{ __schema { queryType { name } } }");
        assert_eq!(
            json,
            r#"{"data":{"__schema":{"queryType":{"name":"Query"}}}}"#
        );
        let json = run(
            &schema,
            r#"{ __type(name: "Query") { kind name fields { name } } }"#,
        );
        assert_eq!(
            json,
            r#"{"data":{"__type":{"kind":"OBJECT","name":"Query","fields":[{"name":"hello"}]}}}"#
        );
        let json = run(&schema, r#"{ __type(name: "Missing") { kind } }"#);
        assert_eq!(json, r#"{"data":{"__type":null}}"#);
    }

    #[test]
    fn aliases_merge_and_keep_first_seen_order() {
        let schema = assemble_sdl("type Query { hello: String! }").unwrap();
        let json = run(
            &schema,
            "{ a: __schema { queryType { name } } b: __schema { mutationType { name } } }",
        );
        assert_eq!(
            json,
            r#"{"data":{"a":{"queryType":{"name":"Query"}},"b":{"mutationType":null}}}"#
        );
        let json = run(
            &schema,
            "{ x: __schema { queryType { name } } x: __schema { types { name } } }",
        );
        assert!(json.starts_with(r#"{"data":{"x":{"queryType":{"name":"Query"},"types":["#));
    }

    #[test]
    fn fragments_and_variables_are_supported() {
        let schema = assemble_sdl("type Query { hello: String! }").unwrap();
        let query = "query Q($t: String!) { __type(name: $t) { ...T } } \
                     fragment T on __Type { kind name }";
        let mut variables = serde_json::Map::new();
        variables.insert("t".to_owned(), JsonValue::String("Query".to_owned()));
        let json = execute(
            &schema,
            ExecRequest {
                query,
                operation_name: "Q",
                variables: &variables,
                actor: Actor::Anonymous,
            },
        )
        .to_json();
        assert_eq!(
            json,
            r#"{"data":{"__type":{"kind":"OBJECT","name":"Query"}}}"#
        );
    }

    #[test]
    fn skip_include_and_typename() {
        let schema = assemble_sdl("type Query { hello: String! }").unwrap();
        let query = "query($s: Boolean!) { __typename __schema @skip(if: $s) { types { name } } }";
        let mut variables = serde_json::Map::new();
        variables.insert("s".to_owned(), JsonValue::Bool(true));
        let json = execute(
            &schema,
            ExecRequest {
                query,
                operation_name: "",
                variables: &variables,
                actor: Actor::Anonymous,
            },
        )
        .to_json();
        assert_eq!(json, r#"{"data":{"__typename":"Query"}}"#);
    }

    #[test]
    fn validation_errors_omit_data() {
        let schema = assemble_sdl("type Query { hello: String! }").unwrap();
        assert_eq!(
            run(&schema, "{ nope }"),
            r#"{"errors":[{"message":"Cannot query field \"nope\" on type \"Query\".","locations":[{"line":1,"column":3}]}]}"#
        );
        assert_eq!(
            run(&schema, "{ __type { kind } }"),
            r#"{"errors":[{"message":"Field \"__type\" argument \"name\" of type \"String!\" is required but not provided.","locations":[{"line":1,"column":3}]}]}"#
        );
        assert_eq!(
            run(&schema, "{ __schema }"),
            r#"{"errors":[{"message":"Field \"__schema\" of type \"__Schema\" must have a selection of subfields. Did you mean \"__schema { ... }\"?","locations":[{"line":1,"column":3}]}]}"#
        );
    }

    #[test]
    fn operation_selection_and_errors() {
        let schema = assemble_sdl("type Query { hello: String! }").unwrap();
        assert_eq!(
            run(&schema, "query A { __typename } query B { __typename }"),
            r#"{"errors":[{"message":"more than one operation in query document and no operation name given"}]}"#
        );
        assert_eq!(
            run(&schema, "subscription S { __typename }"),
            r#"{"errors":[{"message":"graphql-ws protocol header is missing"}]}"#
        );
    }

    #[test]
    fn service_sdl_is_the_user_sdl() {
        let sdl = "type Query { hello: String! }";
        let schema = assemble_sdl(sdl).unwrap();
        let json = run(&schema, "{ _service { sdl } }");
        let parsed: JsonValue = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed["data"]["_service"]["sdl"].as_str().unwrap(),
            schema.schema_string
        );
        assert_eq!(schema.schema_string, sdl);
    }

    struct UserObject {
        uid: i32,
    }

    impl ObjectValue for UserObject {
        fn type_name(&self) -> &'static str {
            "User"
        }

        fn field(
            &self,
            field: &str,
            _arguments: &Arguments,
            _actor: Actor,
        ) -> Result<Option<FieldValue>, ExecError> {
            Ok(Some(match field {
                "id" => FieldValue::Json(Json::Str(format!("User:{}", self.uid))),
                "username" => FieldValue::Json(Json::Str("alice".to_owned())),
                "friends" => FieldValue::List {
                    items: vec![FieldValue::Object(std::sync::Arc::new(UserObject {
                        uid: 2,
                    }))],
                    element_type: Some("User"),
                },
                "broken" => return Err(ExecError::new("boom")),
                _ => return Ok(None),
            }))
        }
    }

    struct QueryObject;

    impl ObjectValue for QueryObject {
        fn type_name(&self) -> &'static str {
            "Query"
        }

        fn field(
            &self,
            field: &str,
            arguments: &Arguments,
            actor: Actor,
        ) -> Result<Option<FieldValue>, ExecError> {
            Ok(Some(match field {
                "user" => match arguments.string("username") {
                    Some("alice") => FieldValue::Object(std::sync::Arc::new(UserObject { uid: 1 })),
                    _ => FieldValue::Null,
                },
                "users" => FieldValue::List {
                    items: vec![
                        FieldValue::Object(std::sync::Arc::new(UserObject { uid: 1 })),
                        FieldValue::Object(std::sync::Arc::new(UserObject { uid: 2 })),
                    ],
                    element_type: Some("User"),
                },
                "usersWithHole" => FieldValue::List {
                    items: vec![
                        FieldValue::Object(std::sync::Arc::new(UserObject { uid: 1 })),
                        FieldValue::Null,
                    ],
                    element_type: Some("User"),
                },
                "echo" => FieldValue::Json(Json::Str(
                    arguments.string("value").unwrap_or_default().to_owned(),
                )),
                "currentUser" => match actor {
                    Actor::Anonymous => FieldValue::Null,
                    Actor::User { uid } => {
                        FieldValue::Object(std::sync::Arc::new(UserObject { uid }))
                    }
                },
                "requiredNull" => return Err(ExecError::new("null")),
                "requiredUser" => FieldValue::Null,
                "requiredFailure" => return Err(ExecError::new("boom")),
                "flagged" => FieldValue::Json(Json::Str(
                    arguments.string("value").unwrap_or_default().to_owned(),
                )),
                _ => return Ok(None),
            }))
        }
    }

    struct Roots {
        query: QueryObject,
    }

    impl RootResolver for Roots {
        fn root(&self, root_type: &str) -> Option<&dyn ObjectValue> {
            (root_type == "Query").then_some(&self.query as &dyn ObjectValue)
        }
    }

    fn business_schema() -> Schema {
        assemble_sdl(
            "type Query { \
               user(username: String!): User \
               users: [User!]! \
               echo(value: String = \"default\"): String! \
               currentUser: User \
               requiredNull: String! \
               requiredUser: User! \
               requiredFailure: String! \
               usersWithHole: [User!]! \
               flagged(value: String! = \"x\"): String! \
             } \
             type User { id: ID! username: String! friends: [User!]! broken: String }",
        )
        .unwrap()
    }

    fn run_business(schema: &Schema, query: &str, actor: Actor) -> String {
        let variables = serde_json::Map::new();
        let roots = Roots { query: QueryObject };
        execute_with(
            schema,
            ExecRequest {
                query,
                operation_name: "",
                variables: &variables,
                actor,
            },
            Some(&roots),
        )
        .to_json()
    }

    #[test]
    fn business_objects_resolve_nested_selections_and_args() {
        let schema = business_schema();
        assert_eq!(
            run_business(
                &schema,
                r#"{ user(username: "alice") { id username friends { username } } }"#,
                Actor::Anonymous,
            ),
            r#"{"data":{"user":{"id":"User:1","username":"alice","friends":[{"username":"alice"}]}}}"#
        );
        assert_eq!(
            run_business(
                &schema,
                r#"{ user(username: "bob") { id } }"#,
                Actor::Anonymous,
            ),
            r#"{"data":{"user":null}}"#
        );
        // Schema default (`value: String = "default"`) and aliases.
        assert_eq!(
            run_business(&schema, "{ first: echo }", Actor::Anonymous),
            r#"{"data":{"first":"default"}}"#
        );
        assert_eq!(
            run_business(
                &schema,
                r#"{ __typename users { __typename username } }"#,
                Actor::Anonymous,
            ),
            r#"{"data":{"__typename":"Query","users":[{"__typename":"User","username":"alice"},{"__typename":"User","username":"alice"}]}}"#
        );
    }

    #[test]
    fn business_actor_is_threaded_to_resolvers() {
        let schema = business_schema();
        assert_eq!(
            run_business(&schema, "{ currentUser { id } }", Actor::Anonymous),
            r#"{"data":{"currentUser":null}}"#
        );
        assert_eq!(
            run_business(&schema, "{ currentUser { id } }", Actor::User { uid: 7 }),
            r#"{"data":{"currentUser":{"id":"User:7"}}}"#
        );
    }

    #[test]
    fn business_errors_and_non_null_propagation() {
        let schema = business_schema();
        // Resolver errors do not add a `got nil for non-null` error.
        assert_eq!(
            run_business(&schema, "{ requiredNull }", Actor::Anonymous),
            r#"{"errors":[{"message":"null","path":["requiredNull"]}],"data":null}"#
        );
        assert_eq!(
            run_business(&schema, "{ requiredFailure }", Actor::Anonymous),
            r#"{"errors":[{"message":"boom","path":["requiredFailure"]}],"data":null}"#
        );
        assert_eq!(
            run_business(&schema, "{ requiredUser { id } }", Actor::Anonymous),
            r#"{"errors":[{"message":"graphql: got nil for non-null \"User\"","path":["requiredUser"]}],"data":null}"#
        );
        assert_eq!(
            run_business(&schema, "{ usersWithHole { id } }", Actor::Anonymous),
            r#"{"errors":[{"message":"graphql: got nil for non-null \"User\"","path":["usersWithHole",1]}],"data":null}"#
        );
        assert_eq!(
            run_business(&schema, "{ flagged }", Actor::Anonymous),
            r#"{"errors":[{"message":"Field \"flagged\" argument \"value\" of type \"String!\" is required but not provided.","locations":[{"line":1,"column":3}]}]}"#
        );
        assert_eq!(
            run_business(
                &schema,
                r#"{ user(username: "alice") { broken } }"#,
                Actor::Anonymous,
            ),
            r#"{"errors":[{"message":"boom","path":["user","broken"]}],"data":{"user":{"broken":null}}}"#
        );
        assert_eq!(
            run_business(
                &schema,
                r#"{ user(username: "alice") { missing } }"#,
                Actor::Anonymous,
            ),
            r#"{"errors":[{"message":"Cannot query field \"missing\" on type \"User\".","locations":[{"line":1,"column":29}]}]}"#
        );
    }

    #[test]
    fn business_fields_without_resolver_report_an_error() {
        let schema = business_schema();
        assert_eq!(
            run(&schema, "{ users { id } }"),
            r#"{"errors":[{"message":"GraphQL field \"users\" of \"Query\" has no ported resolver","path":["users"]}],"data":null}"#
        );
    }
}
