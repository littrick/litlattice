//! GraphQL query cost estimation.
//!
//! Port of `cmd/frontend/graphqlbackend/rate_limit.go` (`EstimateQueryCost`
//! and `calcNodeCost`). The estimator walks a query AST in the same
//! pre-order as `graphql-go/language/visitor` does (see `QueryDocumentKeys`)
//! and accumulates the worst-case field count, depth, duplicate fields,
//! unique fields and aliases.
//!
//! The baseline parses with `graphql-go/language/parser`; we use
//! `graphql-parser` (already used by the schema PoC) for the same language.
//! Parse error text is not part of any response byte: the HTTP handler only
//! logs cost-estimation errors and continues with schema execution.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use graphql_parser::query::{
    Definition, Directive, Document, Field, FragmentDefinition, FragmentSpread, InlineFragment,
    OperationDefinition, Selection, SelectionSet, Type as QueryType, TypeCondition, Value,
    VariableDefinition,
};

/// `costEstimateVersion`.
pub const COST_ESTIMATE_VERSION: i32 = 2;

/// `graphqlbackend.QueryCost`. Field names mirror the Go struct so harnesses
/// can compare JSON encoded with the same keys.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryCost {
    pub field_count: i64,
    pub max_depth: i64,
    pub highest_duplicate_field_count: i64,
    pub unique_field_count: i64,
    pub alias_count: i64,
    pub version: i32,
}

impl QueryCost {
    /// JSON encoding in `encoding/json` struct-field order (used by the
    /// differential harness).
    pub fn encode(&self) -> String {
        format!(
            "{{\"FieldCount\":{},\"MaxDepth\":{},\"HighestDuplicateFieldCount\":{},\
             \"UniqueFieldCount\":{},\"AliasCount\":{},\"Version\":{}}}",
            self.field_count,
            self.max_depth,
            self.highest_duplicate_field_count,
            self.unique_field_count,
            self.alias_count,
            self.version,
        )
    }
}

/// Error returned by [`estimate_query_cost`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostError {
    message: String,
}

impl CostError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for CostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CostError {}

/// `EstimateQueryCost`: worst-case cost of every operation in `query`.
///
/// `variables` has the shape of the decoded GraphQL request (JSON values).
pub fn estimate_query_cost(
    query: &str,
    variables: &BTreeMap<String, serde_json::Value>,
) -> Result<QueryCost, CostError> {
    let doc: Document<'_, String> = graphql_parser::parse_query(query)
        .map_err(|err| CostError::new(format!("parsing query: {err}")))?;

    let mut fragments: Vec<&FragmentDefinition<'_, String>> = Vec::new();
    let mut operations: Vec<&OperationDefinition<'_, String>> = Vec::new();
    for definition in &doc.definitions {
        match definition {
            Definition::Fragment(fragment) => fragments.push(fragment),
            Definition::Operation(operation) => operations.push(operation),
        }
    }

    // Fragment costs, calculated in dependency order. The baseline loops until
    // every fragment has a cost; a cyclic (or undefined) dependency makes it
    // spin forever. We report a deterministic error instead (see overview
    // errata).
    let mut fragment_deps: HashMap<String, HashSet<String>> = HashMap::new();
    for fragment in &fragments {
        fragment_deps.insert(fragment.name.clone(), fragment_dependencies(fragment));
    }
    let mut fragment_costs: HashMap<String, QueryCost> = HashMap::new();
    let mut fragment_seen: HashSet<String> = HashSet::new();
    while fragment_seen.len() < fragments.len() {
        let mut progress = false;
        for fragment in &fragments {
            let ready = fragment_deps[&fragment.name]
                .iter()
                .all(|dep| fragment_costs.contains_key(dep));
            if !ready {
                continue;
            }
            let result = {
                let mut walker = Walker::new(&mut fragment_costs, variables);
                walker
                    .visit_fragment_definition(fragment)
                    .map(|()| walker.into_cost())
            };
            let cost = result
                .map_err(|err| CostError::new(format!("calculating fragment cost: {err}")))?;
            fragment_costs.insert(fragment.name.clone(), cost);
            if fragment_seen.insert(fragment.name.clone()) {
                progress = true;
            }
        }
        if !progress {
            return Err(CostError::new(
                "cyclic or undefined fragment dependency (baseline loops forever)",
            ));
        }
    }

    let mut total = QueryCost {
        version: COST_ESTIMATE_VERSION,
        ..QueryCost::default()
    };
    for operation in &operations {
        let result = {
            let mut walker = Walker::new(&mut fragment_costs, variables);
            walker
                .visit_operation(operation)
                .map(|()| walker.into_cost())
        };
        let cost =
            result.map_err(|err| CostError::new(format!("calculating operation cost: {err}")))?;
        total.field_count += cost.field_count;
        total.alias_count += cost.alias_count;
        total.highest_duplicate_field_count = total
            .highest_duplicate_field_count
            .max(cost.highest_duplicate_field_count);
        total.unique_field_count += cost.unique_field_count;
        total.max_depth = total.max_depth.max(cost.max_depth);
    }

    if total.field_count < 1 {
        total.field_count = 1;
    }
    if total.max_depth < 1 {
        total.max_depth = 1;
    }

    Ok(total)
}

/// `getFragmentDependencies`.
fn fragment_dependencies(fragment: &FragmentDefinition<'_, String>) -> HashSet<String> {
    let mut deps = HashSet::new();
    collect_spreads(&fragment.selection_set, &mut deps);
    deps
}

fn collect_spreads(selection_set: &SelectionSet<'_, String>, out: &mut HashSet<String>) {
    for selection in &selection_set.items {
        match selection {
            Selection::Field(field) => collect_spreads(&field.selection_set, out),
            Selection::FragmentSpread(spread) => {
                out.insert(spread.fragment_name.clone());
            }
            Selection::InlineFragment(inline) => collect_spreads(&inline.selection_set, out),
        }
    }
}

struct Walker<'a> {
    fragment_costs: &'a mut HashMap<String, QueryCost>,
    variables: &'a BTreeMap<String, serde_json::Value>,

    inline_fragment_depth: i64,
    inline_fragments: Vec<String>,

    limit_stack: Vec<i64>,
    current_limit: i64,
    multiplier: i64,

    alias_count: i64,
    field_count: i64,
    duplicate_field_count: i64,
    unique_field_count: i64,
    depth: i64,
    max_depth: i64,

    count_nodes: HashMap<String, i64>,
    unique_fields: HashSet<String>,
    non_null_variables: HashSet<String>,
    default_values: HashMap<String, String>,
}

impl<'a> Walker<'a> {
    fn new(
        fragment_costs: &'a mut HashMap<String, QueryCost>,
        variables: &'a BTreeMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            fragment_costs,
            variables,
            inline_fragment_depth: 0,
            inline_fragments: Vec::new(),
            limit_stack: Vec::new(),
            current_limit: 1,
            multiplier: 1,
            alias_count: 0,
            field_count: 0,
            duplicate_field_count: 0,
            unique_field_count: 0,
            depth: 0,
            max_depth: 0,
            count_nodes: HashMap::new(),
            unique_fields: HashSet::new(),
            non_null_variables: HashSet::new(),
            default_values: HashMap::new(),
        }
    }

    fn push_limit(&mut self) {
        self.multiplier *= self.current_limit;
        self.limit_stack.push(self.current_limit);
        self.current_limit = 1;
    }

    fn pop_limit(&mut self) {
        if let Some(limit) = self.limit_stack.pop() {
            self.current_limit = limit;
            self.multiplier /= self.current_limit;
        }
    }

    fn visit_fragment_definition(
        &mut self,
        fragment: &FragmentDefinition<'_, String>,
    ) -> Result<(), CostError> {
        self.visit_directives(&fragment.directives)?;
        self.visit_selection_set(&fragment.selection_set)
    }

    fn visit_operation(
        &mut self,
        operation: &OperationDefinition<'_, String>,
    ) -> Result<(), CostError> {
        let (variable_definitions, directives, selection_set) = match operation {
            OperationDefinition::SelectionSet(selection_set) => (None, None, selection_set),
            OperationDefinition::Query(query) => (
                Some(&query.variable_definitions),
                Some(&query.directives),
                &query.selection_set,
            ),
            OperationDefinition::Mutation(mutation) => (
                Some(&mutation.variable_definitions),
                Some(&mutation.directives),
                &mutation.selection_set,
            ),
            OperationDefinition::Subscription(subscription) => (
                Some(&subscription.variable_definitions),
                Some(&subscription.directives),
                &subscription.selection_set,
            ),
        };

        if let Some(variable_definitions) = variable_definitions {
            for definition in variable_definitions {
                self.visit_variable_definition(definition)?;
            }
        }
        if let Some(directives) = directives {
            self.visit_directives(directives)?;
        }
        self.visit_selection_set(selection_set)
    }

    fn visit_variable_definition(
        &mut self,
        definition: &VariableDefinition<'_, String>,
    ) -> Result<(), CostError> {
        if matches!(definition.var_type, QueryType::NonNullType(_)) {
            self.non_null_variables.insert(definition.name.clone());
        }
        if let Some(Value::Int(value)) = &definition.default_value {
            if let Some(value) = value.as_i64() {
                self.default_values
                    .insert(definition.name.clone(), value.to_string());
            }
        }
        Ok(())
    }

    fn visit_selection_set(
        &mut self,
        selection_set: &SelectionSet<'_, String>,
    ) -> Result<(), CostError> {
        // The Go AST has a nil selection set for leaf fields; the parser here
        // represents those as an empty set. Only non-empty sets are entered by
        // the baseline visitor.
        if selection_set.items.is_empty() {
            return Ok(());
        }

        self.depth += 1;
        if self.depth > self.max_depth {
            self.max_depth = self.depth;
        }
        self.push_limit();

        for selection in &selection_set.items {
            self.visit_selection(selection)?;
        }

        self.depth -= 1;
        self.pop_limit();
        Ok(())
    }

    fn visit_selection(&mut self, selection: &Selection<'_, String>) -> Result<(), CostError> {
        match selection {
            Selection::Field(field) => self.visit_field(field),
            Selection::FragmentSpread(spread) => self.visit_fragment_spread(spread),
            Selection::InlineFragment(inline) => self.visit_inline_fragment(inline),
        }
    }

    fn visit_field(&mut self, field: &Field<'_, String>) -> Result<(), CostError> {
        if field.alias.is_some() {
            self.alias_count += 1;
        }

        if !self.unique_fields.contains(&field.name) {
            self.unique_fields.insert(field.name.clone());
        }
        *self.count_nodes.entry(field.name.clone()).or_insert(0) += 1;

        let skipped = field.name == "nodes" || field.name == "__typename";
        if !skipped && self.inline_fragment_depth == 0 {
            self.field_count += self.multiplier;
        }

        // Field children in `QueryDocumentKeys` order: Alias, Name, Arguments,
        // Directives, SelectionSet.
        self.visit_arguments(&field.arguments)?;
        self.visit_directives(&field.directives)?;
        self.visit_selection_set(&field.selection_set)
    }

    fn visit_fragment_spread(
        &mut self,
        spread: &FragmentSpread<'_, String>,
    ) -> Result<(), CostError> {
        let cost = *self
            .fragment_costs
            .get(&spread.fragment_name)
            .ok_or_else(|| {
                CostError::new(format!("unknown fragment \"{}\"", spread.fragment_name))
            })?;

        self.field_count += cost.field_count * self.multiplier;
        self.alias_count += cost.alias_count;
        self.unique_field_count += cost.unique_field_count;
        if cost.highest_duplicate_field_count > self.duplicate_field_count {
            self.duplicate_field_count = cost.highest_duplicate_field_count;
        }

        self.visit_directives(&spread.directives)
    }

    fn visit_inline_fragment(
        &mut self,
        inline: &InlineFragment<'_, String>,
    ) -> Result<(), CostError> {
        let type_name = match &inline.type_condition {
            Some(TypeCondition::On(name)) => name.clone(),
            None => {
                return Err(CostError::new(
                    "inline fragment has no type condition (baseline panics)",
                ))
            }
        };

        self.inline_fragment_depth += 1;
        let result = {
            let mut walker = Walker::new(&mut *self.fragment_costs, self.variables);
            walker
                .visit_selection_set(&inline.selection_set)
                .map(|()| walker.into_cost())
        };
        let mut fragment_cost = result
            .map_err(|err| CostError::new(format!("calculating inline fragment cost: {err}")))?;
        fragment_cost.field_count *= self.multiplier;

        self.fragment_costs.insert(type_name.clone(), fragment_cost);
        self.inline_fragments.push(type_name);

        // InlineFragment children: TypeCondition, Directives, SelectionSet.
        self.visit_directives(&inline.directives)?;
        self.visit_selection_set(&inline.selection_set)?;
        self.inline_fragment_depth -= 1;
        Ok(())
    }

    fn visit_directives(&mut self, directives: &[Directive<'_, String>]) -> Result<(), CostError> {
        for directive in directives {
            self.visit_arguments(&directive.arguments)?;
        }
        Ok(())
    }

    fn visit_arguments(
        &mut self,
        arguments: &[(String, Value<'_, String>)],
    ) -> Result<(), CostError> {
        for (name, value) in arguments {
            self.visit_argument(name, value)?;
        }
        Ok(())
    }

    fn visit_argument(&mut self, name: &str, value: &Value<'_, String>) -> Result<(), CostError> {
        if name == "first" || name == "last" {
            match value {
                Value::Variable(variable) => self.visit_limit_variable(variable)?,
                Value::Int(number) => self.visit_limit_int(number),
                _ => {}
            }
        }
        // Nested values are not limit arguments (`shouldCheckParam` only
        // matches a value whose parent is the `Argument` node).
        self.visit_value_children(value)
    }

    fn visit_limit_variable(&mut self, name: &str) -> Result<(), CostError> {
        let default = self.default_values.get(name).cloned();
        let value: Option<serde_json::Value> = match self.variables.get(name) {
            Some(value) => Some(value.clone()),
            None => {
                if self.non_null_variables.contains(name) {
                    return Err(CostError::new(format!(
                        "missing nonnull variable: \"{name}\""
                    )));
                }
                match default {
                    Some(default) => Some(serde_json::Value::String(default)),
                    None => {
                        self.current_limit = 1;
                        return Ok(());
                    }
                }
            }
        };

        if let Some(value) = value {
            let limit = extract_int(&value)
                .map_err(|err| CostError::new(format!("extracting limit: {err}")))?;
            if limit > 0 {
                self.current_limit = limit;
            }
        }
        Ok(())
    }

    fn visit_limit_int(&mut self, number: &graphql_parser::query::Number) {
        let limit = number.as_i64().unwrap_or(0);
        if limit > 0 {
            self.current_limit = limit;
        }
    }

    fn visit_value_children(&mut self, value: &Value<'_, String>) -> Result<(), CostError> {
        match value {
            Value::List(values) => {
                for value in values {
                    self.visit_value_children(value)?;
                }
            }
            Value::Object(fields) => {
                for value in fields.values() {
                    self.visit_value_children(value)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn into_cost(mut self) -> QueryCost {
        let max_inline_fragment_cost = self
            .inline_fragments
            .iter()
            .filter_map(|name| self.fragment_costs.get(name))
            .map(|cost| cost.field_count)
            .max()
            .unwrap_or(0);

        for count in self.count_nodes.values() {
            if *count > 1 && *count > self.duplicate_field_count {
                self.duplicate_field_count = *count;
            }
        }

        if self.unique_fields.len() as i64 > self.unique_field_count {
            self.unique_field_count = self.unique_fields.len() as i64;
        }

        QueryCost {
            field_count: self.field_count + max_inline_fragment_cost,
            max_depth: self.max_depth,
            alias_count: self.alias_count,
            highest_duplicate_field_count: self.duplicate_field_count,
            unique_field_count: self.unique_field_count,
            version: 0,
        }
    }
}

/// `extractInt`.
fn extract_int(value: &serde_json::Value) -> Result<i64, CostError> {
    match value {
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(value)
            } else if let Some(value) = number.as_u64() {
                Ok(value.min(i64::MAX as u64) as i64)
            } else if let Some(value) = number.as_f64() {
                Ok(value as i64)
            } else {
                Ok(0)
            }
        }
        serde_json::Value::String(value) => atoi(value),
        serde_json::Value::Null => Ok(0),
        other => Err(CostError::new(format!(
            "unknown limit type: {}",
            json_type_name(other)
        ))),
    }
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Go `strconv.Atoi` semantics (base 10, signed 64-bit range).
fn atoi(value: &str) -> Result<i64, CostError> {
    let error =
        |reason: &str| CostError::new(format!("strconv.Atoi: parsing \"{value}\": {reason}"));

    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return Err(error("invalid syntax"));
    }
    let (negative, digits) = match bytes[0] {
        b'+' => (false, &value[1..]),
        b'-' => (true, &value[1..]),
        _ => (false, value),
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(error("invalid syntax"));
    }

    let magnitude = digits
        .parse::<u64>()
        .map_err(|_| error("value out of range"))?;
    if negative {
        if magnitude > (i64::MAX as u64) + 1 {
            return Err(error("value out of range"));
        }
        Ok((magnitude as i64).wrapping_neg())
    } else {
        if magnitude > i64::MAX as u64 {
            return Err(error("value out of range"));
        }
        Ok(magnitude as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variables(pairs: &[(&str, serde_json::Value)]) -> BTreeMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect()
    }

    fn cost(query: &str, vars: &BTreeMap<String, serde_json::Value>) -> QueryCost {
        estimate_query_cost(query, vars).expect("cost")
    }

    #[test]
    fn multiple_top_level_queries() {
        let got = cost(
            "query {\n  thing\n}\nquery{\n  thing\n}\n",
            &BTreeMap::new(),
        );
        assert_eq!(
            got,
            QueryCost {
                field_count: 2,
                unique_field_count: 2,
                max_depth: 1,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn simple_query_no_variables() {
        let got = cost(
            "\nquery SiteProductVersion {\n                site {\n                    productVersion\n                    buildVersion\n                    hasCodeIntelligence\n                }\n            }\n",
            &BTreeMap::new(),
        );
        assert_eq!(
            got,
            QueryCost {
                field_count: 4,
                unique_field_count: 4,
                max_depth: 2,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn nodes_field_not_counted() {
        let got = cost(
            "\nquery{\n  externalServices(first: 10){\n    nodes{\n      displayName\n      webhookURL\n    }\n  }\n  somethingElse\n}\n",
            &BTreeMap::new(),
        );
        assert_eq!(
            got,
            QueryCost {
                field_count: 22,
                unique_field_count: 5,
                max_depth: 3,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn default_variables_supplied() {
        let got = cost(
            "\nquery fetchExternalServices($first: Int = 10){\n  externalServices(first: $first){\n    nodes{\n      displayName\n      webhookURL\n    }\n  }\n}\n",
            &variables(&[("first", serde_json::json!(5))]),
        );
        assert_eq!(
            got,
            QueryCost {
                field_count: 11,
                unique_field_count: 4,
                max_depth: 3,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn default_variables_not_supplied() {
        let got = cost(
            "\nquery fetchExternalServices($first: Int = 10){\n  externalServices(first: $first){\n    nodes{\n      displayName\n      webhookURL\n    }\n  }\n}\n",
            &BTreeMap::new(),
        );
        assert_eq!(
            got,
            QueryCost {
                field_count: 21,
                unique_field_count: 4,
                max_depth: 3,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn query_with_fragments() {
        let query = "\nquery StatusMessages {\n\t statusMessages {\n\t\t ...StatusMessageFields\n\t }\n }\n fragment StatusMessageFields on StatusMessage {\n\t __typename\n\t ... on CloningProgress {\n\t\t message\n\t }\n\t ... on SyncError {\n\t\t message\n\t }\n\t ... on ExternalServiceSyncError {\n\t\t message\n\t\t externalService {\n\t\t\t id\n\t\t\t displayName\n\t\t }\n\t }\n }\n";
        let got = cost(query, &BTreeMap::new());
        assert_eq!(
            got,
            QueryCost {
                field_count: 5,
                unique_field_count: 5,
                highest_duplicate_field_count: 3,
                max_depth: 2,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn simple_inline_fragments() {
        let query = "\nquery{\n    __typename\n\t... on Foo {\n         one\n         two\n     }\n     ... on Bar {\n         one\n     }\n}\n";
        let got = cost(query, &BTreeMap::new());
        assert_eq!(
            got,
            QueryCost {
                field_count: 2,
                highest_duplicate_field_count: 2,
                unique_field_count: 3,
                max_depth: 2,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn search_query() {
        let query = include_str!("../tests/fixtures/search_query.graphql");
        let got = cost(query, &BTreeMap::new());
        assert_eq!(
            got,
            QueryCost {
                field_count: 50,
                highest_duplicate_field_count: 10,
                unique_field_count: 51,
                max_depth: 9,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn allow_null_variables() {
        let query = "\nquery RepositoryComparisonDiff($repo: String!, $base: String, $head: String, $first: Int) {\n  repository(name: $repo) {\n    comparison(base: $base, head: $head) {\n      fileDiffs(first: $first) {\n        nodes {\n          ...FileDiffFields\n        }\n        totalCount\n      }\n    }\n  }\n}\n\nfragment FileDiffFields on FileDiff {\n  oldPath\n  newPath\n  internalID\n}\n";
        let got = cost(
            query,
            &variables(&[
                (
                    "base",
                    serde_json::json!("a46cf4a8b6dc42ea7b7b716e53c49dd3508a8678"),
                ),
                (
                    "head",
                    serde_json::json!("0fd3fb1f4e41ae1f95970beeec1c1f7b2d8a7d06"),
                ),
                (
                    "repo",
                    serde_json::json!("github.com/presslabs/mysql-operator"),
                ),
            ]),
        );
        assert_eq!(
            got,
            QueryCost {
                field_count: 7,
                max_depth: 5,
                unique_field_count: 5,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn nested_named_fragments() {
        let query = "\nquery{\n    __typename\n\t...FooFields\n}\nfragment FooFields on Foo {\n\t...BarFields\n}\nfragment BarFields on Bar {\n\tone\n}\n";
        let got = cost(query, &BTreeMap::new());
        assert_eq!(
            got,
            QueryCost {
                field_count: 1,
                max_depth: 1,
                unique_field_count: 1,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn more_nested_fragments() {
        let query = "\n{\n  node {\n    ...FileFragment\n  }\n}\n\nfragment FileFragment on File {\n  ... on Usable {\n    ...UsableFields\n  }\n}\n\nfragment UsableFields on Usable {\n  isUsable\n}\n";
        let got = cost(query, &BTreeMap::new());
        assert_eq!(
            got,
            QueryCost {
                field_count: 3,
                max_depth: 2,
                unique_field_count: 1,
                version: COST_ESTIMATE_VERSION,
                ..QueryCost::default()
            }
        );
    }

    #[test]
    fn aliases_and_duplicates() {
        let query = "query { a: thing a2: thing thing }";
        let got = cost(query, &BTreeMap::new());
        assert_eq!(got.alias_count, 2);
        assert_eq!(got.highest_duplicate_field_count, 3);
        assert_eq!(got.unique_field_count, 1);
        assert_eq!(got.field_count, 3);

        let query = "query { thing thing thing }";
        let got = cost(query, &BTreeMap::new());
        assert_eq!(got.highest_duplicate_field_count, 3);
        assert_eq!(got.unique_field_count, 1);
    }

    #[test]
    fn limit_multiplier_applies_to_selection_set() {
        let query = "query { items(first: 3) { a b } }";
        let got = cost(query, &BTreeMap::new());
        assert_eq!(got.field_count, 7);
        assert_eq!(got.max_depth, 2);
    }

    #[test]
    fn negative_and_zero_limits_are_ignored() {
        let query = "query { items(first: 0) { a b } }";
        assert_eq!(cost(query, &BTreeMap::new()).field_count, 3);
        let query = "query { items(first: -5) { a b } }";
        assert_eq!(cost(query, &BTreeMap::new()).field_count, 3);
    }

    #[test]
    fn string_variable_limit_is_parsed() {
        let query = "query($n: Int) { items(first: $n) { a b } }";
        let got = cost(query, &variables(&[("n", serde_json::json!("2"))]));
        assert_eq!(got.field_count, 5);
    }

    #[test]
    fn missing_nonnull_variable_is_error() {
        let query = "query($n: Int!) { items(first: $n) { a } }";
        let err = estimate_query_cost(query, &BTreeMap::new()).expect_err("missing variable");
        assert!(err.message().contains("missing nonnull variable"));
    }

    #[test]
    fn unknown_limit_type_is_error() {
        let query = "query($n: Int) { items(first: $n) { a } }";
        let err = estimate_query_cost(query, &variables(&[("n", serde_json::json!(true))]))
            .expect_err("bad limit type");
        assert!(err.message().contains("extracting limit"));
    }

    #[test]
    fn unknown_fragment_is_error() {
        let err = estimate_query_cost("query { ...Missing }", &BTreeMap::new())
            .expect_err("unknown fragment");
        assert!(err.message().contains("unknown fragment"));
    }

    #[test]
    fn syntax_error_is_error() {
        let err = estimate_query_cost("query {", &BTreeMap::new()).expect_err("syntax error");
        assert!(err.message().starts_with("parsing query"));
    }

    #[test]
    fn cyclic_fragment_dependency_is_error() {
        let query = "query { ...A } fragment A on T { ...B } fragment B on T { ...A }";
        let err = estimate_query_cost(query, &BTreeMap::new()).expect_err("cycle");
        assert!(err.message().contains("cyclic"));
    }

    #[test]
    fn inline_fragment_without_type_condition_is_error() {
        let query = "query { ... @include(if: true) { a } }";
        let err = estimate_query_cost(query, &BTreeMap::new()).expect_err("no type condition");
        assert!(err.message().contains("no type condition"));
    }

    #[test]
    fn encode_matches_go_field_order() {
        let got = QueryCost {
            field_count: 1,
            max_depth: 2,
            highest_duplicate_field_count: 3,
            unique_field_count: 4,
            alias_count: 5,
            version: 2,
        };
        assert_eq!(
            got.encode(),
            "{\"FieldCount\":1,\"MaxDepth\":2,\"HighestDuplicateFieldCount\":3,\
             \"UniqueFieldCount\":4,\"AliasCount\":5,\"Version\":2}"
        );
    }
}
