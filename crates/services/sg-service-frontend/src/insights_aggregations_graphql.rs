#![forbid(unsafe_code)]

//! `SearchQueryAggregate` GraphQL surface (M9.34a).
//!
//! Ports `cmd/frontend/graphqlbackend/insights_aggregations.graphql` and the
//! resolver shape from
//! `cmd/frontend/internal/insights/resolvers/aggregates_resolvers.go`:
//!
//! - `Query.searchQueryAggregate(query, patternType)`;
//! - `SearchQueryAggregate.modeAvailability` / `.aggregations`;
//! - the `SearchAggregationResult` union (`Exhaustive` / `NonExhaustive` /
//!   `NotAvailable`) and `AggregationGroup`.
//!
//! Availability, default-mode selection, result shaping and every error text
//! come from the M9.33a port in [`crate::insights_aggregations`]. That module
//! remains the single source of truth; this file only maps its data onto
//! GraphQL objects.
//!
//! The SDL (`INSIGHTS_AGGREGATIONS_SCHEMA`) is opt-in: the default
//! `assemble_oss()` schema never includes it, so introspection is unchanged.
//!
//! No table, column or Redis key is added.

use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;

use crate::insights_aggregations::{
    mode_availability, search_aggregations, AggregationGroup, AggregationNotAvailableReasonType,
    AggregationResult, AggregationResults, AggregationSearchSeam, ModeAvailability,
    SearchAggregationMode, SearchAggregationsInput,
};

/// `defaultSearchTimeLimitSeconds`-independent default for the `limit`
/// argument (`insights_aggregations.graphql`: `limit: Int = 50`).
pub const DEFAULT_LIMIT: i32 = 50;

/// The error the unconnected production seam returns; mirrors
/// `UnavailablePreviewSeam`'s `search execution is not connected`.
pub const NOT_CONNECTED: &str = "search execution is not connected";

// ---------------------------------------------------------------------------
// Seam
// ---------------------------------------------------------------------------

/// The GraphQL-facing aggregation seam: mode availability plus one
/// `aggregations(...)` call. Split from [`AggregationSearchSeam`] so the
/// differential harness can feed fixed payloads while production delegates to
/// the M9.33a engine.
pub trait AggregationGraphqlSeam: Send + Sync + std::fmt::Debug {
    /// `searchAggregateResolver.ModeAvailability`.
    fn mode_availability(&self, query: &str, pattern_type: &str) -> Vec<ModeAvailability>;

    /// `searchAggregateResolver.Aggregations`; `Err` becomes a GraphQL error.
    fn aggregations(
        &self,
        input: &SearchAggregationsInput,
    ) -> Result<AggregationResult, String>;
}

/// Production adapter: pure availability plus the M9.33a orchestration over an
/// injected [`AggregationSearchSeam`].
#[derive(Debug)]
pub struct ProductionAggregationSeam {
    search: Arc<dyn AggregationSearchSeam>,
}

impl ProductionAggregationSeam {
    pub fn new(search: Arc<dyn AggregationSearchSeam>) -> Self {
        Self { search }
    }
}

impl AggregationGraphqlSeam for ProductionAggregationSeam {
    fn mode_availability(&self, query: &str, pattern_type: &str) -> Vec<ModeAvailability> {
        mode_availability(query, pattern_type)
    }

    fn aggregations(
        &self,
        input: &SearchAggregationsInput,
    ) -> Result<AggregationResult, String> {
        search_aggregations(self.search.as_ref(), input)
    }
}

/// Default seam: the streaming/tabulator search stack is not wired yet.
/// Availability is still answered (it only parses the query); aggregations
/// fail exactly like the M9.33a `UnavailablePreviewSeam`.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnconnectedAggregationSeam;

impl AggregationGraphqlSeam for UnconnectedAggregationSeam {
    fn mode_availability(&self, query: &str, pattern_type: &str) -> Vec<ModeAvailability> {
        mode_availability(query, pattern_type)
    }

    fn aggregations(
        &self,
        _input: &SearchAggregationsInput,
    ) -> Result<AggregationResult, String> {
        Err(NOT_CONNECTED.to_owned())
    }
}

/// The handle stored on the root resolver.
pub type AggregationsHandle = Arc<dyn AggregationGraphqlSeam>;

// ---------------------------------------------------------------------------
// Object model
// ---------------------------------------------------------------------------

fn json_str(value: impl Into<String>) -> FieldValue {
    FieldValue::Json(Json::Str(value.into()))
}

fn optional_json_str(value: Option<&str>) -> FieldValue {
    match value {
        Some(value) => json_str(value),
        None => FieldValue::Null,
    }
}

/// `SearchQueryAggregate`.
struct SearchQueryAggregateObject {
    query: String,
    pattern_type: String,
    seam: AggregationsHandle,
}

impl ObjectValue for SearchQueryAggregateObject {
    fn type_name(&self) -> &'static str {
        "SearchQueryAggregate"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `searchAggregateResolver.ModeAvailability`.
            "modeAvailability" => {
                let availability = self
                    .seam
                    .mode_availability(&self.query, &self.pattern_type);
                Ok(Some(FieldValue::List {
                    element_type: Some("AggregationModeAvailability"),
                    items: availability
                        .into_iter()
                        .map(|item| {
                            FieldValue::Object(Arc::new(AggregationModeAvailabilityObject {
                                availability: item,
                            }))
                        })
                        .collect(),
                }))
            }
            // `searchAggregateResolver.Aggregations`.
            "aggregations" => {
                let mode = match arguments.string("mode") {
                    Some(raw) => match SearchAggregationMode::from_raw(raw) {
                        Some(mode) => Some(mode),
                        // The schema enumerates the legal values; an unknown
                        // string is a GraphQL-level error in the baseline.
                        None => {
                            return Err(ExecError::new(format!(
                                "unknown SearchAggregationMode {raw}"
                            )))
                        }
                    },
                    None => None,
                };
                let limit = arguments.i64("limit").unwrap_or(i64::from(DEFAULT_LIMIT)) as i32;
                let extended_timeout = arguments.bool("extendedTimeout").unwrap_or(false);
                let input = SearchAggregationsInput {
                    query: self.query.clone(),
                    pattern_type: self.pattern_type.clone(),
                    mode,
                    limit,
                    extended_timeout,
                };
                let result = self.seam.aggregations(&input).map_err(ExecError::new)?;
                Ok(Some(aggregation_result_value(result)))
            }
            _ => Ok(None),
        }
    }
}

fn aggregation_result_value(result: AggregationResult) -> FieldValue {
    match result {
        AggregationResult::Exhaustive { results, mode } => {
            FieldValue::Object(Arc::new(ExhaustiveResultObject { results, mode }))
        }
        AggregationResult::NonExhaustive { results, mode } => {
            FieldValue::Object(Arc::new(NonExhaustiveResultObject { results, mode }))
        }
        AggregationResult::NotAvailable {
            reason,
            reason_type,
            mode,
        } => FieldValue::Object(Arc::new(NotAvailableObject {
            reason,
            reason_type,
            mode,
        })),
    }
}

/// `AggregationModeAvailability`.
struct AggregationModeAvailabilityObject {
    availability: ModeAvailability,
}

impl ObjectValue for AggregationModeAvailabilityObject {
    fn type_name(&self) -> &'static str {
        "AggregationModeAvailability"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "mode" => Ok(Some(json_str(self.availability.mode.as_str()))),
            "available" => Ok(Some(FieldValue::Json(Json::Bool(
                self.availability.available,
            )))),
            "reasonUnavailable" => Ok(Some(optional_json_str(
                self.availability.reason_unavailable.as_deref(),
            ))),
            _ => Ok(None),
        }
    }
}

fn groups_value(results: &AggregationResults) -> FieldValue {
    FieldValue::List {
        element_type: Some("AggregationGroup"),
        items: results
            .groups
            .iter()
            .map(|group| {
                FieldValue::Object(Arc::new(AggregationGroupObject {
                    group: group.clone(),
                }))
            })
            .collect(),
    }
}

/// `ExhaustiveSearchAggregationResult`.
struct ExhaustiveResultObject {
    results: AggregationResults,
    mode: SearchAggregationMode,
}

impl ObjectValue for ExhaustiveResultObject {
    fn type_name(&self) -> &'static str {
        "ExhaustiveSearchAggregationResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "groups" => Ok(Some(groups_value(&self.results))),
            // `searchAggregationModeResultResolver.SupportsPersistence` always
            // returns `false`.
            "supportsPersistence" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "otherResultCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.results.other_result_count,
            ))))),
            "otherGroupCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.results.other_group_count,
            ))))),
            "mode" => Ok(Some(json_str(self.mode.as_str()))),
            _ => Ok(None),
        }
    }
}

/// `NonExhaustiveSearchAggregationResult`.
struct NonExhaustiveResultObject {
    results: AggregationResults,
    mode: SearchAggregationMode,
}

impl ObjectValue for NonExhaustiveResultObject {
    fn type_name(&self) -> &'static str {
        "NonExhaustiveSearchAggregationResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "groups" => Ok(Some(groups_value(&self.results))),
            "supportsPersistence" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "otherResultCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.results.other_result_count,
            ))))),
            // `ApproximateOtherGroupCount` reuses `otherGroupCount`.
            "approximateOtherGroupCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.results.other_group_count,
            ))))),
            "mode" => Ok(Some(json_str(self.mode.as_str()))),
            _ => Ok(None),
        }
    }
}

/// `SearchAggregationNotAvailable`.
struct NotAvailableObject {
    reason: String,
    reason_type: AggregationNotAvailableReasonType,
    mode: SearchAggregationMode,
}

impl ObjectValue for NotAvailableObject {
    fn type_name(&self) -> &'static str {
        "SearchAggregationNotAvailable"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "reason" => Ok(Some(json_str(&self.reason))),
            // Erratum: the baseline's `types.ERROR_OCCURRED` ("ERROR_OCCURRED")
            // is not a member of the SDL enum (`OTHER_ERROR` is), so
            // graph-gophers fails enum serialization whenever this field is
            // selected for an `ERROR_OCCURRED` reason. Match that wire error
            // verbatim rather than emitting the out-of-enum value.
            "reasonType" if self.reason_type == AggregationNotAvailableReasonType::ErrorOccurred => {
                Err(ExecError::new(
                    "Invalid value ERROR_OCCURRED.\n\
                     Expected type NotAvailableReasonType, found ERROR_OCCURRED."
                        .to_owned(),
                ))
            }
            "reasonType" => Ok(Some(json_str(self.reason_type.as_str()))),
            "mode" => Ok(Some(json_str(self.mode.as_str()))),
            _ => Ok(None),
        }
    }
}

/// `AggregationGroup`.
struct AggregationGroupObject {
    group: AggregationGroup,
}

impl ObjectValue for AggregationGroupObject {
    fn type_name(&self) -> &'static str {
        "AggregationGroup"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "label" => Ok(Some(json_str(&self.group.label))),
            "count" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.group.count,
            ))))),
            "query" => Ok(Some(optional_json_str(self.group.query.as_deref()))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Root field
// ---------------------------------------------------------------------------

/// `Query.searchQueryAggregate` (`insights_aggregations.graphql`).
pub fn is_root_field(field: &str) -> bool {
    field == "searchQueryAggregate"
}

/// Resolve `Query.searchQueryAggregate`, if `field` names it.
pub fn root_field(
    seam: &AggregationsHandle,
    field: &str,
    arguments: &Arguments,
    _actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if !is_root_field(field) {
        return Ok(None);
    }
    let (Some(query), Some(pattern_type)) =
        (arguments.string("query"), arguments.string("patternType"))
    else {
        // Both arguments are non-null in the schema, so validation normally
        // rejects a missing one before reaching the resolver.
        return Ok(None);
    };
    Ok(Some(FieldValue::Object(Arc::new(
        SearchQueryAggregateObject {
            query: query.to_owned(),
            pattern_type: pattern_type.to_owned(),
            seam: Arc::clone(seam),
        },
    ))))
}
