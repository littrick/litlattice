#![forbid(unsafe_code)]

//! M9.34a differential harness for the `SearchQueryAggregate` GraphQL surface.
//!
//! Consumes `data/search_query_aggregate/corpus.json`, executes each case
//! through the assembled opt-in schema
//! (`assemble_oss()` + `insights_aggregations.graphql`) and writes the
//! normalized `{"data": ..., "errors": [{"message": ...}]}` envelope. The Go
//! oracle (`cmd/frontend/graphqlbackend/zz_m934a_aggregate_oracle_test.go`,
//! untracked) executes the same corpus through graph-gophers; both are
//! compared by `run_search_query_aggregate_diff.sh`.
//!
//! The corpus carries precomputed availability/aggregation payloads, so this
//! harness pins the GraphQL layer (SDL inclusion, field/argument names, enum
//! serialization, union fragments, nullability). The arithmetic itself is
//! covered by the M9.33a differential.
//!
//! Gated by `SG_M934A_AGG_DIFF=1`.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;
use sg_service_frontend::insights_aggregations::{
    AggregationGroup, AggregationNotAvailableReasonType, AggregationResult, AggregationResults,
    ModeAvailability, SearchAggregationMode, SearchAggregationsInput,
};
use sg_service_frontend::insights_aggregations_graphql::{
    AggregationGraphqlSeam, AggregationsHandle,
};

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    gql: String,
    #[serde(default)]
    availability: Vec<AvailCase>,
    #[serde(default)]
    aggregation: Option<AggCase>,
    #[serde(default, rename = "expectedMode")]
    expected_mode: Option<String>,
    #[serde(default, rename = "expectedLimit")]
    expected_limit: Option<i32>,
    #[serde(default, rename = "expectedExtendedTimeout")]
    expected_extended_timeout: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AvailCase {
    mode: String,
    available: bool,
    #[serde(default, rename = "reasonUnavailable")]
    reason_unavailable: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AggCase {
    kind: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    groups: Vec<GroupCase>,
    #[serde(default, rename = "otherResultCount")]
    other_result_count: i32,
    #[serde(default, rename = "otherGroupCount")]
    other_group_count: i32,
    #[serde(default)]
    reason: String,
    #[serde(default, rename = "reasonType")]
    reason_type: String,
    #[serde(default)]
    message: String,
}

#[derive(Debug, Deserialize)]
struct GroupCase {
    label: String,
    count: i32,
    #[serde(default)]
    query: Option<String>,
}

fn parse_mode(raw: &str) -> SearchAggregationMode {
    SearchAggregationMode::from_raw(raw)
        .unwrap_or_else(|| panic!("unknown aggregation mode {raw:?}"))
}

fn parse_reason_type(raw: &str) -> AggregationNotAvailableReasonType {
    match raw {
        "INVALID_QUERY" => AggregationNotAvailableReasonType::InvalidQuery,
        "INVALID_AGGREGATION_MODE_FOR_QUERY" => {
            AggregationNotAvailableReasonType::InvalidAggregationModeForQuery
        }
        "TIMEOUT_EXTENSION_AVAILABLE" => {
            AggregationNotAvailableReasonType::TimeoutExtensionAvailable
        }
        "TIMEOUT_NO_EXTENSION_AVAILABLE" => {
            AggregationNotAvailableReasonType::TimeoutNoExtensionAvailable
        }
        "ERROR_OCCURRED" => AggregationNotAvailableReasonType::ErrorOccurred,
        "OTHER_ERROR" => AggregationNotAvailableReasonType::OtherError,
        other => panic!("unknown reason type {other:?}"),
    }
}

fn results(case: &AggCase) -> AggregationResults {
    AggregationResults {
        groups: case
            .groups
            .iter()
            .map(|group| AggregationGroup {
                label: group.label.clone(),
                count: group.count,
                query: group.query.clone(),
            })
            .collect(),
        other_result_count: case.other_result_count,
        other_group_count: case.other_group_count,
        total_count: case
            .groups
            .iter()
            .map(|group| group.count.max(0) as u32)
            .sum(),
    }
}

fn aggregation_result(case: &AggCase) -> Result<AggregationResult, String> {
    match case.kind.as_str() {
        "exhaustive" => Ok(AggregationResult::Exhaustive {
            results: results(case),
            mode: parse_mode(&case.mode),
        }),
        "nonexhaustive" => Ok(AggregationResult::NonExhaustive {
            results: results(case),
            mode: parse_mode(&case.mode),
        }),
        "notAvailable" => Ok(AggregationResult::NotAvailable {
            reason: case.reason.clone(),
            reason_type: parse_reason_type(&case.reason_type),
            mode: parse_mode(&case.mode),
        }),
        "error" => Err(case.message.clone()),
        other => panic!("unknown aggregation kind {other:?}"),
    }
}

#[derive(Debug)]
struct FakeSeam {
    availability: Vec<ModeAvailability>,
    aggregation: Option<Result<AggregationResult, String>>,
    expected_mode: Option<String>,
    expected_limit: Option<i32>,
    expected_extended_timeout: Option<bool>,
}

impl FakeSeam {
    fn from_case(case: &Case) -> Self {
        Self {
            availability: case
                .availability
                .iter()
                .map(|item| ModeAvailability {
                    mode: parse_mode(&item.mode),
                    available: item.available,
                    reason_unavailable: item.reason_unavailable.clone(),
                })
                .collect(),
            aggregation: case.aggregation.as_ref().map(aggregation_result),
            expected_mode: case.expected_mode.clone(),
            expected_limit: case.expected_limit,
            expected_extended_timeout: case.expected_extended_timeout,
        }
    }
}

impl AggregationGraphqlSeam for FakeSeam {
    fn mode_availability(&self, _query: &str, _pattern_type: &str) -> Vec<ModeAvailability> {
        self.availability.clone()
    }

    fn aggregations(
        &self,
        input: &SearchAggregationsInput,
    ) -> Result<AggregationResult, String> {
        let expected = self
            .aggregation
            .as_ref()
            .expect("aggregations called without a corpus payload");
        let observed_mode = input.mode.map(SearchAggregationMode::as_str);
        assert_eq!(
            observed_mode,
            self.expected_mode.as_deref(),
            "mode argument mismatch"
        );
        if let Some(limit) = self.expected_limit {
            assert_eq!(input.limit, limit, "limit argument mismatch");
        }
        if let Some(extended) = self.expected_extended_timeout {
            assert_eq!(
                input.extended_timeout, extended,
                "extendedTimeout argument mismatch"
            );
        }
        expected.clone()
    }
}

fn run_case(case: &Case) -> Value {
    let schema = sg_graphql::schema::assemble(&[
        sg_graphql::schema::MAIN_SCHEMA,
        sg_graphql::schema::OUTBOUND_WEBHOOKS_SCHEMA,
        sg_graphql::schema::VIEWER_SCHEMA,
        sg_graphql::schema::INSIGHTS_AGGREGATIONS_SCHEMA,
    ])
    .expect("opt-in schema assembles");
    let seam: AggregationsHandle = Arc::new(FakeSeam::from_case(case));
    let roots = FrontendRootResolver::new().with_insights_aggregations(Some(seam));
    let variables = serde_json::Map::new();
    let response = execute_with(
        &schema,
        ExecRequest {
            query: &case.gql,
            operation_name: "",
            variables: &variables,
            actor: Actor::Anonymous,
        },
        Some(&roots),
    );

    let mut envelope = serde_json::Map::new();
    if let Some(data) = response.data_json() {
        envelope.insert(
            "data".to_owned(),
            serde_json::from_str(&data).expect("valid data JSON"),
        );
    }
    if !response.errors.is_empty() {
        let errors: Vec<Value> = response
            .errors
            .iter()
            .map(|error| json!({ "message": error.message }))
            .collect();
        envelope.insert("errors".to_owned(), Value::Array(errors));
    }
    Value::Object(envelope)
}

#[test]
fn search_query_aggregate_diff() {
    if std::env::var("SG_M934A_AGG_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M934A_CORPUS").expect("M934A_CORPUS");
    let out_dir = PathBuf::from(std::env::var("M934A_OUT").expect("M934A_OUT"));
    fs::create_dir_all(&out_dir).unwrap();

    let corpus: Corpus =
        serde_json::from_str(&fs::read_to_string(&corpus_path).unwrap()).unwrap();
    for case in &corpus.cases {
        let document = run_case(case);
        fs::write(
            out_dir.join(format!("{}.json", case.name)),
            serde_json::to_string_pretty(&document).unwrap(),
        )
        .unwrap();
    }
}
