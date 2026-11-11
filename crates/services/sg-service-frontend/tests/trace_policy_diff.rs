//! M9.8jf differential for the trace policy, sampling/attribute semantics,
//! OTel startup rules and the slow-request surface against
//! `cmd/frontend/graphqlbackend/zz_m9jf_oracle_test.go`.
//!
//! Both sides read the same corpus and emit normalised JSON. The Go oracle
//! transcribes the Redis FIFO (unreachable outside `internal/rcache`) and the
//! tracer sampler/conf watcher (which need a real provider/Redis), but drives
//! the live `policy`, `trace` and `slowRequestResolver` code; this harness
//! exercises the Rust ports directly.
//!
//! Env:
//!
//! * `SG_M9JF_DIFF=1`
//! * `M9JF_CORPUS`, `M9JF_OUT`

#![allow(clippy::unwrap_used)]

use std::fs;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use sg_service_frontend::slow_requests_tracer::{
    get_slow_requests_after, guess_repo_name, load_connection, InMemorySlowRequestStore,
    SlowRequest, SlowRequestStore, SLOW_REQUEST_REDIS_FIFO_LIST_PER_PAGE,
};
use sg_service_frontend::trace_policy::{
    error_attribute, graphql_request_name, plan_otel_startup, propagation_header, request_source,
    request_wants_tracing, scoped, set_trace_policy, should_sample, should_trace, stringers,
    Attribute, AttributeValue, RequestTraceContext, SourceType, TracePolicy, TracingConfig,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Corpus {
    policy: Vec<PolicyCase>,
    request_wants_tracing: Vec<RequestCase>,
    transport: Vec<PolicyCase>,
    sampling: Vec<SamplingCase>,
    attributes: Vec<AttributeCase>,
    request_context: Vec<ContextCase>,
    startup: Vec<StartupCase>,
    slow_requests: SlowRequestCorpus,
}

#[derive(Deserialize)]
struct PolicyCase {
    name: String,
    global: String,
    context: Option<bool>,
}

#[derive(Deserialize)]
struct RequestCase {
    name: String,
    header: Option<String>,
    query: String,
}

#[derive(Deserialize)]
struct SamplingCase {
    name: String,
    global: String,
    parent: Option<bool>,
}

#[derive(Deserialize)]
struct AttributeItem {
    key: String,
    kind: String,
    value: Value,
}

#[derive(Deserialize)]
struct AttributeCase {
    name: String,
    kind: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    items: Vec<AttributeItem>,
    #[serde(default)]
    key: String,
    #[serde(default)]
    values: Vec<String>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContextCase {
    name: String,
    request_name: Option<String>,
    source: Option<String>,
}

#[derive(Deserialize)]
struct TracingCase {
    #[serde(default)]
    sampling: String,
    #[serde(rename = "type", default)]
    tracer_type: String,
    #[serde(default)]
    debug: bool,
}

#[derive(Deserialize)]
struct StartupCase {
    name: String,
    tracing: Option<TracingCase>,
    previous: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SlowCapture {
    start: String,
    duration: i64,
    user_id: i32,
    name: String,
    source: String,
    #[serde(default)]
    variables: Map<String, Value>,
    #[serde(default)]
    errors: Vec<String>,
    query: String,
}

#[derive(Deserialize)]
struct SlowQuery {
    name: String,
    after: Option<String>,
}

#[derive(Deserialize)]
struct SlowRequestCorpus {
    limit: i64,
    synthetic: usize,
    captures: Vec<SlowCapture>,
    queries: Vec<SlowQuery>,
}

#[derive(Serialize)]
struct Output {
    policy: Vec<PolicyOut>,
    #[serde(rename = "requestWantsTracing")]
    request_wants_tracing: Vec<WantsOut>,
    transport: Vec<TransportOut>,
    sampling: Vec<SamplingOut>,
    attributes: Vec<AttributeCaseOut>,
    #[serde(rename = "requestContext")]
    request_context: Vec<ContextOut>,
    startup: Vec<StartupOut>,
    #[serde(rename = "slowRequests")]
    slow_requests: SlowOut,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PolicyOut {
    name: String,
    should_trace: bool,
}

#[derive(Serialize)]
struct WantsOut {
    name: String,
    wants: bool,
}

#[derive(Serialize)]
struct TransportOut {
    name: String,
    header: String,
}

#[derive(Serialize)]
struct SamplingOut {
    name: String,
    sample: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AttrOut {
    key: String,
    #[serde(rename = "type")]
    type_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    string_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    int_value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bool_value: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    string_slice_value: Option<Vec<String>>,
}

#[derive(Serialize)]
struct AttributeCaseOut {
    name: String,
    attributes: Vec<AttrOut>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ContextOut {
    name: String,
    request_name: String,
    source: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StartupOut {
    name: String,
    policy: String,
    tracer_type: String,
    debug: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeOut {
    index: String,
    start: String,
    duration: f64,
    user_id: i32,
    user_null: bool,
    name: String,
    source: String,
    variables: String,
    errors: Vec<String>,
    query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    filepath: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repo_name: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryOut {
    name: String,
    nodes: Vec<NodeOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nodes_error: Option<String>,
    total_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_count_error: Option<String>,
    has_next_page: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_info_error: Option<String>,
}

#[derive(Serialize)]
struct SlowOut {
    nodes: Vec<NodeOut>,
    queries: Vec<QueryOut>,
}

fn policy(value: &str) -> TracePolicy {
    TracePolicy::parse(value).unwrap()
}

fn attribute_from_item(item: &AttributeItem) -> Attribute {
    let value = match item.kind.as_str() {
        "string" => AttributeValue::String(item.value.as_str().unwrap().to_owned()),
        "int" => AttributeValue::Int64(item.value.as_i64().unwrap()),
        "bool" => AttributeValue::Bool(item.value.as_bool().unwrap()),
        _ => AttributeValue::StringSlice(
            item.value
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect(),
        ),
    };
    Attribute {
        key: item.key.clone(),
        value,
    }
}

fn flatten(attributes: Vec<Attribute>) -> Vec<AttrOut> {
    attributes
        .into_iter()
        .map(|attribute| {
            let mut row = AttrOut {
                key: attribute.key,
                type_name: attribute.value.type_name().to_owned(),
                string_value: None,
                int_value: None,
                bool_value: None,
                string_slice_value: None,
            };
            match attribute.value {
                AttributeValue::String(value) => row.string_value = Some(value),
                AttributeValue::Int64(value) => row.int_value = Some(value),
                AttributeValue::Bool(value) => row.bool_value = Some(value),
                AttributeValue::StringSlice(value) => row.string_slice_value = Some(value),
            }
            row
        })
        .collect()
}

fn node(request: &SlowRequest) -> NodeOut {
    NodeOut {
        index: request.index.clone(),
        start: request.start.clone(),
        duration: request.duration_seconds(),
        user_id: request.user_id,
        user_null: request.user_id == 0,
        name: request.name.clone(),
        source: request.source.clone(),
        variables: request.variables_json(),
        errors: request.errors.clone(),
        query: request.query.clone(),
        filepath: request.filepath(),
        repo_name: guess_repo_name(&request.variables),
    }
}

fn synthetic(index: usize) -> SlowRequest {
    SlowRequest {
        start: "2024-01-01T00:00:00Z".to_owned(),
        duration: 1_500_000_000,
        name: format!("S{index}"),
        source: "other".to_owned(),
        errors: Vec::new(),
        ..SlowRequest::default()
    }
}

fn capture(request: &SlowCapture) -> SlowRequest {
    SlowRequest {
        start: request.start.clone(),
        duration: request.duration,
        user_id: request.user_id,
        name: request.name.clone(),
        source: request.source.clone(),
        variables: request.variables.clone(),
        errors: request.errors.clone(),
        query: request.query.clone(),
        ..SlowRequest::default()
    }
}

#[test]
fn trace_policy_diff() {
    if std::env::var("SG_M9JF_DIFF").as_deref() != Ok("1") {
        eprintln!("skipping: set SG_M9JF_DIFF=1");
        return;
    }
    let corpus_path = std::env::var("M9JF_CORPUS").unwrap();
    let out_path = std::env::var("M9JF_OUT").unwrap();
    let corpus: Corpus = serde_json::from_slice(&fs::read(corpus_path).unwrap()).unwrap();

    let mut policy_out = Vec::new();
    for case in &corpus.policy {
        set_trace_policy(policy(&case.global));
        policy_out.push(PolicyOut {
            name: case.name.clone(),
            should_trace: should_trace(case.context),
        });
    }

    let mut wants_out = Vec::new();
    for case in &corpus.request_wants_tracing {
        wants_out.push(WantsOut {
            name: case.name.clone(),
            wants: request_wants_tracing(case.header.as_deref(), &case.query),
        });
    }

    let mut transport_out = Vec::new();
    for case in &corpus.transport {
        set_trace_policy(policy(&case.global));
        transport_out.push(TransportOut {
            name: case.name.clone(),
            header: propagation_header(should_trace(case.context)).to_owned(),
        });
    }

    let mut sampling_out = Vec::new();
    for case in &corpus.sampling {
        set_trace_policy(policy(&case.global));
        sampling_out.push(SamplingOut {
            name: case.name.clone(),
            sample: should_sample(policy(&case.global), case.parent),
        });
    }

    let mut attribute_out = Vec::new();
    for case in &corpus.attributes {
        let attributes = match case.kind.as_str() {
            "scoped" => scoped(
                &case.scope,
                case.items.iter().map(attribute_from_item).collect(),
            ),
            "stringers" => vec![stringers(&case.key, &case.values)],
            _ => vec![error_attribute(case.error.as_deref())],
        };
        attribute_out.push(AttributeCaseOut {
            name: case.name.clone(),
            attributes: flatten(attributes),
        });
    }

    let mut context_out = Vec::new();
    for case in &corpus.request_context {
        let context = RequestTraceContext {
            should_trace: None,
            graphql_request_name: case.request_name.clone(),
            source: case.source.as_deref().map(|source| {
                if source == "browser" {
                    SourceType::Browser
                } else {
                    SourceType::Other
                }
            }),
        };
        context_out.push(ContextOut {
            name: case.name.clone(),
            request_name: graphql_request_name(context.graphql_request_name.as_deref()),
            source: request_source(context.source).as_str().to_owned(),
        });
    }

    let mut startup_out = Vec::new();
    for case in &corpus.startup {
        let previous = policy(&case.previous);
        set_trace_policy(previous);
        let tracing = case.tracing.as_ref().map(|tracing| TracingConfig {
            sampling: tracing.sampling.clone(),
            tracer_type: tracing.tracer_type.clone(),
            debug: tracing.debug,
        });
        let plan = plan_otel_startup(tracing.as_ref(), previous);
        startup_out.push(StartupOut {
            name: case.name.clone(),
            policy: plan.policy.as_str().to_owned(),
            tracer_type: plan.tracer_type.as_str().to_owned(),
            debug: plan.debug,
        });
    }

    let store = InMemorySlowRequestStore::new(corpus.slow_requests.limit);
    for index in 0..corpus.slow_requests.synthetic {
        store.insert(synthetic(index)).unwrap();
    }
    for request in &corpus.slow_requests.captures {
        store.insert(capture(request)).unwrap();
    }

    let size = store.len().unwrap();
    let all_nodes: Vec<NodeOut> = get_slow_requests_after(&store, 0, size)
        .unwrap()
        .iter()
        .map(node)
        .collect();

    let mut queries = Vec::new();
    for query in &corpus.slow_requests.queries {
        let after = query.after.as_deref().unwrap_or("0");
        let connection = load_connection(&store, after, SLOW_REQUEST_REDIS_FIFO_LIST_PER_PAGE);
        queries.push(QueryOut {
            name: query.name.clone(),
            nodes: connection.nodes.iter().map(node).collect(),
            nodes_error: connection.nodes_error,
            total_count: connection.total_count,
            total_count_error: connection.total_count_error,
            has_next_page: connection.has_next_page,
            end_cursor: connection.end_cursor,
            page_info_error: connection.page_info_error,
        });
    }

    let output = Output {
        policy: policy_out,
        request_wants_tracing: wants_out,
        transport: transport_out,
        sampling: sampling_out,
        attributes: attribute_out,
        request_context: context_out,
        startup: startup_out,
        slow_requests: SlowOut {
            nodes: all_nodes,
            queries,
        },
    };

    let mut encoded = serde_json::to_vec(&output).unwrap();
    encoded.push(b'\n');
    fs::write(&out_path, &encoded).unwrap();
    eprintln!("RUST wrote {} bytes", encoded.len());
}
