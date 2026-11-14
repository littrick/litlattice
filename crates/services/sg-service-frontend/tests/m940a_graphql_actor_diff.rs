#![forbid(unsafe_code)]

//! M9.40a differential harness (Rust side): the GraphQL `search` root field
//! passes the request actor (`actor.FromContext(ctx)`) into the search
//! executor, which injects it into `tree_inputs_with_context` through
//! `SearchDeps::execute_with_context`.
//!
//! The Go baseline has no explicit parameter: the actor lives in
//! `context.Context` and reaches the job tree unchanged
//! (`cmd/frontend/graphqlbackend/search.go:57`). This harness pins the Rust
//! equivalent at the GraphQL boundary for the three executor actor shapes
//! plus `dotcom.SourcegraphDotComMode()`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio_stream::Stream;

use sg_graphql::exec::{Actor, Arguments, InputValue, RootField};
use sg_service_frontend::search_client::{Features, Protocol, SearchMode};
use sg_service_frontend::search_graphql::{
    prefetch_search_roots, BackendSearchExecutor, SearchExecutor, SearchRoot,
};
use sg_service_frontend::search_stream::convert::RepoMetadata;
use sg_service_frontend::search_stream::{
    ExecuteItem, PlanError, SearchDeps, SearchInputs,
};
use sg_service_frontend::search_contexts::RequestContext;
use sg_search_query::parser::SearchType;

/// Records the `RequestContext` the GraphQL executor hands to the job tree.
#[derive(Default)]
struct RecordingDeps {
    seen: Mutex<Vec<RequestContext>>,
    dotcom: bool,
}

#[async_trait]
impl SearchDeps for RecordingDeps {
    async fn plan(&self, _args: &sg_service_frontend::search_stream::args::Args) -> Result<SearchInputs, PlanError> {
        Ok(SearchInputs {
            max_results: 30,
            original_query: "type:file".to_owned(),
            plan: Vec::new(),
            search_mode: SearchMode::Precise,
            pattern_type: SearchType::Literal,
            features: Features::default(),
            protocol: Protocol::Batch,
            context_lines: 0,
            sanitize_search_patterns: Vec::new(),
            on_sourcegraph_dot_com: self.dotcom,
            cody_context: None,
        })
    }

    async fn execute(
        &self,
        _inputs: &SearchInputs,
        _query: &str,
    ) -> Pin<Box<dyn Stream<Item = ExecuteItem> + Send>> {
        Box::pin(tokio_stream::iter(Vec::new()))
    }

    async fn execute_with_context(
        &self,
        _inputs: &SearchInputs,
        _query: &str,
        request_context: RequestContext,
    ) -> Pin<Box<dyn Stream<Item = ExecuteItem> + Send>> {
        self.seen
            .lock()
            .expect("record request context")
            .push(request_context);
        Box::pin(tokio_stream::iter(Vec::new()))
    }

    async fn repo_metadata(&self, _ids: &[i32]) -> Result<HashMap<i32, RepoMetadata>, String> {
        Ok(HashMap::new())
    }

    async fn repo_names(&self, _ids: &[i32]) -> Vec<(i32, String)> {
        Vec::new()
    }

}

fn search_root_field() -> RootField {
    RootField {
        name: "search".to_owned(),
        arguments: Arguments::from_entries(vec![
            ("version".to_owned(), InputValue::Str("V3".to_owned())),
            ("query".to_owned(), InputValue::Str("type:file".to_owned())),
        ]),
        selections: Vec::new(),
        type_conditions: Vec::new(),
    }
}

fn run(actor: Actor, dotcom: bool) -> RequestContext {
    let deps = Arc::new(RecordingDeps {
        seen: Mutex::new(Vec::new()),
        dotcom,
    });
    let executor = BackendSearchExecutor::new(deps.clone());
    let fields = vec![search_root_field()];
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("graphql actor diff runtime");
    let root = runtime.block_on(prefetch_search_roots(&executor, &fields, actor));
    assert!(
        matches!(root, Some(SearchRoot::Completed(_))),
        "search root should complete"
    );
    let seen = deps.seen.lock().expect("read request contexts");
    assert_eq!(seen.len(), 1, "execute_with_context called once");
    seen[0]
}

#[test]
fn m940a_graphql_actor_diff() {
    // Anonymous actor: zero-value `actor.FromContext(context.Background())`.
    let anonymous = run(Actor::Anonymous, false);
    assert_eq!(anonymous, RequestContext::anonymous());

    // Authenticated user actor: UID carried through, dotcom from the plan.
    let user = run(Actor::User { uid: 938001 }, true);
    assert_eq!(
        user,
        RequestContext {
            actor_uid: 938001,
            internal: false,
            sourcegraph_dot_com_mode: true,
        }
    );

    // Internal service actor: internal bypass, no UID.
    let internal = run(Actor::Internal, false);
    assert_eq!(
        internal,
        RequestContext {
            actor_uid: 0,
            internal: true,
            sourcegraph_dot_com_mode: false,
        }
    );

    // `SearchExecutor::execute` (no actor) keeps the anonymous path.
    let deps = Arc::new(RecordingDeps::default());
    let executor = BackendSearchExecutor::new(deps.clone());
    let planned = sg_service_frontend::search_graphql::PlannedSearch {
        inputs: SearchInputs {
            max_results: 1,
            original_query: String::new(),
            plan: Vec::new(),
            search_mode: SearchMode::Precise,
            pattern_type: SearchType::Literal,
            features: Features::default(),
            protocol: Protocol::Batch,
            context_lines: 0,
            sanitize_search_patterns: Vec::new(),
            on_sourcegraph_dot_com: false,
            cody_context: None,
        },
        query: String::new(),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("graphql actor diff runtime");
    runtime
        .block_on(executor.execute(
            &planned,
            sg_service_frontend::search_graphql::SearchStreamMode::Batch,
        ))
        .expect("execute");
    assert_eq!(
        deps.seen.lock().expect("read request contexts")[0],
        RequestContext::anonymous()
    );
}
