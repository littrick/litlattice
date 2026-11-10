#![forbid(unsafe_code)]

//! M9.8hd differential harness (Rust side): the ranking GraphQL root resolver.
//!
//! `test_ws/p9-go/run_ranking_graphql_diff.sh` runs the Go oracle
//! (`internal/codeintel/ranking/transport/graphql/zz_m9hd_ranking_oracle_test.go`)
//! and this test against the same corpus, then diffs the canonical `rgql `
//! JSON lines byte-for-byte. Object keys are recursively sorted on both sides;
//! the corpus is ASCII-only so Go's HTML escaping never diverges.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sg_service_frontend::ranking_graphql::{
    empty_response, graph_key, DateTime, DeleteRankingProgressArgs, NoopObserver, Progress,
    RankingError, RankingService, RootResolver, SiteAdminChecker, Summary,
};

struct FakeRankingService {
    derivative_graph_key: Option<String>,
    derivative_graph_key_error: Option<String>,
    summaries: Vec<Summary>,
    summaries_error: Option<String>,
    next_job_starts_at: Option<DateTime>,
    next_job_starts_at_error: Option<String>,
    coverage_counts: sg_service_frontend::ranking_graphql::CoverageCounts,
    coverage_counts_error: Option<String>,
    expected_graph_key: String,
    bump_error: Option<String>,
    delete_error: Option<String>,
    expected_delete_graph_key: String,
}

#[async_trait]
impl RankingService for FakeRankingService {
    async fn derivative_graph_key(&self) -> Result<(String, bool), RankingError> {
        if let Some(message) = &self.derivative_graph_key_error {
            return Err(RankingError::Service(message.clone()));
        }
        Ok(match &self.derivative_graph_key {
            Some(key) => (key.clone(), true),
            None => (String::new(), false),
        })
    }

    async fn bump_derivative_graph_key(&self) -> Result<(), RankingError> {
        match &self.bump_error {
            Some(message) => Err(RankingError::Service(message.clone())),
            None => Ok(()),
        }
    }

    async fn summaries(&self) -> Result<Vec<Summary>, RankingError> {
        match &self.summaries_error {
            Some(message) => Err(RankingError::Service(message.clone())),
            None => Ok(self.summaries.clone()),
        }
    }

    async fn next_job_starts_at(&self) -> Result<(DateTime, bool), RankingError> {
        if let Some(message) = &self.next_job_starts_at_error {
            return Err(RankingError::Service(message.clone()));
        }
        Ok(match self.next_job_starts_at {
            Some(at) => (at, true),
            None => (DateTime::from_unix_seconds(0), false),
        })
    }

    async fn coverage_counts(
        &self,
        graph_key: &str,
    ) -> Result<sg_service_frontend::ranking_graphql::CoverageCounts, RankingError> {
        if let Some(message) = &self.coverage_counts_error {
            return Err(RankingError::Service(message.clone()));
        }
        // The baseline passes `rankingshared.GraphKey()` to the service; a
        // mismatch is surfaced as an error so the diff catches it.
        if graph_key != self.expected_graph_key {
            return Err(RankingError::Service(format!(
                "unexpected graph key: {graph_key}"
            )));
        }
        Ok(self.coverage_counts)
    }

    async fn delete_ranking_progress(&self, graph_key: &str) -> Result<(), RankingError> {
        if let Some(message) = &self.delete_error {
            return Err(RankingError::Service(message.clone()));
        }
        if graph_key != self.expected_delete_graph_key {
            return Err(RankingError::Service(format!(
                "unexpected graph key: {graph_key}"
            )));
        }
        Ok(())
    }
}

struct FakeSiteAdminChecker {
    is_site_admin: bool,
}

#[async_trait]
impl SiteAdminChecker for FakeSiteAdminChecker {
    async fn check_current_user_is_site_admin(&self) -> Result<(), RankingError> {
        if self.is_site_admin {
            Ok(())
        } else {
            Err(RankingError::MustBeSiteAdmin)
        }
    }
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut keys: Vec<String> = object.keys().cloned().collect();
            keys.sort();
            let mut sorted = Map::new();
            for key in keys {
                let entry = object.get(&key).cloned().expect("key from object");
                sorted.insert(key, canonicalize(entry));
            }
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize).collect()),
        other => other,
    }
}

fn text(object: &Value, key: &str) -> Option<String> {
    object.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn required_text(object: &Value, key: &str) -> String {
    text(object, key).unwrap_or_else(|| panic!("missing string field {key}"))
}

fn parse_date_time(value: &Value, key: &str) -> Option<DateTime> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(|raw| DateTime::parse_rfc3339(raw).unwrap_or_else(|| panic!("bad {key}: {raw}")))
}

fn parse_progress(value: &Value) -> Progress {
    Progress {
        started_at: parse_date_time(value, "started_at").expect("started_at"),
        completed_at: parse_date_time(value, "completed_at"),
        processed: value.get("processed").and_then(Value::as_i64).unwrap_or(0),
        total: value.get("total").and_then(Value::as_i64).unwrap_or(0),
    }
}

fn parse_summary(value: &Value) -> Summary {
    Summary {
        graph_key: required_text(value, "graph_key"),
        visible_to_zoekt: value
            .get("visible_to_zoekt")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        path_mapper_progress: parse_progress(
            value
                .get("path_mapper_progress")
                .expect("path_mapper_progress"),
        ),
        reference_mapper_progress: parse_progress(
            value
                .get("reference_mapper_progress")
                .expect("reference_mapper_progress"),
        ),
        reducer_progress: value
            .get("reducer_progress")
            .filter(|value| !value.is_null())
            .map(parse_progress),
    }
}

fn error_for(object: &Value, key: &str) -> Option<String> {
    object
        .get("errors")
        .and_then(|errors| errors.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn coverage_counts(value: Option<&Value>) -> sg_service_frontend::ranking_graphql::CoverageCounts {
    let Some(value) = value else {
        return Default::default();
    };
    sg_service_frontend::ranking_graphql::CoverageCounts {
        num_target_indexes: value
            .get("num_target_indexes")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        num_exported_indexes: value
            .get("num_exported_indexes")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        num_repositories_without_current_ranks: value
            .get("num_repositories_without_current_ranks")
            .and_then(Value::as_i64)
            .unwrap_or(0),
    }
}

#[tokio::test]
async fn ranking_graphql_diff() {
    if std::env::var("SG_M9HD_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HD_CORPUS").expect("M9HD_CORPUS must be set");
    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let corpus: Value = serde_json::from_str(&raw).expect("corpus JSON");
    let cases = corpus
        .get("cases")
        .and_then(Value::as_array)
        .expect("corpus cases");

    for case in cases {
        let name = required_text(case, "name");
        let operation = required_text(case, "operation");
        let is_site_admin = case
            .get("is_site_admin")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let root = text(case, "graph_key_root").unwrap_or_default();
        let effective_graph_key = graph_key(&root);

        let summaries: Vec<Summary> = case
            .get("summaries")
            .and_then(Value::as_array)
            .map(|summaries| summaries.iter().map(parse_summary).collect())
            .unwrap_or_default();

        let service = FakeRankingService {
            derivative_graph_key: text(case, "derivative_graph_key"),
            derivative_graph_key_error: error_for(case, "derivativeGraphKey"),
            summaries,
            summaries_error: error_for(case, "summaries"),
            next_job_starts_at: parse_date_time(case, "next_job_starts_at"),
            next_job_starts_at_error: error_for(case, "nextJobStartsAt"),
            coverage_counts: coverage_counts(case.get("coverage_counts")),
            coverage_counts_error: error_for(case, "coverageCounts"),
            expected_graph_key: effective_graph_key,
            bump_error: error_for(case, "bumpDerivativeGraphKey"),
            delete_error: error_for(case, "deleteRankingProgress"),
            expected_delete_graph_key: text(case, "delete_graph_key").unwrap_or_default(),
        };

        let resolver = RootResolver::new(
            Arc::new(service),
            Arc::new(FakeSiteAdminChecker { is_site_admin }),
            Arc::new(NoopObserver),
            root,
        );

        let result = match operation.as_str() {
            "ranking_summary" => resolver
                .ranking_summary()
                .await
                .map(|summary| summary.to_json()),
            "bump_derivative_graph_key" => resolver
                .bump_derivative_graph_key()
                .await
                .map(|_| empty_response().to_json()),
            "delete_ranking_progress" => resolver
                .delete_ranking_progress(&DeleteRankingProgressArgs {
                    graph_key: text(case, "delete_graph_key").unwrap_or_default(),
                })
                .await
                .map(|_| empty_response().to_json()),
            other => panic!("unknown operation {other}"),
        };

        let output = match result {
            Ok(value) => json!({
                "case": name,
                "operation": operation,
                "result": value,
            }),
            Err(error) => json!({
                "case": name,
                "operation": operation,
                "error": error.to_string(),
            }),
        };

        let encoded = serde_json::to_string(&canonicalize(output)).expect("serialize");
        println!("rgql {encoded}");
    }
}
