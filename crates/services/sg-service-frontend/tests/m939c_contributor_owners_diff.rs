//! M9.39c differential harness: `gitserver.Client.ContributorCount` request
//! and response mapping, external-account handle extraction, and the
//! `Bag`-linkBack containment used by `own.ByTextReference`.
//!
//! Go oracle: `internal/own/search/zz_m939c_contributor_oracle_test.go`
//! (untracked, gated; fake gitserver client).
//!
//! Env: `SG_M939C_DIFF=1`, `M939C_CORPUS`, `M939C_OUT`.

use serde::Deserialize;
use serde_json::{json, Value};

use sg_git::{
    contributor_count_from_proto_parts, contributor_counts_request,
    ContributorOptions as GitContributorOptions,
};
use sg_service_frontend::search_leaf_jobs::OwnershipRef;
use sg_service_frontend::search_owners::{external_account_login, Bag};

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    range: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    counts: Vec<CountSpec>,
    #[serde(default)]
    service_type: String,
    #[serde(default)]
    account_data: String,
    #[serde(default)]
    refs: Vec<RefSpec>,
    #[serde(default)]
    queries: Vec<RefSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct CountSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    count: i32,
}

#[derive(Debug, Default, Deserialize)]
struct RefSpec {
    #[serde(default)]
    handle: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    user_id: Option<i32>,
    #[serde(default)]
    team_id: Option<i32>,
}

impl RefSpec {
    fn to_ref(&self) -> OwnershipRef {
        OwnershipRef {
            handle: self.handle.clone(),
            email: self.email.clone(),
            user_id: self.user_id,
            team_id: self.team_id,
            ..OwnershipRef::default()
        }
    }
}

fn run_contributor(case: &Case) -> Value {
    // `filter_file_contributor.go:137`: zero `After` (Go `time.Time{}`).
    let options = GitContributorOptions {
        range: case.range.clone(),
        after: None,
        path: case.path.clone(),
    };
    let request = contributor_counts_request(&case.repo, &options);
    let counts: Vec<Value> = case
        .counts
        .iter()
        .map(|count| {
            // The wire carries UTF-8 bytes; Go converts with `string(bytes)`.
            let decoded = contributor_count_from_proto_parts(
                count.name.as_bytes(),
                count.email.as_bytes(),
                count.count,
            );
            json!({
                "name": decoded.name,
                "email": decoded.email,
                "count": decoded.count,
            })
        })
        .collect();
    json!({
        "name": case.name,
        "kind": case.kind,
        "request": {
            "repo_name": request.repo_name,
            "range": String::from_utf8_lossy(&request.range),
            "after_seconds": request.after.map(|time| time.seconds),
            "path": String::from_utf8_lossy(&request.path),
        },
        "counts": counts,
    })
}

fn run_external_handle(case: &Case) -> Value {
    json!({
        "name": case.name,
        "kind": case.kind,
        "login": external_account_login(&case.service_type, &case.account_data),
    })
}

fn run_bag_contains(case: &Case) -> Value {
    let mut bag = Bag::empty();
    for spec in &case.refs {
        bag.add_reference(&spec.to_ref());
    }
    let contains: Vec<bool> = case
        .queries
        .iter()
        .map(|query| bag.contains(&query.to_ref()))
        .collect();
    json!({
        "name": case.name,
        "kind": case.kind,
        "contains": contains,
    })
}

#[test]
fn m939c_contributor_owners_diff() {
    if std::env::var("SG_M939C_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M939C_CORPUS").expect("M939C_CORPUS");
    let out_path = std::env::var("M939C_OUT").expect("M939C_OUT");
    let raw = std::fs::read_to_string(corpus).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let out: Vec<Value> = cases
        .iter()
        .map(|case| match case.kind.as_str() {
            "contributor" => run_contributor(case),
            "external_handle" => run_external_handle(case),
            "bag_contains" => run_bag_contains(case),
            other => panic!("unknown kind {other}"),
        })
        .collect();
    std::fs::write(out_path, serde_json::to_vec(&out).unwrap()).expect("write output");
}
