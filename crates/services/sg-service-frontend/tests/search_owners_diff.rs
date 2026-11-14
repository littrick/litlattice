//! M9.38b differential harness: CODEOWNERS parsing/ruleset matching,
//! assigned owners/teams inheritance, `ownersFilters`, and
//! `Reference.ResolutionGuess` against the Go oracle
//! (`internal/own/search/zz_m938b_owners_oracle_test.go`).
//!
//! Env: `SG_M938B_OWNERS_DIFF=1`, `M938B_OWNERS_CORPUS`,
//! `M938B_OWNERS_OUT`.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_leaf_jobs::{
    ownership_terms_match, resolution_guess, OwnershipRef,
};
use sg_service_frontend::search_owners::{
    match_assigned, parse_codeowners, repr, FileOwnershipData, Ruleset,
};
use sg_service_frontend::search_stream::convert::ResolvedOwner;

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    codeowners: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    paths: Vec<String>,
    #[serde(default)]
    assigned_owners: HashMap<String, Vec<i32>>,
    #[serde(default)]
    assigned_teams: HashMap<String, Vec<i32>>,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    owners: Vec<RefSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct RefSpec {
    #[serde(default)]
    handle: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    code_host_kind: Option<String>,
    #[serde(default)]
    repo_name: Option<String>,
}

impl RefSpec {
    fn to_ref(&self) -> OwnershipRef {
        OwnershipRef {
            handle: self.handle.clone(),
            email: self.email.clone(),
            code_host_kind: self.code_host_kind.clone(),
            repo_name: self.repo_name.clone(),
            ..OwnershipRef::default()
        }
    }
}

fn owner_value(owner: &OwnershipRef) -> Value {
    json!({"handle": owner.handle, "email": owner.email})
}

fn parsed_ruleset(case: &Case) -> Ruleset {
    Ruleset::new(parse_codeowners(&case.codeowners).expect("parse codeowners").rules)
}

fn run_parse(case: &Case) -> Value {
    let parsed = parse_codeowners(&case.codeowners).expect("parse codeowners");
    let ruleset = Ruleset::new(parsed.rules.clone());
    let rules: Vec<Value> = parsed
        .rules
        .iter()
        .map(|rule| {
            json!({
                "pattern": rule.pattern,
                "section": rule.section_name,
                "line": rule.line_number,
                "owners": rule.owners.iter().map(owner_value).collect::<Vec<_>>(),
            })
        })
        .collect();
    let matches: Vec<Value> = case
        .paths
        .iter()
        .map(|path| match ruleset.match_path(path) {
            Some(rule) => json!({
                "path": path,
                "owners": rule.owners.iter().map(owner_value).collect::<Vec<_>>(),
            }),
            None => json!({"path": path, "owners": null}),
        })
        .collect();
    json!({
        "name": case.name,
        "kind": case.kind,
        "repr": repr(&parsed),
        "rules": rules,
        "matches": matches,
        "error": false,
    })
}

fn run_assigned(case: &Case) -> Value {
    let matches: Vec<Value> = case
        .paths
        .iter()
        .map(|path| {
            json!({
                "path": path,
                "owners": match_assigned(&case.assigned_owners, path),
                "teams": match_assigned(&case.assigned_teams, path),
            })
        })
        .collect();
    json!({"name": case.name, "kind": case.kind, "matches": matches})
}

fn run_filter(case: &Case) -> Value {
    let ruleset = parsed_ruleset(case);
    let data = FileOwnershipData {
        rule: ruleset.match_path(&case.path).cloned(),
        assigned_owners: match_assigned(&case.assigned_owners, &case.path),
        assigned_teams: match_assigned(&case.assigned_teams, &case.path),
    };
    let include_bags: Vec<_> = case
        .include
        .iter()
        .map(|term| sg_service_frontend::search_owners::Bag::by_text_reference(term))
        .collect();
    let exclude_bags: Vec<_> = case
        .exclude
        .iter()
        .map(|term| sg_service_frontend::search_owners::Bag::by_text_reference(term))
        .collect();
    let references = data.references();
    let include_match = !case.include.is_empty()
        && ownership_terms_match(&references, &case.include, &include_bags, false);
    let exclude_match = !case.exclude.is_empty()
        && ownership_terms_match(&references, &case.exclude, &exclude_bags, true);
    json!({
        "name": case.name,
        "kind": case.kind,
        "include_match": include_match,
        "exclude_match": exclude_match,
        "non_empty": data.non_empty(),
    })
}

fn run_guess(case: &Case) -> Value {
    let guesses: Vec<Value> = case
        .owners
        .iter()
        .map(|spec| {
            let reference = spec.to_ref();
            match resolution_guess(&reference) {
                Some(ResolvedOwner::Person { handle, email, .. }) => {
                    json!({"type": "person", "handle": handle, "email": email})
                }
                Some(ResolvedOwner::Team { handle, email, .. }) => {
                    json!({"type": "team", "handle": handle, "email": email})
                }
                None => Value::Null,
            }
        })
        .collect();
    json!({"name": case.name, "kind": case.kind, "guesses": guesses})
}

#[test]
fn m938b_owners_diff() {
    if std::env::var("SG_M938B_OWNERS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("M938B_OWNERS_CORPUS").expect("M938B_OWNERS_CORPUS");
    let out_path = std::env::var("M938B_OWNERS_OUT").expect("M938B_OWNERS_OUT");
    let raw = std::fs::read_to_string(corpus).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let out: Vec<Value> = cases
        .iter()
        .map(|case| match case.kind.as_str() {
            "parse" => run_parse(case),
            "assigned" => run_assigned(case),
            "filter" => run_filter(case),
            "guess" => run_guess(case),
            other => panic!("unknown kind {other}"),
        })
        .collect();
    std::fs::write(out_path, serde_json::to_vec(&out).unwrap()).expect("write output");
}
