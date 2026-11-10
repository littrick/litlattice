//! M9.8ex differential harness for `EstimateQueryCost`.
//!
//! Gated by `SG_GRAPHQL_COST_HARNESS=1`; reads the corpus named by
//! `GRAPHQL_COST_CORPUS` and prints one canonical `gc` line per case, matching
//! the Go harness in `cmd/frontend/graphqlbackend`.

use std::collections::BTreeMap;

use sg_graphql::estimate_query_cost;

#[test]
fn graphql_cost_harness() {
    if std::env::var("SG_GRAPHQL_COST_HARNESS").as_deref() != Ok("1") {
        eprintln!("set SG_GRAPHQL_COST_HARNESS=1 to run");
        return;
    }

    let path = std::env::var("GRAPHQL_COST_CORPUS").expect("GRAPHQL_COST_CORPUS is not set");
    let raw = std::fs::read_to_string(&path).expect("read corpus");
    let cases: Vec<serde_json::Value> = serde_json::from_str(&raw).expect("parse corpus");

    for case in cases {
        let name = case["name"].as_str().expect("case name");
        let query = case["query"].as_str().expect("case query");
        let variables: BTreeMap<String, serde_json::Value> = match &case["variables"] {
            serde_json::Value::Object(map) => {
                map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            }
            _ => BTreeMap::new(),
        };

        match estimate_query_cost(query, &variables) {
            Ok(cost) => println!("gc {name} err=false cost={}", cost.encode()),
            Err(_) => println!("gc {name} err=true cost=-"),
        }
    }
}
