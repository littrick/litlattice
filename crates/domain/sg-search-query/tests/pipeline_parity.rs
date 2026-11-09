//! M9.8bs differential: `Pipeline(Init(...))` over the pipeline corpus.
//! Mirrors `internal/search/query/pipeline_harness_test.go`.
//! Gated by `SG_QUERY_CORPUS`; prints
//! `p <type>\t<input>\t<plan-len>\t<plan.ToQ()>\t<StringHuman(plan.ToQ())>`.

use sg_search_query::node::to_string;
use sg_search_query::{pipeline, plan_to_q};
use sg_search_query::{string_human, SearchType};

fn search_type(name: &str) -> SearchType {
    match name {
        "regex" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        "codycontext" => SearchType::CodyContext,
        "keyword" => SearchType::Keyword,
        other => panic!("unknown search type {other}"),
    }
}

#[test]
fn pipeline_parity_harness() {
    let Ok(path) = std::env::var("SG_QUERY_CORPUS") else {
        return;
    };
    let corpus = std::fs::read_to_string(path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let (name, input) = line.split_once('\t').expect("corpus line has a tab");
        let search_type = search_type(name);
        let step = pipeline::init(input.to_owned(), search_type);
        match pipeline::pipeline(vec![step]) {
            Ok(plan) => {
                let q = plan_to_q(&plan);
                println!(
                    "p {name}\t{input}\t{}\t{}\t{}",
                    plan.len(),
                    to_string(&q),
                    string_human(&q)
                );
            }
            Err(err) => println!("p {name}\t{input}\terror: {err}"),
        }
    }
}
