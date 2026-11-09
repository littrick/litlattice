//! M9.8bq differential harness: `sg_search_query::parse` against
//! `internal/search/query.Parse`.
//!
//! Mirrors `internal/search/query/parity_harness_test.go`. Only runs when
//! `SG_QUERY_CORPUS` names a corpus file of `<search-type>\t<query>` lines.
//! Prints canonical `q ` lines.

use sg_search_query::{parse, query_to_string, SearchType};

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
fn parse_parity_harness() {
    let Ok(path) = std::env::var("SG_QUERY_CORPUS") else {
        return;
    };
    let corpus = std::fs::read_to_string(&path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let (name, input) = line.split_once('\t').expect("corpus line has a tab");
        match parse(input, search_type(name)) {
            Ok(nodes) => println!("q {name}\t{input}\t{}", query_to_string(&nodes)),
            Err(err) => println!("q {name}\t{input}\terror: {err}"),
        }
    }
}
