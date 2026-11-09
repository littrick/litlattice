//! M9.8br differential: `Parse` + `StringHuman` over the shared corpus.
//! Mirrors `internal/search/query/printer_harness_test.go`.
//! Gated by `SG_QUERY_CORPUS`; prints `h <type>\t<input>\t<human>` lines.

use sg_search_query::{parse, string_human, SearchType};

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
fn printer_parity_harness() {
    let Ok(path) = std::env::var("SG_QUERY_CORPUS") else {
        return;
    };
    let corpus = std::fs::read_to_string(path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let (name, input) = line.split_once('\t').expect("corpus line has a tab");
        match parse(input, search_type(name)) {
            Ok(nodes) => println!("h {name}\t{input}\t{}", string_human(&nodes)),
            Err(err) => println!("h {name}\t{input}\terror: {err}"),
        }
    }
}
