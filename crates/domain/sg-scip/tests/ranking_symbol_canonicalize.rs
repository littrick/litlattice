#![forbid(unsafe_code)]

//! M9.8ds differential harness (Rust side): `SymbolFormatter::no_version` and
//! `canonicalize_symbol` over the shared corpus in
//! `internal/codeintel/ranking/internal/background/exporter/symbol_harness_test.go`.
//!
//! `test_ws/p9-go/run_codeintel_ranking_symbol_canonicalize_diff.sh` diffs the
//! canonical `rsc ` JSON lines; no database is needed.

use serde_json::{json, Value};
use sg_scip::{canonicalize_symbol, SymbolFormatter};

fn print(value: Value) {
    println!("rsc {}", serde_json::to_string(&value).expect("serialize"));
}

fn corpus() -> Vec<&'static str> {
    vec![
        "",
        "local 1",
        "local ",
        "lsif . something",
        "scip-go gomod github.com/sourcegraph/sourcegraph v1.2.3 internal/foo/Bar#",
        "scip-go gomod github.com/sourcegraph/sourcegraph v9.9.9 internal/foo/Bar#",
        "scip-go . . . Foo#",
        "s m a  b v Foo#",
        "s m n v `c d`/",
        "s m n v `c``d`/",
        "s m n v a/b#c.d:e![T](p)h(dis).",
        "s m n v g().",
        "scip-go gomod github.com/sourcegraph/sourcegraph v1.2.3 internal/foo/Bar#baz().",
        "not a symbol",
        "s m n v bad%",
        "a b",
        "[T]",
        "s m n v Foo#Bar.",
    ]
}

fn hex(bytes: &[u8; 16]) -> String {
    let mut value = String::with_capacity(32);
    for byte in bytes {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

#[test]
fn ranking_symbol_canonicalize_harness() {
    if std::env::var("SG_CODEINTEL_RANKING_SYMBOL_DIFF").as_deref() != Ok("1") {
        return;
    }

    let formatter = SymbolFormatter::no_version();
    for symbol in corpus() {
        let formatted = match formatter.format(symbol) {
            Ok(value) => json!(value),
            Err(_) => Value::Null,
        };
        print(json!({
            "kind": "format",
            "symbol": symbol,
            "formatted": formatted,
        }));

        let checksum = match canonicalize_symbol(symbol) {
            Some(value) => json!(hex(&value)),
            None => Value::Null,
        };
        print(json!({
            "kind": "checksum",
            "symbol": symbol,
            "checksum": checksum,
        }));
    }
}
