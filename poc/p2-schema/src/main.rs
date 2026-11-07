//! P2: compare Go GraphQL SDL with the runtime introspection captured from the
//! Go baseline (fixture: `testdata/graphql/introspection-go.json`).
//!
//! Usage:
//!   p2-schema <sdl-dir> <introspection.json> [expected-missing,...]
//!
//! Exit code 0 only when the only SDL-but-not-runtime root fields are listed in
//! `expected-missing` (env-gated extensions such as `dotcom`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use graphql_parser::schema::{Definition, Document, TypeDefinition, TypeExtension};

fn collect_root_fields(doc: &Document<'_, String>, roots: &mut BTreeMap<String, BTreeSet<String>>) {
    for def in &doc.definitions {
        match def {
            Definition::TypeDefinition(TypeDefinition::Object(obj)) => {
                if let Some(entry) = roots.get_mut(&obj.name) {
                    entry.extend(obj.fields.iter().map(|f| f.name.clone()));
                }
            }
            Definition::TypeExtension(TypeExtension::Object(obj)) => {
                if let Some(entry) = roots.get_mut(&obj.name) {
                    entry.extend(obj.fields.iter().map(|f| f.name.clone()));
                }
            }
            _ => {}
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: p2-schema <sdl-dir> <introspection.json> [expected-missing,...]");
        std::process::exit(2);
    }
    let sdl_dir = Path::new(&args[1]);
    let introspection_path = Path::new(&args[2]);
    let expected_missing: BTreeSet<String> = args
        .get(3)
        .map(|s| s.split(',').filter(|x| !x.is_empty()).map(str::to_owned).collect())
        .unwrap_or_default();

    let mut roots: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    roots.insert("Query".into(), BTreeSet::new());
    roots.insert("Mutation".into(), BTreeSet::new());

    let mut sdl_files: Vec<_> = std::fs::read_dir(sdl_dir)
        .expect("read sdl dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "graphql"))
        .collect();
    sdl_files.sort();
    assert!(!sdl_files.is_empty(), "no .graphql files in {}", sdl_dir.display());

    for path in &sdl_files {
        let text = std::fs::read_to_string(path).expect("read sdl file");
        let doc: Document<'_, String> = graphql_parser::schema::parse_schema(&text)
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        collect_root_fields(&doc, &mut roots);
    }

    let raw = std::fs::read_to_string(introspection_path).expect("read introspection");
    let json: serde_json::Value = serde_json::from_str(&raw).expect("parse introspection json");
    let types = json["data"]["__schema"]["types"]
        .as_array()
        .expect("introspection.data.__schema.types");

    let mut runtime: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for name in ["Query", "Mutation"] {
        let t = types
            .iter()
            .find(|t| t["name"] == name)
            .unwrap_or_else(|| panic!("type {name} missing from introspection"));
        runtime.insert(
            name.into(),
            t["fields"]
                .as_array()
                .expect("fields")
                .iter()
                .map(|f| f["name"].as_str().expect("field name").to_owned())
                .collect(),
        );
    }

    let mut ok = true;
    for name in ["Query", "Mutation"] {
        let sdl = &roots[name];
        let got = &runtime[name];
        let sdl_only: Vec<_> = sdl.difference(got).cloned().collect();
        let runtime_only: Vec<_> = got.difference(sdl).cloned().collect();
        let unexpected: Vec<_> = sdl_only
            .iter()
            .filter(|f| !expected_missing.contains(*f))
            .cloned()
            .collect();

        println!("{name}: sdl={} runtime={}", sdl.len(), got.len());
        println!("  sdl-only (expected gated): {sdl_only:?}");
        println!("  sdl-only (unexpected):     {unexpected:?}");
        println!("  runtime-only:              {runtime_only:?}");
        if !unexpected.is_empty() || !runtime_only.is_empty() {
            ok = false;
        }
    }

    if !ok {
        eprintln!("FAIL: SDL and runtime schema disagree beyond expected gating");
        std::process::exit(1);
    }
    println!("OK: root fields match modulo expected gating {expected_missing:?}");
}
