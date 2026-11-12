//! Differential harness (M9.17c) for the migrator CLI surface.
//!
//! Compares `sg_bin_migrator::{COMMANDS, Selection}` against the `urfave/cli`
//! app built by `cmd/migrator/shared`. The Go oracle
//! (`cmd/migrator/shared/zz_m917c_migrator_oracle_test.go`) writes the same
//! document to `M917C_MIGRATOR_OUT`; `run_migrator_bin_diff.sh` compares them.

use serde::Deserialize;
use serde_json::{json, Value};
use sg_bin_migrator::{Selection, APP_NAME, APP_USAGE, COMMANDS};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    selections: Vec<SelectionCase>,
}

#[derive(Deserialize)]
struct SelectionCase {
    #[serde(default)]
    argv: Vec<String>,
}

#[test]
fn migrator_bin_diff() {
    if std::env::var("SG_M917C_MIGRATOR_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M917C_MIGRATOR_CORPUS").expect("M917C_MIGRATOR_CORPUS");
    let out_path = std::env::var("M917C_MIGRATOR_OUT").expect("M917C_MIGRATOR_OUT");
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let commands: Vec<Value> = COMMANDS
        .iter()
        .map(|command| {
            json!({
                "name": command.name,
                "usage": command.usage,
                "flags": command.flags,
            })
        })
        .collect();

    let selections: Vec<Value> = corpus
        .selections
        .iter()
        .map(|case| {
            let argv = Selection::effective_argv(&case.argv);
            match Selection::select(&case.argv) {
                Selection::Command { name, args } => json!({
                    "argv": argv,
                    "selected": name,
                    "args": args,
                }),
                Selection::Help => json!({
                    "argv": argv,
                    "selected": Value::Null,
                    "args": Vec::<String>::new(),
                }),
            }
        })
        .collect();

    let document = json!({
        "app": {
            "name": APP_NAME,
            "usage": APP_USAGE,
        },
        "commands": commands,
        "selections": selections,
    });
    let mut output = serde_json::to_string(&document).expect("encode");
    output.push('\n');
    std::fs::write(&out_path, output).expect("write output");
}
