#![forbid(unsafe_code)]

//! M9.8db differential harness (Rust side): `lib/codeintel/autoindex/config`
//! types and parsers (`types.go`, `json.go`, `yaml.go`).
//!
//! Mirrors `lib/codeintel/autoindex/config/diff_harness_test.go`; the shared
//! script `test_ws/p9-go/run_codeintel_autoindex_config_diff.sh` diffs the
//! canonical `cats ` lines. JSON/YAML error text is not compared, only the
//! wrapper prefix (`invalid JSON` / `invalid YAML`) and the parsed value.

use sg_service_worker::jobs::autoindex_config::{
    extract_indexer_name, marshal_json, unmarshal_json, unmarshal_yaml, AutoIndexJobSpec,
    AutoIndexJobSpecList, DockerStep,
};

#[derive(serde::Serialize)]
struct Line<'a> {
    src: &'a str,
    case: &'a str,
    ok: bool,
    error_kind: &'a str,
    value: Option<AutoIndexJobSpecList>,
    output: &'a str,
    got: &'a str,
}

const JSON_EXAMPLE: &str = r#"
{
	"index_jobs": [
		{
			"steps": [
				{
					// Comments are the future
					"image": "go:latest",
					"commands": ["go mod vendor"],
				}
			],
			"indexer": "lsif-go",
			"indexer_args": ["--no-animation"],
		},
		{
			"root": "web/",
			"indexer": "scip-typescript",
			"indexer_args": ["index", "--yarn-workspaces"],
			"outfile": "lsif.dump",
		},
	]
}
"#;

const YAML_EXAMPLE: &str = r#"
index_jobs:
  -
    steps:
      - image: go:latest
        commands:
          - go mod vendor
    indexer: lsif-go
    indexer_args:
      - --no-animation
  -
    root: web/
    indexer: scip-typescript
    indexer_args: ['index', '--yarn-workspaces']
    outfile: lsif.dump
"#;

fn json_cases() -> Vec<(&'static str, &'static str)> {
    vec![
        ("doc-example", JSON_EXAMPLE),
        ("empty", ""),
        ("whitespace", "  \n\t "),
        ("null", "null"),
        ("index-jobs-null", r#"{"index_jobs": null}"#),
        ("index-jobs-empty", r#"{"index_jobs": []}"#),
        ("uppercase-fields", r#"{"INDEX_JOBS": [{"INDEXER": "a"}]}"#),
        ("mixedcase-fields", r#"{"Index_Jobs": [{"Indexer": "a"}]}"#),
        (
            "duplicate-key",
            r#"{"index_jobs": [{"indexer": "a", "indexer": "b"}]}"#,
        ),
        (
            "null-element",
            r#"{"index_jobs": [null, {"indexer": "a"}]}"#,
        ),
        ("unknown-overflow-number", r#"{"unknown": 1e999}"#),
        ("wrong-type-object", r#"{"index_jobs": {}}"#),
        (
            "step-commands-null",
            r#"{"index_jobs": [{"steps": [{"commands": null}]}]}"#,
        ),
        (
            "requested-envvars",
            r#"{"index_jobs": [{"requestedEnvVars": ["A"]}]}"#,
        ),
        (
            "local-steps-number",
            r#"{"index_jobs": [{"local_steps": [1]}]}"#,
        ),
        ("trailing-comma-empty", r#"{"index_jobs": [] , }"#),
        (
            "comments-anywhere",
            "{/*c*/\"index_jobs\"://x\n[/*y*/]//z\n}",
        ),
        ("unknown-image-field", r#"{"index_jobs": [{"image": "x"}]}"#),
        (
            "root-number",
            r#"{"index_jobs": [{"steps": [{"root": 1, "commands":["a"]}]}]}"#,
        ),
        ("steps-string", r#"{"index_jobs": [{"steps": "x"}]}"#),
        ("top-array", "[1,2]"),
        (
            "trailing-content",
            r#"{"index_jobs": [{"indexer": "a"}]} trailing"#,
        ),
        ("indexer-null", r#"{"index_jobs": [{"indexer": null}]}"#),
        ("comment-only", "// only a comment\n"),
    ]
}

fn yaml_cases() -> Vec<(&'static str, &'static str)> {
    vec![
        ("doc-example", YAML_EXAMPLE),
        ("empty", ""),
        ("null-doc", "null\n"),
        ("doc-marker", "---\nindex_jobs: []\n"),
        ("index-jobs-null", "index_jobs:\n"),
        ("index-jobs-empty", "index_jobs: []\n"),
        (
            "scalar-coercion",
            "index_jobs:\n  - root: 123\n    indexer: true\n",
        ),
        (
            "int-sequence",
            "index_jobs:\n  - root: 1.5\n    local_steps: [1, 2]\n",
        ),
        (
            "duplicate-keys",
            "index_jobs:\n  - indexer: a\n    indexer: b\n",
        ),
        (
            "steps-null",
            "index_jobs:\n  - steps:\n      - image: go\n        commands: ~\n",
        ),
        (
            "flow-steps",
            "index_jobs:\n  - steps:\n      - {image: go, commands: [a]}\n",
        ),
        ("flow-all", "index_jobs: [ {indexer: a}, null ]\n"),
        (
            "quoted-scalars",
            "index_jobs:\n  - root: 'a: b'\n    outfile: \"x\\ty\"\n",
        ),
        (
            "requested-envvars",
            "index_jobs:\n  - requestedEnvVars: [A, B]\n    steps: []\n",
        ),
        ("steps-empty", "index_jobs:\n  - steps: []\n"),
        ("error-index-jobs-string", "index_jobs: \"x\"\n"),
        (
            "error-scalar-sequence",
            "index_jobs:\n  - local_steps: go\n",
        ),
        ("error-indexer-sequence", "index_jobs:\n  - indexer: [a]\n"),
        (
            "error-bad-indent",
            "index_jobs:\n  - indexer: a\n      bad: indent\n",
        ),
        ("unknown-key", "index_jobs:\n  - <tagged>: x\n"),
        (
            "comments",
            "# leading\nindex_jobs: # trailing\n  - indexer: a  # item\n",
        ),
        ("comment-only", "# nothing else\n"),
        ("flow-trailing-comma", "index_jobs: [ {indexer: a}, ]\n"),
    ]
}

fn emit(line: &Line<'_>) {
    println!("cats {}", serde_json::to_string(line).expect("json"));
}

fn error_kind<'a>(error: &str, prefix: &'a str) -> &'a str {
    if error.starts_with(prefix) {
        prefix.trim_end_matches(": ")
    } else {
        "other"
    }
}

#[test]
fn autoindex_config_matches_go_harness() {
    if std::env::var("SG_AUTOINDEX_CONFIG_DIFF").ok().as_deref() != Some("1") {
        return;
    }

    for (case, input) in json_cases() {
        let mut line = Line {
            src: "json",
            case,
            ok: false,
            error_kind: "",
            value: None,
            output: "",
            got: "",
        };
        match unmarshal_json(input) {
            Ok(value) => {
                line.ok = true;
                line.value = Some(value);
            }
            Err(error) => line.error_kind = error_kind(&error, "invalid JSON: "),
        }
        emit(&line);
    }

    for (case, input) in yaml_cases() {
        let mut line = Line {
            src: "yaml",
            case,
            ok: false,
            error_kind: "",
            value: None,
            output: "",
            got: "",
        };
        match unmarshal_yaml(input) {
            Ok(value) => {
                line.ok = true;
                line.value = Some(value);
            }
            Err(error) => line.error_kind = error_kind(&error, "invalid YAML: "),
        }
        emit(&line);
    }

    let spec = AutoIndexJobSpec {
        steps: Some(vec![DockerStep {
            root: String::new(),
            image: "go:latest".to_owned(),
            commands: Some(vec!["go mod vendor".to_owned()]),
        }]),
        root: "web/".to_owned(),
        indexer: "lsif-go".to_owned(),
        indexer_args: Some(vec!["index".to_owned(), "--yarn-workspaces".to_owned()]),
        outfile: "lsif.dump".to_owned(),
        ..Default::default()
    };
    let marshal_cases: Vec<(&str, AutoIndexJobSpecList)> = vec![
        ("empty-list", AutoIndexJobSpecList::default()),
        (
            "one-full",
            AutoIndexJobSpecList {
                job_specs: Some(vec![spec.clone()]),
            },
        ),
        (
            "nil-fields-normalized",
            AutoIndexJobSpecList {
                job_specs: Some(vec![AutoIndexJobSpec::default()]),
            },
        ),
        (
            "html-escaping",
            AutoIndexJobSpecList {
                job_specs: Some(vec![AutoIndexJobSpec {
                    root: "<&>\t\n\"\\".to_owned(),
                    indexer: "\u{2028}\u{2029}".to_owned(),
                    outfile: "café".to_owned(),
                    local_steps: None,
                    ..Default::default()
                }]),
            },
        ),
        (
            "two-specs",
            AutoIndexJobSpecList {
                job_specs: Some(vec![
                    spec,
                    AutoIndexJobSpec {
                        indexer: "scip-typescript".to_owned(),
                        ..Default::default()
                    },
                ]),
            },
        ),
        (
            "empty-commands",
            AutoIndexJobSpecList {
                job_specs: Some(vec![AutoIndexJobSpec {
                    steps: Some(vec![DockerStep {
                        image: "go".to_owned(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }]),
            },
        ),
    ];
    for (case, value) in marshal_cases {
        let output = marshal_json(&value);
        emit(&Line {
            src: "marshal",
            case,
            ok: true,
            error_kind: "",
            value: None,
            output: &output,
            got: "",
        });
    }

    for (case, input, expected) in [
        ("no-prefix", "lsif-go", "lsif-go"),
        ("prefix", "sourcegraph/lsif-go", "lsif-go"),
        (
            "prefix-and-suffix",
            "sourcegraph/lsif-go@sha256:...",
            "lsif-go",
        ),
        ("different-name", "myownlsif-go", "myownlsif-go"),
        (
            "multiple-at-signs",
            "sourcegraph/lsif-go@sha256:a@b",
            "lsif-go@sha256:a",
        ),
    ] {
        let got = extract_indexer_name(input);
        emit(&Line {
            src: "indexer",
            case,
            ok: true,
            error_kind: "",
            value: None,
            output: "",
            got: &got,
        });
        assert_eq!(got, expected);
    }
}
