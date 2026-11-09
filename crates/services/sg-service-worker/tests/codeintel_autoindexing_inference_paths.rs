#![forbid(unsafe_code)]

//! M9.8df differential harness (Rust side): `internal/paths/match.go` glob
//! compilation/matching, `luatypes.PathPattern` flattening and `util.go` path
//! filtering.
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/inference/util_harness_test.go`.
//! Three canonical line kinds: `sgl-glob `, `sgl-flatten `, `sgl-filter `.
//! `test_ws/p9-go/run_codeintel_autoindexing_inference_paths_diff.sh` runs
//! both sides and diffs the filtered lines.

use sg_service_worker::jobs::codeintel_autoindexing_path_patterns::{
    filter_paths_by_patterns, flatten_patterns, GlobAndPathspecPattern, PathPattern,
};
use sg_service_worker::paths;

#[derive(serde::Serialize)]
struct GlobSnapshot {
    case: &'static str,
    pattern: &'static str,
    path: String,
    ok: bool,
    error: String,
}

#[derive(serde::Serialize)]
struct PatternView {
    glob: String,
    pathspecs: Vec<String>,
}

#[derive(serde::Serialize)]
struct FlattenSnapshot {
    case: &'static str,
    inverted: bool,
    patterns: Vec<PatternView>,
    error: String,
}

#[derive(serde::Serialize)]
struct FilterSnapshot {
    case: &'static str,
    paths: Vec<String>,
    matched: Vec<String>,
    error: String,
}

const PATHS: [&str; 10] = [
    "go.mod",
    "pkg/go.mod",
    "src/main.go",
    "src/foo/bar.go",
    "vendor/x/y.go",
    "src/app/package.json",
    "package.json",
    "src/app/components/Label.tsx",
    "README.md",
    "/absolute/main.go",
];

fn emit<T: serde::Serialize>(prefix: &str, value: &T) {
    println!(
        "{prefix} {}",
        serde_json::to_string(value).expect("json snapshot")
    );
}

fn pattern(glob: &str, pathspecs: &[&str]) -> PathPattern {
    PathPattern::new(glob, pathspecs.iter().map(|s| (*s).to_owned()).collect())
}

fn view(patterns: Vec<GlobAndPathspecPattern>) -> Vec<PatternView> {
    patterns
        .into_iter()
        .map(|pattern| PatternView {
            glob: pattern.glob,
            pathspecs: pattern.pathspecs,
        })
        .collect()
}

#[test]
fn autoindexing_inference_paths_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_INFERENCE_PATHS_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }

    let glob_cases: [(&str, &str, &[&str]); 18] = [
        ("literal", "cat", &["cat", "cats", "/cat"]),
        ("rooted-literal", "/cat", &["/cat", "cat"]),
        (
            "basename-wildcard",
            "*.tsx",
            &["/src/app/components/Label.tsx", "/src/app/Label.ts"],
        ),
        (
            "anchored-with-subpaths",
            "src/*/test/**/*Test.java",
            &[
                "/src/java/test/UnitTest.java",
                "/src/java/test/sub/UnitTest.java",
                "/src/java/other/UnitTest.java",
            ],
        ),
        ("single-star", "*", &["a", "a/b"]),
        ("everything", "**", &["a/b/c", "z"]),
        ("trailing-slash", "foo/", &["foo/bar", "foo", "/foo/bar"]),
        ("leading-subpath", "**/foo", &["a/b/foo", "foo", "foo/bar"]),
        (
            "segment-wildcard",
            "foo/*/bar",
            &["foo/x/bar", "foo/x/y/bar", "/foo/x/bar"],
        ),
        ("part-wildcard", "a*b", &["axxb", "a/b", "ab"]),
        ("question-mark-is-literal", "a?c", &["abc", "a?c"]),
        ("wildmatch-in-part", "a*c", &["abc", "ac", "a/c"]),
        ("trailing-subpath", "src/**", &["src/a", "src", "src/a/b"]),
        (
            "subpath-extension",
            "**/*.go",
            &["main.go", "a/b.go", "a/b.txt"],
        ),
        (
            "middle-subpath",
            "foo/**/bar",
            &["foo/bar", "foo/x/bar", "foo/x/y/bar", "foo/xbar"],
        ),
        ("multiple-stars", "foo*bar*baz", &["fooxbarybaz", "foobaz"]),
        ("consecutive-slashes", "a//b", &["a/b"]),
        ("only-slash", "/", &["/"]),
    ];

    for (case, glob, paths) in glob_cases {
        match paths::compile(glob) {
            Err(error) => emit(
                "sgl-glob",
                &GlobSnapshot {
                    case,
                    pattern: glob,
                    path: String::new(),
                    ok: false,
                    error,
                },
            ),
            Ok(compiled) => {
                for path in paths {
                    emit(
                        "sgl-glob",
                        &GlobSnapshot {
                            case,
                            pattern: glob,
                            path: (*path).to_owned(),
                            ok: compiled.match_path(path),
                            error: String::new(),
                        },
                    );
                }
            }
        }
    }

    let trees: Vec<(&str, PathPattern)> = vec![
        (
            "combined",
            PathPattern::combined(vec![
                pattern("**/package.json", &[]),
                pattern("**/go.mod", &[]),
            ]),
        ),
        ("single", pattern("**/*.go", &[])),
        (
            "exclude-only",
            PathPattern::exclude(vec![pattern("**/vendor/**", &[])]),
        ),
        (
            "include-plus-exclude",
            PathPattern::combined(vec![
                pattern("**/*.go", &[]),
                PathPattern::exclude(vec![pattern("**/vendor/**", &[])]),
            ]),
        ),
        (
            "with-pathspecs",
            PathPattern::combined(vec![pattern("**/*.ts", &["*.ts", "src/*.ts"])]),
        ),
        (
            "empty-glob-with-pathspec",
            PathPattern::combined(vec![pattern("", &["only-pathspec"])]),
        ),
        (
            "nested-combined",
            PathPattern::exclude(vec![PathPattern::combined(vec![pattern(
                "**/vendor/**",
                &[],
            )])]),
        ),
    ];

    for (case, tree) in &trees {
        for inverted in [false, true] {
            emit(
                "sgl-flatten",
                &FlattenSnapshot {
                    case,
                    inverted,
                    patterns: view(flatten_patterns(std::slice::from_ref(tree), inverted)),
                    error: String::new(),
                },
            );
        }
    }

    let filter_cases: Vec<(&str, PathPattern)> = vec![
        (
            "include-only",
            PathPattern::combined(vec![
                pattern("**/package.json", &[]),
                pattern("**/go.mod", &[]),
            ]),
        ),
        (
            "exclude-only",
            PathPattern::exclude(vec![pattern("**/vendor/**", &[])]),
        ),
        (
            "include-plus-exclude",
            PathPattern::combined(vec![
                pattern("**/*.go", &[]),
                PathPattern::exclude(vec![pattern("**/vendor/**", &[])]),
            ]),
        ),
        (
            "pathspec-and-glob",
            PathPattern::combined(vec![pattern("src/**", &["src/"])]),
        ),
        (
            "invalid-glob",
            PathPattern::combined(vec![pattern("a//b", &[])]),
        ),
    ];

    let paths: Vec<String> = PATHS.iter().map(|p| (*p).to_owned()).collect();
    for (case, tree) in &filter_cases {
        let mut snapshot = FilterSnapshot {
            case,
            paths: paths.clone(),
            matched: Vec::new(),
            error: String::new(),
        };
        match filter_paths_by_patterns(&paths, std::slice::from_ref(tree)) {
            Ok(matched) => snapshot.matched = matched,
            Err(error) => snapshot.error = error,
        }
        emit("sgl-filter", &snapshot);
    }
}
