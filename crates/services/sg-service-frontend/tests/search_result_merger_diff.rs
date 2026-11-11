//! Differential harness for the result merger partition of
//! `internal/search/result/merger.go` (M9.8hr).
//!
//! Reads `test_ws/p9-go/data/search_result_merger_corpus.json`, runs
//! `sg_service_frontend::search_result_merger::Merger`, and writes normalized
//! JSON for byte comparison with the Go oracle
//! (`internal/search/result/zz_m9hr_oracle_test.go`).
//!
//! Env: `SG_M9HR_DIFF=1`, `M9HR_CORPUS`, `M9HR_OUT`.

use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use serde_json::{json, Value};
use time::OffsetDateTime;

use sg_service_frontend::search_result_merger::{Key, Merger};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MatchedString, MinimalRepo,
    OwnerMatch, RepoMatch, ResolvedOwner, Signature, Symbol, SymbolMatch,
};
use sg_service_frontend::search_stream::events::{Location, Range};

#[derive(Deserialize)]
struct Case {
    name: String,
    num_sources: usize,
    adds: Vec<AddSpec>,
}

#[derive(Deserialize)]
struct AddSpec {
    source: usize,
    matches: Vec<MatchSpec>,
}

#[derive(Deserialize)]
struct MatchSpec {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    input_rev: Option<String>,
    /// Number of ranges in each chunk match.
    #[serde(default)]
    chunks: Vec<i64>,
    #[serde(default)]
    symbols: Vec<SymbolSpec>,
    #[serde(default)]
    limit_hit: bool,
    #[serde(default)]
    author_nanos: i64,
    /// `None` models a nil `MessagePreview`.
    #[serde(default)]
    message_ranges: Option<Vec<[i64; 2]>>,
    /// `Some` sets `DiffPreview` (which also changes the key rank).
    #[serde(default)]
    diff_ranges: Option<Vec<[i64; 2]>>,
    #[serde(default)]
    repo_name_ranges: Vec<[i64; 2]>,
    #[serde(default)]
    description_ranges: Vec<[i64; 2]>,
    #[serde(default)]
    owner: Option<OwnerSpec>,
}

#[derive(Deserialize)]
struct SymbolSpec {
    #[serde(default)]
    line: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    parent: String,
    #[serde(default)]
    parent_kind: String,
}

#[derive(Deserialize)]
struct OwnerSpec {
    kind: String,
    #[serde(default)]
    handle: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    name: String,
}

fn location(line: i64) -> Location {
    Location {
        offset: 0,
        line,
        column: 0,
    }
}

fn ranges(spec: &[[i64; 2]]) -> Vec<Range> {
    spec.iter()
        .map(|range| Range {
            start: location(range[0]),
            end: location(range[1]),
        })
        .collect()
}

fn file_ref(spec: &MatchSpec) -> FileRef {
    FileRef {
        repo: MinimalRepo {
            id: spec.id,
            name: spec.repo.clone(),
        },
        commit_id: spec.commit.clone(),
        path: spec.path.clone(),
        input_rev: spec.input_rev.clone(),
        precise_language: String::new(),
    }
}

fn signature(nanos: i64) -> Signature {
    let date = OffsetDateTime::from_unix_timestamp_nanos(i128::from(nanos))
        .expect("valid author timestamp");
    Signature {
        name: String::new(),
        email: String::new(),
        date,
    }
}

fn build(spec: &MatchSpec) -> Match {
    match spec.kind.as_str() {
        "file" => {
            let file = file_ref(spec);
            let chunk_matches = spec
                .chunks
                .iter()
                .enumerate()
                .map(|(index, count)| ChunkMatch {
                    content: String::new(),
                    content_start: location(index as i64),
                    ranges: vec![
                        Range {
                            start: location(index as i64),
                            end: location(index as i64),
                        };
                        *count as usize
                    ],
                })
                .collect();
            let symbols = spec
                .symbols
                .iter()
                .map(|symbol| SymbolMatch {
                    symbol: Symbol {
                        name: symbol.name.clone(),
                        path: spec.path.clone(),
                        line: symbol.line,
                        character: 0,
                        kind: symbol.kind.clone(),
                        language: String::new(),
                        parent: symbol.parent.clone(),
                        parent_kind: symbol.parent_kind.clone(),
                    },
                    file: file.clone(),
                })
                .collect();
            Match::File(FileMatch {
                file,
                chunk_matches,
                symbols,
                path_matches: Vec::new(),
                limit_hit: spec.limit_hit,
                debug: None,
            })
        }
        "repo" => Match::Repo(RepoMatch {
            name: spec.name.clone(),
            id: spec.id,
            rev: spec.rev.clone(),
            description_matches: ranges(&spec.description_ranges),
            repo_name_matches: ranges(&spec.repo_name_ranges),
        }),
        "commit" => {
            let commit = GitCommit {
                id: spec.commit.clone(),
                message: String::new(),
                author: signature(spec.author_nanos),
                committer: signature(spec.author_nanos),
                parents: Vec::new(),
            };
            let message_preview = spec.message_ranges.as_ref().map(|matched| MatchedString {
                content: String::new(),
                matched_ranges: ranges(matched),
            });
            let diff_preview = spec.diff_ranges.as_ref().map(|matched| MatchedString {
                content: String::new(),
                matched_ranges: ranges(matched),
            });
            Match::Commit(CommitMatch {
                commit,
                repo: MinimalRepo {
                    id: spec.id,
                    name: spec.repo.clone(),
                },
                message_preview,
                diff_preview,
                refs: Vec::new(),
                source_refs: Vec::new(),
                diff: Vec::new(),
                modified_files: Vec::new(),
            })
        }
        "owner" => {
            let owner_spec = spec
                .owner
                .as_ref()
                .expect("owner match requires owner spec");
            let owner = match owner_spec.kind.as_str() {
                "person" => ResolvedOwner::Person {
                    handle: owner_spec.handle.clone(),
                    email: owner_spec.email.clone(),
                    user: None,
                },
                "team" => ResolvedOwner::Team {
                    handle: owner_spec.handle.clone(),
                    email: owner_spec.email.clone(),
                    name: owner_spec.name.clone(),
                    display_name: String::new(),
                },
                other => panic!("unknown owner kind {other}"),
            };
            Match::Owner(OwnerMatch {
                owner,
                repo: MinimalRepo {
                    id: spec.id,
                    name: spec.repo.clone(),
                },
            })
        }
        other => panic!("unknown match type {other}"),
    }
}

fn norm_match(match_: &Match) -> Value {
    match match_ {
        Match::File(fm) => json!({
            "type": "file",
            "repo": fm.file.repo.name,
            "id": fm.file.repo.id,
            "commit": fm.file.commit_id,
            "path": fm.file.path,
            "input_rev": fm.file.input_rev,
            "chunks": fm
                .chunk_matches
                .iter()
                .map(|chunk| chunk.ranges.len())
                .collect::<Vec<_>>(),
            "symbols": fm
                .symbols
                .iter()
                .map(|symbol| json!({
                    "line": symbol.symbol.line,
                    "name": symbol.symbol.name,
                    "kind": symbol.symbol.kind,
                    "parent": symbol.symbol.parent,
                    "parent_kind": symbol.symbol.parent_kind,
                }))
                .collect::<Vec<_>>(),
            "limit_hit": fm.limit_hit,
        }),
        Match::Repo(rm) => json!({
            "type": "repo",
            "name": rm.name,
            "id": rm.id,
            "rev": rm.rev,
            "repo_name_ranges": rm.repo_name_matches.len(),
            "description_ranges": rm.description_matches.len(),
        }),
        Match::Commit(cm) => json!({
            "type": "commit",
            "repo": cm.repo.name,
            "id": cm.repo.id,
            "commit": cm.commit.id,
            "author_nanos": cm.commit.author.date.unix_timestamp_nanos() as i64,
            "has_message": cm.message_preview.is_some(),
            "message_ranges": cm
                .message_preview
                .as_ref()
                .map_or(0, |preview| preview.matched_ranges.len()),
            "has_diff": cm.diff_preview.is_some(),
            "diff_ranges": cm
                .diff_preview
                .as_ref()
                .map_or(0, |preview| preview.matched_ranges.len()),
        }),
        Match::Owner(om) => {
            let key = Key::from_match(match_);
            json!({
                "type": "owner",
                "repo": om.repo.name,
                "id": om.repo.id,
                "owner_metadata": key.owner_metadata,
            })
        }
    }
}

/// Expected number of distinct sources that reported each key.
fn expected_popcounts(case: &Case) -> HashMap<Key, usize> {
    let mut sources: HashMap<Key, HashSet<usize>> = HashMap::new();
    for add in &case.adds {
        for spec in &add.matches {
            let match_ = build(spec);
            sources
                .entry(Key::from_match(&match_))
                .or_default()
                .insert(add.source);
        }
    }
    sources
        .into_iter()
        .map(|(key, sources)| (key, sources.len()))
        .collect()
}

fn run_case(case: &Case) -> Value {
    let popcounts = expected_popcounts(case);
    let popcount = |match_: &Match| {
        popcounts
            .get(&Key::from_match(match_))
            .copied()
            .unwrap_or(0)
    };

    let mut merger = Merger::new(case.num_sources);
    let mut adds = Vec::with_capacity(case.adds.len());
    for add in &case.adds {
        let matches: Vec<Match> = add.matches.iter().map(build).collect();
        let returned = merger.add_matches(matches, add.source);
        adds.push(json!({
            "source": add.source,
            "returned": returned.iter().map(norm_match).collect::<Vec<_>>(),
        }));
    }

    let unsent = merger.unsent_tracked();
    // The baseline's unstable sort leaves equal-popcount ties unordered; only
    // the descending popcount sequence is meaningful.
    let order_ok = unsent
        .windows(2)
        .all(|pair| popcount(&pair[0]) >= popcount(&pair[1]));

    let mut values: Vec<(usize, Value)> = unsent
        .iter()
        .map(|match_| (popcount(match_), norm_match(match_)))
        .collect();
    values.sort_by(|left, right| {
        right.0.cmp(&left.0).then_with(|| {
            serde_json::to_string(&left.1)
                .expect("serialize match")
                .cmp(&serde_json::to_string(&right.1).expect("serialize match"))
        })
    });
    let matches: Vec<Value> = values.into_iter().map(|(_, value)| value).collect();

    json!({
        "name": case.name,
        "num_sources": case.num_sources,
        "adds": adds,
        "unsent": {
            "order_ok": order_ok,
            "matches": matches,
        },
    })
}

#[test]
fn search_result_merger_diff() {
    if std::env::var("SG_M9HR_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HR_CORPUS").expect("M9HR_CORPUS");
    let out_path = std::env::var("M9HR_OUT").expect("M9HR_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let output: Vec<Value> = cases.iter().map(run_case).collect();
    let encoded = serde_json::to_string(&output).expect("serialize output");
    std::fs::write(&out_path, encoded).expect("write output");
}
