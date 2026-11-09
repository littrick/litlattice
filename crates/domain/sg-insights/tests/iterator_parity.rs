//! M9.8bx differential: the pure `internal/insights/scheduler/iterator`
//! state, mirroring
//! `internal/insights/scheduler/iterator/parity_harness_test.go`.
//! Gated by `SG_INSIGHTS_ITERATOR_CORPUS`; each corpus line is
//! `<op>\t<args...>` and produces one output line with a stable prefix.
//! Map-backed outputs are sorted by repo id on both sides.

use std::collections::BTreeMap;

use sg_insights::iterator::{IterationConfig, IterationError, RepoIterator};

fn parse_repos(spec: &str) -> Vec<i32> {
    if spec.is_empty() || spec == "-" {
        return Vec::new();
    }
    spec.split(',')
        .map(|part| part.parse().expect("repo"))
        .collect()
}

fn parse_errors(spec: &str) -> BTreeMap<i32, IterationError> {
    let mut got = BTreeMap::new();
    if spec.is_empty() || spec == "-" {
        return got;
    }
    for part in spec.split('|') {
        let (repo, count) = part.split_once(':').expect("repo:count");
        let repo: i32 = repo.parse().expect("repo");
        got.insert(
            repo,
            IterationError {
                repo_id: repo,
                failure_count: count.parse().expect("failure count"),
                error_messages: vec!["boom".to_owned()],
            },
        );
    }
    got
}

fn format_repos(repos: &[i32]) -> String {
    if repos.is_empty() {
        return "-".to_owned();
    }
    let mut sorted = repos.to_vec();
    sorted.sort_unstable();
    sorted
        .iter()
        .map(|repo| repo.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn format_keys(map: &BTreeMap<i32, IterationError>) -> String {
    if map.is_empty() {
        return "-".to_owned();
    }
    map.keys()
        .map(|repo| repo.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn iterator_with_errors(spec: &str) -> RepoIterator {
    RepoIterator {
        errors: parse_errors(spec),
        ..RepoIterator::default()
    }
}

#[test]
fn insights_iterator_parity() {
    let Ok(path) = std::env::var("SG_INSIGHTS_ITERATOR_CORPUS") else {
        return;
    };
    let corpus = std::fs::read_to_string(path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        match parts[0] {
            "hasmore" => {
                let mut it = RepoIterator {
                    repos: parse_repos(parts[1]),
                    ..RepoIterator::default()
                };
                it.cursor = parts[2].parse().expect("cursor");
                println!("hm\t{}\t{}\t{}", parts[1], parts[2], it.has_more());
            }
            "peekn" => {
                let it = RepoIterator {
                    repos: parse_repos(parts[1]),
                    ..RepoIterator::default()
                };
                let cursor: usize = parts[2].parse().expect("cursor");
                let num: usize = parts[3].parse().expect("num");
                let got = it.peek_n(cursor, num);
                let ok = cursor < it.repos.len();
                println!(
                    "pn\t{}\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    parts[3],
                    ok,
                    format_repos(&got)
                );
            }
            "errors" => {
                let it = RepoIterator {
                    errors: parse_errors(parts[1]),
                    terminal_errors: parse_errors(parts[2]),
                    ..RepoIterator::default()
                };
                println!(
                    "er\t{}\t{}\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    it.errored_repos(),
                    it.total_errors(),
                    it.has_errors(),
                    it.has_terminal_errors()
                );
            }
            "resetretry" => {
                let max_failures: i64 = parts[1].parse().expect("max failures");
                let mut it = iterator_with_errors(parts[2]);
                it.reset_retry(IterationConfig { max_failures });
                println!(
                    "rr\t{}\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    format_repos(&it.retry_repos),
                    format_keys(&it.terminal_errors),
                    format_keys(&it.errors)
                );
            }
            "nextretry" => {
                let max_failures: i64 = parts[1].parse().expect("max failures");
                let mut it = iterator_with_errors(parts[2]);
                it.retry_repos = parse_repos(parts[3]);
                it.retry_cursor = parts[4].parse().expect("retry cursor");
                let got = it
                    .next_retry(IterationConfig { max_failures })
                    .map(i64::from)
                    .unwrap_or(-1);
                println!(
                    "nr\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    parts[3],
                    parts[4],
                    got,
                    it.retry_cursor,
                    format_keys(&it.terminal_errors),
                    format_keys(&it.errors)
                );
            }
            "setterminal" => {
                let repo: i32 = parts[2].parse().expect("repo");
                let mut it = iterator_with_errors(parts[1]);
                it.set_repo_terminal(repo);
                println!(
                    "st\t{}\t{}\t{}\t{}",
                    parts[1],
                    parts[2],
                    format_keys(&it.terminal_errors),
                    format_keys(&it.errors)
                );
            }
            other => panic!("unknown op {other:?}"),
        }
    }
}
