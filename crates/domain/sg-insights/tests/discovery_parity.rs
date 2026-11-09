#![forbid(unsafe_code)]

//! Differential harness for the insights discovery iterators (M9.8cb).
//!
//! Mirror of `internal/insights/discovery/parity_harness_test.go`: scripted
//! in-memory `RepoStore`/`RepoQueryExecutor` fakes on both sides, same
//! canonical `disc ` lines. No database is involved — this slice pins the
//! iteration logic, the page cache and the error wrapping; the real store and
//! executor are plugged in by the backfill handlers.
//!
//! Gated by `SG_DISCOVERY_CORPUS`; when unset the test is a no-op.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use time::{Duration, OffsetDateTime};

use sg_insights::discovery::{
    AllReposIterator, Clock, MinimalRepo, RepoIterator, RepoQueryExecutor, RepoStore, RepoSummary,
    ReposListOptions, SeriesRepoIterator, SeriesScope,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum StoreMode {
    Pages,
    Error,
    Scoped,
}

struct FakeRepoStore {
    mode: StoreMode,
    calls: Mutex<Vec<String>>,
}

impl FakeRepoStore {
    fn new(mode: StoreMode) -> Self {
        Self {
            mode,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("store calls").clone()
    }

    fn record(&self, call: String) {
        self.calls.lock().expect("store calls").push(call);
    }
}

#[async_trait]
impl RepoStore for FakeRepoStore {
    async fn list(&self, opts: &ReposListOptions) -> Result<Vec<RepoSummary>, String> {
        match self.mode {
            StoreMode::Error => {
                self.record("error".to_owned());
                Err("boom".to_owned())
            }
            StoreMode::Scoped => {
                let names = opts.names.as_deref().unwrap_or(&[]);
                self.record(format!("names:{}", names.join(",")));
                let mut out = Vec::new();
                for (index, name) in names.iter().enumerate().rev() {
                    out.push(RepoSummary {
                        id: 100 + i64::try_from(index).expect("index"),
                        name: name.clone(),
                    });
                }
                Ok(out)
            }
            StoreMode::Pages => {
                let page = opts
                    .limit_offset
                    .clone()
                    .expect("pages mode requires a LimitOffset");
                self.record(format!("limit:{}@{}", page.limit, page.offset));
                if page.offset >= 9 {
                    return Ok(Vec::new());
                }
                let mut out = Vec::new();
                for i in 0..3 {
                    let id = page.offset + i + 1;
                    out.push(RepoSummary {
                        id,
                        name: id.to_string(),
                    });
                }
                Ok(out)
            }
        }
    }
}

struct FakeExecutor {
    calls: Mutex<Vec<String>>,
    error: bool,
}

impl FakeExecutor {
    fn new(error: bool) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            error,
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("executor calls").clone()
    }
}

#[async_trait]
impl RepoQueryExecutor for FakeExecutor {
    async fn execute_repo_list(&self, query: &str) -> Result<Vec<MinimalRepo>, String> {
        self.calls
            .lock()
            .expect("executor calls")
            .push(query.to_owned());
        if self.error {
            return Err("boom".to_owned());
        }
        Ok(vec![
            MinimalRepo {
                id: 5,
                name: "github.com/org/repo".to_owned(),
            },
            MinimalRepo {
                id: 6,
                name: "gitlab.com/org1/repo1".to_owned(),
            },
        ])
    }
}

struct TestClock {
    now: Mutex<OffsetDateTime>,
}

impl TestClock {
    fn new(now: OffsetDateTime) -> Self {
        Self {
            now: Mutex::new(now),
        }
    }

    fn advance(&self, delta: Duration) {
        let mut now = self.now.lock().expect("clock");
        *now += delta;
    }
}

impl Clock for TestClock {
    fn now(&self) -> OffsetDateTime {
        *self.now.lock().expect("clock")
    }
}

#[derive(Clone)]
struct Pair {
    id: i64,
    name: String,
}

async fn collect(iterator: &RepoIterator) -> (Vec<Pair>, Result<(), String>) {
    let mut collected = Vec::new();
    let result = iterator
        .for_each(&mut |name, id| {
            collected.push(Pair {
                id,
                name: name.to_owned(),
            });
            Ok(())
        })
        .await;
    (collected, result)
}

fn fmt_pairs(pairs: &[Pair]) -> String {
    pairs
        .iter()
        .map(|pair| format!("{}:{}", pair.id, pair.name))
        .collect::<Vec<_>>()
        .join(",")
}

fn fmt_calls(calls: &[String]) -> String {
    calls.join(",")
}

fn parse_names(spec: &str) -> Vec<String> {
    if spec.is_empty() || spec == "-" {
        Vec::new()
    } else {
        spec.split(',').map(str::to_owned).collect()
    }
}

fn new_all(
    store: &Arc<FakeRepoStore>,
    clock: &Arc<TestClock>,
    cache_secs: i64,
) -> Arc<AllReposIterator> {
    Arc::new(AllReposIterator::new(
        store.clone(),
        clock.clone(),
        Duration::seconds(cache_secs),
    ))
}

#[tokio::test]
async fn insights_discovery_parity() {
    let corpus_path = match std::env::var("SG_DISCOVERY_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");

    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        match parts[0] {
            "allrepos" => {
                let advance: i64 = parts[1].parse().expect("advance seconds");
                let cache: i64 = parts[2].parse().expect("cache seconds");
                let runs: usize = parts[3].parse().expect("runs");
                let store = Arc::new(FakeRepoStore::new(StoreMode::Pages));
                let clock = Arc::new(TestClock::new(OffsetDateTime::UNIX_EPOCH));
                let iterator = new_all(&store, &clock, cache);
                let mut run_outputs = Vec::new();
                for run in 0..runs {
                    let mut collected = Vec::new();
                    iterator
                        .for_each(&mut |name, id| {
                            collected.push(Pair {
                                id,
                                name: name.to_owned(),
                            });
                            Ok(())
                        })
                        .await
                        .expect("allrepos iteration");
                    run_outputs.push(format!("run{run}={}", fmt_pairs(&collected)));
                    if run + 1 < runs {
                        clock.advance(Duration::seconds(advance));
                    }
                }
                println!(
                    "disc allrepos advance={} cache={} runs={} calls={} {}",
                    advance,
                    cache,
                    runs,
                    fmt_calls(&store.calls()),
                    run_outputs.join(" "),
                );
            }
            "allreposerr" => {
                let store = Arc::new(FakeRepoStore::new(StoreMode::Error));
                let clock = Arc::new(TestClock::new(OffsetDateTime::UNIX_EPOCH));
                let iterator = new_all(&store, &clock, 900);
                let mut collected = Vec::new();
                let result = iterator
                    .for_each(&mut |name, id| {
                        collected.push(Pair {
                            id,
                            name: name.to_owned(),
                        });
                        Ok(())
                    })
                    .await;
                println!(
                    "disc allreposerr calls={} err={}",
                    fmt_calls(&store.calls()),
                    result.expect_err("expected an error"),
                );
            }
            "allreposcallbackerr" => {
                let store = Arc::new(FakeRepoStore::new(StoreMode::Pages));
                let clock = Arc::new(TestClock::new(OffsetDateTime::UNIX_EPOCH));
                let iterator = new_all(&store, &clock, 900);
                let mut collected = Vec::new();
                let result = iterator
                    .for_each(&mut |name, id| {
                        if name == "2" {
                            return Err("cb-error".to_owned());
                        }
                        collected.push(Pair {
                            id,
                            name: name.to_owned(),
                        });
                        Ok(())
                    })
                    .await;
                println!(
                    "disc allreposcallbackerr calls={} collected={} err={}",
                    fmt_calls(&store.calls()),
                    fmt_pairs(&collected),
                    result.expect_err("expected an error"),
                );
            }
            "query" | "queryerr" | "queryinvalid" => {
                let query = parts[1];
                let executor = Arc::new(FakeExecutor::new(parts[0] == "queryerr"));
                let result =
                    sg_insights::discovery::RepoIteratorFromQuery::new(query, executor.as_ref())
                        .await;
                match result {
                    Ok(iterator) => {
                        let mut collected = Vec::new();
                        iterator
                            .for_each(&mut |name, id| {
                                collected.push(Pair {
                                    id,
                                    name: name.to_owned(),
                                });
                                Ok(())
                            })
                            .await
                            .expect("query iteration");
                        let calls = executor.calls();
                        println!(
                            "disc {} q={} execcalls={} executor_query=\"{}\" repos={}",
                            parts[0],
                            query,
                            calls.len(),
                            calls.first().cloned().unwrap_or_default(),
                            fmt_pairs(&collected),
                        );
                    }
                    Err(err) => {
                        println!(
                            "disc {} q={} execcalls={} err={}",
                            parts[0],
                            query,
                            executor.calls().len(),
                            err,
                        );
                    }
                }
            }
            "scoped" | "scopederr" => {
                let names = parse_names(parts[1]);
                let mode = if parts[0] == "scopederr" {
                    StoreMode::Error
                } else {
                    StoreMode::Scoped
                };
                let store = Arc::new(FakeRepoStore::new(mode));
                let result =
                    sg_insights::discovery::ScopedRepoIterator::new(&names, store.as_ref()).await;
                match result {
                    Ok(iterator) => {
                        let mut collected = Vec::new();
                        iterator
                            .for_each(&mut |name, id| {
                                collected.push(Pair {
                                    id,
                                    name: name.to_owned(),
                                });
                                Ok(())
                            })
                            .await
                            .expect("scoped iteration");
                        println!(
                            "disc {} names={} storecalls={} repos={}",
                            parts[0],
                            parts[1],
                            fmt_calls(&store.calls()),
                            fmt_pairs(&collected),
                        );
                    }
                    Err(err) => {
                        println!(
                            "disc {} names={} storecalls={} err={}",
                            parts[0],
                            parts[1],
                            fmt_calls(&store.calls()),
                            err,
                        );
                    }
                }
            }
            "seriesall" | "seriesquery" | "seriesscoped" => {
                let store = Arc::new(FakeRepoStore::new(match parts[0] {
                    "seriesscoped" => StoreMode::Scoped,
                    _ => StoreMode::Pages,
                }));
                let clock = Arc::new(TestClock::new(OffsetDateTime::UNIX_EPOCH));
                let executor = Arc::new(FakeExecutor::new(false));
                let all = new_all(&store, &clock, 900);
                let dispatcher = SeriesRepoIterator::new(all, store.clone(), executor.clone());
                let series = match parts[0] {
                    "seriesall" => SeriesScope::default(),
                    "seriesquery" => SeriesScope {
                        repositories: Vec::new(),
                        repository_criteria: Some(parts[1].to_owned()),
                    },
                    _ => SeriesScope {
                        repositories: parse_names(parts[1]),
                        repository_criteria: None,
                    },
                };
                let iterator = dispatcher.for_series(&series).await.expect("for_series");
                let (collected, result) = collect(&iterator).await;
                result.expect("series iteration");
                println!(
                    "disc series kind={} storecalls={} execcalls={} repos={}",
                    iterator.kind(),
                    fmt_calls(&store.calls()),
                    executor.calls().len(),
                    fmt_pairs(&collected),
                );
            }
            other => panic!("unknown op {other}"),
        }
    }
}
