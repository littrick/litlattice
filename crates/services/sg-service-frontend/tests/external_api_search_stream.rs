//! M9.8ew differential harness: the external `/.api/search/stream` frontend
//! search event stream. Mirrors
//! `cmd/frontend/internal/search/zz_search_stream_harness_test.go`.
//!
//! Prints one `ss ` line per case with the status, selected headers and the
//! (gunzipped) body. The diff script normalises `"durationMs":N`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::{header, Method, Request};
use http_body_util::BodyExt;
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MatchedString, MinimalRepo,
    OwnerMatch, RepoMatch, RepoMetadata, ResolvedOwner, Signature, Symbol, SymbolMatch,
};
use sg_service_frontend::search_stream::events::{EventAlert, UserMetadata};
use sg_service_frontend::search_stream::progress::{
    repo_status, RepoStatusMap, SearchEvent, Stats,
};
use sg_service_frontend::search_stream::{
    search_response, ExecuteItem, PlanError, SearchDeps, SearchInputs,
};
use time::OffsetDateTime;

struct FakeDeps {
    plan: Result<SearchInputs, PlanError>,
    items: Mutex<Vec<ExecuteItem>>,
    metadata: HashMap<i32, RepoMetadata>,
    names: HashMap<i32, String>,
}

#[async_trait]
impl SearchDeps for FakeDeps {
    async fn plan(
        &self,
        _args: &sg_service_frontend::search_stream::args::Args,
    ) -> Result<SearchInputs, PlanError> {
        self.plan.clone()
    }

    async fn execute(
        &self,
        _inputs: &SearchInputs,
        _query: &str,
    ) -> std::pin::Pin<Box<dyn tokio_stream::Stream<Item = ExecuteItem> + Send>> {
        let items = std::mem::take(&mut *self.items.lock().expect("items"));
        Box::pin(tokio_stream::iter(items))
    }

    async fn repo_metadata(&self, ids: &[i32]) -> Result<HashMap<i32, RepoMetadata>, String> {
        let mut out = HashMap::new();
        for id in ids {
            if let Some(metadata) = self.metadata.get(id) {
                out.insert(*id, metadata.clone());
            }
        }
        Ok(out)
    }

    async fn repo_names(&self, ids: &[i32]) -> Vec<(i32, String)> {
        ids.iter()
            .filter_map(|id| self.names.get(id).map(|name| (*id, name.clone())))
            .collect()
    }

    fn is_authenticated(&self) -> bool {
        false
    }
}

fn ts(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
}

fn repo_meta() -> HashMap<i32, RepoMetadata> {
    let mut map = HashMap::new();
    let mut kvps = BTreeMap::new();
    kvps.insert("k".to_owned(), Some("v".to_owned()));
    kvps.insert("n".to_owned(), None);
    map.insert(
        1,
        RepoMetadata {
            id: 1,
            name: "github.com/foo/bar".to_owned(),
            description: "A repo <desc> & stuff".to_owned(),
            fork: false,
            archived: false,
            private: true,
            stars: 42,
            last_fetched: Some(ts(1609556645)),
            key_value_pairs: Some(kvps),
            topics: Some(vec!["t1".to_owned(), "t2".to_owned()]),
        },
    );
    map.insert(
        2,
        RepoMetadata {
            id: 2,
            name: "gitlab.com/a/b".to_owned(),
            description: String::new(),
            fork: false,
            archived: false,
            private: false,
            stars: 0,
            last_fetched: None,
            key_value_pairs: None,
            topics: None,
        },
    );
    map.insert(
        3,
        RepoMetadata {
            id: 3,
            name: "example.com/x/y".to_owned(),
            description: String::new(),
            fork: true,
            archived: false,
            private: false,
            stars: 7,
            last_fetched: None,
            key_value_pairs: None,
            topics: Some(vec!["only".to_owned()]),
        },
    );
    map.insert(
        4,
        RepoMetadata {
            id: 4,
            name: "archived.example.com/old/repo".to_owned(),
            description: String::new(),
            fork: false,
            archived: true,
            private: false,
            stars: 0,
            last_fetched: None,
            key_value_pairs: None,
            topics: None,
        },
    );
    map
}

fn repo(id: i32) -> MinimalRepo {
    let metadata = repo_meta();
    let metadata = &metadata[&id];
    MinimalRepo {
        id: metadata.id,
        name: metadata.name.clone(),
    }
}

fn range(
    offset: i64,
    line: i64,
    column: i64,
    length: i64,
) -> sg_service_frontend::search_stream::events::Range {
    use sg_service_frontend::search_stream::events::Location;
    sg_service_frontend::search_stream::events::Range {
        start: Location {
            offset,
            line,
            column,
        },
        end: Location {
            offset: offset + length,
            line,
            column: column + length,
        },
    }
}

fn chunk_file() -> FileMatch {
    use sg_service_frontend::search_stream::events::Location;
    FileMatch {
        file: FileRef {
            repo: repo(1),
            commit_id: "abc123".to_owned(),
            path: "src/main.go".to_owned(),
            input_rev: Some("main".to_owned()),
            precise_language: "Go".to_owned(),
        },
        chunk_matches: vec![
            ChunkMatch {
                content: "line one match\nsecond line\n".to_owned(),
                content_start: Location {
                    offset: 0,
                    line: 1,
                    column: 0,
                },
                ranges: vec![
                    sg_service_frontend::search_stream::events::Range {
                        start: Location {
                            offset: 9,
                            line: 1,
                            column: 9,
                        },
                        end: Location {
                            offset: 14,
                            line: 1,
                            column: 14,
                        },
                    },
                    sg_service_frontend::search_stream::events::Range {
                        start: Location {
                            offset: 15,
                            line: 2,
                            column: 0,
                        },
                        end: Location {
                            offset: 21,
                            line: 2,
                            column: 6,
                        },
                    },
                ],
            },
            ChunkMatch {
                content: "third line\n".to_owned(),
                content_start: Location {
                    offset: 27,
                    line: 3,
                    column: 0,
                },
                ranges: vec![sg_service_frontend::search_stream::events::Range {
                    start: Location {
                        offset: 27,
                        line: 3,
                        column: 0,
                    },
                    end: Location {
                        offset: 32,
                        line: 3,
                        column: 5,
                    },
                }],
            },
        ],
        symbols: Vec::new(),
        path_matches: Vec::new(),
        limit_hit: false,
        debug: None,
    }
}

fn path_file() -> FileMatch {
    FileMatch {
        file: FileRef {
            repo: repo(1),
            commit_id: "def456".to_owned(),
            path: "docs/README.md".to_owned(),
            input_rev: None,
            precise_language: String::new(),
        },
        chunk_matches: Vec::new(),
        symbols: Vec::new(),
        path_matches: vec![range(0, 1, 0, 5)],
        limit_hit: false,
        debug: None,
    }
}

fn symbol_file() -> FileMatch {
    let file = FileRef {
        repo: repo(2),
        commit_id: "sym789".to_owned(),
        path: "src/util.go".to_owned(),
        input_rev: None,
        precise_language: "Go".to_owned(),
    };
    FileMatch {
        file: file.clone(),
        chunk_matches: Vec::new(),
        symbols: vec![
            SymbolMatch {
                symbol: Symbol {
                    name: "Foo".to_owned(),
                    path: "src/util.go".to_owned(),
                    line: 10,
                    character: 4,
                    kind: "function".to_owned(),
                    language: "Go".to_owned(),
                    parent: "Bar".to_owned(),
                    parent_kind: String::new(),
                },
                file: file.clone(),
            },
            SymbolMatch {
                symbol: Symbol {
                    name: "Baz".to_owned(),
                    path: "src/util.go".to_owned(),
                    line: 20,
                    character: 0,
                    kind: "enummember".to_owned(),
                    language: "Go".to_owned(),
                    parent: String::new(),
                    parent_kind: String::new(),
                },
                file,
            },
        ],
        path_matches: Vec::new(),
        limit_hit: false,
        debug: None,
    }
}

fn repo_match() -> RepoMatch {
    RepoMatch {
        name: "gitlab.com/a/b".to_owned(),
        id: 2,
        rev: "release".to_owned(),
        description_matches: vec![range(2, 1, 2, 3)],
        repo_name_matches: vec![range(0, 1, 0, 6)],
    }
}

fn commit_matches() -> Vec<CommitMatch> {
    let author = Signature {
        name: "Alice <A> & \"B\"".to_owned(),
        email: "alice@example.com".to_owned(),
        date: ts(1577934245),
    };
    let committer = Signature {
        name: "Bob".to_owned(),
        email: "bob@example.com".to_owned(),
        date: ts(1578024306),
    };
    let message = CommitMatch {
        commit: GitCommit {
            id: "abcdef1234567890".to_owned(),
            message: "fix: handle <stuff>\n\nBody line\n".to_owned(),
            author: author.clone(),
            committer: committer.clone(),
            parents: Vec::new(),
        },
        repo: repo(1),
        message_preview: Some(MatchedString {
            content: "fix: handle <stuff>\n\nBody line\n".to_owned(),
            matched_ranges: vec![range(5, 1, 5, 6), range(25, 3, 0, 4)],
        }),
        diff_preview: None,
        refs: Vec::new(),
        source_refs: Vec::new(),
        diff: Vec::new(),
        modified_files: Vec::new(),
    };
    let diff = CommitMatch {
        commit: GitCommit {
            id: "fedcba".to_owned(),
            message: "subject only".to_owned(),
            author,
            committer,
            parents: Vec::new(),
        },
        repo: repo(3),
        message_preview: None,
        diff_preview: Some(MatchedString {
            content: "@@ -1 +1 @@\n-old\n+new\n".to_owned(),
            matched_ranges: vec![range(12, 2, 0, 3)],
        }),
        refs: Vec::new(),
        source_refs: Vec::new(),
        diff: Vec::new(),
        modified_files: Vec::new(),
    };
    vec![message, diff]
}

fn owner_matches() -> Vec<Match> {
    vec![
        Match::Owner(OwnerMatch {
            owner: ResolvedOwner::Person {
                handle: "alice".to_owned(),
                email: "alice@example.com".to_owned(),
                user: Some(UserMetadata {
                    username: "alice".to_owned(),
                    display_name: "Alice".to_owned(),
                    avatar_url: "https://example.com/alice.png".to_owned(),
                }),
            },
            repo: repo(1),
        }),
        Match::Owner(OwnerMatch {
            owner: ResolvedOwner::Team {
                handle: "team-x".to_owned(),
                email: "team-x@example.com".to_owned(),
                name: "team-x".to_owned(),
                display_name: "Team X".to_owned(),
            },
            repo: repo(2),
        }),
        Match::Owner(OwnerMatch {
            owner: ResolvedOwner::Person {
                handle: "carol".to_owned(),
                email: "carol@example.com".to_owned(),
                user: None,
            },
            repo: repo(3),
        }),
    ]
}

fn skipped_stats() -> Stats {
    let mut status = RepoStatusMap::default();
    status.update(2, repo_status::MISSING);
    status.update(3, repo_status::CLONING);
    status.update(4, repo_status::TIMED_OUT);
    Stats {
        is_limit_hit: true,
        repos: HashSet::from([1, 2, 3, 4]),
        status,
        backends_missing: 2,
        excluded_forks: 3,
        excluded_archived: 1,
    }
}

fn event(results: Vec<Match>) -> SearchEvent {
    SearchEvent {
        results,
        stats: Stats::default(),
    }
}

fn events() -> HashMap<&'static str, Vec<SearchEvent>> {
    let mut map = HashMap::new();
    map.insert("empty", Vec::new());
    map.insert("path", vec![event(vec![Match::File(path_file())])]);
    map.insert("content-line", vec![event(vec![Match::File(chunk_file())])]);
    map.insert(
        "content-chunk",
        vec![event(vec![Match::File(chunk_file())])],
    );
    map.insert("symbol", vec![event(vec![Match::File(symbol_file())])]);
    map.insert("repo", vec![event(vec![Match::Repo(repo_match())])]);
    map.insert(
        "commit",
        vec![event(
            commit_matches().into_iter().map(Match::Commit).collect(),
        )],
    );
    map.insert("owner", vec![event(owner_matches())]);
    map.insert(
        "progress",
        vec![
            SearchEvent {
                results: Vec::new(),
                stats: Stats {
                    repos: HashSet::from([1]),
                    ..Stats::default()
                },
            },
            SearchEvent {
                results: Vec::new(),
                stats: skipped_stats(),
            },
        ],
    );
    map.insert("display", vec![event(vec![Match::File(chunk_file())])]);
    map.insert(
        "batching",
        vec![
            event(vec![Match::File(chunk_file()), Match::File(path_file())]),
            event(vec![Match::File(path_file())]),
        ],
    );
    map.insert("gzip", vec![event(vec![Match::File(chunk_file())])]);
    map
}

struct Case {
    name: &'static str,
    target: &'static str,
    accept: Option<&'static str>,
    plan: Result<SearchInputs, PlanError>,
    items: Vec<ExecuteItem>,
}

fn inputs() -> SearchInputs {
    use sg_search_query::parser::SearchType;
    use sg_service_frontend::search_client::{Features, Protocol, SearchMode};
    SearchInputs {
        max_results: 10000,
        original_query: "foo".to_owned(),
        plan: sg_search_query::pipeline_query("foo", SearchType::Standard).expect("plan"),
        search_mode: SearchMode::Precise,
        pattern_type: SearchType::Standard,
        features: Features::default(),
        protocol: Protocol::Streaming,
        context_lines: 1,
        sanitize_search_patterns: Vec::new(),
        on_sourcegraph_dot_com: false,
    }
}

fn cases(events: &HashMap<&'static str, Vec<SearchEvent>>) -> Vec<Case> {
    let ok = || Ok(inputs());
    let empty = || Vec::new();
    let ev = |name: &'static str| -> Vec<ExecuteItem> {
        events[name]
            .iter()
            .cloned()
            .map(ExecuteItem::Event)
            .collect()
    };
    vec![
        Case {
            name: "bad-no-query",
            target: "/.api/search/stream?display=x",
            accept: None,
            plan: ok(),
            items: empty(),
        },
        Case {
            name: "bad-display",
            target: "/.api/search/stream?q=foo&display=x",
            accept: None,
            plan: ok(),
            items: empty(),
        },
        Case {
            name: "bad-max-line-len",
            target: "/.api/search/stream?q=foo&display=7&max-line-len=x",
            accept: None,
            plan: ok(),
            items: empty(),
        },
        Case {
            name: "bad-cm",
            target: "/.api/search/stream?q=foo&cm=truex",
            accept: None,
            plan: ok(),
            items: empty(),
        },
        Case {
            name: "bad-cl",
            target: "/.api/search/stream?q=foo&cl=x",
            accept: None,
            plan: ok(),
            items: empty(),
        },
        Case {
            name: "bad-sm",
            target: "/.api/search/stream?q=foo&sm=x",
            accept: None,
            plan: ok(),
            items: empty(),
        },
        Case {
            name: "plan-query",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: Err(PlanError::Query {
                message: "expected operand at 2".to_owned(),
                expected_operand: true,
            }),
            items: empty(),
        },
        Case {
            name: "plan-query-generic",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: Err(PlanError::Query {
                message: "bad \"thing\" <x>".to_owned(),
                expected_operand: false,
            }),
            items: empty(),
        },
        Case {
            name: "plan-other",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: Err(PlanError::Other("boom <x> & \"y\"".to_owned())),
            items: empty(),
        },
        Case {
            name: "empty",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: ev("empty"),
        },
        Case {
            name: "path",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: ev("path"),
        },
        Case {
            name: "content-line",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: ev("content-line"),
        },
        Case {
            name: "content-line-truncate",
            target: "/.api/search/stream?q=foo&max-line-len=6",
            accept: None,
            plan: ok(),
            items: ev("content-line"),
        },
        Case {
            name: "content-chunk",
            target: "/.api/search/stream?q=foo&cm=t",
            accept: None,
            plan: ok(),
            items: ev("content-chunk"),
        },
        Case {
            name: "content-chunk-truncate",
            target: "/.api/search/stream?q=foo&cm=t&max-line-len=6",
            accept: None,
            plan: ok(),
            items: ev("content-chunk"),
        },
        Case {
            name: "symbol",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: ev("symbol"),
        },
        Case {
            name: "repo",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: ev("repo"),
        },
        Case {
            name: "commit",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: ev("commit"),
        },
        Case {
            name: "owner",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: ev("owner"),
        },
        Case {
            name: "progress",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: ev("progress"),
        },
        Case {
            name: "display",
            target: "/.api/search/stream?q=foo&display=2&cm=t",
            accept: None,
            plan: ok(),
            items: ev("display"),
        },
        Case {
            name: "batching",
            target: "/.api/search/stream?q=foo&cm=t",
            accept: None,
            plan: ok(),
            items: ev("batching"),
        },
        Case {
            name: "tail-alert",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: {
                let mut items = ev("path");
                items.push(ExecuteItem::Alert(EventAlert {
                    title: "Alert <title>".to_owned(),
                    description: "Alert & description".to_owned(),
                    kind: "kind-x".to_owned(),
                    proposed_queries: None,
                }));
                items
            },
        },
        Case {
            name: "tail-unsupported",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: {
                let mut items = ev("path");
                items.push(ExecuteItem::Error {
                    message: "unsupported <pattern> & query".to_owned(),
                    unsupported: true,
                });
                items
            },
        },
        Case {
            name: "tail-error",
            target: "/.api/search/stream?q=foo",
            accept: None,
            plan: ok(),
            items: {
                let mut items = ev("path");
                items.push(ExecuteItem::Error {
                    message: "kaput <&>".to_owned(),
                    unsupported: false,
                });
                items
            },
        },
        Case {
            name: "gzip",
            target: "/.api/search/stream?q=foo&cm=t",
            accept: Some("gzip"),
            plan: ok(),
            items: ev("gzip"),
        },
    ]
}

fn field(headers: &http::HeaderMap, name: &str) -> String {
    match headers.get(name) {
        None => "-".to_owned(),
        Some(value) => format!("{value:?}"),
    }
}

fn gunzip(body: &[u8]) -> Vec<u8> {
    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).expect("gunzip");
    out
}

#[tokio::test]
async fn external_api_search_stream_harness() {
    if std::env::var("SG_SEARCH_STREAM_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let events = events();
    for case in cases(&events) {
        let deps = Arc::new(FakeDeps {
            plan: case.plan.clone(),
            items: Mutex::new(case.items),
            metadata: repo_meta(),
            names: repo_meta()
                .into_iter()
                .map(|(id, metadata)| (id, metadata.name))
                .collect(),
        });
        let mut builder = Request::builder().method(Method::GET).uri(case.target);
        if let Some(accept) = case.accept {
            builder = builder.header(header::ACCEPT_ENCODING, accept);
        }
        let request = builder.body(()).expect("request");
        let search_deps: Arc<dyn SearchDeps> = deps;
        let response = search_response(&request, search_deps).await;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = if headers.get(header::CONTENT_ENCODING).is_some() {
            gunzip(&body)
        } else {
            body.to_vec()
        };
        let body = String::from_utf8_lossy(&body);
        println!(
            "ss {} status={} ctype={} cache={} conn={} te={} xab={} vary={} ce={} body={body:?}",
            case.name,
            status,
            field(&headers, "content-type"),
            field(&headers, "cache-control"),
            field(&headers, "connection"),
            field(&headers, "transfer-encoding"),
            field(&headers, "x-accel-buffering"),
            field(&headers, "vary"),
            field(&headers, "content-encoding"),
        );
    }
}
