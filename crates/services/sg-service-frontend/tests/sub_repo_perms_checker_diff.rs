#![forbid(unsafe_code)]

//! M9.38c differential harness (Rust side): the sub-repository permission
//! checker (`subrepoperms.SubRepoPermsClient`) and
//! `jobutil.applySubRepoFiltering`.
//!
//! The Go oracle is `internal/search/job/jobutil/zz_m938c_oracle_test.go`
//! (UNTRACKED, gated by `SG_HARNESS_ON=1`). Both sides read
//! `test_ws/p9-go/data/sub_repo_perms_checker/cases.json`, read the real
//! `sub_repo_permissions` rows seeded by
//! `test_ws/p9-go/sub_repo_perms_checker_fixtures.sql`, and print identical
//! `sr ...` lines.
//!
//! Env: `SG_HARNESS_ON=1`, `SG_QTEST_DSN`, `M938C_CORPUS`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde::Deserialize;
use sqlx::postgres::PgPool;

use sg_service_frontend::search_jobutil_misc_jobs::{
    apply_sub_repo_filtering, Actor, NoopPermsLog,
};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MatchedString, MinimalRepo,
    RepoMatch, Signature, SymbolMatch,
};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::sub_repo_perms_checker::{
    FixtureGetter, SubRepoPermsClient, SubRepoPermissionsGetter,
};

#[derive(Debug, Default, Deserialize)]
struct Corpus {
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Debug, Default, Deserialize)]
struct ActorSpec {
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    internal: bool,
}

#[derive(Debug, Default, Deserialize)]
struct DiffSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    ranges: i32,
}

#[derive(Debug, Default, Deserialize)]
struct MatchSpec {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    repo_id: i32,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    modified_files: Vec<String>,
    #[serde(default)]
    diff: Option<DiffSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    actor: ActorSpec,
    #[serde(default)]
    matches: Vec<MatchSpec>,
}

fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '|' => out.push_str("\\|"),
            ',' => out.push_str("\\,"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

fn matches_text(matches: &[Match]) -> String {
    matches
        .iter()
        .map(|m| match m {
            Match::File(fm) => format!(
                "file|{}|{}",
                escape(&fm.file.repo.name),
                escape(&fm.file.path)
            ),
            Match::Commit(cm) => format!(
                "commit|{}|{}",
                escape(&cm.repo.name),
                escape(&cm.commit.id)
            ),
            Match::Repo(rm) => format!("repo|{}", escape(&rm.name)),
            Match::Owner(_) => "owner".to_owned(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn file_match(spec: &MatchSpec) -> Match {
    Match::File(FileMatch {
        file: FileRef {
            repo: MinimalRepo {
                id: spec.repo_id,
                name: spec.repo.clone(),
            },
            commit_id: String::new(),
            path: spec.path.clone(),
            input_rev: None,
            precise_language: String::new(),
        },
        chunk_matches: Vec::<ChunkMatch>::new(),
        symbols: Vec::<SymbolMatch>::new(),
        path_matches: Vec::new(),
        limit_hit: false,
        debug: None,
    })
}

fn commit_match(spec: &MatchSpec) -> Match {
    let diff_preview = spec.diff.as_ref().map(|diff| MatchedString {
        content: diff.content.clone(),
        matched_ranges: (0..diff.ranges.max(0))
            .map(|_| Range {
                start: Location::default(),
                end: Location::default(),
            })
            .collect(),
    });
    Match::Commit(CommitMatch {
        commit: GitCommit {
            id: spec.commit.clone(),
            message: String::new(),
            author: Signature {
                name: String::new(),
                email: String::new(),
                date: time::OffsetDateTime::UNIX_EPOCH,
            },
            committer: Signature {
                name: String::new(),
                email: String::new(),
                date: time::OffsetDateTime::UNIX_EPOCH,
            },
            parents: Vec::new(),
        },
        repo: MinimalRepo {
            id: spec.repo_id,
            name: spec.repo.clone(),
        },
        message_preview: None,
        diff_preview,
        refs: Vec::new(),
        source_refs: Vec::new(),
        diff: Vec::new(),
        modified_files: spec.modified_files.clone(),
    })
}

fn repo_match(spec: &MatchSpec) -> Match {
    Match::Repo(RepoMatch {
        name: spec.repo.clone(),
        id: spec.repo_id,
        rev: String::new(),
        description_matches: Vec::new(),
        repo_name_matches: Vec::new(),
    })
}

fn build_match(spec: &MatchSpec) -> Match {
    match spec.kind.as_str() {
        "file" => file_match(spec),
        "commit" => commit_match(spec),
        "repo" => repo_match(spec),
        other => panic!("unknown match kind {other}"),
    }
}

fn actor(spec: &ActorSpec) -> Actor {
    Actor {
        uid: spec.uid,
        internal: spec.internal,
        mock_user: false,
    }
}

async fn fixture_getter(pool: &PgPool, cases: &[Case]) -> FixtureGetter {
    let mut users: HashSet<i32> = HashSet::new();
    let mut repo_ids: HashSet<i32> = HashSet::new();
    for case in cases {
        users.insert(case.actor.uid);
        for m in &case.matches {
            repo_ids.insert(m.repo_id);
        }
    }
    let mut getter = FixtureGetter::default();
    for user_id in users {
        let rules = sg_db::sub_repo_perms::get_by_user_with_ips(pool, user_id, false, true)
            .await
            .expect("get_by_user_with_ips");
        let mut by_repo = BTreeMap::new();
        for (repo, perms) in rules {
            by_repo.insert(repo, perms.paths.into_iter().map(|p| p.path).collect());
        }
        getter.rules.insert(user_id, by_repo);
    }
    for repo_id in repo_ids {
        if sg_db::sub_repo_perms::repo_id_supported(pool, repo_id)
            .await
            .expect("repo_id_supported")
        {
            getter.supported_repo_ids.insert(repo_id);
        }
    }
    getter
}

#[tokio::test]
async fn sub_repo_perms_checker_diff() {
    if std::env::var("SG_HARNESS_ON").as_deref() != Ok("1") {
        return;
    }
    let Ok(corpus_path) = std::env::var("M938C_CORPUS") else {
        return;
    };
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let base_getter = fixture_getter(&pool, &corpus.cases).await;

    for case in &corpus.cases {
        let getter: Arc<dyn SubRepoPermissionsGetter> = Arc::new(base_getter.clone());
        let checker = SubRepoPermsClient::new(case.enabled, getter);
        let matches: Vec<Match> = case.matches.iter().map(build_match).collect();
        let (filtered, err) = apply_sub_repo_filtering(
            &checker,
            &actor(&case.actor),
            &HashMap::new(),
            &mut NoopPermsLog,
            matches,
        );
        let error = err.map(|e| e.to_string()).unwrap_or_else(|| "none".to_owned());
        println!(
            "sr case={} matches={} error={}",
            case.name,
            matches_text(&filtered),
            escape(&error)
        );
    }
}
