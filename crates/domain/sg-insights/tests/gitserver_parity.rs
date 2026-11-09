// M9.8ci differential harness for `sg_insights::gitserver`.
//
// Prints canonical `gs ` lines for the corpus in
// test_ws/p9-go/data/insights_gitserver_corpus.txt; the Go baseline prints
// the same lines from internal/insights/gitserver/parity_harness_test.go
// (driving the real GitCommitClient against gitserver.MockClient).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use sg_git::{Commit, CommitsOptions, CommitsOrder};
use sg_insights::gitserver::{CommitError, GitCommitClient, GitserverCommitSource};
use time::format_description::well_known::Rfc3339;
use time::macros::datetime;
use time::OffsetDateTime;
use tokio::sync::Mutex;

const REPO: &str = "myrepo";
const TARGET: OffsetDateTime = datetime!(2021-01-01 0:00 UTC);

fn commit(oid: &str, committer_date: Option<OffsetDateTime>) -> Commit {
    Commit {
        oid: oid.to_owned(),
        parents: Vec::new(),
        author_name: String::new(),
        author_email: String::new(),
        author_date: None,
        committer_date: committer_date.map(timestamp),
        message: String::new(),
    }
}

fn timestamp(value: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond() as i32,
    }
}

fn timestamp_string(value: prost_types::Timestamp) -> String {
    let time = OffsetDateTime::from_unix_timestamp(value.seconds)
        .expect("valid unix timestamp")
        .replace_nanosecond(value.nanos.max(0) as u32)
        .expect("valid nanoseconds");
    time.format(&Rfc3339).expect("format time")
}

struct FakeSource {
    scene: String,
    first_commit_calls: AtomicUsize,
    last_options: Mutex<Option<CommitsOptions>>,
    last_repo: Mutex<String>,
}

#[async_trait]
impl GitserverCommitSource for FakeSource {
    async fn commits(
        &self,
        repo: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<Commit>, CommitError> {
        *self.last_options.lock().await = Some(options.clone());
        *self.last_repo.lock().await = repo.to_owned();
        if self.scene == "recent-error" {
            return Err(CommitError::Other("boom".to_owned()));
        }
        Ok(vec![commit("111", None), commit("222", None)])
    }

    async fn first_ever_commit(&self, _repo: &str) -> Result<Commit, CommitError> {
        let calls = self.first_commit_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.scene == "first-error-then-ok" && calls == 1 {
            return Err(CommitError::Other("boom".to_owned()));
        }
        Ok(commit("first-commit", Some(datetime!(2020-05-01 0:00 UTC))))
    }
}

fn order_name(order: CommitsOrder) -> &'static str {
    match order {
        CommitsOrder::CommitDate => "commit-date",
        CommitsOrder::TopoDate => "topo-date",
        CommitsOrder::Default => "default",
    }
}

async fn run_recent_scene(label: &str, scene: &str) {
    let source = Arc::new(FakeSource {
        scene: scene.to_owned(),
        first_commit_calls: AtomicUsize::new(0),
        last_options: Mutex::new(None),
        last_repo: Mutex::new(String::new()),
    });
    let client = GitCommitClient::new(source.clone());

    let revision = if scene == "recent-rev" { "abc123" } else { "" };
    let result = client.recent_commits(REPO, TARGET, revision).await;

    let options = source
        .last_options
        .lock()
        .await
        .clone()
        .expect("Commits was called");
    let repo = source.last_repo.lock().await.clone();
    let before = options
        .before
        .map(timestamp_string)
        .unwrap_or_else(|| "-".to_owned());
    let order = order_name(options.order.unwrap_or(CommitsOrder::Default));
    let ranges = options.ranges.join(",");
    let commits = match &result {
        Ok(commits) if !commits.is_empty() => commits
            .iter()
            .map(|commit| commit.oid.clone())
            .collect::<Vec<_>>()
            .join(","),
        _ => "-".to_owned(),
    };
    let err = match &result {
        Ok(_) => "none".to_owned(),
        Err(error) => error.to_string(),
    };
    println!(
        "gs recent {label} repo={repo} n={} before={before} order={order} ranges={ranges} commits={commits} err={err}",
        options.max_commits
    );
}

async fn run_first_scene(label: &str, scene: &str) {
    let source = Arc::new(FakeSource {
        scene: scene.to_owned(),
        first_commit_calls: AtomicUsize::new(0),
        last_options: Mutex::new(None),
        last_repo: Mutex::new(String::new()),
    });
    let client = GitCommitClient::new(source.clone());

    for call in 0..2 {
        let result = client.first_commit(REPO).await;
        let fetches = source.first_commit_calls.load(Ordering::SeqCst);
        let (id, committer, err) = match result {
            Ok(commit) => (
                commit.oid,
                commit
                    .committer_date
                    .map(timestamp_string)
                    .unwrap_or_else(|| "-".to_owned()),
                "none".to_owned(),
            ),
            Err(error) => ("-".to_owned(), "-".to_owned(), error.to_string()),
        };
        println!(
            "gs first {label} call={call} repo={REPO} fetches={fetches} id={id} committer={committer} err={err}"
        );
    }
}

fn run_empty_scene(label: &str, scene: &str) {
    let error = match scene {
        "empty-other" => CommitError::Other("A different error".to_owned()),
        "empty-revision" => CommitError::RevisionNotFound {
            repo: "foo".to_owned(),
            spec: "HEAD".to_owned(),
        },
        "empty-wrapped-revision" => CommitError::wrapped(
            "nice!",
            CommitError::RevisionNotFound {
                repo: "foo".to_owned(),
                spec: "HEAD".to_owned(),
            },
        ),
        "empty-wrapped-other" => CommitError::wrapped(
            "Another message",
            CommitError::Other("Not an empty repo".to_owned()),
        ),
        other => panic!("unknown scene {other}"),
    };
    println!(
        "gs empty {label} repo=foo result={}",
        sg_insights::gitserver::is_empty_repo_error(&error)
    );
}

#[tokio::test]
async fn gitserver_parity() {
    if std::env::var("SG_HARNESS_ON").as_deref() != Ok("1") {
        eprintln!("set SG_HARNESS_ON=1 to run the insights gitserver parity harness");
        return;
    }
    let corpus = std::env::var("SG_INSIGHTS_GITSERVER_CORPUS")
        .expect("SG_INSIGHTS_GITSERVER_CORPUS is required");
    let contents = std::fs::read_to_string(&corpus)
        .unwrap_or_else(|err| panic!("read corpus {corpus}: {err}"));

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split('\t');
        let label = parts.next().expect("label");
        let scene = parts.next().expect("scene");
        match scene {
            "recent-head" | "recent-rev" | "recent-error" => run_recent_scene(label, scene).await,
            "first-cached" | "first-error-then-ok" => run_first_scene(label, scene).await,
            _ => run_empty_scene(label, scene),
        }
    }
}
