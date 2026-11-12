//! Differential harness for `sg_extsvc::batches_state`.
//!
//! Reads the shared corpus at `M922B_CORPUS` (default
//! `/home/developer/ws/test_ws/p9-go/data/batches_state/corpus.json`), runs the
//! ported Go logic, and writes normalized JSON to `M922B_OUT` (default
//! `/tmp/m922b_rust.json`). The Go oracle
//! (`zz_m922_state_oracle_test.go`) writes the same shape, so the two files can
//! be compared byte-for-byte.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::format_description::FormatItem;
use time::macros::format_description;
use time::OffsetDateTime;

use sg_extsvc::batches_state as bs;
use sg_extsvc::batches_state::{ChangesetMetadata, EventMetadata};

const GO_TIME: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    changeset: CChangeset,
    #[serde(default)]
    events: Vec<CEvent>,
    #[serde(default)]
    repo_archived: bool,
    #[serde(default)]
    counts: Option<CRange>,
    #[serde(default)]
    states_at: Vec<String>,
}

#[derive(Deserialize)]
struct CChangeset {
    id: i64,
    external_service_type: String,
    updated_at: String,
    #[serde(default)]
    external_deleted_at: String,
    metadata: Value,
}

#[derive(Deserialize)]
struct CEvent {
    changeset_id: i64,
    kind: String,
    metadata: Value,
}

#[derive(Deserialize)]
struct CRange {
    start: String,
    end: String,
}

// ---- JSON accessors mirroring the Go oracle's `cs/cb/ci/ca/co/cobj` ----

fn cs(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_string()
}

fn cb(v: &Value, k: &str) -> bool {
    v.get(k).and_then(Value::as_bool).unwrap_or(false)
}

fn ci(v: &Value, k: &str) -> i64 {
    v.get(k).and_then(Value::as_i64).unwrap_or(0)
}

fn arr<'a>(v: &'a Value, k: &str) -> &'a [Value] {
    v.get(k).and_then(Value::as_array).map_or(&[], |a| a.as_slice())
}

fn cobj<'a>(v: &'a Value, k: &str) -> Vec<&'a Value> {
    arr(v, k)
        .iter()
        .filter(|x| x.is_object())
        .collect()
}

fn ct(v: &Value, k: &str) -> OffsetDateTime {
    let raw = cs(v, k);
    OffsetDateTime::parse(&raw, &time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|e| panic!("bad time {raw:?}: {e}"))
        .to_offset(time::UtcOffset::UTC)
}

fn cms(v: &Value, k: &str) -> OffsetDateTime {
    let ms = ci(v, k);
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .expect("ms in range")
        .to_offset(time::UtcOffset::UTC)
}

fn strings(v: &Value, k: &str) -> Vec<String> {
    arr(v, k)
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

// ---- builders ----

fn build_metadata(v: &Value) -> ChangesetMetadata {
    match cs(v, "type").as_str() {
        "github_pr" => {
            let commits = cobj(v, "commits")
                .into_iter()
                .map(|c| bs::GithubCommitWithChecks {
                    oid: cs(c, "oid"),
                    committed_date: ct(c, "committed_date"),
                    contexts: cobj(c, "contexts")
                        .into_iter()
                        .map(|x| bs::GithubContext {
                            context: cs(x, "context"),
                            state: cs(x, "state"),
                        })
                        .collect(),
                    check_suites: cobj(c, "check_suites")
                        .into_iter()
                        .map(|s| bs::GithubCheckSuite {
                            id: cs(s, "id"),
                            status: cs(s, "status"),
                            conclusion: cs(s, "conclusion"),
                            runs: cobj(s, "runs")
                                .into_iter()
                                .map(|r| bs::GithubCheckRun {
                                    id: cs(r, "id"),
                                    status: cs(r, "status"),
                                    conclusion: cs(r, "conclusion"),
                                })
                                .collect(),
                        })
                        .collect(),
                })
                .collect();
            ChangesetMetadata::Github(bs::GithubPullRequest {
                is_draft: cb(v, "is_draft"),
                state: cs(v, "state"),
                review_decision: cs(v, "review_decision"),
                created_at: ct(v, "created_at"),
                labels: cobj(v, "labels")
                    .into_iter()
                    .map(|l| bs::ChangesetLabel {
                        name: cs(l, "name"),
                        color: cs(l, "color"),
                        description: cs(l, "description"),
                    })
                    .collect(),
                commits,
            })
        }
        "gitlab_mr" => {
            let head_pipeline = v
                .get("head_pipeline")
                .filter(|x| !x.is_null())
                .map(|hp| bs::GitlabPipeline {
                    status: cs(hp, "status"),
                    created_at: ct(hp, "created_at"),
                });
            ChangesetMetadata::Gitlab(bs::GitlabMergeRequest {
                state: cs(v, "state"),
                work_in_progress: cb(v, "work_in_progress"),
                created_at: ct(v, "created_at"),
                notes: cobj(v, "notes")
                    .into_iter()
                    .map(|n| bs::GitlabNote {
                        body: cs(n, "body"),
                        author: cs(n, "author"),
                        created_at: ct(n, "created_at"),
                        system: cb(n, "system"),
                    })
                    .collect(),
                pipelines: cobj(v, "pipelines")
                    .into_iter()
                    .map(|p| bs::GitlabPipeline {
                        status: cs(p, "status"),
                        created_at: ct(p, "created_at"),
                    })
                    .collect(),
                head_pipeline,
                labels: strings(v, "labels"),
            })
        }
        "bitbucketserver_pr" => ChangesetMetadata::BitbucketServer(bs::BitbucketServerPullRequest {
            state: cs(v, "state"),
            created_date: cms(v, "created_date_ms"),
            commits: cobj(v, "commits")
                .into_iter()
                .map(|c| bs::BitbucketServerCommit {
                    id: cs(c, "id"),
                    committer_timestamp: cms(c, "committer_timestamp_ms"),
                })
                .collect(),
            commit_status: cobj(v, "commit_status")
                .into_iter()
                .map(|st| bs::BitbucketServerCommitStatus {
                    commit: cs(st, "commit"),
                    key: cs(st, "key"),
                    state: cs(st, "state"),
                    date_added: cms(st, "date_added_ms"),
                })
                .collect(),
            reviewers: cobj(v, "reviewers")
                .into_iter()
                .map(|r| bs::BitbucketServerReviewer {
                    status: cs(r, "status"),
                })
                .collect(),
        }),
        "bitbucketcloud_pr" => ChangesetMetadata::BitbucketCloud(bs::BitbucketCloudPullRequest {
            state: cs(v, "state"),
            created_on: ct(v, "created_on"),
            statuses: cobj(v, "statuses")
                .into_iter()
                .map(|st| bs::BitbucketCloudStatus {
                    key: cs(st, "key"),
                    state: cs(st, "state"),
                })
                .collect(),
            participants: cobj(v, "participants")
                .into_iter()
                .map(|p| bs::BitbucketCloudParticipant {
                    author: cs(p, "author"),
                    state: cs(p, "state"),
                })
                .collect(),
        }),
        "azuredevops_pr" => ChangesetMetadata::AzureDevOps(bs::AzureDevOpsPullRequest {
            status: cs(v, "status"),
            is_draft: cb(v, "is_draft"),
            creation_date: ct(v, "creation_date"),
            statuses: cobj(v, "statuses")
                .into_iter()
                .map(|st| bs::AzureDevOpsStatus {
                    id: ci(st, "id") as i32,
                    state: cs(st, "state"),
                })
                .collect(),
            reviewers: cobj(v, "reviewers")
                .into_iter()
                .map(|r| bs::AzureDevOpsReviewer {
                    vote: ci(r, "vote") as i32,
                    unique_name: cs(r, "unique_name"),
                })
                .collect(),
        }),
        "gerrit_change" => {
            // Presence of the key (even null-vs-absent) distinguishes Go's nil
            // slice from an empty non-nil slice.
            let reviewers = match v.get("reviewers") {
                None => None,
                Some(Value::Null) => None,
                Some(Value::Array(items)) => Some(
                    items
                        .iter()
                        .filter(|x| x.is_object())
                        .map(|r| bs::GerritReviewer {
                            approvals: match r.get("approvals") {
                                Some(Value::Object(m)) => m
                                    .iter()
                                    .filter_map(|(k, val)| {
                                        val.as_str().map(|s| (k.clone(), s.to_string()))
                                    })
                                    .collect::<BTreeMap<_, _>>(),
                                _ => BTreeMap::new(),
                            },
                        })
                        .collect(),
                ),
                Some(_) => None,
            };
            ChangesetMetadata::Gerrit(bs::GerritChange {
                state: cs(v, "status"),
                work_in_progress: cb(v, "work_in_progress"),
                created: ct(v, "created"),
                reviewers,
                hashtags: strings(v, "hashtags"),
            })
        }
        "perforce_changelist" => ChangesetMetadata::Perforce(bs::PerforceChangelist {
            state: cs(v, "state"),
            creation_date: ct(v, "creation_date"),
        }),
        other => panic!("unknown changeset metadata type {other:?}"),
    }
}

fn build_event_metadata(v: &Value) -> EventMetadata {
    match cs(v, "type").as_str() {
        "github_commit_status" => EventMetadata::GithubCommitStatus {
            sha: cs(v, "sha"),
            context: cs(v, "context"),
            state: cs(v, "state"),
            received_at: ct(v, "received_at"),
        },
        "github_pull_request_commit" => EventMetadata::GithubPullRequestCommit {
            oid: cs(v, "oid"),
            committed_date: ct(v, "committed_date"),
        },
        "github_check_suite" => EventMetadata::GithubCheckSuite {
            id: cs(v, "id"),
            status: cs(v, "status"),
            conclusion: cs(v, "conclusion"),
            received_at: ct(v, "received_at"),
            runs: cobj(v, "runs")
                .into_iter()
                .map(|r| bs::GithubCheckRun {
                    id: cs(r, "id"),
                    status: cs(r, "status"),
                    conclusion: cs(r, "conclusion"),
                })
                .collect(),
        },
        "github_check_run" => EventMetadata::GithubCheckRun {
            id: cs(v, "id"),
            status: cs(v, "status"),
            conclusion: cs(v, "conclusion"),
            received_at: ct(v, "received_at"),
        },
        "github_label" => {
            let l = v.get("label").cloned().unwrap_or(Value::Null);
            EventMetadata::GithubLabel {
                label: bs::ChangesetLabel {
                    name: cs(&l, "name"),
                    color: cs(&l, "color"),
                    description: cs(&l, "description"),
                },
                created_at: ct(v, "created_at"),
                removed: cb(v, "removed"),
            }
        }
        "github_review" => EventMetadata::GithubReview {
            author: cs(v, "author"),
            state: cs(v, "state"),
            updated_at: ct(v, "updated_at"),
        },
        "github_review_dismissed" => EventMetadata::GithubReviewDismissed {
            author: cs(v, "author"),
            created_at: ct(v, "created_at"),
        },
        "github_closed" | "github_merged" | "github_reopened" | "github_ready_for_review"
        | "github_convert_to_draft" => EventMetadata::GithubCreated {
            created_at: ct(v, "created_at"),
        },
        "gitlab_pipeline" => EventMetadata::GitlabPipeline {
            status: cs(v, "status"),
            created_at: ct(v, "created_at"),
        },
        "gitlab_approved" | "gitlab_unapproved" | "gitlab_mark_wip" | "gitlab_unmark_wip" => {
            EventMetadata::GitlabReview {
                author: cs(v, "author"),
                created_at: ct(v, "created_at"),
            }
        }
        "gitlab_closed" | "gitlab_merged" | "gitlab_reopened" => EventMetadata::GitlabCreated {
            created_at: ct(v, "created_at"),
        },
        "bitbucketserver_activity" => EventMetadata::BitbucketServerActivity {
            author: cs(v, "author"),
            created_date: cms(v, "created_date_ms"),
        },
        "bitbucketserver_commit_status" => EventMetadata::BitbucketServerCommitStatus {
            commit: cs(v, "commit"),
            key: cs(v, "key"),
            state: cs(v, "state"),
            date_added: cms(v, "date_added_ms"),
        },
        "bitbucketcloud_commit_status_created" | "bitbucketcloud_commit_status_updated" => {
            EventMetadata::BitbucketCloudCommitStatus {
                key: cs(v, "key"),
                state: cs(v, "state"),
                created_on: ct(v, "created_on"),
                updated_on: ct(v, "updated_on"),
                updated: cs(v, "type") == "bitbucketcloud_commit_status_updated",
            }
        }
        "bitbucketcloud_approved" | "bitbucketcloud_unapproved" => {
            EventMetadata::BitbucketCloudApproval {
                author: cs(v, "author"),
                date: ct(v, "date"),
            }
        }
        "bitbucketcloud_changes_request_created" | "bitbucketcloud_changes_request_removed" => {
            EventMetadata::BitbucketCloudChangesRequest {
                author: cs(v, "author"),
                date: ct(v, "date"),
            }
        }
        "bitbucketcloud_fulfilled" | "bitbucketcloud_rejected" => {
            EventMetadata::BitbucketCloudUpdated {
                updated_on: ct(v, "updated_on"),
            }
        }
        "azuredevops_approved"
        | "azuredevops_approved_with_suggestions"
        | "azuredevops_waiting_for_author"
        | "azuredevops_rejected"
        | "azuredevops_merged" => EventMetadata::AzureDevOpsEvent {
            author: cs(v, "author"),
            created_date: ct(v, "created_date"),
        },
        other => panic!("unknown event metadata type {other:?}"),
    }
}

fn build_changeset(c: &CChangeset) -> bs::Changeset {
    bs::Changeset {
        id: c.id,
        external_service_type: c.external_service_type.clone(),
        updated_at: ct_str(&c.updated_at),
        external_deleted_at: if c.external_deleted_at.is_empty() {
            None
        } else {
            Some(ct_str(&c.external_deleted_at))
        },
        metadata: build_metadata(&c.metadata),
        external_check_state: None,
        external_state: None,
        external_review_state: None,
        published: false,
        sync_state: bs::ChangesetSyncState::default(),
        base_ref_oid: String::new(),
        base_ref: String::new(),
        head_ref_oid: String::new(),
        head_ref: String::new(),
        diff_stat: None,
    }
}

fn ct_str(raw: &str) -> OffsetDateTime {
    OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|e| panic!("bad time {raw:?}: {e}"))
        .to_offset(time::UtcOffset::UTC)
}

fn build_events(events: &[CEvent]) -> Vec<bs::ChangesetEvent> {
    events
        .iter()
        .map(|e| bs::ChangesetEvent {
            changeset_id: e.changeset_id,
            kind: e.kind.clone(),
            metadata: build_event_metadata(&e.metadata),
        })
        .collect()
}

// ---- outputs ----

#[derive(Serialize)]
struct OutResult {
    ok: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    value: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
}

#[derive(Serialize)]
struct OutLabel {
    name: String,
    color: String,
    description: String,
}

#[derive(Serialize)]
struct OutPoint {
    t: String,
    external_state: String,
    review_state: String,
}

#[derive(Serialize)]
struct OutHistory {
    ok: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    value: Vec<OutPoint>,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
}

#[derive(Serialize)]
struct OutStateAt {
    t: String,
    found: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    external_state: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    review_state: String,
}

#[derive(Serialize)]
struct OutCounts {
    time: String,
    total: i32,
    merged: i32,
    closed: i32,
    draft: i32,
    open: i32,
    open_approved: i32,
    open_changes_requested: i32,
    open_pending: i32,
}

#[derive(Serialize)]
struct OutCase {
    name: String,
    check_state: String,
    external_state: OutResult,
    review_state: OutResult,
    labels: Vec<OutLabel>,
    history: OutHistory,
    states_at: Vec<OutStateAt>,
    counts: Vec<OutCounts>,
}

fn fmt_time(t: OffsetDateTime) -> String {
    t.to_offset(time::UtcOffset::UTC)
        .format(GO_TIME)
        .expect("format")
}

fn point(p: bs::ChangesetStatesAtTime) -> OutPoint {
    OutPoint {
        t: fmt_time(p.t),
        external_state: p.external_state.as_db().to_string(),
        review_state: p.review_state.as_db().to_string(),
    }
}

fn ok_result(r: Result<String, String>) -> OutResult {
    match r {
        Ok(value) => OutResult {
            ok: true,
            value,
            error: String::new(),
        },
        Err(error) => OutResult {
            ok: false,
            value: String::new(),
            error,
        },
    }
}

fn run_case(c: &Case) -> OutCase {
    let ch = build_changeset(&c.changeset);
    let events = build_events(&c.events);

    // Mirror state.SetDerivedState: copy+sort before computing history.
    let mut sorted = events.clone();
    bs::sort_events(&mut sorted);

    let check_state = bs::compute_check_state(&ch, &sorted).as_db().to_string();

    let repo = bs::RepoInfo {
        name: String::new(),
        archived: c.repo_archived,
    };

    let (history, history_out) = match bs::compute_history(&ch, &sorted) {
        Ok(history) => {
            let out = OutHistory {
                ok: true,
                value: history.iter().copied().map(point).collect(),
                error: String::new(),
            };
            (history, out)
        }
        Err(error) => (
            Vec::new(),
            OutHistory {
                ok: false,
                value: Vec::new(),
                error,
            },
        ),
    };

    let external_state = ok_result(
        bs::compute_external_state(&ch, &history, &repo).map(|s| s.as_db().to_string()),
    );
    let review_state = ok_result(
        bs::compute_review_state(&ch, &history).map(|s| s.as_db().to_string()),
    );

    let labels = bs::compute_labels(Some(&ch), &sorted)
        .into_iter()
        .map(|l| OutLabel {
            name: l.name,
            color: l.color,
            description: l.description,
        })
        .collect();

    let states_at = c
        .states_at
        .iter()
        .map(|raw| {
            let t = ct_str(raw);
            let at = bs::states_at_time(&history, t);
            OutStateAt {
                t: fmt_time(t),
                found: at.is_some(),
                external_state: at.map_or_else(String::new, |s| s.external_state.as_db().to_string()),
                review_state: at.map_or_else(String::new, |s| s.review_state.as_db().to_string()),
            }
        })
        .collect();

    let counts = match &c.counts {
        Some(range) => {
            let start = ct_str(&range.start);
            let end = ct_str(&range.end);
            let counts = bs::calc_counts(start, end, std::slice::from_ref(&ch), &events)
                .expect("calc_counts");
            counts
                .into_iter()
                .map(|cc| OutCounts {
                    time: fmt_time(cc.time),
                    total: cc.total,
                    merged: cc.merged,
                    closed: cc.closed,
                    draft: cc.draft,
                    open: cc.open,
                    open_approved: cc.open_approved,
                    open_changes_requested: cc.open_changes_requested,
                    open_pending: cc.open_pending,
                })
                .collect()
        }
        None => Vec::new(),
    };

    OutCase {
        name: c.name.clone(),
        check_state,
        external_state,
        review_state,
        labels,
        history: history_out,
        states_at,
        counts,
    }
}

#[test]
fn batches_state_diff() {
    let corpus_path = std::env::var("M922B_CORPUS").unwrap_or_else(|_| {
        "/home/developer/ws/test_ws/p9-go/data/batches_state/corpus.json".to_string()
    });
    let out_path = std::env::var("M922B_OUT").unwrap_or_else(|_| "/tmp/m922b_rust.json".to_string());

    let data = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|e| panic!("read {corpus_path}: {e}"));
    let corpus: Corpus = serde_json::from_str(&data).expect("parse corpus");
    let results: Vec<OutCase> = corpus.cases.iter().map(run_case).collect();
    let mut encoded = serde_json::to_string_pretty(&results).expect("encode");
    encoded.push('\n');
    std::fs::write(&out_path, encoded).unwrap_or_else(|e| panic!("write {out_path}: {e}"));
    eprintln!("wrote {} cases to {out_path}", results.len());
}
