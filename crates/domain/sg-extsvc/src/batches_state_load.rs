#![forbid(unsafe_code)]

//! Production loader for [`super::batches_state`]: the SQL side of
//! `internal/batches/store` that feeds `state.SetDerivedState`.
//!
//! The baseline loads a `btypes.Changeset` (`ScanChangeset`), its repository
//! (`repos.Get`) and its events (`ListChangesetEvents`, `ORDER BY id ASC`)
//! and then calls `state.SetDerivedState`. This module reproduces the decode
//! of `changesets.metadata`, `changeset_events.metadata` (via
//! `btypes.NewChangesetEventMetadata`), `sync_state` and the repo fields, and
//! writes the derived columns back with the same `SetDerivedState` +
//! `UpdateChangesetCodeHostState` semantics:
//!
//! * `updated_at = clock_timestamp()` (`s.now()`),
//! * `external_state`, `external_review_state`, `external_check_state`,
//! * `diff_stat_added`, `diff_stat_deleted`,
//! * `sync_state`.
//!
//! No schema change is involved: every column already exists. Metadata,
//! title and fork columns are owned by the code-host source step and are not
//! rewritten here.
//!
//! Gitserver stays injectable. Without a client the pure, network-independent
//! state is still computed and persisted; the sync-state/diffstat step is
//! skipped exactly where the baseline logs a `computeSyncState` error.

use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};
use time::format_description::FormatItem;
use time::macros::format_description;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, PrimitiveDateTime};

use super::batches_state::{
    compute_labels, go_zero_time, set_derived_state, Changeset, ChangesetCheckState,
    ChangesetEvent, ChangesetExternalState, ChangesetLabel, ChangesetMetadata,
    ChangesetReviewState, ChangesetSyncState, DiffStat, EventMetadata, GitserverClient,
    GithubCheckRun, GithubCheckSuite, GithubCommitWithChecks, GithubContext, GithubPullRequest,
    GitlabMergeRequest, GitlabNote, GitlabPipeline, RepoInfo, AzureDevOpsPullRequest,
    AzureDevOpsReviewer, AzureDevOpsStatus, BitbucketCloudParticipant, BitbucketCloudPullRequest,
    BitbucketCloudStatus, BitbucketServerCommit, BitbucketServerCommitStatus,
    BitbucketServerPullRequest, BitbucketServerReviewer, GerritChange, GerritReviewer,
    PerforceChangelist,
};

/// The PostgreSQL instant format used by the Rust `repo`/`changesets` schema.
///
/// `go_zero_time()` is used for absent/zero Go times.

/// A loaded `changesets` row plus its repository and events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedChangeset {
    pub changeset: Changeset,
    pub events: Vec<ChangesetEvent>,
    pub repo: RepoInfo,
}

/// The derived state computed from [`LoadedChangeset`], mirroring the fields
/// `SetDerivedState` mutates plus the labels (`ComputeLabels`) derived from the
/// same inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedState {
    pub changeset_id: i64,
    pub external_state: Option<ChangesetExternalState>,
    pub external_review_state: Option<ChangesetReviewState>,
    pub external_check_state: Option<ChangesetCheckState>,
    pub sync_state: ChangesetSyncState,
    pub diff_stat: Option<DiffStat>,
    pub labels: Vec<ChangesetLabel>,
}

// ---------------------------------------------------------------------------
// Time decoding (Go `time.Time` JSON formatting)
// ---------------------------------------------------------------------------

const GITLAB_ALT_NO_ZONE: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

const GITLAB_ALT_OFFSET: &[FormatItem<'static>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour sign:mandatory][offset_minute]"
);

/// `gerrit.Change.UnmarshalJSON`: `2006-01-02 15:04:05.000000000`, no zone.
const GERRIT_TIME: &[FormatItem<'static>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"
);

/// Decode a timestamp the way the relevant Go unmarshaler would.
///
/// `time.Time` (and `gitlab.Time`'s fallback formats) accept RFC3339 first;
/// GitLab additionally accepts `2006-01-02 15:04:05 MST` and
/// `2006-01-02 15:04:05 -0700`. A value that cannot be decoded becomes the Go
/// zero time, matching a `null` unmarshalled into a `time.Time`.
pub fn parse_go_time(raw: &str) -> OffsetDateTime {
    if let Ok(t) = OffsetDateTime::parse(raw, &Rfc3339) {
        return t;
    }
    if let Some(rest) = raw.strip_suffix(" UTC") {
        if let Ok(t) = PrimitiveDateTime::parse(rest, GITLAB_ALT_NO_ZONE) {
            return t.assume_utc();
        }
    }
    if let Ok(t) = OffsetDateTime::parse(raw, GITLAB_ALT_OFFSET) {
        return t;
    }
    if let Ok(t) = PrimitiveDateTime::parse(raw, GERRIT_TIME) {
        return t.assume_utc();
    }
    if let Ok(t) = PrimitiveDateTime::parse(raw, GITLAB_ALT_NO_ZONE) {
        return t.assume_utc();
    }
    go_zero_time()
}

fn time_value(value: Option<&Value>) -> OffsetDateTime {
    match value {
        Some(Value::String(raw)) => parse_go_time(raw),
        _ => go_zero_time(),
    }
}

fn time_at(value: &Value, pointer: &str) -> OffsetDateTime {
    time_value(value.pointer(pointer))
}

fn string_at(value: &Value, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn bool_at(value: &Value, pointer: &str) -> bool {
    value
        .pointer(pointer)
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn i32_at(value: &Value, pointer: &str) -> i32 {
    value
        .pointer(pointer)
        .and_then(Value::as_i64)
        .map(|n| n as i32)
        .unwrap_or_default()
}

fn array_at<'a>(value: &'a Value, pointer: &str) -> &'a [Value] {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn opt_array_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a [Value]> {
    value.pointer(pointer).and_then(Value::as_array).map(Vec::as_slice)
}

/// Decode a Bitbucket Server millisecond epoch (`unixMilliToTime`).
fn unix_millis(value: Option<&Value>) -> OffsetDateTime {
    let millis = value
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
        })
        .unwrap_or_default();
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
        .unwrap_or_else(|_| go_zero_time())
}

fn millis_at(value: &Value, pointer: &str) -> OffsetDateTime {
    unix_millis(value.pointer(pointer))
}

// ---------------------------------------------------------------------------
// Changeset metadata (`ScanChangeset`)
// ---------------------------------------------------------------------------

fn parse_labels(value: &Value, pointer: &str) -> Vec<ChangesetLabel> {
    array_at(value, pointer)
        .iter()
        .map(|label| ChangesetLabel {
            name: string_at(label, "/Name"),
            color: string_at(label, "/Color"),
            description: string_at(label, "/Description"),
        })
        .collect()
}

fn parse_github_commit(node: &Value) -> GithubCommitWithChecks {
    let contexts = array_at(node, "/Commit/Status/Contexts")
        .iter()
        .map(|c| GithubContext {
            context: string_at(c, "/Context"),
            state: string_at(c, "/State"),
        })
        .collect();
    let check_suites = array_at(node, "/Commit/CheckSuites/Nodes")
        .iter()
        .map(|s| GithubCheckSuite {
            id: string_at(s, "/ID"),
            status: string_at(s, "/Status"),
            conclusion: string_at(s, "/Conclusion"),
            runs: array_at(s, "/CheckRuns/Nodes")
                .iter()
                .map(|r| GithubCheckRun {
                    id: string_at(r, "/ID"),
                    status: string_at(r, "/Status"),
                    conclusion: string_at(r, "/Conclusion"),
                })
                .collect(),
        })
        .collect();
    GithubCommitWithChecks {
        oid: string_at(node, "/Commit/OID"),
        committed_date: time_at(node, "/Commit/CommittedDate"),
        contexts,
        check_suites,
    }
}

fn parse_github_metadata(value: &Value) -> GithubPullRequest {
    GithubPullRequest {
        is_draft: bool_at(value, "/IsDraft"),
        state: string_at(value, "/State"),
        review_decision: string_at(value, "/ReviewDecision"),
        created_at: time_at(value, "/CreatedAt"),
        labels: parse_labels(value, "/Labels/Nodes"),
        commits: array_at(value, "/Commits/Nodes")
            .iter()
            .map(parse_github_commit)
            .collect(),
    }
}

fn parse_gitlab_pipeline(value: &Value) -> GitlabPipeline {
    GitlabPipeline {
        status: string_at(value, "/status"),
        created_at: time_at(value, "/created_at"),
    }
}

fn parse_gitlab_metadata(value: &Value) -> GitlabMergeRequest {
    GitlabMergeRequest {
        state: string_at(value, "/state"),
        work_in_progress: bool_at(value, "/work_in_progress"),
        created_at: time_at(value, "/created_at"),
        notes: array_at(value, "/Notes")
            .iter()
            .map(|note| GitlabNote {
                body: string_at(note, "/body"),
                author: string_at(note, "/author/username"),
                created_at: time_at(note, "/created_at"),
                system: bool_at(note, "/system"),
            })
            .collect(),
        pipelines: array_at(value, "/Pipelines")
            .iter()
            .map(parse_gitlab_pipeline)
            .collect(),
        head_pipeline: value
            .pointer("/head_pipeline")
            .filter(|v| !v.is_null())
            .map(parse_gitlab_pipeline),
        labels: array_at(value, "/labels")
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
    }
}

fn parse_bitbucketserver_metadata(value: &Value) -> BitbucketServerPullRequest {
    BitbucketServerPullRequest {
        state: string_at(value, "/state"),
        created_date: millis_at(value, "/createdDate"),
        commits: array_at(value, "/commits")
            .iter()
            .map(|c| BitbucketServerCommit {
                id: string_at(c, "/id"),
                committer_timestamp: millis_at(c, "/committerTimestamp"),
            })
            .collect(),
        commit_status: array_at(value, "/commit_status")
            .iter()
            .map(|s| BitbucketServerCommitStatus {
                commit: string_at(s, "/commit"),
                key: string_at(s, "/status/key"),
                state: string_at(s, "/status/state"),
                date_added: millis_at(s, "/status/dateAdded"),
            })
            .collect(),
        reviewers: array_at(value, "/reviewers")
            .iter()
            .map(|r| BitbucketServerReviewer {
                status: string_at(r, "/status"),
            })
            .collect(),
    }
}

fn parse_bitbucketcloud_metadata(value: &Value) -> BitbucketCloudPullRequest {
    BitbucketCloudPullRequest {
        state: string_at(value, "/state"),
        created_on: time_at(value, "/created_on"),
        // `AnnotatedPullRequest.Statuses` is untagged, so the JSON key is the
        // Go field name; each element keeps its snake_case tags.
        statuses: array_at(value, "/Statuses")
            .iter()
            .map(|s| BitbucketCloudStatus {
                key: string_at(s, "/key"),
                state: string_at(s, "/state"),
            })
            .collect(),
        participants: array_at(value, "/participants")
            .iter()
            .map(|p| BitbucketCloudParticipant {
                author: string_at(p, "/user/uuid"),
                state: string_at(p, "/state"),
            })
            .collect(),
    }
}

fn parse_azuredevops_metadata(value: &Value) -> AzureDevOpsPullRequest {
    AzureDevOpsPullRequest {
        status: string_at(value, "/status"),
        is_draft: bool_at(value, "/isDraft"),
        creation_date: time_at(value, "/creationDate"),
        statuses: array_at(value, "/Statuses")
            .iter()
            .map(|s| AzureDevOpsStatus {
                id: i32_at(s, "/id"),
                state: string_at(s, "/state"),
            })
            .collect(),
        reviewers: array_at(value, "/reviewers")
            .iter()
            .map(|r| AzureDevOpsReviewer {
                vote: i32_at(r, "/vote"),
                unique_name: string_at(r, "/uniqueName"),
            })
            .collect(),
    }
}

fn parse_gerrit_metadata(value: &Value) -> GerritChange {
    let reviewers = opt_array_at(value, "/reviewers").map(|reviewers| {
        reviewers
            .iter()
            .map(|r| GerritReviewer {
                approvals: r
                    .pointer("/approvals")
                    .and_then(Value::as_object)
                    .map(|map| {
                        map.iter()
                            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned()))
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>()
    });
    GerritChange {
        state: string_at(value, "/change/status"),
        work_in_progress: bool_at(value, "/change/work_in_progress"),
        created: parse_go_time(&string_at(value, "/change/created")),
        reviewers,
        hashtags: array_at(value, "/change/hashtags")
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
    }
}

fn parse_perforce_metadata(value: &Value) -> PerforceChangelist {
    PerforceChangelist {
        state: string_at(value, "/State"),
        creation_date: time_at(value, "/CreationDate"),
    }
}

/// `ScanChangeset`'s metadata switch (`internal/batches/store/changesets.go`).
pub fn parse_changeset_metadata(
    external_service_type: &str,
    metadata: &Value,
) -> Result<ChangesetMetadata, String> {
    match external_service_type {
        "github" => Ok(ChangesetMetadata::Github(parse_github_metadata(metadata))),
        "bitbucketServer" => Ok(ChangesetMetadata::BitbucketServer(
            parse_bitbucketserver_metadata(metadata),
        )),
        "gitlab" => Ok(ChangesetMetadata::Gitlab(parse_gitlab_metadata(metadata))),
        "bitbucketCloud" => Ok(ChangesetMetadata::BitbucketCloud(
            parse_bitbucketcloud_metadata(metadata),
        )),
        "azuredevops" => Ok(ChangesetMetadata::AzureDevOps(
            parse_azuredevops_metadata(metadata),
        )),
        "gerrit" => Ok(ChangesetMetadata::Gerrit(parse_gerrit_metadata(metadata))),
        "perforce" => Ok(ChangesetMetadata::Perforce(parse_perforce_metadata(
            metadata,
        ))),
        other => Err(format!("unknown external service type: {other}")),
    }
}

/// Populate the flat ref fields from the metadata, mirroring
/// `Changeset.HeadRefOid`/`HeadRef`/`BaseRefOid`/`BaseRef`. `computeSyncState`
/// resolves these through gitserver; a code host that does not expose an OID
/// yields the empty string, exactly like the baseline.
fn apply_refs(changeset: &mut Changeset, metadata: &Value) {
    let prefixed = |name: &str| format!("refs/heads/{name}");
    let (head_ref_oid, head_ref, base_ref_oid, base_ref) =
        match changeset.external_service_type.as_str() {
            "github" => (
                string_at(metadata, "/HeadRefOid"),
                prefixed(&string_at(metadata, "/HeadRefName")),
                string_at(metadata, "/BaseRefOid"),
                prefixed(&string_at(metadata, "/BaseRefName")),
            ),
            "bitbucketServer" => (
                String::new(),
                string_at(metadata, "/fromRef/id"),
                String::new(),
                string_at(metadata, "/toRef/id"),
            ),
            "gitlab" => (
                string_at(metadata, "/diff_refs/head_sha"),
                prefixed(&string_at(metadata, "/source_branch")),
                string_at(metadata, "/diff_refs/base_sha"),
                prefixed(&string_at(metadata, "/target_branch")),
            ),
            "bitbucketCloud" => (
                string_at(metadata, "/source/commit/hash"),
                prefixed(&string_at(metadata, "/source/branch/name")),
                string_at(metadata, "/destination/commit/hash"),
                prefixed(&string_at(metadata, "/destination/branch/name")),
            ),
            "azuredevops" => (
                String::new(),
                string_at(metadata, "/sourceRefName"),
                String::new(),
                string_at(metadata, "/targetRefName"),
            ),
            "gerrit" => (
                String::new(),
                String::new(),
                String::new(),
                prefixed(&string_at(metadata, "/change/branch")),
            ),
            // Perforce exposes no refs.
            _ => (String::new(), String::new(), String::new(), String::new()),
        };
    changeset.head_ref_oid = head_ref_oid;
    changeset.head_ref = head_ref;
    changeset.base_ref_oid = base_ref_oid;
    changeset.base_ref = base_ref;
}

fn parse_sync_state(value: &Value) -> ChangesetSyncState {
    ChangesetSyncState {
        base_ref_oid: string_at(value, "/BaseRefOid"),
        head_ref_oid: string_at(value, "/HeadRefOid"),
        is_complete: bool_at(value, "/IsComplete"),
    }
}

// ---------------------------------------------------------------------------
// Event metadata (`NewChangesetEventMetadata`)
// ---------------------------------------------------------------------------

fn parse_github_event(kind: &str, value: &Value) -> Result<EventMetadata, String> {
    Ok(match kind {
        "github:assigned"
        | "github:closed"
        | "github:renamed"
        | "github:merged"
        | "github:reopened"
        | "github:review_request_removed"
        | "github:review_requested"
        | "github:ready_for_review"
        | "github:convert_to_draft"
        | "github:unassigned" => EventMetadata::GithubCreated {
            created_at: time_at(value, "/CreatedAt"),
        },
        "github:commented" | "github:review_commented" => EventMetadata::GithubUpdated {
            updated_at: time_at(value, "/UpdatedAt"),
        },
        "github:reviewed" => EventMetadata::GithubReview {
            author: string_at(value, "/Author/Login"),
            state: string_at(value, "/State"),
            updated_at: time_at(value, "/UpdatedAt"),
        },
        "github:review_dismissed" => EventMetadata::GithubReviewDismissed {
            author: string_at(value, "/Review/Author/Login"),
            created_at: time_at(value, "/CreatedAt"),
        },
        "github:commit" => EventMetadata::GithubPullRequestCommit {
            oid: string_at(value, "/Commit/OID"),
            committed_date: time_at(value, "/Commit/CommittedDate"),
        },
        "github:labeled" | "github:unlabeled" => EventMetadata::GithubLabel {
            label: ChangesetLabel {
                name: string_at(value, "/Label/Name"),
                color: string_at(value, "/Label/Color"),
                description: string_at(value, "/Label/Description"),
            },
            created_at: time_at(value, "/CreatedAt"),
            removed: kind == "github:unlabeled" || bool_at(value, "/Removed"),
        },
        "github:commit_status" => EventMetadata::GithubCommitStatus {
            sha: string_at(value, "/SHA"),
            context: string_at(value, "/Context"),
            state: string_at(value, "/State"),
            received_at: time_at(value, "/ReceivedAt"),
        },
        "github:check_suite" => EventMetadata::GithubCheckSuite {
            id: string_at(value, "/ID"),
            status: string_at(value, "/Status"),
            conclusion: string_at(value, "/Conclusion"),
            received_at: time_at(value, "/ReceivedAt"),
            runs: array_at(value, "/CheckRuns/Nodes")
                .iter()
                .map(|r| GithubCheckRun {
                    id: string_at(r, "/ID"),
                    status: string_at(r, "/Status"),
                    conclusion: string_at(r, "/Conclusion"),
                })
                .collect(),
        },
        "github:check_run" => EventMetadata::GithubCheckRun {
            id: string_at(value, "/ID"),
            status: string_at(value, "/Status"),
            conclusion: string_at(value, "/Conclusion"),
            received_at: time_at(value, "/ReceivedAt"),
        },
        other => return Err(format!("unknown github changeset event kind {other:?}")),
    })
}

fn parse_gitlab_event(kind: &str, value: &Value) -> Result<EventMetadata, String> {
    Ok(match kind {
        "gitlab:approved" | "gitlab:unapproved" | "gitlab:mark_wip" | "gitlab:unmark_wip" => {
            EventMetadata::GitlabReview {
                author: string_at(value, "/author/username"),
                created_at: time_at(value, "/created_at"),
            }
        }
        "gitlab:pipeline" => EventMetadata::GitlabPipeline {
            status: string_at(value, "/status"),
            // The event row's `created_at` is filled in by the loader below.
            created_at: time_at(value, "/created_at"),
        },
        "gitlab:closed" | "gitlab:merged" | "gitlab:reopened" => EventMetadata::GitlabCreated {
            created_at: time_at(value, "/created_at"),
        },
        other => return Err(format!("unknown gitlab changeset event kind {other:?}")),
    })
}

fn parse_bitbucketserver_event(kind: &str, value: &Value) -> Result<EventMetadata, String> {
    Ok(match kind {
        "bitbucketserver:commit_status" => EventMetadata::BitbucketServerCommitStatus {
            commit: string_at(value, "/commit"),
            key: string_at(value, "/status/key"),
            state: string_at(value, "/status/state"),
            date_added: millis_at(value, "/status/dateAdded"),
        },
        "bitbucketserver:participant_status:unapproved" => EventMetadata::BitbucketServerActivity {
            author: string_at(value, "/user/name"),
            created_date: millis_at(value, "/createdDate"),
        },
        _ if kind.starts_with("bitbucketserver:") => EventMetadata::BitbucketServerActivity {
            author: string_at(value, "/user/name"),
            created_date: millis_at(value, "/createdDate"),
        },
        other => return Err(format!("unknown bitbucketserver changeset event kind {other:?}")),
    })
}

fn parse_bitbucketcloud_event(kind: &str, value: &Value) -> Result<EventMetadata, String> {
    Ok(match kind {
        "bitbucketcloud:approved" | "bitbucketcloud:reviewed" => {
            EventMetadata::BitbucketCloudApproval {
                author: string_at(value, "/user/uuid"),
                date: time_at(value, "/participated_on"),
            }
        }
        "bitbucketcloud:changes_requested" => EventMetadata::BitbucketCloudChangesRequest {
            author: string_at(value, "/user/uuid"),
            date: time_at(value, "/participated_on"),
        },
        "bitbucketcloud:commit_status" => EventMetadata::BitbucketCloudCommitStatus {
            key: string_at(value, "/key"),
            state: string_at(value, "/state"),
            created_on: time_at(value, "/created_on"),
            updated_on: time_at(value, "/updated_on"),
            updated: false,
        },
        "bitbucketcloud:pullrequest:approved" | "bitbucketcloud:pullrequest:unapproved" => {
            EventMetadata::BitbucketCloudApproval {
                author: string_at(value, "/approval/user/uuid"),
                date: time_at(value, "/approval/date"),
            }
        }
        "bitbucketcloud:pullrequest:changes_request_created"
        | "bitbucketcloud:pullrequest:changes_request_removed" => {
            EventMetadata::BitbucketCloudChangesRequest {
                author: string_at(value, "/changes_request/user/uuid"),
                date: time_at(value, "/changes_request/date"),
            }
        }
        "bitbucketcloud:pullrequest:comment_created"
        | "bitbucketcloud:pullrequest:comment_deleted"
        | "bitbucketcloud:pullrequest:comment_updated" => {
            EventMetadata::BitbucketCloudComment {
                created_on: time_at(value, "/comment/created_on"),
                updated_on: time_at(value, "/comment/updated_on"),
                updated: kind != "bitbucketcloud:pullrequest:comment_created",
            }
        }
        "bitbucketcloud:pullrequest:fulfilled"
        | "bitbucketcloud:pullrequest:rejected"
        | "bitbucketcloud:pullrequest:updated" => EventMetadata::BitbucketCloudUpdated {
            updated_on: time_at(value, "/pullrequest/updated_on"),
        },
        "bitbucketcloud:repo:commit_status_created"
        | "bitbucketcloud:repo:commit_status_updated" => {
            EventMetadata::BitbucketCloudCommitStatus {
                key: string_at(value, "/commit_status/key"),
                state: string_at(value, "/commit_status/state"),
                created_on: time_at(value, "/commit_status/created_on"),
                updated_on: time_at(value, "/commit_status/updated_on"),
                updated: kind == "bitbucketcloud:repo:commit_status_updated",
            }
        }
        other => return Err(format!("unknown bitbucketcloud changeset event kind {other:?}")),
    })
}

fn azure_author(value: &Value) -> String {
    let reviewers = array_at(value, "/resource/reviewers");
    match reviewers.last() {
        Some(reviewer) => string_at(reviewer, "/uniqueName"),
        None => string_at(value, "/resource/createdBy/uniqueName"),
    }
}

fn parse_azuredevops_event(kind: &str, value: &Value) -> Result<EventMetadata, String> {
    Ok(match kind {
        "azuredevops:pullrequest:build_succeeded"
        | "azuredevops:pullrequest:build_failed"
        | "azuredevops:pullrequest:build_error"
        | "azuredevops:pullrequest:build_pending" => EventMetadata::Zero,
        "azuredevops:pullrequest:merged"
        | "azuredevops:pullrequest:updated"
        | "azuredevops:pullrequest:approved"
        | "azuredevops:pullrequest:approved_with_suggestions"
        | "azuredevops:pullrequest:reviewed"
        | "azuredevops:pullrequest:waiting_for_author"
        | "azuredevops:pullrequest:rejected" => EventMetadata::AzureDevOpsEvent {
            author: azure_author(value),
            created_date: time_at(value, "/createdDate"),
        },
        other => return Err(format!("unknown azuredevops changeset event kind {other:?}")),
    })
}

fn parse_gerrit_event(kind: &str, _value: &Value) -> Result<EventMetadata, String> {
    match kind {
        "gerrit:change:approved"
        | "gerrit:change:approved_with_suggestions"
        | "gerrit:change:reviewed"
        | "gerrit:change:needs_changes"
        | "gerrit:change:rejected"
        | "gerrit:change:build_failed"
        | "gerrit:change:build_pending"
        | "gerrit:change:build_succeeded" => Ok(EventMetadata::Zero),
        other => Err(format!("unknown gerrit changeset event kind {other:?}")),
    }
}

/// `btypes.NewChangesetEventMetadata` + `json.Unmarshal` for the event row.
///
/// `row_created_at` is the `changeset_events.created_at` column, which the
/// baseline's `*gitlabwebhooks.PipelineEvent` timestamp case returns.
pub fn parse_event_metadata(
    kind: &str,
    metadata: &Value,
    row_created_at: OffsetDateTime,
) -> Result<EventMetadata, String> {
    if kind.starts_with("bitbucketcloud") {
        return parse_bitbucketcloud_event(kind, metadata);
    }
    if kind.starts_with("bitbucketserver") {
        return parse_bitbucketserver_event(kind, metadata);
    }
    if kind.starts_with("github") {
        return parse_github_event(kind, metadata);
    }
    if kind.starts_with("gitlab") {
        let parsed = parse_gitlab_event(kind, metadata)?;
        // The baseline uses the event row's created_at for pipeline events.
        if kind == "gitlab:pipeline" {
            if let EventMetadata::GitlabPipeline { status, .. } = parsed {
                return Ok(EventMetadata::GitlabPipeline {
                    status,
                    created_at: row_created_at,
                });
            }
        }
        return Ok(parsed);
    }
    if kind.starts_with("azuredevops") {
        return parse_azuredevops_event(kind, metadata);
    }
    if kind.starts_with("gerrit") {
        return parse_gerrit_event(kind, metadata);
    }
    Err(format!("changeset event metadata unknown changeset event kind {kind:?}"))
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/// Load `GetChangeset` + `ListChangesetEvents` (`ORDER BY id ASC`).
pub async fn load_derived_inputs(
    conn: &mut PgConnection,
    changeset_id: i64,
) -> Result<Option<LoadedChangeset>, String> {
    let row = sqlx::query(
        r#"
SELECT c.id,
       c.external_service_type,
       c.updated_at,
       c.external_deleted_at,
       c.metadata,
       c.external_check_state,
       c.external_state,
       c.external_review_state,
       c.publication_state,
       c.sync_state,
       c.diff_stat_added,
       c.diff_stat_deleted,
       r.name::text AS repo_name,
       r.archived AS repo_archived
FROM changesets c
INNER JOIN repo r ON r.id = c.repo_id
WHERE c.id = $1
  AND r.deleted_at IS NULL
LIMIT 1
"#,
    )
    .bind(changeset_id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    let Some(row) = row else {
        return Ok(None);
    };

    let external_service_type: String = row.get("external_service_type");
    let metadata: Option<Value> = row.get("metadata");
    let metadata = metadata.unwrap_or(Value::Null);
    let changeset_metadata = parse_changeset_metadata(&external_service_type, &metadata)?;

    let sync_state: Value = row.get("sync_state");
    let sync_state = parse_sync_state(&sync_state);

    let diff_stat = match (
        row.get::<Option<i32>, _>("diff_stat_added"),
        row.get::<Option<i32>, _>("diff_stat_deleted"),
    ) {
        (Some(added), Some(deleted)) => Some(DiffStat { added, deleted }),
        _ => None,
    };

    let external_check_state: Option<String> = row.get("external_check_state");
    let external_state: Option<String> = row.get("external_state");
    let external_review_state: Option<String> = row.get("external_review_state");
    let publication_state: Option<String> = row.get("publication_state");

    let mut changeset = Changeset {
        id: row.get("id"),
        external_service_type,
        updated_at: row.get("updated_at"),
        external_deleted_at: row.get("external_deleted_at"),
        metadata: changeset_metadata,
        external_check_state: external_check_state
            .as_deref()
            .and_then(parse_check_state),
        external_state: external_state.as_deref().and_then(ChangesetExternalState::from_db),
        external_review_state: external_review_state
            .as_deref()
            .and_then(ChangesetReviewState::from_db),
        published: publication_state.as_deref() == Some("PUBLISHED"),
        sync_state,
        base_ref: String::new(),
        base_ref_oid: String::new(),
        head_ref: String::new(),
        head_ref_oid: String::new(),
        diff_stat,
    };
    apply_refs(&mut changeset, &metadata);

    let repo = RepoInfo {
        name: row.get("repo_name"),
        archived: row.get("repo_archived"),
    };

    let event_rows = sqlx::query(
        r#"
SELECT changeset_id, kind, created_at, metadata
FROM changeset_events
WHERE changeset_id = $1
ORDER BY id ASC
"#,
    )
    .bind(changeset_id)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;

    let mut events = Vec::with_capacity(event_rows.len());
    for row in event_rows {
        let kind: String = row.get("kind");
        let created_at: OffsetDateTime = row.get("created_at");
        let metadata: Value = row.get("metadata");
        let metadata = parse_event_metadata(&kind, &metadata, created_at)
            .map_err(|err| format!("scanChangesetEvent: {err}"))?;
        events.push(ChangesetEvent {
            changeset_id: row.get("changeset_id"),
            kind,
            metadata,
        });
    }

    Ok(Some(LoadedChangeset {
        changeset,
        events,
        repo,
    }))
}

fn parse_check_state(raw: &str) -> Option<ChangesetCheckState> {
    match raw {
        "UNKNOWN" => Some(ChangesetCheckState::Unknown),
        "PENDING" => Some(ChangesetCheckState::Pending),
        "PASSED" => Some(ChangesetCheckState::Passed),
        "FAILED" => Some(ChangesetCheckState::Failed),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

fn sync_state_json(state: &ChangesetSyncState) -> Value {
    serde_json::json!({
        "BaseRefOid": state.base_ref_oid,
        "HeadRefOid": state.head_ref_oid,
        "IsComplete": state.is_complete,
    })
}

/// Persist the `UpdateChangesetCodeHostState` columns derived by
/// `SetDerivedState`. `metadata`, title and fork columns belong to the
/// code-host source step and are intentionally untouched.
pub async fn persist_derived_state(
    conn: &mut PgConnection,
    changeset: &Changeset,
) -> Result<(), String> {
    let check = changeset.external_check_state.map(ChangesetCheckState::as_db);
    let external = changeset.external_state.map(ChangesetExternalState::as_db);
    let review = changeset
        .external_review_state
        .map(ChangesetReviewState::as_db);
    let (added, deleted) = match changeset.diff_stat {
        Some(stat) => (Some(stat.added), Some(stat.deleted)),
        None => (None, None),
    };
    sqlx::query(
        r#"
UPDATE changesets
SET updated_at = clock_timestamp(),
    external_state = $2,
    external_review_state = $3,
    external_check_state = $4,
    diff_stat_added = $5,
    diff_stat_deleted = $6,
    sync_state = $7
WHERE id = $1
"#,
    )
    .bind(changeset.id)
    .bind(external)
    .bind(review)
    .bind(check)
    .bind(added)
    .bind(deleted)
    .bind(sync_state_json(&changeset.sync_state))
    .execute(&mut *conn)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// Compute + persist `SetDerivedState` for one changeset.
///
/// Returns `None` when the changeset (or its repo) does not exist, mirroring
/// `GetChangeset`'s `ErrNoResults` and `repos.Get`'s not-found path.
pub async fn refresh_derived_state(
    conn: &mut PgConnection,
    changeset_id: i64,
    client: Option<&dyn GitserverClient>,
) -> Result<Option<DerivedState>, String> {
    let Some(mut loaded) = load_derived_inputs(&mut *conn, changeset_id).await? else {
        return Ok(None);
    };

    // The reduced reconciler writer does not persist `external_deleted_at`,
    // so a changeset the executor marked deleted would otherwise be revived
    // by metadata-driven history. Go derives `Deleted` from a non-zero
    // `ExternalDeletedAt`; preserving the stored `DELETED` state is the
    // equivalent here.
    let was_deleted = loaded.changeset.external_state == Some(ChangesetExternalState::Deleted);

    // `SetDerivedState` never propagates errors: every step logs and returns.
    // Without a gitserver client the pure fields are still set before the
    // sync-state step reports the seam.
    let _ = set_derived_state(
        client,
        &mut loaded.changeset,
        &loaded.events,
        &loaded.repo,
    );
    if was_deleted {
        loaded.changeset.external_state = Some(ChangesetExternalState::Deleted);
    }

    persist_derived_state(&mut *conn, &loaded.changeset).await?;

    let mut sorted = loaded.events.clone();
    super::batches_state::sort_events(&mut sorted);
    let labels = compute_labels(Some(&loaded.changeset), &sorted);

    Ok(Some(DerivedState {
        changeset_id: loaded.changeset.id,
        external_state: loaded.changeset.external_state,
        external_review_state: loaded.changeset.external_review_state,
        external_check_state: loaded.changeset.external_check_state,
        sync_state: loaded.changeset.sync_state,
        diff_stat: loaded.changeset.diff_stat,
        labels,
    }))
}

/// Pool convenience used by repo-updater's changeset syncer backend.
pub async fn refresh_derived_state_pool(
    pool: &PgPool,
    changeset_id: i64,
    client: Option<&dyn GitserverClient>,
) -> Result<Option<DerivedState>, String> {
    let mut conn = pool.acquire().await.map_err(|err| err.to_string())?;
    refresh_derived_state(&mut conn, changeset_id, client).await
}
