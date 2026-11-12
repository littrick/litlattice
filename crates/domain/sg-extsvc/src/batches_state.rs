#![forbid(unsafe_code)]

//! `internal/batches/state`: derived changeset state.
//!
//! Ported: `SetDerivedState`'s orchestration and every pure function it relies
//! on — check state (`computeCheckState` and the per-code-host parsers/combines),
//! external state, review state, review/history merging (`computeHistory`,
//! `StatesAtTime`, `reduceReviewStates`, `initialExternalState`), `ComputeLabels`,
//! and the aggregate counts (`CalcCounts`, `GenerateTimestamps`).
//!
//! The code-host metadata types are modelled locally (the repository-side
//! `sg-extsvc` clients do not carry the Batch Changes metadata payloads) and
//! follow the Go structs field-for-field where a compute function reads them.
//!
//! Gitserver-dependent pieces (`computeSyncState`, `computeRev`,
//! `computeDiffStat`) stay behind [`GitserverClient`]. When no client is
//! injected, [`set_derived_state`] still computes the pure parts and reports
//! the gitserver step as a seam error rather than fabricating sync state.

use std::collections::{BTreeMap, HashMap};

use time::OffsetDateTime;

/// `btypes.ChangesetExternalState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangesetExternalState {
    Draft,
    Open,
    Closed,
    Merged,
    Deleted,
    ReadOnly,
}

impl ChangesetExternalState {
    pub fn from_db(raw: &str) -> Option<Self> {
        match raw {
            "DRAFT" => Some(Self::Draft),
            "OPEN" => Some(Self::Open),
            "CLOSED" => Some(Self::Closed),
            "MERGED" => Some(Self::Merged),
            "DELETED" => Some(Self::Deleted),
            "READONLY" => Some(Self::ReadOnly),
            _ => None,
        }
    }

    pub fn as_db(self) -> &'static str {
        match self {
            Self::Draft => "DRAFT",
            Self::Open => "OPEN",
            Self::Closed => "CLOSED",
            Self::Merged => "MERGED",
            Self::Deleted => "DELETED",
            Self::ReadOnly => "READONLY",
        }
    }
}

/// `btypes.ChangesetReviewState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangesetReviewState {
    Approved,
    ChangesRequested,
    Pending,
    Commented,
    Dismissed,
}

impl ChangesetReviewState {
    pub fn from_db(raw: &str) -> Option<Self> {
        match raw {
            "APPROVED" => Some(Self::Approved),
            "CHANGES_REQUESTED" => Some(Self::ChangesRequested),
            "PENDING" => Some(Self::Pending),
            "COMMENTED" => Some(Self::Commented),
            "DISMISSED" => Some(Self::Dismissed),
            _ => None,
        }
    }

    pub fn as_db(self) -> &'static str {
        match self {
            Self::Approved => "APPROVED",
            Self::ChangesRequested => "CHANGES_REQUESTED",
            Self::Pending => "PENDING",
            Self::Commented => "COMMENTED",
            Self::Dismissed => "DISMISSED",
        }
    }
}

/// `btypes.ChangesetCheckState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangesetCheckState {
    Unknown,
    Pending,
    Passed,
    Failed,
}

impl ChangesetCheckState {
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Pending => "PENDING",
            Self::Passed => "PASSED",
            Self::Failed => "FAILED",
        }
    }
}

/// `btypes.ChangesetLabel`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetLabel {
    pub name: String,
    pub color: String,
    pub description: String,
}

// ---------------------------------------------------------------------------
// Metadata model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubContext {
    pub context: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubCheckRun {
    pub id: String,
    pub status: String,
    pub conclusion: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubCheckSuite {
    pub id: String,
    pub status: String,
    pub conclusion: String,
    pub runs: Vec<GithubCheckRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubCommitWithChecks {
    pub oid: String,
    pub committed_date: OffsetDateTime,
    pub contexts: Vec<GithubContext>,
    pub check_suites: Vec<GithubCheckSuite>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubPullRequest {
    pub is_draft: bool,
    pub state: String,
    pub review_decision: String,
    pub created_at: OffsetDateTime,
    pub labels: Vec<ChangesetLabel>,
    pub commits: Vec<GithubCommitWithChecks>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabNote {
    pub body: String,
    pub author: String,
    pub created_at: OffsetDateTime,
    pub system: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabPipeline {
    pub status: String,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabMergeRequest {
    pub state: String,
    pub work_in_progress: bool,
    pub created_at: OffsetDateTime,
    pub notes: Vec<GitlabNote>,
    pub pipelines: Vec<GitlabPipeline>,
    pub head_pipeline: Option<GitlabPipeline>,
    /// `MergeRequest.Labels`: bare names, rendered with color `000000`.
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketServerCommit {
    pub id: String,
    pub committer_timestamp: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketServerCommitStatus {
    pub commit: String,
    pub key: String,
    pub state: String,
    pub date_added: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketServerReviewer {
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketServerPullRequest {
    pub state: String,
    pub created_date: OffsetDateTime,
    pub commits: Vec<BitbucketServerCommit>,
    pub commit_status: Vec<BitbucketServerCommitStatus>,
    pub reviewers: Vec<BitbucketServerReviewer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketCloudStatus {
    pub key: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketCloudParticipant {
    pub author: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketCloudPullRequest {
    pub state: String,
    pub created_on: OffsetDateTime,
    pub statuses: Vec<BitbucketCloudStatus>,
    pub participants: Vec<BitbucketCloudParticipant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureDevOpsStatus {
    pub id: i32,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureDevOpsReviewer {
    pub vote: i32,
    pub unique_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureDevOpsPullRequest {
    pub status: String,
    pub is_draft: bool,
    pub creation_date: OffsetDateTime,
    pub statuses: Vec<AzureDevOpsStatus>,
    pub reviewers: Vec<AzureDevOpsReviewer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GerritReviewer {
    pub approvals: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GerritChange {
    pub state: String,
    pub work_in_progress: bool,
    pub created: OffsetDateTime,
    /// `None` is Go's nil slice (distinct from an empty non-nil slice).
    pub reviewers: Option<Vec<GerritReviewer>>,
    /// `Change.Hashtags`, surfaced as labels with color `000000`.
    pub hashtags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerforceChangelist {
    pub state: String,
    pub creation_date: OffsetDateTime,
}

/// The code-host metadata payload of a changeset (`btypes.Changeset.Metadata`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangesetMetadata {
    Github(GithubPullRequest),
    Gitlab(GitlabMergeRequest),
    BitbucketServer(BitbucketServerPullRequest),
    BitbucketCloud(BitbucketCloudPullRequest),
    AzureDevOps(AzureDevOpsPullRequest),
    Gerrit(GerritChange),
    Perforce(PerforceChangelist),
}

/// The code-host metadata payload of a webhook/sync event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventMetadata {
    /// `*github.CommitStatus`.
    GithubCommitStatus {
        sha: String,
        context: String,
        state: String,
        received_at: OffsetDateTime,
    },
    /// `*github.PullRequestCommit`.
    GithubPullRequestCommit {
        oid: String,
        committed_date: OffsetDateTime,
    },
    /// `*github.CheckSuite`.
    GithubCheckSuite {
        id: String,
        status: String,
        conclusion: String,
        received_at: OffsetDateTime,
        runs: Vec<GithubCheckRun>,
    },
    /// `*github.CheckRun`.
    GithubCheckRun {
        id: String,
        status: String,
        conclusion: String,
        received_at: OffsetDateTime,
    },
    /// `*github.LabelEvent`.
    GithubLabel {
        label: ChangesetLabel,
        created_at: OffsetDateTime,
        removed: bool,
    },
    /// `*github.PullRequestReview`.
    GithubReview {
        author: String,
        state: String,
        updated_at: OffsetDateTime,
    },
    /// `*github.ReviewDismissedEvent`.
    GithubReviewDismissed {
        author: String,
        created_at: OffsetDateTime,
    },
    /// `github.ClosedEvent`/`MergedEvent`/`ReopenedEvent`/`ReadyForReviewEvent`/
    /// `ConvertToDraftEvent` — all carry only `CreatedAt` for our purposes; the
    /// event `kind` distinguishes them.
    GithubCreated { created_at: OffsetDateTime },
    /// `*gitlab.Pipeline` (no `Timestamp()` case in the baseline).
    GitlabPipeline {
        status: String,
        created_at: OffsetDateTime,
    },
    /// `gitlab.ReviewApprovedEvent`/`ReviewUnapprovedEvent`/
    /// `MarkWorkInProgressEvent`/`UnmarkWorkInProgressEvent` (all embed `Note`).
    GitlabReview {
        author: String,
        created_at: OffsetDateTime,
    },
    /// `gitlab.MergeRequestClosedEvent`/`ReopenedEvent`/`MergedEvent`.
    GitlabCreated { created_at: OffsetDateTime },
    /// `*bitbucketserver.Activity` / `*bitbucketserver.ParticipantStatusEvent`.
    BitbucketServerActivity {
        author: String,
        created_date: OffsetDateTime,
    },
    /// `*bitbucketserver.CommitStatus`.
    BitbucketServerCommitStatus {
        commit: String,
        key: String,
        state: String,
        date_added: OffsetDateTime,
    },
    /// `bitbucketcloud.RepoCommitStatusCreatedEvent`/`UpdatedEvent`; `updated`
    /// selects which timestamp `Timestamp()` returns.
    BitbucketCloudCommitStatus {
        key: String,
        state: String,
        created_on: OffsetDateTime,
        updated_on: OffsetDateTime,
        updated: bool,
    },
    /// `bitbucketcloud.PullRequestApprovedEvent`/`UnapprovedEvent`.
    BitbucketCloudApproval {
        author: String,
        date: OffsetDateTime,
    },
    /// `bitbucketcloud.PullRequestChangesRequestCreatedEvent`/`RemovedEvent`.
    BitbucketCloudChangesRequest {
        author: String,
        date: OffsetDateTime,
    },
    /// `bitbucketcloud.PullRequestFulfilledEvent`/`RejectedEvent`.
    BitbucketCloudUpdated { updated_on: OffsetDateTime },
    /// `azuredevops.PullRequest{Merged,Approved,ApprovedWithSuggestions,
    /// WaitingForAuthor,Rejected}Event`.
    AzureDevOpsEvent {
        author: String,
        created_date: OffsetDateTime,
    },
}

/// A `ChangesetEvent` (`kind`, metadata, and owning changeset id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetEvent {
    pub changeset_id: i64,
    pub kind: String,
    pub metadata: EventMetadata,
}

/// `types.Repo`, reduced to the fields the derived state reads.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepoInfo {
    pub name: String,
    pub archived: bool,
}

/// `btypes.ChangesetSyncState`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChangesetSyncState {
    pub base_ref_oid: String,
    pub head_ref_oid: String,
    pub is_complete: bool,
}

/// Aggregated diff stat (`diff.Stat` after `SetDiffStat`'s Changed folding is
/// the caller's responsibility; this is the raw added/deleted pair).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DiffStat {
    pub added: i32,
    pub deleted: i32,
}

/// The subset of `btypes.Changeset` the derived state reads and writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changeset {
    pub id: i64,
    pub external_service_type: String,
    pub updated_at: OffsetDateTime,
    pub external_deleted_at: Option<OffsetDateTime>,
    pub metadata: ChangesetMetadata,
    pub external_check_state: Option<ChangesetCheckState>,
    pub external_state: Option<ChangesetExternalState>,
    pub external_review_state: Option<ChangesetReviewState>,
    pub published: bool,
    pub sync_state: ChangesetSyncState,
    pub base_ref_oid: String,
    pub base_ref: String,
    pub head_ref_oid: String,
    pub head_ref: String,
    pub diff_stat: Option<DiffStat>,
}

impl Changeset {
    /// `Changeset.Complete`.
    pub fn complete(&self) -> bool {
        self.published
            && !matches!(
                self.external_state,
                Some(ChangesetExternalState::Open) | Some(ChangesetExternalState::Draft)
            )
    }

    /// `Changeset.ExternalCreatedAt` (None = zero time).
    pub fn external_created_at(&self) -> Option<OffsetDateTime> {
        match &self.metadata {
            ChangesetMetadata::Github(pr) => Some(pr.created_at),
            ChangesetMetadata::Gitlab(mr) => Some(mr.created_at),
            ChangesetMetadata::BitbucketServer(pr) => Some(pr.created_date),
            ChangesetMetadata::BitbucketCloud(pr) => Some(pr.created_on),
            ChangesetMetadata::AzureDevOps(pr) => Some(pr.creation_date),
            ChangesetMetadata::Gerrit(change) => Some(change.created),
            ChangesetMetadata::Perforce(cl) => Some(cl.creation_date),
        }
    }

    /// `Changeset.Labels`.
    pub fn labels(&self) -> Vec<ChangesetLabel> {
        fn plain(names: &[String]) -> Vec<ChangesetLabel> {
            names
                .iter()
                .map(|name| ChangesetLabel {
                    name: name.clone(),
                    color: "000000".to_string(),
                    description: String::new(),
                })
                .collect()
        }
        match &self.metadata {
            ChangesetMetadata::Github(pr) => pr.labels.clone(),
            ChangesetMetadata::Gitlab(mr) => plain(&mr.labels),
            ChangesetMetadata::Gerrit(change) => plain(&change.hashtags),
            _ => Vec::new(),
        }
    }
}

/// `ChangesetEventKind` constants used by the history/label logic.
pub mod kinds {
    pub const GITHUB_CLOSED: &str = "github:closed";
    pub const GITHUB_MERGED: &str = "github:merged";
    pub const GITHUB_REOPENED: &str = "github:reopened";
    pub const GITHUB_READY_FOR_REVIEW: &str = "github:ready_for_review";
    pub const GITHUB_CONVERT_TO_DRAFT: &str = "github:convert_to_draft";
    pub const GITHUB_REVIEWED: &str = "github:reviewed";
    pub const GITHUB_REVIEW_DISMISSED: &str = "github:review_dismissed";
    pub const GITHUB_LABELED: &str = "github:labeled";
    pub const GITHUB_UNLABELED: &str = "github:unlabeled";

    pub const GITLAB_CLOSED: &str = "gitlab:closed";
    pub const GITLAB_MERGED: &str = "gitlab:merged";
    pub const GITLAB_REOPENED: &str = "gitlab:reopened";
    pub const GITLAB_APPROVED: &str = "gitlab:approved";
    pub const GITLAB_UNAPPROVED: &str = "gitlab:unapproved";
    pub const GITLAB_MARK_WIP: &str = "gitlab:mark_wip";
    pub const GITLAB_UNMARK_WIP: &str = "gitlab:unmark_wip";

    pub const BBS_APPROVED: &str = "bitbucketserver:approved";
    pub const BBS_UNAPPROVED: &str = "bitbucketserver:unapproved";
    pub const BBS_DECLINED: &str = "bitbucketserver:declined";
    pub const BBS_REVIEWED: &str = "bitbucketserver:reviewed";
    pub const BBS_REOPENED: &str = "bitbucketserver:reopened";
    pub const BBS_MERGED: &str = "bitbucketserver:merged";
    pub const BBS_DISMISSED: &str = "bitbucketserver:participant_status:unapproved";

    pub const BBC_APPROVED: &str = "bitbucketcloud:approved";
    pub const BBC_CHANGES_REQUESTED: &str = "bitbucketcloud:changes_requested";
    pub const BBC_PR_APPROVED: &str = "bitbucketcloud:pullrequest:approved";
    pub const BBC_PR_FULFILLED: &str = "bitbucketcloud:pullrequest:fulfilled";
    pub const BBC_PR_REJECTED: &str = "bitbucketcloud:pullrequest:rejected";
    pub const BBC_PR_UNAPPROVED: &str = "bitbucketcloud:pullrequest:unapproved";
    pub const BBC_PR_CHANGES_REQUEST_CREATED: &str =
        "bitbucketcloud:pullrequest:changes_request_created";
    pub const BBC_PR_CHANGES_REQUEST_REMOVED: &str =
        "bitbucketcloud:pullrequest:changes_request_removed";

    pub const AZURE_PR_MERGED: &str = "azuredevops:pullrequest:merged";
    pub const AZURE_PR_APPROVED: &str = "azuredevops:pullrequest:approved";
    pub const AZURE_PR_APPROVED_WITH_SUGGESTIONS: &str =
        "azuredevops:pullrequest:approved_with_suggestions";
    pub const AZURE_PR_WAITING_FOR_AUTHOR: &str = "azuredevops:pullrequest:waiting_for_author";
    pub const AZURE_PR_REJECTED: &str = "azuredevops:pullrequest:rejected";
}

/// `state.ComputeLabelsRequiredEventTypes`.
pub const COMPUTE_LABELS_REQUIRED_EVENT_TYPES: &[&str] =
    &[kinds::GITHUB_LABELED, kinds::GITHUB_UNLABELED];

/// `state.RequiredEventTypesForHistory`.
pub const REQUIRED_EVENT_TYPES_FOR_HISTORY: &[&str] = &[
    kinds::GITHUB_READY_FOR_REVIEW,
    kinds::GITLAB_UNMARK_WIP,
    kinds::GITHUB_CONVERT_TO_DRAFT,
    kinds::GITLAB_MARK_WIP,
    kinds::BBC_PR_REJECTED,
    kinds::BBS_DECLINED,
    kinds::GITHUB_CLOSED,
    kinds::GITLAB_CLOSED,
    kinds::BBC_PR_FULFILLED,
    kinds::BBS_MERGED,
    kinds::GITHUB_MERGED,
    kinds::GITLAB_MERGED,
    kinds::AZURE_PR_MERGED,
    kinds::BBS_REOPENED,
    kinds::GITHUB_REOPENED,
    kinds::GITLAB_REOPENED,
    kinds::GITHUB_REVIEWED,
    kinds::BBC_APPROVED,
    kinds::BBC_PR_APPROVED,
    kinds::BBS_APPROVED,
    kinds::BBS_REVIEWED,
    kinds::GITLAB_APPROVED,
    kinds::AZURE_PR_APPROVED,
    kinds::AZURE_PR_APPROVED_WITH_SUGGESTIONS,
    kinds::BBC_PR_CHANGES_REQUEST_REMOVED,
    kinds::BBC_PR_UNAPPROVED,
    kinds::BBS_UNAPPROVED,
    kinds::BBS_DISMISSED,
    kinds::GITLAB_UNAPPROVED,
    kinds::AZURE_PR_WAITING_FOR_AUTHOR,
    kinds::AZURE_PR_REJECTED,
];

// ---------------------------------------------------------------------------
// Event ordering
// ---------------------------------------------------------------------------

/// `ChangesetEvent.Timestamp()` — `None` is the Go zero time.
pub fn event_timestamp(event: &ChangesetEvent) -> Option<OffsetDateTime> {
    match &event.metadata {
        EventMetadata::GithubCommitStatus { received_at, .. } => Some(*received_at),
        EventMetadata::GithubPullRequestCommit { committed_date, .. } => Some(*committed_date),
        EventMetadata::GithubCheckSuite { received_at, .. } => Some(*received_at),
        EventMetadata::GithubCheckRun { received_at, .. } => Some(*received_at),
        EventMetadata::GithubLabel { created_at, .. } => Some(*created_at),
        EventMetadata::GithubReview { updated_at, .. } => Some(*updated_at),
        EventMetadata::GithubReviewDismissed { created_at, .. } => Some(*created_at),
        EventMetadata::GithubCreated { created_at } => Some(*created_at),
        // `*gitlab.Pipeline` has no `Timestamp()` case in the baseline.
        EventMetadata::GitlabPipeline { .. } => None,
        EventMetadata::GitlabReview { created_at, .. } => Some(*created_at),
        EventMetadata::GitlabCreated { created_at } => Some(*created_at),
        EventMetadata::BitbucketServerActivity { created_date, .. } => Some(*created_date),
        EventMetadata::BitbucketServerCommitStatus { date_added, .. } => Some(*date_added),
        EventMetadata::BitbucketCloudCommitStatus {
            created_on,
            updated_on,
            updated,
            ..
        } => Some(if *updated { *updated_on } else { *created_on }),
        EventMetadata::BitbucketCloudApproval { date, .. } => Some(*date),
        EventMetadata::BitbucketCloudChangesRequest { date, .. } => Some(*date),
        EventMetadata::BitbucketCloudUpdated { updated_on } => Some(*updated_on),
        EventMetadata::AzureDevOpsEvent { created_date, .. } => Some(*created_date),
    }
}

/// `ChangesetEvent.ReviewAuthor()` (`""` when not a review event).
pub fn event_review_author(event: &ChangesetEvent) -> String {
    match &event.metadata {
        EventMetadata::GithubReview { author, .. }
        | EventMetadata::GithubReviewDismissed { author, .. }
        | EventMetadata::BitbucketServerActivity { author, .. }
        | EventMetadata::GitlabReview { author, .. }
        | EventMetadata::BitbucketCloudApproval { author, .. }
        | EventMetadata::BitbucketCloudChangesRequest { author, .. }
        | EventMetadata::AzureDevOpsEvent { author, .. } => author.clone(),
        _ => String::new(),
    }
}

/// `ChangesetEvent.ReviewState()`.
pub fn event_review_state(event: &ChangesetEvent) -> ChangesetReviewState {
    match event.kind.as_str() {
        kinds::BBS_APPROVED
        | kinds::GITLAB_APPROVED
        | kinds::BBC_APPROVED
        | kinds::BBC_PR_APPROVED
        | kinds::AZURE_PR_APPROVED => ChangesetReviewState::Approved,
        kinds::BBS_REVIEWED
        | kinds::BBC_CHANGES_REQUESTED
        | kinds::BBC_PR_CHANGES_REQUEST_CREATED
        | kinds::AZURE_PR_WAITING_FOR_AUTHOR
        | kinds::AZURE_PR_APPROVED_WITH_SUGGESTIONS => ChangesetReviewState::ChangesRequested,
        kinds::GITHUB_REVIEWED => match &event.metadata {
            EventMetadata::GithubReview { state, .. } => {
                let upper = state.to_uppercase();
                ChangesetReviewState::from_db(&upper).unwrap_or(ChangesetReviewState::Pending)
            }
            _ => ChangesetReviewState::Pending,
        },
        kinds::GITHUB_REVIEW_DISMISSED
        | kinds::BBS_UNAPPROVED
        | kinds::BBS_DISMISSED
        | kinds::GITLAB_UNAPPROVED
        | kinds::BBC_PR_UNAPPROVED
        | kinds::BBC_PR_CHANGES_REQUEST_REMOVED
        | kinds::AZURE_PR_REJECTED => ChangesetReviewState::Dismissed,
        _ => ChangesetReviewState::Pending,
    }
}

fn opt_cmp(a: Option<OffsetDateTime>, b: Option<OffsetDateTime>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(&b),
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
    }
}

/// `sort.Sort(ChangesetEvents)` — ascending by `Timestamp()`.
pub fn sort_events(events: &mut [ChangesetEvent]) {
    events.sort_by(|a, b| opt_cmp(event_timestamp(a), event_timestamp(b)));
}

pub fn events_sorted(events: &[ChangesetEvent]) -> bool {
    events
        .windows(2)
        .all(|w| opt_cmp(event_timestamp(&w[0]), event_timestamp(&w[1])) != std::cmp::Ordering::Greater)
}

// ---------------------------------------------------------------------------
// Check state
// ---------------------------------------------------------------------------

fn parse_github_check_state(state: &str) -> ChangesetCheckState {
    match state.to_uppercase().as_str() {
        "ERROR" | "FAILURE" => ChangesetCheckState::Failed,
        "EXPECTED" | "PENDING" => ChangesetCheckState::Pending,
        "SUCCESS" => ChangesetCheckState::Passed,
        _ => ChangesetCheckState::Unknown,
    }
}

fn parse_github_check_suite_state(status: &str, conclusion: &str) -> ChangesetCheckState {
    let status = status.to_uppercase();
    let conclusion = conclusion.to_uppercase();
    match status.as_str() {
        "IN_PROGRESS" | "QUEUED" | "REQUESTED" => return ChangesetCheckState::Pending,
        _ => {}
    }
    if status != "COMPLETED" {
        return ChangesetCheckState::Unknown;
    }
    match conclusion.as_str() {
        "SUCCESS" | "NEUTRAL" | "SKIPPED" => ChangesetCheckState::Passed,
        "ACTION_REQUIRED" => ChangesetCheckState::Pending,
        "CANCELLED" | "FAILURE" | "TIMED_OUT" => ChangesetCheckState::Failed,
        _ => ChangesetCheckState::Unknown,
    }
}

fn parse_bitbucket_server_build_state(state: &str) -> ChangesetCheckState {
    match state {
        "FAILED" => ChangesetCheckState::Failed,
        "INPROGRESS" => ChangesetCheckState::Pending,
        "SUCCESSFUL" => ChangesetCheckState::Passed,
        _ => ChangesetCheckState::Unknown,
    }
}

fn parse_bitbucket_cloud_build_state(state: &str) -> ChangesetCheckState {
    match state {
        "FAILED" | "STOPPED" => ChangesetCheckState::Failed,
        "INPROGRESS" => ChangesetCheckState::Pending,
        "SUCCESSFUL" => ChangesetCheckState::Passed,
        _ => ChangesetCheckState::Unknown,
    }
}

fn parse_azure_devops_build_state(state: &str) -> ChangesetCheckState {
    match state {
        "error" | "failed" => ChangesetCheckState::Failed,
        "pending" => ChangesetCheckState::Pending,
        "succeeded" => ChangesetCheckState::Passed,
        _ => ChangesetCheckState::Unknown,
    }
}

fn parse_gerrit_build_state(state: &str) -> ChangesetCheckState {
    match state {
        "-2" | "-1" => ChangesetCheckState::Failed,
        " 0" => ChangesetCheckState::Pending,
        "+2" | "+1" => ChangesetCheckState::Passed,
        _ => ChangesetCheckState::Unknown,
    }
}

/// `combineCheckStates`: Pending > Failed > Unknown > Passed; empty → Unknown.
pub fn combine_check_states(states: &[ChangesetCheckState]) -> ChangesetCheckState {
    if states.is_empty() {
        return ChangesetCheckState::Unknown;
    }
    let has = |s: ChangesetCheckState| states.contains(&s);
    if has(ChangesetCheckState::Pending) {
        ChangesetCheckState::Pending
    } else if has(ChangesetCheckState::Failed) {
        ChangesetCheckState::Failed
    } else if has(ChangesetCheckState::Unknown) {
        ChangesetCheckState::Unknown
    } else if has(ChangesetCheckState::Passed) {
        ChangesetCheckState::Passed
    } else {
        ChangesetCheckState::Unknown
    }
}

fn compute_github_check_state(
    last_synced: OffsetDateTime,
    pr: &GithubPullRequest,
    events: &[ChangesetEvent],
) -> ChangesetCheckState {
    let mut latest_commit_time: Option<OffsetDateTime> = None;
    let mut latest_oid: Option<String> = None;
    let mut status_per_context: BTreeMap<String, ChangesetCheckState> = BTreeMap::new();
    let mut status_per_check_suite: BTreeMap<String, ChangesetCheckState> = BTreeMap::new();
    let mut status_per_check_run: BTreeMap<String, ChangesetCheckState> = BTreeMap::new();

    if let Some(commit) = pr.commits.first() {
        latest_commit_time = Some(commit.committed_date);
        latest_oid = Some(commit.oid.clone());
        for c in &commit.contexts {
            status_per_context.insert(c.context.clone(), parse_github_check_state(&c.state));
        }
        for suite in &commit.check_suites {
            if (suite.status == "QUEUED" || suite.status == "COMPLETED") && suite.runs.is_empty() {
                continue;
            }
            status_per_check_suite.insert(
                suite.id.clone(),
                parse_github_check_suite_state(&suite.status, &suite.conclusion),
            );
            for run in &suite.runs {
                status_per_check_run.insert(
                    run.id.clone(),
                    parse_github_check_suite_state(&run.status, &run.conclusion),
                );
            }
        }
    }

    let mut statuses: Vec<(String, String, String, OffsetDateTime)> = Vec::new();
    for e in events {
        match &e.metadata {
            EventMetadata::GithubCommitStatus {
                sha,
                context,
                state,
                received_at,
            } => {
                if *received_at > last_synced {
                    statuses.push((sha.clone(), context.clone(), state.clone(), *received_at));
                }
            }
            EventMetadata::GithubPullRequestCommit { oid, committed_date } => {
                let newer = match latest_commit_time {
                    Some(t) => *committed_date > t,
                    None => true,
                };
                if newer {
                    latest_commit_time = Some(*committed_date);
                    latest_oid = Some(oid.clone());
                    status_per_context.clear();
                }
            }
            EventMetadata::GithubCheckSuite {
                id,
                status,
                conclusion,
                received_at,
                runs,
            } => {
                if (*status == "QUEUED" || *status == "COMPLETED") && runs.is_empty() {
                    continue;
                }
                if *received_at > last_synced {
                    status_per_check_suite.insert(
                        id.clone(),
                        parse_github_check_suite_state(status, conclusion),
                    );
                }
            }
            EventMetadata::GithubCheckRun {
                id,
                status,
                conclusion,
                received_at,
            } => {
                if *received_at > last_synced {
                    status_per_check_run.insert(
                        id.clone(),
                        parse_github_check_suite_state(status, conclusion),
                    );
                }
            }
            _ => {}
        }
    }

    if !statuses.is_empty() {
        statuses.sort_by(|a, b| a.3.cmp(&b.3));
        for (sha, context, state, _) in statuses {
            if latest_oid.as_deref() == Some(sha.as_str()) {
                status_per_context.insert(context, parse_github_check_state(&state));
            }
        }
    }

    let mut final_states: Vec<ChangesetCheckState> = Vec::new();
    final_states.extend(status_per_context.values().copied());
    final_states.extend(status_per_check_suite.values().copied());
    final_states.extend(status_per_check_run.values().copied());
    combine_check_states(&final_states)
}

fn compute_bitbucket_server_build_status(
    last_synced: OffsetDateTime,
    pr: &BitbucketServerPullRequest,
    events: &[ChangesetEvent],
) -> ChangesetCheckState {
    let mut latest_commit: Option<&BitbucketServerCommit> = None;
    for c in &pr.commits {
        latest_commit = match latest_commit {
            Some(prev) if prev.committer_timestamp > c.committer_timestamp => Some(prev),
            _ => Some(c),
        };
    }
    let latest_id = latest_commit.map(|c| c.id.clone()).unwrap_or_default();

    let mut state_map: BTreeMap<String, ChangesetCheckState> = BTreeMap::new();
    for status in &pr.commit_status {
        state_map.insert(status.key.clone(), parse_bitbucket_server_build_state(&status.state));
    }
    for e in events {
        if let EventMetadata::BitbucketServerCommitStatus {
            commit,
            key,
            state,
            date_added,
        } = &e.metadata
        {
            if *commit != latest_id {
                continue;
            }
            if *date_added < last_synced {
                continue;
            }
            state_map.insert(key.clone(), parse_bitbucket_server_build_state(state));
        }
    }
    let states: Vec<ChangesetCheckState> = state_map.values().copied().collect();
    combine_check_states(&states)
}

fn compute_bitbucket_cloud_build_state(
    last_synced: OffsetDateTime,
    pr: &BitbucketCloudPullRequest,
    events: &[ChangesetEvent],
) -> ChangesetCheckState {
    let mut state_map: BTreeMap<String, ChangesetCheckState> = BTreeMap::new();
    for status in &pr.statuses {
        state_map.insert(status.key.clone(), parse_bitbucket_cloud_build_state(&status.state));
    }
    for e in events {
        if let EventMetadata::BitbucketCloudCommitStatus {
            key,
            state,
            created_on,
            ..
        } = &e.metadata
        {
            if last_synced < *created_on {
                state_map.insert(key.clone(), parse_bitbucket_cloud_build_state(state));
            }
        }
    }
    let states: Vec<ChangesetCheckState> = state_map.values().copied().collect();
    combine_check_states(&states)
}

fn compute_azure_devops_build_state(pr: &AzureDevOpsPullRequest) -> ChangesetCheckState {
    let mut state_map: BTreeMap<String, ChangesetCheckState> = BTreeMap::new();
    for status in &pr.statuses {
        state_map.insert(
            status.id.to_string(),
            parse_azure_devops_build_state(&status.state),
        );
    }
    let states: Vec<ChangesetCheckState> = state_map.values().copied().collect();
    combine_check_states(&states)
}

fn compute_gerrit_build_state(change: &GerritChange) -> ChangesetCheckState {
    let mut state_map: BTreeMap<String, ChangesetCheckState> = BTreeMap::new();
    if let Some(reviewers) = &change.reviewers {
        for reviewer in reviewers {
            for (key, val) in &reviewer.approvals {
                if key != "Code-Review" {
                    state_map.insert(key.clone(), parse_gerrit_build_state(val));
                }
            }
        }
    }
    let states: Vec<ChangesetCheckState> = state_map.values().copied().collect();
    combine_check_states(&states)
}

fn parse_gitlab_pipeline_status(status: &str) -> ChangesetCheckState {
    match status {
        "success" => ChangesetCheckState::Passed,
        "failed" | "canceled" => ChangesetCheckState::Failed,
        "pending" | "running" | "created" => ChangesetCheckState::Pending,
        _ => ChangesetCheckState::Unknown,
    }
}

fn compute_gitlab_check_state(
    last_synced: OffsetDateTime,
    mr: &GitlabMergeRequest,
    events: &[ChangesetEvent],
) -> ChangesetCheckState {
    let mut last_pipeline_event: Option<GitlabPipeline> = None;
    for e in events {
        if let EventMetadata::GitlabPipeline {
            status,
            created_at,
        } = &e.metadata
        {
            let candidate = GitlabPipeline {
                status: status.clone(),
                created_at: *created_at,
            };
            let replace = match &last_pipeline_event {
                None => true,
                Some(prev) => prev.created_at < candidate.created_at,
            };
            if replace {
                last_pipeline_event = Some(candidate);
            }
        }
    }

    let use_merge_request = match &last_pipeline_event {
        None => true,
        Some(p) => p.created_at < last_synced,
    };
    if use_merge_request {
        if mr.pipelines.is_empty() {
            return match &mr.head_pipeline {
                Some(hp) => parse_gitlab_pipeline_status(&hp.status),
                None => ChangesetCheckState::Unknown,
            };
        }
        let mut pipelines: Vec<&GitlabPipeline> = mr.pipelines.iter().collect();
        pipelines.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        return parse_gitlab_pipeline_status(&pipelines[0].status);
    }

    parse_gitlab_pipeline_status(&last_pipeline_event.expect("checked").status)
}

/// `computeCheckState`.
pub fn compute_check_state(
    changeset: &Changeset,
    events: &[ChangesetEvent],
) -> ChangesetCheckState {
    let last_synced = changeset.updated_at;
    match &changeset.metadata {
        ChangesetMetadata::Github(pr) => compute_github_check_state(last_synced, pr, events),
        ChangesetMetadata::BitbucketServer(pr) => {
            compute_bitbucket_server_build_status(last_synced, pr, events)
        }
        ChangesetMetadata::Gitlab(mr) => compute_gitlab_check_state(last_synced, mr, events),
        ChangesetMetadata::BitbucketCloud(pr) => {
            compute_bitbucket_cloud_build_state(last_synced, pr, events)
        }
        ChangesetMetadata::AzureDevOps(pr) => compute_azure_devops_build_state(pr),
        ChangesetMetadata::Gerrit(change) => compute_gerrit_build_state(change),
        // Perforce has no built-in builds; be explicit like the baseline.
        ChangesetMetadata::Perforce(_) => ChangesetCheckState::Unknown,
    }
}

// ---------------------------------------------------------------------------
// External / review state
// ---------------------------------------------------------------------------

/// `selectReviewState`: ChangesRequested > Approved > Pending.
pub fn select_review_state(states: &[ChangesetReviewState]) -> ChangesetReviewState {
    for state in [
        ChangesetReviewState::ChangesRequested,
        ChangesetReviewState::Approved,
    ] {
        if states.contains(&state) {
            return state;
        }
    }
    ChangesetReviewState::Pending
}

/// `computeGitHubReviewState`.
fn compute_github_review_state(pr: &GithubPullRequest) -> ChangesetReviewState {
    match pr.review_decision.as_str() {
        "REVIEW_REQUIRED" => ChangesetReviewState::Pending,
        "APPROVED" => ChangesetReviewState::Approved,
        "CHANGES_REQUESTED" => ChangesetReviewState::ChangesRequested,
        _ => ChangesetReviewState::Pending,
    }
}

/// `computeSingleChangesetExternalState`.
pub fn compute_single_changeset_external_state(
    changeset: &Changeset,
) -> Result<ChangesetExternalState, String> {
    if changeset.external_deleted_at.is_some() {
        return Ok(ChangesetExternalState::Deleted);
    }

    let state = match &changeset.metadata {
        ChangesetMetadata::Github(pr) => {
            if pr.is_draft && pr.state == "OPEN" {
                ChangesetExternalState::Draft
            } else {
                ChangesetExternalState::from_db(&pr.state)
                    .ok_or_else(|| format!("changeset state \"{}\" invalid", pr.state))?
            }
        }
        ChangesetMetadata::BitbucketServer(pr) => {
            if pr.state == "DECLINED" {
                ChangesetExternalState::Closed
            } else {
                ChangesetExternalState::from_db(&pr.state)
                    .ok_or_else(|| format!("changeset state \"{}\" invalid", pr.state))?
            }
        }
        ChangesetMetadata::Gitlab(mr) => match mr.state.as_str() {
            "closed" | "locked" => ChangesetExternalState::Closed,
            "merged" => ChangesetExternalState::Merged,
            "opened" => {
                if mr.work_in_progress {
                    ChangesetExternalState::Draft
                } else {
                    ChangesetExternalState::Open
                }
            }
            other => return Err(format!("unknown GitLab merge request state: {other}")),
        },
        ChangesetMetadata::BitbucketCloud(pr) => match pr.state.as_str() {
            "DECLINED" | "SUPERSEDED" => ChangesetExternalState::Closed,
            "MERGED" => ChangesetExternalState::Merged,
            "OPEN" => ChangesetExternalState::Open,
            other => {
                return Err(format!(
                    "unknown Bitbucket Cloud pull request state: {other}"
                ))
            }
        },
        ChangesetMetadata::AzureDevOps(pr) => match pr.status.as_str() {
            "abandoned" => ChangesetExternalState::Closed,
            "completed" => ChangesetExternalState::Merged,
            "active" => {
                if pr.is_draft {
                    ChangesetExternalState::Draft
                } else {
                    ChangesetExternalState::Open
                }
            }
            other => {
                return Err(format!(
                    "unknown Azure DevOps pull request state: {other}"
                ))
            }
        },
        ChangesetMetadata::Gerrit(change) => match change.state.as_str() {
            "ABANDONED" => ChangesetExternalState::Closed,
            "MERGED" => ChangesetExternalState::Merged,
            "NEW" => {
                if change.work_in_progress {
                    ChangesetExternalState::Draft
                } else {
                    ChangesetExternalState::Open
                }
            }
            other => return Err(format!("unknown Gerrit Change state: {other}")),
        },
        ChangesetMetadata::Perforce(cl) => match cl.state.as_str() {
            "closed" => ChangesetExternalState::Closed,
            "submitted" => ChangesetExternalState::Merged,
            "pending" | "shelved" => ChangesetExternalState::Open,
            other => return Err(format!("unknown Perforce Change state: {other}")),
        },
    };
    Ok(state)
}

/// `computeSingleChangesetReviewState`.
pub fn compute_single_changeset_review_state(
    changeset: &Changeset,
) -> Result<ChangesetReviewState, String> {
    let mut states: Vec<ChangesetReviewState> = Vec::new();
    match &changeset.metadata {
        ChangesetMetadata::BitbucketServer(pr) => {
            for r in &pr.reviewers {
                match r.status.as_str() {
                    "UNAPPROVED" => states.push(ChangesetReviewState::Pending),
                    "NEEDS_WORK" => states.push(ChangesetReviewState::ChangesRequested),
                    "APPROVED" => states.push(ChangesetReviewState::Approved),
                    _ => {}
                }
            }
        }
        ChangesetMetadata::Gitlab(mr) => {
            // `Note.ToEvent()` only produces approval/unapproval for these
            // bodies; the first one wins.
            for note in &mr.notes {
                if !note.system {
                    continue;
                }
                match note.body.as_str() {
                    "approved this merge request" => {
                        return Ok(ChangesetReviewState::Approved)
                    }
                    "unapproved this merge request" => {
                        return Ok(ChangesetReviewState::ChangesRequested)
                    }
                    _ => {}
                }
            }
            return Ok(ChangesetReviewState::Pending);
        }
        ChangesetMetadata::BitbucketCloud(pr) => {
            for participant in &pr.participants {
                match participant.state.as_str() {
                    "approved" => states.push(ChangesetReviewState::Approved),
                    "changes_requested" => states.push(ChangesetReviewState::ChangesRequested),
                    _ => states.push(ChangesetReviewState::Pending),
                }
            }
        }
        ChangesetMetadata::AzureDevOps(pr) => {
            for reviewer in &pr.reviewers {
                match reviewer.vote {
                    10 => states.push(ChangesetReviewState::Approved),
                    5 | -5 | -10 => states.push(ChangesetReviewState::ChangesRequested),
                    _ => states.push(ChangesetReviewState::Pending),
                }
            }
        }
        ChangesetMetadata::Gerrit(change) => match &change.reviewers {
            None => states.push(ChangesetReviewState::Pending),
            Some(reviewers) => {
                for reviewer in reviewers {
                    for (key, val) in &reviewer.approvals {
                        if key == "Code-Review" {
                            match val.as_str() {
                                "+2" | "+1" => states.push(ChangesetReviewState::Approved),
                                " 0" => states.push(ChangesetReviewState::Pending),
                                "-1" | "-2" => {
                                    states.push(ChangesetReviewState::ChangesRequested)
                                }
                                _ => states.push(ChangesetReviewState::Pending),
                            }
                        }
                    }
                }
            }
        },
        ChangesetMetadata::Perforce(_) => states.push(ChangesetReviewState::Pending),
        ChangesetMetadata::Github(_) => {
            return Err("unknown changeset type".to_string());
        }
    }
    Ok(select_review_state(&states))
}

/// A `changesetStatesAtTime`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangesetStatesAtTime {
    pub t: OffsetDateTime,
    pub external_state: ChangesetExternalState,
    pub review_state: ChangesetReviewState,
}

/// `computeExternalState`.
pub fn compute_external_state(
    changeset: &Changeset,
    history: &[ChangesetStatesAtTime],
    repo: &RepoInfo,
) -> Result<ChangesetExternalState, String> {
    if repo.archived {
        return Ok(ChangesetExternalState::ReadOnly);
    }
    if history.is_empty() {
        return compute_single_changeset_external_state(changeset);
    }
    let newest = history.last().expect("non-empty");
    if changeset.updated_at > newest.t {
        return compute_single_changeset_external_state(changeset);
    }
    Ok(newest.external_state)
}

/// `computeReviewState`.
pub fn compute_review_state(
    changeset: &Changeset,
    history: &[ChangesetStatesAtTime],
) -> Result<ChangesetReviewState, String> {
    if changeset.external_service_type == "github" {
        if let ChangesetMetadata::Github(pr) = &changeset.metadata {
            return Ok(compute_github_review_state(pr));
        }
        // The baseline type-asserts the metadata and would panic; treat the
        // impossible combination as the single-changeset path.
    }
    if history.is_empty() {
        return compute_single_changeset_review_state(changeset);
    }
    let newest = history.last().expect("non-empty");
    if changeset.updated_at > newest.t {
        return compute_single_changeset_review_state(changeset);
    }
    Ok(newest.review_state)
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

/// `reduceReviewStates`.
pub fn reduce_review_states(
    states_by_author: &BTreeMap<String, ChangesetReviewState>,
) -> ChangesetReviewState {
    let states: Vec<ChangesetReviewState> = states_by_author.values().copied().collect();
    select_review_state(&states)
}

/// `initialExternalState`.
pub fn initial_external_state(
    changeset: &Changeset,
    events: &[ChangesetEvent],
) -> ChangesetExternalState {
    let mut open = true;
    match &changeset.metadata {
        ChangesetMetadata::Github(pr) => {
            if pr.is_draft {
                open = false;
            }
        }
        ChangesetMetadata::Gitlab(mr) => {
            if mr.work_in_progress {
                open = false;
            }
        }
        ChangesetMetadata::AzureDevOps(pr) => {
            if pr.is_draft {
                open = false;
            }
        }
        ChangesetMetadata::Gerrit(change) => {
            if change.work_in_progress {
                open = false;
            }
        }
        _ => return ChangesetExternalState::Open,
    }
    // Walk backwards in time.
    for e in events.iter().rev() {
        match e.kind.as_str() {
            kinds::GITLAB_UNMARK_WIP | kinds::GITHUB_READY_FOR_REVIEW => open = false,
            kinds::GITLAB_MARK_WIP | kinds::GITHUB_CONVERT_TO_DRAFT => open = true,
            _ => {}
        }
    }
    if open {
        ChangesetExternalState::Open
    } else {
        ChangesetExternalState::Draft
    }
}

/// `computeHistory`. `events` must be sorted by [`event_timestamp`].
pub fn compute_history(
    changeset: &Changeset,
    events: &[ChangesetEvent],
) -> Result<Vec<ChangesetStatesAtTime>, String> {
    if !events_sorted(events) {
        return Err("changeset events not sorted".to_string());
    }

    let mut states: Vec<ChangesetStatesAtTime> = Vec::new();
    let mut current_ext_state = initial_external_state(changeset, events);
    let mut current_review_state = ChangesetReviewState::Pending;
    let mut last_review_by_author: BTreeMap<String, ChangesetReviewState> = BTreeMap::new();
    let mut is_draft = current_ext_state == ChangesetExternalState::Draft;

    let opened_at = changeset
        .external_created_at()
        .ok_or_else(|| "changeset ExternalCreatedAt has zero value".to_string())?;
    states.push(ChangesetStatesAtTime {
        t: opened_at,
        external_state: current_ext_state,
        review_state: current_review_state,
    });

    for e in events {
        let Some(et) = event_timestamp(e) else {
            continue;
        };

        match e.kind.as_str() {
            kinds::GITHUB_CLOSED | kinds::BBS_DECLINED | kinds::GITLAB_CLOSED => {
                if current_ext_state != ChangesetExternalState::Merged
                    && current_ext_state != ChangesetExternalState::ReadOnly
                {
                    current_ext_state = ChangesetExternalState::Closed;
                    states.push(ChangesetStatesAtTime {
                        t: et,
                        external_state: current_ext_state,
                        review_state: current_review_state,
                    });
                }
            }
            kinds::BBC_PR_REJECTED => {
                if current_ext_state != ChangesetExternalState::Merged
                    && current_ext_state != ChangesetExternalState::ReadOnly
                {
                    current_ext_state = ChangesetExternalState::Closed;
                    states.push(ChangesetStatesAtTime {
                        t: et,
                        external_state: current_ext_state,
                        review_state: current_review_state,
                    });
                }
            }
            kinds::GITHUB_MERGED
            | kinds::BBS_MERGED
            | kinds::GITLAB_MERGED
            | kinds::BBC_PR_FULFILLED
            | kinds::AZURE_PR_MERGED => {
                current_ext_state = ChangesetExternalState::Merged;
                states.push(ChangesetStatesAtTime {
                    t: et,
                    external_state: current_ext_state,
                    review_state: current_review_state,
                });
            }
            kinds::GITLAB_MARK_WIP => {
                is_draft = true;
                if current_ext_state == ChangesetExternalState::Open {
                    current_ext_state = ChangesetExternalState::Draft;
                    states.push(ChangesetStatesAtTime {
                        t: et,
                        external_state: current_ext_state,
                        review_state: current_review_state,
                    });
                }
            }
            kinds::GITHUB_CONVERT_TO_DRAFT => {
                is_draft = true;
                if current_ext_state != ChangesetExternalState::Merged
                    && current_ext_state != ChangesetExternalState::ReadOnly
                {
                    current_ext_state = ChangesetExternalState::Draft;
                    states.push(ChangesetStatesAtTime {
                        t: et,
                        external_state: current_ext_state,
                        review_state: current_review_state,
                    });
                }
            }
            kinds::GITLAB_UNMARK_WIP | kinds::GITHUB_READY_FOR_REVIEW => {
                is_draft = false;
                if current_ext_state == ChangesetExternalState::Draft {
                    current_ext_state = ChangesetExternalState::Open;
                    states.push(ChangesetStatesAtTime {
                        t: et,
                        external_state: current_ext_state,
                        review_state: current_review_state,
                    });
                }
            }
            kinds::GITHUB_REOPENED | kinds::BBS_REOPENED | kinds::GITLAB_REOPENED => {
                if current_ext_state != ChangesetExternalState::Merged
                    && current_ext_state != ChangesetExternalState::ReadOnly
                {
                    current_ext_state = if is_draft {
                        ChangesetExternalState::Draft
                    } else {
                        ChangesetExternalState::Open
                    };
                    states.push(ChangesetStatesAtTime {
                        t: et,
                        external_state: current_ext_state,
                        review_state: current_review_state,
                    });
                }
            }
            kinds::GITHUB_REVIEWED
            | kinds::BBS_APPROVED
            | kinds::BBS_REVIEWED
            | kinds::GITLAB_APPROVED
            | kinds::BBC_APPROVED
            | kinds::BBC_PR_APPROVED
            | kinds::AZURE_PR_APPROVED => {
                let s = event_review_state(e);
                if s != ChangesetReviewState::Approved
                    && s != ChangesetReviewState::ChangesRequested
                    && s != ChangesetReviewState::Dismissed
                {
                    continue;
                }
                let author = event_review_author(e);
                if author.is_empty() {
                    continue;
                }
                let old_review_state = current_review_state;
                if s == ChangesetReviewState::Dismissed {
                    last_review_by_author.remove(&author);
                } else {
                    last_review_by_author.insert(author, s);
                }
                let new_review_state = reduce_review_states(&last_review_by_author);
                if new_review_state != old_review_state {
                    current_review_state = new_review_state;
                    states.push(ChangesetStatesAtTime {
                        t: et,
                        external_state: current_ext_state,
                        review_state: current_review_state,
                    });
                }
            }
            kinds::BBS_UNAPPROVED
            | kinds::BBS_DISMISSED
            | kinds::GITLAB_UNAPPROVED
            | kinds::BBC_PR_CHANGES_REQUEST_REMOVED
            | kinds::BBC_PR_UNAPPROVED => {
                let author = event_review_author(e);
                if author.is_empty() {
                    continue;
                }
                if e.kind == kinds::BBS_UNAPPROVED {
                    match last_review_by_author.get(&author) {
                        Some(state) if *state == ChangesetReviewState::Approved => {}
                        _ => continue,
                    }
                }
                if e.kind == kinds::BBS_DISMISSED {
                    match last_review_by_author.get(&author) {
                        Some(state) if *state == ChangesetReviewState::ChangesRequested => {}
                        _ => continue,
                    }
                }
                let old_review_state = current_review_state;
                last_review_by_author.remove(&author);
                let new_review_state = reduce_review_states(&last_review_by_author);
                if new_review_state != old_review_state {
                    current_review_state = new_review_state;
                    states.push(ChangesetStatesAtTime {
                        t: et,
                        external_state: current_ext_state,
                        review_state: current_review_state,
                    });
                }
            }
            kinds::AZURE_PR_REJECTED
            | kinds::AZURE_PR_APPROVED_WITH_SUGGESTIONS
            | kinds::AZURE_PR_WAITING_FOR_AUTHOR => {
                current_review_state = ChangesetReviewState::ChangesRequested;
                let author = event_review_author(e);
                last_review_by_author.insert(author, current_review_state);
                states.push(ChangesetStatesAtTime {
                    t: et,
                    external_state: current_ext_state,
                    review_state: current_review_state,
                });
            }
            _ => {}
        }
    }

    if let Some(deleted_at) = changeset.external_deleted_at {
        current_ext_state = ChangesetExternalState::Closed;
        states.push(ChangesetStatesAtTime {
            t: deleted_at,
            external_state: current_ext_state,
            review_state: current_review_state,
        });
    }

    Ok(states)
}

/// `changesetHistory.StatesAtTime`.
pub fn states_at_time(
    history: &[ChangesetStatesAtTime],
    t: OffsetDateTime,
) -> Option<ChangesetStatesAtTime> {
    if history.is_empty() {
        return None;
    }
    let mut found: Option<ChangesetStatesAtTime> = None;
    for s in history {
        if s.t > t {
            break;
        }
        found = Some(*s);
    }
    found
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

/// `ComputeLabels`. `changeset` may be `None`, matching a nil receiver.
pub fn compute_labels(
    changeset: Option<&Changeset>,
    events: &[ChangesetEvent],
) -> Vec<ChangesetLabel> {
    let mut set: BTreeMap<String, ChangesetLabel> = BTreeMap::new();
    let mut since: Option<OffsetDateTime> = None;
    if let Some(c) = changeset {
        for l in c.labels() {
            set.insert(l.name.clone(), l);
        }
        since = Some(c.updated_at);
    }
    for e in events {
        if let EventMetadata::GithubLabel {
            label,
            created_at,
            removed,
        } = &e.metadata
        {
            if let Some(since) = since {
                if *created_at < since {
                    continue;
                }
            }
            if *removed {
                set.remove(&label.name);
            } else {
                set.insert(label.name.clone(), label.clone());
            }
        }
    }
    set.into_values().collect()
}

// ---------------------------------------------------------------------------
// Counts
// ---------------------------------------------------------------------------

/// `timestampCount`.
pub const TIMESTAMP_COUNT: i64 = 150;

/// `ChangesetCounts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangesetCounts {
    pub time: OffsetDateTime,
    pub total: i32,
    pub merged: i32,
    pub closed: i32,
    pub draft: i32,
    pub open: i32,
    pub open_approved: i32,
    pub open_changes_requested: i32,
    pub open_pending: i32,
}

/// `GenerateTimestamps`.
pub fn generate_timestamps(start: OffsetDateTime, end: OffsetDateTime) -> Vec<OffsetDateTime> {
    if end < start {
        return Vec::new();
    }
    let total_ns = (end - start).whole_nanoseconds();
    let step_ns = (total_ns / i128::from(TIMESTAMP_COUNT)) as i64;
    let step = time::Duration::nanoseconds(step_ns);
    let mut ts: Vec<OffsetDateTime> = Vec::new();
    let mut t = end;
    loop {
        if t < start {
            break;
        }
        ts.push(t);
        if step <= time::Duration::ZERO {
            break;
        }
        t -= step;
    }
    ts.reverse();
    ts
}

/// `CalcCounts`. `events` are expected to be pre-sorted (as in the baseline).
pub fn calc_counts(
    start: OffsetDateTime,
    end: OffsetDateTime,
    changesets: &[Changeset],
    events: &[ChangesetEvent],
) -> Result<Vec<ChangesetCounts>, String> {
    let ts = generate_timestamps(start, end);
    let mut counts: Vec<ChangesetCounts> = ts
        .into_iter()
        .map(|time| ChangesetCounts {
            time,
            total: 0,
            merged: 0,
            closed: 0,
            draft: 0,
            open: 0,
            open_approved: 0,
            open_changes_requested: 0,
            open_pending: 0,
        })
        .collect();

    let mut by_changeset_id: HashMap<i64, Vec<&ChangesetEvent>> = HashMap::new();
    for e in events {
        by_changeset_id
            .entry(e.changeset_id)
            .or_default()
            .push(e);
    }

    for changeset in changesets {
        let empty: Vec<&ChangesetEvent> = Vec::new();
        let cs_events = by_changeset_id
            .get(&changeset.id)
            .unwrap_or(&empty)
            .iter()
            .map(|e| (*e).clone())
            .collect::<Vec<ChangesetEvent>>();
        let history = compute_history(changeset, &cs_events)?;

        for c in &mut counts {
            let Some(states) = states_at_time(&history, c.time) else {
                continue;
            };
            c.total += 1;
            match states.external_state {
                ChangesetExternalState::Draft => c.draft += 1,
                ChangesetExternalState::Open => {
                    c.open += 1;
                    match states.review_state {
                        ChangesetReviewState::Pending => c.open_pending += 1,
                        ChangesetReviewState::Approved => c.open_approved += 1,
                        ChangesetReviewState::ChangesRequested => c.open_changes_requested += 1,
                        _ => {}
                    }
                }
                ChangesetExternalState::Merged => c.merged += 1,
                ChangesetExternalState::Closed | ChangesetExternalState::ReadOnly => c.closed += 1,
                ChangesetExternalState::Deleted => {}
            }
        }
    }

    Ok(counts)
}

// ---------------------------------------------------------------------------
// Gitserver seam + SetDerivedState
// ---------------------------------------------------------------------------

/// The gitserver operations `computeSyncState`/`computeDiffStat` need.
pub trait GitserverClient: Send + Sync {
    /// `ResolveRevision(..., EnsureRevision: true)`.
    fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, String>;
    /// The aggregated `Added += Added+Changed`, `Deleted += Deleted+Changed`
    /// that `computeDiffStat` accumulates from the diff iterator.
    fn diff_stat(&self, repo: &str, base: &str, head: &str) -> Result<DiffStat, String>;
}

fn compute_rev(
    client: &dyn GitserverClient,
    repo: &str,
    oid: &str,
    reference: &str,
) -> Result<String, String> {
    let mut rev = oid.to_string();
    if rev.is_empty() {
        rev = reference.to_string();
    }
    client.resolve_revision(repo, &rev)
}

/// `computeSyncState`.
pub fn compute_sync_state(
    client: &dyn GitserverClient,
    changeset: &Changeset,
    repo: &str,
) -> Result<ChangesetSyncState, String> {
    let base = compute_rev(
        client,
        repo,
        &changeset.base_ref_oid,
        &changeset.base_ref,
    )?;
    let head = compute_rev(
        client,
        repo,
        &changeset.head_ref_oid,
        &changeset.head_ref,
    )?;
    Ok(ChangesetSyncState {
        base_ref_oid: base,
        head_ref_oid: head,
        is_complete: changeset.complete(),
    })
}

/// `computeDiffStat`.
pub fn compute_diff_stat(
    client: &dyn GitserverClient,
    changeset: &Changeset,
    repo: &str,
) -> Result<DiffStat, String> {
    if changeset.sync_state.base_ref_oid == changeset.sync_state.head_ref_oid {
        return changeset
            .diff_stat
            .ok_or_else(|| "diffstat unavailable".to_string());
    }
    client.diff_stat(
        repo,
        &changeset.sync_state.base_ref_oid,
        &changeset.sync_state.head_ref_oid,
    )
}

fn warn(_message: &str) {}

/// `state.SetDerivedState`.
///
/// The pure state is always computed. The final sync-state/diffstat step only
/// runs when a [`GitserverClient`] is injected; without one it returns an
/// error after updating the pure fields, mirroring the baseline's
/// "log and return" at the gitserver boundary. Individual sub-errors are
/// logged-and-skipped like the baseline.
pub fn set_derived_state(
    client: Option<&dyn GitserverClient>,
    changeset: &mut Changeset,
    events: &[ChangesetEvent],
    repo: &RepoInfo,
) -> Result<(), String> {
    let mut sorted: Vec<ChangesetEvent> = events.to_vec();
    sort_events(&mut sorted);

    changeset.external_check_state = Some(compute_check_state(changeset, &sorted));

    let history = match compute_history(changeset, &sorted) {
        Ok(history) => history,
        Err(err) => {
            warn(&format!("Computing changeset history: {err}"));
            return Ok(());
        }
    };

    match compute_external_state(changeset, &history, repo) {
        Ok(state) => changeset.external_state = Some(state),
        Err(err) => warn(&format!("Computing external changeset state: {err}")),
    }
    match compute_review_state(changeset, &history) {
        Ok(state) => changeset.external_review_state = Some(state),
        Err(err) => warn(&format!("Computing changeset review state: {err}")),
    }

    // If the changeset was complete last sync and is still complete, we do not
    // need to touch gitserver (the head OID may no longer be available).
    if changeset.sync_state.is_complete && changeset.complete() {
        return Ok(());
    }

    let Some(client) = client else {
        return Err(
            "computing sync state requires a gitserver client (seam)".to_string(),
        );
    };

    let old_state = changeset.sync_state.clone();
    let new_state = match compute_sync_state(client, changeset, &repo.name) {
        Ok(state) => state,
        Err(err) => return Err(format!("computing sync state: {err}")),
    };
    changeset.sync_state = new_state;

    if old_state != changeset.sync_state {
        match compute_diff_stat(client, changeset, &repo.name) {
            Ok(stat) => changeset.diff_stat = Some(stat),
            Err(err) => warn(&format!("Computing diffstat: {err}")),
        }
    }

    Ok(())
}
