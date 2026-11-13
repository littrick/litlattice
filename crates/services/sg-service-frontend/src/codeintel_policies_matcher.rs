#![forbid(unsafe_code)]

//! `internal/codeintel/policies/matcher.go`: the configuration-policy matcher
//! shared by the uploads `retentionPolicyOverview` resolver
//! (`RetentionExtractor`, tip-of-default-branch, no created-date filter) and
//! `previewGitObjectFilter` (no-op extractor).
//!
//! [`PolicyMatcher::commits_described_by_policy`] returns, for a repository,
//! the commits described by at least one configuration policy together with
//! the matching policy metadata. The three arms mirror the baseline:
//!
//! 1. **tags** — `ListRefs(headsOnly, tagsOnly, pointsAtCommit)` refs whose
//!    short name matches a `GIT_TAG` policy pattern;
//! 2. **branch heads** — `GIT_TREE` policies matched against branch short
//!    names. With `include_tip_of_default_branch`, the tip of the default
//!    branch also gets an entry with a `nil` policy id and no duration.
//!    Policies whose extractor reports `include_intermediate_commits` queue a
//!    `Commits` request (`HEAD` for the default branch, `HEAD..<tip>`
//!    otherwise) and every returned commit receives a match;
//! 3. **commit policies** — `GIT_COMMIT` patterns resolved through
//!    `GetCommit`; a `RevisionNotFound` is skipped, any other error aborts.
//!
//! `filter_by_created_date` is `false` for data retention (the policy
//! duration is compared against the *upload* date by the caller) and `true`
//! for auto-indexing/syntactic indexing.
//!
//! The baseline iterates Go maps in `matchCommitsOnBranch`; this port uses a
//! `BTreeMap`, making tie order deterministic without changing the match set.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use sg_codeintel::glob::Glob;
use sg_db::codeintel_policies::ConfigurationPolicy;
use time::{Duration, OffsetDateTime};

use crate::git_graphql::{GitObjectSource, GitSourceError};

pub const GIT_OBJECT_TYPE_COMMIT: &str = "GIT_COMMIT";
pub const GIT_OBJECT_TYPE_TAG: &str = "GIT_TAG";
pub const GIT_OBJECT_TYPE_TREE: &str = "GIT_TREE";

/// `gitdomain.RefType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefType {
    Unknown,
    Branch,
    Tag,
}

/// The subset of `gitdomain.Ref` the matcher reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatcherRef {
    pub short_name: String,
    /// `ref.CommitID` (the commit the ref points at).
    pub commit: String,
    /// `ref.CreatedDate`.
    pub created_at: OffsetDateTime,
    pub ref_type: RefType,
    pub is_head: bool,
}

/// The subset of `gitdomain.Commit` the matcher reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatcherCommit {
    pub id: String,
    /// `commit.Committer.Date`.
    pub committer_date: OffsetDateTime,
}

/// `GetCommit` failures the matcher distinguishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetCommitError {
    /// `gitdomain.RevisionNotFoundError`; the policy is skipped.
    RevisionNotFound,
    Other(String),
}

/// The gitserver operations the baseline matcher uses, reduced to its inputs
/// and outputs. The production adapter is [`GitObjectSourceMatcher`]; the
/// differential harness supplies a deterministic fake, mirroring
/// `gitserver.MockClient`.
#[async_trait]
pub trait MatcherGit: Send + Sync {
    /// `ListRefs(repo, ListRefsOpts{HeadsOnly: true, TagsOnly: true,
    /// PointsAtCommit: filter_commits})`.
    async fn list_refs(
        &self,
        repo_id: i32,
        repo: &str,
        points_at_commit: &[String],
    ) -> Result<Vec<MatcherRef>, String>;

    /// `GetCommit(repo, commit)`.
    async fn get_commit(
        &self,
        repo_id: i32,
        repo: &str,
        commit: &str,
    ) -> Result<MatcherCommit, GetCommitError>;

    /// `Commits(repo, CommitsOptions{Ranges, After})`.
    async fn commits(
        &self,
        repo_id: i32,
        repo: &str,
        ranges: &[String],
        after: Option<OffsetDateTime>,
    ) -> Result<Vec<MatcherCommit>, String>;
}

/// `policies.PolicyMatch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyMatch {
    pub name: String,
    /// `nil` for the tip-of-default-branch match.
    pub policy_id: Option<i32>,
    /// `nil` when the policy is unbounded.
    pub policy_duration: Option<Duration>,
    pub committed_at: OffsetDateTime,
}

/// `policies.Extractor`: which policy fields supply the max age and the
/// intermediate-commit flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Extractor {
    /// `policies.NoopExtractor`: always `(nil, false)`.
    Noop,
    /// `policies.RetentionExtractor`: `(RetentionDuration,
    /// RetainIntermediateCommits)`.
    Retention,
    /// `policies.IndexingExtractor`: `(IndexCommitMaxAge,
    /// IndexIntermediateCommits)`.
    Indexing,
}

impl Extractor {
    /// `(maxAge, includeIntermediateCommits)` for one policy.
    fn extract(self, policy: &ConfigurationPolicy) -> (Option<Duration>, bool) {
        // `optionalDuration`: hours → `time.Duration`; `NULL` stays `nil` (a
        // stored `0` is a real zero duration).
        match self {
            Self::Noop => (None, false),
            Self::Retention => (
                policy
                    .retention_duration_hours
                    .map(|hours| Duration::hours(i64::from(hours))),
                policy.retain_intermediate_commits,
            ),
            Self::Indexing => (
                policy
                    .index_commit_max_age_hours
                    .map(|hours| Duration::hours(i64::from(hours))),
                policy.index_intermediate_commits,
            ),
        }
    }
}

/// `policies.matcher` (`extractor`, `includeTipOfDefaultBranch` and
/// `filterByCreatedDate` supplied by the constructor).
pub struct PolicyMatcher<'a, G: MatcherGit + ?Sized> {
    git: &'a G,
    extractor: Extractor,
    include_tip_of_default_branch: bool,
    filter_by_created_date: bool,
}

/// `matcherContext.branchRequests` entry.
struct BranchRequestMeta {
    is_default_branch: bool,
    commit_id: String,
    policy_duration_by_ids: BTreeMap<i32, Option<Duration>>,
}

impl<'a, G: MatcherGit + ?Sized> PolicyMatcher<'a, G> {
    /// `policies.NewMatcher`.
    pub fn new(
        git: &'a G,
        extractor: Extractor,
        include_tip_of_default_branch: bool,
        filter_by_created_date: bool,
    ) -> Self {
        Self {
            git,
            extractor,
            include_tip_of_default_branch,
            filter_by_created_date,
        }
    }

    /// `Matcher.CommitsDescribedByPolicy`.
    pub async fn commits_described_by_policy(
        &self,
        repository_id: i32,
        repo_name: &str,
        policies: &[ConfigurationPolicy],
        now: OffsetDateTime,
        filter_commits: &[String],
    ) -> Result<BTreeMap<String, Vec<PolicyMatch>>, String> {
        if policies.is_empty() && !self.include_tip_of_default_branch {
            return Ok(BTreeMap::new());
        }

        let patterns = compile_patterns(policies)?;

        let mut commit_map: BTreeMap<String, Vec<PolicyMatch>> = BTreeMap::new();
        let mut branch_requests: BTreeMap<String, BranchRequestMeta> = BTreeMap::new();

        let refs = self
            .git
            .list_refs(repository_id, repo_name, filter_commits)
            .await
            .map_err(|err| format!("gitserver.ListRefs: {err}"))?;

        for git_ref in &refs {
            match git_ref.ref_type {
                RefType::Tag => {
                    self.match_tagged_commits(policies, &patterns, &mut commit_map, git_ref, now);
                }
                RefType::Branch => {
                    self.match_branch_heads(
                        policies,
                        &patterns,
                        &mut commit_map,
                        &mut branch_requests,
                        git_ref,
                        now,
                    );
                }
                RefType::Unknown => {}
            }
        }

        // Match commits on branches but not at tip.
        self.match_commits_on_branch(
            repository_id,
            repo_name,
            &mut commit_map,
            &branch_requests,
            now,
        )
        .await?;

        // Match commits via rev-parse.
        self.match_commit_policies(repository_id, repo_name, policies, &mut commit_map, now)
            .await?;

        Ok(commit_map)
    }

    /// `matchTaggedCommits`.
    fn match_tagged_commits(
        &self,
        policies: &[ConfigurationPolicy],
        patterns: &HashMap<String, Glob>,
        commit_map: &mut BTreeMap<String, Vec<PolicyMatch>>,
        git_ref: &MatcherRef,
        now: OffsetDateTime,
    ) {
        for policy in policies {
            if policy.policy_type != GIT_OBJECT_TYPE_TAG
                || !self.policy_matches_ref(policy, patterns, git_ref, now)
            {
                continue;
            }
            let (policy_duration, _) = self.extractor.extract(policy);
            commit_map
                .entry(git_ref.commit.clone())
                .or_default()
                .push(PolicyMatch {
                    name: git_ref.short_name.clone(),
                    policy_id: Some(policy.id),
                    policy_duration,
                    committed_at: git_ref.created_at,
                });
        }
    }

    /// `matchBranchHeads`.
    fn match_branch_heads(
        &self,
        policies: &[ConfigurationPolicy],
        patterns: &HashMap<String, Glob>,
        commit_map: &mut BTreeMap<String, Vec<PolicyMatch>>,
        branch_requests: &mut BTreeMap<String, BranchRequestMeta>,
        git_ref: &MatcherRef,
        now: OffsetDateTime,
    ) {
        if git_ref.is_head && self.include_tip_of_default_branch {
            commit_map
                .entry(git_ref.commit.clone())
                .or_default()
                .push(PolicyMatch {
                    name: git_ref.short_name.clone(),
                    policy_id: None,
                    policy_duration: None,
                    committed_at: git_ref.created_at,
                });
        }

        for policy in policies {
            if policy.policy_type != GIT_OBJECT_TYPE_TREE
                || !self.policy_matches_ref(policy, patterns, git_ref, now)
            {
                continue;
            }
            let (policy_duration, include_intermediate_commits) = self.extractor.extract(policy);
            commit_map
                .entry(git_ref.commit.clone())
                .or_default()
                .push(PolicyMatch {
                    name: git_ref.short_name.clone(),
                    policy_id: Some(policy.id),
                    policy_duration,
                    committed_at: git_ref.created_at,
                });

            if include_intermediate_commits {
                let meta = branch_requests
                    .entry(git_ref.short_name.clone())
                    .or_insert_with(|| BranchRequestMeta {
                        is_default_branch: false,
                        commit_id: String::new(),
                        policy_duration_by_ids: BTreeMap::new(),
                    });
                meta.policy_duration_by_ids
                    .insert(policy.id, policy_duration);
                meta.is_default_branch |= git_ref.is_head;
                meta.commit_id.clone_from(&git_ref.commit);
            }
        }
    }

    /// `matchCommitsOnBranch` + `commitsUniqueToBranch`.
    async fn match_commits_on_branch(
        &self,
        repository_id: i32,
        repo_name: &str,
        commit_map: &mut BTreeMap<String, Vec<PolicyMatch>>,
        branch_requests: &BTreeMap<String, BranchRequestMeta>,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        for (branch_name, request) in branch_requests {
            let max_commit_age = if self.filter_by_created_date {
                get_max_age(&request.policy_duration_by_ids, now)
            } else {
                // Do not filter out any commits by date.
                None
            };

            let range = if request.is_default_branch {
                "HEAD".to_owned()
            } else {
                format!("HEAD..{}", request.commit_id)
            };
            let commits = self
                .git
                .commits(repository_id, repo_name, &[range], max_commit_age)
                .await
                .map_err(|err| format!("gitserver.CommitsUniqueToBranch: {err}"))?;

            for commit in &commits {
                'policy_loop: for (policy_id, policy_duration) in &request.policy_duration_by_ids {
                    if let Some(matches) = commit_map.get(&commit.id) {
                        if matches
                            .iter()
                            .any(|matched| matched.policy_id == Some(*policy_id))
                        {
                            // Skip duplicates (can happen at head of branch).
                            continue 'policy_loop;
                        }
                    }

                    if self.filter_by_created_date {
                        if let Some(duration) = policy_duration {
                            if now - commit.committer_date > *duration {
                                // Policy duration was less than max age and
                                // re-check failed.
                                continue 'policy_loop;
                            }
                        }
                    }

                    commit_map
                        .entry(commit.id.clone())
                        .or_default()
                        .push(PolicyMatch {
                            name: branch_name.clone(),
                            policy_id: Some(*policy_id),
                            policy_duration: *policy_duration,
                            committed_at: commit.committer_date,
                        });
                }
            }
        }
        Ok(())
    }

    /// `matchCommitPolicies`.
    async fn match_commit_policies(
        &self,
        repository_id: i32,
        repo_name: &str,
        policies: &[ConfigurationPolicy],
        commit_map: &mut BTreeMap<String, Vec<PolicyMatch>>,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        for policy in policies {
            if policy.policy_type != GIT_OBJECT_TYPE_COMMIT {
                continue;
            }
            let commit = match self
                .git
                .get_commit(repository_id, repo_name, &policy.pattern)
                .await
            {
                Ok(commit) => commit,
                Err(GetCommitError::RevisionNotFound) => continue,
                Err(GetCommitError::Other(err)) => return Err(err),
            };

            let (policy_duration, _) = self.extractor.extract(policy);
            if self.filter_by_created_date {
                if let Some(duration) = policy_duration {
                    if now - commit.committer_date > duration {
                        continue;
                    }
                }
            }

            // The baseline keys the map by the *pattern* (`policy.Pattern`),
            // not by the resolved commit id.
            commit_map
                .entry(policy.pattern.clone())
                .or_default()
                .push(PolicyMatch {
                    name: commit.id.clone(),
                    policy_id: Some(policy.id),
                    policy_duration,
                    committed_at: commit.committer_date,
                });
        }
        Ok(())
    }

    /// `policyMatchesRef`.
    fn policy_matches_ref(
        &self,
        policy: &ConfigurationPolicy,
        patterns: &HashMap<String, Glob>,
        git_ref: &MatcherRef,
        now: OffsetDateTime,
    ) -> bool {
        let Some(pattern) = patterns.get(&policy.pattern) else {
            // `compilePatterns` compiles every pattern of a branch/tag policy;
            // a missing entry means the policy type is not branch/tag, which
            // the callers already filter on.
            return false;
        };
        if !pattern.matches(&git_ref.short_name) {
            return false;
        }
        if self.filter_by_created_date {
            if let Some(duration) = self.extractor.extract(policy).0 {
                if now - git_ref.created_at > duration {
                    return false;
                }
            }
        }
        true
    }
}

/// `compilePatterns`: one glob per distinct pattern; commit-type policies are
/// skipped.
fn compile_patterns(policies: &[ConfigurationPolicy]) -> Result<HashMap<String, Glob>, String> {
    let mut patterns = HashMap::new();
    for policy in policies {
        if patterns.contains_key(&policy.pattern) || policy.policy_type == GIT_OBJECT_TYPE_COMMIT {
            continue;
        }
        let pattern = Glob::compile(&policy.pattern).map_err(|err| {
            format!(
                "failed to compile glob pattern `{}` in configuration policy {}: {err}",
                policy.pattern, policy.id
            )
        })?;
        patterns.insert(policy.pattern.clone(), pattern);
    }
    Ok(patterns)
}

/// `getMaxAge`: `nil` if any duration is unbounded, otherwise `now - max`.
fn get_max_age(
    policy_duration_by_ids: &BTreeMap<i32, Option<Duration>>,
    now: OffsetDateTime,
) -> Option<OffsetDateTime> {
    let mut max_duration: Option<Duration> = None;
    for duration in policy_duration_by_ids.values() {
        let Some(duration) = duration else {
            // If any duration is nil, the policy is unbounded.
            return None;
        };
        if max_duration.is_none_or(|current| current < *duration) {
            max_duration = Some(*duration);
        }
    }
    max_duration.map(|duration| now - duration)
}

/// `(*timestamppb.Timestamp)(nil).AsTime()` is the Unix epoch; the matcher
/// only compares dates and subtracts them from `now`.
pub(crate) fn timestamp_to_offset(timestamp: Option<prost_types::Timestamp>) -> OffsetDateTime {
    let Some(timestamp) = timestamp else {
        return OffsetDateTime::UNIX_EPOCH;
    };
    OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .map(|value| value + time::Duration::nanoseconds(i64::from(timestamp.nanos)))
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn offset_to_timestamp(value: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond() as i32,
    }
}

/// [`MatcherGit`] over the frontend [`GitObjectSource`] seam.
pub struct GitObjectSourceMatcher<'a> {
    source: &'a dyn GitObjectSource,
}

impl<'a> GitObjectSourceMatcher<'a> {
    pub fn new(source: &'a dyn GitObjectSource) -> Self {
        Self { source }
    }
}

#[async_trait]
impl MatcherGit for GitObjectSourceMatcher<'_> {
    async fn list_refs(
        &self,
        repo_id: i32,
        repo: &str,
        points_at_commit: &[String],
    ) -> Result<Vec<MatcherRef>, String> {
        let refs = self
            .source
            .list_refs_with_points(repo_id, repo, true, true, points_at_commit)
            .await
            .map_err(|err| err.message)?;
        Ok(refs
            .into_iter()
            .map(|git_ref| MatcherRef {
                short_name: git_ref.short_name,
                commit: git_ref.target_commit,
                created_at: timestamp_to_offset(git_ref.created_at),
                ref_type: match git_ref.ref_type {
                    sg_git::RefType::Tag => RefType::Tag,
                    sg_git::RefType::Branch => RefType::Branch,
                    sg_git::RefType::Unknown => RefType::Unknown,
                },
                is_head: git_ref.is_head,
            })
            .collect())
    }

    async fn get_commit(
        &self,
        repo_id: i32,
        repo: &str,
        commit: &str,
    ) -> Result<MatcherCommit, GetCommitError> {
        match self.source.get_commit(repo_id, repo, commit).await {
            Ok(commit) => Ok(MatcherCommit {
                id: commit.oid,
                committer_date: timestamp_to_offset(
                    commit.committer.and_then(|signature| signature.date),
                ),
            }),
            Err(GitSourceError {
                revision_not_found: true,
                ..
            }) => Err(GetCommitError::RevisionNotFound),
            Err(err) => Err(GetCommitError::Other(err.message)),
        }
    }

    async fn commits(
        &self,
        repo_id: i32,
        repo: &str,
        ranges: &[String],
        after: Option<OffsetDateTime>,
    ) -> Result<Vec<MatcherCommit>, String> {
        let options = sg_git::CommitsOptions {
            ranges: ranges.to_vec(),
            after: after.map(offset_to_timestamp),
            ..Default::default()
        };
        let commits = self
            .source
            .commits(repo_id, repo, &options)
            .await
            .map_err(|err| err.message)?;
        Ok(commits
            .into_iter()
            .map(|commit| MatcherCommit {
                id: commit.oid,
                committer_date: timestamp_to_offset(commit.committer_date),
            })
            .collect())
    }
}
