#![forbid(unsafe_code)]

//! `internal/codeintel/policies/matcher.go`: the configuration-policy matcher
//! used by the upload expirer (`policies.NewMatcher(gitserverClient,
//! policies.RetentionExtractor, true, false)`).
//!
//! [`PolicyMatcher::commits_described_by_policy`] returns the set of commits
//! in a repository that are described by at least one data-retention policy,
//! together with the matching policy metadata. The three matching arms are
//! exactly the baseline's:
//!
//! 1. **tags** — `ListRefs(headsOnly, tagsOnly)` refs whose short name matches
//!    a `GIT_TAG` policy pattern;
//! 2. **branch heads** — `GIT_TREE` policies matched against branch short
//!    names. When `include_tip_of_default_branch` is set, the tip of the
//!    default branch also gets an entry with a `nil` policy id and no
//!    duration. Policies with `RetainIntermediateCommits` enqueue a
//!    `Commits` request for the branch (`HEAD` for the default branch,
//!    `HEAD..<tip>` otherwise) and every commit on it receives a match;
//! 3. **commit policies** — `GIT_COMMIT` patterns are resolved through
//!    `GetCommit`; a `RevisionNotFound` is skipped, any other error aborts.
//!
//! `filter_by_created_date` is `false` for data retention (the policy
//! duration is compared against the *upload* date by the expirer, not against
//! the commit date), so the date filters in this module are dead code on the
//! production path; they are kept for fidelity and for future users of the
//! matcher (auto-indexing passes `true`).
//!
//! The baseline iterates Go maps in `matchCommitsOnBranch`, so ties between
//! two branches matched by the same intermediate-commit policy are
//! non-deterministic there; this port uses a `BTreeMap` and is deterministic.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use sg_codeintel::glob::Glob;
use sg_db::codeintel_policies::ConfigurationPolicy;
use time::{Duration, OffsetDateTime};

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

/// The gitserver operations `internal/codeintel/policies/matcher.go` uses,
/// reduced to the matcher's inputs and outputs. The worker job implements it
/// on top of [`sg_git::GitserverClient`]; the differential harness supplies a
/// deterministic fake, mirroring `gitserver.MockClient`.
#[async_trait]
pub trait MatcherGit: Send + Sync {
    /// `ListRefs(repo, ListRefsOpts{HeadsOnly: true, TagsOnly: true,
    /// PointsAtCommit: filterCommits})`.
    async fn list_refs(
        &self,
        repo: &str,
        points_at_commit: &[String],
    ) -> Result<Vec<MatcherRef>, String>;

    /// `GetCommit(repo, commit)`.
    async fn get_commit(&self, repo: &str, commit: &str) -> Result<MatcherCommit, GetCommitError>;

    /// `Commits(repo, CommitsOptions{Ranges, After})`.
    async fn commits(
        &self,
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

/// `policies.RetentionExtractor`: `(RetentionDuration,
/// RetainIntermediateCommits)`.
pub fn retention_extractor(policy: &ConfigurationPolicy) -> (Option<Duration>, bool) {
    // `optionalDuration`: hours → `time.Duration`; `NULL` stays `nil` (a
    // stored `0` is a real zero duration).
    let duration = policy
        .retention_duration_hours
        .map(|hours| Duration::hours(i64::from(hours)));
    (duration, policy.retain_intermediate_commits)
}

/// `policies.Matcher` (`includeTipOfDefaultBranch`/`filterByCreatedDate`
/// supplied by the constructor).
pub struct PolicyMatcher<'a, G: MatcherGit + ?Sized> {
    git: &'a G,
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
        include_tip_of_default_branch: bool,
        filter_by_created_date: bool,
    ) -> Self {
        Self {
            git,
            include_tip_of_default_branch,
            filter_by_created_date,
        }
    }

    /// `Matcher.CommitsDescribedByPolicy`.
    pub async fn commits_described_by_policy(
        &self,
        _repository_id: i32,
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
            .list_refs(repo_name, filter_commits)
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
        self.match_commits_on_branch(repo_name, &mut commit_map, &branch_requests, now)
            .await?;

        // Match commits via rev-parse.
        self.match_commit_policies(repo_name, policies, &mut commit_map, now)
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
            let (policy_duration, _) = retention_extractor(policy);
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
            let (policy_duration, _) = retention_extractor(policy);
            commit_map
                .entry(git_ref.commit.clone())
                .or_default()
                .push(PolicyMatch {
                    name: git_ref.short_name.clone(),
                    policy_id: Some(policy.id),
                    policy_duration,
                    committed_at: git_ref.created_at,
                });

            if retention_extractor(policy).1 {
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
                .commits(repo_name, &[range], max_commit_age)
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
        repo_name: &str,
        policies: &[ConfigurationPolicy],
        commit_map: &mut BTreeMap<String, Vec<PolicyMatch>>,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        for policy in policies {
            if policy.policy_type != GIT_OBJECT_TYPE_COMMIT {
                continue;
            }
            let commit = match self.git.get_commit(repo_name, &policy.pattern).await {
                Ok(commit) => commit,
                Err(GetCommitError::RevisionNotFound) => continue,
                Err(GetCommitError::Other(err)) => return Err(err),
            };

            let (policy_duration, _) = retention_extractor(policy);
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
            if let (Some(duration), true) = (retention_extractor(policy).0, true) {
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
