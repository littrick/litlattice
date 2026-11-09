#![forbid(unsafe_code)]

//! `internal/insights/compression`: backfill query-plan compression.
//!
//! Given the sample times of a historical backfill, [`GitserverFilter`]
//! fetches the nearest commit for each sample time (newest first) and merges
//! sample times that resolve to the same commit into one [`QueryExecution`],
//! which records the shared sample times as [`QueryExecution::shared_recordings`].
//! An unreadable commit (error or no commits) falls back to an uncompressed
//! execution for that sample time rather than failing the plan.
//!
//! [`NoopFilter`] returns the uncompressed plan in input order. The baseline's
//! `NewGitserverFilter(logger, gitserverClient)` needs the gitserver commit
//! client (not ported yet), so the commit source is injected through
//! [`CommitFetcher`]; the logger is only used for debug output in the
//! baseline and is omitted.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use time::OffsetDateTime;

/// `gitdomain.Commit` restricted to what compression reads (`commits[0].ID`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub id: String,
}

/// The baseline's `commitFetcher` interface
/// (`internal/insights/gitserver.GitCommitClient.RecentCommits`).
#[async_trait]
pub trait CommitFetcher: Send + Sync {
    async fn recent_commits(
        &self,
        repo_name: &str,
        target: OffsetDateTime,
        revision: &str,
    ) -> Result<Vec<Commit>, String>;
}

/// The baseline's `compression.DataFrameFilter`.
#[async_trait]
pub trait DataFrameFilter: Send + Sync {
    async fn filter(&self, sample_times: Vec<OffsetDateTime>, repo_name: &str) -> BackfillPlan;
}

/// `store.RecordMode` (`RecordSeriesPoints` persist mode).
pub const RECORD_MODE: &str = "record";

/// One `store.RecordSeriesPointArgs`: the value recorded for an execution's
/// recording time (and each shared recording time).
#[derive(Debug, Clone, PartialEq)]
pub struct RecordingArgs {
    pub series_id: String,
    pub time: OffsetDateTime,
    pub value: f64,
    pub repo_name: Option<String>,
    pub repo_id: Option<i32>,
    pub persist_mode: String,
}

/// `compression.QueryExecution`.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryExecution {
    pub revision: String,
    pub recording_time: OffsetDateTime,
    pub shared_recordings: Vec<OffsetDateTime>,
}

impl QueryExecution {
    /// `QueryExecution.RecordCount`.
    pub fn record_count(&self) -> usize {
        self.shared_recordings.len() + 1
    }

    /// `QueryExecution.ToRecording`: one point for the execution time plus one
    /// per shared recording time, all with the same value.
    pub fn to_recording(
        &self,
        series_id: &str,
        repo_name: &str,
        repo_id: i32,
        value: f64,
    ) -> Vec<RecordingArgs> {
        let base = |recorded_at: OffsetDateTime| RecordingArgs {
            series_id: series_id.to_owned(),
            time: recorded_at,
            value,
            repo_name: Some(repo_name.to_owned()),
            repo_id: Some(repo_id),
            persist_mode: RECORD_MODE.to_owned(),
        };

        let mut args = Vec::with_capacity(self.record_count());
        args.push(base(self.recording_time));
        for shared_time in &self.shared_recordings {
            args.push(base(*shared_time));
        }
        args
    }
}

/// `compression.BackfillPlan`.
#[derive(Debug, Clone, PartialEq)]
pub struct BackfillPlan {
    pub executions: Vec<QueryExecution>,
    pub record_count: usize,
}

/// `compression.GitserverFilter` with the commit source injected.
pub struct GitserverFilter {
    commit_fetcher: Arc<dyn CommitFetcher>,
}

impl GitserverFilter {
    pub fn new(commit_fetcher: Arc<dyn CommitFetcher>) -> Self {
        Self { commit_fetcher }
    }
}

#[async_trait]
impl DataFrameFilter for GitserverFilter {
    /// `gitserverFilter.Filter`: sort the sample times newest first and merge
    /// executions whose nearest commit is the same.
    async fn filter(&self, mut sample_times: Vec<OffsetDateTime>, repo_name: &str) -> BackfillPlan {
        sample_times.sort_by(|a, b| b.cmp(a));

        let mut nodes: Vec<QueryExecution> = Vec::new();
        let mut executions: HashMap<String, QueryExecution> = HashMap::new();
        let mut prev = String::new();

        for sample_time in sample_times {
            let commit = self
                .commit_fetcher
                .recent_commits(repo_name, sample_time, &prev)
                .await;
            match commit {
                Ok(commits) if !commits.is_empty() => {
                    let commit_id = commits[0].id.clone();
                    if let Some(current) = executions.get_mut(&commit_id) {
                        // The existing execution is newer than this sample
                        // time; carry it as a shared recording of the current
                        // (older) one.
                        let previous_time = current.recording_time;
                        current.recording_time = sample_time;
                        current.shared_recordings.insert(0, previous_time);
                    } else {
                        executions.insert(
                            commit_id.clone(),
                            QueryExecution {
                                revision: commit_id.clone(),
                                recording_time: sample_time,
                                shared_recordings: Vec::new(),
                            },
                        );
                        prev = commit_id;
                    }
                }
                // An error or a missing commit falls back to an uncompressed
                // point for this sample time.
                _ => nodes.push(QueryExecution {
                    revision: String::new(),
                    recording_time: sample_time,
                    shared_recordings: Vec::new(),
                }),
            }
        }

        nodes.extend(executions.into_values());
        nodes.sort_by_key(|node| node.recording_time);

        BackfillPlan {
            record_count: nodes.len(),
            executions: nodes,
        }
    }
}

/// `compression.NoopFilter`.
pub struct NoopFilter;

#[async_trait]
impl DataFrameFilter for NoopFilter {
    async fn filter(&self, sample_times: Vec<OffsetDateTime>, _repo_name: &str) -> BackfillPlan {
        uncompressed_plan(sample_times)
    }
}

/// `uncompressedPlan`: one execution per sample time, in input order.
pub fn uncompressed_plan(sample_times: Vec<OffsetDateTime>) -> BackfillPlan {
    let executions: Vec<QueryExecution> = sample_times
        .into_iter()
        .map(|sample_time| QueryExecution {
            revision: String::new(),
            recording_time: sample_time,
            shared_recordings: Vec::new(),
        })
        .collect();
    BackfillPlan {
        record_count: executions.len(),
        executions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    struct FakeFetcher {
        hashes: HashMap<OffsetDateTime, String>,
        errors: HashMap<OffsetDateTime, String>,
    }

    #[async_trait]
    impl CommitFetcher for FakeFetcher {
        async fn recent_commits(
            &self,
            _repo_name: &str,
            target: OffsetDateTime,
            _revision: &str,
        ) -> Result<Vec<Commit>, String> {
            match self.hashes.get(&target) {
                Some(hash) => Ok(vec![Commit { id: hash.clone() }]),
                None => match self.errors.get(&target) {
                    Some(error) => Err(error.clone()),
                    None => Ok(Vec::new()),
                },
            }
        }
    }

    /// `buildFakeFetcher`: the nth value maps to `2021-01-01 + n months`.
    fn fake_fetcher(values: &[Result<&str, &str>]) -> (FakeFetcher, Vec<OffsetDateTime>) {
        let mut hashes = HashMap::new();
        let mut errors = HashMap::new();
        let mut times = Vec::new();
        let mut current = datetime!(2021-01-01 0:00 UTC);
        for value in values {
            match value {
                Ok(hash) => {
                    hashes.insert(current, (*hash).to_owned());
                }
                Err(error) => {
                    errors.insert(current, (*error).to_owned());
                }
            }
            times.push(current);
            current = add_months(current, 1);
        }
        (FakeFetcher { hashes, errors }, times)
    }

    fn add_months(value: OffsetDateTime, months: u8) -> OffsetDateTime {
        let (year, month) = (value.year(), value.month() as u8 + months);
        let (year, month) = if month > 12 {
            (year + 1, month - 12)
        } else {
            (year, month)
        };
        datetime!(2021-01-01 0:00 UTC)
            .replace_year(year)
            .unwrap()
            .replace_month(time::Month::try_from(month).unwrap())
            .unwrap()
    }

    fn revisions(plan: &BackfillPlan) -> Vec<&str> {
        plan.executions
            .iter()
            .map(|execution| execution.revision.as_str())
            .collect()
    }

    #[tokio::test]
    async fn distinct_commits_are_not_compressed() {
        let (fetcher, times) = fake_fetcher(&[Ok("1"), Ok("2"), Ok("3"), Ok("4")]);
        let filter = GitserverFilter::new(Arc::new(fetcher));
        let plan = filter.filter(times, "myrepo").await;
        assert_eq!(revisions(&plan), ["1", "2", "3", "4"]);
        assert_eq!(plan.record_count, 4);
        for execution in &plan.executions {
            assert!(execution.shared_recordings.is_empty());
        }
    }

    #[tokio::test]
    async fn shared_commits_are_compressed() {
        let (fetcher, times) = fake_fetcher(&[Ok("1"), Ok("1"), Ok("1"), Ok("2")]);
        let filter = GitserverFilter::new(Arc::new(fetcher));
        let plan = filter.filter(times, "myrepo").await;

        assert_eq!(revisions(&plan), ["1", "2"]);
        assert_eq!(
            plan.executions[0].recording_time,
            datetime!(2021-01-01 0:00 UTC)
        );
        assert_eq!(
            plan.executions[0].shared_recordings,
            [
                datetime!(2021-02-01 0:00 UTC),
                datetime!(2021-03-01 0:00 UTC)
            ]
        );
        assert_eq!(plan.executions[0].record_count(), 3);
        assert_eq!(
            plan.executions[1].recording_time,
            datetime!(2021-04-01 0:00 UTC)
        );
    }

    #[tokio::test]
    async fn fetch_errors_fall_back_to_uncompressed_points() {
        let (fetcher, times) = fake_fetcher(&[Ok("1"), Ok("2"), Err("asdf"), Ok("4")]);
        let filter = GitserverFilter::new(Arc::new(fetcher));
        let plan = filter.filter(times, "myrepo").await;
        assert_eq!(revisions(&plan), ["1", "2", "", "4"]);
        assert_eq!(
            plan.executions[2].recording_time,
            datetime!(2021-03-01 0:00 UTC)
        );
    }

    #[tokio::test]
    async fn missing_commits_fall_back_to_uncompressed_points() {
        let (fetcher, _times) = fake_fetcher(&[]);
        let filter = GitserverFilter::new(Arc::new(fetcher));
        let plan = filter
            .filter(
                vec![
                    datetime!(2021-01-01 0:00 UTC),
                    datetime!(2021-02-01 0:00 UTC),
                ],
                "myrepo",
            )
            .await;
        assert_eq!(revisions(&plan), ["", ""]);
        assert_eq!(plan.record_count, 2);
    }

    #[tokio::test]
    async fn noop_filter_preserves_input_order() {
        let plan = NoopFilter
            .filter(
                vec![
                    datetime!(2021-03-01 0:00 UTC),
                    datetime!(2021-01-01 0:00 UTC),
                ],
                "myrepo",
            )
            .await;
        assert_eq!(
            plan.executions
                .iter()
                .map(|execution| execution.recording_time)
                .collect::<Vec<_>>(),
            [
                datetime!(2021-03-01 0:00 UTC),
                datetime!(2021-01-01 0:00 UTC)
            ]
        );
    }

    #[test]
    fn to_recording_includes_shared_times() {
        let execution = QueryExecution {
            revision: "asdf1234".to_owned(),
            recording_time: datetime!(2020-01-01 0:00 UTC),
            shared_recordings: vec![datetime!(2020-01-02 0:00 UTC)],
        };
        let recordings = execution.to_recording("series1", "repoName1", 1, 5.0);
        assert_eq!(recordings.len(), 2);
        assert_eq!(recordings[0].series_id, "series1");
        assert_eq!(recordings[0].time, datetime!(2020-01-01 0:00 UTC));
        assert_eq!(recordings[0].value, 5.0);
        assert_eq!(recordings[0].repo_name.as_deref(), Some("repoName1"));
        assert_eq!(recordings[0].repo_id, Some(1));
        assert_eq!(recordings[0].persist_mode, "record");
        assert_eq!(recordings[1].time, datetime!(2020-01-02 0:00 UTC));
    }
}
