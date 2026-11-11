#![forbid(unsafe_code)]

//! Production adapters for the ranking GraphQL surface (M9.8ii).
//!
//! [`PgRankingService`] implements [`crate::ranking_graphql::RankingService`] on
//! top of the already-ported `sg-codeintel-ranking` store functions
//! (`internal/codeintel/ranking/service.go:124-163`). The baseline service reads
//! the live site configuration on every call, so the adapter keeps an
//! `Arc<LiveConfig>` instead of a snapshot.
//!
//! The site-admin check is request-scoped
//! ([`crate::ranking_graphql::ViewerSiteAdminChecker`]) and lives in the
//! GraphQL executor, not here.

use std::sync::Arc;

use async_trait::async_trait;
use sg_codeintel_ranking as ranking;
use sg_config::LiveConfig;
use sqlx::PgPool;

use crate::ranking_graphql::{
    CoverageCounts, DateTime, Progress, RankingError, RankingService, Summary,
};

/// Go's zero `time.Time` (year 1, UTC) as Unix seconds; `gqlutil.DateTime`
/// formats it as `0001-01-01T00:00:00Z`.
const GO_ZERO_TIME_UNIX_SECONDS: i64 = -62_135_596_800;

/// `ranking.Service` backed by the frontend Postgres pool.
pub struct PgRankingService {
    pool: PgPool,
    live: Arc<LiveConfig>,
}

impl PgRankingService {
    pub fn new(pool: PgPool, live: Arc<LiveConfig>) -> Self {
        Self { pool, live }
    }

    fn site(&self) -> sg_config::SiteConfiguration {
        self.live.current().site.clone()
    }
}

fn date_time(value: time::OffsetDateTime) -> DateTime {
    DateTime::from_unix_seconds(value.unix_timestamp())
}

fn progress(value: ranking::Progress) -> Progress {
    Progress {
        started_at: date_time(value.started_at),
        completed_at: value.completed_at.map(date_time),
        processed: value.processed,
        total: value.total,
    }
}

fn summary(value: ranking::Summary) -> Summary {
    Summary {
        graph_key: value.graph_key,
        visible_to_zoekt: value.visible_to_zoekt,
        path_mapper_progress: progress(value.path_mapper_progress),
        reference_mapper_progress: progress(value.reference_mapper_progress),
        reducer_progress: value.reducer_progress.map(progress),
    }
}

fn coverage_counts(value: ranking::CoverageCounts) -> CoverageCounts {
    CoverageCounts {
        num_target_indexes: value.num_target_indexes,
        num_exported_indexes: value.num_exported_indexes,
        num_repositories_without_current_ranks: value.num_repositories_without_current_ranks,
    }
}

fn service_error(error: impl std::fmt::Display) -> RankingError {
    RankingError::Service(error.to_string())
}

#[async_trait]
impl RankingService for PgRankingService {
    async fn derivative_graph_key(&self) -> Result<(String, bool), RankingError> {
        ranking::derivative_graph_key(&self.pool, &self.site())
            .await
            .map_err(service_error)
    }

    async fn bump_derivative_graph_key(&self) -> Result<(), RankingError> {
        ranking::bump_derivative_graph_key(&self.pool)
            .await
            .map_err(service_error)
    }

    async fn summaries(&self) -> Result<Vec<Summary>, RankingError> {
        let summaries = ranking::summaries(&self.pool)
            .await
            .map_err(service_error)?;
        Ok(summaries.into_iter().map(summary).collect())
    }

    async fn next_job_starts_at(&self) -> Result<(DateTime, bool), RankingError> {
        match ranking::next_job_starts_at(&self.pool, &self.site())
            .await
            .map_err(service_error)?
        {
            ranking::NextJobStart::NoGraphKey => Ok((DateTime::from_unix_seconds(0), false)),
            ranking::NextJobStart::At(at) => Ok((date_time(at), true)),
            // `expr.Next` returned no match but a graph key exists: the
            // baseline returns Go's zero `time.Time` with `ok == true`.
            ranking::NextJobStart::Never => {
                Ok((DateTime::from_unix_seconds(GO_ZERO_TIME_UNIX_SECONDS), true))
            }
        }
    }

    async fn coverage_counts(&self, graph_key: &str) -> Result<CoverageCounts, RankingError> {
        ranking::coverage_counts(&self.pool, graph_key)
            .await
            .map(coverage_counts)
            .map_err(service_error)
    }

    async fn delete_ranking_progress(&self, graph_key: &str) -> Result<(), RankingError> {
        ranking::delete_ranking_progress(&self.pool, graph_key)
            .await
            .map_err(service_error)
    }
}
