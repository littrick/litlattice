#![forbid(unsafe_code)]

//! Port of the `internal/codeintel/ranking` service layer: the frontend-facing
//! composition of the already-ported `sg-db::codeintel_ranking` storage
//! functions.
//!
//! The GraphQL transport (`internal/codeintel/ranking/transport/graphql`) is
//! not ported here: it needs the frontend resolver framework, `SiteAdminChecker`
//! and `gqlutil.DateTime`, none of which exist in the Rust frontend yet. This
//! crate provides the service surface those resolvers call, plus the shared
//! types (`RepoPathRanks`, `Summary`, `Progress`, `CoverageCounts`).

use std::collections::BTreeMap;
use std::fmt;

use sg_config::SiteConfiguration;
use sg_cronexpr::{Expression, ParseError};
use sg_db::codeintel_ranking;
use sqlx::postgres::PgPool;
use time::OffsetDateTime;

pub use sg_db::codeintel_ranking::{CoverageCounts, Progress, Summary};

/// `types.RepoPathRanks` (`internal/codeintel/types/ranking.go`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RepoPathRanks {
    /// `MeanRank`: the binary log mean of reference counts over all
    /// repositories (`GetReferenceCountStatistics`).
    pub mean_rank: f64,
    /// `Paths`: path → normalized reference count (`log2` of the stored rank,
    /// with an exact zero kept as zero).
    pub paths: BTreeMap<String, f64>,
}

/// `NextJobStartsAt`'s `(time.Time, bool, error)` result. The baseline returns
/// Go's zero `time.Time` when the cron expression has no future match but a
/// graph key exists, so the two cases are kept distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextJobStart {
    /// No derivative graph key exists (`ok == false`).
    NoGraphKey,
    /// The next matching instant.
    At(OffsetDateTime),
    /// A graph key exists but `Next` has no match (the baseline's zero time).
    Never,
}

/// `squashRange`: map `[0, inf)` to `[0, 1)` monotonically.
pub fn squash_range(j: f64) -> f64 {
    j / (1.0 + j)
}

/// `repoRankFromConfig`: sum the `experimentalFeatures.ranking.repoScores`
/// entries matching every `/`-delimited prefix of the repository name and the
/// full name. Scores are added in prefix order (the baseline iterates the
/// string indices); missing configuration or scores are zero.
///
/// A non-numeric score entry is skipped. The baseline would fail to load the
/// whole site configuration in that case; the site-config parser owns that
/// validation, so this accessor does not repeat it.
pub fn repo_rank_from_config(site: &SiteConfiguration, repo_name: &str) -> f64 {
    let Some(scores) = site
        .raw()
        .get("experimentalFeatures")
        .and_then(|value| value.get("ranking"))
        .and_then(|value| value.get("repoScores"))
        .and_then(serde_json::Value::as_object)
    else {
        return 0.0;
    };

    let score = |name: &str| {
        scores
            .get(name)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0)
    };

    let mut value = 0.0;
    for (index, byte) in repo_name.bytes().enumerate() {
        if byte == b'/' {
            value += score(&repo_name[..index]);
        }
    }
    value + score(repo_name)
}

/// `conf.CodeIntelRankingDocumentReferenceCountsEnabled`: the site setting
/// `codeIntelRanking.documentReferenceCountsEnabled`, defaulting to `false`.
pub fn document_reference_counts_enabled(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("codeIntelRanking.documentReferenceCountsEnabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// `conf.CodeIntelRankingDocumentReferenceCountsGraphKey`: the site setting
/// `codeIntelRanking.documentReferenceCountsGraphKey`, defaulting to `"dev"`
/// when absent or empty.
pub fn document_reference_counts_graph_key(site: &SiteConfiguration) -> String {
    site.raw()
        .get("codeIntelRanking.documentReferenceCountsGraphKey")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(codeintel_ranking::DEFAULT_DOCUMENT_REFERENCE_COUNTS_GRAPH_KEY)
        .to_owned()
}

/// `rankingshared.GraphKey()`: `NewGraphKey(conf...GraphKey())`.
pub fn root_graph_key(site: &SiteConfiguration) -> String {
    codeintel_ranking::new_graph_key(&document_reference_counts_graph_key(site))
}

/// `rankingshared.DerivativeGraphKeyFromPrefix`: encode the configured root and
/// append the encoded prefix.
pub fn derivative_graph_key_for_prefix(site: &SiteConfiguration, prefix: &str) -> String {
    codeintel_ranking::derivative_graph_key_from_prefix(
        &document_reference_counts_graph_key(site),
        prefix,
    )
}

/// `conf.CodeIntelRankingDocumentReferenceCountsCronExpression`: parse the
/// site setting, defaulting to `@weekly` when absent. A present but empty value
/// is parsed as-is and therefore fails, exactly like the baseline.
pub fn document_reference_counts_cron_expression(
    site: &SiteConfiguration,
) -> Result<Expression, ParseError> {
    match site
        .raw()
        .get("codeIntelRanking.documentReferenceCountsCronExpression")
    {
        Some(serde_json::Value::String(expression)) => Expression::parse(expression),
        _ => Expression::parse("@weekly"),
    }
}

/// `Service.GetRepoRank`: `[squashRange(userRank), starRank]`.
pub async fn get_repo_rank(
    pool: &PgPool,
    site: &SiteConfiguration,
    repo_name: &str,
) -> Result<Vec<f64>, sqlx::Error> {
    let user_rank = repo_rank_from_config(site, repo_name);
    let star_rank = codeintel_ranking::star_rank(pool, repo_name).await?;
    Ok(vec![squash_range(user_rank), star_rank])
}

/// `Service.GetDocumentRanks`: the log2-normalized path ranks and the global
/// log-mean or the zero value when no reducer-completed progress exists.
pub async fn get_document_ranks(
    pool: &PgPool,
    repo_name: &str,
) -> Result<RepoPathRanks, sqlx::Error> {
    let (document_ranks, ok) = codeintel_ranking::document_ranks(pool, repo_name).await?;
    if !ok {
        return Ok(RepoPathRanks::default());
    }

    let mean_rank = codeintel_ranking::reference_count_statistics(pool).await?;
    let paths = document_ranks
        .into_iter()
        .map(|(path, rank)| {
            let rank = if rank == 0.0 { 0.0 } else { rank.log2() };
            (path, rank)
        })
        .collect();

    Ok(RepoPathRanks { mean_rank, paths })
}

/// `Service.NextJobStartsAt` failure modes.
#[derive(Debug)]
pub enum NextJobStartsAtError {
    /// The configured cron expression did not parse.
    Cron(ParseError),
    /// The derivative graph key lookup failed.
    Database(sqlx::Error),
}

impl fmt::Display for NextJobStartsAtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cron(err) => err.fmt(f),
            Self::Database(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for NextJobStartsAtError {}

/// `Service.NextJobStartsAt`: parse the configured cron expression, then
/// return the next trigger after the newest derivative graph key. The
/// expression is parsed **before** the graph key is read, like the baseline.
pub async fn next_job_starts_at(
    pool: &PgPool,
    site: &SiteConfiguration,
) -> Result<NextJobStart, NextJobStartsAtError> {
    let expression =
        document_reference_counts_cron_expression(site).map_err(NextJobStartsAtError::Cron)?;

    let Some((_, previous)) = codeintel_ranking::derivative_graph_key(pool)
        .await
        .map_err(NextJobStartsAtError::Database)?
    else {
        return Ok(NextJobStart::NoGraphKey);
    };

    Ok(match expression.next(previous) {
        Some(next) => NextJobStart::At(next),
        None => NextJobStart::Never,
    })
}

/// `Service.DerivativeGraphKey`: the encoded derivative key and whether a
/// graph key exists. Without one the prefix is empty, yielding the baseline's
/// `"<root>."`.
pub async fn derivative_graph_key(
    pool: &PgPool,
    site: &SiteConfiguration,
) -> Result<(String, bool), sqlx::Error> {
    match codeintel_ranking::derivative_graph_key(pool).await? {
        Some((prefix, _)) => Ok((derivative_graph_key_for_prefix(site, &prefix), true)),
        None => Ok((derivative_graph_key_for_prefix(site, ""), false)),
    }
}

/// `Service.Summaries`.
pub async fn summaries(pool: &PgPool) -> Result<Vec<Summary>, sqlx::Error> {
    codeintel_ranking::summaries(pool).await
}

/// `Service.BumpDerivativeGraphKey`.
pub async fn bump_derivative_graph_key(pool: &PgPool) -> Result<(), sqlx::Error> {
    codeintel_ranking::bump_derivative_graph_key(pool).await
}

/// `Service.DeleteRankingProgress`.
pub async fn delete_ranking_progress(pool: &PgPool, graph_key: &str) -> Result<(), sqlx::Error> {
    codeintel_ranking::delete_ranking_progress(pool, graph_key).await
}

/// `Service.CoverageCounts`.
pub async fn coverage_counts(
    pool: &PgPool,
    graph_key: &str,
) -> Result<CoverageCounts, sqlx::Error> {
    codeintel_ranking::coverage_counts(pool, graph_key).await
}

/// `Service.LastUpdatedAt`.
pub async fn last_updated_at(
    pool: &PgPool,
    repo_ids: &[i32],
) -> Result<BTreeMap<i32, OffsetDateTime>, sqlx::Error> {
    codeintel_ranking::last_updated_at(pool, repo_ids).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn squash_range_is_monotonic() {
        assert_eq!(squash_range(0.0), 0.0);
        assert_eq!(squash_range(1.0), 0.5);
        assert_eq!(squash_range(9.0), 0.9);
    }

    #[test]
    fn repo_rank_sums_matching_prefixes_only() {
        let configured = site(
            r#"{"experimentalFeatures": {"ranking": {"repoScores": {
                "github.com": 500,
                "github.com/foo": 300,
                "github.com/foo/baz": 700,
                "github.com/bar": 200
            }}}}"#,
        );
        assert_eq!(
            repo_rank_from_config(&configured, "github.com/foo/bar"),
            800.0 // 500 + 300, `github.com/foo/baz` does not match
        );
        assert_eq!(
            repo_rank_from_config(&configured, "github.com/bar"),
            700.0 // 500 + 200
        );
        assert_eq!(repo_rank_from_config(&configured, "example.com/x"), 0.0);
    }

    #[test]
    fn repo_rank_is_zero_without_configuration() {
        assert_eq!(repo_rank_from_config(&site("{}"), "github.com/foo"), 0.0);
        assert_eq!(
            repo_rank_from_config(
                &site(r#"{"experimentalFeatures": {"ranking": {"repoScores": {}}}}"#),
                "github.com/foo"
            ),
            0.0
        );
    }

    #[test]
    fn config_helpers_default_like_the_baseline() {
        let empty = site("{}");
        assert!(!document_reference_counts_enabled(&empty));
        assert_eq!(document_reference_counts_graph_key(&empty), "dev");
        assert_eq!(root_graph_key(&empty), "dev");
        assert_eq!(
            derivative_graph_key_for_prefix(&empty, "2024_05"),
            "dev.2024_05"
        );
        assert_eq!(derivative_graph_key_for_prefix(&empty, ""), "dev.");

        let configured = site(
            r#"{
                "codeIntelRanking.documentReferenceCountsEnabled": true,
                "codeIntelRanking.documentReferenceCountsGraphKey": "prod.v1",
                "codeIntelRanking.documentReferenceCountsCronExpression": "0 12 * * MON"
            }"#,
        );
        assert!(document_reference_counts_enabled(&configured));
        assert_eq!(document_reference_counts_graph_key(&configured), "prod.v1");
        assert_eq!(
            derivative_graph_key_for_prefix(&configured, "2024.05-x"),
            "prod_v1.2024_05_x"
        );
        let expression =
            document_reference_counts_cron_expression(&configured).expect("valid cron expression");
        assert_eq!(
            expression
                .next(OffsetDateTime::from_unix_timestamp(1714521600).expect("may 1"))
                .map(|next| next.unix_timestamp()),
            Some(1714996800) // 2024-05-06T12:00:00Z
        );
    }

    #[test]
    fn cron_expression_defaults_to_weekly_and_reports_parse_errors() {
        let expression =
            document_reference_counts_cron_expression(&site("{}")).expect("default @weekly");
        assert_eq!(
            expression
                .next(OffsetDateTime::from_unix_timestamp(1714521600).expect("may 1"))
                .map(|next| next.unix_timestamp()),
            Some(1714867200) // 2024-05-05T00:00:00Z
        );

        let empty = site(r#"{"codeIntelRanking.documentReferenceCountsCronExpression": ""}"#);
        assert_eq!(
            document_reference_counts_cron_expression(&empty)
                .expect_err("empty cron fails")
                .to_string(),
            "missing field(s)"
        );
    }

    #[test]
    fn next_job_start_models_the_three_baseline_results() {
        let at = OffsetDateTime::from_unix_timestamp(1714867200).expect("timestamp");
        assert_ne!(NextJobStart::NoGraphKey, NextJobStart::At(at));
        assert_ne!(NextJobStart::Never, NextJobStart::At(at));
    }
}
