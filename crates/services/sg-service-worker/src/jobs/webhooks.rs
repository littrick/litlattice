#![forbid(unsafe_code)]

//! `cmd/worker/internal/webhooks`: the `webhook-log-janitor` job.
//!
//! The baseline job owns no configuration (`Config() == nil`), builds one
//! `PeriodicGoroutine` named `batchchanges.webhook-log-janitor` with an
//! interval of one hour, and its handler deletes webhook logs older than the
//! `webhook.logging.retention` site setting (default `72h`).
//!
//! Erratum vs. the baseline: the Go job calls `workerdb.InitDB` again inside
//! `Routines` (it is cached, so this is the same connection pool the process
//! already opened); the Rust job receives that pool through [`JobDeps`].

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};

/// `max(ctx)`-free constant from the baseline: `1 * time.Hour`.
pub const INTERVAL: Duration = Duration::from_secs(3600);
pub const ROUTINE_NAME: &str = "batchchanges.webhook-log-janitor";
pub const ROUTINE_DESCRIPTION: &str = "cleans up stale webhook logs";
/// `defaultRetention`, which matches the documented site configuration value.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(72 * 3600);

/// `cmd/worker/internal/webhooks.janitor`.
pub struct WebhookLogJanitor;

#[async_trait]
impl Job for WebhookLogJanitor {
    fn description(&self) -> String {
        // `Description() string { return "" }`.
        String::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<PeriodicRoutine>, String> {
        let handler = Arc::new(WebhookLogJanitorHandler {
            frontend: deps.frontend.clone(),
            live_config: Arc::clone(&deps.live_config),
        });

        Ok(vec![PeriodicRoutine::new(handler)
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(INTERVAL)])
    }
}

/// `cmd/worker/internal/webhooks.handler`.
pub struct WebhookLogJanitorHandler {
    frontend: PgPool,
    live_config: Arc<LiveConfig>,
}

#[async_trait]
impl Handler for WebhookLogJanitorHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let retention = calculate_retention(&self.live_config.current().site);
        tracing::debug!(retention = ?retention, "purging webhook logs");

        delete_stale(&self.frontend, retention)
            .await
            .map_err(HandlerError::new)
            .map(|_| ())
    }

    /// `HandleError`: `log15.Error("error deleting stale webhook logs", ...)`.
    fn handle_error(&self, error: &HandlerError) {
        tracing::error!(err = %error, "error deleting stale webhook logs");
    }
}

/// `webhookLogStore.DeleteStale`: `DELETE FROM webhook_logs WHERE
/// received_at <= now - retention`, using the process clock (`timeutil.Now`,
/// UTC) rather than the database clock.
pub async fn delete_stale(pool: &PgPool, retention: Duration) -> Result<u64, String> {
    let before = sg_db::OffsetDateTime::now_utc()
        - time::Duration::try_from(retention).map_err(|_| {
            format!(
                "webhook log retention {retention:?} is outside the supported range for timestamp arithmetic"
            )
        })?;

    let result = sqlx::query("DELETE FROM webhook_logs WHERE received_at <= $1")
        .bind(before)
        .execute(pool)
        .await
        .map_err(|err| format!("deleting stale webhook logs: {err}"))?;

    Ok(result.rows_affected())
}

/// `calculateRetention(c *conf.Unified)`.
///
/// Reads `webhook.logging` (the schema's JSON name for `WebhookLogging`) from
/// the raw site configuration. A present object with an unparsable
/// `retention` logs the baseline's warning and falls back to
/// [`DEFAULT_RETENTION`].
pub fn calculate_retention(site: &SiteConfiguration) -> Duration {
    if let Some(config) = site.raw().get("webhook.logging") {
        if !config.is_null() {
            let raw = config
                .get("retention")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            match parse_go_duration(raw) {
                Ok(retention) => return retention,
                Err(err) => tracing::warn!(
                    raw,
                    error = %err,
                    "invalid webhook log retention period; ignoring"
                ),
            }
        }
    }

    DEFAULT_RETENTION
}

/// `time.ParseDuration` for the subset of syntax site configs use.
///
/// A local copy of the parser already used by the searcher / symbols /
/// precise-worker configs (the baseline itself calls Go's `time.ParseDuration`
/// from several packages).
pub fn parse_go_duration(input: &str) -> Result<Duration, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("invalid duration \"\": empty".to_owned());
    }
    if let Ok(nanos) = input.parse::<u64>() {
        return Ok(Duration::from_nanos(nanos));
    }

    let mut rest = input;
    let mut total_nanos: u128 = 0;
    while !rest.is_empty() {
        let number_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if number_end == 0 {
            return Err(format!("invalid duration {input:?}"));
        }
        let number: f64 = rest[..number_end]
            .parse()
            .map_err(|_| format!("invalid duration {input:?}"))?;
        rest = &rest[number_end..];

        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];

        let unit_nanos: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3600.0 * 1_000_000_000.0,
            "" => return Err(format!("invalid duration {input:?}: missing unit")),
            _ => return Err(format!("invalid duration {input:?}: unknown unit {unit:?}")),
        };
        total_nanos += (number * unit_nanos) as u128;
    }

    u64::try_from(total_nanos)
        .map(Duration::from_nanos)
        .map_err(|_| format!("invalid duration {input:?}: out of range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn default_retention_is_72h() {
        assert_eq!(calculate_retention(&site("{}")), DEFAULT_RETENTION);
        assert_eq!(
            calculate_retention(&site(r#"{"webhook.logging": null}"#)),
            DEFAULT_RETENTION
        );
        assert_eq!(
            calculate_retention(&site(r#"{"webhook.logging": {}}"#)),
            DEFAULT_RETENTION,
            "an empty object has an empty retention, which fails to parse"
        );
    }

    #[test]
    fn retention_comes_from_the_site_config() {
        assert_eq!(
            calculate_retention(&site(r#"{"webhook.logging": {"retention": "48h"}}"#)),
            Duration::from_secs(48 * 3600)
        );
        assert_eq!(
            calculate_retention(&site(r#"{"webhook.logging": {"retention": "90m"}}"#)),
            Duration::from_secs(90 * 60)
        );
    }

    #[test]
    fn invalid_retention_falls_back_to_the_default() {
        assert_eq!(
            calculate_retention(&site(r#"{"webhook.logging": {"retention": "soon"}}"#)),
            DEFAULT_RETENTION
        );
    }

    #[tokio::test]
    async fn job_constructs_the_baseline_routine() {
        let deps = JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        };
        let routines = WebhookLogJanitor
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");
        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), ROUTINE_NAME);
        assert_eq!(routines[0].description(), ROUTINE_DESCRIPTION);
        assert_eq!(routines[0].interval(), Duration::from_secs(3600));
    }
}
