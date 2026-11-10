#![forbid(unsafe_code)]

//! Port of the DB-backed `configurationSource` in
//! `cmd/frontend/internal/cli/config.go:454-484`.
//!
//! [`DbConfigurationSource`] implements
//! [`sg_config::server::ConfigurationSource`]: `read` calls
//! `critical_and_site_config`'s `SiteGetLatest` (seeding the deployment
//! default when the table is empty, M9.8eg) and attaches the environment
//! driven `serviceConnections` (M9.8eh), producing the `RawUnified` that the
//! configuration service hands to the rest of the process. `write` follows
//! `WriteWithOverride` (M9.8ej): read latest, reject a stale `last_id`, then
//! `SiteCreateIfUpToDate`.
//!
//! Errata (see the M9.8ei and M9.8ej overviews):
//! * the read path has no actor context, so the seeding insert uses author 0
//!   (stored as NULL) like the baseline's unauthenticated reads;
//! * `ValidateSite`/`Validate`'s JSON Schema and custom checks and
//!   `UnredactSecrets`/`RedactAndHashSecrets` are not ported: writes are only
//!   syntax-checked and store an empty `redacted_contents` (the baseline's
//!   redaction-failure fallback, so secrets are never persisted there);
//! * frontend startup wiring (connection pool creation,
//!   `InitConfigurationServerFrontendOnly`, the update loop and fsnotify) is
//!   still not connected.

use sg_config::server::ConfigurationSource;
use sg_config::RawUnified;
use sg_db::conf as conf_db;
use sqlx::PgPool;

use crate::{deploy_defaults, service_connections};

/// `config.go`'s `configurationSource` (DB-backed).
///
/// Cloning shares the pool; the watcher (M9.8ge) gets its own handle while the
/// configuration server keeps the original.
#[derive(Clone)]
pub struct DbConfigurationSource {
    pool: PgPool,
    default_site: &'static str,
    default_redacted: &'static str,
}

impl DbConfigurationSource {
    /// Resolve the deployment defaults from `DEPLOY_TYPE` at construction,
    /// mirroring `defaultConfigForDeployment` at package init (an invalid
    /// deployment type is the same fatal-text error).
    pub fn new(pool: PgPool) -> Result<Self, String> {
        let getenv: &dyn Fn(&str) -> Option<String> = &|key| std::env::var(key).ok();
        let deploy_type = service_connections::deploy_type(getenv);
        Self::with_deploy_type(pool, &deploy_type)
    }

    pub fn with_deploy_type(pool: PgPool, deploy_type: &str) -> Result<Self, String> {
        let (default_site, default_redacted) =
            deploy_defaults::default_site_config_pair(deploy_type)?;
        Ok(Self {
            pool,
            default_site,
            default_redacted,
        })
    }

    /// `configurationSource.Read` with the acting user's ID for the seeding
    /// insert (0 without an actor, which stores NULL).
    pub async fn read_with_author(&self, author_user_id: i32) -> Result<RawUnified, String> {
        let site = conf_db::site_get_latest(
            &self.pool,
            author_user_id,
            self.default_site,
            self.default_redacted,
        )
        .await
        .map_err(|err| format!("ConfStore.SiteGetLatest: {err}"))?;

        Ok(RawUnified {
            id: site.id,
            site: site.contents,
            service_connections: service_connections::service_connections()?,
        })
    }

    /// `configurationSource.WriteWithOverride`.
    ///
    /// The `isOverride` flag selects `conf.Validate` (true) or
    /// `conf.ValidateSite` (false) in the baseline; neither's schema checks
    /// are ported, so both paths only syntax-check the document (see the
    /// M9.8ej overview).
    pub async fn write_with_override(
        &self,
        data: RawUnified,
        last_id: i32,
        author_user_id: i32,
        _is_override: bool,
    ) -> Result<(), String> {
        // `WriteWithOverride` reads through `SiteGetLatest(ctx)`, whose seed
        // author comes from the actor in the context (0 here), not from the
        // write's `authorUserID`.
        let site =
            conf_db::site_get_latest(&self.pool, 0, self.default_site, self.default_redacted)
                .await
                .map_err(|err| format!("ConfStore.SiteGetLatest: {err}"))?;
        if site.id != last_id {
            return Err(
                "site config has been modified by another request, write not allowed".to_owned(),
            );
        }

        validate_site(&data.site)?;

        // `RedactAndHashSecrets` is not ported. The baseline continues with an
        // empty `redacted_contents` when redaction fails, so the safe fallback
        // is to store the same empty value (secrets are never copied there).
        conf_db::site_create_if_up_to_date(
            &self.pool,
            Some(last_id),
            author_user_id,
            &data.site,
            "",
            self.default_site,
            self.default_redacted,
        )
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "SiteConfig creation failed");
            format!("ConfStore.SiteCreateIfUpToDate: {err}")
        })?;
        Ok(())
    }
}

/// `ValidateSite`/`Validate`'s syntax stage only: the JSON Schema
/// (`schema.SiteSchemaJSON`), `validateCustomRaw` and `UnredactSecrets` are
/// not ported, so a document that parses as JSONC is accepted.
fn validate_site(site: &str) -> Result<(), String> {
    sg_config::SiteConfiguration::parse(site)
        .map(|_| ())
        .map_err(|err| format!("failed to validate site configuration: {err}"))
}

impl ConfigurationSource for DbConfigurationSource {
    async fn read(&self) -> Result<RawUnified, String> {
        // The ported read path has no actor context.
        self.read_with_author(0).await
    }

    async fn write(
        &self,
        data: RawUnified,
        last_id: i32,
        author_user_id: i32,
    ) -> Result<(), String> {
        self.write_with_override(data, last_id, author_user_id, false)
            .await
    }
}
