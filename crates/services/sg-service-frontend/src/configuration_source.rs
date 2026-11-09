#![forbid(unsafe_code)]

//! Port of the DB-backed `configurationSource` in
//! `cmd/frontend/internal/cli/config.go:450-467` (read side only).
//!
//! [`DbConfigurationSource`] implements
//! [`sg_config::server::ConfigurationSource`]: `read` calls
//! `critical_and_site_config`'s `SiteGetLatest` (seeding the deployment
//! default when the table is empty, M9.8eg) and attaches the environment
//! driven `serviceConnections` (M9.8eh), producing the `RawUnified` that the
//! configuration service hands to the rest of the process.
//!
//! Errata (see the M9.8ei overview):
//! * the read path has no actor context, so the seeding insert uses author 0
//!   (stored as NULL) like the baseline's unauthenticated reads;
//! * the write path (`SiteCreateIfUpToDate`) is not ported and `write`
//!   returns an error;
//! * frontend startup wiring (connection pool creation,
//!   `InitConfigurationServerFrontendOnly`, the update loop and fsnotify) is
//!   still not connected.

use sg_config::server::ConfigurationSource;
use sg_config::RawUnified;
use sg_db::conf as conf_db;
use sqlx::PgPool;

use crate::{deploy_defaults, service_connections};

/// `config.go`'s `configurationSource` (DB-backed, read only).
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
}

impl ConfigurationSource for DbConfigurationSource {
    async fn read(&self) -> Result<RawUnified, String> {
        // The ported read path has no actor context.
        self.read_with_author(0).await
    }

    async fn write(
        &self,
        _data: RawUnified,
        _last_id: i32,
        _author_user_id: i32,
    ) -> Result<(), String> {
        Err("site configuration write path is not ported".to_owned())
    }
}
