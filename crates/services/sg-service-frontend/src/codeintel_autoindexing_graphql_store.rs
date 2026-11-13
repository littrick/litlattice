#![forbid(unsafe_code)]

//! Production Postgres adapter for
//! [`crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingDatabase`]
//! (M9.28b).
//!
//! Ports the store half of `internal/codeintel/autoindexing/internal/store`:
//! `GetIndexConfigurationByRepositoryID`, `UpdateIndexConfigurationByRepositoryID`,
//! `GetInferenceScript` and `SetInferenceScript`. The inference/job-selection
//! and enqueuing halves live in `sg-service-worker` (which depends on this
//! crate), so they are injected through a
//! [`crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingJobsRuntime`].
//! No table, column or Redis key is added.

use std::sync::Arc;

use sqlx::{PgPool, Row};

use sg_config::SiteConfiguration;
use sg_db::codeintel_autoindexing_config::{
    get_index_configuration_by_repository_id, get_inference_script,
    set_inference_script, update_index_configuration_by_repository_id,
};
use sg_graphql::exec::Actor;

use crate::codeintel_autoindexing_graphql::{
    CodeIntelAutoindexingDatabase, CodeIntelAutoindexingJobsRuntime, InferenceOutcome,
    MUST_BE_SITE_ADMIN, NOT_AUTHENTICATED,
};
use crate::codeintel_uploads_graphql::AutoIndexJobRow;
use crate::codeintel_uploads_graphql_store::block_on;

/// `conf.CodeIntelAutoIndexingEnabled` (`conf/auto_indexing.go`): the
/// `codeIntelAutoIndexing.enabled` setting, absent meaning false.
pub fn auto_indexing_enabled(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("codeIntelAutoIndexing.enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

pub struct PgCodeIntelAutoindexingStore {
    pool: PgPool,
    enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    jobs: Option<Arc<dyn CodeIntelAutoindexingJobsRuntime>>,
}

impl PgCodeIntelAutoindexingStore {
    pub fn new(
        pool: PgPool,
        enabled: Arc<dyn Fn() -> bool + Send + Sync>,
        jobs: Option<Arc<dyn CodeIntelAutoindexingJobsRuntime>>,
    ) -> Self {
        Self { pool, enabled, jobs }
    }

    /// Convenience constructor for a fixed site-configuration snapshot.
    pub fn with_site_configuration(
        pool: PgPool,
        site: SiteConfiguration,
        jobs: Option<Arc<dyn CodeIntelAutoindexingJobsRuntime>>,
    ) -> Self {
        let site = Arc::new(site);
        Self::new(
            pool,
            Arc::new(move || auto_indexing_enabled(&site)),
            jobs,
        )
    }

    fn jobs(&self) -> Result<&Arc<dyn CodeIntelAutoindexingJobsRuntime>, String> {
        self.jobs
            .as_ref()
            .ok_or_else(|| "auto-indexing jobs runtime is not configured".to_owned())
    }
}

const SITE_ADMIN_SQL: &str = "SELECT site_admin FROM users WHERE id = $1";

impl CodeIntelAutoindexingDatabase for PgCodeIntelAutoindexingStore {
    fn check_site_admin(&self, actor: Actor) -> Result<(), String> {
        // `auth.CheckCurrentUserIsSiteAdmin`.
        if actor.is_internal() {
            return Ok(());
        }
        let Some(uid) = actor.uid() else {
            return Err(NOT_AUTHENTICATED.to_owned());
        };
        let row = block_on(
            sqlx::query(SITE_ADMIN_SQL)
                .bind(uid)
                .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        let Some(row) = row else {
            return Err(NOT_AUTHENTICATED.to_owned());
        };
        let site_admin: bool = row.try_get("site_admin").map_err(|error| error.to_string())?;
        if !site_admin {
            return Err(MUST_BE_SITE_ADMIN.to_owned());
        }
        Ok(())
    }

    fn auto_indexing_enabled(&self) -> bool {
        (self.enabled)()
    }

    fn get_index_configuration(
        &self,
        repository_id: i32,
    ) -> Result<Option<Vec<u8>>, String> {
        let configuration = block_on(get_index_configuration_by_repository_id(
            &self.pool,
            repository_id,
        ))
        .map_err(|error| error.to_string())?;
        Ok(configuration.map(|configuration| configuration.data))
    }

    fn update_index_configuration(
        &self,
        repository_id: i32,
        data: &[u8],
    ) -> Result<(), String> {
        block_on(update_index_configuration_by_repository_id(
            &self.pool,
            repository_id,
            data,
        ))
        .map_err(|error| error.to_string())
    }

    fn get_inference_script(&self) -> Result<String, String> {
        block_on(get_inference_script(&self.pool)).map_err(|error| error.to_string())
    }

    fn set_inference_script(&self, script: &str) -> Result<(), String> {
        // `gopher-lua/parse.Parse` only compiles the script; so does
        // `mlua::Lua::load(...).into_function()`. The dialect differs, so the
        // error text is not byte-identical (recorded as errata).
        let lua = sg_luasandbox::mlua::Lua::new();
        lua.load(script)
            .set_name("(inference script)")
            .into_function()
            .map_err(|error| error.to_string())?;
        block_on(set_inference_script(&self.pool, script)).map_err(|error| error.to_string())
    }

    fn infer_index_configuration(
        &self,
        repository_id: i32,
        commit: &str,
        local_override_script: &str,
        bypass_limit: bool,
    ) -> Result<InferenceOutcome, String> {
        self.jobs()?.infer_index_configuration(
            repository_id,
            commit,
            local_override_script,
            bypass_limit,
        )
    }

    fn queue_auto_index_jobs(
        &self,
        repository_id: i32,
        rev: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJobRow>, String> {
        self.jobs()?.queue_auto_index_jobs(
            repository_id,
            rev,
            configuration,
            force,
            bypass_limit,
        )
    }
}
