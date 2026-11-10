#![forbid(unsafe_code)]

//! Production adapter for [`crate::graphql_resolvers::FeatureFlagStore`]:
//! `db.FeatureFlags().GetFeatureFlag` / `GetFeatureFlags` /
//! `GetOverridesForFlag` / `CreateFeatureFlag` / `UpdateFeatureFlag` /
//! `DeleteFeatureFlag` (`internal/database/feature_flags.go`).

use async_trait::async_trait;
use sqlx::PgPool;

use crate::graphql_resolvers::FeatureFlagStore;
use sg_db::feature_flags::{
    create_feature_flag, delete_feature_flag, get_feature_flag, get_feature_flags,
    get_overrides_for_flag, update_feature_flag, FeatureFlag, FeatureFlagError, FeatureFlagKind,
    FeatureFlagOverride,
};

pub struct PgFeatureFlagStore {
    pool: PgPool,
}

impl PgFeatureFlagStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// The GraphQL-facing error text. `sql.ErrNoRows` is what the baseline's
/// `GetFeatureFlag` returns for a missing row.
fn error_text(error: FeatureFlagError) -> String {
    match error {
        FeatureFlagError::NotFound => "sql: no rows in result set".to_owned(),
        other => other.to_string(),
    }
}

#[async_trait]
impl FeatureFlagStore for PgFeatureFlagStore {
    async fn get_feature_flag(&self, name: &str) -> Result<Option<FeatureFlag>, String> {
        match get_feature_flag(&self.pool, name).await {
            Ok(flag) => Ok(Some(flag)),
            Err(FeatureFlagError::NotFound) => Ok(None),
            Err(error) => Err(error_text(error)),
        }
    }

    async fn get_feature_flags(&self) -> Result<Vec<FeatureFlag>, String> {
        get_feature_flags(&self.pool).await.map_err(error_text)
    }

    async fn get_overrides_for_flag(&self, name: &str) -> Result<Vec<FeatureFlagOverride>, String> {
        get_overrides_for_flag(&self.pool, name)
            .await
            .map_err(error_text)
    }

    async fn create_feature_flag(
        &self,
        name: &str,
        kind: FeatureFlagKind,
    ) -> Result<FeatureFlag, String> {
        create_feature_flag(&self.pool, name, kind)
            .await
            .map_err(error_text)
    }

    async fn update_feature_flag(
        &self,
        name: &str,
        kind: FeatureFlagKind,
    ) -> Result<FeatureFlag, String> {
        update_feature_flag(&self.pool, name, kind)
            .await
            .map_err(error_text)
    }

    async fn delete_feature_flag(&self, name: &str) -> Result<(), String> {
        delete_feature_flag(&self.pool, name)
            .await
            .map_err(error_text)
    }
}
