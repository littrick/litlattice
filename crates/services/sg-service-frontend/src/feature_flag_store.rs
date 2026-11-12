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

    async fn get_user_flags(
        &self,
        user_id: i32,
    ) -> Result<std::collections::HashMap<String, bool>, String> {
        sg_db::feature_flags::get_user_flags(&self.pool, user_id)
            .await
            .map_err(error_text)
    }

    /// `GetAnonymousUserFlags` (`internal/database/feature_flags.go:578-591`):
    /// every flag evaluated against the anonymous UID.
    async fn get_anonymous_flags(
        &self,
        anonymous_uid: &str,
    ) -> Result<std::collections::HashMap<String, bool>, String> {
        let flags = get_feature_flags(&self.pool).await.map_err(error_text)?;
        Ok(flags
            .into_iter()
            .map(|flag| {
                let value = flag.evaluate_for_anonymous_user(anonymous_uid);
                (flag.name, value)
            })
            .collect())
    }

    async fn get_global_flags(&self) -> Result<std::collections::HashMap<String, bool>, String> {
        sg_db::feature_flags::get_global_feature_flags(&self.pool)
            .await
            .map_err(error_text)
    }

    async fn get_org_override_for_flag(
        &self,
        org_id: i32,
        flag_name: &str,
    ) -> Result<Option<FeatureFlagOverride>, String> {
        let row: Option<(Option<i32>, Option<i32>, String, bool)> = sqlx::query_as(
            "SELECT namespace_org_id, namespace_user_id, flag_name, flag_value \
             FROM feature_flag_overrides \
             WHERE namespace_org_id = $1 AND flag_name = $2 AND deleted_at IS NULL",
        )
        .bind(org_id)
        .bind(flag_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(row.map(|(org_id, user_id, flag_name, value)| {
            FeatureFlagOverride::from_columns(org_id, user_id, flag_name, value)
        }))
    }

    async fn get_org_overrides_for_user(
        &self,
        user_id: i32,
    ) -> Result<Vec<FeatureFlagOverride>, String> {
        let rows: Vec<(Option<i32>, Option<i32>, String, bool)> = sqlx::query_as(
            "SELECT namespace_org_id, namespace_user_id, flag_name, flag_value \
             FROM feature_flag_overrides \
             WHERE EXISTS ( \
                 SELECT org_id FROM org_members \
                 WHERE org_members.user_id = $1 \
                   AND feature_flag_overrides.namespace_org_id = org_members.org_id \
             ) AND deleted_at IS NULL",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(rows
            .into_iter()
            .map(|(org_id, user_id, flag_name, value)| {
                FeatureFlagOverride::from_columns(org_id, user_id, flag_name, value)
            })
            .collect())
    }

    async fn create_override(
        &self,
        user_id: Option<i32>,
        org_id: Option<i32>,
        flag_name: &str,
        value: bool,
    ) -> Result<FeatureFlagOverride, String> {
        let row: (Option<i32>, Option<i32>, String, bool) = sqlx::query_as(
            "INSERT INTO feature_flag_overrides ( \
                 namespace_org_id, namespace_user_id, flag_name, flag_value \
             ) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (namespace_user_id, flag_name) \
             DO UPDATE SET flag_value = EXCLUDED.flag_value, updated_at = now(), deleted_at = NULL \
             RETURNING namespace_org_id, namespace_user_id, flag_name, flag_value",
        )
        .bind(org_id)
        .bind(user_id)
        .bind(flag_name)
        .bind(value)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(FeatureFlagOverride::from_columns(
            row.0, row.1, row.2, row.3,
        ))
    }

    async fn update_override(
        &self,
        user_id: Option<i32>,
        org_id: Option<i32>,
        flag_name: &str,
        value: bool,
    ) -> Result<FeatureFlagOverride, String> {
        let query = match (org_id, user_id) {
            (Some(_), _) => {
                "UPDATE feature_flag_overrides SET flag_value = $1 \
                 WHERE namespace_org_id = $2 AND flag_name = $3 \
                 RETURNING namespace_org_id, namespace_user_id, flag_name, flag_value"
            }
            (None, Some(_)) => {
                "UPDATE feature_flag_overrides SET flag_value = $1 \
                 WHERE namespace_user_id = $2 AND flag_name = $3 \
                 RETURNING namespace_org_id, namespace_user_id, flag_name, flag_value"
            }
            (None, None) => {
                return Err("must set either orgID or userID".to_owned());
            }
        };
        let namespace = org_id.or(user_id);
        let row: Option<(Option<i32>, Option<i32>, String, bool)> = sqlx::query_as(query)
            .bind(value)
            .bind(namespace)
            .bind(flag_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| error.to_string())?;
        match row {
            Some((org_id, user_id, flag_name, value)) => Ok(FeatureFlagOverride::from_columns(
                org_id, user_id, flag_name, value,
            )),
            None => Err("sql: no rows in result set".to_owned()),
        }
    }

    async fn delete_override(
        &self,
        user_id: Option<i32>,
        org_id: Option<i32>,
        flag_name: &str,
    ) -> Result<(), String> {
        let query = match (org_id, user_id) {
            (Some(_), _) => {
                "DELETE FROM feature_flag_overrides \
                             WHERE namespace_org_id = $1 AND flag_name = $2"
            }
            (None, Some(_)) => {
                "DELETE FROM feature_flag_overrides \
                               WHERE namespace_user_id = $1 AND flag_name = $2"
            }
            (None, None) => return Err("must set either orgID or userID".to_owned()),
        };
        let namespace = org_id.or(user_id);
        sqlx::query(query)
            .bind(namespace)
            .bind(flag_name)
            .execute(&self.pool)
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}
