#![forbid(unsafe_code)]

//! `internal/database/feature_flags.go` and `internal/featureflag`: the
//! part of the feature flag store the worker needs.
//!
//! `GetFeatureFlag` plus the global evaluation were the first ports; the
//! GraphQL read surface (`Query.featureFlags` / `Query.featureFlag`) also
//! needs the flag list and the per-flag overrides, so those queries live
//! here too. The baseline returns `sql.ErrNoRows` when the flag does not
//! exist and `ErrInvalidColumnState` when the `flag_type` / value columns
//! disagree; callers such as the aggregated users statistics job treat any
//! error as "flag disabled".

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

/// `featureflag.FeatureFlag`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureFlag {
    pub name: String,
    pub bool_value: Option<bool>,
    pub rollout: Option<i32>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `featureflag.Override`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureFlagOverride {
    pub org_id: Option<i32>,
    pub user_id: Option<i32>,
    pub flag_name: String,
    pub value: bool,
}

/// Errors returned by [`get_feature_flag`], mirroring the baseline's
/// `sql.ErrNoRows` / `ErrInvalidColumnState` split.
#[derive(Debug)]
pub enum FeatureFlagError {
    NotFound,
    InvalidColumnState,
    Database(sqlx::Error),
}

impl std::fmt::Display for FeatureFlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("feature flag not found"),
            Self::InvalidColumnState => {
                f.write_str("encountered column that is unexpectedly null based on column type")
            }
            Self::Database(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for FeatureFlagError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(err) => Some(err),
            _ => None,
        }
    }
}

impl From<sqlx::Error> for FeatureFlagError {
    fn from(err: sqlx::Error) -> Self {
        Self::Database(err)
    }
}

impl FeatureFlag {
    /// `scanFeatureFlag`: exactly one of `bool_value` / `rollout` is set,
    /// selected by `flag_type`.
    pub fn from_columns(
        name: String,
        flag_type: &str,
        bool_value: Option<bool>,
        rollout: Option<i32>,
        created_at: OffsetDateTime,
        updated_at: OffsetDateTime,
    ) -> Result<Self, FeatureFlagError> {
        match flag_type {
            "bool" => {
                let value = bool_value.ok_or(FeatureFlagError::InvalidColumnState)?;
                Ok(Self {
                    name,
                    bool_value: Some(value),
                    rollout: None,
                    created_at,
                    updated_at,
                })
            }
            "rollout" => {
                let value = rollout.ok_or(FeatureFlagError::InvalidColumnState)?;
                Ok(Self {
                    name,
                    bool_value: None,
                    rollout: Some(value),
                    created_at,
                    updated_at,
                })
            }
            _ => Err(FeatureFlagError::InvalidColumnState),
        }
    }

    /// `FeatureFlag.EvaluateGlobal`: only concrete boolean flags are
    /// evaluatable without a user; rollouts return `None`.
    pub fn evaluate_global(&self) -> Option<bool> {
        self.bool_value
    }
}

impl FeatureFlagOverride {
    /// `scanFeatureFlagOverride` (`internal/database/feature_flags.go:476-485`):
    /// the four override columns in declaration order.
    pub fn from_columns(
        org_id: Option<i32>,
        user_id: Option<i32>,
        flag_name: String,
        value: bool,
    ) -> Self {
        Self {
            org_id,
            user_id,
            flag_name,
            value,
        }
    }
}

/// `flag_name, flag_type, bool_value, rollout, created_at, updated_at`.
type FeatureFlagRow = (
    String,
    String,
    Option<bool>,
    Option<i32>,
    OffsetDateTime,
    OffsetDateTime,
);

/// `namespace_org_id, namespace_user_id, flag_name, flag_value`.
type FeatureFlagOverrideRow = (Option<i32>, Option<i32>, String, bool);

const GET_FEATURE_FLAG: &str = r#"
SELECT
	flag_name,
	flag_type::text,
	bool_value,
	rollout,
	created_at,
	updated_at
FROM feature_flags
WHERE deleted_at IS NULL
	AND flag_name = $1
"#;

/// `featureFlagStore.GetFeatureFlag`.
pub async fn get_feature_flag(pool: &PgPool, name: &str) -> Result<FeatureFlag, FeatureFlagError> {
    let row: Option<FeatureFlagRow> = sqlx::query_as(GET_FEATURE_FLAG)
        .bind(name)
        .fetch_optional(pool)
        .await?;
    let (name, flag_type, bool_value, rollout, created_at, updated_at) =
        row.ok_or(FeatureFlagError::NotFound)?;
    FeatureFlag::from_columns(
        name, &flag_type, bool_value, rollout, created_at, updated_at,
    )
}

/// `featureFlagStore.GetFeatureFlags` (`internal/database/feature_flags.go:247-276`):
/// every non-deleted flag, in the database's physical row order (the baseline
/// has no `ORDER BY`).
pub async fn get_feature_flags(pool: &PgPool) -> Result<Vec<FeatureFlag>, FeatureFlagError> {
    let rows: Vec<FeatureFlagRow> = sqlx::query_as(
        r#"
SELECT
	flag_name,
	flag_type::text,
	bool_value,
	rollout,
	created_at,
	updated_at
FROM feature_flags
WHERE deleted_at IS NULL
"#,
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(
            |(name, flag_type, bool_value, rollout, created_at, updated_at)| {
                FeatureFlag::from_columns(
                    name, &flag_type, bool_value, rollout, created_at, updated_at,
                )
            },
        )
        .collect()
}

/// `featureFlagStore.GetOverridesForFlag`
/// (`internal/database/feature_flags.go:372-390`): non-deleted overrides for
/// one flag, in the database's physical row order (no `ORDER BY`).
pub async fn get_overrides_for_flag(
    pool: &PgPool,
    name: &str,
) -> Result<Vec<FeatureFlagOverride>, FeatureFlagError> {
    let rows: Vec<FeatureFlagOverrideRow> = sqlx::query_as(
        r#"
SELECT
	namespace_org_id,
	namespace_user_id,
	flag_name,
	flag_value
FROM feature_flag_overrides
WHERE flag_name = $1
	AND deleted_at IS NULL
"#,
    )
    .bind(name)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(org_id, user_id, flag_name, value)| {
            FeatureFlagOverride::from_columns(org_id, user_id, flag_name, value)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
    }

    #[test]
    fn scan_matches_the_baseline_column_rules() {
        let flag = FeatureFlag::from_columns("f".into(), "bool", Some(true), None, at(1), at(2))
            .expect("bool flag");
        assert_eq!(flag.evaluate_global(), Some(true));
        assert_eq!(flag.created_at, at(1));
        assert_eq!(flag.updated_at, at(2));

        let flag = FeatureFlag::from_columns("f".into(), "rollout", None, Some(5000), at(1), at(2))
            .expect("rollout");
        assert_eq!(
            flag.evaluate_global(),
            None,
            "rollouts have no global value"
        );

        assert!(matches!(
            FeatureFlag::from_columns("f".into(), "bool", None, None, at(1), at(2)),
            Err(FeatureFlagError::InvalidColumnState)
        ));
        assert!(matches!(
            FeatureFlag::from_columns("f".into(), "rollout", None, None, at(1), at(2)),
            Err(FeatureFlagError::InvalidColumnState)
        ));
        assert!(matches!(
            FeatureFlag::from_columns("f".into(), "mystery", Some(true), None, at(1), at(2)),
            Err(FeatureFlagError::InvalidColumnState)
        ));
    }

    #[test]
    fn override_columns_keep_the_baseline_shape() {
        let override_row = FeatureFlagOverride::from_columns(None, Some(7), "f".into(), true);
        assert_eq!(
            override_row,
            FeatureFlagOverride {
                org_id: None,
                user_id: Some(7),
                flag_name: "f".into(),
                value: true,
            }
        );
    }
}
