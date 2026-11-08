#![forbid(unsafe_code)]

//! `internal/database/feature_flags.go` and `internal/featureflag`: the
//! part of the feature flag store the worker needs.
//!
//! Only `GetFeatureFlag` and the global evaluation are ported. The baseline
//! returns `sql.ErrNoRows` when the flag does not exist and
//! `ErrInvalidColumnState` when the `flag_type` / value columns disagree;
//! callers such as the aggregated users statistics job treat any error as
//! "flag disabled".

use sqlx::postgres::PgPool;

/// `featureflag.FeatureFlag`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureFlag {
    pub name: String,
    pub bool_value: Option<bool>,
    pub rollout: Option<i32>,
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
    ) -> Result<Self, FeatureFlagError> {
        match flag_type {
            "bool" => {
                let value = bool_value.ok_or(FeatureFlagError::InvalidColumnState)?;
                Ok(Self {
                    name,
                    bool_value: Some(value),
                    rollout: None,
                })
            }
            "rollout" => {
                let value = rollout.ok_or(FeatureFlagError::InvalidColumnState)?;
                Ok(Self {
                    name,
                    bool_value: None,
                    rollout: Some(value),
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

const GET_FEATURE_FLAG: &str = r#"
SELECT
	flag_name,
	flag_type::text,
	bool_value,
	rollout
FROM feature_flags
WHERE deleted_at IS NULL
	AND flag_name = $1
"#;

/// `featureFlagStore.GetFeatureFlag`.
pub async fn get_feature_flag(pool: &PgPool, name: &str) -> Result<FeatureFlag, FeatureFlagError> {
    let row: Option<(String, String, Option<bool>, Option<i32>)> = sqlx::query_as(GET_FEATURE_FLAG)
        .bind(name)
        .fetch_optional(pool)
        .await?;
    let (name, flag_type, bool_value, rollout) = row.ok_or(FeatureFlagError::NotFound)?;
    FeatureFlag::from_columns(name, &flag_type, bool_value, rollout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_matches_the_baseline_column_rules() {
        let flag =
            FeatureFlag::from_columns("f".into(), "bool", Some(true), None).expect("bool flag");
        assert_eq!(flag.evaluate_global(), Some(true));

        let flag =
            FeatureFlag::from_columns("f".into(), "rollout", None, Some(5000)).expect("rollout");
        assert_eq!(
            flag.evaluate_global(),
            None,
            "rollouts have no global value"
        );

        assert!(matches!(
            FeatureFlag::from_columns("f".into(), "bool", None, None),
            Err(FeatureFlagError::InvalidColumnState)
        ));
        assert!(matches!(
            FeatureFlag::from_columns("f".into(), "rollout", None, None),
            Err(FeatureFlagError::InvalidColumnState)
        ));
        assert!(matches!(
            FeatureFlag::from_columns("f".into(), "mystery", Some(true), None),
            Err(FeatureFlagError::InvalidColumnState)
        ));
    }
}
