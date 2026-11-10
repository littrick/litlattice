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

use std::collections::HashMap;

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

    /// `FeatureFlag.EvaluateForUser`: boolean flags return their value,
    /// rollouts hash the little-endian user ID plus the flag name with FNV-1.
    pub fn evaluate_for_user(&self, user_id: i32) -> bool {
        match (self.bool_value, self.rollout) {
            (Some(value), _) => value,
            (None, Some(rollout)) => {
                hash_user_and_flag(user_id, &self.name) % 10_000 < rollout as u32
            }
            // `from_columns` never produces this state.
            (None, None) => false,
        }
    }

    /// `FeatureFlag.EvaluateForAnonymousUser`: rollouts hash the anonymous UID
    /// plus the flag name with FNV-1.
    pub fn evaluate_for_anonymous_user(&self, anonymous_uid: &str) -> bool {
        match (self.bool_value, self.rollout) {
            (Some(value), _) => value,
            (None, Some(rollout)) => {
                hash_anonymous_user_and_flag(anonymous_uid, &self.name) % 10_000 < rollout as u32
            }
            // `from_columns` never produces this state.
            (None, None) => false,
        }
    }
}

/// `hashUserAndFlag` (`internal/featureflag/featureflag.go:33-38`): FNV-1
/// (multiply, then xor) over the little-endian `int32` user ID and the flag
/// name. Note this is **not** FNV-1a.
fn hash_user_and_flag(user_id: i32, flag_name: &str) -> u32 {
    fnv1_32(&[&user_id.to_le_bytes(), flag_name.as_bytes()])
}

/// `hashAnonymousUserAndFlag` (`featureflag.go:53-58`).
fn hash_anonymous_user_and_flag(anonymous_uid: &str, flag_name: &str) -> u32 {
    fnv1_32(&[anonymous_uid.as_bytes(), flag_name.as_bytes()])
}

fn fnv1_32(parts: &[&[u8]]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for part in parts {
        for byte in *part {
            hash = hash.wrapping_mul(0x0100_0193);
            hash ^= u32::from(*byte);
        }
    }
    hash
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

/// One of `featureflag.FeatureFlagBool` / `FeatureFlagRollout` as accepted by
/// the create/update mutations (`CreateBool` / `CreateRollout`,
/// `internal/database/feature_flags.go:157-176`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureFlagKind {
    Bool(bool),
    Rollout(i32),
}

impl FeatureFlagKind {
    /// `(flag_type, bool_value, rollout)` as the baseline spreads them into
    /// the INSERT/UPDATE statements.
    fn columns(self) -> (&'static str, Option<bool>, Option<i32>) {
        match self {
            Self::Bool(value) => ("bool", Some(value), None),
            Self::Rollout(rollout) => ("rollout", None, Some(rollout)),
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

/// `INSERT ... RETURNING` shared by the create/update mutations
/// (`internal/database/feature_flags.go:70-112`). The baseline also returns
/// `deleted_at`; it is always `NULL` for a freshly created/updated row and is
/// not part of the GraphQL object, so it is not selected here.
const CREATE_FEATURE_FLAG: &str = r#"
INSERT INTO feature_flags (
	flag_name,
	flag_type,
	bool_value,
	rollout
) VALUES (
	$1,
	$2::feature_flag_type,
	$3,
	$4
) RETURNING
	flag_name,
	flag_type::text,
	bool_value,
	rollout,
	created_at,
	updated_at
"#;

/// `featureFlagStore.CreateFeatureFlag`.
pub async fn create_feature_flag(
    pool: &PgPool,
    name: &str,
    kind: FeatureFlagKind,
) -> Result<FeatureFlag, FeatureFlagError> {
    let (flag_type, bool_value, rollout) = kind.columns();
    let (name, flag_type, bool_value, rollout, created_at, updated_at): FeatureFlagRow =
        sqlx::query_as(CREATE_FEATURE_FLAG)
            .bind(name)
            .bind(flag_type)
            .bind(bool_value)
            .bind(rollout)
            .fetch_one(pool)
            .await?;
    FeatureFlag::from_columns(
        name, &flag_type, bool_value, rollout, created_at, updated_at,
    )
}

/// `UPDATE ... RETURNING` for
/// `internal/database/feature_flags.go:114-155`. `updated_at = NOW()` mirrors
/// the baseline; a missing row yields no `RETURNING` row, i.e. the baseline's
/// `sql.ErrNoRows`.
const UPDATE_FEATURE_FLAG: &str = r#"
UPDATE feature_flags
SET
	flag_type = $1::feature_flag_type,
	bool_value = $2,
	rollout = $3,
	updated_at = NOW()
WHERE flag_name = $4
RETURNING
	flag_name,
	flag_type::text,
	bool_value,
	rollout,
	created_at,
	updated_at
"#;

/// `featureFlagStore.UpdateFeatureFlag`.
pub async fn update_feature_flag(
    pool: &PgPool,
    name: &str,
    kind: FeatureFlagKind,
) -> Result<FeatureFlag, FeatureFlagError> {
    let (flag_type, bool_value, rollout) = kind.columns();
    let row: Option<FeatureFlagRow> = sqlx::query_as(UPDATE_FEATURE_FLAG)
        .bind(flag_type)
        .bind(bool_value)
        .bind(rollout)
        .bind(name)
        .fetch_optional(pool)
        .await?;
    let (name, flag_type, bool_value, rollout, created_at, updated_at) =
        row.ok_or(FeatureFlagError::NotFound)?;
    FeatureFlag::from_columns(
        name, &flag_type, bool_value, rollout, created_at, updated_at,
    )
}

/// `featureFlagStore.DeleteFeatureFlag`
/// (`internal/database/feature_flags.go:157-168`): soft delete by renaming the
/// row with a random suffix and setting `deleted_at`. As in the baseline, a
/// missing row affects zero rows and is not an error.
const DELETE_FEATURE_FLAG: &str = r#"
UPDATE feature_flags
SET
	flag_name = flag_name || '-DELETED-' || TRUNC(random() * 1000000)::varchar(255),
	deleted_at = now()
WHERE flag_name = $1
"#;

/// `featureFlagStore.DeleteFeatureFlag`.
pub async fn delete_feature_flag(pool: &PgPool, name: &str) -> Result<(), FeatureFlagError> {
    sqlx::query(DELETE_FEATURE_FLAG)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
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

/// `featureFlagStore.GetUserFlags`
/// (`internal/database/feature_flags.go:490-576`): the effective value for
/// every flag visible to the user, with user overrides beating org overrides
/// and the newest org override winning per flag.
const GET_USER_FLAGS: &str = r#"
WITH user_overrides AS (
	SELECT
		flag_name,
		flag_value
	FROM feature_flag_overrides
	WHERE namespace_user_id = $1
		AND deleted_at IS NULL
), org_overrides AS (
	SELECT
		DISTINCT ON (flag_name)
		flag_name,
		flag_value
	FROM feature_flag_overrides
	WHERE EXISTS (
		SELECT org_id
		FROM org_members
		WHERE org_members.user_id = $1
			AND feature_flag_overrides.namespace_org_id = org_members.org_id
	) AND deleted_at IS NULL
	ORDER BY flag_name, created_at desc
)
SELECT
	COALESCE(ff.flag_name, uo.flag_name, oo.flag_name),
	ff.flag_type::text,
	ff.bool_value,
	ff.rollout,
	COALESCE(uo.flag_value, oo.flag_value) AS override
FROM feature_flags ff
FULL JOIN org_overrides oo ON ff.flag_name = oo.flag_name
FULL JOIN user_overrides uo ON ff.flag_name = uo.flag_name
WHERE deleted_at IS NULL
"#;

/// `flag_name, flag_type, bool_value, rollout, override`.
type UserFlagRow = (
    String,
    Option<String>,
    Option<bool>,
    Option<i32>,
    Option<bool>,
);

/// `GetUserFlags`.
pub async fn get_user_flags(
    pool: &PgPool,
    user_id: i32,
) -> Result<HashMap<String, bool>, FeatureFlagError> {
    let rows: Vec<UserFlagRow> = sqlx::query_as(GET_USER_FLAGS)
        .bind(user_id)
        .fetch_all(pool)
        .await?;

    let mut flags = HashMap::with_capacity(rows.len());
    for (name, flag_type, bool_value, rollout, override_value) in rows {
        let value = if let Some(value) = override_value {
            value
        } else {
            match flag_type.as_deref() {
                Some("bool") => bool_value.ok_or(FeatureFlagError::InvalidColumnState)?,
                Some("rollout") => {
                    let rollout = rollout.ok_or(FeatureFlagError::InvalidColumnState)?;
                    hash_user_and_flag(user_id, &name) % 10_000 < rollout as u32
                }
                _ => return Err(FeatureFlagError::InvalidColumnState),
            }
        };
        flags.insert(name, value);
    }
    Ok(flags)
}

/// `featureFlagStore.GetGlobalFeatureFlags`
/// (`internal/database/feature_flags.go:592-604`): only boolean flags are
/// evaluatable without a user; rollouts are omitted.
pub async fn get_global_feature_flags(
    pool: &PgPool,
) -> Result<HashMap<String, bool>, FeatureFlagError> {
    let flags = get_feature_flags(pool).await?;
    let mut values = HashMap::with_capacity(flags.len());
    for flag in flags {
        if let Some(value) = flag.evaluate_global() {
            values.insert(flag.name, value);
        }
    }
    Ok(values)
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
    fn rollout_hashing_matches_the_baselines_fnv1() {
        // Values computed with Go's `hash/fnv.New32` over the little-endian
        // int32 user ID followed by the flag name.
        assert_eq!(hash_user_and_flag(1, "auditlog-expansion"), 3193298911);
        assert_eq!(hash_user_and_flag(2, "auditlog-expansion"), 2636226334);
        assert_eq!(hash_user_and_flag(-1, "auditlog-expansion"), 3883836408);
        assert_eq!(
            hash_anonymous_user_and_flag("anonymous-uid", "auditlog-expansion"),
            3717833776
        );
    }

    #[test]
    fn rollout_evaluation_is_below_the_basis_points_modulus() {
        let flag = FeatureFlag::from_columns(
            "auditlog-expansion".into(),
            "rollout",
            None,
            Some(7000),
            at(1),
            at(2),
        )
        .expect("rollout");
        assert!(!flag.evaluate_for_user(1), "8911 >= 7000");
        assert!(flag.evaluate_for_user(2), "6334 < 7000");

        let none = FeatureFlag::from_columns(
            "auditlog-expansion".into(),
            "rollout",
            None,
            Some(0),
            at(1),
            at(2),
        )
        .expect("rollout");
        assert!(!none.evaluate_for_user(1));

        let bool_flag =
            FeatureFlag::from_columns("f".into(), "bool", Some(true), None, at(1), at(2))
                .expect("bool");
        assert!(bool_flag.evaluate_for_user(7));
        assert!(bool_flag.evaluate_for_anonymous_user("anon"));
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
