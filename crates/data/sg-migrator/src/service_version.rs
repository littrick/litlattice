#![forbid(unsafe_code)]

//! `internal/version/upgradestore`: the `versions` table (service version and
//! upgrade policy) used by `upgrade`/`downgrade`/`drift`.

use sqlx::PgPool;

use crate::error::MigratorError;

/// One parsed `major.minor` pair (patch and pre-release are ignored by the
/// upgrade policy, matching `Masterminds/semver` usage in Go).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MajorMinor {
    pub major: u64,
    pub minor: u64,
}

/// Parse a go-semver string leniently: strips a leading `v`, optional
/// `+dev`/pre-release suffixes. Returns `None` when the major/minor pair is
/// not numeric (Go ignores parse errors and treats the version as nil).
pub fn parse_major_minor(version: &str) -> Option<MajorMinor> {
    let version = version.strip_prefix('v').unwrap_or(version);
    let version = version.split(['+', '-']).next().unwrap_or(version);
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().unwrap_or("0").parse().ok()?;
    Some(MajorMinor { major, minor })
}

/// `upgradestore.IsValidUpgrade`: nil versions are always valid; rollbacks are
/// always valid; otherwise only same-major or previous-major-next-minor-zero.
pub fn is_valid_upgrade(previous: Option<&str>, latest: Option<&str>) -> bool {
    let previous = previous.and_then(parse_major_minor);
    let latest = latest.and_then(parse_major_minor);
    let (Some(previous), Some(latest)) = (previous, latest) else {
        return true;
    };
    if previous.major > latest.major {
        return true;
    }
    if previous.major == latest.major {
        return previous.minor >= latest.minor || previous.minor + 1 == latest.minor;
    }
    if previous.major + 1 == latest.major {
        return latest.minor == 0;
    }
    false
}

/// `getServiceVersionQuery`.
const GET_SERVICE_VERSION_QUERY: &str = "SELECT version FROM versions WHERE service = $1";

/// `getAutoUpgradeQuery` / fallback when `auto_upgrade` is absent.
const GET_AUTO_UPGRADE_QUERY: &str = "SELECT version, auto_upgrade FROM versions WHERE service = 'frontend'";
const GET_AUTO_UPGRADE_FALLBACK_QUERY: &str = "SELECT version FROM versions WHERE service = 'frontend'";
const SET_SERVICE_VERSION_QUERY: &str = "UPDATE versions SET version = $1, updated_at = NOW() WHERE versions.service = $2";

/// `Store.GetAutoUpgrade`.
pub async fn get_auto_upgrade(pool: &PgPool) -> Result<(Option<String>, bool), MigratorError> {
    match sqlx::query_as::<_, (String, bool)>(GET_AUTO_UPGRADE_QUERY)
        .fetch_optional(pool)
        .await
    {
        Ok(Some((version, enabled))) => Ok((Some(version), enabled)),
        Ok(None) => Ok((None, false)),
        Err(error) => match &error {
            sqlx::Error::Database(database)
                if database.code().as_deref() == Some("42703") =>
            {
                let version = sqlx::query_scalar::<_, String>(GET_AUTO_UPGRADE_FALLBACK_QUERY)
                    .fetch_optional(pool)
                    .await
                    .map_err(db_error)?;
                Ok((version, false))
            }
            sqlx::Error::Database(database)
                if database.code().as_deref() == Some("42P01") =>
            {
                Ok((None, false))
            }
            _ => Err(db_error(error)),
        },
    }
}

/// `Store.SetServiceVersion`: unconditional update (no upgrade-policy check).
pub async fn set_service_version(pool: &PgPool, version: &str) -> Result<(), MigratorError> {
    sqlx::query(SET_SERVICE_VERSION_QUERY)
        .bind(version)
        .bind("frontend")
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(db_error)
}

/// `updateServiceVersionSelectQuery`.
const SELECT_SERVICE_VERSION_QUERY: &str = "SELECT version FROM versions WHERE service = $1";

/// `updateServiceVersionSelectUpsertQuery`.
const UPSERT_SERVICE_VERSION_QUERY: &str = r#"
INSERT INTO versions (service, version, updated_at)
VALUES ($1, $2, NOW()) ON CONFLICT (service) DO
UPDATE SET (version, updated_at) = (excluded.version, excluded.updated_at)
WHERE versions.version = $3
"#;

/// `Store.GetServiceVersion`: `Ok(None)` when the table or row is missing.
pub async fn get_service_version(pool: &PgPool) -> Result<Option<String>, MigratorError> {
    match sqlx::query_scalar::<_, String>(GET_SERVICE_VERSION_QUERY)
        .bind("frontend")
        .fetch_optional(pool)
        .await
    {
        Ok(version) => Ok(version),
        Err(error) if is_missing_relation(&error) => Ok(None),
        Err(error) => Err(db_error(error)),
    }
}

/// `Store.UpdateServiceVersion`: validate the upgrade policy, then upsert the
/// frontend row guarded by the previously-read version.
pub async fn update_service_version(
    pool: &PgPool,
    version: &str,
) -> Result<(), MigratorError> {
    let previous = match sqlx::query_scalar::<_, String>(SELECT_SERVICE_VERSION_QUERY)
        .bind("frontend")
        .fetch_optional(pool)
        .await
    {
        Ok(previous) => previous,
        Err(error) if is_missing_relation(&error) => None,
        Err(error) => return Err(db_error(error)),
    };

    if !is_valid_upgrade(previous.as_deref(), Some(version)) {
        return Err(MigratorError::new(format!(
            "upgrading \"frontend\" from \"{}\" to \"{}\" is not allowed, please refer to https://sourcegraph.com/docs/admin/updates#update-policy",
            previous.unwrap_or_default(),
            version
        )));
    }

    sqlx::query(UPSERT_SERVICE_VERSION_QUERY)
        .bind("frontend")
        .bind(version)
        .bind(previous)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(db_error)
}

fn db_error(error: sqlx::Error) -> MigratorError {
    MigratorError::new(error.to_string())
}

fn is_missing_relation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.code().as_deref() == Some("42P01"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_versions_leniently() {
        assert_eq!(parse_major_minor("5.5.0"), Some(MajorMinor { major: 5, minor: 5 }));
        assert_eq!(parse_major_minor("v5.5.0+dev"), Some(MajorMinor { major: 5, minor: 5 }));
        assert_eq!(parse_major_minor("4.5"), Some(MajorMinor { major: 4, minor: 5 }));
        assert_eq!(parse_major_minor("insiders"), None);
    }

    #[test]
    fn upgrade_policy_matches_go() {
        assert!(is_valid_upgrade(None, Some("5.5.0")));
        assert!(is_valid_upgrade(Some("5.4.0"), Some("5.5.0")));
        assert!(is_valid_upgrade(Some("5.5.0"), Some("5.5.0")));
        assert!(is_valid_upgrade(Some("5.5.0"), Some("5.4.0"))); // rollback
        assert!(is_valid_upgrade(Some("4.5.0"), Some("5.0.0")));
        assert!(!is_valid_upgrade(Some("5.4.0"), Some("5.6.0")));
        assert!(!is_valid_upgrade(Some("4.5.0"), Some("5.1.0")));
        assert!(is_valid_upgrade(Some("4.0.0"), Some("5.0.0")));
        assert!(!is_valid_upgrade(Some("3.0.0"), Some("5.0.0")));
    }
}
