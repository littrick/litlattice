//! `internal/github_apps/store`: the queries the
//! `github-apps-installation-validation-job` performs.
//!
//! Ported paths:
//!
//! * `List` — every `github_apps` row (the job lists unconditionally; the
//!   `domain` filter is not used by this slice). The baseline decrypts at this
//!   point too, because its store owns the keyring; here the columns stay raw
//!   (`client_secret`, `private_key`, `encryption_key_id`) and the worker
//!   decrypts with `sg-encryption`, since `sg-db` does not depend on it.
//!   The baseline scans `logo` into a plain `string`, so a NULL `logo`
//!   aborts the whole list; the column is selected as a presence flag and
//!   the same failure is produced (the remaining scanned columns are NOT
//!   NULL, and `webhook_id` is scanned into a pointer).
//! * `GetInstallations` — all `github_app_installs` rows for the *database*
//!   `app_id` (the `github_apps.id` foreign key, not the GitHub App ID).
//! * `Install` — the `github_app_installs` upsert keyed by
//!   `(app_id, installation_id)`, returning the stored row.
//! * `BulkRemoveInstallations` — `DELETE ... WHERE app_id = $1 AND
//!   installation_id = ANY($2)`. The baseline renders an inline `IN (...)` list
//!   with `sqlf`; the resulting row set is identical. Callers never pass an
//!   empty list (an empty `IN ()` list would be a syntax error in the
//!   baseline).
//!
//! No table, column or sequence is added or changed.

use sqlx::postgres::PgPool;
use sqlx::Row;

/// `ghtypes.GitHubApp`, restricted to the columns this job reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubApp {
    /// `github_apps.id`; the value stored in `github_app_installs.app_id`.
    pub id: i32,
    /// The GitHub App ID (`github_apps.app_id`), used in the JWT issuer.
    pub app_id: i32,
    pub name: String,
    pub base_url: String,
    /// Encrypted (or plaintext) at rest; see `encryption_key_id`.
    pub client_secret: String,
    /// Encrypted (or plaintext) at rest; see `encryption_key_id`.
    pub private_key: String,
    /// Empty for plaintext rows (`MaybeDecrypt` passes them through).
    pub encryption_key_id: String,
}

/// `ghtypes.GitHubAppInstallation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubAppInstallation {
    pub id: i32,
    pub app_id: i32,
    pub installation_id: i32,
    pub url: Option<String>,
    pub account_login: Option<String>,
    pub account_avatar_url: Option<String>,
    pub account_url: Option<String>,
    pub account_type: Option<String>,
}

const SELECT_APPS_QUERY: &str = r#"
SELECT
    id,
    app_id,
    name,
    base_url,
    client_secret,
    private_key,
    encryption_key_id,
    logo IS NOT NULL AS logo_present
FROM github_apps
WHERE true
"#;

/// `gitHubAppsStore.List`: every app row, in the database's order (the
/// baseline query has no `ORDER BY`).
pub async fn list(pool: &PgPool) -> Result<Vec<GitHubApp>, String> {
    let rows = sqlx::query(SELECT_APPS_QUERY)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    rows.iter().map(scan_app).collect()
}

fn scan_app(row: &sqlx::postgres::PgRow) -> Result<GitHubApp, String> {
    let logo_present: bool = row.try_get("logo_present").map_err(|err| err.to_string())?;
    if !logo_present {
        // `scanGitHubApp`'s `&app.Logo` (a plain string) rejects NULL.
        return Err(
            "sql: Scan error on column index 13, name \"logo\": converting NULL to string is unsupported"
                .to_owned(),
        );
    }
    Ok(GitHubApp {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        app_id: row.try_get("app_id").map_err(|err| err.to_string())?,
        name: row.try_get("name").map_err(|err| err.to_string())?,
        base_url: row.try_get("base_url").map_err(|err| err.to_string())?,
        client_secret: row
            .try_get("client_secret")
            .map_err(|err| err.to_string())?,
        private_key: row.try_get("private_key").map_err(|err| err.to_string())?,
        encryption_key_id: row
            .try_get("encryption_key_id")
            .map_err(|err| err.to_string())?,
    })
}

const SELECT_INSTALLS_QUERY: &str = r#"
SELECT
    id,
    app_id,
    installation_id,
    url,
    account_login,
    account_avatar_url,
    account_url,
    account_type
FROM github_app_installs
WHERE app_id = $1
"#;

const SELECT_APP_BY_DOMAIN_KIND_QUERY: &str = r#"
SELECT
    id,
    app_id,
    name,
    base_url,
    client_secret,
    private_key,
    encryption_key_id,
    logo IS NOT NULL AS logo_present
FROM github_apps
WHERE domain = $1
  AND kind = $2
  AND trim(trailing '/' from base_url) = trim(trailing '/' from $3)
LIMIT 1
"#;

/// `gitHubAppsStore.GetByDomainAndKind` (`store.go:430-433`): the first app
/// matching `domain = ? AND kind = ? AND trim(trailing '/' from base_url) = ?`.
///
/// The baseline's `get` has no `ORDER BY`; `LIMIT 1` matches its
/// `ScanFirstGitHubApp` and keeps the result deterministic. The private key is
/// returned encrypted; callers with a keyring decrypt it.
pub async fn get_by_domain_and_kind(
    pool: &PgPool,
    domain: &str,
    kind: &str,
    base_url: &str,
) -> Result<Option<GitHubApp>, String> {
    let rows = sqlx::query(SELECT_APP_BY_DOMAIN_KIND_QUERY)
        .bind(domain)
        .bind(kind)
        .bind(base_url)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    rows.first().map(scan_app).transpose()
}

const SELECT_INSTALL_ID_QUERY: &str = r#"
SELECT installation_id
FROM github_app_installs
JOIN github_apps ON github_app_installs.app_id = github_apps.id
WHERE github_apps.app_id = $1
  AND LOWER(github_app_installs.account_login) = $2
ORDER BY github_app_installs.id DESC
LIMIT 1
"#;

/// `gitHubAppsStore.GetInstallID` (`store.go:324-334`): the newest
/// installation of the GitHub App (`github_apps.app_id`, not the row id) whose
/// lowercased `account_login` matches `account`.
pub async fn get_install_id(
    pool: &PgPool,
    app_id: i32,
    account: &str,
) -> Result<Option<i32>, String> {
    let row = sqlx::query(SELECT_INSTALL_ID_QUERY)
        .bind(app_id)
        .bind(account.to_ascii_lowercase())
        .fetch_optional(pool)
        .await
        .map_err(|err| err.to_string())?;
    match row {
        Some(row) => Ok(Some(
            row.try_get("installation_id")
                .map_err(|err| err.to_string())?,
        )),
        None => Ok(None),
    }
}

/// `gitHubAppsStore.GetInstallations`: installations for the database app
/// row id.
pub async fn get_installations(
    pool: &PgPool,
    app_id: i32,
) -> Result<Vec<GitHubAppInstallation>, String> {
    let rows = sqlx::query(SELECT_INSTALLS_QUERY)
        .bind(app_id)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    rows.iter().map(scan_installation).collect()
}

fn scan_installation(row: &sqlx::postgres::PgRow) -> Result<GitHubAppInstallation, String> {
    Ok(GitHubAppInstallation {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        app_id: row.try_get("app_id").map_err(|err| err.to_string())?,
        installation_id: row
            .try_get("installation_id")
            .map_err(|err| err.to_string())?,
        url: row.try_get("url").map_err(|err| err.to_string())?,
        account_login: row
            .try_get("account_login")
            .map_err(|err| err.to_string())?,
        account_avatar_url: row
            .try_get("account_avatar_url")
            .map_err(|err| err.to_string())?,
        account_url: row.try_get("account_url").map_err(|err| err.to_string())?,
        account_type: row.try_get("account_type").map_err(|err| err.to_string())?,
    })
}

const INSTALL_QUERY: &str = r#"
INSERT INTO github_app_installs (
    app_id,
    installation_id,
    url,
    account_login,
    account_avatar_url,
    account_url,
    account_type
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (app_id, installation_id)
DO UPDATE SET
    (app_id, installation_id, url, account_login, account_avatar_url, account_url, account_type)
    =
    (excluded.app_id, excluded.installation_id, excluded.url, excluded.account_login, excluded.account_avatar_url, excluded.account_url, excluded.account_type)
WHERE github_app_installs.app_id = excluded.app_id
  AND github_app_installs.installation_id = excluded.installation_id
RETURNING
    id,
    app_id,
    installation_id,
    url,
    account_login,
    account_avatar_url,
    account_url,
    account_type
"#;

/// `gitHubAppsStore.Install`: upsert one installation and return the stored
/// row.
pub async fn install(
    pool: &PgPool,
    install: &GitHubAppInstallation,
) -> Result<GitHubAppInstallation, String> {
    let row = sqlx::query(INSTALL_QUERY)
        .bind(install.app_id)
        .bind(install.installation_id)
        .bind(install.url.as_deref())
        .bind(install.account_login.as_deref())
        .bind(install.account_avatar_url.as_deref())
        .bind(install.account_url.as_deref())
        .bind(install.account_type.as_deref())
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?;
    scan_installation(&row)
}

const BULK_REMOVE_QUERY: &str = r#"
DELETE FROM github_app_installs
WHERE app_id = $1 AND installation_id = ANY($2)
"#;

/// `gitHubAppsStore.BulkRemoveInstallations`.
pub async fn bulk_remove_installations(
    pool: &PgPool,
    app_id: i32,
    installation_ids: &[i32],
) -> Result<(), String> {
    if installation_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(BULK_REMOVE_QUERY)
        .bind(app_id)
        .bind(installation_ids)
        .execute(pool)
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_are_pinned_to_the_baseline_contract() {
        // The upsert conflict target is the unique constraint the schema
        // declares; moving it would silently insert duplicates.
        assert!(INSTALL_QUERY.contains("ON CONFLICT (app_id, installation_id)"));
        assert!(SELECT_INSTALLS_QUERY.contains("WHERE app_id = $1"));
        assert!(BULK_REMOVE_QUERY.contains("installation_id = ANY($2)"));
    }
}
