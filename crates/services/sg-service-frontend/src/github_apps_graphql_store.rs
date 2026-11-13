#![forbid(unsafe_code)]

//! Production Postgres adapter for
//! [`crate::github_apps_graphql::GitHubAppsGraphqlDatabase`] (M9.25c).
//!
//! Ports the read queries of `internal/github_apps/store/store.go` (`List`,
//! `GetByID`, `GetByAppID`, `GetInstallations`), the site-admin join in
//! `internal/database/users.go`, and
//! `ExternalServices().List({Kinds: [GITHUB]})`
//! (`internal/database/external_services.go:268-295` +
//! `OrderByDirection: "DESC"`).
//!
//! No table, column or index is added. Secrets are decrypted with the same
//! `sg-encryption` helper the baseline's store uses (`MaybeDecrypt`), with
//! the GitHub App key for `github_apps` and the external service key for
//! `external_services`.

use std::future::Future;
use std::sync::Arc;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use sg_graphql::exec::Actor;

use crate::github_apps_graphql::{
    GitHubAppData, GitHubAppInstallationData, GitHubAppsGraphqlDatabase, GitHubExternalServiceData,
};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("github apps blocking runtime")
            .block_on(future),
    }
}

/// `githubAppColumns` as selected by `get`/`list` (`store.go:340-358`).
const SELECT_APP_COLUMNS: &str = "id, app_id, name, domain, slug, base_url, app_url, \
     client_id, client_secret, webhook_id, private_key, encryption_key_id, kind, logo, \
     created_at, updated_at, creator_id";

pub struct GitHubAppsPgDatabase {
    pool: PgPool,
    keyring: Arc<sg_keyring::Keyring>,
    /// `conf.Get().ExternalURL`, used for `Webhook.url`.
    external_url: String,
}

impl GitHubAppsPgDatabase {
    pub fn new(pool: PgPool, keyring: Arc<sg_keyring::Keyring>) -> Self {
        Self {
            pool,
            keyring,
            external_url: String::new(),
        }
    }

    /// Set `conf.Get().ExternalURL` (defaults to empty, like `conf.Unified{}`).
    pub fn with_external_url(mut self, external_url: impl Into<String>) -> Self {
        self.external_url = external_url.into();
        self
    }

    fn github_app_key(&self) -> Option<Arc<dyn sg_encryption::Key>> {
        self.keyring.github_app_key()
    }

    fn external_service_key(&self) -> Option<Arc<dyn sg_encryption::Key>> {
        self.keyring.external_service_key()
    }

    fn scan_app(&self, row: &PgRow) -> Result<GitHubAppData, String> {
        let logo: Option<String> = row.try_get("logo").map_err(|err| err.to_string())?;
        let Some(logo) = logo else {
            // `scanGitHubApp` scans `logo` into a plain `string`.
            return Err(
                "sql: Scan error on column index 13, name \"logo\": converting NULL to string is unsupported"
                    .to_owned(),
            );
        };
        let raw_secret: String = row
            .try_get("client_secret")
            .map_err(|err| err.to_string())?;
        let raw_private_key: String = row
            .try_get("private_key")
            .map_err(|err| err.to_string())?;
        let encryption_key_id: String = row
            .try_get("encryption_key_id")
            .map_err(|err| err.to_string())?;
        let key = self.github_app_key();
        let client_secret = block_on(sg_encryption::maybe_decrypt(
            key.as_deref(),
            &raw_secret,
            &encryption_key_id,
        ))
        .map_err(|err| err.to_string())?;
        let private_key = block_on(sg_encryption::maybe_decrypt(
            key.as_deref(),
            &raw_private_key,
            &encryption_key_id,
        ))
        .map_err(|err| err.to_string())?;
        Ok(GitHubAppData {
            id: i64::from(row.try_get::<i32, _>("id").map_err(|err| err.to_string())?),
            app_id: i64::from(
                row.try_get::<i32, _>("app_id")
                    .map_err(|err| err.to_string())?,
            ),
            name: row.try_get("name").map_err(|err| err.to_string())?,
            domain: row.try_get("domain").map_err(|err| err.to_string())?,
            slug: row.try_get("slug").map_err(|err| err.to_string())?,
            base_url: row.try_get("base_url").map_err(|err| err.to_string())?,
            app_url: row.try_get("app_url").map_err(|err| err.to_string())?,
            client_id: row.try_get("client_id").map_err(|err| err.to_string())?,
            client_secret,
            private_key,
            logo,
            webhook_id: row
                .try_get::<Option<i32>, _>("webhook_id")
                .map_err(|err| err.to_string())?
                .map(i64::from),
            created_at: row
                .try_get::<OffsetDateTime, _>("created_at")
                .map_err(|err| err.to_string())?,
            updated_at: row
                .try_get::<OffsetDateTime, _>("updated_at")
                .map_err(|err| err.to_string())?,
        })
    }

    fn query_apps(&self, domain: Option<&str>) -> Result<Vec<GitHubAppData>, String> {
        let mut query = format!("SELECT {SELECT_APP_COLUMNS} FROM github_apps WHERE ");
        let rows = match domain {
            Some(domain) => {
                query.push_str("domain = $1");
                block_on(
                    sqlx::query(&query)
                        .bind(domain)
                        .fetch_all(&self.pool),
                )
            }
            None => {
                query.push_str("true");
                block_on(sqlx::query(&query).fetch_all(&self.pool))
            }
        }
        .map_err(|err| err.to_string())?;
        rows.iter().map(|row| self.scan_app(row)).collect()
    }

    fn query_app_by_id(&self, id: i64) -> Result<Option<GitHubAppData>, String> {
        let query = format!("SELECT {SELECT_APP_COLUMNS} FROM github_apps WHERE id = $1");
        let row = block_on(
            sqlx::query(&query)
                .bind(i32::try_from(id).map_err(|err| err.to_string())?)
                .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        row.as_ref().map(|row| self.scan_app(row)).transpose()
    }

    fn query_app_by_app_id(
        &self,
        app_id: i64,
        base_url: &str,
    ) -> Result<Option<GitHubAppData>, String> {
        let query = format!(
            "SELECT {SELECT_APP_COLUMNS} FROM github_apps \
             WHERE app_id = $1 AND trim(trailing '/' from base_url) = $2"
        );
        let row = block_on(
            sqlx::query(&query)
                .bind(i32::try_from(app_id).map_err(|err| err.to_string())?)
                .bind(base_url.trim_end_matches('/'))
                .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        row.as_ref().map(|row| self.scan_app(row)).transpose()
    }

    fn query_app_by_domain_and_kind(
        &self,
        domain: &str,
        kind: &str,
        base_url: &str,
    ) -> Result<Option<GitHubAppData>, String> {
        let query = format!(
            "SELECT {SELECT_APP_COLUMNS} FROM github_apps \
             WHERE domain = $1 AND kind = $2::github_app_kind \
             AND trim(trailing '/' from base_url) = $3"
        );
        let row = block_on(
            sqlx::query(&query)
                .bind(domain)
                .bind(kind)
                .bind(base_url.trim_end_matches('/'))
                .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        row.as_ref().map(|row| self.scan_app(row)).transpose()
    }
}

impl GitHubAppsGraphqlDatabase for GitHubAppsPgDatabase {
    fn current_user_site_admin(&self, actor: Actor) -> Result<Option<bool>, String> {
        match actor {
            Actor::Internal => Ok(Some(true)),
            Actor::Anonymous => Ok(None),
            Actor::User { uid } => {
                block_on(sg_db::users::get_site_admin(&self.pool, uid)).map_err(|err| err.to_string())
            }
        }
    }

    fn list_apps(&self, domain: Option<&str>) -> Result<Vec<GitHubAppData>, String> {
        self.query_apps(domain)
    }

    fn get_app_by_id(&self, id: i64) -> Result<Option<GitHubAppData>, String> {
        self.query_app_by_id(id)
    }

    fn get_app_by_domain_and_kind(
        &self,
        domain: &str,
        kind: &str,
        base_url: &str,
    ) -> Result<Option<GitHubAppData>, String> {
        self.query_app_by_domain_and_kind(domain, kind, base_url)
    }

    fn get_app_by_app_id(
        &self,
        app_id: i64,
        base_url: &str,
    ) -> Result<Option<GitHubAppData>, String> {
        self.query_app_by_app_id(app_id, base_url)
    }

    fn get_installations(
        &self,
        app_id: i64,
    ) -> Result<Vec<GitHubAppInstallationData>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT installation_id, url, account_login, account_avatar_url, account_url, \
                 account_type FROM github_app_installs WHERE app_id = $1",
            )
            .bind(i32::try_from(app_id).map_err(|err| err.to_string())?)
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        rows.iter()
            .map(|row| {
                Ok(GitHubAppInstallationData {
                    installation_id: i64::from(
                        row.try_get::<i32, _>("installation_id")
                            .map_err(|err| err.to_string())?,
                    ),
                    url: row
                        .try_get::<Option<String>, _>("url")
                        .map_err(|err| err.to_string())?
                        .unwrap_or_default(),
                    account_login: row
                        .try_get::<Option<String>, _>("account_login")
                        .map_err(|err| err.to_string())?
                        .unwrap_or_default(),
                    account_avatar_url: row
                        .try_get::<Option<String>, _>("account_avatar_url")
                        .map_err(|err| err.to_string())?
                        .unwrap_or_default(),
                    account_url: row
                        .try_get::<Option<String>, _>("account_url")
                        .map_err(|err| err.to_string())?
                        .unwrap_or_default(),
                    account_type: row
                        .try_get::<Option<String>, _>("account_type")
                        .map_err(|err| err.to_string())?
                        .unwrap_or_default(),
                })
            })
            .collect()
    }

    fn list_github_external_services(&self) -> Result<Vec<GitHubExternalServiceData>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT id, kind, display_name, config, encryption_key_id, created_at, updated_at \
                 FROM external_services \
                 WHERE deleted_at IS NULL AND kind = ANY($1) \
                 ORDER BY id DESC",
            )
            .bind(vec!["GITHUB".to_owned()])
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        let key = self.external_service_key();
        let mut services = Vec::with_capacity(rows.len());
        for row in &rows {
            let raw: String = row.try_get("config").map_err(|err| err.to_string())?;
            let key_id: String = row
                .try_get("encryption_key_id")
                .map_err(|err| err.to_string())?;
            // `ParseEncryptableConfig` errors are skipped by the resolver.
            let config = block_on(sg_encryption::maybe_decrypt(key.as_deref(), &raw, &key_id)).ok();
            services.push(GitHubExternalServiceData {
                id: row.try_get("id").map_err(|err| err.to_string())?,
                kind: row.try_get("kind").map_err(|err| err.to_string())?,
                display_name: row
                    .try_get("display_name")
                    .map_err(|err| err.to_string())?,
                config,
                created_at: row
                    .try_get::<OffsetDateTime, _>("created_at")
                    .map_err(|err| err.to_string())?,
                updated_at: row
                    .try_get::<OffsetDateTime, _>("updated_at")
                    .map_err(|err| err.to_string())?,
            });
        }
        Ok(services)
    }

    fn delete_app(&self, id: i64) -> Result<(), String> {
        // `gitHubAppsStore.Delete` (`store.go:161-164`). A zero-row delete is
        // not an error.
        block_on(
            sqlx::query("DELETE FROM github_apps WHERE id = $1")
                .bind(i32::try_from(id).map_err(|err| err.to_string())?)
                .execute(&self.pool),
        )
        .map(|_| ())
        .map_err(|err| err.to_string())
    }

    fn install_installation(
        &self,
        app_id: i64,
        install: &GitHubAppInstallationData,
    ) -> Result<(), String> {
        let record = sg_db::github_apps::GitHubAppInstallation {
            id: 0,
            app_id: i32::try_from(app_id).map_err(|err| err.to_string())?,
            installation_id: i32::try_from(install.installation_id)
                .unwrap_or(i32::MAX),
            url: Some(install.url.clone()),
            account_login: Some(install.account_login.clone()),
            account_avatar_url: Some(install.account_avatar_url.clone()),
            account_url: Some(install.account_url.clone()),
            account_type: Some(install.account_type.clone()),
        };
        block_on(sg_db::github_apps::install(&self.pool, &record)).map(|_| ())
    }

    fn bulk_remove_installations(
        &self,
        app_id: i64,
        installation_ids: &[i64],
    ) -> Result<(), String> {
        // `BulkRemoveInstallations` renders an inline `IN (...)` list, so an
        // empty list is never sent (the baseline would produce invalid SQL).
        if installation_ids.is_empty() {
            return Ok(());
        }
        let installation_ids: Vec<i32> = installation_ids
            .iter()
            .map(|id| i32::try_from(*id).unwrap_or(i32::MAX))
            .collect();
        block_on(sg_db::github_apps::bulk_remove_installations(
            &self.pool,
            i32::try_from(app_id).map_err(|err| err.to_string())?,
            &installation_ids,
        ))
    }

    fn get_webhook_by_id(
        &self,
        id: i64,
    ) -> Result<Option<crate::graphql_inventory_c::WebhookData>, String> {
        // `webhook_id` is an `int` in Go; the resolver converts it with
        // `int32(*r.app.WebhookID)`, which wraps like `as i32`.
        let id = id as i32;
        // The resolver ignores every `GetByID` error and renders `null`.
        Ok(block_on(crate::graphql_inventory_c::load_webhook_by_id(
            &self.pool,
            id,
            &self.external_url,
        ))
        .ok())
    }
}
