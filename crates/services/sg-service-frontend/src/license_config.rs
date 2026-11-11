#![forbid(unsafe_code)]

//! Production license reading for the configuration surface and the M9.8ja
//! auth seams (`internal/licensing/licensing.go`, `internal/license/license.go`).
//!
//! The baseline's `licensing.GetConfiguredProductLicenseInfo` reads the license
//! key from the site configuration and verifies it with a built-in RSA public
//! key (`internal/licensing/licensing.go:145-220`). This slice is required to
//! read the already-verified `product_licenses` row instead; the parsed values
//! live in `license_tags`, `license_user_count` and `license_expires_at`
//! (`migrations/frontend/squashed.sql:4239-4259`). No signature verification is
//! repeated here.
//!
//! The baseline caches the parsed key by its text (`lastKeyText/lastInfo`); the
//! port keeps the same cache shape in [`LicenseConfig`]. With no
//! non-revoked row it falls back to the free license that the baseline returns
//! for an empty `licenseKey` (`GetFreeLicenseInfo`).
//!
//! The M9.8ja seams only need a decision:
//! [`LicenseState`](crate::authz::LicenseState) for the PostAuth middleware and
//! [`LicenseForToken`](crate::actor_middleware::LicenseForToken) for access
//! tokens. Both are derived from the same cached [`ResolvedLicense`].

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;
use time::Month;

use crate::actor_middleware::LicenseForToken;
use crate::authz::LicenseState;
use crate::config_graphql::LicenseInfo;
use crate::graphql_resolvers::datetime_rfc3339;

/// `plan:free-1` (`internal/licensing/licensing.go:90-99`).
pub const FREE_PLAN_TAG: &str = "plan:free-1";
/// `GetFreeLicenseInfo`'s user count.
pub const FREE_LICENSE_USER_COUNT: i32 = 10;
/// `GetFreeLicenseInfo`: `time.Now().Add(time.Hour * 8760)`.
pub const FREE_LICENSE_VALIDITY_HOURS: i64 = 8760;

/// One `product_licenses` row reduced to the columns the frontend reads.
///
/// All three license columns are nullable; the baseline's zero values are
/// `Tags: nil`, `UserCount: 0` and the zero `time.Time` (which `IsExpired`
/// treats as expired).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductLicense {
    pub license_key: String,
    pub tags: Vec<String>,
    pub user_count: i32,
    pub expires_at: Option<OffsetDateTime>,
}

/// The database seam for the current product license. `None` means there is no
/// non-revoked row (the baseline's empty `conf.LicenseKey`).
#[async_trait]
pub trait ProductLicenseStore: Send + Sync {
    async fn current_license(&self) -> Result<Option<ProductLicense>, String>;
}

/// `product_licenses`' "current" row.
///
/// The baseline has no single-current query; the closest existing operation is
/// `dbLicenses.List` ordered by `created_at DESC`
/// (`cmd/frontend/internal/dotcom/productsubscription/licenses_db.go:334-360`).
/// This adapter takes the newest non-revoked row.
#[derive(Clone)]
pub struct PgProductLicenseStore {
    pool: PgPool,
}

impl PgProductLicenseStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProductLicenseStore for PgProductLicenseStore {
    async fn current_license(&self) -> Result<Option<ProductLicense>, String> {
        let row = sqlx::query_as::<
            _,
            (
                String,
                Option<Vec<String>>,
                Option<i32>,
                Option<OffsetDateTime>,
            ),
        >(
            "SELECT license_key, license_tags, license_user_count, license_expires_at \
             FROM product_licenses \
             WHERE revoked_at IS NULL \
             ORDER BY created_at DESC \
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| format!("ProductLicenses.List: {err}"))?;

        Ok(row.map(
            |(license_key, tags, user_count, expires_at)| ProductLicense {
                license_key,
                tags: tags.unwrap_or_default(),
                user_count: user_count.unwrap_or(0),
                expires_at,
            },
        ))
    }
}

/// A license resolved to the GraphQL-visible fields plus the timestamp the auth
/// seams compare against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedLicense {
    pub info: LicenseInfo,
    pub expires_at: OffsetDateTime,
}

impl ResolvedLicense {
    /// `license.Info.IsExpired`: `ExpiresAt.Before(time.Now())`.
    pub fn is_expired(&self) -> bool {
        self.expires_at < OffsetDateTime::now_utc()
    }

    fn from_row(row: &ProductLicense) -> Self {
        let expires_at = row.expires_at.unwrap_or_else(zero_time);
        Self {
            info: LicenseInfo {
                tags: row.tags.clone(),
                // `int32(license.UserCount)`.
                user_count: row.user_count,
                expires_at: datetime_rfc3339(expires_at),
            },
            expires_at,
        }
    }

    /// `GetFreeLicenseInfo`.
    fn free(now: OffsetDateTime) -> Self {
        let expires_at = now + time::Duration::hours(FREE_LICENSE_VALIDITY_HOURS);
        Self {
            info: LicenseInfo {
                tags: vec![FREE_PLAN_TAG.to_owned()],
                user_count: FREE_LICENSE_USER_COUNT,
                expires_at: datetime_rfc3339(expires_at),
            },
            expires_at,
        }
    }
}

/// The Go zero `time.Time` (`0001-01-01T00:00:00Z`).
fn zero_time() -> OffsetDateTime {
    time::Date::from_calendar_date(1, Month::January, 1)
        .expect("year 1 is valid")
        .midnight()
        .assume_utc()
}

/// `licensing.GetConfiguredProductLicenseInfo` over
/// [`ProductLicenseStore`], with the baseline's per-key-text cache.
pub struct LicenseConfig {
    store: Arc<dyn ProductLicenseStore>,
    cache: Mutex<Option<(String, ResolvedLicense)>>,
}

impl LicenseConfig {
    pub fn new(store: Arc<dyn ProductLicenseStore>) -> Self {
        Self {
            store,
            cache: Mutex::new(None),
        }
    }

    /// The resolved license, or the free license when there is no row.
    ///
    /// The error text is the store's (the baseline surfaces license parse
    /// errors through the same channel).
    pub async fn resolved(&self) -> Result<ResolvedLicense, String> {
        let Some(row) = self.store.current_license().await? else {
            // The baseline recomputes the free license's expiry on every call;
            // do the same rather than caching it.
            return Ok(ResolvedLicense::free(OffsetDateTime::now_utc()));
        };

        if let Some((key, cached)) = self.cache.lock().expect("license cache").as_ref() {
            if key == &row.license_key {
                return Ok(cached.clone());
            }
        }

        let resolved = ResolvedLicense::from_row(&row);
        *self.cache.lock().expect("license cache") =
            Some((row.license_key.clone(), resolved.clone()));
        Ok(resolved)
    }

    /// The GraphQL `licenseInfo` shape.
    pub async fn info(&self) -> Result<LicenseInfo, String> {
        self.resolved().await.map(|resolved| resolved.info)
    }

    /// M9.8ja's `authz.LicenseState`.
    pub async fn license_state(&self) -> LicenseState {
        match self.resolved().await {
            Err(_) => LicenseState::Error,
            Ok(resolved) if resolved.is_expired() => LicenseState::Expired,
            Ok(_) => LicenseState::Valid,
        }
    }

    /// M9.8ja's `actor_middleware.LicenseForToken`.
    pub async fn license_for_token(&self) -> LicenseForToken {
        match self.resolved().await {
            Err(_) => LicenseForToken::Error,
            Ok(resolved) => LicenseForToken::Ok {
                expired: resolved.is_expired(),
            },
        }
    }
}

#[async_trait]
impl crate::config_graphql::LicenseInfoProvider for LicenseConfig {
    async fn configured_license(&self) -> Result<Option<LicenseInfo>, String> {
        // `GetConfiguredProductLicenseInfo` is never nil: an empty key yields
        // the free license, so the GraphQL `LicenseInfo!` stays non-null.
        self.info().await.map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeStore {
        license: Result<Option<ProductLicense>, String>,
    }

    #[async_trait]
    impl ProductLicenseStore for FakeStore {
        async fn current_license(&self) -> Result<Option<ProductLicense>, String> {
            self.license.clone()
        }
    }

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
    }

    #[tokio::test]
    async fn empty_table_falls_back_to_the_free_license() {
        let config = LicenseConfig::new(Arc::new(FakeStore { license: Ok(None) }));
        let info = config.info().await.expect("license");
        assert_eq!(info.tags, vec![FREE_PLAN_TAG.to_owned()]);
        assert_eq!(info.user_count, FREE_LICENSE_USER_COUNT);
        assert_eq!(config.license_state().await, LicenseState::Valid);
        assert_eq!(
            config.license_for_token().await,
            LicenseForToken::Ok { expired: false }
        );
    }

    #[tokio::test]
    async fn row_values_are_mapped_and_expiry_drives_the_seams() {
        let config = LicenseConfig::new(Arc::new(FakeStore {
            license: Ok(Some(ProductLicense {
                license_key: "key".to_owned(),
                tags: vec!["plan:enterprise-1".to_owned()],
                user_count: 50,
                expires_at: Some(at(4_102_444_800)), // 2100-01-01T00:00:00Z
            })),
        }));
        let info = config.info().await.expect("license");
        assert_eq!(info.tags, vec!["plan:enterprise-1".to_owned()]);
        assert_eq!(info.user_count, 50);
        assert_eq!(info.expires_at, "2100-01-01T00:00:00Z");
        assert_eq!(config.license_state().await, LicenseState::Valid);

        let expired = LicenseConfig::new(Arc::new(FakeStore {
            license: Ok(Some(ProductLicense {
                license_key: "old".to_owned(),
                tags: Vec::new(),
                user_count: 1,
                expires_at: Some(at(0)),
            })),
        }));
        assert_eq!(expired.license_state().await, LicenseState::Expired);
        assert_eq!(
            expired.license_for_token().await,
            LicenseForToken::Ok { expired: true }
        );
    }

    #[tokio::test]
    async fn store_errors_map_to_the_error_seams() {
        let config = LicenseConfig::new(Arc::new(FakeStore {
            license: Err("boom".to_owned()),
        }));
        assert_eq!(config.license_state().await, LicenseState::Error);
        assert_eq!(config.license_for_token().await, LicenseForToken::Error);
        assert!(config.info().await.is_err());
    }

    #[tokio::test]
    async fn missing_expiry_is_the_go_zero_time() {
        let config = LicenseConfig::new(Arc::new(FakeStore {
            license: Ok(Some(ProductLicense {
                license_key: "key".to_owned(),
                tags: Vec::new(),
                user_count: 0,
                expires_at: None,
            })),
        }));
        let info = config.info().await.expect("license");
        assert_eq!(info.expires_at, "0001-01-01T00:00:00Z");
        assert_eq!(config.license_state().await, LicenseState::Expired);
    }
}
