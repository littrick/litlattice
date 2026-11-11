#![forbid(unsafe_code)]

//! The remaining read-only `SiteConfiguration` GraphQL fields
//! (`cmd/frontend/graphqlbackend/site.go:198-292`):
//!
//! - `ValidationMessages` — admin check, then `EffectiveContents` and
//!   `conf.ValidateSite`;
//! - `LicenseInfo` — admin check, then
//!   `licensing.GetConfiguredProductLicenseInfo`;
//! - `History` — admin check, then a `SiteConfigurationChangeConnection`.
//!
//! `ID` and `EffectiveContents` live in [`crate::site_configuration_query`]
//! and are reused, not reimplemented. The write path
//! (`UpdateSiteConfiguration`) lives in
//! [`crate::site_configuration_mutation`] and is untouched.
//!
//! As with the other resolver cores in this crate, the module is not wired
//! into [`crate::graphql_resolvers::QueryRoot`]; it exposes plain async
//! functions over injected seams so the parent slice can attach it to the
//! resolver tree.

use async_trait::async_trait;

use sg_graphql::exec::Actor;

use crate::site_config_change::{
    ConnectionResolverArgs, ConnectionResolverOptions, SiteConfigHistoryStore,
    SiteConfigurationChangeConnection,
};
use crate::site_configuration_query::{
    effective_contents, SiteConfigurationBackend, SiteConfigurationResolver,
};

/// `licenseInfoResolver` (`site.go:233-243`), with `expiresAt` already
/// formatted as `gqlutil.DateTime`'s UTC RFC3339 string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenseInfo {
    pub tags: Vec<String>,
    pub user_count: i32,
    pub expires_at: String,
}

/// `licensing.GetConfiguredProductLicenseInfo`.
///
/// `Ok(None)` is the Go `nil` license (GraphQL `null`); `Err` carries the
/// baseline's error text (e.g. an unparseable license key).
#[async_trait]
pub trait LicenseInfoProvider: Send + Sync {
    async fn configured_license(&self) -> Result<Option<LicenseInfo>, String>;
}

/// `conf.ValidateSite` (`internal/conf/validate.go`).
#[async_trait]
pub trait SiteConfigValidator: Send + Sync {
    async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String>;
}

/// `siteConfigurationResolver.ValidationMessages` (`site.go:245-257`).
pub async fn validation_messages(
    backend: &dyn SiteConfigurationBackend,
    validator: &dyn SiteConfigValidator,
    actor: Actor,
    resolver: &SiteConfigurationResolver,
) -> Result<Vec<String>, String> {
    // 🚨 SECURITY: the site configuration contains secret tokens and
    // credentials, so only admins may validate it.
    backend.check_current_user_is_site_admin(actor).await?;
    let contents = effective_contents(backend, actor, resolver).await?;
    validator.validate_site(&contents).await
}

/// `siteConfigurationResolver.LicenseInfo` (`site.go:235-250`).
pub async fn license_info(
    backend: &dyn SiteConfigurationBackend,
    licenses: &dyn LicenseInfoProvider,
    actor: Actor,
) -> Result<Option<LicenseInfo>, String> {
    // 🚨 SECURITY: only site admins can view license information.
    backend.check_current_user_is_site_admin(actor).await?;
    licenses.configured_license().await
}

/// `siteConfigurationResolver.History` (`site.go:259-272`).
///
/// The returned connection borrows `store`; `nodes`, `page_info` and
/// `total_count` are the same methods the `site_config_change` differential
/// already covers.
pub async fn history<'a>(
    backend: &dyn SiteConfigurationBackend,
    store: &'a dyn SiteConfigHistoryStore,
    actor: Actor,
    args: Option<ConnectionResolverArgs>,
    options: ConnectionResolverOptions,
) -> Result<SiteConfigurationChangeConnection<'a>, String> {
    // 🚨 SECURITY: only site admins may view the history.
    backend.check_current_user_is_site_admin(actor).await?;
    Ok(SiteConfigurationChangeConnection::new(store, args, options))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::site_config_change::SiteConfigRow;
    use crate::site_configuration_query::SiteConfigRow as LatestRow;
    use time::OffsetDateTime;

    struct Backend {
        admin: bool,
        raw: String,
    }

    #[async_trait]
    impl SiteConfigurationBackend for Backend {
        async fn check_current_user_is_site_admin(&self, _actor: Actor) -> Result<(), String> {
            if self.admin {
                Ok(())
            } else {
                Err("must be site admin".to_owned())
            }
        }

        async fn auditlog_expansion(&self, _actor: Actor) -> bool {
            false
        }

        async fn log_security_event(
            &self,
            _event: crate::site_configuration_query::SecurityEvent,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn site_get_latest(&self) -> Result<LatestRow, String> {
            Ok(LatestRow { id: 3 })
        }

        fn raw_site(&self) -> String {
            self.raw.clone()
        }
    }

    struct Validator;

    #[async_trait]
    impl SiteConfigValidator for Validator {
        async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String> {
            if contents.contains("bad") {
                Ok(vec!["bad value".to_owned()])
            } else {
                Ok(Vec::new())
            }
        }
    }

    struct Licenses;

    #[async_trait]
    impl LicenseInfoProvider for Licenses {
        async fn configured_license(&self) -> Result<Option<LicenseInfo>, String> {
            Ok(Some(LicenseInfo {
                tags: vec!["plan:enterprise".to_owned()],
                user_count: 10,
                expires_at: "2030-01-02T03:04:05Z".to_owned(),
            }))
        }
    }

    struct HistoryStore;

    #[async_trait]
    impl SiteConfigHistoryStore for HistoryStore {
        async fn get_site_configs(&self) -> Result<Vec<SiteConfigRow>, String> {
            Ok(vec![SiteConfigRow {
                id: 1,
                author_user_id: 2,
                contents: "{}".to_owned(),
                redacted_contents: Some("{}".to_owned()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            }])
        }

        async fn get_site_config_count(&self) -> Result<usize, String> {
            Ok(1)
        }
    }

    fn resolver() -> SiteConfigurationResolver {
        SiteConfigurationResolver {
            return_safe_configs_only: false,
        }
    }

    #[tokio::test]
    async fn non_admins_are_denied_every_field() {
        let backend = Backend {
            admin: false,
            raw: "{}".to_owned(),
        };
        let actor = Actor::Anonymous;
        assert!(
            validation_messages(&backend, &Validator, actor, &resolver())
                .await
                .is_err()
        );
        assert!(license_info(&backend, &Licenses, actor).await.is_err());
        let store = HistoryStore;
        assert!(history(
            &backend,
            &store,
            actor,
            None,
            ConnectionResolverOptions::default()
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn admins_get_validation_messages() {
        let backend = Backend {
            admin: true,
            raw: r#"{"bad": true}"#.to_owned(),
        };
        let messages =
            validation_messages(&backend, &Validator, Actor::User { uid: 1 }, &resolver())
                .await
                .expect("messages");
        assert_eq!(messages, vec!["bad value".to_owned()]);
    }

    #[tokio::test]
    async fn admins_get_license_info() {
        let backend = Backend {
            admin: true,
            raw: "{}".to_owned(),
        };
        let info = license_info(&backend, &Licenses, Actor::User { uid: 1 })
            .await
            .expect("license")
            .expect("some");
        assert_eq!(info.user_count, 10);
        assert_eq!(info.expires_at, "2030-01-02T03:04:05Z");
    }

    #[tokio::test]
    async fn admins_get_history_connection() {
        let backend = Backend {
            admin: true,
            raw: "{}".to_owned(),
        };
        let store = HistoryStore;
        let connection = history(
            &backend,
            &store,
            Actor::User { uid: 1 },
            Some(ConnectionResolverArgs {
                first: Some(10),
                ..ConnectionResolverArgs::default()
            }),
            ConnectionResolverOptions::default(),
        )
        .await
        .expect("history");
        assert_eq!(connection.total_count().await.expect("count"), 1);
        assert_eq!(connection.nodes().await.expect("nodes").len(), 1);
    }
}
