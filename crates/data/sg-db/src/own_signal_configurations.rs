#![forbid(unsafe_code)]

//! `internal/database/own_signal_configurations.go`: the read side of
//! `own_signal_configurations` used by the Own background worker's scheduler
//! (`loadConfig`) and feature-flag wrapper.
//!
//! Only `LoadConfigurations` is ported; the update/create paths belong to the
//! GraphQL API, which is not part of the worker.

use sqlx::postgres::PgPool;

/// `database.SignalConfiguration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalConfiguration {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub excluded_repo_patterns: Vec<String>,
    pub enabled: bool,
}

impl SignalConfiguration {
    /// `query.SignalConfiguration` exposed to the GraphQL resolvers.
    pub fn to_graphql(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "excludedRepoPatterns": self.excluded_repo_patterns,
            "enabled": self.enabled,
        })
    }
}

/// `LoadSignalConfigurationArgs`.
#[derive(Debug, Clone, Default)]
pub struct LoadSignalConfigurationArgs {
    pub name: Option<String>,
}

/// `signalConfigurationStore.LoadConfigurations`:
///
/// ```sql
/// SELECT id, name, description, excluded_repo_patterns, enabled
/// FROM own_signal_configurations
/// [WHERE name = $1]
/// ORDER BY id;
/// ```
pub async fn load_configurations(
    pool: &PgPool,
    args: &LoadSignalConfigurationArgs,
) -> Result<Vec<SignalConfiguration>, sqlx::Error> {
    let rows = match &args.name {
        Some(name) => {
            sqlx::query_as::<_, (i32, String, String, Option<Vec<String>>, bool)>(
                "SELECT id, name, description, excluded_repo_patterns, enabled \
                 FROM own_signal_configurations \
                 WHERE name = $1 \
                 ORDER BY id",
            )
            .bind(name)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, (i32, String, String, Option<Vec<String>>, bool)>(
                "SELECT id, name, description, excluded_repo_patterns, enabled \
                 FROM own_signal_configurations \
                 ORDER BY id",
            )
            .fetch_all(pool)
            .await?
        }
    };

    Ok(rows
        .into_iter()
        .map(
            |(id, name, description, excluded_repo_patterns, enabled)| SignalConfiguration {
                id,
                name,
                description,
                excluded_repo_patterns: excluded_repo_patterns.unwrap_or_default(),
                enabled,
            },
        )
        .collect())
}

/// `loadConfig` (the scheduler's per-job lookup): the first configuration with
/// the given name, or the baseline's error text.
pub async fn load_config(pool: &PgPool, name: &str) -> Result<SignalConfiguration, String> {
    let configurations = load_configurations(
        pool,
        &LoadSignalConfigurationArgs {
            name: Some(name.to_owned()),
        },
    )
    .await
    .map_err(|err| format!("LoadConfigurations: {err}"))?;

    match configurations.into_iter().next() {
        Some(configuration) => Ok(configuration),
        None => Err(format!(
            "ownership signal configuration not found for name: {name}\n"
        )),
    }
}
