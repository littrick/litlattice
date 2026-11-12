#![forbid(unsafe_code)]

//! Embeddings read surface for the frontend GraphQL API (M9.14c).
//!
//! Ported from the Go baseline:
//!
//! * `Repository.embeddingExists` (`cmd/frontend/graphqlbackend/repository.go:
//!   116-122`): `conf.EmbeddingsEnabled()` short-circuits to `false`, otherwise
//!   `db.Repos().RepoEmbeddingExists(id)`.
//! * `repoStore.RepoEmbeddingExists` (`internal/database/repos.go`):
//!   `SELECT EXISTS(... FROM repo_embedding_jobs WHERE repo_id = $1 AND
//!   state = 'completed')`.
//! * `repoEmbeddingJobsStore.CountRepoEmbeddings`
//!   (`internal/embeddings/background/repo/store.go`), also used by
//!   `RepositoryStats.embedded`.
//! * The `conf.EmbeddingsEnabled` gate (`internal/conf/computed.go:430,1125`,
//!   `:244`, `internal/conf/embeddings.go:17`), reduced to the "is a config
//!   computed" decision.
//!
//! The executor is synchronous, so every selected `embeddingExists` is resolved
//! during the request's async prefetch phase and replayed from
//! [`EmbeddingsPrefetch`].

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use sg_graphql::exec::RootField;

/// The database seam for embeddings reads. `Ok(None)` is not used: the Go
/// store returns `false` for a missing job.
#[async_trait]
pub trait RepositoryEmbeddingsStore: Send + Sync {
    /// `RepoEmbeddingExists`.
    async fn repo_embedding_exists(&self, repo_id: i32) -> Result<bool, String>;
    /// `CountRepoEmbeddings`.
    async fn count_repo_embeddings(&self) -> Result<i64, String>;
}

/// Production store: two read-only queries against `repo_embedding_jobs`.
pub struct PgRepositoryEmbeddingsStore {
    pool: sqlx::PgPool,
}

impl PgRepositoryEmbeddingsStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RepositoryEmbeddingsStore for PgRepositoryEmbeddingsStore {
    async fn repo_embedding_exists(&self, repo_id: i32) -> Result<bool, String> {
        sg_db::repo_embeddings::repo_embedding_exists(&self.pool, repo_id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn count_repo_embeddings(&self) -> Result<i64, String> {
        sg_db::repo_embeddings::count_repo_embeddings(&self.pool)
            .await
            .map_err(|error| error.to_string())
    }
}

/// The per-request embeddings snapshot shared by every `Repository` object.
///
/// * `enabled` mirrors `conf.EmbeddingsEnabled()` for this request. When it is
///   `false`, `Repository.embeddingExists` is `false` without touching the
///   database (exactly like the baseline).
/// * `exists` holds one lookup per repository that appeared in a
///   selection-carrying root field. A lookup error is replayed only if the
///   field is selected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddingsPrefetch {
    pub enabled: bool,
    pub exists: HashMap<i32, Result<bool, String>>,
}

/// Resolve every `embeddingExists` that could be reached by this request.
///
/// `repo_ids` is the union of the repository ids the root prefetchers produced
/// (`Query.repository`, `Query.repositoryRedirect`, `Query.node(Repository)`
/// and `Query.repositories` pages). When the gate is disabled the database is
/// not queried.
pub async fn prefetch_embeddings(
    store: &dyn RepositoryEmbeddingsStore,
    fields: &[RootField],
    repo_ids: impl IntoIterator<Item = i32>,
    enabled: bool,
) -> EmbeddingsPrefetch {
    let mut prefetch = EmbeddingsPrefetch {
        enabled,
        exists: HashMap::new(),
    };
    if !field_selected(fields, "embeddingExists") {
        return prefetch;
    }
    if !enabled {
        return prefetch;
    }
    let unique: HashSet<i32> = repo_ids.into_iter().collect();
    for repo_id in unique {
        let lookup = store.repo_embedding_exists(repo_id).await;
        prefetch.exists.insert(repo_id, lookup);
    }
    prefetch
}

/// Recursively test whether any selection set (including inline fragments and
/// fragment spreads) selects `name`.
pub fn field_selected(fields: &[RootField], name: &str) -> bool {
    for field in fields {
        if field.name == name {
            return true;
        }
        if field_selected(&field.selections, name) {
            return true;
        }
        for (_, selection) in &field.type_conditions {
            if field_selected(selection, name) {
                return true;
            }
        }
    }
    false
}

/// `conf.EmbeddingsEnabled()` = `GetEmbeddingsConfig(siteConfig) != nil`.
///
/// `force_allow` mirrors `conf.ForceAllowEmbeddings()` (the
/// `SRC_INTERNAL_EMBEDDINGS_ALLOWED` env flag). Only the decision "is there a
/// usable config" is ported; all defaults from `GetEmbeddingsConfig` are
/// applied in the same order as the baseline, but the computed config itself
/// is not exposed because nothing else reads it in this slice.
pub fn embeddings_enabled(site: &serde_json::Value, force_allow: bool) -> bool {
    let cody_enabled = match site.get("cody").and_then(|cody| cody.get("enabled")) {
        Some(value) => value.as_bool().unwrap_or(false),
        None => match site.get("completions") {
            Some(completions) => completions
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            None => false,
        },
    };
    if !cody_enabled {
        return false;
    }
    if !force_allow {
        return false;
    }

    let embeddings = site.get("embeddings");
    if let Some(enabled) = embeddings
        .and_then(|config| config.get("enabled"))
        .and_then(serde_json::Value::as_bool)
    {
        if !enabled {
            return false;
        }
    }

    let access_token = embeddings
        .and_then(|config| config.get("accessToken"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let mut provider = embeddings
        .and_then(|config| config.get("provider"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_owned();
    if provider.is_empty() {
        if !access_token.is_empty() {
            return false;
        }
        provider = "sourcegraph".to_owned();
    }

    match provider.as_str() {
        "sourcegraph" => {
            // `getSourcegraphProviderAccessToken`: configured token, else a
            // token derived from the license key (non-empty license implies a
            // non-empty derived token).
            let license_key = site
                .get("licenseKey")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            !access_token.is_empty() || !license_key.is_empty()
        }
        "openai" => !access_token.is_empty(),
        "azureopenai" => {
            let endpoint = embeddings
                .and_then(|config| config.get("endpoint"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let model = embeddings
                .and_then(|config| config.get("model"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            !endpoint.is_empty() && !model.is_empty()
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gate_requires_cody_force_allow_and_provider() {
        let site = json!({"cody": {"enabled": true}});
        assert!(!embeddings_enabled(&site, false));
        assert!(!embeddings_enabled(&site, true));
        let site = json!({
            "cody": {"enabled": true},
            "embeddings": {"provider": "openai", "accessToken": "x"}
        });
        assert!(embeddings_enabled(&site, true));
        let site = json!({
            "cody": {"enabled": true},
            "embeddings": {"provider": "openai"}
        });
        assert!(!embeddings_enabled(&site, true));
    }

    #[test]
    fn explicit_disable_and_empty_provider_with_token_fail() {
        let site = json!({
            "cody": {"enabled": true},
            "embeddings": {"enabled": false, "accessToken": "x"}
        });
        assert!(!embeddings_enabled(&site, true));
        let site = json!({
            "cody": {"enabled": true},
            "embeddings": {"accessToken": "x"}
        });
        assert!(!embeddings_enabled(&site, true));
        let site = json!({
            "cody": {"enabled": true},
            "licenseKey": "k"
        });
        assert!(embeddings_enabled(&site, true));
        let legacy = json!({
            "completions": {"enabled": true},
            "embeddings": {"provider": "openai", "accessToken": "x"}
        });
        assert!(embeddings_enabled(&legacy, true));
    }
}
