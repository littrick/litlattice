#![forbid(unsafe_code)]

//! `Repository.mirrorInfo` (`cmd/frontend/graphqlbackend/repository_mirror.go`).
//!
//! The row-backed fields come from `gitserver_repos` (clone status, last
//! fetch, size, shard, last error, corruption log). The scheduler fields
//! (`nextSyncAt`, `updateSchedule`, `updateQueue`) need the repo-updater RPC
//! and `cloneProgress` needs the gitserver `RepoCloneProgress` RPC; neither is
//! wired in this frontend, so they resolve to null/absent like a scheduler
//! without state.

use std::collections::HashMap;
use std::sync::Arc;

use sg_db::gitserver_repos::{CloneStatus, GitserverRepo, GitserverReposStore};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;
use sqlx::postgres::PgPool;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::graphql_resolvers::datetime_rfc3339;

/// The prefetched `MirrorRepositoryInfo` values keyed by `repo.id`.
pub type MirrorInfoLookup = Result<MirrorInfoData, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorInfoData {
    pub name: String,
    pub cloned: bool,
    pub clone_in_progress: bool,
    pub updated_at: Option<OffsetDateTime>,
    pub is_corrupted: bool,
    pub corruption_logs: Vec<OffsetDateTime>,
    pub last_error: Option<String>,
    pub byte_size: i64,
    pub shard: Option<String>,
    /// `repo.CloneURLs()[0]` with the userinfo removed, from
    /// `external_service_repos.clone_url`.
    pub remote_url: Option<String>,
    /// `GitserverRepos().GetLastSyncOutput` (`gitserver_repos_sync_output`).
    pub last_sync_output: Option<String>,
}

fn parse_corruption_logs(value: &serde_json::Value) -> Vec<OffsetDateTime> {
    let Some(entries) = value.as_array() else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| {
            entry
                .get("timestamp")
                .and_then(serde_json::Value::as_str)
                .and_then(|timestamp| OffsetDateTime::parse(timestamp, &Rfc3339).ok())
        })
        .collect()
}

/// `removeUserinfo` (`repository_mirror.go:71-100`): strip credentials from
/// an HTTPS/SSH URL, or the SCP-style `user@host:path` prefix.
fn strip_userinfo(raw: &str) -> String {
    if raw.contains("://") {
        match url::Url::parse(raw) {
            Ok(mut parsed) => {
                let _ = parsed.set_username("");
                let _ = parsed.set_password(None);
                return parsed.to_string();
            }
            Err(_) => return raw.to_owned(),
        }
    }
    match raw.split_once('@') {
        Some((_, rest)) => rest.to_owned(),
        None => raw.to_owned(),
    }
}

fn mirror_info_from_row(row: &GitserverRepo) -> MirrorInfoData {
    MirrorInfoData {
        name: row.name.clone(),
        cloned: row.clone_status == CloneStatus::Cloned,
        clone_in_progress: row.clone_status == CloneStatus::Cloning,
        updated_at: (row.last_fetched != OffsetDateTime::UNIX_EPOCH).then_some(row.last_fetched),
        is_corrupted: row.corrupted_at.is_some(),
        corruption_logs: parse_corruption_logs(&row.corruption_logs),
        last_error: row.last_error.clone(),
        byte_size: row.repo_size_bytes.unwrap_or(0),
        shard: (!row.shard_id.is_empty()).then(|| row.shard_id.clone()),
        remote_url: None,
        last_sync_output: None,
    }
}

/// True when `mirrorInfo` is selected anywhere under these fields.
pub fn selects_mirror_info(selections: &[sg_graphql::exec::RootField]) -> bool {
    selections.iter().any(|selection| {
        selection.name == "mirrorInfo"
            || selects_mirror_info(&selection.selections)
            || selection
                .type_conditions
                .iter()
                .any(|(_, nested)| selects_mirror_info(nested))
    })
}

/// Prefetch the gitserver rows for the repositories selected by a request.
pub async fn prefetch_mirror_infos(
    pool: &PgPool,
    repo_ids: &[i32],
) -> HashMap<i32, MirrorInfoLookup> {
    let store = GitserverReposStore::new(pool.clone());
    let mut infos = HashMap::new();
    for repo_id in repo_ids {
        if infos.contains_key(repo_id) {
            continue;
        }
        let lookup = match store.get_by_id(*repo_id).await {
            Ok(Some(row)) => {
                let mut data = mirror_info_from_row(&row);
                data.remote_url = sg_db::external_service_repos::first_clone_url_by_repo_id(
                    pool, *repo_id,
                )
                .await
                .ok()
                .flatten()
                .map(|url| strip_userinfo(&url));
                if !row.name.is_empty() {
                    data.last_sync_output = store
                        .get_last_sync_output(&row.name)
                        .await
                        .ok()
                        .flatten();
                }
                Ok(data)
            }
            // No row means "not yet in the gitserver schedule": the baseline
            // `GetByID` returns a zero row in this case in practice; expose a
            // zeroed mirror info rather than an error.
            Ok(None) => Ok(MirrorInfoData {
                name: String::new(),
                cloned: false,
                clone_in_progress: false,
                updated_at: None,
                is_corrupted: false,
                corruption_logs: Vec::new(),
                last_error: None,
                byte_size: 0,
                shard: None,
                remote_url: None,
                last_sync_output: None,
            }),
            Err(error) => Err(error.to_string()),
        };
        infos.insert(*repo_id, lookup);
    }
    infos
}

/// `repositoryMirrorInfoResolver` for one repository.
pub struct MirrorInfoObject {
    pub data: MirrorInfoLookup,
    pub viewer_site_admin: bool,
}

impl ObjectValue for MirrorInfoObject {
    fn type_name(&self) -> &'static str {
        "MirrorRepositoryInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let data = match &self.data {
            Ok(data) => data,
            Err(message) => return Err(ExecError::new(message.clone())),
        };
        match field {
            // `RemoteURL` (`repository_mirror.go:64-107`): site admins only,
            // because the URL can contain credentials (stripped here).
            "remoteURL" => {
                if !self.viewer_site_admin {
                    return Err(ExecError::new("must be site admin"));
                }
                match &data.remote_url {
                    Some(url) => Ok(Some(FieldValue::Json(Json::Str(url.clone())))),
                    None => Err(ExecError::new(format!(
                        "no sources for {:?}",
                        data.name
                    ))),
                }
            }
            "cloned" => Ok(Some(FieldValue::Json(Json::Bool(data.cloned)))),
            "cloneInProgress" => Ok(Some(FieldValue::Json(Json::Bool(
                data.clone_in_progress,
            )))),
            // `RepoCloneProgress` needs the gitserver RPC, not wired here.
            "cloneProgress" => Ok(Some(FieldValue::Null)),
            "updatedAt" => Ok(Some(match data.updated_at {
                Some(value) => FieldValue::Json(Json::Str(datetime_rfc3339(value))),
                None => FieldValue::Null,
            })),
            // Repo-updater scheduler state is not wired.
            "nextSyncAt" | "updateSchedule" | "updateQueue" => Ok(Some(FieldValue::Null)),
            "isCorrupted" => Ok(Some(FieldValue::Json(Json::Bool(data.is_corrupted)))),
            "corruptionLogs" => Ok(Some(FieldValue::List {
                items: data
                    .corruption_logs
                    .iter()
                    .map(|timestamp| {
                        FieldValue::Object(Arc::new(RepoCorruptionLogObject {
                            timestamp: *timestamp,
                        }))
                    })
                    .collect(),
                element_type: Some("RepoCorruptionLog"),
            })),
            "lastError" => Ok(Some(match &data.last_error {
                Some(value) => FieldValue::Json(Json::Str(value.clone())),
                None => FieldValue::Null,
            })),
            // `LastSyncOutput` (`repository_mirror.go:145-155`).
            "lastSyncOutput" => Ok(Some(match &data.last_sync_output {
                Some(output) => FieldValue::Json(Json::Str(output.clone())),
                None => FieldValue::Null,
            })),
            "byteSize" => Ok(Some(FieldValue::Json(Json::Int(data.byte_size)))),
            // `Shard` (`repository_mirror.go:229-246`): site admins only.
            "shard" => {
                if !self.viewer_site_admin {
                    // `auth.ErrMustBeSiteAdmin` (`internal/auth/site_admin.go:14`).
                    return Err(ExecError::new("must be site admin"));
                }
                Ok(Some(match &data.shard {
                    Some(value) => FieldValue::Json(Json::Str(value.clone())),
                    None => FieldValue::Null,
                }))
            }
            _ => Ok(None),
        }
    }
}

/// `corruptionLogResolver` (`repository_mirror.go:208-217`).
pub struct RepoCorruptionLogObject {
    timestamp: OffsetDateTime,
}

impl ObjectValue for RepoCorruptionLogObject {
    fn type_name(&self) -> &'static str {
        "RepoCorruptionLog"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "timestamp" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.timestamp,
            ))))),
            _ => Ok(None),
        }
    }
}
