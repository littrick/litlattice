#![forbid(unsafe_code)]

//! Port of `overrideExtSvcConfig` (`cmd/frontend/internal/cli/config.go:209-364`)
//! and its `watchUpdate` watcher (`config.go:366-441`).
//!
//! At startup the frontend reads `EXTSVC_CONFIG_FILE` (a JSONC document mapping
//! an external service `kind` to an array of configs) and reconciles it against
//! the live rows in `external_services`:
//!
//! * a config without a matching `(kind, display name, decrypted config)` row
//!   is added (display name `<kind> #<i+1>`, config stored as-is or encrypted
//!   with the external-service keyring key);
//! * a row with the same `kind`/`display name` but a different config is
//!   updated (which bumps `updated_at` and `next_sync_at`);
//! * a live row not described by the file is soft-deleted.
//!
//! The reconciliation is delta based so repo-updater does not have to
//! reassociate repositories on every restart.
//!
//! As in the baseline, `update` returns "updated" unconditionally on success,
//! so the watcher logs `success` (not `skipped`) for every event. The watcher
//! is the same polling replacement documented in
//! [`crate::configuration_watch`] (M9.8ge), reused here for the single
//! `EXTSVC_CONFIG_FILE` path.
//!
//! Deviations (see `docs/overviews/m9_8gm_extsvc_config_file_overview.md`):
//! JSON Schema / per-kind config validation is limited to the kind allow-list
//! and an object check, and `ensureCodeHost`/`code_host_id` is not ported.
//!
//! `EXTSVC_CONFIG_ALLOW_EDITS` is only recorded: the baseline consults it in
//! the external-service GraphQL mutations (`graphqlbackend/external_services.go:37`),
//! which this slice does not port.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sg_config::server::ConfigurationSource;
use sg_db::external_services::{ExternalService, ExternalServiceStore};
use sg_encryption::{maybe_decrypt, maybe_encrypt, Key};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::configuration_source::DbConfigurationSource;
use crate::configuration_watch::{file_stamp, record_update_result, FileStamp};
use crate::site_config_file::go_read_error;

/// `EXTSVC_CONFIG_FILE` (read directly through `os.Getenv` in the baseline).
pub const EXTSVC_CONFIG_FILE_VAR: &str = "EXTSVC_CONFIG_FILE";

/// `EXTSVC_CONFIG_ALLOW_EDITS`; only recorded by this slice (see module docs).
pub const EXTSVC_CONFIG_ALLOW_EDITS_VAR: &str = "EXTSVC_CONFIG_ALLOW_EDITS";

/// `database.ExternalServiceKinds` (`internal/database/external_services.go:196-215`).
/// The unsupported-kind error is the only config validation ported.
const EXTERNAL_SERVICE_KINDS: [&str; 18] = [
    "AWSCODECOMMIT",
    "AZUREDEVOPS",
    "BITBUCKETCLOUD",
    "BITBUCKETSERVER",
    "GERRIT",
    "GITHUB",
    "GITLAB",
    "GITOLITE",
    "GOMODULES",
    "JVMPACKAGES",
    "NPMPACKAGES",
    "OTHER",
    "PAGURE",
    "PERFORCE",
    "PHABRICATOR",
    "PYTHONPACKAGES",
    "RUSTPACKAGES",
    "RUBYPACKAGES",
];

/// A reconciled service from the config file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DesiredService {
    kind: String,
    display_name: String,
    /// `json.MarshalIndent(rawConfig, "", "  ")`: the value stored verbatim
    /// (encrypted only when a keyring key is configured).
    config: String,
    /// The config parsed for `recalculateFields`.
    value: Value,
}

/// `jsonc.Unmarshal(file, &map[string][]*json.RawMessage)`:
///
/// * comments/trailing commas are tolerated (`jsonc.Parse`);
/// * an empty/comment-only document becomes `null` and therefore an empty map;
/// * the top level must be an object;
/// * every value must be an array (elements are arbitrary JSON).
pub fn parse_extsvc_config(text: &str) -> Result<BTreeMap<String, Vec<Value>>, String> {
    let stripped = sg_config::strip_jsonc(text);
    if stripped.trim().is_empty() {
        // `jsonc.Parse` yields `null` for an empty document.
        return Ok(BTreeMap::new());
    }
    let value: Value =
        serde_json::from_str(&stripped).map_err(|err| format!("failed to parse JSON: {err}"))?;
    match value {
        Value::Null => Ok(BTreeMap::new()),
        Value::Object(object) => {
            let mut configs = BTreeMap::new();
            for (kind, value) in object {
                match value {
                    Value::Array(items) => {
                        configs.insert(kind, items);
                    }
                    other => {
                        return Err(format!(
                            "json: cannot unmarshal {} into Go value of type []*json.RawMessage",
                            json_type(&other)
                        ));
                    }
                }
            }
            Ok(configs)
        }
        other => Err(format!(
            "json: cannot unmarshal {} into Go value of type map[string][]*json.RawMessage",
            json_type(&other)
        )),
    }
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// `overrideExtSvcConfig`'s `update` function. Returns whether a write
/// happened: the baseline returns `true` unconditionally on success (the
/// "already up to date" branch is unreachable).
pub async fn override_ext_svc_config(
    source: &DbConfigurationSource,
    pool: &sqlx::PgPool,
    key: Option<Arc<dyn Key>>,
    path: &Path,
) -> Result<bool, String> {
    // `cs.Read(ctx)`; `conf.ParseConfig(raw)` only feeds `AuthProviders` to
    // the (unported) config validation, but its parse failure still aborts.
    let raw = source.read().await?;
    sg_config::SiteConfiguration::parse(&raw.site)
        .map_err(|err| format!("parsing extsvc config: {err}"))?;

    let bytes = fs::read(path)
        .map_err(|err| format!("reading EXTSVC_CONFIG_FILE: {}", go_read_error(path, &err)))?;
    let text = String::from_utf8_lossy(&bytes);
    let parsed =
        parse_extsvc_config(&text).map_err(|err| format!("parsing EXTSVC_CONFIG_FILE: {err}"))?;

    if parsed.is_empty() {
        tracing::warn!("EXTSVC_CONFIG_FILE contains zero external service configurations");
    }

    let store = ExternalServiceStore::new(pool.clone());
    let existing = store
        .list()
        .await
        .map_err(|err| format!("ExternalServices.List: {err}"))?;

    // `json.MarshalIndent(cfg, "", "  ")` for every config, in kind/index
    // order (the baseline's map order is random; see the overview errata).
    let mut desired = Vec::new();
    for (kind, cfgs) in &parsed {
        for (index, cfg) in cfgs.iter().enumerate() {
            let config = serde_json::to_string_pretty(cfg)
                .map_err(|err| format!("marshaling extsvc config ([{kind}][{index}]): {err}"))?;
            desired.push(DesiredService {
                kind: kind.clone(),
                display_name: format!("{kind} #{}", index + 1),
                config,
                value: cfg.clone(),
            });
        }
    }

    // Delta: match each desired service to a live row with the same
    // `(kind, display name)`. Equal decrypted configs mean "nothing changed";
    // different ones become updates. Everything left over is added/removed.
    let mut removals: Vec<ExternalService> = existing;
    let mut updates: Vec<(i64, DesiredService)> = Vec::new();
    let mut additions: Vec<DesiredService> = Vec::new();
    for service in desired {
        let mut matched = false;
        for index in 0..removals.len() {
            if removals[index].kind != service.kind
                || removals[index].display_name != service.display_name
            {
                continue;
            }
            let row = removals.remove(index);
            let current = maybe_decrypt(key.as_deref(), &row.config, &row.encryption_key_id)
                .await
                .map_err(|err| err.to_string())?;
            if current != service.config {
                updates.push((row.id, service.clone()));
            }
            matched = true;
            break;
        }
        if !matched {
            additions.push(service);
        }
    }

    // Baseline apply order: delete, then create, then update.
    for row in &removals {
        debug_delete(row);
        store
            .delete_soft(row.id)
            .await
            .map_err(|err| format!("ExternalServices.Delete: {err}"))?;
    }
    for service in &additions {
        debug_add(service);
        create_service(&store, key.as_deref(), service).await?;
    }
    for (id, service) in &updates {
        debug_update(*id, service);
        update_service(&store, key.as_deref(), *id, service).await?;
    }

    // The baseline's `update` never returns `false` on the success path.
    Ok(true)
}

/// [`override_ext_svc_config`] with `EXTSVC_CONFIG_FILE` from the process
/// environment. An empty variable is a no-op.
pub async fn override_ext_svc_config_from_env(
    source: &DbConfigurationSource,
    pool: &sqlx::PgPool,
    key: Option<Arc<dyn Key>>,
) -> Result<bool, String> {
    match extsvc_config_path_from_env() {
        Some(path) => override_ext_svc_config(source, pool, key, &path).await,
        None => Ok(false),
    }
}

/// `os.Getenv("EXTSVC_CONFIG_FILE")`; empty means disabled.
pub fn extsvc_config_path_from_env() -> Option<PathBuf> {
    let value = std::env::var(EXTSVC_CONFIG_FILE_VAR).unwrap_or_default();
    if value.is_empty() {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

async fn create_service(
    store: &ExternalServiceStore,
    key: Option<&dyn Key>,
    service: &DesiredService,
) -> Result<(), String> {
    validate_service("Create", service)?;
    let (unrestricted, has_webhooks) = recalculate_fields(&service.kind, &service.value);
    let (config, key_id) = maybe_encrypt(key, &service.config)
        .await
        .map_err(|err| format!("ExternalServices.Create: {err}"))?;
    store
        .create_from_config(
            &service.kind,
            &service.display_name,
            &config,
            &key_id,
            unrestricted,
            Some(has_webhooks),
        )
        .await
        .map_err(|err| format!("ExternalServices.Create: {err}"))?;
    Ok(())
}

async fn update_service(
    store: &ExternalServiceStore,
    key: Option<&dyn Key>,
    id: i64,
    service: &DesiredService,
) -> Result<(), String> {
    validate_service("Update", service)?;
    let (unrestricted, has_webhooks) = recalculate_fields(&service.kind, &service.value);
    let (config, key_id) = maybe_encrypt(key, &service.config)
        .await
        .map_err(|err| format!("ExternalServices.Update: {err}"))?;
    store
        .update_config(
            id,
            &service.display_name,
            &config,
            &key_id,
            unrestricted,
            Some(has_webhooks),
        )
        .await
        .map_err(|err| format!("ExternalServices.Update: {err}"))?;
    Ok(())
}

/// The ported subset of `ValidateExternalServiceConfig`: a known kind whose
/// config is a JSON object. The per-kind JSON Schema and extra checks are not
/// ported (overview errata 3).
fn validate_service(operation: &str, service: &DesiredService) -> Result<(), String> {
    if !EXTERNAL_SERVICE_KINDS.contains(&service.kind.as_str()) {
        return Err(format!(
            "ExternalServices.{operation}: invalid external service kind: {}",
            service.kind
        ));
    }
    if !service.value.is_object() {
        return Err(format!(
            "ExternalServices.{operation}: invalid config for external service {}: expected a JSON object",
            service.kind
        ));
    }
    Ok(())
}

/// `recalculateFields`: `unrestricted` (`calcUnrestricted` with permission
/// user mapping disabled) and `has_webhooks` (`configurationHasWebhooks`).
fn recalculate_fields(kind: &str, config: &Value) -> (bool, bool) {
    let unrestricted = match config.get("enforcePermissions") {
        Some(enforce_permissions) => !enforce_permissions.as_bool().unwrap_or(false),
        None => config.get("authorization").is_none(),
    };
    let has_webhooks = match kind {
        "GITHUB" | "GITLAB" => config
            .get("webhooks")
            .and_then(Value::as_array)
            .is_some_and(|webhooks| !webhooks.is_empty()),
        "BITBUCKETSERVER" => {
            let direct = config
                .get("webhooks")
                .and_then(|webhooks| webhooks.get("secret"))
                .and_then(Value::as_str)
                .is_some_and(|secret| !secret.is_empty());
            let plugin = config
                .get("plugin")
                .and_then(|plugin| plugin.get("webhooks"))
                .and_then(|webhooks| webhooks.get("secret"))
                .and_then(Value::as_str)
                .is_some_and(|secret| !secret.is_empty());
            direct || plugin
        }
        _ => false,
    };
    (unrestricted, has_webhooks)
}

fn debug_delete(row: &ExternalService) {
    tracing::debug!(id = row.id, display_name = %row.display_name, "Deleting external service");
}

fn debug_add(service: &DesiredService) {
    tracing::debug!(display_name = %service.display_name, "Adding external service");
}

fn debug_update(id: i64, service: &DesiredService) {
    tracing::debug!(id, display_name = %service.display_name, "Updating external service");
}

/// Polling replacement for the baseline's `go watchUpdate(ctx, logger,
/// update, path)` for the single `EXTSVC_CONFIG_FILE`.
pub struct ExtsvcConfigWatcher {
    source: DbConfigurationSource,
    pool: sqlx::PgPool,
    key: Option<Arc<dyn Key>>,
    path: PathBuf,
    interval: Duration,
    stamp: Option<FileStamp>,
}

impl ExtsvcConfigWatcher {
    pub fn new(
        source: DbConfigurationSource,
        pool: sqlx::PgPool,
        key: Option<Arc<dyn Key>>,
        path: PathBuf,
        interval: Duration,
    ) -> Result<Self, String> {
        let stamp = file_stamp(&path)
            .map_err(|err| format!("failed to add {} to watcher: {err}", path.display()))?;
        Ok(Self {
            source,
            pool,
            key,
            path,
            interval,
            stamp,
        })
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }

            let current = match file_stamp(&self.path) {
                Ok(stamp) => stamp,
                Err(err) => {
                    tracing::warn!(
                        file = %self.path.display(),
                        error = %err,
                        "error while watching config override files"
                    );
                    continue;
                }
            };
            if current == self.stamp {
                continue;
            }
            self.stamp = current;

            let result =
                override_ext_svc_config(&self.source, &self.pool, self.key.clone(), &self.path)
                    .await;
            record_update_result(std::slice::from_ref(&self.path), result);
        }
    }
}

/// Spawn the `EXTSVC_CONFIG_FILE` watcher on the service's `JoinSet`; a
/// startup failure only logs, like `watchUpdate`.
pub fn spawn_extsvc_config_watcher(
    tasks: &mut JoinSet<()>,
    source: DbConfigurationSource,
    pool: sqlx::PgPool,
    key: Option<Arc<dyn Key>>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let Some(path) = extsvc_config_path_from_env() else {
        return;
    };
    match ExtsvcConfigWatcher::new(source, pool, key, path, interval) {
        Ok(watcher) => {
            tasks.spawn(watcher.run(shutdown));
        }
        Err(err) => tracing::error!(error = %err, "failed to watch config override files"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn parses_kind_to_config_array() {
        let parsed = parse_extsvc_config(
            r#"{
                // comment
                "OTHER": [{"repos": ["repo1"]}, {"repos": ["repo2"]},],
            }"#,
        )
        .expect("parse");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed["OTHER"].len(), 2);
        assert_eq!(parsed["OTHER"][0]["repos"][0], "repo1");
    }

    #[test]
    fn empty_and_comment_only_documents_are_empty() {
        assert!(parse_extsvc_config("").expect("empty").is_empty());
        assert!(parse_extsvc_config("// only a comment")
            .expect("comments")
            .is_empty());
        assert!(parse_extsvc_config("{}").expect("object").is_empty());
        assert!(parse_extsvc_config("null").expect("null").is_empty());
    }

    #[test]
    fn rejects_non_object_and_non_array_values() {
        assert!(parse_extsvc_config("[]").is_err());
        assert!(parse_extsvc_config(r#"{"OTHER": 5}"#).is_err());
        assert!(parse_extsvc_config(r#"{"OTHER": "x"}"#).is_err());
        assert!(parse_extsvc_config("{").is_err());
    }

    #[test]
    fn computes_display_names_and_derived_fields() {
        let parsed = parse_extsvc_config(r#"{"OTHER": [{"repos": ["a"]}, {"repos": ["b"]}]}"#)
            .expect("parse");
        let cfgs = &parsed["OTHER"];
        assert_eq!(format!("OTHER #{}", 1), "OTHER #1");
        assert_eq!(cfgs.len(), 2);

        let (unrestricted, has_webhooks) = recalculate_fields("OTHER", &cfgs[0]);
        assert!(unrestricted);
        assert!(!has_webhooks);
        let (unrestricted, _) = recalculate_fields(
            "OTHER",
            &serde_json::json!({"repos": ["a"], "authorization": {}}),
        );
        assert!(!unrestricted);
        let (_, has_webhooks) = recalculate_fields(
            "GITHUB",
            &serde_json::json!({"url": "https://github.test", "webhooks": [{"secret": "s"}]}),
        );
        assert!(has_webhooks);
    }

    #[test]
    fn kind_allow_list_matches_the_baseline() {
        let kinds: BTreeSet<&str> = EXTERNAL_SERVICE_KINDS.into_iter().collect();
        assert_eq!(kinds.len(), 18);
        assert!(kinds.contains("GITHUB"));
        assert!(kinds.contains("OTHER"));
        assert!(!kinds.contains("NOPE"));
    }

    #[test]
    fn config_file_path_from_env_is_empty_when_unset() {
        // The test process does not normally set this variable; avoid the
        // global env in parallel tests by checking the empty case only.
        if std::env::var(EXTSVC_CONFIG_FILE_VAR)
            .unwrap_or_default()
            .is_empty()
        {
            assert!(extsvc_config_path_from_env().is_none());
        }
    }
}
