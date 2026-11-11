#![forbid(unsafe_code)]

//! `Mutation.settingsMutation` / `ConfigurationMutation`
//! (`cmd/frontend/graphqlbackend/settings_mutation.go`):
//! `nodeByID` + `settingsSubjectForNodeAndCheckAccess` + `ViewerCanAdminister`,
//! `toKeyPath`, `editSettings` (`jsonx.ComputePropertyEdit`/
//! `ComputePropertyRemoval` + `ApplyEdits`) and `overwriteSettings`, writing
//! through `settingsCreateIfUpToDate` (`settings.go:55-81`) →
//! `settingsStore.CreateIfUpToDate` (`internal/database/settings.go:48-86`).
//!
//! The GraphQL executor resolves fields synchronously while database access is
//! async, so mutation work runs in [`prefetch_settings_mutation`] before
//! execution and the resolver objects only replay the outcome. Root-stage
//! failures (subject lookup/authorization) become the `settingsMutation` field
//! error; field-stage failures are deferred to `editSettings`/
//! `overwriteSettings`, matching graph-gophers' error paths.
//!
//! Limitations (see `docs/overviews/m9_8ft_settings_mutation_overview.md`):
//! one occurrence per mutation root field name, and repeated aliases of the
//! same mutation field execute once.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use sg_db::settings::{
    create_if_up_to_date, latest_for_org, latest_for_site, latest_for_user, SettingsRecord,
    SettingsSubjectKey,
};
use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField,
};
use sg_graphql::json::Json;
use sg_jsonx::{
    apply_edits, compute_property_edit, compute_property_removal, settings_format_options,
    EditValue, Path, Segment,
};
use sg_settings_schema::validate_settings;
use sqlx::PgPool;

use crate::graphql_resolvers::{
    org_access_error, relay_kind, unmarshal_relay_id, unmarshal_relay_spec_string,
    OrgMembershipsStore, MUST_BE_SITE_ADMIN_OR_SAME_USER,
};

/// A subject that has passed `settingsSubjectForNodeAndCheckAccess`
/// (`settings_subject.go:93-127`; the baseline's
/// `checkedAccess_DO_NOT_SET_THIS_MANUALLY_OR_YOU_WILL_LEAK_SECRETS` flag).
/// Only [`resolve_subject`] constructs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckedSettingsSubject(SettingsSubjectKey);

/// `settingsMutation`'s outcome for one field selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationFieldOutcome {
    Ok,
    Err(String),
}

/// Prefetched `Mutation.settingsMutation` result. `RootError` is a failure of
/// `SchemaResolver.SettingsMutation` itself; `Ready` holds each selected
/// mutation field's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsMutationPrefetch {
    RootError(String),
    Ready(HashMap<String, MutationFieldOutcome>),
}

/// `Mutation` root (`schemaResolver.SettingsMutation` /
/// `ConfigurationMutation` plus the deprecated log-event mutations). Root
/// fields are keyed by their schema name.
#[derive(Debug, Clone, Default)]
pub struct MutationRoot {
    settings_mutations: HashMap<String, SettingsMutationPrefetch>,
    /// `Mutation.logEvent` / `Mutation.logEvents` outcomes
    /// (`crate::log_events::prefetch_log_events`).
    log_events: HashMap<String, Result<(), String>>,
    /// `Mutation.overwriteTemporarySettings` /
    /// `editTemporarySettings` outcomes
    /// (`crate::temporary_settings::prefetch_temporary_settings_mutations`).
    temporary_settings: HashMap<String, Result<(), String>>,
    /// `Mutation.createFeatureFlag` / `updateFeatureFlag` /
    /// `deleteFeatureFlag` outcomes
    /// (`crate::feature_flag_mutations::prefetch_feature_flag_mutations`).
    feature_flags: HashMap<String, crate::feature_flag_mutations::FeatureFlagMutationOutcome>,
    /// `Mutation.updateSiteConfiguration` outcomes
    /// (`crate::site_configuration_mutation::prefetch_update_site_configuration`).
    site_configuration: HashMap<String, Result<bool, String>>,
    /// `Mutation.addExternalAccount` / `deleteExternalAccount` outcomes
    /// (`crate::external_accounts_graphql::prefetch_external_account_mutations`,
    /// M9.9b).
    external_accounts: HashMap<String, Result<(), String>>,
}

impl MutationRoot {
    /// Attach a prefetched feature-flag mutation outcome.
    pub fn with_feature_flag_mutation(
        mut self,
        field: impl Into<String>,
        outcome: crate::feature_flag_mutations::FeatureFlagMutationOutcome,
    ) -> Self {
        self.feature_flags.insert(field.into(), outcome);
        self
    }

    /// Attach a prefetched temporary-settings write outcome.
    pub fn with_temporary_settings(
        mut self,
        field: impl Into<String>,
        outcome: Result<(), String>,
    ) -> Self {
        self.temporary_settings.insert(field.into(), outcome);
        self
    }

    /// Attach a prefetched `updateSiteConfiguration` outcome.
    pub fn with_site_configuration(
        mut self,
        field: impl Into<String>,
        outcome: Result<bool, String>,
    ) -> Self {
        self.site_configuration.insert(field.into(), outcome);
        self
    }

    /// Attach a prefetched external-account mutation outcome.
    pub fn with_external_account_mutation(
        mut self,
        field: impl Into<String>,
        outcome: Result<(), String>,
    ) -> Self {
        self.external_accounts.insert(field.into(), outcome);
        self
    }

    /// Attach a prefetched log-event write outcome.
    pub fn with_log_events(
        mut self,
        field: impl Into<String>,
        outcome: Result<(), String>,
    ) -> Self {
        self.log_events.insert(field.into(), outcome);
        self
    }

    pub fn with_settings_mutation(
        mut self,
        field: impl Into<String>,
        prefetch: SettingsMutationPrefetch,
    ) -> Self {
        self.settings_mutations.insert(field.into(), prefetch);
        self
    }
}

impl ObjectValue for MutationRoot {
    fn type_name(&self) -> &'static str {
        "Mutation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "settingsMutation" | "configurationMutation" => {
                match self.settings_mutations.get(field) {
                    Some(SettingsMutationPrefetch::RootError(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    Some(SettingsMutationPrefetch::Ready(outcomes)) => {
                        Ok(Some(FieldValue::Object(Arc::new(SettingsMutationObject {
                            outcomes: outcomes.clone(),
                        }))))
                    }
                    None => Ok(None),
                }
            }
            // `schemaResolver.LogEvent` / `LogEvents` return a nil
            // `*EmptyResponse` on success (GraphQL `null`) and the write error
            // otherwise.
            "logEvent" | "logEvents" => match self.log_events.get(field) {
                Some(Ok(())) => Ok(Some(FieldValue::Null)),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Ok(None),
            },
            // `schemaResolver.CreateFeatureFlag` / `UpdateFeatureFlag` return
            // the written flag (union `FeatureFlag`); `DeleteFeatureFlag`
            // returns a non-nil `&EmptyResponse{}`.
            "createFeatureFlag" | "updateFeatureFlag" | "deleteFeatureFlag" => {
                match self.feature_flags.get(field) {
                    Some(crate::feature_flag_mutations::FeatureFlagMutationOutcome::Flag(
                        data,
                        registry,
                    )) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::graphql_resolvers::FeatureFlagObject::new(
                            (**data).clone(),
                            registry.clone(),
                        ),
                    )))),
                    Some(crate::feature_flag_mutations::FeatureFlagMutationOutcome::Empty) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::temporary_settings::EmptyResponseObject,
                        ))))
                    }
                    Some(crate::feature_flag_mutations::FeatureFlagMutationOutcome::Error(
                        message,
                    )) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            // `schemaResolver.UpdateSiteConfiguration` returns
            // `configurationServer.NeedServerRestart()` (`site.go:362`).
            "updateSiteConfiguration" => match self.site_configuration.get(field) {
                Some(Ok(restart)) => Ok(Some(FieldValue::Json(Json::Bool(*restart)))),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Ok(None),
            },
            // `schemaResolver.AddExternalAccount` / `DeleteExternalAccount`
            // return a non-nil `&EmptyResponse{}` on success and the resolver
            // error otherwise (M9.9b).
            "addExternalAccount" | "deleteExternalAccount" => {
                match self.external_accounts.get(field) {
                    Some(Ok(())) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::temporary_settings::EmptyResponseObject,
                    )))),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            // `schemaResolver.LogUserEvent` has been a no-op since PR 38826.
            "logUserEvent" => Ok(Some(FieldValue::Null)),
            // `OverwriteTemporarySettings` / `EditTemporarySettings` return a
            // non-nil `*EmptyResponse`, so success is an (empty) object; the
            // baseline reports authentication/database errors on the field.
            "overwriteTemporarySettings" | "editTemporarySettings" => {
                match self.temporary_settings.get(field) {
                    Some(Ok(())) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::temporary_settings::EmptyResponseObject,
                    )))),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

/// `settingsMutation` (`settings_mutation.go:47-83`): replays the prefetched
/// per-field outcomes.
#[derive(Debug, Clone)]
pub struct SettingsMutationObject {
    outcomes: HashMap<String, MutationFieldOutcome>,
}

impl ObjectValue for SettingsMutationObject {
    fn type_name(&self) -> &'static str {
        "SettingsMutation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "editSettings" | "editConfiguration" | "overwriteSettings" => {
                match self.outcomes.get(field) {
                    Some(MutationFieldOutcome::Ok) => {
                        Ok(Some(FieldValue::Object(Arc::new(UpdateSettingsPayload))))
                    }
                    Some(MutationFieldOutcome::Err(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

/// `updateSettingsPayload` (`settings_mutation.go:76-78`): `empty` is always
/// `null` (`Empty()` returns a nil `*EmptyResponse`).
struct UpdateSettingsPayload;

impl ObjectValue for UpdateSettingsPayload {
    fn type_name(&self) -> &'static str {
        "UpdateSettingsPayload"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "empty" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

/// Run the mutation for every selected `settingsMutation` root field.
pub async fn prefetch_settings_mutation(
    pool: &PgPool,
    store: &dyn OrgMembershipsStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> MutationRoot {
    let mut root = MutationRoot::default();
    for field in fields {
        if !matches!(
            field.name.as_str(),
            "settingsMutation" | "configurationMutation"
        ) {
            continue;
        }
        let prefetch = prefetch_one(pool, store, field, actor, viewer_site_admin).await;
        root = root.with_settings_mutation(field.name.clone(), prefetch);
    }
    root
}

async fn prefetch_one(
    pool: &PgPool,
    store: &dyn OrgMembershipsStore,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> SettingsMutationPrefetch {
    let subject_id = match field.arguments.get("input") {
        Some(InputValue::Object(input)) => match input.get("subject") {
            Some(InputValue::Str(subject_id)) => subject_id,
            _ => return SettingsMutationPrefetch::RootError("invalid id".to_owned()),
        },
        _ => return SettingsMutationPrefetch::RootError("invalid id".to_owned()),
    };
    let last_id = match field.arguments.get("input").and_then(|input| match input {
        InputValue::Object(input) => input.get("lastID"),
        _ => None,
    }) {
        Some(InputValue::Int(last_id)) => i32::try_from(*last_id).ok(),
        _ => None,
    };

    let subject = match resolve_subject(store, subject_id, actor, viewer_site_admin).await {
        Ok(subject) => subject,
        Err(message) => return SettingsMutationPrefetch::RootError(message),
    };

    let author_user_id = actor.uid().unwrap_or(0);
    let mut outcomes = HashMap::new();
    for selection in &field.selections {
        let outcome = match selection.name.as_str() {
            "editSettings" | "editConfiguration" => {
                edit_settings(pool, subject, last_id, author_user_id, &selection.arguments).await
            }
            "overwriteSettings" => match selection.arguments.get("contents") {
                Some(InputValue::Str(contents)) => {
                    settings_create_if_up_to_date(pool, subject, last_id, author_user_id, contents)
                        .await
                }
                _ => Err("invalid contents".to_owned()),
            },
            _ => continue,
        };
        outcomes.insert(
            selection.name.clone(),
            match outcome {
                Ok(()) => MutationFieldOutcome::Ok,
                Err(message) => MutationFieldOutcome::Err(message),
            },
        );
    }
    SettingsMutationPrefetch::Ready(outcomes)
}

/// `SettingsMutation` (`settings_mutation.go:47-83`): `nodeByID` →
/// `settingsSubjectForNodeAndCheckAccess` → `ViewerCanAdminister`.
async fn resolve_subject(
    store: &dyn OrgMembershipsStore,
    id: &str,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<CheckedSettingsSubject, String> {
    // `nodeByID` (`node.go:38-45`): only the registered kinds settings
    // subjects use are ported. `DefaultSettings` has no `nodeByIDFns` entry in
    // the baseline either, so both report `invalid id`.
    let kind = relay_kind(id).unwrap_or_default();
    let key = match kind.as_str() {
        "User" => {
            let user_id = unmarshal_relay_id(id)?;
            let user = store.user_by_id(user_id).await?;
            // `settings_subject.go:100-104`: site admin or the user itself.
            let authorized = match actor {
                Actor::User { uid } if uid == user.id => true,
                _ => viewer_site_admin,
            };
            if !authorized {
                return Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned());
            }
            // `viewerCanAdministerSettings` (`user.go:590-599`) runs the same
            // check, so an authorized user is always an administrator.
            SettingsSubjectKey::User(user.id)
        }
        "Org" => {
            let org_id = unmarshal_relay_id(id)?;
            let org = store
                .org_by_id(org_id, actor.uid().unwrap_or(0), viewer_site_admin)
                .await?;
            // `settings_subject.go:106-111`: org member or site admin.
            if !org.viewer_can_administer {
                return Err(org_access_error(actor).to_owned());
            }
            SettingsSubjectKey::Org(org.id)
        }
        "Site" => {
            let site_id = unmarshal_relay_spec_string(id)?;
            // `siteByGQLID` (`site.go:51-61`).
            if site_id != "site" {
                return Err(format!("site not found: {site_id:?}"));
            }
            // `Site.ViewerCanAdminister` (`site.go:135-142`): only site
            // admins may edit global settings.
            if !viewer_site_admin {
                return Err("viewer is not allowed to edit these settings".to_owned());
            }
            SettingsSubjectKey::Site
        }
        _ => return Err("invalid id".to_owned()),
    };
    Ok(CheckedSettingsSubject(key))
}

/// `settingsMutation.EditSettings` (`settings_mutation.go:121-167`).
async fn edit_settings(
    pool: &PgPool,
    subject: CheckedSettingsSubject,
    last_id: Option<i32>,
    author_user_id: i32,
    arguments: &Arguments,
) -> Result<(), String> {
    let Some(InputValue::Object(edit)) = arguments.get("edit") else {
        return Err("invalid edit".to_owned());
    };

    let key_path = match edit.get("keyPath") {
        Some(key_path) => to_key_path(key_path)?,
        None => Vec::new(),
    };

    // `remove := args.Edit.Value == nil`. graph-gophers does not unmarshal a
    // JSON `null` into the `*JSONValue` scalar (the pointer stays nil), so an
    // explicit JSON null is indistinguishable from an absent field and also
    // removes — only a non-null value is an edit. `Edit.Value.Value` is never
    // null in the baseline.
    let value_field = edit.get("value");
    let value = match value_field {
        None | Some(InputValue::Null) => None,
        Some(value) => Some(value),
    };
    let value_jsonc = matches!(
        edit.get("valueIsJSONCEncodedString"),
        Some(InputValue::Bool(true))
    );
    let value = if value_jsonc {
        match value {
            Some(InputValue::Str(text)) => Some(EditValue::Raw(text.clone())),
            _ => {
                return Err("value must be a string for valueIsJSONCEncodedString".to_owned());
            }
        }
    } else {
        value.map(|value| EditValue::Json(input_value_json(value)))
    };

    let current = get_current_settings(pool, subject.0, last_id).await?;
    let options = settings_format_options();
    let edits = match value {
        None => compute_property_removal(&current, &key_path, &options),
        Some(value) => compute_property_edit(&current, &key_path, value, &options),
    }?;
    let new_settings = apply_edits(&current, &edits)?;

    settings_create_if_up_to_date(pool, subject, last_id, author_user_id, &new_settings).await
}

/// `toKeyPath` (`settings_mutation.go:104-119`).
fn to_key_path(value: &InputValue) -> Result<Path, String> {
    let InputValue::List(segments) = value else {
        return Err("invalid key path".to_owned());
    };
    let mut path = Vec::with_capacity(segments.len());
    for (index, segment) in segments.iter().enumerate() {
        let InputValue::Object(segment) = segment else {
            return Err(invalid_key_path_segment(index));
        };
        let property = match segment.get("property") {
            None | Some(InputValue::Null) => None,
            Some(InputValue::Str(property)) => Some(property.clone()),
            Some(_) => None,
        };
        let array_index = match segment.get("index") {
            None | Some(InputValue::Null) => None,
            Some(InputValue::Int(array_index)) => Some(*array_index),
            Some(_) => None,
        };
        match (property, array_index) {
            (Some(property), None) => path.push(Segment::Property(property)),
            (None, Some(array_index)) => path.push(Segment::Index(array_index)),
            _ => return Err(invalid_key_path_segment(index)),
        }
    }
    Ok(path)
}

fn invalid_key_path_segment(index: usize) -> String {
    format!(
        "invalid key path segment at index {index}: exactly 1 of property and index must be non-null"
    )
}

/// `json.Marshal` of a `JSONValue` scalar: variables are decoded by Go into
/// `map[string]interface{}`/`[]interface{}`/`float64`/`string`/`bool`/`nil`,
/// so object keys sort and Go's float formatting applies.
pub(crate) fn input_value_json(value: &InputValue) -> String {
    fn to_json(value: &InputValue) -> Json {
        match value {
            InputValue::Null => Json::Null,
            InputValue::Bool(value) => Json::Bool(*value),
            InputValue::Int(value) => Json::Int(*value),
            InputValue::Float(value) => Json::Float(*value),
            InputValue::Str(value) | InputValue::Enum(value) => Json::Str(value.clone()),
            InputValue::List(items) => Json::Arr(items.iter().map(to_json).collect()),
            // `InputValue::Object` is a `BTreeMap`, i.e. already sorted like
            // `json.Marshal` of a Go map.
            InputValue::Object(entries) => Json::Obj(
                entries
                    .iter()
                    .map(|(key, value)| (key.clone(), to_json(value)))
                    .collect(),
            ),
        }
    }
    to_json(value).to_compact_string()
}

/// `settingsMutation.getCurrentSettings` (`settings_mutation.go:204-224`).
async fn get_current_settings(
    pool: &PgPool,
    subject: SettingsSubjectKey,
    last_id: Option<i32>,
) -> Result<String, String> {
    let latest = latest_settings(pool, subject).await?;
    match latest {
        Some(settings) if last_id == Some(settings.id) => Ok(settings.contents),
        None if last_id.is_none() => Ok(String::new()),
        Some(settings) => Err(version_mismatch(Some(settings.id), last_id)),
        None => Err(version_mismatch(None, last_id)),
    }
}

/// `getCurrentSettings`'s version-mismatch error (`intOrNull` for both IDs).
fn version_mismatch(latest_id: Option<i32>, wanted: Option<i32>) -> String {
    format!(
        "update settings version mismatch: last ID is {} (mutation wanted {})",
        int_or_null(latest_id),
        int_or_null(wanted)
    )
}

fn int_or_null(value: Option<i32>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "null".to_owned(),
    }
}

async fn latest_settings(
    pool: &PgPool,
    subject: SettingsSubjectKey,
) -> Result<Option<SettingsRecord>, String> {
    let latest = match subject {
        SettingsSubjectKey::User(user_id) => latest_for_user(pool, user_id).await,
        SettingsSubjectKey::Org(org_id) => latest_for_org(pool, org_id).await,
        SettingsSubjectKey::Site => latest_for_site(pool).await,
    };
    latest.map_err(|error| error.to_string())
}

/// `settingsCreateIfUpToDate` (`settings.go:55-81`) +
/// `settingsStore.CreateIfUpToDate` (`internal/database/settings.go:48-86`).
async fn settings_create_if_up_to_date(
    pool: &PgPool,
    subject: CheckedSettingsSubject,
    last_id: Option<i32>,
    author_user_id: i32,
    contents: &str,
) -> Result<(), String> {
    // `subject.assertCheckedAccess()` is enforced by the `CheckedSettingsSubject`
    // type: only `resolve_subject` builds it, after the access checks.

    if std::env::var("GLOBAL_SETTINGS_FILE")
        .map(|file| !file.is_empty())
        .unwrap_or(false)
        && subject.0 == SettingsSubjectKey::Site
        && !global_settings_allow_edits()
    {
        return Err(
            "Updating global settings not allowed when using GLOBAL_SETTINGS_FILE".to_owned(),
        );
    }

    // `conf.ValidateSettings`: problems are joined with "," after the
    // `invalid settings: ` prefix (`internal/database/settings.go:48-52`).
    let problems = validate_settings(contents);
    if !problems.is_empty() {
        return Err(format!("invalid settings: {}", problems.join(",")));
    }

    create_if_up_to_date(pool, subject.0, last_id, Some(author_user_id), contents)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// `globalSettingsAllowEdits` (`settings.go:57`): `strconv.ParseBool` of
/// `GLOBAL_SETTINGS_ALLOW_EDITS` (default false, parse errors ignored),
/// evaluated once at startup in the baseline and once per process here.
fn global_settings_allow_edits() -> bool {
    static ALLOW: OnceLock<bool> = OnceLock::new();
    *ALLOW.get_or_init(|| {
        std::env::var("GLOBAL_SETTINGS_ALLOW_EDITS")
            .map(|value| parse_bool(&value).unwrap_or(false))
            .unwrap_or(false)
    })
}

/// `strconv.ParseBool`.
fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(entries: Vec<(&str, InputValue)>) -> InputValue {
        InputValue::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        )
    }

    #[test]
    fn key_path_segment_validation() {
        assert_eq!(
            to_key_path(&InputValue::List(vec![object(vec![
                ("index", InputValue::Int(1)),
                ("property", InputValue::Null),
            ])])),
            Ok(vec![Segment::Index(1)])
        );
        let err = to_key_path(&InputValue::List(vec![object(vec![])])).expect_err("missing both");
        assert_eq!(err, invalid_key_path_segment(0));
        let err = to_key_path(&InputValue::List(vec![object(vec![
            ("index", InputValue::Int(0)),
            ("property", InputValue::Str("a".to_owned())),
        ])]))
        .expect_err("both set");
        assert!(err.starts_with("invalid key path segment at index 0"));
    }

    #[test]
    fn json_value_marshalling_sorts_keys_and_escapes() {
        assert_eq!(input_value_json(&InputValue::Null), "null");
        assert_eq!(input_value_json(&InputValue::Int(2)), "2");
        assert_eq!(input_value_json(&InputValue::Float(2.0)), "2");
        assert_eq!(
            input_value_json(&object(vec![
                ("b", InputValue::List(vec![InputValue::Bool(true)])),
                ("a", InputValue::Str("<a&b>".to_owned())),
            ])),
            // Go `json.Marshal` HTML-escapes `<`, `>`, `&`.
            "{\"a\":\"\\u003ca\\u0026b\\u003e\",\"b\":[true]}"
        );
    }

    #[test]
    fn parse_bool_matches_strconv() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("False"), Some(false));
        assert_eq!(parse_bool("yes"), None);
    }

    #[test]
    fn version_mismatch_formats_like_go() {
        assert_eq!(
            version_mismatch(None, Some(7)),
            "update settings version mismatch: last ID is null (mutation wanted 7)"
        );
        assert_eq!(
            version_mismatch(Some(3), None),
            "update settings version mismatch: last ID is 3 (mutation wanted null)"
        );
    }
}
