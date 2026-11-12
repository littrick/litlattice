#![forbid(unsafe_code)]

//! `Query.evaluateFeatureFlag` / `evaluatedFeatureFlags` /
//! `organizationFeatureFlagValue` / `organizationFeatureFlagOverrides` and the
//! override mutations (`cmd/frontend/graphqlbackend/feature_flags.go:145-200,
//! 290-340`, `internal/database/feature_flags.go:278-390,416-462,490-640`,
//! `internal/featureflag/{middleware,flagset}.go`).
//!
//! `evaluateFeatureFlag` uses the middleware's lazy flag set: the user's flags
//! for an authenticated actor, the anonymous flags for an anonymous UID, and
//! the global flags otherwise; it also writes the result into the Redis
//! evaluation cache (`FlagSet.GetBool`). `evaluatedFeatureFlags` reads that
//! cache for the request actor (`getEvaluatedFlagSetFromCache`), so the two
//! fields share the cache across a request in query order.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use serde::Deserialize;
use sg_db::feature_flags::FeatureFlagOverride;
use sg_graphql::exec::{Actor, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;

use crate::admin_store::{check_site_admin_or_same_user, NOT_AUTHENTICATED};
use crate::feature_flag_cache::{
    get_evaluated_flag_set_from_cache, get_bool, load_flags_for_actor, EvaluatedFlagCache,
    FlagActor,
};
use crate::graphql_resolvers::{
    feature_flag_data, marshal_feature_flag_override_id, unmarshal_relay_id,
    FeatureFlagOverrideData, FeatureFlagOverrideObject, FeatureFlagRegistry, FeatureFlagStore,
    NamespaceLookup, OrgMembershipsStore,
};

/// Prefetched business feature-flag roots.
#[derive(Debug, Clone, Default)]
pub struct FeatureFlagBusinessRoots {
    /// The middleware's lazily fetched `FlagSet` for the request actor.
    pub evaluated: Option<HashMap<String, bool>>,
    /// `evaluatedFeatureFlags` snapshot, keyed by the query order (sorted by
    /// name for a deterministic response).
    pub evaluated_flags: Option<Vec<(String, bool)>>,
    /// `evaluateFeatureFlag(flagName)` results keyed by flag name.
    pub evaluate: HashMap<String, Result<Option<bool>, String>>,
    /// `organizationFeatureFlagValue(orgID, flagName)` results keyed by
    /// `(org_id, flag_name)`.
    pub org_value: HashMap<(i32, String), Result<bool, String>>,
    /// `organizationFeatureFlagOverrides` result (only authenticated).
    pub org_overrides: Option<Result<Vec<FeatureFlagOverrideData>, String>>,
    /// `targetFlag` data for the overrides above.
    pub org_override_registry: Arc<FeatureFlagRegistry>,
    /// Override mutations keyed by root field ordinal.
    pub mutations: HashMap<String, OverrideMutationOutcome>,
}

/// One prefetched override mutation outcome.
#[derive(Debug, Clone)]
pub enum OverrideMutationOutcome {
    Override {
        data: FeatureFlagOverrideData,
        registry: Arc<FeatureFlagRegistry>,
    },
    Empty,
    Error(String),
}

/// Prefetch the four business feature-flag query roots.
pub async fn prefetch_feature_flag_business(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    fields: &[sg_graphql::exec::RootField],
    actor: Actor,
    anonymous_uid: &str,
    viewer_site_admin: bool,
    cache: Option<&dyn EvaluatedFlagCache>,
) -> FeatureFlagBusinessRoots {
    let mut roots = FeatureFlagBusinessRoots::default();
    let flag_actor = FlagActor::from_exec(actor, anonymous_uid);

    // `featureflag.Middleware`'s lazy `flagSetFetcher`: the actor's FlagSet is
    // only loaded when a field that needs it is selected. The loader mirrors
    // `fetchForActor`: user flags, then anonymous flags, then global flags
    // (`internal/featureflag/middleware.go:67-91`).
    let needs_flags = fields
        .iter()
        .any(|field| field.name == "evaluateFeatureFlag" || field.name == "evaluatedFeatureFlags");
    let flags = if needs_flags {
        let loaded = load_flags_for_actor(flags_store, &flag_actor).await;
        roots.evaluated = Some(loaded.clone());
        Some(loaded)
    } else {
        None
    };

    // graph-gophers resolves root fields serially in query order. Mirror that
    // order so a `evaluateFeatureFlag` cache write (Go's `FlagSet.GetBool`,
    // `internal/featureflag/flagset.go:41-51`) is visible to a later
    // `evaluatedFeatureFlags` in the same request.
    for field in fields {
        match field.name.as_str() {
            "evaluateFeatureFlag" => {
                let Some(name) = field.arguments.string("flagName") else {
                    continue;
                };
                if roots.evaluate.contains_key(name) {
                    continue;
                }
                let flags = flags.as_ref().expect("flag set loaded above");
                let value = get_bool(cache, flags, &flag_actor, name).await;
                roots.evaluate.insert(name.to_owned(), Ok(value));
            }
            "evaluatedFeatureFlags" => {
                let snapshot = match (cache, flags.as_ref()) {
                    (Some(cache), Some(flags)) => {
                        get_evaluated_flag_set_from_cache(cache, flags, &flag_actor).await
                    }
                    // No cache seam (transport-only wiring): the empty set,
                    // like the pre-cache port.
                    _ => HashMap::new(),
                };
                let mut pairs: Vec<(String, bool)> = snapshot.into_iter().collect();
                pairs.sort_by(|left, right| left.0.cmp(&right.0));
                roots.evaluated_flags = Some(pairs);
            }
            _ => {}
        }
    }

    for field in fields {
        match field.name.as_str() {
            "organizationFeatureFlagValue" => {
                let Some(org_id) = field
                    .arguments
                    .string("orgID")
                    .and_then(|id| unmarshal_relay_id(id).ok())
                else {
                    continue;
                };
                let Some(flag_name) = field.arguments.string("flagName") else {
                    continue;
                };
                if roots
                    .org_value
                    .contains_key(&(org_id, flag_name.to_owned()))
                {
                    continue;
                }
                // `auth.CheckOrgAccessOrSiteAdmin`: any auth error is
                // swallowed and the resolver returns `false`.
                let allowed =
                    org_access_allowed(namespaces, actor, viewer_site_admin, org_id).await;
                let value = if !allowed {
                    Ok(false)
                } else {
                    match flags_store
                        .get_org_override_for_flag(org_id, flag_name)
                        .await
                    {
                        Ok(Some(override_row)) => Ok(override_row.value),
                        Ok(None) => match flags_store.get_feature_flag(flag_name).await {
                            Ok(Some(flag)) => Ok(flag.evaluate_global().unwrap_or(false)),
                            // `GetFeatureFlag` errors (including no rows) are
                            // ignored: `globalFlag` stays nil.
                            Ok(None) | Err(_) => Ok(false),
                        },
                        Err(message) => Err(message),
                    }
                };
                roots
                    .org_value
                    .insert((org_id, flag_name.to_owned()), value);
            }
            "organizationFeatureFlagOverrides" => {
                if roots.org_overrides.is_some() {
                    continue;
                }
                let Some(uid) = actor.uid() else {
                    roots.org_overrides = Some(Err("no current user".to_owned()));
                    continue;
                };
                let rows = flags_store.get_org_overrides_for_user(uid).await;
                let org_overrides_outcome = match rows {
                    Ok(rows) => {
                        let mut data = Vec::with_capacity(rows.len());
                        for row in rows {
                            // `FeatureFlagOverrideResolver.Namespace`
                            // prefers the user namespace when both are set
                            // (`feature_flags.go:100-108`).
                            let namespace = match (row.user_id, row.org_id) {
                                (Some(user_id), _) => match namespaces.user_by_id(user_id).await {
                                    Ok(user) => NamespaceLookup::User(user),
                                    Err(message) => NamespaceLookup::Error(message),
                                },
                                (None, Some(org_id)) => {
                                    namespace_org(namespaces, org_id, actor, viewer_site_admin)
                                        .await
                                }
                                (None, None) => NamespaceLookup::NotFound,
                            };
                            data.push(FeatureFlagOverrideData {
                                user_id: row.user_id,
                                org_id: row.org_id,
                                flag_name: row.flag_name,
                                value: row.value,
                                namespace,
                            });
                        }
                        let mut registry = FeatureFlagRegistry::default();
                        for override_data in &data {
                            if !registry.flags.contains_key(&override_data.flag_name) {
                                if let Ok(Some(flag)) =
                                    flags_store.get_feature_flag(&override_data.flag_name).await
                                {
                                    let flag_data = feature_flag_data(
                                        flags_store,
                                        namespaces,
                                        flag,
                                        actor,
                                        viewer_site_admin,
                                    )
                                    .await;
                                    registry.flags.insert(flag_data.name.clone(), flag_data);
                                }
                            }
                        }
                        roots.org_override_registry = Arc::new(registry);
                        Ok(data)
                    }
                    Err(message) => Err(message),
                };
                roots.org_overrides = Some(org_overrides_outcome);
            }
            _ => {}
        }
    }
    roots
}

async fn org_access_allowed(
    namespaces: &dyn OrgMembershipsStore,
    actor: Actor,
    viewer_site_admin: bool,
    org_id: i32,
) -> bool {
    if actor.is_internal() || viewer_site_admin {
        return true;
    }
    let Some(uid) = actor.uid() else {
        return false;
    };
    namespaces
        .org_by_id(org_id, uid, viewer_site_admin)
        .await
        .map(|org| org.viewer_is_member)
        .unwrap_or(false)
}

async fn namespace_org(
    namespaces: &dyn OrgMembershipsStore,
    org_id: i32,
    actor: Actor,
    viewer_site_admin: bool,
) -> NamespaceLookup {
    match namespaces
        .org_by_id(org_id, actor.uid().unwrap_or(0), viewer_site_admin)
        .await
    {
        Ok(org) => NamespaceLookup::Org(org),
        Err(message) => NamespaceLookup::Error(message),
    }
}

/// Prefetch `createFeatureFlagOverride` / `updateFeatureFlagOverride` /
/// `deleteFeatureFlagOverride`.
pub async fn prefetch_feature_flag_override_mutations(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    fields: &[sg_graphql::exec::RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, OverrideMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "createFeatureFlagOverride" => {
                create_override(flags_store, namespaces, field, actor, viewer_site_admin).await
            }
            "updateFeatureFlagOverride" => {
                update_override(flags_store, namespaces, field, actor, viewer_site_admin).await
            }
            "deleteFeatureFlagOverride" => {
                delete_override(flags_store, field, actor, viewer_site_admin).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

async fn create_override(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    field: &sg_graphql::exec::RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> OverrideMutationOutcome {
    let Some(namespace_id) = field.arguments.string("namespace") else {
        return OverrideMutationOutcome::Error("invalid graphql.ID".to_owned());
    };
    let (uid, oid) = match unmarshal_namespace_id(namespace_id) {
        Ok(ids) => ids,
        Err(message) => return OverrideMutationOutcome::Error(message),
    };
    if let Err(message) = check_site_admin_or_same_user(actor, viewer_site_admin, uid) {
        return OverrideMutationOutcome::Error(message);
    }
    let Some(flag_name) = field.arguments.string("flagName") else {
        return OverrideMutationOutcome::Error("missing flagName argument".to_owned());
    };
    let Some(value) = field.arguments.bool("value") else {
        return OverrideMutationOutcome::Error("missing value argument".to_owned());
    };
    let (user_id, org_id) = if uid != 0 {
        (Some(uid), None)
    } else if oid != 0 {
        (None, Some(oid))
    } else {
        (None, None)
    };
    match flags_store
        .create_override(user_id, org_id, flag_name, value)
        .await
    {
        Ok(row) => override_response(flags_store, namespaces, row, actor, viewer_site_admin).await,
        Err(message) => OverrideMutationOutcome::Error(message),
    }
}

async fn update_override(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    field: &sg_graphql::exec::RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> OverrideMutationOutcome {
    if let Err(message) =
        crate::graphql_resolvers::check_current_user_is_site_admin(actor, viewer_site_admin)
    {
        return OverrideMutationOutcome::Error(message);
    }
    let Some(spec) = field
        .arguments
        .string("id")
        .and_then(|id| unmarshal_override_id(id).ok())
    else {
        return OverrideMutationOutcome::Error("invalid graphql.ID".to_owned());
    };
    let Some(value) = field.arguments.bool("value") else {
        return OverrideMutationOutcome::Error("missing value argument".to_owned());
    };
    match flags_store
        .update_override(spec.user_id, spec.org_id, &spec.flag_name, value)
        .await
    {
        Ok(row) => override_response(flags_store, namespaces, row, actor, viewer_site_admin).await,
        Err(message) => OverrideMutationOutcome::Error(message),
    }
}

async fn delete_override(
    flags_store: &dyn FeatureFlagStore,
    field: &sg_graphql::exec::RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> OverrideMutationOutcome {
    if let Err(message) =
        crate::graphql_resolvers::check_current_user_is_site_admin(actor, viewer_site_admin)
    {
        return OverrideMutationOutcome::Error(message);
    }
    let Some(spec) = field
        .arguments
        .string("id")
        .and_then(|id| unmarshal_override_id(id).ok())
    else {
        return OverrideMutationOutcome::Error("invalid graphql.ID".to_owned());
    };
    match flags_store
        .delete_override(spec.user_id, spec.org_id, &spec.flag_name)
        .await
    {
        Ok(()) => OverrideMutationOutcome::Empty,
        Err(message) => OverrideMutationOutcome::Error(message),
    }
}

async fn override_response(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    row: FeatureFlagOverride,
    actor: Actor,
    viewer_site_admin: bool,
) -> OverrideMutationOutcome {
    let namespace = match (row.user_id, row.org_id) {
        (Some(user_id), _) => match namespaces.user_by_id(user_id).await {
            Ok(user) => NamespaceLookup::User(user),
            Err(message) => NamespaceLookup::Error(message),
        },
        (None, Some(org_id)) => namespace_org(namespaces, org_id, actor, viewer_site_admin).await,
        (None, None) => NamespaceLookup::Error("one of userID or orgID must be set".to_owned()),
    };
    let registry = match flags_store.get_feature_flag(&row.flag_name).await {
        Ok(Some(flag)) => {
            let data =
                feature_flag_data(flags_store, namespaces, flag, actor, viewer_site_admin).await;
            let mut registry = FeatureFlagRegistry::default();
            registry.flags.insert(data.name.clone(), data);
            Arc::new(registry)
        }
        Ok(None) => Arc::new(FeatureFlagRegistry::default()),
        Err(message) => return OverrideMutationOutcome::Error(message),
    };
    OverrideMutationOutcome::Override {
        data: FeatureFlagOverrideData {
            user_id: row.user_id,
            org_id: row.org_id,
            flag_name: row.flag_name,
            value: row.value,
            namespace,
        },
        registry,
    }
}

/// `EvaluatedFeatureFlag` object.
pub struct EvaluatedFeatureFlagObject {
    pub name: String,
    pub value: bool,
}

impl ObjectValue for EvaluatedFeatureFlagObject {
    fn type_name(&self) -> &'static str {
        "EvaluatedFeatureFlag"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &sg_graphql::exec::Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "value" => Ok(Some(FieldValue::Json(Json::Bool(self.value)))),
            _ => Ok(None),
        }
    }
}

/// Render one prefetched `organizationFeatureFlagOverrides` entry.
pub struct OrgOverrideObject {
    pub data: FeatureFlagOverrideData,
    pub registry: Arc<FeatureFlagRegistry>,
}

impl ObjectValue for OrgOverrideObject {
    fn type_name(&self) -> &'static str {
        "FeatureFlagOverride"
    }

    fn field(
        &self,
        field: &str,
        arguments: &sg_graphql::exec::Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        FeatureFlagOverrideObject::field_object(&self.data, &self.registry, field, arguments, actor)
    }
}

/// `FeatureFlagOverride` resolver shared by the read and write surfaces.
impl FeatureFlagOverrideObject {
    pub fn field_object(
        data: &FeatureFlagOverrideData,
        registry: &Arc<FeatureFlagRegistry>,
        field: &str,
        _arguments: &sg_graphql::exec::Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_feature_flag_override_id(data.user_id, data.org_id, &data.flag_name),
            )))),
            "value" => Ok(Some(FieldValue::Json(Json::Bool(data.value)))),
            "namespace" => match &data.namespace {
                NamespaceLookup::User(user) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::UserObject::from_data_as_viewer(user.clone(), false),
                )))),
                NamespaceLookup::Org(org) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::OrgObject::from_data_as_viewer(org.clone(), false),
                )))),
                NamespaceLookup::NotFound => Err(ExecError::new("namespace not found")),
                NamespaceLookup::Error(message) => Err(ExecError::new(message.clone())),
            },
            "targetFlag" => match registry.flags.get(&data.flag_name) {
                Some(flag) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::FeatureFlagObject::new(
                        flag.clone(),
                        registry.clone(),
                    ),
                )))),
                None => Err(ExecError::new("sql: no rows in result set")),
            },
            _ => Ok(None),
        }
    }
}

/// `relay.UnmarshalSpec` for `FeatureFlagOverride` IDs.
#[derive(Debug, Deserialize)]
struct OverrideSpec {
    #[serde(rename = "UserID")]
    user_id: Option<i32>,
    #[serde(rename = "OrgID")]
    org_id: Option<i32>,
    #[serde(rename = "FlagName")]
    flag_name: String,
}

fn unmarshal_override_id(id: &str) -> Result<OverrideSpec, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// `UnmarshalNamespaceID`: any `Kind:int32` ID yields `(uid, oid)`, decided by
/// the kind. An unknown kind is 0/0 (the baseline passes both zero).
fn unmarshal_namespace_id(id: &str) -> Result<(i32, i32), String> {
    let kind = crate::graphql_resolvers::relay_kind(id).unwrap_or_default();
    let value = unmarshal_relay_id(id)?;
    match kind.as_str() {
        "User" => Ok((value, 0)),
        "Org" => Ok((0, value)),
        _ => Ok((0, 0)),
    }
}

/// `auth.ErrNotAuthenticated` text used by the unauthenticated overrides root.
pub const NO_CURRENT_USER: &str = "no current user";

pub fn unauthenticated_overrides() -> Result<Vec<FeatureFlagOverrideData>, String> {
    Err(NO_CURRENT_USER.to_owned())
}

pub const _NOT_AUTHENTICATED: &str = NOT_AUTHENTICATED;

/// Expose the prefetched `evaluateFeatureFlag` value as `*bool`.
pub fn evaluate_nullable(value: Option<bool>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Bool(value)),
        None => FieldValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use sg_db::feature_flags::{FeatureFlag, FeatureFlagKind, FeatureFlagOverride};

    use crate::graphql_resolvers::{
        MembershipData, NamespaceName, OrgData, SettingsData, UserData,
    };

    #[derive(Default)]
    struct MockFlags {
        user: HashMap<String, bool>,
        anonymous: HashMap<String, bool>,
        global: HashMap<String, bool>,
    }

    #[async_trait]
    impl FeatureFlagStore for MockFlags {
        async fn get_feature_flag(&self, _name: &str) -> Result<Option<FeatureFlag>, String> {
            unimplemented!()
        }
        async fn get_feature_flags(&self) -> Result<Vec<FeatureFlag>, String> {
            unimplemented!()
        }
        async fn get_overrides_for_flag(
            &self,
            _name: &str,
        ) -> Result<Vec<FeatureFlagOverride>, String> {
            unimplemented!()
        }
        async fn create_feature_flag(
            &self,
            _name: &str,
            _kind: FeatureFlagKind,
        ) -> Result<FeatureFlag, String> {
            unimplemented!()
        }
        async fn update_feature_flag(
            &self,
            _name: &str,
            _kind: FeatureFlagKind,
        ) -> Result<FeatureFlag, String> {
            unimplemented!()
        }
        async fn delete_feature_flag(&self, _name: &str) -> Result<(), String> {
            unimplemented!()
        }
        async fn get_user_flags(&self, _user_id: i32) -> Result<HashMap<String, bool>, String> {
            Ok(self.user.clone())
        }
        async fn get_anonymous_flags(
            &self,
            _anonymous_uid: &str,
        ) -> Result<HashMap<String, bool>, String> {
            Ok(self.anonymous.clone())
        }
        async fn get_global_flags(&self) -> Result<HashMap<String, bool>, String> {
            Ok(self.global.clone())
        }
        async fn get_org_override_for_flag(
            &self,
            _org_id: i32,
            _flag_name: &str,
        ) -> Result<Option<FeatureFlagOverride>, String> {
            unimplemented!()
        }
        async fn get_org_overrides_for_user(
            &self,
            _user_id: i32,
        ) -> Result<Vec<FeatureFlagOverride>, String> {
            unimplemented!()
        }
        async fn create_override(
            &self,
            _user_id: Option<i32>,
            _org_id: Option<i32>,
            _flag_name: &str,
            _value: bool,
        ) -> Result<FeatureFlagOverride, String> {
            unimplemented!()
        }
        async fn update_override(
            &self,
            _user_id: Option<i32>,
            _org_id: Option<i32>,
            _flag_name: &str,
            _value: bool,
        ) -> Result<FeatureFlagOverride, String> {
            unimplemented!()
        }
        async fn delete_override(
            &self,
            _user_id: Option<i32>,
            _org_id: Option<i32>,
            _flag_name: &str,
        ) -> Result<(), String> {
            unimplemented!()
        }
    }

    struct NoNamespaces;

    #[async_trait]
    impl OrgMembershipsStore for NoNamespaces {
        async fn memberships_for_user(&self, _user_id: i32) -> Result<Vec<MembershipData>, String> {
            unimplemented!()
        }
        async fn namespace_by_name(&self, _name: &str) -> Result<Option<NamespaceName>, String> {
            unimplemented!()
        }
        async fn org_by_id(
            &self,
            _org_id: i32,
            _viewer_uid: i32,
            _viewer_site_admin: bool,
        ) -> Result<OrgData, String> {
            unimplemented!()
        }
        async fn user_by_id(&self, _user_id: i32) -> Result<UserData, String> {
            unimplemented!()
        }
        async fn latest_settings_for_user(
            &self,
            _user_id: i32,
        ) -> Result<Option<SettingsData>, String> {
            unimplemented!()
        }
        async fn latest_settings_for_org(
            &self,
            _org_id: i32,
        ) -> Result<Option<SettingsData>, String> {
            unimplemented!()
        }
        async fn latest_settings_for_site(&self) -> Result<Option<SettingsData>, String> {
            unimplemented!()
        }
    }

    #[derive(Default)]
    struct MemoryCache {
        hashes: Mutex<HashMap<(String, String), String>>,
    }

    #[async_trait]
    impl EvaluatedFlagCache for MemoryCache {
        async fn hget_bool(&self, key: &str, field: &str) -> Result<Option<bool>, String> {
            Ok(self
                .hashes
                .lock()
                .unwrap()
                .get(&(key.to_owned(), field.to_owned()))
                .and_then(|value| crate::feature_flag_cache::parse_go_bool(value)))
        }
        async fn hset_bool(&self, key: &str, field: &str, value: bool) -> Result<(), String> {
            self.hashes.lock().unwrap().insert(
                (key.to_owned(), field.to_owned()),
                crate::feature_flag_cache::format_go_bool(value).to_owned(),
            );
            Ok(())
        }
        async fn del(&self, key: &str) -> Result<(), String> {
            self.hashes
                .lock()
                .unwrap()
                .retain(|(stored_key, _), _| stored_key != key);
            Ok(())
        }
    }

    async fn fields_for(query: &str, actor: Actor) -> Vec<sg_graphql::exec::RootField> {
        let schema = sg_graphql::schema::assemble_oss().expect("oss schema");
        let variables = serde_json::Map::new();
        let request = sg_graphql::exec::ExecRequest {
            query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        sg_graphql::exec::root_fields(&schema, &request)
    }

    /// `evaluateFeatureFlag` writes the actor's cached value, and a later
    /// `evaluatedFeatureFlags` reads it back (`flagset.go:41-51`,
    /// `cache.go:16-32`).
    #[tokio::test]
    async fn evaluate_then_evaluated_uses_request_actor() {
        let fields = fields_for(
            "{ evaluateFeatureFlag(flagName: \"alpha\") evaluatedFeatureFlags { name value } }",
            Actor::User { uid: 7 },
        )
        .await;
        let store = MockFlags {
            user: HashMap::from([("alpha".to_owned(), true)]),
            ..MockFlags::default()
        };
        let cache = MemoryCache::default();
        let roots = prefetch_feature_flag_business(
            &store,
            &NoNamespaces,
            &fields,
            Actor::User { uid: 7 },
            "",
            false,
            Some(&cache),
        )
        .await;

        assert_eq!(roots.evaluate.get("alpha"), Some(&Ok(Some(true))));
        assert_eq!(
            roots.evaluated_flags,
            Some(vec![("alpha".to_owned(), true)])
        );
        assert_eq!(
            cache
                .hashes
                .lock()
                .unwrap()
                .get(&("ff_alpha".to_owned(), "uid_7".to_owned())),
            Some(&"true".to_owned())
        );
    }

    /// Field order matters: `evaluatedFeatureFlags` selected *before* the
    /// evaluation sees no cached write yet, exactly like graph-gophers'
    /// serial resolver execution.
    #[tokio::test]
    async fn evaluated_before_evaluate_is_empty_and_anonymous_uid_is_used() {
        let fields = fields_for(
            "{ evaluatedFeatureFlags { name value } evaluateFeatureFlag(flagName: \"beta\") }",
            Actor::Anonymous,
        )
        .await;
        // Global says false; only the anonymous set says true. `fetchForActor`
        // must pick the anonymous branch for `auid_anonZ`.
        let store = MockFlags {
            anonymous: HashMap::from([("beta".to_owned(), true)]),
            global: HashMap::from([("beta".to_owned(), false)]),
            ..MockFlags::default()
        };
        let cache = MemoryCache::default();
        let roots = prefetch_feature_flag_business(
            &store,
            &NoNamespaces,
            &fields,
            Actor::Anonymous,
            "anonZ",
            false,
            Some(&cache),
        )
        .await;

        assert_eq!(roots.evaluated_flags, Some(Vec::new()));
        assert_eq!(roots.evaluate.get("beta"), Some(&Ok(Some(true))));
        assert_eq!(
            cache
                .hashes
                .lock()
                .unwrap()
                .get(&("ff_beta".to_owned(), "auid_anonZ".to_owned())),
            Some(&"true".to_owned())
        );
    }

    /// No authentication and no anonymous UID: visitor ID is empty, so reads
    /// are empty and writes are skipped; the loader falls back to global flags.
    #[tokio::test]
    async fn uidless_actor_reads_global_and_never_writes() {
        let fields = fields_for(
            "{ evaluateFeatureFlag(flagName: \"gamma\") evaluatedFeatureFlags { name value } }",
            Actor::Anonymous,
        )
        .await;
        let store = MockFlags {
            global: HashMap::from([("gamma".to_owned(), true)]),
            ..MockFlags::default()
        };
        let cache = MemoryCache::default();
        let roots = prefetch_feature_flag_business(
            &store,
            &NoNamespaces,
            &fields,
            Actor::Anonymous,
            "",
            false,
            Some(&cache),
        )
        .await;

        assert_eq!(roots.evaluate.get("gamma"), Some(&Ok(Some(true))));
        assert_eq!(roots.evaluated_flags, Some(Vec::new()));
        assert!(cache.hashes.lock().unwrap().is_empty());
    }
}
