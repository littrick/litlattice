#![forbid(unsafe_code)]

//! `Mutation.addExternalService` / `updateExternalService` /
//! `deleteExternalService` / `excludeRepoFromExternalServices`
//! (`cmd/frontend/graphqlbackend/external_services.go:35-360`).
//!
//! Only site admins may write. The code-host connection probe
//! (`newExternalServices(...).ValidateConnection`) and the sync-job enqueue are
//! [`AdminEffects`] seams; the warning text they produce is part of the GraphQL
//! response.

use sg_graphql::exec::{Actor, InputValue, RootField};

use crate::admin_store::{
    check_current_user_is_site_admin, AdminEffects, AdminMutationOutcome, AdminStore,
    NewExternalService,
};
use crate::external_services::unmarshal_external_service_id;

/// `externalServicesWritable()` (`external_services.go:35-40`).
const EXTSVC_CONFIG_FILE_ERROR: &str =
    "adding external service not allowed when using EXTSVC_CONFIG_FILE";

pub async fn prefetch_external_service_mutations(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, AdminMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "addExternalService" => {
                add_external_service(store, effects, field, actor, viewer_site_admin).await
            }
            "updateExternalService" => {
                update_external_service(store, effects, field, actor, viewer_site_admin).await
            }
            "deleteExternalService" => {
                delete_external_service(store, effects, field, actor, viewer_site_admin).await
            }
            "excludeRepoFromExternalServices" => {
                exclude_repo_from_external_services(store, field, actor, viewer_site_admin).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

fn input_object(field: &RootField) -> Option<&std::collections::BTreeMap<String, InputValue>> {
    match field.arguments.get("input") {
        Some(InputValue::Object(input)) => Some(input),
        _ => None,
    }
}

fn input_string<'a>(
    input: &'a std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<&'a str> {
    input.get(name).and_then(InputValue::as_str)
}

async fn add_external_service(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if !effects.external_services_writable() {
        return AdminMutationOutcome::Error(EXTSVC_CONFIG_FILE_ERROR.to_owned());
    }
    // `AddExternalService` maps *any* authorization failure (including the
    // anonymous actor's "not authenticated") to `auth.ErrMustBeSiteAdmin`
    // (`external_services.go:63-66`).
    if check_current_user_is_site_admin(actor, viewer_site_admin).is_err() {
        return AdminMutationOutcome::Error("must be site admin".to_owned());
    }
    let Some(input) = input_object(field) else {
        return AdminMutationOutcome::Error("missing input argument".to_owned());
    };
    let Some(kind) = input_string(input, "kind") else {
        return AdminMutationOutcome::Error("missing kind argument".to_owned());
    };
    let Some(display_name) = input_string(input, "displayName") else {
        return AdminMutationOutcome::Error("missing displayName argument".to_owned());
    };
    let Some(config) = input_string(input, "config") else {
        return AdminMutationOutcome::Error("missing config argument".to_owned());
    };
    let user_id = actor.uid().unwrap_or(0);

    let new = NewExternalService {
        kind: kind.to_owned(),
        display_name: display_name.to_owned(),
        config: config.to_owned(),
        namespace_user_id: None,
        namespace_org_id: None,
        creator_id: Some(user_id),
        last_updater_id: Some(user_id),
    };
    let id = match store.create_external_service(&new).await {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };

    if effects.auditlog_expansion() {
        let namespace = input
            .get("namespace")
            .and_then(InputValue::as_str)
            .map(|value| serde_json::Value::String(value.to_owned()))
            .unwrap_or(serde_json::Value::Null);
        let arguments = serde_json::json!({
            "Kind": kind,
            "DisplayName": display_name,
            "Namespace": namespace,
        });
        effects
            .audit("CodeHostConnectionAdded", user_id, arguments)
            .await;
    }

    // Not fatal: the scheduler retries the sync.
    let _ = effects.enqueue_external_service_sync(id).await;

    let warning = match effects.validate_external_service(kind, config).await {
        Ok(()) => None,
        Err(error) => Some(format!(
            "External service created, but we encountered a problem while validating the external service: {error}"
        )),
    };

    let row = match store.get_external_service(id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return AdminMutationOutcome::Error(format!("external service not found: {id}"))
        }
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    AdminMutationOutcome::ExternalService {
        row: std::sync::Arc::new(row),
        warning,
    }
}

async fn update_external_service(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if !effects.external_services_writable() {
        return AdminMutationOutcome::Error(EXTSVC_CONFIG_FILE_ERROR.to_owned());
    }
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let Some(input) = input_object(field) else {
        return AdminMutationOutcome::Error("missing input argument".to_owned());
    };
    let Some(raw_id) = input_string(input, "id") else {
        return AdminMutationOutcome::Error("missing id argument".to_owned());
    };
    let id = match unmarshal_external_service_id(raw_id) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    let existing = match store.get_external_service(id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return AdminMutationOutcome::Error(format!("external service not found: {id}"))
        }
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    let old_config = existing.config.clone();
    // Baseline uses the redacted config for the audit event and only warns
    // when redaction fails.
    let prev_config =
        crate::external_services::redact_config(&existing.kind, &existing.config)
            .unwrap_or_default();

    let new_config = input_string(input, "config");
    if let Some(config) = new_config {
        if config.trim().is_empty() {
            return AdminMutationOutcome::Error(
                "blank external service configuration is invalid (must be valid JSONC)".to_owned(),
            );
        }
    }
    let display_name = input_string(input, "displayName");
    let user_id = actor.uid().unwrap_or(0);

    if let Err(error) = store
        .update_external_service(id, display_name, new_config, Some(user_id))
        .await
    {
        return AdminMutationOutcome::Error(error);
    }

    let row = match store.get_external_service(id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return AdminMutationOutcome::Error(format!("external service not found: {id}"))
        }
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    let latest_config = crate::external_services::redact_config(&row.kind, &row.config)
        .unwrap_or_default();

    if effects.auditlog_expansion() {
        let arguments = serde_json::json!({
            "ID": raw_id,
            "DisplayName": display_name,
            "UpdaterID": user_id,
            "PrevConfig": prev_config,
            "LatestConfig": latest_config,
        });
        effects
            .audit("CodeHostConnectionUpdated", user_id, arguments)
            .await;
    }

    let _ = effects.enqueue_external_service_sync(id).await;

    let warning = if old_config != row.config {
        match effects
            .validate_external_service(&row.kind, &row.config)
            .await
        {
            Ok(()) => None,
            Err(error) => Some(format!(
                "External service updated, but we encountered a problem while validating the external service: {error}"
            )),
        }
    } else {
        None
    };

    AdminMutationOutcome::ExternalService {
        row: std::sync::Arc::new(row),
        warning,
    }
}

async fn delete_external_service(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if !effects.external_services_writable() {
        return AdminMutationOutcome::Error(EXTSVC_CONFIG_FILE_ERROR.to_owned());
    }
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let Some(raw_id) = field.arguments.string("externalService") else {
        return AdminMutationOutcome::Error("missing externalService argument".to_owned());
    };
    let id = match unmarshal_external_service_id(raw_id) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    match store.get_external_service(id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return AdminMutationOutcome::Error(format!("external service not found: {id}"))
        }
        Err(error) => return AdminMutationOutcome::Error(error),
    }
    let async_delete = field.arguments.bool("async").unwrap_or(false);
    if let Err(error) = store.delete_external_service(id, async_delete).await {
        return AdminMutationOutcome::Error(error);
    }

    if effects.auditlog_expansion() {
        let user_id = actor.uid().unwrap_or(0);
        let arguments = serde_json::json!({
            "GraphQL ID": raw_id,
            "External Service ID": id,
        });
        effects
            .audit("CodeHostConnectionDeleted", user_id, arguments)
            .await;
    }
    AdminMutationOutcome::Empty
}

async fn exclude_repo_from_external_services(
    store: &dyn AdminStore,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let raw_services: Vec<String> = match field.arguments.get("externalServices") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    };
    let mut ids = Vec::with_capacity(raw_services.len());
    for raw in &raw_services {
        match unmarshal_external_service_id(raw) {
            Ok(id) => ids.push(id),
            Err(error) => return AdminMutationOutcome::Error(error),
        }
    }
    let Some(raw_repo) = field.arguments.string("repo") else {
        return AdminMutationOutcome::Error("missing repo argument".to_owned());
    };
    let repo_id = match crate::external_services::unmarshal_repository_id(raw_repo) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    match store
        .exclude_repo_from_external_services(&ids, repo_id)
        .await
    {
        Ok(()) => AdminMutationOutcome::Empty,
        Err(error) => AdminMutationOutcome::Error(error),
    }
}
