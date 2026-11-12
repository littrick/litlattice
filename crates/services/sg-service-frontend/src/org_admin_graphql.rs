#![forbid(unsafe_code)]

//! `Mutation.createOrganization` / `updateOrganization` / `deleteOrganization`
//! / `addUserToOrganization` / `removeUserFromOrganization`
//! (`cmd/frontend/graphqlbackend/org.go:257-399`,
//! `site_admin.go:191-209`).
//!
//! Authorization matches `auth.CheckOrgAccessOrSiteAdmin`
//! (`internal/auth/orgs.go:20-63`): the internal actor passes, an anonymous
//! actor gets `not authenticated`, a site admin passes, everyone else must be a
//! member (`current user is not an org member`).

use std::sync::Arc;

use sg_graphql::exec::{Actor, RootField};

use crate::admin_store::{
    check_current_user_is_site_admin, check_name_allowed_for_user_or_organization,
    AdminEffects, AdminMutationOutcome, AdminStore, NOT_AN_ORG_MEMBER, NOT_AUTHENTICATED,
};
use crate::graphql_resolvers::{relay_kind, unmarshal_relay_id};

/// Run every organization-admin mutation root field in document order.
pub async fn prefetch_org_admin_mutations(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, AdminMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "createOrganization" => {
                create_organization(store, effects, field, actor, viewer_site_admin).await
            }
            "updateOrganization" => {
                update_organization(store, effects, field, actor, viewer_site_admin).await
            }
            "deleteOrganization" => {
                delete_organization(store, field, actor, viewer_site_admin).await
            }
            "addUserToOrganization" => {
                add_user_to_organization(store, effects, field, actor, viewer_site_admin).await
            }
            "removeUserFromOrganization" => {
                remove_user_from_organization(store, effects, field, actor, viewer_site_admin).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

/// `UnmarshalOrgID`: the Relay kind must be `Org`.
fn unmarshal_org_id(raw: &str) -> Result<i32, String> {
    let kind = relay_kind(raw).unwrap_or_default();
    if kind != "Org" {
        return Err(format!("invalid org id of kind {kind:?}"));
    }
    unmarshal_relay_id(raw)
}

/// `auth.CheckOrgAccessOrSiteAdmin` against the already-known viewer.
async fn check_org_access(
    store: &dyn AdminStore,
    actor: Actor,
    viewer_site_admin: bool,
    org_id: i32,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    let Some(viewer_id) = actor.uid() else {
        return Err(NOT_AUTHENTICATED.to_owned());
    };
    if viewer_site_admin {
        return Ok(());
    }
    match store.is_org_member(org_id, viewer_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(NOT_AN_ORG_MEMBER.to_owned()),
        Err(error) => Err(error),
    }
}

async fn create_organization(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    _viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let Some(creator_id) = actor.uid() else {
        return AdminMutationOutcome::Error("no current user".to_owned());
    };
    let Some(name) = field.arguments.string("name") else {
        return AdminMutationOutcome::Error("missing name argument".to_owned());
    };
    if let Err(message) = check_name_allowed_for_user_or_organization(effects.dotcom(), name) {
        return AdminMutationOutcome::Error(message);
    }
    let display_name = field.arguments.string("displayName");
    let org = match store.create_org(name, display_name).await {
        Ok(org) => org,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if effects.auditlog_expansion() {
        let arguments = serde_json::json!({"Name": name, "DisplayName": display_name});
        effects.audit("OrganizationCreated", creator_id, arguments).await;
    }
    if let Err(error) = store.add_org_member(org.id, creator_id).await {
        return AdminMutationOutcome::Error(error);
    }
    AdminMutationOutcome::Org {
        org: Arc::new(org),
        viewer_can_administer: true,
        viewer_is_member: true,
    }
}

async fn update_organization(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let Some(raw_id) = field.arguments.string("id") else {
        return AdminMutationOutcome::Error("missing id argument".to_owned());
    };
    // `UpdateOrganization` uses `relay.UnmarshalSpec` without a kind check.
    let org_id = match unmarshal_relay_id(raw_id) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_org_access(store, actor, viewer_site_admin, org_id).await {
        return AdminMutationOutcome::Error(message);
    }
    let display_name = field.arguments.string("displayName");
    let org = match store.update_org(org_id, display_name).await {
        Ok(org) => org,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if effects.auditlog_expansion() {
        if let Some(user_id) = actor.uid() {
            let arguments = serde_json::json!({"ID": raw_id, "DisplayName": display_name});
            effects.audit("OrganizationUpdated", user_id, arguments).await;
        }
    }
    let viewer_is_member = match actor.uid() {
        Some(viewer_id) => store.is_org_member(org_id, viewer_id).await.unwrap_or(false),
        None => false,
    };
    AdminMutationOutcome::Org {
        org: Arc::new(org),
        viewer_can_administer: viewer_site_admin || viewer_is_member,
        viewer_is_member,
    }
}

async fn delete_organization(
    store: &dyn AdminStore,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let Some(raw_id) = field.arguments.string("organization") else {
        return AdminMutationOutcome::Error("missing organization argument".to_owned());
    };
    let org_id = match unmarshal_org_id(raw_id) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    match store.delete_org(org_id).await {
        Ok(()) => AdminMutationOutcome::Empty,
        Err(error) => AdminMutationOutcome::Error(error),
    }
}

async fn add_user_to_organization(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let Some(raw_org) = field.arguments.string("organization") else {
        return AdminMutationOutcome::Error("missing organization argument".to_owned());
    };
    // `relay.UnmarshalSpec` without a kind check.
    let org_id = match unmarshal_relay_id(raw_org) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return AdminMutationOutcome::Error(message);
    }
    let Some(username) = field.arguments.string("username") else {
        return AdminMutationOutcome::Error("missing username argument".to_owned());
    };
    let user = match store.find_user_by_username(username).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return AdminMutationOutcome::Error(format!(
                "user not found: [{username}]"
            ))
        }
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(error) = store.add_org_member(org_id, user.id).await {
        return AdminMutationOutcome::Error(error);
    }
    let _ = effects
        .schedule_perms_sync(&[user.id], REASON_USER_ADDED_TO_ORG)
        .await;
    AdminMutationOutcome::Empty
}

async fn remove_user_from_organization(
    store: &dyn AdminStore,
    effects: &dyn AdminEffects,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> AdminMutationOutcome {
    let Some(raw_org) = field.arguments.string("organization") else {
        return AdminMutationOutcome::Error("missing organization argument".to_owned());
    };
    let org_id = match unmarshal_org_id(raw_org) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    let Some(raw_user) = field.arguments.string("user") else {
        return AdminMutationOutcome::Error("missing user argument".to_owned());
    };
    let user_id = match unmarshal_relay_id(raw_user) {
        Ok(id) => id,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    if let Err(message) = check_org_access(store, actor, viewer_site_admin, org_id).await {
        return AdminMutationOutcome::Error(message);
    }
    let member_count = match store.org_member_count(org_id).await {
        Ok(count) => count,
        Err(error) => return AdminMutationOutcome::Error(error),
    };
    // `siteAdminSelfRemoving`: site admin AND removing themselves.
    let self_removing = viewer_site_admin && actor.uid() == Some(user_id);
    if member_count == 1 && !self_removing {
        return AdminMutationOutcome::Error(
            "you can\u{2019}t remove the only member of an organization".to_owned(),
        );
    }
    if let Err(error) = store.remove_org_member(org_id, user_id).await {
        return AdminMutationOutcome::Error(error);
    }
    let _ = effects
        .schedule_perms_sync(&[user_id], REASON_USER_REMOVED_FROM_ORG)
        .await;
    // `RemoveUserFromOrganization` returns `nil, nil` (GraphQL `null`).
    AdminMutationOutcome::Null
}

/// The baseline reasons (`database.Reason*`).
pub const REASON_USER_ADDED_TO_ORG: &str = "REASON_USER_ADDED_TO_ORG";
pub const REASON_USER_REMOVED_FROM_ORG: &str = "REASON_USER_REMOVED_FROM_ORG";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn org_id_kind_is_validated() {
        let raw = crate::graphql_resolvers::OrgObject::relay_id(7);
        assert_eq!(unmarshal_org_id(&raw), Ok(7));
        let user = crate::graphql_resolvers::UserObject::relay_id(7);
        assert_eq!(
            unmarshal_org_id(&user),
            Err("invalid org id of kind \"User\"".to_owned())
        );
    }
}
