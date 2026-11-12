#![forbid(unsafe_code)]

//! M9.12b: the frontend admin/account GraphQL write surface.
//!
//! [`AdminMutationDeps`] bundles the [`AdminStore`] and [`AdminEffects`] seams
//! and [`prefetch_admin_mutations`] runs every selected write before the
//! synchronous executor replays the owned [`AdminMutationOutcome`] from
//! `MutationRoot`.
//!
//! Covered root fields:
//!
//! * `createUser`, `updateUser`, `deleteUser`, `deleteUsers`
//!   ([`crate::user_admin_graphql`])
//! * `createOrganization`, `updateOrganization`, `deleteOrganization`,
//!   `addUserToOrganization`, `removeUserFromOrganization`
//!   ([`crate::org_admin_graphql`])
//! * `createAccessToken`, `deleteAccessToken`
//!   ([`crate::access_tokens_graphql`])
//! * `addUserEmail`, `removeUserEmail`, `setUserEmailPrimary`,
//!   `setUserEmailVerified`, `resendVerificationEmail`
//!   ([`crate::user_emails_graphql`])
//! * `addExternalService`, `updateExternalService`, `deleteExternalService`,
//!   `excludeRepoFromExternalServices`
//!   ([`crate::external_service_mutations`])
//! * `recloneRepository`, `deleteRepositoryFromDisk`, `reindexRepository`
//!   ([`crate::repository_mutations`])

use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;

use crate::access_tokens_graphql::marshal_access_token_id;
use crate::admin_store::{
    AdminEffects, AdminMutationOutcome, AdminStore, CreatedAccessToken, UserRow,
};

/// The seams every admin mutation needs.
#[derive(Clone)]
pub struct AdminMutationDeps {
    pub store: Arc<dyn AdminStore>,
    pub effects: Arc<dyn AdminEffects>,
    /// `ExternalService.webhookURL` needs the instance external URL.
    pub external_url: String,
}

impl AdminMutationDeps {
    pub fn new(store: Arc<dyn AdminStore>, effects: Arc<dyn AdminEffects>) -> Self {
        Self {
            store,
            effects,
            external_url: "http://localhost:3080".to_owned(),
        }
    }

    pub fn with_external_url(mut self, external_url: impl Into<String>) -> Self {
        self.external_url = external_url.into();
        self
    }
}

/// Run the selected admin mutations, grouped by resolver family. Each
/// outcome is replayed from `MutationRoot` by root-field name; the differential
/// corpus issues one write root per document (as the web UI does).
pub async fn prefetch_admin_mutations(
    deps: &AdminMutationDeps,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, AdminMutationOutcome)> {
    let mut outcomes = Vec::new();
    outcomes.extend(
        crate::user_admin_graphql::prefetch_user_admin_mutations(
            deps.store.as_ref(),
            deps.effects.as_ref(),
            fields,
            actor,
            viewer_site_admin,
        )
        .await,
    );
    outcomes.extend(
        crate::org_admin_graphql::prefetch_org_admin_mutations(
            deps.store.as_ref(),
            deps.effects.as_ref(),
            fields,
            actor,
            viewer_site_admin,
        )
        .await,
    );
    outcomes.extend(
        crate::access_tokens_graphql::prefetch_access_token_mutations(
            deps.store.as_ref(),
            deps.effects.as_ref(),
            fields,
            actor,
            viewer_site_admin,
        )
        .await,
    );
    outcomes.extend(
        crate::user_emails_graphql::prefetch_user_email_mutations(
            deps.store.as_ref(),
            deps.effects.as_ref(),
            fields,
            actor,
            viewer_site_admin,
        )
        .await,
    );
    outcomes.extend(
        crate::external_service_mutations::prefetch_external_service_mutations(
            deps.store.as_ref(),
            deps.effects.as_ref(),
            fields,
            actor,
            viewer_site_admin,
        )
        .await,
    );
    outcomes.extend(
        crate::repository_mutations::prefetch_repository_mutations(
            deps.store.as_ref(),
            fields,
            actor,
            viewer_site_admin,
        )
        .await,
    );
    outcomes
}

/// `CreateUserResult` (`schema.graphql:1290-1305`).
pub struct CreateUserResultObject {
    pub user: Arc<UserRow>,
    pub reset_password_url: Option<String>,
    pub viewer_site_admin: bool,
}

impl ObjectValue for CreateUserResultObject {
    fn type_name(&self) -> &'static str {
        "CreateUserResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "user" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::UserObject::from_data_as_viewer(
                    self.user.user_data(),
                    self.viewer_site_admin,
                ),
            )))),
            "resetPasswordURL" => Ok(Some(match &self.reset_password_url {
                Some(url) => FieldValue::Json(Json::Str(url.clone())),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

/// `CreateAccessTokenResult` (`schema.graphql:1265-1275`).
pub struct CreateAccessTokenResultObject {
    pub created: CreatedAccessToken,
}

impl ObjectValue for CreateAccessTokenResultObject {
    fn type_name(&self) -> &'static str {
        "CreateAccessTokenResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_access_token_id(
                self.created.id,
            ))))),
            "token" => Ok(Some(FieldValue::Json(Json::Str(
                self.created.token.clone(),
            )))),
            _ => Ok(None),
        }
    }
}
