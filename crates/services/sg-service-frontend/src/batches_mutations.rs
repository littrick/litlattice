#![forbid(unsafe_code)]

//! Batch Changes GraphQL **write** surface (M9.25b).
//!
//! Ports the mutation side of
//! `cmd/frontend/internal/batches/resolvers/resolver.go` for
//! `createBatchChange`, `applyBatchChange`, `createEmptyBatchChange`,
//! `moveBatchChange`, `closeBatchChange`, `deleteBatchChange` and
//! `reenqueueChangesets`, plus the argument parsing, enterprise gates, RBAC
//! check, publication-state preparation and `BulkOperation` result object.
//!
//! Side effects run directly in the synchronous `MutationRoot::field`; the
//! production store bridges its `sqlx` transaction with the same
//! `block_in_place` helper as the read surface (M9.24c), so no new prefetch
//! plumbing is needed.

use std::collections::HashSet;
use std::sync::Arc;

use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue,
};
use sg_graphql::json::Json;

use crate::batches_graphql::{
    marshal_bulk_operation_id, unmarshal_namespace, unmarshal_relay_int,
    unmarshal_relay_string, ApplyBatchChangeRequest, BatchChangeObject, BatchesHandle,
    PublishedValue, BATCH_CHANGES_DISABLED,
};
use crate::graphql_resolvers::{datetime_rfc3339, UserObject};

/// `enterprise.ErrBatchChangesDisabledForUser.Error()`
/// (`cmd/frontend/enterprise/enterprise.go:151-155`).
pub const BATCH_CHANGES_DISABLED_FOR_USER: &str = "batch changes are disabled for non-site-admin \
     users. Ask a site admin to unset 'batchChanges.restrictToAdmins' in the site configuration to \
     enable the feature for all users.";

/// `resolvers.ErrIDIsZero` (`errors.go:17-23`).
pub const ID_IS_ZERO: &str = "invalid node id";
/// `auth.ErrNotAuthenticated` / `rbac.CheckCurrentUserHasPermission`.
pub const NOT_AUTHENTICATED: &str = "not authenticated";
/// `auth.ErrMustBeSiteAdminOrSameUser` (`site_admin.go:15`).
pub const MUST_BE_SITE_ADMIN_OR_SAME_USER: &str =
    "must be authenticated as the authorized user or site admin";
/// `auth.ErrNotAnOrgMember` (`auth/orgs.go:43`).
pub const NOT_AN_ORG_MEMBER: &str = "current user is not an org member";
/// `service.ErrNoNamespace` (`service.go:1244`).
pub const NO_NAMESPACE: &str = "no namespace given";
/// `rbac.ErrNotAuthorized.Error()`.
pub const MISSING_BATCH_CHANGES_WRITE: &str = "user is missing permission BATCH_CHANGES#WRITE";
/// `store.ErrNoResults`.
pub const NO_RESULTS: &str = "no results";
/// `store.ErrDeletedNamespace`.
pub const DELETED_NAMESPACE: &str = "namespace has been deleted";
/// `service.ErrNameNotUnique` (`service.go:229`).
pub const NAME_NOT_UNIQUE: &str = "a batch change with this name already exists in this namespace";
/// `resolvers.ErrBatchChangeInvalidName`.
pub const BATCH_CHANGE_INVALID_NAME: &str =
    "The batch change name can only contain word characters, dots and dashes.";
/// `lib/batches.ParseBatchSpec`'s friendlier name-pattern message.
pub const NAME_PATTERN_MESSAGE: &str = "The batch change name can only contain word characters, \
     dots and dashes. No whitespace or newlines allowed.";
/// `store.ErrInvalidBatchChangeName` (`batch_changes.go:25`).
pub const INVALID_BATCH_CHANGE_NAME: &str = "batch change name violates name policy";
/// `service.ErrEnsureBatchChangeFailed`.
pub const ENSURE_BATCH_CHANGE_FAILED: &str = "a batch change in the given namespace and with the \
     given name exists but does not match the given ID";
/// `service.ErrApplyClosedBatchChange`.
pub const APPLY_CLOSED_BATCH_CHANGE: &str = "existing batch change matched by batch spec is closed";
/// `service.ErrMatchingBatchChangeExists`.
pub const MATCHING_BATCH_CHANGE_EXISTS: &str =
    "a batch change matching the given batch spec already exists";
/// `service.ErrChangesetsForJobNotFound`.
pub const CHANGESETS_FOR_JOB_NOT_FOUND: &str = "some changesets could not be found";

/// The mutation root fields this module resolves.
pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "createBatchChange"
            | "applyBatchChange"
            | "createEmptyBatchChange"
            | "moveBatchChange"
            | "closeBatchChange"
            | "deleteBatchChange"
            | "reenqueueChangesets"
    )
}

/// Resolve one Batch Changes mutation root field.
pub fn mutation_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "createBatchChange" => create_batch_change(db, arguments, actor),
        "applyBatchChange" => apply_batch_change(db, arguments, actor),
        "createEmptyBatchChange" => create_empty_batch_change(db, arguments, actor),
        "moveBatchChange" => move_batch_change(db, arguments, actor),
        "closeBatchChange" => close_batch_change(db, arguments, actor),
        "deleteBatchChange" => delete_batch_change(db, arguments, actor),
        "reenqueueChangesets" => reenqueue_changesets(db, arguments, actor),
        _ => Ok(None),
    }
}

/// `enterprise.BatchChangesEnabledForUser`.
fn batch_changes_enabled_for_user(
    db: &BatchesHandle,
    actor: Actor,
) -> Result<(), String> {
    if !db.batch_changes_enabled() {
        return Err(BATCH_CHANGES_DISABLED.to_owned());
    }
    if db.batch_changes_restricted_to_admins() {
        let site_admin = db
            .current_user_site_admin(actor)?
            .unwrap_or(false);
        if !site_admin {
            return Err(BATCH_CHANGES_DISABLED_FOR_USER.to_owned());
        }
    }
    Ok(())
}

fn batch_change_value(
    row: crate::batches_graphql::BatchChangeRow,
    db: &BatchesHandle,
) -> Option<FieldValue> {
    Some(FieldValue::Object(Arc::new(BatchChangeObject::new(
        row,
        db.clone(),
    ))))
}

/// `relay.UnmarshalSpec` into the integer batch change ID.
fn batch_change_id(arguments: &Arguments, name: &str) -> Result<i64, String> {
    let raw = arguments
        .string(name)
        .ok_or_else(|| "invalid graphql.ID".to_owned())?;
    unmarshal_relay_int(raw)
}

// ---------------------------------------------------------------------------
// createBatchChange / applyBatchChange
// ---------------------------------------------------------------------------

fn create_batch_change(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    // `Resolver.CreateBatchChange` checks RBAC before `applyOrCreate` runs the
    // enterprise gate.
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let row = apply_or_create(db, arguments, actor, true).map_err(ExecError::new)?;
    Ok(batch_change_value(row, db))
}

fn apply_batch_change(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let row = apply_or_create(db, arguments, actor, false).map_err(ExecError::new)?;
    Ok(batch_change_value(row, db))
}

/// `Resolver.applyOrCreateBatchChange`.
fn apply_or_create(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
    fail_if_batch_change_exists: bool,
) -> Result<crate::batches_graphql::BatchChangeRow, String> {
    batch_changes_enabled_for_user(db, actor)?;

    let raw_spec = arguments
        .string("batchSpec")
        .ok_or_else(|| "invalid graphql.ID".to_owned())?;
    let batch_spec_rand_id = unmarshal_relay_string(raw_spec)?;
    if batch_spec_rand_id.is_empty() {
        return Err(ID_IS_ZERO.to_owned());
    }

    // The Go baseline also enforces the `FeatureBatchChanges` license here.
    // Rust has no license subsystem in this slice; the check is omitted so the
    // mutation behaves like an unrestricted license (documented in the M9.25b
    // overview).

    let ensure_batch_change_id = match arguments.string("ensureBatchChange") {
        Some(raw) => unmarshal_relay_int(raw)?,
        None => 0,
    };

    let publication_states = parse_publication_states(arguments.get("publicationStates"))?;
    let request = ApplyBatchChangeRequest {
        batch_spec_rand_id,
        ensure_batch_change_id,
        fail_if_batch_change_exists,
        publication_states,
    };
    db.apply_batch_change(&request, actor)
}

/// `Resolver.addPublicationStatesToOptions`.
fn parse_publication_states(
    input: Option<&InputValue>,
) -> Result<Vec<(String, PublishedValue)>, String> {
    let items = match input {
        None | Some(InputValue::Null) => return Ok(Vec::new()),
        Some(InputValue::List(items)) => items,
        Some(_) => return Ok(Vec::new()),
    };
    let mut states = Vec::with_capacity(items.len());
    let mut seen = HashSet::new();
    let mut errors = Vec::new();
    for item in items {
        let InputValue::Object(entry) = item else {
            return Err("invalid graphql.ID".to_owned());
        };
        let Some(InputValue::Str(raw_spec)) = entry.get("changesetSpec") else {
            return Err("invalid graphql.ID".to_owned());
        };
        let rand_id = unmarshal_relay_string(raw_spec)?;
        let value = parse_published_value(entry.get("publicationState"))?;
        if !seen.insert(rand_id.clone()) {
            errors.push(format!("duplicate changeset spec: {rand_id}"));
            continue;
        }
        states.push((rand_id, value));
    }
    if !errors.is_empty() {
        return Err(join_multi_errors(&errors));
    }
    Ok(states)
}

/// `batches.PublishedValue.UnmarshalGraphQL`.
fn parse_published_value(value: Option<&InputValue>) -> Result<PublishedValue, String> {
    match value {
        None | Some(InputValue::Null) => Ok(PublishedValue::Null),
        Some(InputValue::Bool(value)) => Ok(PublishedValue::Bool(*value)),
        Some(InputValue::Str(value)) if value == "draft" => Ok(PublishedValue::Draft),
        Some(other) => Err(format!(
            "invalid PublishedValue: {}",
            input_value_display(other)
        )),
    }
}

fn input_value_display(value: &InputValue) -> String {
    match value {
        InputValue::Null => "null".to_owned(),
        InputValue::Bool(value) => value.to_string(),
        InputValue::Int(value) => value.to_string(),
        InputValue::Float(value) => value.to_string(),
        InputValue::Str(value) | InputValue::Enum(value) => value.clone(),
        InputValue::List(_) => "[...]".to_owned(),
        InputValue::Object(_) => "map[...]".to_owned(),
    }
}

/// `errors.MultiError` formatting (`lib/errors/cockroach.go:136-143`).
pub fn join_multi_errors(errors: &[String]) -> String {
    if errors.len() == 1 {
        return errors[0].clone();
    }
    format!(
        "{} errors occurred:\n\t* {}",
        errors.len(),
        errors.join("\n\t* ")
    )
}

// ---------------------------------------------------------------------------
// createEmptyBatchChange
// ---------------------------------------------------------------------------

fn create_empty_batch_change(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let raw_namespace = arguments
        .string("namespace")
        .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
    let namespace =
        unmarshal_namespace(raw_namespace).map_err(ExecError::new)?;
    let name = arguments.string("name").unwrap_or_default().to_owned();
    let row = db
        .create_empty_batch_change(namespace, &name, actor)
        .map_err(ExecError::new)?;
    Ok(batch_change_value(row, db))
}

// ---------------------------------------------------------------------------
// moveBatchChange
// ---------------------------------------------------------------------------

fn move_batch_change(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let batch_change_id = batch_change_id(arguments, "batchChange").map_err(ExecError::new)?;
    if batch_change_id == 0 {
        return Err(ExecError::new(ID_IS_ZERO));
    }
    let new_name = arguments.string("newName").map(str::to_owned);
    let new_namespace = match arguments.string("newNamespace") {
        Some(raw) => Some(unmarshal_namespace(raw).map_err(ExecError::new)?),
        None => None,
    };
    let row = db
        .move_batch_change(
            batch_change_id,
            new_name.as_deref(),
            new_namespace,
            actor,
        )
        .map_err(ExecError::new)?;
    Ok(batch_change_value(row, db))
}

// ---------------------------------------------------------------------------
// closeBatchChange / deleteBatchChange
// ---------------------------------------------------------------------------

fn close_batch_change(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let batch_change_id = batch_change_id(arguments, "batchChange").map_err(|error| {
        ExecError::new(format!("unmarshaling batch change id: {error}"))
    })?;
    if batch_change_id == 0 {
        return Err(ExecError::new(ID_IS_ZERO));
    }
    let close_changesets = arguments.bool("closeChangesets").unwrap_or(false);
    let row = db
        .close_batch_change(batch_change_id, close_changesets, actor)
        .map_err(|error| ExecError::new(format!("closing batch change: {error}")))?;
    Ok(batch_change_value(row, db))
}

fn delete_batch_change(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let batch_change_id = batch_change_id(arguments, "batchChange").map_err(ExecError::new)?;
    if batch_change_id == 0 {
        return Err(ExecError::new(ID_IS_ZERO));
    }
    db.delete_batch_change(batch_change_id, actor)
        .map_err(ExecError::new)?;
    Ok(Some(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    ))))
}

// ---------------------------------------------------------------------------
// reenqueueChangesets
// ---------------------------------------------------------------------------

fn reenqueue_changesets(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let batch_change_id = batch_change_id(arguments, "batchChange").map_err(ExecError::new)?;
    if batch_change_id == 0 {
        return Err(ExecError::new(ID_IS_ZERO));
    }
    let Some(InputValue::List(items)) = arguments.get("changesets") else {
        return Err(ExecError::new("specify at least one changeset"));
    };
    let mut changeset_ids = Vec::with_capacity(items.len());
    for item in items {
        let Some(raw) = item.as_str() else {
            return Err(ExecError::new("invalid graphql.ID"));
        };
        let id = unmarshal_relay_int(raw).map_err(ExecError::new)?;
        if id == 0 {
            return Err(ExecError::new(ID_IS_ZERO));
        }
        changeset_ids.push(id);
    }
    if changeset_ids.is_empty() {
        return Err(ExecError::new("specify at least one changeset"));
    }
    let row = db
        .reenqueue_changesets(batch_change_id, &changeset_ids, actor)
        .map_err(ExecError::new)?;
    Ok(Some(match row {
        Some(row) => FieldValue::Object(Arc::new(BulkOperationObject {
            row,
            db: db.clone(),
        })),
        // `bulkOperationByIDString` returns `nil, nil` for `ErrNoResults`; the
        // SDL type is non-null, so the executor reports the nil.
        None => FieldValue::Null,
    }))
}

// ---------------------------------------------------------------------------
// BulkOperation
// ---------------------------------------------------------------------------

pub struct BulkOperationObject {
    pub row: crate::batches_graphql::BulkOperationRow,
    pub db: BatchesHandle,
}

impl BulkOperationObject {
    /// `changesetJobTypeToBulkOperationType`.
    fn operation_type(job_type: &str) -> Result<&'static str, String> {
        Ok(match job_type {
            "comment" => "COMMENT",
            "detach" => "DETACH",
            "reenqueue" => "REENQUEUE",
            "merge" => "MERGE",
            "export" => "EXPORT",
            "close" => "CLOSE",
            "publish" => "PUBLISH",
            other => return Err(format!("invalid job type {other:?}")),
        })
    }
}

impl ObjectValue for BulkOperationObject {
    fn type_name(&self) -> &'static str {
        "BulkOperation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_bulk_operation_id(&self.row.id),
            )))),
            "type" => Ok(Some(FieldValue::Json(Json::Str(
                Self::operation_type(&self.row.job_type)
                    .map_err(ExecError::new)?
                    .to_owned(),
            )))),
            "state" => Ok(Some(FieldValue::Json(Json::Str(self.row.state.clone())))),
            "progress" => Ok(Some(FieldValue::Json(Json::Float(self.row.progress)))),
            // `Resolver.Errors`: the freshly enqueued jobs have no failure
            // message, so the list is empty (the `REENQUEUE` flow is the only
            // one this slice starts).
            "errors" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("ChangesetJobError"),
            })),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.row.created_at,
            ))))),
            "finishedAt" => Ok(Some(match self.row.finished_at {
                Some(value) => FieldValue::Json(Json::Str(datetime_rfc3339(value))),
                None => FieldValue::Null,
            })),
            "initiator" => {
                let user = self
                    .db
                    .user_by_id(self.row.user_id)
                    .map_err(ExecError::new)?
                    .ok_or_else(|| {
                        ExecError::new(format!("user not found: id={}", self.row.user_id))
                    })?;
                Ok(Some(FieldValue::Object(Arc::new(
                    UserObject::from_data_as_viewer(user, false),
                ))))
            }
            "changesetCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.row.changeset_count,
            )))),
            _ => Ok(None),
        }
    }
}
