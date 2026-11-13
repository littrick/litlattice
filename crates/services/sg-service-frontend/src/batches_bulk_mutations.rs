#![forbid(unsafe_code)]

//! M9.27c: Batch Changes bulk-operation mutations, the
//! `availableBulkOperations` root query and the `BulkOperation` node.
//!
//! Ports `cmd/frontend/internal/batches/resolvers/resolver.go`
//! (`DetachChangesets`, `CreateChangesetComments`, `MergeChangesets`,
//! `CloseChangesets`, `PublishChangesets`, `AvailableBulkOperations`,
//! `bulkOperationByID`) and the `service.CreateChangesetJobs` /
//! `service.GetAvailableBulkOperations` semantics in
//! `internal/batches/service/service.go`. `reenqueueChangesets` stays in
//! `batches_mutations.rs` and is reused unchanged.
//!
//! Only existing tables/columns are written; no new storage.

use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue};

use crate::batches_changeset_read::BulkOperationObject;
use crate::batches_graphql::{
    unmarshal_relay_int, unmarshal_relay_string, BatchesHandle, BulkOperationRow,
};
use crate::batches_mutations::{batch_changes_enabled_for_user, ID_IS_ZERO};

/// `bulkOperationIDKind` (`bulk_operation.go:22`).
pub const BULK_OPERATION_KIND: &str = "BulkOperation";

/// `resolvers.ErrIDIsZero`; reused for empty relay payloads.
pub const NO_CHANGESETS_PROVIDED: &str = "no changesets provided";
/// `unmarshalBulkOperationBaseArgs`.
pub const SPECIFY_AT_LEAST_ONE_CHANGESET: &str = "specify at least one changeset";
/// `Resolver.CreateChangesetComments`.
pub const EMPTY_COMMENT_BODY: &str = "empty comment body is not allowed";

/// `service.CreateChangesetJobs` request as the resolvers build it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateChangesetJobsRequest {
    pub batch_change_id: i64,
    pub changeset_ids: Vec<i64>,
    /// Database `changeset_jobs.job_type` (`detach`, `commentatore`,
    /// `merge`, `close`, `publish`).
    pub job_type: &'static str,
    /// JSON payload text (`jsonbColumn` in the Go store).
    pub payload: String,
    pub only_archived: bool,
    pub include_archived: bool,
    pub publication_state: Option<&'static str>,
    pub reconciler_states: Vec<&'static str>,
    pub external_states: Vec<&'static str>,
}

/// Mutation root fields owned by this module.
pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "detachChangesets"
            | "createChangesetComments"
            | "mergeChangesets"
            | "closeChangesets"
            | "publishChangesets"
    )
}

/// Resolve one bulk-operation mutation.
pub fn mutation_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "detachChangesets" => detach_changesets(db, arguments, actor),
        "createChangesetComments" => create_changeset_comments(db, arguments, actor),
        "mergeChangesets" => merge_changesets(db, arguments, actor),
        "closeChangesets" => close_changesets(db, arguments, actor),
        "publishChangesets" => publish_changesets(db, arguments, actor),
        _ => Ok(None),
    }
}

/// `unmarshalBulkOperationBaseArgs` (`resolver.go:2099-2127`).
fn bulk_operation_base_args(arguments: &Arguments) -> Result<(i64, Vec<i64>), String> {
    let raw = arguments
        .string("batchChange")
        .ok_or_else(|| "invalid graphql.ID".to_owned())?;
    let batch_change_id = unmarshal_relay_int(raw)?;
    if batch_change_id == 0 {
        return Err(ID_IS_ZERO.to_owned());
    }

    let items: &[InputValue] = match arguments.get("changesets") {
        Some(InputValue::List(items)) => items,
        _ => &[],
    };
    let mut changeset_ids = Vec::with_capacity(items.len());
    for item in items {
        let Some(raw) = item.as_str() else {
            return Err("invalid graphql.ID".to_owned());
        };
        let id = unmarshal_relay_int(raw)?;
        if id == 0 {
            return Err(ID_IS_ZERO.to_owned());
        }
        changeset_ids.push(id);
    }

    if changeset_ids.is_empty() {
        return Err(SPECIFY_AT_LEAST_ONE_CHANGESET.to_owned());
    }
    Ok((batch_change_id, changeset_ids))
}

fn bulk_operation_value(
    row: BulkOperationRow,
    db: &BatchesHandle,
) -> FieldValue {
    FieldValue::Object(Arc::new(BulkOperationObject::from_row(db.clone(), row)))
}

/// `service.CreateChangesetJobs` + `bulkOperationByIDString`.
fn enqueue(
    db: &BatchesHandle,
    request: CreateChangesetJobsRequest,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let row = db
        .create_changeset_jobs(&request, actor)
        .map_err(ExecError::new)?;
    Ok(Some(match row {
        Some(row) => bulk_operation_value(row, db),
        // `bulkOperationByIDString` returns `nil, nil` for `ErrNoResults`.
        None => FieldValue::Null,
    }))
}

fn detach_changesets(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let (batch_change_id, changeset_ids) =
        bulk_operation_base_args(arguments).map_err(ExecError::new)?;
    enqueue(
        db,
        CreateChangesetJobsRequest {
            batch_change_id,
            changeset_ids,
            job_type: "detach",
            payload: "{}".to_owned(),
            only_archived: true,
            include_archived: false,
            publication_state: None,
            reconciler_states: Vec::new(),
            external_states: Vec::new(),
        },
        actor,
    )
}

fn create_changeset_comments(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    if arguments.string("body").unwrap_or_default().is_empty() {
        return Err(ExecError::new(EMPTY_COMMENT_BODY));
    }
    let (batch_change_id, changeset_ids) =
        bulk_operation_base_args(arguments).map_err(ExecError::new)?;
    let payload = serde_json::json!({ "message": arguments.string("body").unwrap_or_default() })
        .to_string();
    enqueue(
        db,
        CreateChangesetJobsRequest {
            batch_change_id,
            changeset_ids,
            job_type: "commentatore",
            payload,
            only_archived: false,
            include_archived: true,
            publication_state: Some("PUBLISHED"),
            reconciler_states: Vec::new(),
            external_states: Vec::new(),
        },
        actor,
    )
}

fn merge_changesets(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let (batch_change_id, changeset_ids) =
        bulk_operation_base_args(arguments).map_err(ExecError::new)?;
    let squash = arguments.bool("squash").unwrap_or(false);
    // `ChangesetJobMergePayload` has `squash,omitempty`.
    let payload = if squash {
        "{\"squash\":true}".to_owned()
    } else {
        "{}".to_owned()
    };
    enqueue(
        db,
        CreateChangesetJobsRequest {
            batch_change_id,
            changeset_ids,
            job_type: "merge",
            payload,
            only_archived: false,
            include_archived: false,
            publication_state: Some("PUBLISHED"),
            reconciler_states: vec!["completed"],
            external_states: vec!["OPEN"],
        },
        actor,
    )
}

fn close_changesets(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let (batch_change_id, changeset_ids) =
        bulk_operation_base_args(arguments).map_err(ExecError::new)?;
    enqueue(
        db,
        CreateChangesetJobsRequest {
            batch_change_id,
            changeset_ids,
            job_type: "close",
            payload: "{}".to_owned(),
            only_archived: false,
            include_archived: false,
            publication_state: Some("PUBLISHED"),
            reconciler_states: vec!["completed"],
            external_states: vec!["OPEN", "DRAFT"],
        },
        actor,
    )
}

fn publish_changesets(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    db.current_user_has_batch_changes_write(actor)
        .map_err(ExecError::new)?;
    let (batch_change_id, changeset_ids) =
        bulk_operation_base_args(arguments).map_err(ExecError::new)?;
    let draft = arguments.bool("draft").unwrap_or(false);
    // `ChangesetJobPublishPayload{Draft}` has no `omitempty`.
    let payload = format!("{{\"draft\":{draft}}}");
    enqueue(
        db,
        CreateChangesetJobsRequest {
            batch_change_id,
            changeset_ids,
            job_type: "publish",
            payload,
            only_archived: false,
            include_archived: false,
            publication_state: None,
            reconciler_states: Vec::new(),
            external_states: Vec::new(),
        },
        actor,
    )
}

// ---------------------------------------------------------------------------
// Query.availableBulkOperations
// ---------------------------------------------------------------------------

/// Whether `field` is a root query owned by this module.
pub fn is_root_field(field: &str) -> bool {
    field == "availableBulkOperations"
}

/// `Query.availableBulkOperations` (`resolver.go:1903-1932`).
pub fn root_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if field != "availableBulkOperations" {
        return Ok(None);
    }
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;

    let empty = match arguments.get("changesets") {
        Some(InputValue::List(items)) => items.is_empty(),
        _ => true,
    };
    if empty {
        return Err(ExecError::new(NO_CHANGESETS_PROVIDED));
    }

    let (batch_change_id, changeset_ids) =
        bulk_operation_base_args(arguments).map_err(ExecError::new)?;
    let operations = db
        .available_bulk_operations(batch_change_id, &changeset_ids, actor)
        .map_err(ExecError::new)?;
    Ok(Some(FieldValue::List {
        items: operations
            .into_iter()
            .map(|operation| FieldValue::Json(sg_graphql::json::Json::Str(operation)))
            .collect(),
        element_type: Some("BulkOperationType"),
    }))
}

// ---------------------------------------------------------------------------
// Query.node kind `BulkOperation`
// ---------------------------------------------------------------------------

/// Whether `kind` is the `BulkOperation` node kind.
pub fn is_node_kind(kind: &str) -> bool {
    kind == BULK_OPERATION_KIND
}

/// `Resolver.bulkOperationByID` (`resolver.go:361-390`).
pub fn node_field(
    db: &BatchesHandle,
    kind: &str,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if kind != BULK_OPERATION_KIND {
        return Ok(None);
    }
    if let Err(error) = batch_changes_enabled_for_user(db, actor) {
        return Err(ExecError::new(error));
    }
    let bulk_group = unmarshal_relay_string(id).map_err(ExecError::new)?;
    if bulk_group.is_empty() {
        return Err(ExecError::new(ID_IS_ZERO));
    }
    match db.get_bulk_operation(&bulk_group).map_err(ExecError::new)? {
        Some(row) => Ok(Some(bulk_operation_value(row, db))),
        // `bulkOperationByIDString` returns `nil, nil`; `node` is nullable.
        None => Ok(Some(FieldValue::Null)),
    }
}
