#![forbid(unsafe_code)]

//! Telemetry V2 GraphQL surface (`telemetry.graphql`,
//! `cmd/frontend/internal/telemetry/init.go`).
//!
//! The 5.5 web client exports every recorded interaction through
//! `mutation ExportTelemetryEvents { telemetry { recordEvents(events: ...) } }`.
//! Without the SDL document the mutation fails with
//! `Cannot query field "telemetry" on type "Mutation"` and Apollo surfaces an
//! uncaught error.
//!
//! ENV_GAP: the events are accepted and dropped. The baseline forwards them to
//! the telemetry exporter (Redis/export queue); this image does not ship
//! telemetry export. `Query.telemetry.exportedEvents` is an empty connection.

use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;

use crate::graphql_resolvers::ConnectionPageInfoObject;

/// `TelemetryQuery` (the `Query.telemetry` object).
pub struct TelemetryQueryObject;

impl ObjectValue for TelemetryQueryObject {
    fn type_name(&self) -> &'static str {
        "TelemetryQuery"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "exportedEvents" => Ok(Some(FieldValue::Object(Arc::new(
                ExportedEventsConnectionObject,
            )))),
            _ => Ok(None),
        }
    }
}

/// `ExportedEventsConnection`: no telemetry export queue is ported.
pub struct ExportedEventsConnectionObject;

impl ObjectValue for ExportedEventsConnectionObject {
    fn type_name(&self) -> &'static str {
        "ExportedEventsConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("ExportedEvent"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(0)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                ConnectionPageInfoObject::new(None, None, false, false),
            )))),
            _ => Ok(None),
        }
    }
}

/// `TelemetryMutation` (the `Mutation.telemetry` object).
pub struct TelemetryMutationObject;

impl ObjectValue for TelemetryMutationObject {
    fn type_name(&self) -> &'static str {
        "TelemetryMutation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `recordEvents(events: [TelemetryEventInput!]!): EmptyResponse`.
            // The schema enforces the non-null list; the events are dropped
            // (ENV_GAP: no telemetry exporter).
            "recordEvents" => Ok(Some(FieldValue::Object(Arc::new(
                crate::temporary_settings::EmptyResponseObject,
            )))),
            _ => Ok(None),
        }
    }
}
