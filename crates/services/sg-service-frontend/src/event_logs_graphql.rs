#![forbid(unsafe_code)]

//! `User.eventLogs` (`cmd/frontend/graphqlbackend/event_logs.go:16-70`).
//!
//! The connection is produced lazily by the Go resolver, but the executor here
//! is synchronous, so [`prefetch_user_event_logs`] resolves the selected
//! `eventLogs(first, eventName)` connections once per request (for
//! `Query.currentUser` and `Query.node(id: <User>)`) and the `User` resolver
//! replays them.

use std::collections::HashMap;
use std::sync::Arc;

use sg_db::event_logs::{count_by_user, list_by_user, UserEventLogRow};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::postgres::PgPool;

use crate::graphql_resolvers::{
    datetime_rfc3339, maybe_query_is_id, PageInfoObject, UserData, UserObject,
};

/// `eventLogsArgs` (`event_logs.go:11-14`): `ConnectionArgs.first` + `eventName`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventLogsArgs {
    pub first: Option<i32>,
    pub event_name: Option<String>,
}

impl EventLogsArgs {
    pub fn from_arguments(arguments: &Arguments) -> Self {
        Self {
            first: arguments.i64("first").map(|value| value as i32),
            event_name: arguments.string("eventName").map(str::to_owned),
        }
    }
}

/// One prefetched `User.eventLogs` value.
#[derive(Debug, Clone, PartialEq)]
pub enum UserEventLogsLookup {
    Found {
        events: Vec<UserEventLogRow>,
        total: i64,
    },
    Error(String),
}

/// Collect the `eventLogs` selections that belong to `user_id`.
fn collect(
    selections: &[RootField],
    user_id: i32,
    out: &mut Vec<(i32, EventLogsArgs)>,
) {
    for selection in selections {
        if selection.name == "eventLogs" {
            out.push((user_id, EventLogsArgs::from_arguments(&selection.arguments)));
        }
        collect(&selection.selections, user_id, out);
        for (_, condition) in &selection.type_conditions {
            collect(condition, user_id, out);
        }
    }
}

/// Prefetch every `User.eventLogs` connection selected by this operation.
pub async fn prefetch_user_event_logs(
    pool: &PgPool,
    fields: &[RootField],
    current_user: Option<&UserData>,
) -> HashMap<(i32, EventLogsArgs), UserEventLogsLookup> {
    let mut wanted: Vec<(i32, EventLogsArgs)> = Vec::new();
    for field in fields {
        match field.name.as_str() {
            "currentUser" => {
                if let Some(user) = current_user {
                    collect(&field.selections, user.id, &mut wanted);
                }
            }
            "node" => {
                let Some(id) = field
                    .arguments
                    .string("id")
                    .and_then(maybe_query_is_id)
                else {
                    continue;
                };
                for (condition, selections) in &field.type_conditions {
                    if condition == "User" {
                        collect(selections, id, &mut wanted);
                    }
                }
            }
            _ => {}
        }
    }

    let mut lookups = HashMap::new();
    for (user_id, args) in wanted {
        if lookups.contains_key(&(user_id, args.clone())) {
            continue;
        }
        let result = async {
            let events = list_by_user(
                pool,
                user_id,
                args.event_name.as_deref(),
                args.first.map(i64::from),
            )
            .await?;
            let total = count_by_user(pool, user_id, args.event_name.as_deref()).await?;
            Ok::<_, String>((events, total))
        }
        .await;
        let lookup = match result {
            Ok((events, total)) => UserEventLogsLookup::Found { events, total },
            Err(error) => UserEventLogsLookup::Error(error),
        };
        lookups.insert((user_id, args), lookup);
    }
    lookups
}

/// `userEventLogsConnectionResolver` (`event_logs.go:28-70`).
pub struct EventLogsConnectionObject {
    user: Arc<UserObject>,
    events: Vec<UserEventLogRow>,
    total: i64,
    first: Option<i32>,
}

impl EventLogsConnectionObject {
    pub fn new(
        user: Arc<UserObject>,
        events: Vec<UserEventLogRow>,
        total: i64,
        first: Option<i32>,
    ) -> Self {
        Self {
            user,
            events,
            total,
            first,
        }
    }
}

impl ObjectValue for EventLogsConnectionObject {
    fn type_name(&self) -> &'static str {
        "EventLogsConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .events
                    .iter()
                    .map(|event| {
                        FieldValue::Object(Arc::new(EventLogObject {
                            user: self.user.clone(),
                            row: event.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("EventLog"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.total)))),
            // `gqlutil.HasNextPage(opt.LimitOffset != nil && count > opt.Limit)`
            // (`event_logs.go:62-70`): the UI only asks for `hasNextPage`.
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.first.is_some_and(|first| self.total > i64::from(first)),
                None,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `userEventLogResolver` (`event_logs.go:35-47`): the row fields.
pub struct EventLogObject {
    user: Arc<UserObject>,
    row: UserEventLogRow,
}

impl ObjectValue for EventLogObject {
    fn type_name(&self) -> &'static str {
        "EventLog"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.row.name.clone())))),
            // All listed events belong to this user (`UserID` filter), so the
            // event's `user` is the parent resolver.
            "user" => Ok(Some(FieldValue::Object(self.user.clone()))),
            "anonymousUserID" => Ok(Some(FieldValue::Json(Json::Str(
                self.row.anonymous_user_id.clone(),
            )))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.row.url.clone())))),
            "source" => Ok(Some(FieldValue::Json(Json::Str(self.row.source.clone())))),
            "argument" => Ok(Some(FieldValue::Json(Json::Str(self.row.argument.clone())))),
            "version" => Ok(Some(FieldValue::Json(Json::Str(self.row.version.clone())))),
            "timestamp" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.row.timestamp,
            ))))),
            _ => Ok(None),
        }
    }
}
