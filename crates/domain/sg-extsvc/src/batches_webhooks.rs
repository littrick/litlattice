#![forbid(unsafe_code)]

//! `internal/batches/webhooks`: outbound Batch Changes webhooks.
//!
//! Ported faithfully so the payload bytes are identical to the baseline:
//!
//! * the eight event types and their registrations ([`EVENT_TYPES`]),
//! * relay GraphQL ID encoding (`MarshalChangesetID`/`MarshalBatchChangeID`),
//! * the two GraphQL queries and their `/.internal/graphql?Name` URLs,
//! * `batchChange`/`changeset` payload structs in Go field order,
//! * `json.Marshal`-compatible JSON: HTML escaping, lowercase `\u00xx`
//!   control escapes, U+2028/U+2029, and `time.RFC3339Nano` formatting,
//! * `Enqueue`/`EnqueueBatchChange`/`EnqueueChangeset` over injectable HTTP
//!   and queue seams.
//!
//! The queue write itself stays with the caller: the worker adapts
//! [`WebhookEnqueueSink`] to the existing `outbound_webhook_jobs` queue
//! (`WebhookJobQueue::create(event_type, None, payload)`), which is exactly
//! Go's `outbound.OutboundWebhookService.Enqueue(ctx, eventType, nil, payload)`.
//! No new tables or keys are introduced.

use std::fmt::Write as _;

use serde_json::Value;
use time::OffsetDateTime;

use crate::{HttpDoer, HttpRequest, HttpResponse};

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

pub const BATCH_CHANGE_APPLY: &str = "batch_change:apply";
pub const BATCH_CHANGE_CLOSE: &str = "batch_change:close";
pub const BATCH_CHANGE_DELETE: &str = "batch_change:delete";
pub const CHANGESET_CLOSE: &str = "changeset:close";
pub const CHANGESET_PUBLISH: &str = "changeset:publish";
pub const CHANGESET_PUBLISH_ERROR: &str = "changeset:publish_error";
pub const CHANGESET_UPDATE: &str = "changeset:update";
pub const CHANGESET_UPDATE_ERROR: &str = "changeset:update_error";

/// `webhooks/event_types.go`'s `RegisterEventType` calls, in registration
/// order.
pub const EVENT_TYPES: &[(&str, &str)] = &[
    (BATCH_CHANGE_APPLY, "sent when a batch change is applied"),
    (BATCH_CHANGE_CLOSE, "sent when a batch change is closed"),
    (BATCH_CHANGE_DELETE, "sent when a batch change is deleted"),
    (CHANGESET_CLOSE, "sent when a changeset is closed"),
    (
        CHANGESET_PUBLISH,
        "sent when a changeset is published to the code host",
    ),
    (
        CHANGESET_PUBLISH_ERROR,
        "sent when an attempt to publish a changeset to the code host fails",
    ),
    (CHANGESET_UPDATE, "sent when a changeset is updated on the code host"),
    (
        CHANGESET_UPDATE_ERROR,
        "sent when an attempt to update a changeset on the code host fails",
    ),
];

/// `internalapi.frontendInternal`'s default (`SRC_FRONTEND_INTERNAL`), with
/// the default port the baseline appends.
pub const DEFAULT_INTERNAL_API_URL: &str = "http://sourcegraph-frontend-internal:80";

// ---------------------------------------------------------------------------
// Relay IDs
// ---------------------------------------------------------------------------

/// `relay.MarshalID("Changeset", id)`: padded URL-safe base64 of
/// `"Changeset:<decimal>"`.
pub fn changeset_gql_id(id: i64) -> String {
    relay_marshal_id("Changeset", id)
}

/// `relay.MarshalID("BatchChange", id)`.
pub fn batch_change_gql_id(id: i64) -> String {
    relay_marshal_id("BatchChange", id)
}

fn relay_marshal_id(kind: &str, id: i64) -> String {
    use base64::Engine as _;
    let raw = format!("{kind}:{id}");
    base64::engine::general_purpose::URL_SAFE.encode(raw.as_bytes())
}

/// `relay.UnmarshalKind`, used by the diff harness and callers that need to
/// validate an ID.
pub fn relay_id_kind(id: &str) -> Option<String> {
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::URL_SAFE.decode(id.as_bytes()).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (kind, _) = text.split_once(':')?;
    Some(kind.to_owned())
}

// ---------------------------------------------------------------------------
// GraphQL requests
// ---------------------------------------------------------------------------

pub const GQL_BATCH_CHANGE_QUERY: &str = r#"query BatchChange($id: ID!) {
	node(id: $id) {
		... on BatchChange {
			id
			namespace {
				id
			}
			name
			description
			state
			creator {
				id
			}
			lastApplier {
				id
			}
			url
			createdAt
			updatedAt
			lastAppliedAt
			closedAt
		}
	}
}"#;

pub const GQL_CHANGESET_QUERY: &str = r#"query Changeset($id: ID!) {
	node(id: $id) {
		... on ExternalChangeset {
			id
			externalID
			batchChanges {
				nodes {
					id
				}
			}
			repository {
				id
			}
			createdAt
			updatedAt
			title
			body
			author {
				name
				email
			}
			state
			labels {
				text
			}
			externalURL {
				url
			}
			forkNamespace
			reviewState
			checkState
			error
			syncerError
			forkName
			ownedByBatchChange
		}
	}
}"#;

/// `gqlURL`: replace the URL's path with `/.internal/graphql` and its query
/// with the query name. The base URL's scheme/host/port are preserved.
pub fn gql_url(internal_api_url: &str, query_name: &str) -> String {
    let prefix = match internal_api_url.find("://") {
        Some(index) => match internal_api_url[index + 3..].find('/') {
            Some(slash) => &internal_api_url[..index + 3 + slash],
            None => internal_api_url,
        },
        None => internal_api_url,
    };
    format!("{prefix}/.internal/graphql?{query_name}")
}

// ---------------------------------------------------------------------------
// Go-compatible JSON writing
// ---------------------------------------------------------------------------

/// Write a JSON string exactly like `encoding/json` with HTML escaping on.
pub fn write_go_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_go_opt_string(out: &mut String, value: Option<&str>) {
    match value {
        Some(value) => write_go_string(out, value),
        None => out.push_str("null"),
    }
}

fn write_go_string_list(out: &mut String, values: &[String]) {
    // Go's nil slice marshals as `null`; these lists are only built by
    // appending, so an empty list is always nil in the baseline.
    if values.is_empty() {
        out.push_str("null");
        return;
    }
    out.push('[');
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_go_string(out, value);
    }
    out.push(']');
}

/// `time.Time.MarshalJSON` → `t.Format(RFC3339Nano)` quoted.
///
/// Fractional seconds are trimmed of trailing zeros (and the dot dropped when
/// zero), and the offset is `Z` for UTC, `±hh:mm` otherwise.
pub fn format_go_rfc3339_nano(t: OffsetDateTime) -> String {
    let mut out = String::with_capacity(35);
    let _ = write!(
        out,
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    let nanos = t.nanosecond();
    if nanos != 0 {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        out.push('.');
        out.push_str(&fraction);
    }
    let offset = t.offset();
    if offset.is_utc() {
        out.push('Z');
    } else {
        let seconds = offset.whole_seconds();
        let sign = if seconds < 0 { '-' } else { '+' };
        let abs = seconds.abs();
        let _ = write!(out, "{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60);
    }
    out
}

fn write_go_time(out: &mut String, t: OffsetDateTime) {
    write_go_string(out, &format_go_rfc3339_nano(t));
}

fn write_go_opt_time(out: &mut String, t: Option<OffsetDateTime>) {
    match t {
        Some(t) => write_go_time(out, t),
        None => out.push_str("null"),
    }
}

/// `json.Marshal(queryInfo{...})`: struct field order `Query`, `Variables`,
/// `Name`; the variables map has the single `id` key.
pub fn query_info_json(variables: &[(&str, &str)], query: &str, name: &str) -> Vec<u8> {
    let mut out = String::new();
    out.push_str("{\"Query\":");
    write_go_string(&mut out, query);
    out.push_str(",\"Variables\":{");
    for (i, (key, value)) in variables.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_go_string(&mut out, key);
        out.push(':');
        write_go_string(&mut out, value);
    }
    out.push_str("},\"Name\":");
    write_go_string(&mut out, name);
    out.push('}');
    out.into_bytes()
}

// ---------------------------------------------------------------------------
// Payload types and marshalling
// ---------------------------------------------------------------------------

/// `webhooks.batchChange`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchChangePayload {
    pub id: String,
    pub namespace_id: String,
    pub name: String,
    pub description: String,
    pub state: String,
    pub creator_user_id: String,
    pub last_applier_user_id: Option<String>,
    pub url: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub last_applied_at: Option<OffsetDateTime>,
    pub closed_at: Option<OffsetDateTime>,
}

/// `webhooks.changeset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangesetPayload {
    pub id: String,
    pub external_id: String,
    pub batch_change_ids: Vec<String>,
    pub repository_id: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub title: Option<String>,
    pub body: Option<String>,
    pub author_name: Option<String>,
    pub author_email: Option<String>,
    pub state: String,
    pub labels: Vec<String>,
    pub external_url: Option<String>,
    pub fork_namespace: Option<String>,
    pub review_state: Option<String>,
    pub check_state: Option<String>,
    pub error: Option<String>,
    pub syncer_error: Option<String>,
    pub fork_name: Option<String>,
    pub owning_batch_change_id: Option<String>,
}

/// `json.Marshal(batchChange{...})`.
pub fn marshal_batch_change(payload: &BatchChangePayload) -> Vec<u8> {
    let mut out = String::new();
    out.push('{');
    out.push_str("\"id\":");
    write_go_string(&mut out, &payload.id);
    out.push_str(",\"namespace_id\":");
    write_go_string(&mut out, &payload.namespace_id);
    out.push_str(",\"name\":");
    write_go_string(&mut out, &payload.name);
    out.push_str(",\"description\":");
    write_go_string(&mut out, &payload.description);
    out.push_str(",\"state\":");
    write_go_string(&mut out, &payload.state);
    out.push_str(",\"creator_user_id\":");
    write_go_string(&mut out, &payload.creator_user_id);
    out.push_str(",\"last_applier_user_id\":");
    write_go_opt_string(&mut out, payload.last_applier_user_id.as_deref());
    out.push_str(",\"url\":");
    write_go_string(&mut out, &payload.url);
    out.push_str(",\"created_at\":");
    write_go_time(&mut out, payload.created_at);
    out.push_str(",\"updated_at\":");
    write_go_time(&mut out, payload.updated_at);
    out.push_str(",\"last_applied_at\":");
    write_go_opt_time(&mut out, payload.last_applied_at);
    out.push_str(",\"closed_at\":");
    write_go_opt_time(&mut out, payload.closed_at);
    out.push('}');
    out.into_bytes()
}

/// `json.Marshal(changeset{...})`.
pub fn marshal_changeset(payload: &ChangesetPayload) -> Vec<u8> {
    let mut out = String::new();
    out.push('{');
    out.push_str("\"id\":");
    write_go_string(&mut out, &payload.id);
    out.push_str(",\"external_id\":");
    write_go_string(&mut out, &payload.external_id);
    out.push_str(",\"batch_change_ids\":");
    write_go_string_list(&mut out, &payload.batch_change_ids);
    out.push_str(",\"repository_id\":");
    write_go_string(&mut out, &payload.repository_id);
    out.push_str(",\"created_at\":");
    write_go_time(&mut out, payload.created_at);
    out.push_str(",\"updated_at\":");
    write_go_time(&mut out, payload.updated_at);
    out.push_str(",\"title\":");
    write_go_opt_string(&mut out, payload.title.as_deref());
    out.push_str(",\"body\":");
    write_go_opt_string(&mut out, payload.body.as_deref());
    out.push_str(",\"author_name\":");
    write_go_opt_string(&mut out, payload.author_name.as_deref());
    out.push_str(",\"author_email\":");
    write_go_opt_string(&mut out, payload.author_email.as_deref());
    out.push_str(",\"state\":");
    write_go_string(&mut out, &payload.state);
    out.push_str(",\"labels\":");
    write_go_string_list(&mut out, &payload.labels);
    out.push_str(",\"external_url\":");
    write_go_opt_string(&mut out, payload.external_url.as_deref());
    out.push_str(",\"fork_namespace\":");
    write_go_opt_string(&mut out, payload.fork_namespace.as_deref());
    out.push_str(",\"review_state\":");
    write_go_opt_string(&mut out, payload.review_state.as_deref());
    out.push_str(",\"check_state\":");
    write_go_opt_string(&mut out, payload.check_state.as_deref());
    out.push_str(",\"error\":");
    write_go_opt_string(&mut out, payload.error.as_deref());
    out.push_str(",\"syncer_error\":");
    write_go_opt_string(&mut out, payload.syncer_error.as_deref());
    out.push_str(",\"fork_name\":");
    write_go_opt_string(&mut out, payload.fork_name.as_deref());
    out.push_str(",\"owning_batch_change_id\":");
    write_go_opt_string(&mut out, payload.owning_batch_change_id.as_deref());
    out.push('}');
    out.into_bytes()
}

// ---------------------------------------------------------------------------
// GraphQL response decoding
// ---------------------------------------------------------------------------

/// Case-insensitive object lookup, matching Go's `encoding/json` field
/// matching.
fn ci<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    let object = value.as_object()?;
    object
        .iter()
        .find_map(|(key, value)| key.eq_ignore_ascii_case(name).then_some(value))
}

fn ci_obj<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    ci(value, name).filter(|value| value.is_object())
}

fn ci_array<'a>(value: &'a Value, name: &str) -> &'a [Value] {
    ci(value, name)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn ci_string(value: &Value, name: &str) -> String {
    ci(value, name)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn ci_opt_string(value: &Value, name: &str) -> Option<String> {
    match ci(value, name) {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(other) => Some(other.to_string()),
    }
}

fn ci_time(value: &Value, name: &str) -> Result<OffsetDateTime, String> {
    match ci(value, name) {
        None | Some(Value::Null) => Ok(super::batches_state::go_zero_time()),
        Some(Value::String(raw)) => OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
            .map_err(|err| format!("decode {name}: {err}")),
        Some(other) => Err(format!("decode {name}: expected string, got {other}")),
    }
}

fn ci_opt_time(value: &Value, name: &str) -> Result<Option<OffsetDateTime>, String> {
    match ci(value, name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => Ok(Some(
            OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
                .map_err(|err| format!("decode {name}: {err}"))?,
        )),
        Some(other) => Err(format!("decode {name}: expected string, got {other}")),
    }
}

fn graphql_errors(value: &Value) -> Result<(), String> {
    let errors = ci_array(value, "errors");
    if errors.is_empty() {
        return Ok(());
    }
    Err(format!("graphql errors: {errors:?}"))
}

fn data_node<'a>(value: &'a Value) -> &'a Value {
    ci_obj(value, "data")
        .and_then(|data| ci_obj(data, "node"))
        .unwrap_or(&Value::Null)
}

/// `gqlBatchChangeResponse` → `batchChange`.
pub fn parse_batch_change_response(body: &[u8]) -> Result<BatchChangePayload, String> {
    let value: Value = serde_json::from_slice(body).map_err(|err| format!("decode response: {err}"))?;
    graphql_errors(&value)?;
    let node = data_node(&value);
    let namespace_id = ci_obj(node, "namespace")
        .map(|namespace| ci_string(namespace, "id"))
        .unwrap_or_default();
    let creator_user_id = ci_obj(node, "creator")
        .map(|creator| ci_string(creator, "id"))
        .unwrap_or_default();
    let last_applier_user_id = ci_obj(node, "lastApplier").and_then(|applier| {
        match ci(applier, "id") {
            None | Some(Value::Null) => None,
            Some(Value::String(text)) => Some(text.clone()),
            Some(other) => Some(other.to_string()),
        }
    });
    Ok(BatchChangePayload {
        id: ci_string(node, "id"),
        namespace_id,
        name: ci_string(node, "name"),
        description: ci_string(node, "description"),
        state: ci_string(node, "state"),
        creator_user_id,
        last_applier_user_id,
        url: ci_string(node, "url"),
        created_at: ci_time(node, "createdAt")?,
        updated_at: ci_time(node, "updatedAt")?,
        last_applied_at: ci_opt_time(node, "lastAppliedAt")?,
        closed_at: ci_opt_time(node, "closedAt")?,
    })
}

/// `gqlChangesetResponse` → `changeset`.
pub fn parse_changeset_response(body: &[u8]) -> Result<ChangesetPayload, String> {
    let value: Value = serde_json::from_slice(body).map_err(|err| format!("decode response: {err}"))?;
    graphql_errors(&value)?;
    let node = data_node(&value);

    let batch_change_ids = ci_obj(node, "batchChanges")
        .map(|batch_changes| {
            ci_array(batch_changes, "nodes")
                .iter()
                .map(|entry| ci_string(entry, "id"))
                .collect()
        })
        .unwrap_or_default();

    let labels = ci_array(node, "labels")
        .iter()
        .map(|entry| ci_string(entry, "text"))
        .collect();

    let author = ci_obj(node, "author");
    let author_name = author.map(|author| ci_string(author, "name"));
    let author_email = author.map(|author| ci_string(author, "email"));

    let external_url = ci_obj(node, "externalURL").map(|url| ci_string(url, "url"));

    let repository_id = ci_obj(node, "repository")
        .map(|repository| ci_string(repository, "id"))
        .unwrap_or_default();

    Ok(ChangesetPayload {
        id: ci_string(node, "id"),
        external_id: ci_string(node, "externalID"),
        batch_change_ids,
        repository_id,
        created_at: ci_time(node, "createdAt")?,
        updated_at: ci_time(node, "updatedAt")?,
        title: ci_opt_string(node, "title"),
        body: ci_opt_string(node, "body"),
        author_name,
        author_email,
        state: ci_string(node, "state"),
        labels,
        external_url,
        fork_namespace: ci_opt_string(node, "forkNamespace"),
        review_state: ci_opt_string(node, "reviewState"),
        check_state: ci_opt_string(node, "checkState"),
        error: ci_opt_string(node, "error"),
        syncer_error: ci_opt_string(node, "syncerError"),
        fork_name: ci_opt_string(node, "forkName"),
        owning_batch_change_id: ci_opt_string(node, "ownedByBatchChange"),
    })
}

// ---------------------------------------------------------------------------
// Transport + queue seams
// ---------------------------------------------------------------------------

/// `outbound.OutboundWebhookService.Enqueue(ctx, eventType, nil, payload)`.
#[async_trait::async_trait]
pub trait WebhookEnqueueSink: Send + Sync {
    /// Enqueue one webhook job. `scope` is always `None` (the baseline passes
    /// a nil scope).
    async fn enqueue(
        &self,
        event_type: &str,
        scope: Option<&str>,
        payload: &[u8],
    ) -> Result<(), String>;
}

async fn make_request(
    http: &dyn HttpDoer,
    internal_api_url: &str,
    query_name: &str,
    query: &str,
    id: &str,
) -> Result<HttpResponse, String> {
    let request = HttpRequest {
        method: "POST".to_string(),
        url: gql_url(internal_api_url, query_name),
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: query_info_json(&[("id", id)], query, query_name),
    };
    http.do_request(request).await
}

/// `marshalBatchChange` with an injected HTTP client.
pub async fn fetch_batch_change(
    http: &dyn HttpDoer,
    internal_api_url: &str,
    id: &str,
) -> Result<BatchChangePayload, String> {
    let response = make_request(
        http,
        internal_api_url,
        "BatchChange",
        GQL_BATCH_CHANGE_QUERY,
        id,
    )
    .await?;
    parse_batch_change_response(&response.body)
}

/// `marshalChangeset` with an injected HTTP client.
pub async fn fetch_changeset(
    http: &dyn HttpDoer,
    internal_api_url: &str,
    id: &str,
) -> Result<ChangesetPayload, String> {
    let response = make_request(
        http,
        internal_api_url,
        "Changeset",
        GQL_CHANGESET_QUERY,
        id,
    )
    .await?;
    parse_changeset_response(&response.body)
}

/// `EnqueueBatchChange`.
pub async fn enqueue_batch_change(
    sink: &dyn WebhookEnqueueSink,
    http: &dyn HttpDoer,
    internal_api_url: &str,
    event_type: &str,
    id: &str,
) -> Result<(), String> {
    let payload = fetch_batch_change(http, internal_api_url, id).await?;
    let bytes = marshal_batch_change(&payload);
    sink.enqueue(event_type, None, &bytes).await
}

/// `EnqueueChangeset`.
pub async fn enqueue_changeset(
    sink: &dyn WebhookEnqueueSink,
    http: &dyn HttpDoer,
    internal_api_url: &str,
    event_type: &str,
    id: &str,
) -> Result<(), String> {
    let payload = fetch_changeset(http, internal_api_url, id).await?;
    let bytes = marshal_changeset(&payload);
    sink.enqueue(event_type, None, &bytes).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(raw: &str) -> OffsetDateTime {
        OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339).unwrap()
    }

    #[test]
    fn relay_ids_match_go() {
        // base64.URLEncoding("Changeset:4711")
        assert_eq!(changeset_gql_id(4711), "Q2hhbmdlc2V0OjQ3MTE=");
        assert_eq!(batch_change_gql_id(1), "QmF0Y2hDaGFuZ2U6MQ==");
        assert_eq!(relay_id_kind(&changeset_gql_id(4711)).as_deref(), Some("Changeset"));
    }

    #[test]
    fn gql_url_replaces_path_and_query() {
        assert_eq!(
            gql_url(
                "http://sourcegraph-frontend-internal:80",
                "Changeset"
            ),
            "http://sourcegraph-frontend-internal:80/.internal/graphql?Changeset"
        );
        assert_eq!(
            gql_url("https://host/some/path?x=1", "N"),
            "https://host/.internal/graphql?N"
        );
    }

    #[test]
    fn go_time_formatting_trims_fraction() {
        assert_eq!(
            format_go_rfc3339_nano(at("2020-01-02T03:04:05Z")),
            "2020-01-02T03:04:05Z"
        );
        assert_eq!(
            format_go_rfc3339_nano(at("2020-01-02T03:04:05.120Z")),
            "2020-01-02T03:04:05.12Z"
        );
        assert_eq!(
            format_go_rfc3339_nano(at("2020-01-02T03:04:05.000000001Z")),
            "2020-01-02T03:04:05.000000001Z"
        );
        assert_eq!(
            format_go_rfc3339_nano(at("2020-01-02T03:04:05+02:00")),
            "2020-01-02T03:04:05+02:00"
        );
    }

    #[test]
    fn go_string_escaping() {
        let mut out = String::new();
        write_go_string(&mut out, "a<b>&c\u{2028}\u{0001}\n\t\"\\");
        assert_eq!(out, "\"a\\u003cb\\u003e\\u0026c\\u2028\\u0001\\n\\t\\\"\\\\\"");
    }

    #[test]
    fn batch_change_payload_matches_go_shape() {
        let payload = BatchChangePayload {
            id: "x".into(),
            namespace_id: "n".into(),
            name: "nm".into(),
            description: "".into(),
            state: "OPEN".into(),
            creator_user_id: "c".into(),
            last_applier_user_id: None,
            url: "u".into(),
            created_at: at("2020-01-02T03:04:05Z"),
            updated_at: at("2020-01-02T03:04:06Z"),
            last_applied_at: None,
            closed_at: Some(at("2020-01-03T00:00:00Z")),
        };
        let json = String::from_utf8(marshal_batch_change(&payload)).unwrap();
        assert_eq!(
            json,
            "{\"id\":\"x\",\"namespace_id\":\"n\",\"name\":\"nm\",\"description\":\"\",\
             \"state\":\"OPEN\",\"creator_user_id\":\"c\",\"last_applier_user_id\":null,\
             \"url\":\"u\",\"created_at\":\"2020-01-02T03:04:05Z\",\
             \"updated_at\":\"2020-01-02T03:04:06Z\",\"last_applied_at\":null,\
             \"closed_at\":\"2020-01-03T00:00:00Z\"}"
        );
    }

    #[test]
    fn changeset_payload_nil_slices_are_null() {
        let payload = ChangesetPayload {
            id: "i".into(),
            external_id: "e".into(),
            batch_change_ids: vec![],
            repository_id: "r".into(),
            created_at: at("2020-01-02T03:04:05Z"),
            updated_at: at("2020-01-02T03:04:05Z"),
            title: None,
            body: None,
            author_name: None,
            author_email: None,
            state: "OPEN".into(),
            labels: vec![],
            external_url: None,
            fork_namespace: None,
            review_state: None,
            check_state: None,
            error: None,
            syncer_error: None,
            fork_name: None,
            owning_batch_change_id: None,
        };
        let json = String::from_utf8(marshal_changeset(&payload)).unwrap();
        assert!(json.contains("\"batch_change_ids\":null"));
        assert!(json.contains("\"labels\":null"));
        assert!(json.contains("\"title\":null"));
        assert!(json.ends_with("\"owning_batch_change_id\":null}"));
    }

    #[test]
    fn parse_batch_change_response_case_insensitive() {
        let body = br#"{"data":{"node":{"id":"a","namespace":{"id":"n"},"name":"x",
            "description":"d","state":"OPEN","creator":{"id":"c"},"lastApplier":{"id":"l"},
            "url":"u","createdAt":"2020-01-02T03:04:05Z","updatedAt":"2020-01-02T03:04:06Z",
            "lastAppliedAt":null,"closedAt":"2020-01-03T00:00:00Z"}}}"#;
        let payload = parse_batch_change_response(body).unwrap();
        assert_eq!(payload.last_applier_user_id.as_deref(), Some("l"));
        assert_eq!(payload.closed_at, Some(at("2020-01-03T00:00:00Z")));
        assert_eq!(payload.last_applied_at, None);
    }

    #[test]
    fn parse_changeset_response_uses_graphql_field_names() {
        let body = br#"{"data":{"node":{"id":"i","externalID":"42",
            "batchChanges":{"nodes":[{"id":"b1"},{"id":"b2"}]},
            "repository":{"id":"r"},"createdAt":"2020-01-02T03:04:05Z",
            "updatedAt":"2020-01-02T03:04:06Z","title":null,"body":"b",
            "author":null,"state":"OPEN","labels":[{"text":"l1"}],
            "externalURL":{"url":"u"},"forkNamespace":null,"reviewState":"APPROVED",
            "checkState":"PASSED","error":null,"syncerError":null,"forkName":null,
            "ownedByBatchChange":"bc"}}}"#;
        let payload = parse_changeset_response(body).unwrap();
        assert_eq!(payload.external_id, "42");
        assert_eq!(payload.batch_change_ids, vec!["b1", "b2"]);
        assert_eq!(payload.labels, vec!["l1"]);
        assert_eq!(payload.author_name, None);
        assert_eq!(payload.external_url.as_deref(), Some("u"));
        assert_eq!(payload.owning_batch_change_id.as_deref(), Some("bc"));
    }

    #[test]
    fn graphql_errors_fail_marshalling() {
        let body = br#"{"errors":[{"message":"boom"}],"data":null}"#;
        assert!(parse_changeset_response(body).is_err());
        assert!(parse_batch_change_response(body).is_err());
    }
}
