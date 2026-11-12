#![forbid(unsafe_code)]

//! `internal/extsvc/phabricator`: the Phabricator Conduit surface the
//! repo-updater source needs (`diffusion.repository.search`).
//!
//! The baseline uses `github.com/uber/gonduit`; this port re-implements the
//! small subset: dial (`conduit.getcapabilities`), the urlencoded `output` /
//! `params` form with `__conduit__` metadata, Conduit error mapping, the
//! `apiRepo::ToRepo` conversion (including its `dateModified` →
//! `DateCreated` assignment bug), and the exact error texts.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use url::Url;

use crate::{HttpDoer, HttpRequest};

/// `extsvc.TypePhabricator`.
pub const TYPE_PHABRICATOR: &str = "phabricator";

const ZERO_TIME: &str = "0001-01-01T00:00:00Z";

/// `phabricator.Client`.
#[derive(Clone)]
pub struct Client {
    host: String,
    token: String,
    http: Arc<dyn HttpDoer>,
}

/// `phabricator.Cursor`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub limit: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub after: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub before: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub order: String,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// `phabricator.URI`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uri {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "PHID", default)]
    pub phid: String,
    #[serde(rename = "Display", default)]
    pub display: String,
    #[serde(rename = "Effective", default)]
    pub effective: String,
    #[serde(rename = "Normalized", default)]
    pub normalized: String,
    #[serde(rename = "Disabled", default)]
    pub disabled: bool,
    #[serde(rename = "BuiltinProtocol", default)]
    pub builtin_protocol: String,
    #[serde(rename = "BuiltinIdentifier", default)]
    pub builtin_identifier: String,
    #[serde(
        rename = "DateCreated",
        default,
        serialize_with = "serialize_go_time",
        deserialize_with = "deserialize_go_time"
    )]
    pub date_created: Option<OffsetDateTime>,
    #[serde(
        rename = "DateModified",
        default,
        serialize_with = "serialize_go_time",
        deserialize_with = "deserialize_go_time"
    )]
    pub date_modified: Option<OffsetDateTime>,
}

/// `phabricator.Repo`, serialized as `repo.metadata` with Go's exported field
/// names (the struct has no JSON tags).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repo {
    #[serde(rename = "ID", default)]
    pub id: u64,
    #[serde(rename = "PHID", default)]
    pub phid: String,
    #[serde(rename = "Name", default)]
    pub name: String,
    #[serde(rename = "VCS", default)]
    pub vcs: String,
    #[serde(rename = "Callsign", default)]
    pub callsign: String,
    #[serde(rename = "Shortname", default)]
    pub shortname: String,
    #[serde(rename = "Status", default)]
    pub status: String,
    #[serde(
        rename = "DateCreated",
        default,
        serialize_with = "serialize_go_time",
        deserialize_with = "deserialize_go_time"
    )]
    pub date_created: Option<OffsetDateTime>,
    #[serde(
        rename = "DateModified",
        default,
        serialize_with = "serialize_go_time",
        deserialize_with = "deserialize_go_time"
    )]
    pub date_modified: Option<OffsetDateTime>,
    #[serde(rename = "ViewPolicy", default)]
    pub view_policy: String,
    #[serde(rename = "EditPolicy", default)]
    pub edit_policy: String,
    #[serde(rename = "URIs", default)]
    pub uris: Vec<Uri>,
}

fn serialize_go_time<S>(value: &Option<OffsetDateTime>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let rendered = match value {
        Some(value) => value.format(&Rfc3339).map_err(serde::ser::Error::custom)?,
        None => ZERO_TIME.to_owned(),
    };
    serializer.serialize_str(&rendered)
}

fn deserialize_go_time<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match value {
        Some(raw) if raw != ZERO_TIME => OffsetDateTime::parse(&raw, &Rfc3339)
            .map(Some)
            .map_err(serde::de::Error::custom),
        _ => Ok(None),
    }
}

/// `phabricator.ParseDate`: epoch seconds, quoted or not.
pub fn parse_date(value: &Value) -> Result<Option<OffsetDateTime>, String> {
    let seconds = match value {
        Value::Null => return Ok(None),
        Value::String(raw) => raw
            .parse::<i64>()
            .map_err(|err| format!("phabricator: could not parse date: {err}"))?,
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| "phabricator: could not parse date".to_owned())?,
        _ => return Err("phabricator: could not parse date".to_owned()),
    };
    Ok(Some(OffsetDateTime::from_unix_timestamp(seconds).map_err(
        |err| format!("phabricator: could not parse date: {err}"),
    )?))
}

/// `phabricator.Client` errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhabricatorError {
    /// `core.ConduitError`.
    Conduit { code: String, info: String },
    /// `core.ErrMissingResults`.
    MissingResults,
    Transport(String),
    Json(String),
    Capability(String),
}

impl std::fmt::Display for PhabricatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conduit { code, info } => write!(f, "{code}: {info}"),
            Self::MissingResults => f.write_str("Results key was not provided in the response object."),
            Self::Transport(err) | Self::Json(err) | Self::Capability(err) => f.write_str(err),
        }
    }
}

impl std::error::Error for PhabricatorError {}

impl Client {
    /// `phabricator.NewClient` before dialing.
    pub fn new(url: &str, token: &str, http: Arc<dyn HttpDoer>) -> Result<Self, String> {
        // `core.GetEndpointURI` trims a single trailing slash.
        let host = url.trim_end_matches('/').to_owned();
        Url::parse(&host).map_err(|err| err.to_string())?;
        Ok(Self {
            host,
            token: token.to_owned(),
            http,
        })
    }

    /// `gonduit.DialContext`.
    ///
    /// Errata: gonduit v0.13.0 computes `assertSupportedCapabilities` but
    /// discards the returned error (`dialer.go`), so a server advertising
    /// capabilities the client cannot use still dials successfully; the
    /// failure only surfaces on the next call. Mirror that behavior.
    pub async fn dial(&self) -> Result<(), PhabricatorError> {
        let result = self
            .call("conduit.getcapabilities", &Value::Object(Map::new()), false)
            .await?;
        let capabilities: Capabilities = serde_json::from_value(result)
            .map_err(|err| PhabricatorError::Json(err.to_string()))?;
        let _ = assert_supported_capabilities(&capabilities, &self.token);
        Ok(())
    }

    /// `Conn.CallContext`: POST `<host>/api/<method>` with the Conduit form.
    async fn call(
        &self,
        method: &str,
        params: &Value,
        with_metadata: bool,
    ) -> Result<Value, PhabricatorError> {
        let mut params = params.clone();
        if with_metadata {
            let metadata = if self.token.is_empty() {
                Value::Object(Map::new())
            } else {
                serde_json::json!({ "token": self.token })
            };
            if let Value::Object(map) = &mut params {
                map.insert("__conduit__".to_owned(), metadata);
            }
        }

        let encoded_params = serde_json::to_string(&params)
            .map_err(|err| PhabricatorError::Json(err.to_string()))?;
        let body = format!("output=json&params={}", url_encode(&encoded_params));
        let url = format!("{}/api/{method}", self.host);

        let response = self
            .http
            .do_request(HttpRequest {
                method: "POST".to_owned(),
                url,
                headers: vec![(
                    "Content-Type".to_owned(),
                    "application/x-www-form-urlencoded".to_owned(),
                )],
                body: body.into_bytes(),
            })
            .await
            .map_err(PhabricatorError::Transport)?;

        let body_text = String::from_utf8_lossy(&response.body).into_owned();
        let body_value: Value = match serde_json::from_str(&body_text) {
            Ok(value) => value,
            Err(_) => {
                return Err(PhabricatorError::Conduit {
                    code: response.status.to_string(),
                    info: body_text,
                })
            }
        };

        if let Some(code) = body_value
            .get("error_code")
            .and_then(Value::as_str)
            .filter(|code| !code.is_empty())
        {
            return Err(PhabricatorError::Conduit {
                code: code.to_owned(),
                info: body_value
                    .get("error_info")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            });
        }

        match body_value.get("result") {
            // Go unmarshals an empty array into a nil result and returns nil.
            Some(Value::Array(items)) if items.is_empty() => Ok(Value::Null),
            Some(result) => Ok(result.clone()),
            None => Err(PhabricatorError::MissingResults),
        }
    }

    /// `Client.ListRepos`.
    pub async fn list_repos(
        &self,
        cursor: &Cursor,
    ) -> Result<(Vec<Repo>, Cursor), PhabricatorError> {
        let mut cursor = cursor.clone();
        if cursor.order.is_empty() {
            cursor.order = "oldest".to_owned();
        }
        if cursor.limit == 0 {
            cursor.limit = 100;
        }

        let mut params = Map::new();
        let cursor_value = serde_json::to_value(&cursor)
            .map_err(|err| PhabricatorError::Json(err.to_string()))?;
        if let Value::Object(map) = cursor_value {
            for (key, value) in map {
                params.insert(key, value);
            }
        }
        params.insert("attachments".to_owned(), serde_json::json!({"uris": true}));

        let result = self
            .call(
                "diffusion.repository.search",
                &Value::Object(params),
                true,
            )
            .await?;

        let response: SearchResponse = serde_json::from_value(result)
            .map_err(|err| PhabricatorError::Json(err.to_string()))?;
        let repos = response.data.iter().map(ApiRepo::to_repo).collect();
        Ok((repos, response.cursor))
    }
}

/// `gonduit.assertSupportedCapabilities`. The caller ignores the result, as
/// Go does; kept as a separate function so the errata above is obvious.
fn assert_supported_capabilities(
    capabilities: &Capabilities,
    token: &str,
) -> Result<(), PhabricatorError> {
    if !token.is_empty() && !capabilities.authentication.iter().any(|v| v == "token") {
        return Err(PhabricatorError::Capability(
            "Token authentication is not supported".to_owned(),
        ));
    }
    if !capabilities.input.iter().any(|v| v == "urlencoded") {
        return Err(PhabricatorError::Capability(
            "urlencoded input not supported".to_owned(),
        ));
    }
    if !capabilities.output.iter().any(|v| v == "json") {
        return Err(PhabricatorError::Capability(
            "JSON output is not supported".to_owned(),
        ));
    }
    Ok(())
}

fn url_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

#[derive(Debug, Default, Deserialize)]
struct Capabilities {
    #[serde(default)]
    authentication: Vec<String>,
    #[serde(default)]
    input: Vec<String>,
    #[serde(default)]
    output: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SearchResponse {
    #[serde(default)]
    data: Vec<ApiRepo>,
    #[serde(default)]
    cursor: Cursor,
}

#[derive(Debug, Default, Deserialize)]
struct ApiRepo {
    #[serde(default)]
    id: u64,
    #[serde(default)]
    phid: String,
    #[serde(default)]
    fields: ApiRepoFields,
    #[serde(default)]
    attachments: ApiRepoAttachments,
}

#[derive(Debug, Default, Deserialize)]
struct ApiRepoFields {
    #[serde(default)]
    name: String,
    #[serde(default)]
    vcs: String,
    #[serde(default)]
    callsign: String,
    #[serde(default)]
    shortname: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    policy: ApiRepoPolicy,
    #[serde(rename = "dateCreated", default)]
    date_created: Value,
    #[serde(rename = "dateModified", default)]
    date_modified: Value,
}

#[derive(Debug, Default, Deserialize)]
struct ApiRepoPolicy {
    #[serde(default)]
    view: String,
    #[serde(default)]
    edit: String,
}

#[derive(Debug, Default, Deserialize)]
struct ApiRepoAttachments {
    #[serde(default)]
    uris: ApiUrisContainer,
}

#[derive(Debug, Default, Deserialize)]
struct ApiUrisContainer {
    #[serde(default)]
    uris: Vec<ApiUri>,
}

#[derive(Debug, Default, Deserialize)]
struct ApiUri {
    #[serde(default)]
    id: String,
    #[serde(default)]
    phid: String,
    #[serde(default)]
    fields: ApiUriFields,
}

#[derive(Debug, Default, Deserialize)]
struct ApiUriFields {
    #[serde(default)]
    uri: ApiUris,
    #[serde(default)]
    builtin: ApiUriBuiltin,
    #[serde(default)]
    disabled: bool,
    #[serde(rename = "dateCreated", default)]
    date_created: Value,
    #[serde(rename = "dateModified", default)]
    date_modified: Value,
}

#[derive(Debug, Default, Deserialize)]
struct ApiUris {
    #[serde(default)]
    display: String,
    #[serde(default)]
    effective: String,
    #[serde(default)]
    normalized: String,
}

#[derive(Debug, Default, Deserialize)]
struct ApiUriBuiltin {
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    identifier: String,
}

impl ApiRepo {
    /// `apiRepo.ToRepo`.
    fn to_repo(&self) -> Repo {
        let mut uris = Vec::with_capacity(self.attachments.uris.uris.len());
        for uri in &self.attachments.uris.uris {
            let mut mapped = Uri {
                id: uri.id.clone(),
                phid: uri.phid.clone(),
                display: uri.fields.uri.display.clone(),
                effective: uri.fields.uri.effective.clone(),
                normalized: uri.fields.uri.normalized.clone(),
                disabled: uri.fields.disabled,
                builtin_protocol: uri.fields.builtin.protocol.clone(),
                builtin_identifier: uri.fields.builtin.identifier.clone(),
                date_created: parse_date(&uri.fields.date_created).unwrap_or(None),
                date_modified: None,
            };
            // Baseline bug: the DateModified timestamp is assigned to
            // `uri.DateCreated`, leaving `DateModified` zero.
            if let Ok(Some(modified)) = parse_date(&uri.fields.date_modified) {
                mapped.date_created = Some(modified);
            }
            uris.push(mapped);
        }

        Repo {
            id: self.id,
            phid: self.phid.clone(),
            name: self.fields.name.clone(),
            vcs: self.fields.vcs.clone(),
            callsign: self.fields.callsign.clone(),
            shortname: self.fields.shortname.clone(),
            status: self.fields.status.clone(),
            date_created: parse_date(&self.fields.date_created).unwrap_or(None),
            date_modified: parse_date(&self.fields.date_modified).unwrap_or(None),
            view_policy: self.fields.policy.view.clone(),
            edit_policy: self.fields.policy.edit.clone(),
            uris,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_repo_conversion_including_date_bug() {
        let raw = serde_json::json!({
            "id": 3,
            "phid": "PHID-REPO-3",
            "fields": {
                "name": "repo",
                "vcs": "git",
                "callsign": "R",
                "shortname": "repo",
                "status": "active",
                "policy": {"view": "public", "edit": "admin"},
                "dateCreated": "100",
                "dateModified": "200"
            },
            "attachments": {"uris": {"uris": [{
                "id": "1", "phid": "PHID-URI-1",
                "fields": {
                    "uri": {"display": "d", "effective": "e", "normalized": "n"},
                    "builtin": {"protocol": "https", "identifier": "shortname"},
                    "disabled": false,
                    "dateCreated": "10",
                    "dateModified": "20"
                }
            }]}}
        });
        let api: ApiRepo = serde_json::from_value(raw).unwrap();
        let repo = api.to_repo();
        assert_eq!(repo.id, 3);
        assert_eq!(repo.date_created.unwrap().unix_timestamp(), 100);
        assert_eq!(repo.date_modified.unwrap().unix_timestamp(), 200);
        assert_eq!(repo.uris.len(), 1);
        // The baseline writes dateModified into DateCreated for URIs.
        assert_eq!(repo.uris[0].date_created.unwrap().unix_timestamp(), 20);
        assert!(repo.uris[0].date_modified.is_none());
    }

    #[test]
    fn metadata_uses_go_exported_field_names() {
        let repo = Repo {
            id: 1,
            phid: "p".to_owned(),
            name: "n".to_owned(),
            vcs: "git".to_owned(),
            callsign: "c".to_owned(),
            shortname: "s".to_owned(),
            status: "active".to_owned(),
            date_created: None,
            date_modified: None,
            view_policy: "v".to_owned(),
            edit_policy: "e".to_owned(),
            uris: Vec::new(),
        };
        let value = serde_json::to_value(&repo).unwrap();
        assert_eq!(value["DateCreated"], serde_json::json!(ZERO_TIME));
        assert_eq!(value["URIs"], serde_json::json!([]));
        assert!(value.get("ID").is_some());
    }

    #[test]
    fn parse_date_accepts_strings_and_numbers() {
        assert_eq!(
            parse_date(&serde_json::json!("100")).unwrap().unwrap().unix_timestamp(),
            100
        );
        assert_eq!(
            parse_date(&serde_json::json!(200)).unwrap().unwrap().unix_timestamp(),
            200
        );
        assert!(parse_date(&serde_json::Value::Null).unwrap().is_none());
    }
}
