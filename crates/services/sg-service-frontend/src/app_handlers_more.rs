#![forbid(unsafe_code)]

//! Remaining `cmd/frontend/internal/app` HTTP handlers (M9.8jb):
//! `verify_email.go`, `usage_stats.go`, `one_click_export.go`, `ping.go`,
//! `editor.go`, the offline subset of `debug.go`, and the old-tree redirect
//! (`router/old_tree_redirects.go`).
//!
//! Like [`crate::app_handlers`], every process-global dependency is injected:
//! the actor, the site-admin check, the user/email stores, the usage-stats and
//! one-click-export archives, the latest ping row, and the editor's clone-URL
//! / revision lookups. Handlers whose dependency is absent answer
//! [`crate::app_handlers::not_found`] (the same 404 as an unported route), so
//! the differential harness can choose per case.

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{self, HeaderValue};
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use serde::Deserialize;
use tonic::body::BoxBody;

use sg_graphql::exec::Actor;

use crate::app_handlers::{
    go_error, go_path_clean, go_path_escape, go_query_escape, not_found, query_param, redirect,
    AppEnv,
};

/// The dependencies of the handlers in this module.
///
/// `actor` is filled per request by the session middleware (M9.8ja); until
/// that lands it stays [`Actor::Anonymous`].
#[derive(Clone, Default)]
pub struct MoreDeps {
    pub actor: Actor,
    pub admin: Option<std::sync::Arc<dyn SiteAdminCheck>>,
    pub verify_email: Option<std::sync::Arc<dyn VerifyEmailDb>>,
    pub usage_stats: Option<std::sync::Arc<dyn UsageStatsArchive>>,
    pub one_click_export: Option<std::sync::Arc<dyn OneClickExport>>,
    pub latest_ping: Option<std::sync::Arc<dyn LatestPing>>,
    pub editor: Option<std::sync::Arc<dyn EditorBackend>>,
}

/// `auth.CheckCurrentUserIsSiteAdmin` (`internal/auth/site_admin.go:18-33`).
#[async_trait]
pub trait SiteAdminCheck: Send + Sync {
    /// `Err(message)` carries the baseline error text (`not authenticated`,
    /// `must be site admin`, or a database error).
    async fn check_site_admin(&self, actor: Actor) -> Result<(), String>;
}

/// The current user returned by `db.Users().GetByCurrentAuthUser`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentUser {
    pub id: i32,
}

/// The `db.Users()` / `db.UserEmails()` / `db.SecurityEventLogs()` /
/// `db.Authz()` slice `serveVerifyEmail` reads.
#[async_trait]
pub trait VerifyEmailDb: Send + Sync {
    /// `db.Users().GetByCurrentAuthUser(ctx)`.
    async fn current_user(&self) -> Result<CurrentUser, String>;
    /// `db.UserEmails().Get(ctx, userID, email)` → `(canonicalEmail, verified)`.
    async fn get_email(&self, user_id: i32, email: &str) -> Result<(String, bool), String>;
    /// `db.UserEmails().Verify(ctx, userID, email, code)`.
    async fn verify(&self, user_id: i32, email: &str, code: &str) -> Result<bool, String>;
    /// `db.UserEmails().GetPrimaryEmail(ctx, userID)`.
    async fn get_primary_email(&self, user_id: i32) -> Result<(String, bool), String>;
    /// `db.UserEmails().SetPrimaryEmail(ctx, userID, email)`.
    async fn set_primary_email(&self, user_id: i32, email: &str) -> Result<(), String>;
    /// `db.SecurityEventLogs().LogSecurityEvent(...)`; failures are ignored.
    async fn log_security_event(
        &self,
        user_id: i32,
        url_path: &str,
        email: &str,
    ) -> Result<(), String>;
    /// `db.Authz().GrantPendingPermissions(...)`; failures are ignored.
    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), String>;
}

/// `usagestats.GetArchive` (`internal/usagestats/usage_stats.go:24-109`).
#[async_trait]
pub trait UsageStatsArchive: Send + Sync {
    async fn get_archive(&self) -> Result<Vec<u8>, String>;
}

/// `oneclickexport.ExportRequest` (`oneclickexport/export.go:76-80`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OneClickExportRequest {
    pub include_site_config: bool,
    pub include_code_host_config: bool,
    pub db_queries: Vec<OneClickExportDbQuery>,
}

/// `oneclickexport.DBQueryRequest`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OneClickExportDbQuery {
    pub table_name: String,
    pub count: i64,
}

/// `oce.NewDataExporter(db, logger).Export`.
#[async_trait]
pub trait OneClickExport: Send + Sync {
    async fn export(&self, request: &OneClickExportRequest) -> Result<Vec<u8>, String>;
}

/// `db.EventLogs().LatestPing(ctx)` (`internal/database/event_logs.go:415-424`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LatestPingResult {
    Found(Vec<u8>),
    NoRows,
}

#[async_trait]
pub trait LatestPing: Send + Sync {
    async fn latest_ping(&self) -> Result<LatestPingResult, String>;
}

/// The `cloneurls.RepoSourceCloneURLToRepoName` + `backend.Repos` slice the
/// editor handler uses.
#[async_trait]
pub trait EditorBackend: Send + Sync {
    /// `cloneurls.RepoSourceCloneURLToRepoName`; `None` means "not supported".
    async fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<Option<String>, String>;
    /// `repos.GetByName`; `Err` is the baseline's non-nil error path.
    async fn get_by_name(&self, repo: &str) -> Result<(), String>;
    /// `repos.ResolveRev(ctx, repo, rev)`.
    async fn resolve_rev(&self, repo: &str, rev: &str) -> Result<String, String>;
}

// ---------------------------------------------------------------------------
// Small response helpers (Go net/http).
// ---------------------------------------------------------------------------

/// `http.Error` (`net/http/server.go:2200-2213`).
fn http_error(status: u16, message: &str) -> Response<BoxBody> {
    let mut response = crate::middleware::text_response(
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        "text/plain; charset=utf-8",
        format!("{message}\n"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// A bare `w.WriteHeader(status)`.
fn status_only(status: u16) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() =
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    response
}

/// The headers set by `usageStatsArchiveHandler` / `oneClickExportHandler`
/// before they touch the archive.
fn zip_response(filename: &str) -> Response<BoxBody> {
    let mut response = status_only(200);
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/zip"),
    );
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\"")) {
        response
            .headers_mut()
            .insert(header::CONTENT_DISPOSITION, value);
    }
    response
}

fn set_body(response: &mut Response<BoxBody>, body: Vec<u8>) {
    *response.body_mut() = tonic::body::boxed(Full::new(Bytes::from(body)));
}

/// `r.URL.String()` for an incoming origin-form request: the raw path plus
/// query, exactly as the client sent it.
fn request_url_string<B>(req: &Request<B>) -> String {
    match req.uri().path_and_query() {
        Some(path_and_query) => path_and_query.as_str().to_owned(),
        None => req.uri().path().to_owned(),
    }
}

/// `fmt.Sprintf("%q", s)` for the plain values these handlers carry.
fn go_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        match ch {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            ch if (ch as u32) < 0x20 => quoted.push_str(&format!("\\x{:02x}", ch as u32)),
            ch => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

// ---------------------------------------------------------------------------
// /-/verify-email (`verify_email.go:15-79`)
// ---------------------------------------------------------------------------

/// `serveVerifyEmail`.
pub async fn verify_email<B>(req: &Request<B>, deps: &MoreDeps) -> Response<BoxBody> {
    let Some(db) = deps.verify_email.as_deref() else {
        return not_found();
    };
    let query = req.uri().query();

    if !deps.actor.is_authenticated() {
        let return_to = request_url_string(req);
        let location = format!("/sign-in?returnTo={}", go_query_escape(&return_to));
        return redirect(req, &location, 302);
    }

    let requested_email = query_param(query, "email").unwrap_or_default();
    let verify_code = query_param(query, "code").unwrap_or_default();

    let user = match db.current_user().await {
        Ok(user) => user,
        Err(_) => return http_error(401, "Could not get current user"),
    };
    // On `Get` failure the baseline's first return value is the empty string.
    let (email, already_verified) = match db.get_email(user.id, &requested_email).await {
        Ok(result) => result,
        Err(_) => return http_error(400, &format!("No email \"\" found for user {}", user.id)),
    };
    if already_verified {
        return http_error(
            400,
            &format!(
                "User {} email {} is already verified",
                user.id,
                go_quote(&email)
            ),
        );
    }

    match db.verify(user.id, &email, &verify_code).await {
        Err(_) => return http_error(500, "Could not verify user email"),
        Ok(false) => {
            return http_error(
                401,
                "Could not verify user email. Email verification code did not match.",
            )
        }
        Ok(true) => {}
    }

    if db.get_primary_email(user.id).await.is_err()
        && db.set_primary_email(user.id, &email).await.is_err()
    {
        return http_error(500, "Could not set primary email.");
    }

    // Both failures are logged and ignored by the baseline.
    let _ = db
        .log_security_event(user.id, req.uri().path(), &email)
        .await;
    let _ = db.grant_pending_permissions(user.id).await;

    redirect(req, "/search", 302)
}

// ---------------------------------------------------------------------------
// /site-admin/usage-statistics/archive (`usage_stats.go:13-31`)
// ---------------------------------------------------------------------------

/// `usageStatsArchiveHandler`.
pub async fn usage_stats<B>(_req: &Request<B>, deps: &MoreDeps) -> Response<BoxBody> {
    let (Some(admin), Some(archive)) = (deps.admin.as_deref(), deps.usage_stats.as_deref()) else {
        return not_found();
    };
    // 🚨 SECURITY: only site admins may download the usage archive.
    if admin.check_site_admin(deps.actor).await.is_err() {
        return status_only(401);
    }

    let mut response = zip_response("SourcegraphUsersUsageArchive.zip");
    match archive.get_archive().await {
        Ok(bytes) => set_body(&mut response, bytes),
        Err(_) => *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR,
    }
    response
}

// ---------------------------------------------------------------------------
// /site-admin/data-export/archive (`one_click_export.go:15-46`)
// ---------------------------------------------------------------------------

/// `oneClickExportHandler`. `body` is the already-collected request body.
pub async fn one_click_export<B>(
    _req: &Request<B>,
    deps: &MoreDeps,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    let (Some(admin), Some(exporter)) = (deps.admin.as_deref(), deps.one_click_export.as_deref())
    else {
        return not_found();
    };
    // 🚨 SECURITY: only site admins may export.
    if admin.check_site_admin(deps.actor).await.is_err() {
        return status_only(401);
    }

    // Headers are set before the request is decoded, and `http.Error` does not
    // clear `Content-Disposition`; both are observable on the 400 path.
    let mut response = zip_response("SourcegraphDataExport.zip");
    let request = match decode_export_request(body.unwrap_or_default()) {
        Ok(request) => request,
        Err(message) => {
            let mut error = http_error(400, &message);
            if let Some(value) = response.headers().get(header::CONTENT_DISPOSITION) {
                error
                    .headers_mut()
                    .insert(header::CONTENT_DISPOSITION, value.clone());
            }
            return error;
        }
    };

    match exporter.export(&request).await {
        Ok(bytes) => {
            set_body(&mut response, bytes);
            response
        }
        Err(_) => {
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

/// Decodes exactly one JSON value like `json.NewDecoder(r.Body).Decode`, with
/// the baseline's error texts for the shapes the corpus exercises.
fn decode_export_request(body: &[u8]) -> Result<OneClickExportRequest, String> {
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let value = serde_json::Value::deserialize(&mut deserializer)
        .map_err(|error| go_decode_error(body, &error))?;
    match value {
        serde_json::Value::Null => Ok(OneClickExportRequest::default()),
        serde_json::Value::Object(mut object) => {
            let mut request = OneClickExportRequest::default();
            if let Some(value) = object.remove("includeSiteConfig") {
                request.include_site_config = export_bool(value, "includeSiteConfig")?;
            }
            if let Some(value) = object.remove("includeCodeHostConfig") {
                request.include_code_host_config = export_bool(value, "includeCodeHostConfig")?;
            }
            if let Some(value) = object.remove("dbQueries") {
                match value {
                    serde_json::Value::Null => {}
                    serde_json::Value::Array(items) => {
                        for item in items {
                            match item {
                                serde_json::Value::Null => {}
                                serde_json::Value::Object(mut query) => {
                                    let mut db_query = OneClickExportDbQuery::default();
                                    if let Some(value) = query.remove("tableName") {
                                        db_query.table_name = match value {
                                            serde_json::Value::Null => String::new(),
                                            serde_json::Value::String(value) => value,
                                            other => return Err(format!(
                                                "json: cannot unmarshal {} into Go struct field DBQueryRequest.dbQueries.tableName of type string",
                                                json_kind(&other)
                                            )),
                                        };
                                    }
                                    if let Some(value) = query.remove("count") {
                                        db_query.count = match value {
                                            serde_json::Value::Null => 0,
                                            serde_json::Value::Number(number) => {
                                                match number.as_i64() {
                                                    Some(count) => count,
                                                    None => return Err(format!(
                                                        "json: cannot unmarshal number {} into Go struct field DBQueryRequest.dbQueries.count of type oneclickexport.Limit",
                                                        number
                                                    )),
                                                }
                                            }
                                            other => return Err(format!(
                                                "json: cannot unmarshal {} into Go struct field DBQueryRequest.dbQueries.count of type oneclickexport.Limit",
                                                json_kind(&other)
                                            )),
                                        };
                                    }
                                    request.db_queries.push(db_query);
                                }
                                other => {
                                    return Err(format!(
                                        "json: cannot unmarshal {} into Go struct field ExportRequest.dbQueries of type oneclickexport.DBQueryRequest",
                                        json_kind(&other)
                                    ))
                                }
                            }
                        }
                    }
                    other => {
                        return Err(format!(
                            "json: cannot unmarshal {} into Go struct field ExportRequest.dbQueries of type []*oneclickexport.DBQueryRequest",
                            json_kind(&other)
                        ))
                    }
                }
            }
            Ok(request)
        }
        other => Err(format!(
            "json: cannot unmarshal {} into Go value of type oneclickexport.ExportRequest",
            json_kind(&other)
        )),
    }
}

fn export_bool(value: serde_json::Value, field: &str) -> Result<bool, String> {
    match value {
        serde_json::Value::Null => Ok(false),
        serde_json::Value::Bool(value) => Ok(value),
        other => Err(format!(
            "json: cannot unmarshal {} into Go struct field ExportRequest.{field} of type bool",
            json_kind(&other)
        )),
    }
}

/// `encoding/json`'s type names in error messages.
fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// `json.Decoder.Decode` error texts: `EOF` for an empty body, `unexpected
/// EOF` for truncated input, and the invalid-character syntax error otherwise.
fn go_decode_error(body: &[u8], error: &serde_json::Error) -> String {
    if error.is_eof() {
        let empty = body.iter().all(|byte| byte.is_ascii_whitespace());
        return if empty {
            "EOF".to_owned()
        } else {
            "unexpected EOF".to_owned()
        };
    }
    go_json_syntax_error(body)
}

/// `json.Unmarshal` error texts (used by the editor's hostname-patterns
/// option): truncated input reports `unexpected end of JSON input`.
fn go_unmarshal_error(body: &[u8], error: &serde_json::Error) -> String {
    if error.is_eof() {
        return "unexpected end of JSON input".to_owned();
    }
    go_json_syntax_error(body)
}

fn go_json_syntax_error(body: &[u8]) -> String {
    match body
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
    {
        None => "unexpected end of JSON input".to_owned(),
        Some(byte) => format!(
            "invalid character {} looking for beginning of value",
            go_quote(&(byte as char).to_string())
        ),
    }
}

// ---------------------------------------------------------------------------
// /site-admin/pings/latest (`ping.go:16-36`)
// ---------------------------------------------------------------------------

/// `latestPingHandler`.
pub async fn latest_ping<B>(_req: &Request<B>, deps: &MoreDeps) -> Response<BoxBody> {
    let (Some(admin), Some(ping)) = (deps.admin.as_deref(), deps.latest_ping.as_deref()) else {
        return not_found();
    };
    // 🚨 SECURITY: only site admins may read ping data.
    if admin.check_site_admin(deps.actor).await.is_err() {
        return status_only(401);
    }

    let mut response = status_only(200);
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    match ping.latest_ping().await {
        Ok(LatestPingResult::NoRows) => set_body(&mut response, b"{}".to_vec()),
        Ok(LatestPingResult::Found(argument)) => set_body(&mut response, argument),
        Err(_) => *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR,
    }
    response
}

// ---------------------------------------------------------------------------
// /-/editor (`editor.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, PartialEq, Eq)]
struct EditorOpenFileRequest {
    remote_url: String,
    branch: String,
    revision: String,
    file: String,
    start_row: i64,
    end_row: i64,
    start_col: i64,
    end_col: i64,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct EditorSearchRequest {
    query: String,
    remote_url: String,
    branch: String,
    revision: String,
    file: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
enum EditorRequest {
    #[default]
    Empty,
    Search(Box<EditorSearchRequest>),
    OpenFile(Box<EditorOpenFileRequest>),
}

/// `serveEditor` (`editor.go:255-272`).
pub async fn editor<B>(req: &Request<B>, deps: &MoreDeps) -> Response<BoxBody> {
    let Some(backend) = deps.editor.as_deref() else {
        return not_found();
    };
    let editor_request = match parse_editor_request(req.uri().query()) {
        Ok(request) => request,
        Err(message) => return editor_bad_request(&message),
    };
    match editor_redirect_url(backend, &editor_request).await {
        Ok(location) => redirect(req, &location, 303),
        Err(message) => editor_bad_request(&message),
    }
}

/// `serveEditor`'s `WriteHeader(400); fmt.Fprintf(w, "%s", err)`.
fn editor_bad_request(message: &str) -> Response<BoxBody> {
    // `WriteHeader(400)` then `fmt.Fprintf` without a Content-Type: the
    // recorder never sniffs one, so the header stays absent.
    let mut response = status_only(400);
    set_body(&mut response, message.as_bytes().to_vec());
    response
}

/// `parseEditorRequest` (`editor.go:222-253`).
fn parse_editor_request(query: Option<&str>) -> Result<EditorRequest, String> {
    // `r.URL.Query()` is never nil, so the baseline's nil branch is
    // unreachable over HTTP.
    let query = query.unwrap_or("");
    let get = |name: &str| query_param(Some(query), name).unwrap_or_default();

    let search = get("search");
    if !search.is_empty() {
        let patterns = get("search_hostname_patterns");
        if !patterns.is_empty() {
            parse_hostname_patterns(&patterns)?;
        }
        return Ok(EditorRequest::Search(Box::new(EditorSearchRequest {
            query: search,
            remote_url: get("search_remote_url"),
            branch: get("search_branch"),
            revision: get("search_revision"),
            file: get("search_file"),
        })));
    }

    let remote_url = get("remote_url");
    if !remote_url.is_empty() {
        let patterns = get("hostname_patterns");
        if !patterns.is_empty() {
            parse_hostname_patterns(&patterns)?;
        }
        return Ok(EditorRequest::OpenFile(Box::new(EditorOpenFileRequest {
            remote_url,
            branch: get("branch"),
            revision: get("revision"),
            file: get("file"),
            start_row: go_atoi(&get("start_row")),
            end_row: go_atoi(&get("end_row")),
            start_col: go_atoi(&get("start_col")),
            end_col: go_atoi(&get("end_col")),
        })));
    }

    Ok(EditorRequest::Empty)
}

/// `json.Unmarshal` into `map[string]string`.
fn parse_hostname_patterns(raw: &str) -> Result<(), String> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|error| go_unmarshal_error(raw.as_bytes(), &error))?;
    match value {
        serde_json::Value::Null => Ok(()),
        serde_json::Value::Object(entries) => {
            for (_, value) in entries {
                if let serde_json::Value::String(_) | serde_json::Value::Null = value {
                    continue;
                }
                return Err(format!(
                    "json: cannot unmarshal {} into Go value of type string",
                    json_kind(&value)
                ));
            }
            Ok(())
        }
        other => Err(format!(
            "json: cannot unmarshal {} into Go value of type map[string]string",
            json_kind(&other)
        )),
    }
}

/// `strconv.Atoi` ignoring the error (the zero value stays 0).
fn go_atoi(value: &str) -> i64 {
    value.parse::<i64>().unwrap_or(0)
}

/// `editorRequest.redirectURL` (`editor.go:203-211`).
async fn editor_redirect_url(
    backend: &dyn EditorBackend,
    request: &EditorRequest,
) -> Result<String, String> {
    match request {
        EditorRequest::Search(search) => search_redirect(backend, search).await,
        EditorRequest::OpenFile(open) => open_file_redirect(backend, open).await,
        EditorRequest::Empty => {
            Err("could not determine request type, missing ?search or ?remote_url".to_owned())
        }
    }
}

/// `editorRequest.searchRedirect` (`editor.go:86-143`).
async fn search_redirect(
    backend: &dyn EditorBackend,
    search: &EditorSearchRequest,
) -> Result<String, String> {
    let mut repo_filter = String::new();
    if !search.remote_url.is_empty() {
        let repo_name = backend
            .clone_url_to_repo_name(&search.remote_url)
            .await?
            .unwrap_or_default();
        if repo_name.is_empty() {
            return Err(format!(
                "Git remote URL {} not supported",
                go_quote(&search.remote_url)
            ));
        }
        repo_filter = format!("repo:{}$", quote_meta(&repo_name));
    }

    if !repo_filter.is_empty() && !search.revision.is_empty() {
        repo_filter.push('@');
        repo_filter.push_str(&search.revision);
    } else if !repo_filter.is_empty() && !search.branch.is_empty() {
        repo_filter.push('@');
        repo_filter.push_str(&search.branch);
    }

    let file_filter = if search.file.is_empty() {
        String::new()
    } else {
        format!("file:^{}$", quote_meta(&search.file))
    };

    let search_query = [repo_filter, file_filter, search.query.clone()]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");

    Ok(format!(
        "/search?patternType=literal&q={}",
        go_query_escape(&search_query)
    ))
}

/// `editorRequest.openFileRedirect` (`editor.go:145-199`).
async fn open_file_redirect(
    backend: &dyn EditorBackend,
    open: &EditorOpenFileRequest,
) -> Result<String, String> {
    let repo_name = backend
        .clone_url_to_repo_name(&open.remote_url)
        .await?
        .unwrap_or_default();
    if repo_name.is_empty() {
        return Err(format!(
            "git remote URL {} not supported",
            go_quote(&open.remote_url)
        ));
    }

    let (input_rev, be_explicit) = if open.revision.is_empty() {
        (open.branch.as_str(), false)
    } else {
        (open.revision.as_str(), true)
    };
    let rev = editor_rev(backend, &repo_name, input_rev, be_explicit).await;

    let path = go_path_clean(&format!(
        "/{repo_name}{rev}/-/blob/{file}",
        file = open.file
    ));
    let line = if open.start_row == open.end_row && open.start_col == open.end_col {
        format!("L{}", open.start_row + 1)
    } else {
        format!(
            "L{}:{}-{}:{}",
            open.start_row + 1,
            open.start_col + 1,
            open.end_row + 1,
            open.end_col + 1
        )
    };
    Ok(format!(
        "{}?{}",
        go_path_escape(&path),
        go_query_escape(&line)
    ))
}

/// `editorRev` (`editor.go:17-48`).
async fn editor_rev(
    backend: &dyn EditorBackend,
    repo_name: &str,
    rev: &str,
    be_explicit: bool,
) -> String {
    if be_explicit {
        return format!("@{rev}");
    }
    if rev == "HEAD" {
        return String::new();
    }
    if backend.get_by_name(repo_name).await.is_err() {
        return format!("@{rev}");
    }
    let default_branch = match backend.resolve_rev(repo_name, "").await {
        Ok(commit) => commit,
        Err(_) => return format!("@{rev}"),
    };
    let branch = match backend.resolve_rev(repo_name, rev).await {
        Ok(commit) => commit,
        Err(_) => return format!("@{rev}"),
    };
    if default_branch == branch {
        String::new()
    } else {
        format!("@{rev}")
    }
}

/// `regexp.QuoteMeta`.
fn quote_meta(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len());
    for ch in value.chars() {
        if "\\.+*?()|[]{}^$".contains(ch) {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted
}

// ---------------------------------------------------------------------------
// old-tree redirect (`router/old_tree_redirects.go`)
// ---------------------------------------------------------------------------

/// The inline old-tree handler: `path.Clean` + `revStr` + 301.
pub fn old_tree_redirect<B>(
    req: &Request<B>,
    repo: &str,
    rev: &str,
    path: &str,
) -> Response<BoxBody> {
    let mut cleaned = go_path_clean(path);
    if !cleaned.starts_with('/') && !cleaned.is_empty() {
        cleaned.insert(0, '/');
    }
    // `revStr`: empty or already `@`-prefixed revs are emitted as-is.
    let rev = if rev.is_empty() || rev.starts_with('@') {
        rev.to_owned()
    } else {
        format!("@{rev}")
    };
    let location = go_path_escape(&format!("/{repo}{rev}/-/tree/{cleaned}"));
    redirect(req, &location, 301)
}

// ---------------------------------------------------------------------------
// /-/debug offline subset (`debug.go`, `debugproxies/handler.go`)
// ---------------------------------------------------------------------------

const NO_K8S_BODY: &str =
    "Cluster information not available<br><br><a href=\"headers\">headers</a><br>";
const NO_ENDPOINTS_BODY: &str =
    "Instrumentation: no endpoints found<br><br><a href=\"headers\">headers</a><br>";

/// `deploy.IsDeployTypeKubernetes` (`internal/conf/deploy/deploytype.go:42-50`).
fn is_kubernetes(deploy_type: &str) -> bool {
    matches!(
        deploy_type,
        "k8s" | "cluster" | "kubernetes" | "helm" | "kustomize" | "k3s" | "appliance"
    )
}

/// `/-/debug` subrouter dispatch for the offline-replicable routes.
///
/// Not replicated (seams): the Grafana/Jaeger reverse proxies when their env
/// URLs are set, the Sentry tunnel's POST forwarding (non-POST answers the
/// baseline's 405), the OTLP adapter when an endpoint is configured, and the
/// populated per-service debug proxy index (`debugserver.Services`). Those
/// paths answer the unported 404. See the overview §4.
pub async fn debug<B>(req: &Request<B>, deps: &MoreDeps, env: &AppEnv) -> Response<BoxBody> {
    let path = req.uri().path();
    let sub = &path["/-/debug".len()..];
    // The baseline wraps every subroute in `debugproxies.AdminOnly`.
    let Some(admin) = deps.admin.as_deref() else {
        return not_found();
    };

    match sub {
        // `StrictSlash` on the subrouter's `Path` routes.
        "" => debug_redirect(req, "/-/debug/"),
        "/" => {
            let body = if is_kubernetes(&env.deploy_type) {
                NO_ENDPOINTS_BODY
            } else {
                NO_K8S_BODY
            };
            debug_admin(admin, deps.actor, body).await
        }
        "/grafana" => {
            if env_value("GRAFANA_SERVER_URL").is_some() {
                return not_found();
            }
            debug_admin(
                admin,
                deps.actor,
                "Grafana endpoint proxying: Please set env var GRAFANA_SERVER_URL",
            )
            .await
        }
        "/jaeger" => {
            if env_value("JAEGER_SERVER_URL").is_some() {
                return not_found();
            }
            debug_admin(
                admin,
                deps.actor,
                "Jaeger endpoint proxying: Please set env var JAEGER_SERVER_URL",
            )
            .await
        }
        // `StrictSlash` on the `Path` routes above; with the env vars set the
        // routes are `PathPrefix`es, which do not redirect.
        "/grafana/" => {
            if env_value("GRAFANA_SERVER_URL").is_some() {
                return not_found();
            }
            debug_redirect(req, "/-/debug/grafana")
        }
        "/jaeger/" => {
            if env_value("JAEGER_SERVER_URL").is_some() {
                return not_found();
            }
            debug_redirect(req, "/-/debug/jaeger")
        }
        "/sentry_tunnel/" => debug_redirect(req, "/-/debug/sentry_tunnel"),
        "/sentry_tunnel" => {
            if req.method() != http::Method::POST {
                status_only(405)
            } else {
                // Seam: forwarding to Sentry needs the site config DSN and an
                // outbound HTTP client.
                not_found()
            }
        }
        "/proxies" | "/proxies/" => debug_proxy_missing(admin, deps.actor, env).await,
        // `PathPrefix("/proxies")` is unanchored in the baseline.
        sub if sub.starts_with("/proxies") => debug_proxy_missing(admin, deps.actor, env).await,
        sub if sub.starts_with("/otlp") => {
            if env_value("OTEL_EXPORTER_OTLP_ENDPOINT").is_some() {
                // Seam: the real adapter forwards to the configured backend.
                return not_found();
            }
            debug_admin(
                admin,
                deps.actor,
                "OpenTelemetry protocol tunnel: please configure an exporter endpoint with OTEL_EXPORTER_OTLP_ENDPOINT",
            )
            .await
        }
        // `/-/debug` itself, `/-/debugxyz`, and the populated proxy index.
        _ => not_found(),
    }
}

/// `mux`'s `StrictSlash` redirect: a 301 to the toggled path, carrying the
/// request's query string (as `Router.redirectHandler` does).
fn debug_redirect<B>(req: &Request<B>, target: &str) -> Response<BoxBody> {
    let location = match req.uri().query() {
        Some(query) if !query.is_empty() => format!("{target}?{query}"),
        _ => target.to_owned(),
    };
    redirect(req, &location, 301)
}

async fn debug_admin(admin: &dyn SiteAdminCheck, actor: Actor, body: &str) -> Response<BoxBody> {
    if let Err(message) = admin.check_site_admin(actor).await {
        return http_error(403, &message);
    }
    crate::middleware::text_response(StatusCode::OK, "text/plain; charset=utf-8", body.to_owned())
}

async fn debug_proxy_missing(
    admin: &dyn SiteAdminCheck,
    actor: Actor,
    env: &AppEnv,
) -> Response<BoxBody> {
    if let Err(message) = admin.check_site_admin(actor).await {
        return http_error(403, &message);
    }
    go_error(404, "proxy endpoint missing", env.insecure_dev)
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_tree_cleans_the_path() {
        let request = Request::builder()
            .method("GET")
            .uri("/github.com/foo/bar/.tree/./src/../README.md")
            .body(())
            .expect("request");
        let response = old_tree_redirect(&request, "github.com/foo/bar", "", "/./src/../README.md");
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers()[header::LOCATION],
            "/github.com/foo/bar/-/tree/README.md"
        );
    }

    #[test]
    fn old_tree_keeps_explicit_revs() {
        let request = Request::builder()
            .method("GET")
            .uri("/github.com/foo/bar@main/.tree/a")
            .body(())
            .expect("request");
        let response = old_tree_redirect(&request, "github.com/foo/bar", "@main", "/a");
        assert_eq!(
            response.headers()[header::LOCATION],
            "/github.com/foo/bar@main/-/tree/a"
        );
    }

    #[test]
    fn export_decode_error_texts() {
        assert_eq!(decode_export_request(b"").expect_err("empty"), "EOF");
        assert_eq!(
            decode_export_request(b"[]").expect_err("array"),
            "json: cannot unmarshal array into Go value of type oneclickexport.ExportRequest"
        );
        assert_eq!(
            decode_export_request(br#"{"includeSiteConfig":"x"}"#).expect_err("bool"),
            "json: cannot unmarshal string into Go struct field ExportRequest.includeSiteConfig of type bool"
        );
        assert_eq!(
            decode_export_request(br#"{"dbQueries":[{"count":true}]}"#).expect_err("count"),
            "json: cannot unmarshal bool into Go struct field DBQueryRequest.dbQueries.count of type oneclickexport.Limit"
        );
        let request = decode_export_request(
            br#"{"includeSiteConfig":true,"dbQueries":[{"tableName":"external_services","count":5}]}"#,
        )
        .expect("valid");
        assert!(request.include_site_config);
        assert_eq!(
            request.db_queries,
            vec![OneClickExportDbQuery {
                table_name: "external_services".to_owned(),
                count: 5,
            }]
        );
    }

    #[test]
    fn hostname_pattern_errors() {
        assert_eq!(
            parse_hostname_patterns("[]").expect_err("array"),
            "json: cannot unmarshal array into Go value of type map[string]string"
        );
        assert_eq!(
            parse_hostname_patterns(r#"{"a":1}"#).expect_err("value"),
            "json: cannot unmarshal number into Go value of type string"
        );
        assert_eq!(
            parse_hostname_patterns("{").expect_err("truncated"),
            "unexpected end of JSON input"
        );
        assert!(parse_hostname_patterns("null").is_ok());
        assert!(parse_hostname_patterns(r#"{"a":"b"}"#).is_ok());
    }

    struct FakeEditor;

    #[async_trait]
    impl EditorBackend for FakeEditor {
        async fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<Option<String>, String> {
            if clone_url.contains("github.com/foo/bar") {
                Ok(Some("github.com/foo/bar".to_owned()))
            } else {
                Ok(None)
            }
        }

        async fn get_by_name(&self, _repo: &str) -> Result<(), String> {
            Err("repo not found".to_owned())
        }

        async fn resolve_rev(&self, _repo: &str, _rev: &str) -> Result<String, String> {
            Err("rev not found".to_owned())
        }
    }

    #[tokio::test]
    async fn editor_search_url_matches_go() {
        let search = EditorSearchRequest {
            query: "foo bar".to_owned(),
            remote_url: "https://github.com/foo/bar.git".to_owned(),
            branch: "dev".to_owned(),
            revision: "main".to_owned(),
            file: "src/a.go".to_owned(),
        };
        let url = search_redirect(&FakeEditor, &search).await.expect("url");
        assert_eq!(
            url,
            "/search?patternType=literal&q=repo%3Agithub%5C.com%2Ffoo%2Fbar%24%40main+file%3A%5Esrc%2Fa%5C.go%24+foo+bar"
        );
    }

    #[tokio::test]
    async fn editor_open_file_url_matches_go() {
        let open = EditorOpenFileRequest {
            remote_url: "https://github.com/foo/bar.git".to_owned(),
            revision: "main".to_owned(),
            file: "src/a.go".to_owned(),
            start_row: 1,
            end_row: 3,
            start_col: 2,
            end_col: 4,
            ..EditorOpenFileRequest::default()
        };
        let url = open_file_redirect(&FakeEditor, &open).await.expect("url");
        assert_eq!(url, "/github.com/foo/bar@main/-/blob/src/a.go?L2%3A3-4%3A5");
    }
}
