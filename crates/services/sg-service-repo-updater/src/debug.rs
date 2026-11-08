#![forbid(unsafe_code)]

//! Per-service debug endpoint (`SRC_PROF_HTTP`), equivalent to the baseline
//! `internal/debugserver` plus the repo-updater-specific
//! `/repo-updater-state` dumper and `/manual-purge`.
//!
//! The Go server also exposes pprof, `/vars` and `/debug/requests`, which
//! this pilot does not implement (it serves Prometheus `/metrics`, which
//! monitoring scrapes).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use http::{header, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use sg_db::external_service_sync_jobs::SyncJobsStore;

use crate::metrics::Metrics;
use crate::purge::PurgeWorker;
use crate::scheduler::UpdateScheduler;

/// Baseline `debugserver` default (`SRC_PROF_HTTP`).
pub const DEFAULT_ADDR: &str = ":6060";

const INDEX_HTML: &str =
    "<!DOCTYPE html>\n<html><head><title>Sourcegraph repo-updater debug</title></head>\
<body><a href=\"metrics\">Metrics</a><br>\
<a href=\"repo-updater-state?format=json\">Repo Updater State</a><br></body></html>\n";

/// The state the debug endpoints read.
pub struct DebugContext {
    pub scheduler: Arc<UpdateScheduler>,
    pub sync_jobs: Arc<SyncJobsStore>,
    pub purge: Arc<PurgeWorker>,
    /// Metric handles, including the scrape-time state gauges.
    pub metrics: Arc<Metrics>,
    /// Structured-concurrency tracker for manual purges.
    pub tasks: TaskTracker,
}

/// Bind `addr` and serve the debug HTTP surface until `shutdown` is cancelled.
/// A bind error is logged and swallowed.
pub async fn serve(addr: &str, context: Arc<DebugContext>, shutdown: CancellationToken) {
    let listener = match sg_http::listen(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::warn!(addr, error = %err, "debug server failed to listen");
            return;
        }
    };
    tracing::info!(addr, "debug server listening");
    if let Err(err) = sg_http::serve_http(listener, handler(context), shutdown).await {
        tracing::error!(addr, error = %err, "debug server failed");
    }
}

pub fn handler(context: Arc<DebugContext>) -> sg_http::HttpHandler {
    Arc::new(move |req: Request<Incoming>| {
        let context = Arc::clone(&context);
        Box::pin(async move { route(&context, req).await })
    })
}

async fn route(context: &DebugContext, req: Request<Incoming>) -> Response<tonic::body::BoxBody> {
    // Owned so the request can be moved into `manual_purge` for its body.
    let path = req.uri().path().to_owned();
    match path.as_str() {
        "/metrics" => {
            // The baseline registers the state families as `GaugeFunc`s, so
            // each scrape recomputes them from the database.
            context
                .metrics
                .refresh_state(context.sync_jobs.pool())
                .await;
            text(
                StatusCode::OK,
                sg_metrics::CONTENT_TYPE,
                sg_metrics::render_default(),
            )
        }
        "/repo-updater-state" => {
            // `repoUpdaterStatsHandler`: the `repos` dumper renders an HTML
            // table unless `format=json` is requested explicitly.
            let query = req.uri().query().unwrap_or_default();
            let want_dumper = query_param(query, "dumper").unwrap_or_default();
            let want_format = query_param(query, "format").unwrap_or_default();
            if !matches!(want_dumper.as_str(), "" | "repos") {
                // `dumps` stays nil when no dumper matches: `json.MarshalIndent`
                // then writes `null`.
                return text(StatusCode::OK, "application/json", "null");
            }
            let dump = state_dump(&context.scheduler, &context.sync_jobs).await;
            if want_format != "json" {
                // `html/template.Execute` writes the body directly; net/http
                // sniffs `text/html; charset=utf-8` from `<!doctype html>`.
                return text(
                    StatusCode::OK,
                    "text/html; charset=utf-8",
                    render_state_html(&dump),
                );
            }
            // `json.MarshalIndent` output: same field order, `null` for nil
            // slices, and no trailing newline.
            text(StatusCode::OK, "application/json", state_json(&dump))
        }
        "/manual-purge" => manual_purge(context, req).await,
        "/" => text(StatusCode::OK, "text/html; charset=utf-8", INDEX_HTML),
        // Go's `http.NotFound`.
        _ => go_error(StatusCode::NOT_FOUND, "404 page not found"),
    }
}

/// `manualPurgeHandler`. `r.FormValue` reads the URL query and, for
/// `POST`/`PUT`/`PATCH` with an urlencoded body, the body too (body values
/// come first, like `net/http`'s `r.Form`).
async fn manual_purge(
    context: &DebugContext,
    req: Request<Incoming>,
) -> Response<tonic::body::BoxBody> {
    let params = form_params(req).await;
    let limit_param = form_value(&params, "limit").unwrap_or_default();
    // `r.FormValue("limit")` returns "" when the parameter is absent, and
    // `strconv.Atoi("")` fails; both surface as `invalid limit: ...`.
    let limit = match go_atoi(&limit_param) {
        Ok(limit) => limit,
        Err(err) => return go_error(StatusCode::BAD_REQUEST, &format!("invalid limit: {err}")),
    };
    if limit <= 0 {
        return go_error(StatusCode::BAD_REQUEST, "limit must be greater than 0");
    }
    if limit > 10000 {
        return go_error(StatusCode::BAD_REQUEST, "limit must be less than 10000");
    }

    let mut per_second = 1.0;
    if let Some(param) = form_value(&params, "perSecond") {
        if !param.is_empty() {
            per_second = match go_parse_float(&param) {
                Ok(value) => value,
                Err(err) => {
                    return go_error(
                        StatusCode::BAD_REQUEST,
                        &format!("invalid per second rate limit: {err}"),
                    );
                }
            };
            if per_second <= 0.1 {
                return go_error(
                    StatusCode::BAD_REQUEST,
                    &format!("invalid per second rate limit. Must be > 0.1, got {per_second:.6}"),
                );
            }
        }
    }

    match context
        .purge
        .purge_oldest_repos(limit, per_second, &context.tasks)
    {
        Ok(()) => text(
            StatusCode::OK,
            "text/plain; charset=utf-8",
            format!("manual purge started with limit of {limit} and rate of {per_second:.6}"),
        ),
        Err(err) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            "text/plain; charset=utf-8",
            format!("starting manual purge: {err}\n"),
        ),
    }
}

/// `http.Error`: text/plain, `X-Content-Type-Options: nosniff`, and a
/// trailing newline.
fn go_error(status: StatusCode, message: &str) -> Response<tonic::body::BoxBody> {
    let mut response = text(status, "text/plain; charset=utf-8", format!("{message}\n"));
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    response
}

/// `UpdateScheduler.DebugDump` for the `repos` dumper, including
/// `ExternalServices().GetSyncJobs`. The structs below mirror the Go types
/// (`struct{Name; UpdateQueue; Schedule; SyncJobs}` and its members) so the
/// JSON field order and `null`-for-nil-slice behavior are identical.
#[derive(serde::Serialize)]
struct RepoDump {
    #[serde(rename = "ID")]
    id: i32,
    #[serde(rename = "Name")]
    name: String,
}

#[derive(serde::Serialize)]
struct QueueEntryDump {
    #[serde(rename = "Repo")]
    repo: RepoDump,
    #[serde(rename = "Priority")]
    priority: i32,
    #[serde(rename = "Seq")]
    seq: u64,
    #[serde(rename = "Updating")]
    updating: bool,
}

#[derive(serde::Serialize)]
struct ScheduleEntryDump {
    #[serde(rename = "Repo")]
    repo: RepoDump,
    // `time.Duration` marshals as an integer number of nanoseconds; the HTML
    // page prints Go's duration string instead.
    #[serde(rename = "Interval", serialize_with = "serialize_nanos")]
    interval: std::time::Duration,
    #[serde(rename = "Due", serialize_with = "serialize_time")]
    due: SystemTime,
}

#[derive(serde::Serialize)]
struct SyncJobDump {
    #[serde(rename = "ID")]
    id: i64,
    #[serde(rename = "State")]
    state: String,
    #[serde(rename = "FailureMessage")]
    failure_message: String,
    #[serde(rename = "QueuedAt", serialize_with = "serialize_optional_time")]
    queued_at: Option<sg_db::OffsetDateTime>,
    #[serde(rename = "StartedAt", serialize_with = "serialize_optional_time")]
    started_at: Option<sg_db::OffsetDateTime>,
    #[serde(rename = "FinishedAt", serialize_with = "serialize_optional_time")]
    finished_at: Option<sg_db::OffsetDateTime>,
    #[serde(rename = "ProcessAfter", serialize_with = "serialize_optional_time")]
    process_after: Option<sg_db::OffsetDateTime>,
    #[serde(rename = "NumResets")]
    num_resets: i32,
    #[serde(rename = "ExternalServiceID")]
    external_service_id: i64,
    #[serde(rename = "NumFailures")]
    num_failures: i32,
    #[serde(rename = "Cancel")]
    cancel: bool,
    #[serde(rename = "ReposSynced")]
    repos_synced: i32,
    #[serde(rename = "RepoSyncErrors")]
    repo_sync_errors: i32,
    #[serde(rename = "ReposAdded")]
    repos_added: i32,
    #[serde(rename = "ReposDeleted")]
    repos_deleted: i32,
    #[serde(rename = "ReposModified")]
    repos_modified: i32,
    #[serde(rename = "ReposUnmodified")]
    repos_unmodified: i32,
}

#[derive(serde::Serialize)]
struct StateDump {
    #[serde(rename = "Name")]
    name: &'static str,
    #[serde(rename = "UpdateQueue")]
    update_queue: Option<Vec<QueueEntryDump>>,
    #[serde(rename = "Schedule")]
    schedule: Option<Vec<ScheduleEntryDump>>,
    #[serde(rename = "SyncJobs")]
    sync_jobs: Option<Vec<SyncJobDump>>,
}

/// Go marshals nil slices as `null`; both dumper loops start from nil.
fn non_empty<T>(values: Vec<T>) -> Option<Vec<T>> {
    (!values.is_empty()).then_some(values)
}

async fn state_dump(scheduler: &UpdateScheduler, jobs: &SyncJobsStore) -> StateDump {
    let queue: Vec<QueueEntryDump> = scheduler
        .debug_queue()
        .into_iter()
        .map(|update| QueueEntryDump {
            repo: RepoDump {
                id: update.repo.id,
                name: update.repo.name,
            },
            priority: update.priority as i32,
            seq: update.seq,
            updating: update.updating,
        })
        .collect();
    let schedule: Vec<ScheduleEntryDump> = scheduler
        .debug_schedule()
        .into_iter()
        .map(|entry| ScheduleEntryDump {
            repo: RepoDump {
                id: entry.repo.id,
                name: entry.repo.name,
            },
            interval: entry.interval,
            due: entry.due,
        })
        .collect();

    // A failed query leaves `SyncJobs` nil in the baseline, which marshals
    // as `null`; so does an empty result (`basestore.NewSliceScanner`
    // returns a nil slice).
    let sync_jobs = match jobs.list_sync_jobs().await {
        Ok(jobs) => non_empty(
            jobs.into_iter()
                .map(|job| SyncJobDump {
                    id: job.id as i64,
                    state: job.state,
                    failure_message: job.failure_message.unwrap_or_default(),
                    queued_at: job.queued_at,
                    started_at: job.started_at,
                    finished_at: job.finished_at,
                    process_after: job.process_after,
                    num_resets: job.num_resets,
                    external_service_id: job.external_service_id,
                    num_failures: job.num_failures,
                    cancel: job.cancel,
                    repos_synced: job.repos_synced,
                    repo_sync_errors: job.repo_sync_errors,
                    repos_added: job.repos_added,
                    repos_deleted: job.repos_deleted,
                    repos_modified: job.repos_modified,
                    repos_unmodified: job.repos_unmodified,
                })
                .collect(),
        ),
        Err(err) => {
            tracing::warn!(error = %err, "getting external service sync jobs for debug page");
            None
        }
    };

    StateDump {
        name: "repos",
        update_queue: non_empty(queue),
        schedule: non_empty(schedule),
        sync_jobs,
    }
}

/// `json.MarshalIndent` writes no trailing newline.
fn state_json(dump: &StateDump) -> String {
    serde_json::to_string_pretty(&[dump]).expect("serialize debug dump")
}

/// `time.Duration` marshals as an integer number of nanoseconds.
fn serialize_nanos<S: serde::Serializer>(
    value: &std::time::Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_i64(value.as_nanos() as i64)
}

/// `time.Time` marshals with `FormatRFC3339Nano`.
fn serialize_time<S: serde::Serializer>(
    value: &SystemTime,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format_rfc3339(*value))
}

fn serialize_optional_time<S: serde::Serializer>(
    value: &Option<sg_db::OffsetDateTime>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format_optional_time(*value))
}

/// Go's zero `time.Time` renders as `0001-01-01T00:00:00Z`.
fn format_optional_time(time: Option<sg_db::OffsetDateTime>) -> String {
    match time {
        Some(time) => format_rfc3339(SystemTime::from(time)),
        None => "0001-01-01T00:00:00Z".to_owned(),
    }
}

/// Query-string parameter, like `url.Values.Get`.
fn query_param(query: &str, name: &str) -> Option<String> {
    form_value(&parse_form(query), name)
}

/// First value for `name`, matching `url.Values.Get`.
fn form_value(params: &[(String, String)], name: &str) -> Option<String> {
    params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}

/// `r.FormValue` input: `net/http` puts urlencoded POST/PUT/PATCH body values
/// before the URL query values in `r.Form`.
async fn form_params(req: Request<Incoming>) -> Vec<(String, String)> {
    let mut params = Vec::new();
    let parses_body = matches!(*req.method(), Method::POST | Method::PUT | Method::PATCH)
        && req
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/x-www-form-urlencoded"));
    let query = req.uri().query().unwrap_or_default().to_owned();
    if parses_body {
        // `ParseForm` caps bodies at 10 MiB; debug requests are far smaller.
        match req.into_body().collect().await {
            Ok(body) => params.extend(parse_form(&String::from_utf8_lossy(&body.to_bytes()))),
            Err(err) => tracing::warn!(error = %err, "reading manual purge form body"),
        }
    }
    params.extend(parse_form(&query));
    params
}

/// `url.ParseQuery` for the subset `FormValue` sees: `&`-separated pairs,
/// `+` as space and `%XX` escapes.
fn parse_form(input: &str) -> Vec<(String, String)> {
    input
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (form_unescape(key), form_unescape(value))
        })
        .collect()
}

fn form_unescape(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let high = (bytes[i + 1] as char).to_digit(16);
                let low = (bytes[i + 2] as char).to_digit(16);
                match (high, low) {
                    (Some(high), Some(low)) => {
                        out.push((high * 16 + low) as u8);
                        i += 2;
                    }
                    _ => out.push(b'%'),
                }
            }
            byte => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `strconv.Atoi` error strings, so `/manual-purge`'s messages match the
/// baseline byte for byte.
fn go_atoi(value: &str) -> Result<i64, String> {
    match value.parse::<i64>() {
        Ok(parsed) => Ok(parsed),
        Err(err) => {
            let reason = match err.kind() {
                std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                    "value out of range"
                }
                _ => "invalid syntax",
            };
            Err(format!("strconv.Atoi: parsing {value:?}: {reason}"))
        }
    }
}

/// `strconv.ParseFloat` error strings.
fn go_parse_float(value: &str) -> Result<f64, String> {
    match value.parse::<f64>() {
        Ok(parsed) => Ok(parsed),
        Err(_) => Err(format!(
            "strconv.ParseFloat: parsing {value:?}: invalid syntax"
        )),
    }
}

/// The `text/template` body of `state.html.tmpl`, shared verbatim with the
/// baseline so the rendered whitespace cannot drift.
const STATE_HTML_TEMPLATE: &str = include_str!("state.html.tmpl");

/// One row of a `{{range}}` in `state.html.tmpl`.
enum HtmlItem<'a> {
    Schedule(&'a ScheduleEntryDump),
    Queue(&'a QueueEntryDump),
    Job(&'a SyncJobDump),
}

/// Render `state.html.tmpl` like `html/template.Execute`: literal text is
/// copied verbatim, `{{range}}` bodies are repeated per item (or the
/// `{{else}}` body when the slice is nil), and actions are HTML-escaped in
/// text context.
fn render_state_html(dump: &StateDump) -> String {
    let mut output = String::new();
    render_template(&mut output, STATE_HTML_TEMPLATE, None, dump);
    output
}

fn render_template(
    output: &mut String,
    template: &str,
    item: Option<&HtmlItem<'_>>,
    dump: &StateDump,
) {
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            output.push_str(&rest[start..]);
            return;
        };
        let action = after[..end].trim();
        let tail = &after[end + 2..];

        if let Some(name) = action.strip_prefix("range $schedulerDump.") {
            let (body, else_body, remainder) = split_range(tail);
            match name {
                "Schedule" => match &dump.schedule {
                    Some(entries) => {
                        for entry in entries {
                            render_template(output, body, Some(&HtmlItem::Schedule(entry)), dump);
                        }
                    }
                    None => render_else(output, else_body, dump),
                },
                "UpdateQueue" => match &dump.update_queue {
                    Some(entries) => {
                        for entry in entries {
                            render_template(output, body, Some(&HtmlItem::Queue(entry)), dump);
                        }
                    }
                    None => render_else(output, else_body, dump),
                },
                "SyncJobs" => match &dump.sync_jobs {
                    Some(entries) => {
                        for entry in entries {
                            render_template(output, body, Some(&HtmlItem::Job(entry)), dump);
                        }
                    }
                    None => render_else(output, else_body, dump),
                },
                _ => {}
            }
            rest = remainder;
            continue;
        }

        output.push_str(&render_action(action, item));
        rest = tail;
    }
    output.push_str(rest);
}

fn render_else(output: &mut String, else_body: Option<&str>, dump: &StateDump) {
    if let Some(else_body) = else_body {
        render_template(output, else_body, None, dump);
    }
}

/// Split the text after a `{{range}}` action into (body, else body, rest).
fn split_range(rest: &str) -> (&str, Option<&str>, &str) {
    let end_start = rest.find("{{end}}").expect("range end");
    match rest[..end_start].find("{{else}}") {
        Some(else_start) => (
            &rest[..else_start],
            Some(&rest[else_start + "{{else}}".len()..end_start]),
            &rest[end_start + "{{end}}".len()..],
        ),
        None => (
            &rest[..end_start],
            None,
            &rest[end_start + "{{end}}".len()..],
        ),
    }
}

fn render_action(action: &str, item: Option<&HtmlItem<'_>>) -> String {
    let Some(item) = item else {
        // `{{$schedulerDump := .}}` emits nothing.
        return String::new();
    };
    match item {
        HtmlItem::Schedule(entry) => match action {
            ".Repo.ID" => entry.repo.id.to_string(),
            ".Repo.Name" => html_escape(&entry.repo.name),
            "truncateDuration .Interval" => html_escape(&go_duration_string(entry.interval)),
            ".Due.Format \"Mon, 02 Jan 2006 15:04:05 MST\"" => {
                html_escape(&go_time_format(entry.due))
            }
            _ => String::new(),
        },
        HtmlItem::Queue(entry) => match action {
            ".Repo.ID" => entry.repo.id.to_string(),
            ".Repo.Name" => html_escape(&entry.repo.name),
            ".Updating" => entry.updating.to_string(),
            ".Priority" => entry.priority.to_string(),
            ".Seq" => entry.seq.to_string(),
            _ => String::new(),
        },
        HtmlItem::Job(job) => match action {
            ".ExternalServiceID" => job.external_service_id.to_string(),
            ".State" => html_escape(&job.state),
            ".StartedAt" => html_escape(&go_optional_time_string(job.started_at)),
            ".FinishedAt" => html_escape(&go_optional_time_string(job.finished_at)),
            ".FailureMessage" => html_escape(&job.failure_message),
            _ => String::new(),
        },
    }
}

/// `html/template`'s text-context escape table.
fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\0' => escaped.push('\u{FFFD}'),
            '"' => escaped.push_str("&#34;"),
            '&' => escaped.push_str("&amp;"),
            '\'' => escaped.push_str("&#39;"),
            '+' => escaped.push_str("&#43;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            character => escaped.push(character),
        }
    }
    escaped
}

/// `truncateDuration` applied by the template: `d.Truncate(time.Second)`
/// followed by `time.Duration.String()`.
fn go_duration_string(duration: std::time::Duration) -> String {
    let seconds = duration.as_secs();
    if seconds == 0 {
        return "0s".to_owned();
    }
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}h{minutes}m{seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds}s")
    } else {
        format!("{seconds}s")
    }
}

/// `.Due.Format "Mon, 02 Jan 2006 15:04:05 MST"` (UTC).
fn go_time_format(time: SystemTime) -> String {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    let (seconds, _) = unix_parts(time);
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let weekday = WEEKDAYS[(days + 4).rem_euclid(7) as usize];
    format!(
        "{weekday}, {day:02} {} {year:04} {:02}:{:02}:{:02} UTC",
        MONTHS[(month - 1) as usize],
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60
    )
}

/// `time.Time.String()` in UTC: `2006-01-02 15:04:05.999999999 -0700 MST`.
fn go_time_string(time: SystemTime) -> String {
    let (seconds, nanos) = unix_parts(time);
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let mut rendered = format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60
    );
    if nanos > 0 {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        rendered.push('.');
        rendered.push_str(&fraction);
    }
    rendered.push_str(" +0000 UTC");
    rendered
}

/// `{{.StartedAt}}` on a `time.Time`; the zero value matches Go.
fn go_optional_time_string(time: Option<sg_db::OffsetDateTime>) -> String {
    match time {
        Some(time) => go_time_string(SystemTime::from(time)),
        None => "0001-01-01 00:00:00 +0000 UTC".to_owned(),
    }
}

/// Seconds since the Unix epoch and the sub-second nanoseconds.
fn unix_parts(time: SystemTime) -> (i64, u32) {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => (after.as_secs() as i64, after.subsec_nanos()),
        Err(before) => {
            let duration = before.duration();
            let mut seconds = -(duration.as_secs() as i64);
            let mut nanos = -(duration.subsec_nanos() as i64);
            if nanos < 0 {
                seconds -= 1;
                nanos += 1_000_000_000;
            }
            (seconds, nanos as u32)
        }
    }
}

/// `FormatRFC3339Nano`-compatible rendering (UTC, trailing zeros trimmed).
pub fn format_rfc3339(time: SystemTime) -> String {
    let (seconds, nanos) = unix_parts(time);
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = second_of_day / 3600;
    let minute = (second_of_day % 3600) / 60;
    let second = second_of_day % 60;
    let mut rendered = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if nanos > 0 {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        rendered.push('.');
        rendered.push_str(&fraction);
    }
    rendered.push('Z');
    rendered
}

/// Howard Hinnant's `civil_from_days` (days since 1970-01-01).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m as u32, d as u32)
}

fn text(
    status: StatusCode,
    content_type: &str,
    body: impl Into<Bytes>,
) -> Response<tonic::body::BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(body.into())));
    *response.status_mut() = status;
    if let Ok(value) = header::HeaderValue::from_str(content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn formats_rfc3339_nano() {
        assert_eq!(format_rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_rfc3339(UNIX_EPOCH + Duration::new(1_700_000_000, 123_000_000)),
            "2023-11-14T22:13:20.123Z"
        );
        assert_eq!(
            format_rfc3339(UNIX_EPOCH + Duration::new(951_782_400, 0)),
            "2000-02-29T00:00:00Z"
        );
    }

    #[test]
    fn parses_query_parameters() {
        assert_eq!(
            query_param("dumper=repos&format=json", "format").as_deref(),
            Some("json")
        );
        assert_eq!(query_param("dumper=repos", "format"), None);
    }

    #[test]
    fn go_numeric_errors_match_the_baseline() {
        assert_eq!(
            go_atoi("abc").unwrap_err(),
            "strconv.Atoi: parsing \"abc\": invalid syntax"
        );
        assert_eq!(
            go_atoi("").unwrap_err(),
            "strconv.Atoi: parsing \"\": invalid syntax"
        );
        assert_eq!(
            go_atoi("999999999999999999999999").unwrap_err(),
            "strconv.Atoi: parsing \"999999999999999999999999\": value out of range"
        );
        assert_eq!(
            go_parse_float("x").unwrap_err(),
            "strconv.ParseFloat: parsing \"x\": invalid syntax"
        );
        assert_eq!(go_atoi("42").unwrap(), 42);
        assert_eq!(go_parse_float("1.5").unwrap(), 1.5);
    }

    #[test]
    fn missing_times_use_the_go_zero_value() {
        assert_eq!(format_optional_time(None), "0001-01-01T00:00:00Z");
    }

    #[test]
    fn form_values_unescape_like_net_url() {
        assert_eq!(form_unescape("a+b%20c"), "a b c");
        assert_eq!(form_unescape("%2B"), "+");
        assert_eq!(form_unescape("100%"), "100%");
        assert_eq!(
            parse_form("limit=1&perSecond=0.5"),
            vec![
                ("limit".to_owned(), "1".to_owned()),
                ("perSecond".to_owned(), "0.5".to_owned())
            ]
        );
    }

    fn system_time(seconds: i64, nanos: u32) -> SystemTime {
        UNIX_EPOCH + Duration::new(seconds as u64, nanos)
    }

    /// The same synthetic data `state_html_golden.go` renders with the real
    /// `html/template`, including escaping and Go time/duration formats.
    fn populated_dump() -> StateDump {
        let repo = |id, name: &str| RepoDump {
            id,
            name: name.to_owned(),
        };
        StateDump {
            name: "repos",
            schedule: Some(vec![
                ScheduleEntryDump {
                    repo: repo(1, "example.com/owner/repo"),
                    interval: Duration::from_secs(46 * 60 + 37),
                    due: system_time(1794129846, 0),
                },
                ScheduleEntryDump {
                    repo: repo(2, "a<b&c\"d'e+f.com/r"),
                    interval: Duration::from_secs(3600),
                    due: system_time(1794130245, 0),
                },
                ScheduleEntryDump {
                    repo: repo(3, "unicode.example.com/é/r"),
                    interval: Duration::ZERO,
                    due: system_time(1798761600, 0),
                },
                ScheduleEntryDump {
                    repo: repo(4, "seconds.example.com/r"),
                    interval: Duration::from_secs(59),
                    due: system_time(1794182399, 0),
                },
                ScheduleEntryDump {
                    repo: repo(5, "long.example.com/r"),
                    interval: Duration::from_secs(2 * 3600 + 3 * 60 + 4),
                    due: system_time(1794096000, 0),
                },
            ]),
            update_queue: Some(vec![
                QueueEntryDump {
                    repo: repo(5, "queue.example.com/a<b>"),
                    updating: true,
                    priority: 0,
                    seq: 0,
                },
                QueueEntryDump {
                    repo: repo(6, "queue.example.com/c&d"),
                    updating: false,
                    priority: -3,
                    seq: 1_234_567_890_123,
                },
            ]),
            sync_jobs: Some(vec![
                SyncJobDump {
                    id: 42,
                    state: "completed".to_owned(),
                    failure_message: "ok".to_owned(),
                    queued_at: None,
                    started_at: Some(sg_db::system_time_to_offset(system_time(1794128400, 0))),
                    finished_at: Some(sg_db::system_time_to_offset(system_time(
                        1794128462,
                        987_654_321,
                    ))),
                    process_after: None,
                    num_resets: 0,
                    external_service_id: 7,
                    num_failures: 0,
                    cancel: false,
                    repos_synced: 0,
                    repo_sync_errors: 0,
                    repos_added: 0,
                    repos_deleted: 0,
                    repos_modified: 0,
                    repos_unmodified: 0,
                },
                SyncJobDump {
                    id: 43,
                    state: "errored".to_owned(),
                    failure_message: "boom <script>&'\" +".to_owned(),
                    queued_at: None,
                    started_at: None,
                    finished_at: None,
                    process_after: None,
                    num_resets: 0,
                    external_service_id: 9,
                    num_failures: 0,
                    cancel: false,
                    repos_synced: 0,
                    repo_sync_errors: 0,
                    repos_added: 0,
                    repos_deleted: 0,
                    repos_modified: 0,
                    repos_unmodified: 0,
                },
            ]),
        }
    }

    /// Byte-for-byte against `html/template` output captured from the
    /// baseline template (see `testdata/state_html_populated.html`).
    #[test]
    fn renders_populated_state_html_like_the_baseline() {
        assert_eq!(
            render_state_html(&populated_dump()),
            include_str!("../testdata/state_html_populated.html")
        );
    }

    /// The `{{else}}` branches render when the slices are nil.
    #[test]
    fn renders_empty_state_html_like_the_baseline() {
        let dump = StateDump {
            name: "repos",
            update_queue: None,
            schedule: None,
            sync_jobs: None,
        };
        assert_eq!(
            render_state_html(&dump),
            include_str!("../testdata/state_html_empty.html")
        );
    }

    /// The JSON shape mirrors `json.MarshalIndent`: struct field order and
    /// `null` for nil slices, with no trailing newline.
    #[test]
    fn empty_state_dump_marshals_like_the_baseline() {
        let dump = StateDump {
            name: "repos",
            update_queue: None,
            schedule: None,
            sync_jobs: None,
        };
        let text = serde_json::to_string_pretty(&[dump]).expect("serialize");
        assert_eq!(
            text,
            "[
  {
    \"Name\": \"repos\",
    \"UpdateQueue\": null,\n    \"Schedule\": null,\n    \"SyncJobs\": null
  }
]"
        );
    }
}
