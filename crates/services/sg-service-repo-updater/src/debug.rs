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

use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use sg_db::external_service_sync_jobs::SyncJobsStore;

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
        Box::pin(async move { route(&context, &req).await })
    })
}

async fn route(context: &DebugContext, req: &Request<Incoming>) -> Response<tonic::body::BoxBody> {
    match req.uri().path() {
        "/metrics" => text(
            StatusCode::OK,
            sg_metrics::CONTENT_TYPE,
            sg_metrics::render_default(),
        ),
        "/repo-updater-state" => {
            // `repoUpdaterStatsHandler`: JSON is the only non-HTML rendering
            // implemented here; `format=json` is what the baseline's UI and
            // debug tooling request.
            let query = req.uri().query().unwrap_or_default();
            // The baseline renders an HTML table unless `format=json`;
            // serving the JSON body in both cases keeps the endpoint
            // machine-readable (documented divergence).
            let want_dumper = query_param(query, "dumper");
            let dumper = want_dumper.unwrap_or_else(|| "repos".to_owned());
            if dumper != "repos" {
                return text(StatusCode::OK, "application/json", "[]");
            }
            text(
                StatusCode::OK,
                "application/json",
                state_json(&context.scheduler, &context.sync_jobs).await,
            )
        }
        "/manual-purge" => manual_purge(context, req.uri().query().unwrap_or_default()).await,
        "/" => text(StatusCode::OK, "text/html; charset=utf-8", INDEX_HTML),
        // Go's `http.NotFound`.
        _ => text(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "404 page not found\n",
        ),
    }
}

/// `manualPurgeHandler`.
async fn manual_purge(context: &DebugContext, query: &str) -> Response<tonic::body::BoxBody> {
    // `r.FormValue("limit")` returns "" when the parameter is absent, and
    // `strconv.Atoi("")` fails; both surface as `invalid limit: ...`.
    let limit_param = query_param(query, "limit").unwrap_or_default();
    let limit = match go_atoi(&limit_param) {
        Ok(limit) => limit,
        Err(err) => return bad_request(&format!("invalid limit: {err}")),
    };
    if limit <= 0 {
        return bad_request("limit must be greater than 0");
    }
    if limit > 10000 {
        return bad_request("limit must be less than 10000");
    }

    let mut per_second = 1.0;
    if let Some(param) = query_param(query, "perSecond") {
        if !param.is_empty() {
            per_second = match go_parse_float(&param) {
                Ok(value) => value,
                Err(err) => return bad_request(&format!("invalid per second rate limit: {err}")),
            };
            if per_second <= 0.1 {
                return bad_request(&format!(
                    "invalid per second rate limit. Must be > 0.1, got {per_second:.6}"
                ));
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

fn bad_request(message: &str) -> Response<tonic::body::BoxBody> {
    // `http.Error` writes text/plain and a trailing newline.
    text(
        StatusCode::BAD_REQUEST,
        "text/plain; charset=utf-8",
        format!("{message}\n"),
    )
}

/// `UpdateScheduler.DebugDump` for the `repos` dumper, including
/// `ExternalServices().GetSyncJobs`.
pub async fn state_json(scheduler: &UpdateScheduler, jobs: &SyncJobsStore) -> String {
    let queue: Vec<serde_json::Value> = scheduler
        .debug_queue()
        .into_iter()
        .map(|update| {
            serde_json::json!({
                "Repo": { "ID": update.repo.id, "Name": update.repo.name },
                "Priority": update.priority as i32,
                "Seq": update.seq,
                "Updating": update.updating,
            })
        })
        .collect();
    let schedule: Vec<serde_json::Value> = scheduler
        .debug_schedule()
        .into_iter()
        .map(|entry| {
            serde_json::json!({
                "Repo": { "ID": entry.repo.id, "Name": entry.repo.name },
                // `time.Duration` marshals as an integer number of
                // nanoseconds.
                "Interval": entry.interval.as_nanos() as i64,
                "Due": format_rfc3339(entry.due),
            })
        })
        .collect();

    // A failed query leaves `SyncJobs` nil in the baseline, which marshals
    // as `null`.
    let sync_jobs = match jobs.list_sync_jobs().await {
        Ok(jobs) => serde_json::Value::Array(
            jobs.into_iter()
                .map(|job| {
                    serde_json::json!({
                        "ID": job.id,
                        "State": job.state,
                        "FailureMessage": job.failure_message.unwrap_or_default(),
                        "QueuedAt": format_optional_time(job.queued_at),
                        "StartedAt": format_optional_time(job.started_at),
                        "FinishedAt": format_optional_time(job.finished_at),
                        "ProcessAfter": format_optional_time(job.process_after),
                        "NumResets": job.num_resets,
                        "ExternalServiceID": job.external_service_id,
                        "NumFailures": job.num_failures,
                        "Cancel": job.cancel,
                        "ReposSynced": job.repos_synced,
                        "RepoSyncErrors": job.repo_sync_errors,
                        "ReposAdded": job.repos_added,
                        "ReposDeleted": job.repos_deleted,
                        "ReposModified": job.repos_modified,
                        "ReposUnmodified": job.repos_unmodified,
                    })
                })
                .collect(),
        ),
        Err(err) => {
            tracing::warn!(error = %err, "getting external service sync jobs for debug page");
            serde_json::Value::Null
        }
    };

    let dump = serde_json::json!([{
        "Name": "repos",
        "UpdateQueue": queue,
        "Schedule": schedule,
        "SyncJobs": sync_jobs,
    }]);
    let mut rendered = serde_json::to_string_pretty(&dump).expect("serialize debug dump");
    rendered.push('\n');
    rendered
}

/// Go's zero `time.Time` renders as `0001-01-01T00:00:00Z`.
fn format_optional_time(time: Option<sg_db::OffsetDateTime>) -> String {
    match time {
        Some(time) => format_rfc3339(SystemTime::from(time)),
        None => "0001-01-01T00:00:00Z".to_owned(),
    }
}

fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then(|| percent_decode(value))
    })
}

fn percent_decode(value: &str) -> String {
    value.replace("%20", " ").replace('+', " ")
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

/// `FormatRFC3339Nano`-compatible rendering (UTC, trailing zeros trimmed).
pub fn format_rfc3339(time: SystemTime) -> String {
    let (seconds, nanos) = match time.duration_since(UNIX_EPOCH) {
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
    };
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
}
