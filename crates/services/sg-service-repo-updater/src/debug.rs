#![forbid(unsafe_code)]

//! Per-service debug endpoint (`SRC_PROF_HTTP`), equivalent to the baseline
//! `internal/debugserver` plus the repo-updater-specific
//! `/repo-updater-state` dumper.
//!
//! The purge endpoints (`/manual-purge`) land with the purge slice; the Go
//! server also exposes pprof, `/vars` and `/debug/requests`, which this pilot
//! does not implement (it serves Prometheus `/metrics`, which monitoring
//! scrapes).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use http::{header, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use tokio_util::sync::CancellationToken;

use crate::scheduler::UpdateScheduler;

/// Baseline `debugserver` default (`SRC_PROF_HTTP`).
pub const DEFAULT_ADDR: &str = ":6060";

const INDEX_HTML: &str =
    "<!DOCTYPE html>\n<html><head><title>Sourcegraph repo-updater debug</title></head>\
<body><a href=\"metrics\">Metrics</a><br>\
<a href=\"repo-updater-state?format=json\">Repo Updater State</a><br></body></html>\n";

/// Bind `addr` and serve the debug HTTP surface until `shutdown` is cancelled.
/// A bind error is logged and swallowed.
pub async fn serve(addr: &str, scheduler: Arc<UpdateScheduler>, shutdown: CancellationToken) {
    let listener = match sg_http::listen(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::warn!(addr, error = %err, "debug server failed to listen");
            return;
        }
    };
    tracing::info!(addr, "debug server listening");
    if let Err(err) = sg_http::serve_http(listener, handler(scheduler), shutdown).await {
        tracing::error!(addr, error = %err, "debug server failed");
    }
}

pub fn handler(scheduler: Arc<UpdateScheduler>) -> sg_http::HttpHandler {
    Arc::new(move |req: Request<Incoming>| {
        let scheduler = Arc::clone(&scheduler);
        Box::pin(async move { route(&scheduler, &req) })
    })
}

fn route(scheduler: &UpdateScheduler, req: &Request<Incoming>) -> Response<tonic::body::BoxBody> {
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
            text(StatusCode::OK, "application/json", state_json(scheduler))
        }
        "/" => text(StatusCode::OK, "text/html; charset=utf-8", INDEX_HTML),
        // Go's `http.NotFound`.
        _ => text(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "404 page not found\n",
        ),
    }
}

/// `UpdateScheduler.DebugDump` for the `repos` dumper.
pub fn state_json(scheduler: &UpdateScheduler) -> String {
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
    let dump = serde_json::json!([{
        "Name": "repos",
        "UpdateQueue": queue,
        "Schedule": schedule,
        // `SyncJobs` requires the external-service syncer (later slice).
        "SyncJobs": serde_json::Value::Null,
    }]);
    let mut rendered = serde_json::to_string_pretty(&dump).expect("serialize debug dump");
    rendered.push('\n');
    rendered
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
}
