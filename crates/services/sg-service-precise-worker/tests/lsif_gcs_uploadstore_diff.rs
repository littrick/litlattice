//! M9.8kc differential harness: the GCS backend of the LSIF upload store.
//!
//! The Go oracle
//! (`internal/codeintel/shared/lsifuploadstore/zz_m9kc_oracle_test.go`, build
//! tag `m9kc_oracle`) drives the real `lsifuploadstore.New` (which dispatches
//! to `object.gcsStore`) against an httptest fake of the GCS JSON + XML API.
//! This harness replays the same corpus through
//! [`sg_codeintel::lsifuploadstore`] with an injected transport implementing
//! the same fake server, then renders the identical transcript. The runner
//! `test_ws/p9-go/run_lsif_gcs_uploadstore_diff.sh` diffs the two files.
//!
//! No network access: the transport is in-process.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use sg_codeintel::lsifuploadstore::{initialize_store_with_transport, Config};
use sg_objectstorage::gcs::{GcsHttpRequest, GcsHttpResponse, GcsTransport};
use sg_objectstorage::ObjectByteStream;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(serde::Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase", default)]
struct Case {
    name: String,
    kind: String,
    key: String,
    payload: Option<Payload>,
    seed: Vec<Seed>,
    prefix: String,
    max_age_seconds: i64,
    page_size: usize,
    manage_bucket: bool,
    bucket_exists: Option<bool>,
    create_status: u16,
    fail_op: String,
    fail_status: u16,
    fail_times: i32,
}

#[derive(serde::Deserialize, Clone, Default)]
#[serde(default)]
struct Payload {
    kind: String,
    value: String,
    byte: String,
    count: usize,
}

#[derive(serde::Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase", default)]
struct Seed {
    key: String,
    content: String,
    created: String,
    age_seconds: i64,
}

fn payload_bytes(payload: Option<&Payload>) -> Vec<u8> {
    let Some(payload) = payload else {
        return Vec::new();
    };
    match payload.kind.as_str() {
        "text" => payload.value.as_bytes().to_vec(),
        "repeat" => {
            let byte = payload.byte.as_bytes().first().copied().unwrap_or(b'x');
            vec![byte; payload.count]
        }
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Fake GCS server, behind the injectable transport
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Recorded {
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Recorded {
    fn header(&self, name: &str) -> &str {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .unwrap_or("")
    }
}

struct FakeState {
    bucket: bool,
    objects: HashMap<String, Vec<u8>>,
    created: HashMap<String, String>,
    order: Vec<String>,
    reqs: Vec<Recorded>,
    fail_status: u16,
    fail_times: i32,
    fail_kind: String,
    create_code: u16,
    page_size: usize,
    sessions: HashMap<String, Vec<u8>>,
    list_snapshot: Vec<String>,
}

impl FakeState {
    fn should_fail(&mut self, kind: &str) -> bool {
        if self.fail_kind == kind && self.fail_times > 0 {
            self.fail_times -= 1;
            return true;
        }
        false
    }
}

struct FakeTransport {
    state: Mutex<FakeState>,
    endpoint: String,
}

fn json_response(status: u16, value: serde_json::Value) -> GcsHttpResponse {
    GcsHttpResponse {
        status,
        headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
        body: serde_json::to_vec(&value).unwrap_or_default(),
    }
}

fn error_response(status: u16, message: &str) -> GcsHttpResponse {
    json_response(
        status,
        serde_json::json!({ "error": { "code": status, "message": message } }),
    )
}

/// The same canonical query surface as the Go oracle: only parameters that
/// carry upload-store semantics survive.
fn canonical_query(query: &[(String, String)]) -> String {
    const KEEP: [&str; 5] = ["uploadType", "name", "prefix", "pageToken", "project"];
    let mut parts: Vec<String> = query
        .iter()
        .filter(|(key, value)| KEEP.contains(&key.as_str()) && !value.is_empty())
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    parts.sort();
    parts.join("&")
}

impl FakeTransport {
    fn record(
        &self,
        state: &mut FakeState,
        request: &GcsHttpRequest,
        path: &str,
        query: &[(String, String)],
    ) {
        state.reqs.push(Recorded {
            method: request.method.clone(),
            path: path.to_owned(),
            query: canonical_query(query),
            headers: request.headers.clone(),
            body: request.body.clone(),
        });
    }

    fn handle(&self, request: &GcsHttpRequest) -> GcsHttpResponse {
        let url = url::Url::parse(&request.url).expect("request url");
        let path = url.path().to_owned();
        let query: Vec<(String, String)> = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        let query_value = |name: &str| {
            query
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        let bucket = "test-bucket".to_owned();
        let mut state = self.state.lock().expect("fake state");

        // POST /upload/session/{id}: chunk.
        if request.method == "POST" && path.starts_with("/upload/session/") {
            self.record(&mut state, request, &path, &query);
            let id = path.trim_start_matches("/upload/session/").to_owned();
            let Some(data) = state.sessions.get_mut(&id) else {
                return error_response(404, "not found");
            };
            data.extend_from_slice(&request.body);
            let content_range = request
                .headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("Content-Range"))
                .map(|(_, value)| value.as_str())
                .unwrap_or("");
            if content_range.ends_with("/*") {
                return GcsHttpResponse {
                    status: 200,
                    headers: vec![("X-Http-Status-Code-Override".to_owned(), "308".to_owned())],
                    body: Vec::new(),
                };
            }
            let size = state.sessions.get(&id).map(|data| data.len()).unwrap_or(0);
            return json_response(
                200,
                serde_json::json!({
                    "bucket": bucket,
                    "name": "uploaded",
                    "size": size.to_string(),
                }),
            );
        }

        // POST /upload/storage/v1/b/{bucket}/o: upload (multipart or resumable
        // initiation).
        if request.method == "POST" && path.starts_with("/upload/storage/v1/b/") {
            self.record(&mut state, request, &path, &query);
            if state.should_fail("upload") {
                let status = state.fail_status;
                return error_response(status, "upload failed");
            }
            match query_value("uploadType").as_str() {
                "multipart" => {
                    let content_type = request
                        .headers
                        .iter()
                        .find(|(key, _)| key.eq_ignore_ascii_case("Content-Type"))
                        .map(|(_, value)| value.as_str())
                        .unwrap_or("");
                    let boundary = content_type
                        .split("boundary=")
                        .nth(1)
                        .unwrap_or("")
                        .trim_matches('"');
                    let parts = parse_multipart(&request.body, boundary);
                    let name = parts
                        .first()
                        .and_then(|(_, data)| {
                            serde_json::from_slice::<serde_json::Value>(data).ok()
                        })
                        .and_then(|value| {
                            value
                                .get("name")
                                .and_then(|name| name.as_str())
                                .map(str::to_owned)
                        })
                        .unwrap_or_default();
                    let media = parts
                        .get(1)
                        .map(|(_, data)| data.clone())
                        .unwrap_or_default();
                    let size = media.len();
                    state.objects.insert(name.clone(), media);
                    set_created(&mut state, &name);
                    json_response(
                        200,
                        serde_json::json!({
                            "bucket": bucket,
                            "name": name,
                            "size": size.to_string(),
                        }),
                    )
                }
                "resumable" => {
                    let id = format!("s{}", state.sessions.len() + 1);
                    state.sessions.insert(id.clone(), Vec::new());
                    GcsHttpResponse {
                        status: 200,
                        headers: vec![(
                            "Location".to_owned(),
                            format!("{}/upload/session/{id}", self.endpoint),
                        )],
                        body: Vec::new(),
                    }
                }
                _ => error_response(400, "unhandled upload type"),
            }
        } else if request.method == "POST" && path == "/storage/v1/b" {
            self.record(&mut state, request, &path, &query);
            let code = if state.create_code == 0 {
                200
            } else {
                state.create_code
            };
            if code == 200 {
                state.bucket = true;
                json_response(200, serde_json::json!({ "name": bucket }))
            } else {
                error_response(code, "create failed")
            }
        } else if request.method == "GET"
            && path.starts_with("/storage/v1/b/")
            && path.ends_with("/o")
        {
            self.record(&mut state, request, &path, &query);
            if !state.bucket {
                return error_response(404, "not found");
            }
            if state.should_fail("list") {
                let status = state.fail_status;
                return error_response(status, "list failed");
            }
            handle_list(&mut state, &query, &bucket)
        } else if request.method == "GET"
            && path.starts_with("/storage/v1/b/")
            && !path.contains("/o/")
        {
            self.record(&mut state, request, &path, &query);
            if !state.bucket {
                return error_response(404, "not found");
            }
            json_response(200, serde_json::json!({ "name": bucket }))
        } else if request.method == "DELETE" && path.starts_with("/storage/v1/b/") {
            self.record(&mut state, request, &path, &query);
            if state.should_fail("delete") {
                let status = state.fail_status;
                return error_response(status, "delete failed");
            }
            let prefix = format!("/storage/v1/b/{bucket}/o/");
            let key = path.trim_start_matches(&prefix).to_owned();
            if state.objects.remove(&key).is_none() {
                return error_response(404, "not found");
            }
            state.created.remove(&key);
            state.order.retain(|value| value != &key);
            GcsHttpResponse {
                status: 204,
                headers: Vec::new(),
                body: Vec::new(),
            }
        } else if request.method == "GET" {
            self.record(&mut state, request, &path, &query);
            if state.should_fail("get") {
                let status = state.fail_status;
                return GcsHttpResponse {
                    status,
                    headers: Vec::new(),
                    body: b"boom".to_vec(),
                };
            }
            let prefix = format!("/{bucket}/");
            let key = path.trim_start_matches(&prefix).to_owned();
            match state.objects.get(&key) {
                Some(data) => GcsHttpResponse {
                    status: 200,
                    headers: vec![("Content-Length".to_owned(), data.len().to_string())],
                    body: data.clone(),
                },
                None => error_response(404, "not found"),
            }
        } else {
            GcsHttpResponse {
                status: 400,
                headers: Vec::new(),
                body: b"unhandled".to_vec(),
            }
        }
    }
}

/// `setCreated` on the Go side: newly uploaded objects get a fresh timestamp.
fn set_created(state: &mut FakeState, name: &str) {
    if !state.created.contains_key(name) {
        state.created.insert(name.to_owned(), rfc3339_now());
        state.order.push(name.to_owned());
    }
}

fn rfc3339_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .unwrap_or_default();
    rfc3339_from_secs(secs)
}

fn rfc3339_from_secs(secs: u64) -> String {
    // Minimal civil-from-days conversion, mirroring the Go oracle's
    // `time.RFC3339` (UTC, whole seconds).
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hour, minute, second) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn handle_list(state: &mut FakeState, query: &[(String, String)], bucket: &str) -> GcsHttpResponse {
    let prefix = query
        .iter()
        .find(|(key, _)| key == "prefix")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    let token = query
        .iter()
        .find(|(key, _)| key == "pageToken")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    let idx = token
        .strip_prefix('t')
        .and_then(|rest| rest.parse::<usize>().ok())
        .unwrap_or(0);

    if token.is_empty() {
        state.list_snapshot = state
            .order
            .iter()
            .filter(|name| name.starts_with(&prefix))
            .cloned()
            .collect();
    }
    let names = state.list_snapshot.clone();
    let size = if state.page_size == 0 {
        1000
    } else {
        state.page_size
    };
    let start = idx.min(names.len());
    let end = (start + size).min(names.len());
    let items: Vec<serde_json::Value> = names[start..end]
        .iter()
        .map(|name| {
            let created = state.created.get(name).cloned().unwrap_or_default();
            if created.is_empty() {
                serde_json::json!({ "name": name })
            } else {
                serde_json::json!({ "name": name, "timeCreated": created })
            }
        })
        .collect();
    let mut response = serde_json::json!({ "items": items });
    if end < names.len() {
        response["nextPageToken"] = serde_json::Value::String(format!("t{end}"));
    }
    let _ = bucket;
    json_response(200, response)
}

#[async_trait]
impl GcsTransport for FakeTransport {
    async fn execute(&self, request: GcsHttpRequest) -> Result<GcsHttpResponse, String> {
        Ok(self.handle(&request))
    }
}

/// `(part headers, part body)`.
type MultipartPart = (Vec<(String, String)>, Vec<u8>);

fn parse_multipart(body: &[u8], boundary: &str) -> Vec<MultipartPart> {
    let delimiter = format!("--{boundary}");
    let delimiter = delimiter.as_bytes();
    let mut parts = Vec::new();
    let mut positions = Vec::new();
    let mut index = 0usize;
    while index + delimiter.len() <= body.len() {
        if &body[index..index + delimiter.len()] == delimiter {
            positions.push(index);
            index += delimiter.len();
        } else {
            index += 1;
        }
    }
    for window in positions.windows(2) {
        let start = window[0] + delimiter.len();
        let end = window[1];
        let mut part = &body[start..end];
        part = part.strip_prefix(b"\r\n").unwrap_or(part);
        part = part.strip_suffix(b"\r\n").unwrap_or(part);
        if part.starts_with(b"--") {
            continue;
        }
        let Some(split) = find_subslice(part, b"\r\n\r\n") else {
            continue;
        };
        let header_bytes = &part[..split];
        let data = part[split + 4..].to_vec();
        let headers = String::from_utf8_lossy(header_bytes)
            .lines()
            .filter_map(|line| line.split_once(':'))
            .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
            .collect();
        parts.push((headers, data));
    }
    parts
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---------------------------------------------------------------------------
// Rendering (must match m9kcRender in the Go oracle)
// ---------------------------------------------------------------------------

fn canonical_json(data: &[u8]) -> String {
    match serde_json::from_slice::<serde_json::Value>(data) {
        Ok(value) => serde_json::to_string(&value).unwrap_or_else(|_| "invalid".to_owned()),
        Err(_) => "invalid".to_owned(),
    }
}

fn normalize_content_type(value: &str) -> &str {
    if value.starts_with("multipart/related") {
        "multipart/related"
    } else {
        value
    }
}

fn render_body(request: &Recorded) -> String {
    let content_type = request.header("Content-Type");
    if content_type.starts_with("multipart/related") {
        let boundary = content_type
            .split("boundary=")
            .nth(1)
            .unwrap_or("")
            .trim_matches('"');
        let parts = parse_multipart(&request.body, boundary);
        let mut rendered = Vec::new();
        for (index, (headers, data)) in parts.iter().enumerate() {
            if index == 0 {
                rendered.push(format!("meta={}", canonical_json(data)));
            } else {
                let part_type = headers
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("Content-Type"))
                    .map(|(_, value)| value.as_str())
                    .unwrap_or("");
                rendered.push(format!(
                    "media-ct={part_type} media-len={} media-sha256={}",
                    data.len(),
                    sha256_hex(data)
                ));
            }
        }
        return format!("multipart {}", rendered.join(" "));
    }
    if content_type.contains("application/json") {
        return format!("json={}", canonical_json(&request.body));
    }
    if !request.body.is_empty() {
        return format!(
            "media-len={} media-sha256={}",
            request.body.len(),
            sha256_hex(&request.body)
        );
    }
    "empty".to_owned()
}

fn render(name: &str, reqs: &[Recorded], result: &str) -> String {
    let mut deletes: Vec<&Recorded> = reqs.iter().filter(|req| req.method == "DELETE").collect();
    let regular: Vec<&Recorded> = reqs.iter().filter(|req| req.method != "DELETE").collect();
    deletes.sort_by(|left, right| {
        format!("{} {}", left.method, left.path).cmp(&format!("{} {}", right.method, right.path))
    });
    let mut out = String::new();
    out.push_str(&format!("case {name}\n"));
    for (index, request) in regular.into_iter().chain(deletes).enumerate() {
        out.push_str(&format!(
            "req {index} {} {} query={}\n",
            request.method, request.path, request.query
        ));
        out.push_str(&format!(
            "hdr {index} ct={} cr={} ut={} auth={}\n",
            normalize_content_type(request.header("Content-Type")),
            request.header("Content-Range"),
            request.header("X-Upload-Content-Type"),
            if request.header("Authorization").is_empty() {
                "none"
            } else {
                "bearer"
            }
        ));
        out.push_str(&format!("body {index} {}\n", render_body(request)));
    }
    out.push_str(result);
    out.push('\n');
    out.push_str(&format!("end {name}\n"));
    out
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn class(err: &str) -> &'static str {
    if err.contains("not found") || err.contains("doesn't exist") || err.contains("Error 404") {
        "not_found"
    } else {
        "other"
    }
}

// ---------------------------------------------------------------------------
// Case execution
// ---------------------------------------------------------------------------

fn build_fake(case: &Case) -> Arc<FakeTransport> {
    let mut state = FakeState {
        bucket: case.bucket_exists.unwrap_or(true),
        objects: HashMap::new(),
        created: HashMap::new(),
        order: Vec::new(),
        reqs: Vec::new(),
        fail_status: case.fail_status,
        fail_times: case.fail_times,
        fail_kind: case.fail_op.clone(),
        create_code: case.create_status,
        page_size: case.page_size,
        sessions: HashMap::new(),
        list_snapshot: Vec::new(),
    };
    for seed in &case.seed {
        let created = if seed.created.is_empty() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|value| value.as_secs())
                .unwrap_or_default();
            let age = seed.age_seconds.max(0) as u64;
            rfc3339_from_secs(now.saturating_sub(age))
        } else {
            seed.created.clone()
        };
        state
            .objects
            .insert(seed.key.clone(), seed.content.as_bytes().to_vec());
        state.created.insert(seed.key.clone(), created);
        state.order.push(seed.key.clone());
    }
    Arc::new(FakeTransport {
        state: Mutex::new(state),
        endpoint: "http://fake".to_owned(),
    })
}

fn fake_config(case: &Case) -> Config {
    Config {
        backend: "gcs".to_owned(),
        manage_bucket: case.manage_bucket,
        bucket: "test-bucket".to_owned(),
        ttl: Duration::from_secs(168 * 3600),
        s3_region: String::new(),
        s3_endpoint: String::new(),
        s3_use_path_style: false,
        s3_access_key_id: String::new(),
        s3_secret_access_key: String::new(),
        s3_session_token: String::new(),
        gcs_project_id: "pid".to_owned(),
        gcs_credentials_file: String::new(),
        gcs_credentials_file_contents: String::new(),
    }
}

async fn run_case(case: &Case) -> String {
    let transport = build_fake(case);
    let config = fake_config(case);

    let result =
        match initialize_store_with_transport(&config, "http://fake", transport.clone()).await {
            Err(err) => format!("result err class={}", class(&err)),
            Ok(_) if case.kind == "init" => "result ok".to_owned(),
            Ok(store) => match case.kind.as_str() {
                "upload" => {
                    let data = payload_bytes(case.payload.as_ref());
                    match store.upload(&case.key, ObjectByteStream::from(data)).await {
                        Ok(bytes) => format!("result ok bytes={bytes}"),
                        Err(err) => format!("result err class={}", class(&err.to_string())),
                    }
                }
                "get" => match store.get(&case.key).await {
                    Ok(body) => {
                        let mut data = Vec::new();
                        match body.into_async_read().read_to_end(&mut data).await {
                            Ok(_) => {
                                format!("result ok len={} sha256={}", data.len(), sha256_hex(&data))
                            }
                            Err(_) => "result err class=other".to_owned(),
                        }
                    }
                    Err(err) => format!("result err class={}", class(&err.to_string())),
                },
                "delete" => match store.delete(&case.key).await {
                    Ok(()) => "result ok".to_owned(),
                    Err(err) => format!("result err class={}", class(&err.to_string())),
                },
                "expire" => {
                    // The baseline ExpireObjects returns nil even when the iterator
                    // or a delete fails (the next run retries).
                    let _ = store
                        .expire_objects(
                            &case.prefix,
                            Duration::from_secs(case.max_age_seconds as u64),
                        )
                        .await;
                    "result ok".to_owned()
                }
                _ => "result err class=other unknown-kind".to_owned(),
            },
        };

    let state = transport.state.lock().expect("fake state");
    render(&case.name, &state.reqs, &result)
}

#[tokio::test(flavor = "multi_thread")]
async fn lsif_gcs_uploadstore_diff() {
    let (Ok(corpus_path), Ok(out_path)) = (std::env::var("M9KC_CORPUS"), std::env::var("M9KC_OUT"))
    else {
        eprintln!("M9KC_CORPUS / M9KC_OUT not set; skipping LSIF GCS uploadstore diff");
        return;
    };
    let raw = std::fs::read(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_slice(&raw).expect("parse corpus");

    let mut transcript = String::new();
    for case in &corpus.cases {
        transcript.push_str(&run_case(case).await);
    }
    std::fs::write(&out_path, transcript).expect("write transcript");
    println!("M9KC_RUST_HARNESS_OK ({} cases)", corpus.cases.len());
}
